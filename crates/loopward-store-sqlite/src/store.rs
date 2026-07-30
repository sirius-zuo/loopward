//! [`SqliteLoopStore`]: connection configuration, `load`, and `append`.
//!
//! Every mutation opens its transaction with `BEGIN IMMEDIATE`, so `SQLite`
//! takes the write lock up front instead of discovering a failed upgrade
//! after it has already read. Every fencing, revision, and Checkpoint
//! version check then happens *inside* that transaction, against rows read
//! under that same write lock — never as a separate check-then-act with a
//! gap another driver could slip through (§2, §5, §7, §10).

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use loopward_api::ids::{LoopId, Timestamp};
use loopward_core::{AggregateState, LoopEvent};
use loopward_runtime::ports::{
    AcquireLease, CheckpointCommitRequest, LoopLease, LoopStore, StoreError, StoredLoop,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, Sqlite, SqliteConnection, SqlitePool, Transaction};
use time::format_description::well_known::Rfc3339;

use crate::error::SqliteStoreError;
use crate::{checkpoint, event, lease};

/// The `SQLite` statement every mutating transaction begins with.
///
/// `BEGIN IMMEDIATE` acquires the database write lock at `BEGIN` rather
/// than on the first write, so a busy database is reported (and retried
/// under `busy_timeout`) before this transaction has read any state it
/// would then have to discard.
pub(crate) const BEGIN_IMMEDIATE: &str = "BEGIN IMMEDIATE";

/// How aggressively `SQLite` must fsync (`docs/03` §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Synchronous {
    /// Phase One's default: durable across process crashes, and — in WAL
    /// mode — safe against corruption on power loss, at the cost of
    /// possibly losing the most recent commits.
    #[default]
    Normal,
    /// The high-durability choice `docs/03` §8 permits deployments to make.
    Full,
}

impl From<Synchronous> for SqliteSynchronous {
    fn from(value: Synchronous) -> Self {
        match value {
            Synchronous::Normal => Self::Normal,
            Synchronous::Full => Self::Full,
        }
    }
}

/// How to open the `SQLite` database backing a [`SqliteLoopStore`].
#[derive(Debug, Clone)]
pub struct SqliteStoreConfig {
    database_path: PathBuf,
    busy_timeout: Duration,
    synchronous: Synchronous,
    max_connections: u32,
    create_if_missing: bool,
}

impl SqliteStoreConfig {
    /// Phase One's defaults for the database at `database_path`: WAL,
    /// foreign keys enforced, a five second busy timeout, and
    /// `synchronous = NORMAL` (§8).
    #[must_use]
    pub fn new(database_path: impl Into<PathBuf>) -> Self {
        Self {
            database_path: database_path.into(),
            busy_timeout: Duration::from_secs(5),
            synchronous: Synchronous::Normal,
            max_connections: 8,
            create_if_missing: true,
        }
    }

    /// Sets how long a connection waits for a contended write lock before
    /// reporting the database busy.
    #[must_use]
    pub fn with_busy_timeout(mut self, busy_timeout: Duration) -> Self {
        self.busy_timeout = busy_timeout;
        self
    }

    /// Selects the `synchronous` durability level.
    #[must_use]
    pub fn with_synchronous(mut self, synchronous: Synchronous) -> Self {
        self.synchronous = synchronous;
        self
    }

    /// Sets the pool's maximum connection count.
    #[must_use]
    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Whether to create the database file when it does not exist.
    #[must_use]
    pub fn with_create_if_missing(mut self, create_if_missing: bool) -> Self {
        self.create_if_missing = create_if_missing;
        self
    }

    /// The database file this configuration opens.
    #[must_use]
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// The per-connection options this configuration produces.
    ///
    /// Held on [`SqliteConnectOptions`] rather than issued as one-off
    /// `PRAGMA` statements after connecting, so *every* connection the pool
    /// opens — including ones it opens later, under load, to replace a
    /// closed one — comes up configured (§8).
    fn connect_options(&self) -> SqliteConnectOptions {
        SqliteConnectOptions::new()
            .filename(&self.database_path)
            .create_if_missing(self.create_if_missing)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(self.busy_timeout)
            .synchronous(self.synchronous.into())
    }
}

/// A [`LoopStore`] backed by a real `SQLite` database.
///
/// Cheap to clone: the pool is shared.
#[derive(Debug, Clone)]
pub struct SqliteLoopStore {
    pool: SqlitePool,
}

impl SqliteLoopStore {
    /// Opens (creating if configured) the database, configures every pooled
    /// connection per §8, and applies all pending migrations.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteStoreError::Sqlx`] if the database cannot be opened
    /// and [`SqliteStoreError::Migrate`] if migrations cannot be applied.
    pub async fn open(config: SqliteStoreConfig) -> Result<Self, SqliteStoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(config.connect_options())
            .await?;
        crate::run_migrations(&pool).await?;
        Ok(Self { pool })
    }

    /// Wraps an already-open, already-migrated pool.
    ///
    /// The caller is responsible for having configured the pool per §8 (see
    /// [`SqliteStoreConfig`]) and for having run [`crate::run_migrations`].
    #[must_use]
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// The underlying pool, so an adapter sharing this database — a
    /// [`crate::FileResourceBroker`], say — can reuse it.
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Begins a `BEGIN IMMEDIATE` transaction on this store's pool.
    pub(crate) async fn begin(&self) -> Result<Transaction<'static, Sqlite>, SqliteStoreError> {
        Ok(self.pool.begin_with(BEGIN_IMMEDIATE).await?)
    }

    /// Appends `events` inside an already-open write transaction.
    async fn append_in_transaction(
        connection: &mut SqliteConnection,
        loop_id: LoopId,
        expected_revision: u64,
        fencing_token: u64,
        events: &[LoopEvent],
        now: Timestamp,
    ) -> Result<StoredLoop, StoreError> {
        // Fencing first, then existence, then the revision — the same order
        // the port's own contract test drives, and all three read under the
        // write lock this transaction already holds.
        let current_token = lease::current_fencing_token(connection, loop_id).await?;
        if fencing_token != current_token {
            return Err(StoreError::StaleFencingToken {
                loop_id,
                given: fencing_token,
                current: current_token,
            });
        }

        let Some(mut state) = load_projection(connection, loop_id).await? else {
            return Err(StoreError::LoopNotFound(loop_id));
        };
        let actual_revision = state.execution.aggregate_revision;
        if actual_revision != expected_revision {
            return Err(StoreError::RevisionConflict {
                loop_id,
                expected: expected_revision,
                actual: actual_revision,
            });
        }

        let mut sequence = event::max_sequence(connection, loop_id).await?;
        let mut revision = actual_revision;
        for domain_event in events {
            revision += 1;
            sequence += 1;
            event::insert(connection, loop_id, sequence, revision, domain_event, now).await?;
        }
        state.execution.aggregate_revision = revision;

        write_projection_row(connection, loop_id, &state).await?;
        load_stored(connection, loop_id)
            .await?
            .ok_or(StoreError::LoopNotFound(loop_id))
    }
}

#[async_trait]
impl LoopStore for SqliteLoopStore {
    async fn load(&self, loop_id: LoopId) -> Result<StoredLoop, StoreError> {
        // A deferred read transaction, not a bare pooled connection: the
        // projection and the journal are two statements, and §10 requires
        // them to be transactionally consistent. Reading them under one
        // transaction pins a single WAL snapshot, so a Checkpoint commit
        // landing between the two can never produce a `StoredLoop` whose
        // projection and journal disagree.
        let mut transaction = self.pool.begin().await.map_err(SqliteStoreError::from)?;
        let stored = load_stored(&mut transaction, loop_id).await;
        transaction
            .rollback()
            .await
            .map_err(SqliteStoreError::from)?;
        stored?.ok_or(StoreError::LoopNotFound(loop_id))
    }

    async fn append(
        &self,
        loop_id: LoopId,
        expected_revision: u64,
        fencing_token: u64,
        events: Vec<LoopEvent>,
    ) -> Result<StoredLoop, StoreError> {
        let now = Timestamp::now_utc();
        let mut transaction = self.begin().await?;
        let outcome = Self::append_in_transaction(
            &mut transaction,
            loop_id,
            expected_revision,
            fencing_token,
            &events,
            now,
        )
        .await;
        finish(transaction, outcome).await
    }

    async fn commit_checkpoint(
        &self,
        request: CheckpointCommitRequest,
    ) -> Result<StoredLoop, StoreError> {
        let now = Timestamp::now_utc();
        let mut transaction = self.begin().await?;
        let outcome = checkpoint::commit_in_transaction(&mut transaction, &request, now).await;
        finish(transaction, outcome).await
    }

    async fn acquire_lease(&self, request: AcquireLease) -> Result<LoopLease, StoreError> {
        let now = Timestamp::now_utc();
        let mut transaction = self.begin().await?;
        let outcome = lease::acquire_in_transaction(&mut transaction, request, now).await;
        finish(transaction, outcome).await
    }

    async fn renew_lease(&self, lease: &LoopLease) -> Result<LoopLease, StoreError> {
        let now = Timestamp::now_utc();
        let mut transaction = self.begin().await?;
        let outcome = lease::renew_in_transaction(&mut transaction, lease, now).await;
        finish(transaction, outcome).await
    }
}

/// Commits `outcome`'s transaction on success, and rolls it back — before
/// returning — on failure.
///
/// Rollback is explicit rather than left to [`Transaction`]'s `Drop`, so a
/// caller that observes the error also observes a database with no trace of
/// the attempt (§9: "`SQLite` rollback preserves prior Checkpoint").
async fn finish<T>(
    transaction: Transaction<'static, Sqlite>,
    outcome: Result<T, StoreError>,
) -> Result<T, StoreError> {
    match outcome {
        Ok(value) => {
            transaction.commit().await.map_err(SqliteStoreError::from)?;
            Ok(value)
        }
        Err(error) => {
            transaction
                .rollback()
                .await
                .map_err(SqliteStoreError::from)?;
            Err(error)
        }
    }
}

/// Reads a Loop's durable projection, or `None` if the Loop does not exist.
pub(crate) async fn load_projection(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<Option<AggregateState>, SqliteStoreError> {
    let Some(row) = sqlx::query("SELECT projection_json FROM loops WHERE loop_id = ?1")
        .bind(loop_id.to_string())
        .fetch_optional(connection)
        .await?
    else {
        return Ok(None);
    };
    let state: AggregateState =
        serde_json::from_str(row.try_get::<&str, _>(0)?).map_err(|source| {
            SqliteStoreError::Decode {
                what: "projection",
                source,
            }
        })?;
    Ok(Some(state))
}

/// Reads a Loop's durable projection together with its journal.
pub(crate) async fn load_stored(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<Option<StoredLoop>, SqliteStoreError> {
    let Some(state) = load_projection(connection, loop_id).await? else {
        return Ok(None);
    };
    let events = event::read_all(connection, loop_id).await?;
    Ok(Some(StoredLoop { state, events }))
}

/// Inserts or updates the `loops` row for `state`.
///
/// `projection_json` is authoritative; `status`, `aggregate_revision`, and
/// `checkpoint_version` are the typed query columns §3 calls for, written
/// from the very same projection in the very same statement so they can
/// never drift apart.
pub(crate) async fn write_projection_row(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
    state: &AggregateState,
) -> Result<(), SqliteStoreError> {
    let projection = serde_json::to_string(state).map_err(|source| SqliteStoreError::Encode {
        what: "projection",
        source,
    })?;

    sqlx::query(
        "INSERT INTO loops (loop_id, status, aggregate_revision, checkpoint_version, projection_json) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(loop_id) DO UPDATE SET \
             status = excluded.status, \
             aggregate_revision = excluded.aggregate_revision, \
             checkpoint_version = excluded.checkpoint_version, \
             projection_json = excluded.projection_json",
    )
    .bind(loop_id.to_string())
    .bind(state.execution.status.label())
    .bind(sql_i64("aggregate revision", state.execution.aggregate_revision)?)
    .bind(sql_i64("checkpoint version", state.execution.checkpoint_version)?)
    .bind(projection)
    .execute(connection)
    .await?;

    Ok(())
}

/// Narrows a domain `u64` to the signed integer `SQLite` actually stores.
pub(crate) fn sql_i64(what: &'static str, value: u64) -> Result<i64, SqliteStoreError> {
    i64::try_from(value).map_err(|_| SqliteStoreError::integer_range(what, &value))
}

/// Widens a stored `SQLite` integer back to the domain's `u64`.
pub(crate) fn sql_u64(what: &'static str, value: i64) -> Result<u64, SqliteStoreError> {
    u64::try_from(value).map_err(|_| SqliteStoreError::integer_range(what, &value))
}

/// Renders a timestamp in the RFC 3339 form every `TEXT` timestamp column
/// stores, so stored instants sort lexicographically in journal order.
pub(crate) fn format_timestamp(value: Timestamp) -> Result<String, SqliteStoreError> {
    value
        .format(&Rfc3339)
        .map_err(|_| SqliteStoreError::Timestamp {
            value: value.to_string(),
        })
}

/// Renders bytes as lower-case hexadecimal, the form every digest is stored
/// and referenced in (`sha256:<hex>`).
pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut rendered, byte| {
            // Writing to a `String` is infallible.
            let _ = write!(rendered, "{byte:02x}");
            rendered
        },
    )
}

/// Parses a stored RFC 3339 timestamp.
pub(crate) fn parse_timestamp(value: &str) -> Result<Timestamp, SqliteStoreError> {
    Timestamp::parse(value, &Rfc3339).map_err(|_| SqliteStoreError::Timestamp {
        value: value.to_owned(),
    })
}
