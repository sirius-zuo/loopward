//! Checkpoint commit: the one transaction that may advance a Loop's
//! Checkpoint version (`docs/03-durable-state-and-transactions.md` §5).
//!
//! One `SQLite` transaction writes the immutable Checkpoint, the current
//! Loop projection, the Episode/Session/Work Unit/Attempt state the commit
//! implies, the resource edges, and the journal events — or it writes
//! nothing at all. Partial commits are forbidden, and the statements below
//! are ordered so that every referential and invariant check is a database
//! constraint or an in-transaction read, never a check performed before the
//! transaction opened.
//!
//! Genesis is the one asymmetric case. [`LoopStore`] has no "create a Loop"
//! method, so a commit against an unknown `loop_id` is accepted as genesis
//! creation exactly when both `expected_revision` and
//! `expected_checkpoint_version` are `0`; any other expectation against an
//! unknown `loop_id` is [`StoreError::LoopNotFound`].
//!
//! [`LoopStore`]: loopward_runtime::ports::LoopStore

use loopward_api::ids::{EpisodeId, LoopId, Timestamp};
use loopward_core::{
    AggregateState, Checkpoint, Episode, EpisodeStatus, RuntimeSession, SessionStatus, WorkUnit,
    WorkUnitAttempt, work_unit_status_label,
};
use loopward_runtime::ports::{CheckpointCommitRequest, StoreError, StoredLoop};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use crate::error::SqliteStoreError;
use crate::store::{
    format_timestamp, hex_lower, load_stored, sql_i64, sql_u64, write_projection_row,
};
use crate::{CHECKPOINT_SCHEMA_VERSION, WORK_UNIT_SCHEMA_VERSION, event, lease};

/// Commits a Checkpoint inside an already-open `BEGIN IMMEDIATE`
/// transaction. The caller commits or rolls back the transaction based on
/// what this returns; nothing here is durable until it does.
pub(crate) async fn commit_in_transaction(
    connection: &mut SqliteConnection,
    request: &CheckpointCommitRequest,
    now: Timestamp,
) -> Result<StoredLoop, StoreError> {
    let loop_id = request.loop_id;

    // 1. Fencing. Read under this transaction's write lock, so no
    //    acquisition can interleave between the check and the writes below.
    let current_token = lease::current_fencing_token(connection, loop_id).await?;
    if request.fencing_token != current_token {
        return Err(StoreError::StaleFencingToken {
            loop_id,
            given: request.fencing_token,
            current: current_token,
        });
    }

    // 2. Optimistic concurrency, or genesis.
    match read_versions(connection, loop_id).await? {
        None => {
            if request.expected_revision != 0 || request.expected_checkpoint_version != 0 {
                return Err(StoreError::LoopNotFound(loop_id));
            }
        }
        Some((actual_revision, actual_checkpoint_version)) => {
            if actual_revision != request.expected_revision {
                return Err(StoreError::RevisionConflict {
                    loop_id,
                    expected: request.expected_revision,
                    actual: actual_revision,
                });
            }
            if actual_checkpoint_version != request.expected_checkpoint_version {
                return Err(StoreError::CheckpointVersionConflict {
                    loop_id,
                    expected: request.expected_checkpoint_version,
                    actual: actual_checkpoint_version,
                });
            }
        }
    }

    // 3. The request must describe a self-consistent, monotonically
    //    advancing state before any of it is written.
    let state = validated_state(request)?;

    // 4. The projection row first: every other table is a foreign key onto
    //    it, and on genesis this is the row that brings the Loop into
    //    existence.
    write_projection_row(connection, loop_id, &state).await?;

    // 5. The journal. Sequences continue the Loop's existing journal with
    //    no gap, and each event names the revision it advanced the Loop to.
    let mut sequence = event::max_sequence(connection, loop_id).await?;
    let mut revision = request.expected_revision;
    for domain_event in &request.events {
        revision += 1;
        sequence += 1;
        event::insert(connection, loop_id, sequence, revision, domain_event, now).await?;
    }

    // 6. The immutable Checkpoint. `checkpoints.checkpoint_id` is UNIQUE
    //    and `(loop_id, checkpoint_version)` is its primary key, so a
    //    replayed or colliding commit fails here — inside the transaction,
    //    which then rolls back over everything above.
    insert_checkpoint(connection, &request.checkpoint, now).await?;

    // 7. Lifecycle projections and their §10 invariants.
    write_lifecycle(connection, loop_id, &state).await?;

    // 8. Resource edges, which is where "unfinalized resources cannot enter
    //    a Checkpoint" is enforced.
    write_resource_edges(connection, loop_id, &state).await?;

    load_stored(connection, loop_id)
        .await?
        .ok_or(StoreError::LoopNotFound(loop_id))
}

/// The Loop's current `(aggregate_revision, checkpoint_version)`, or `None`
/// when the Loop does not exist yet.
async fn read_versions(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<Option<(u64, u64)>, SqliteStoreError> {
    let Some(row) =
        sqlx::query("SELECT aggregate_revision, checkpoint_version FROM loops WHERE loop_id = ?1")
            .bind(loop_id.to_string())
            .fetch_optional(connection)
            .await?
    else {
        return Ok(None);
    };
    Ok(Some((
        sql_u64("aggregate revision", row.try_get::<i64, _>(0)?)?,
        sql_u64("checkpoint version", row.try_get::<i64, _>(1)?)?,
    )))
}

/// Checks the request against §10's monotonicity invariants and returns the
/// projection to persist.
///
/// The persisted projection carries the Checkpoint being committed, so a
/// later `load` can never report a Loop whose projection disagrees with the
/// `checkpoints` row this same transaction wrote.
fn validated_state(request: &CheckpointCommitRequest) -> Result<AggregateState, SqliteStoreError> {
    let loop_id = request.loop_id;
    let checkpoint = &request.checkpoint;
    let state = &request.state;

    if checkpoint.loop_id != loop_id {
        return Err(SqliteStoreError::InconsistentRequest {
            loop_id,
            detail: format!("checkpoint belongs to loop {}", checkpoint.loop_id),
        });
    }
    if state.execution.id != loop_id {
        return Err(SqliteStoreError::InconsistentRequest {
            loop_id,
            detail: format!("projection belongs to loop {}", state.execution.id),
        });
    }
    if state.execution.checkpoint_version != checkpoint.version {
        return Err(SqliteStoreError::InconsistentRequest {
            loop_id,
            detail: format!(
                "projection is at checkpoint version {} but the committed checkpoint is version {}",
                state.execution.checkpoint_version, checkpoint.version
            ),
        });
    }
    if checkpoint.version <= request.expected_checkpoint_version {
        return Err(SqliteStoreError::NonMonotonicCheckpointVersion {
            loop_id,
            expected_checkpoint_version: request.expected_checkpoint_version,
            given: checkpoint.version,
        });
    }

    // A Checkpoint commit is itself a durable lifecycle transition, so the
    // revision must advance past the expected one even when the commit
    // journals no events of its own (§2).
    let events = request.events.len();
    let required = request.expected_revision
        + u64::try_from(events.max(1))
            .map_err(|_| SqliteStoreError::integer_range("event count", &events))?;
    if state.execution.aggregate_revision < required {
        return Err(SqliteStoreError::NonMonotonicAggregateRevision {
            loop_id,
            required,
            events,
            given: state.execution.aggregate_revision,
        });
    }

    let mut state = state.clone();
    state.checkpoint = checkpoint.clone();
    Ok(state)
}

/// Writes the immutable Checkpoint row and its integrity digest.
///
/// The digest is `SHA-256` over exactly the bytes stored in
/// `checkpoint_json` (§5), computed here rather than trusted from the
/// request so it always describes what is actually durable.
async fn insert_checkpoint(
    connection: &mut SqliteConnection,
    checkpoint: &Checkpoint,
    now: Timestamp,
) -> Result<(), SqliteStoreError> {
    let checkpoint_json =
        serde_json::to_string(checkpoint).map_err(|source| SqliteStoreError::Encode {
            what: "checkpoint",
            source,
        })?;
    let digest = format!(
        "sha256:{}",
        hex_lower(&Sha256::digest(checkpoint_json.as_bytes()))
    );

    sqlx::query(
        "INSERT INTO checkpoints \
         (loop_id, checkpoint_version, checkpoint_id, aggregate_revision, parent_checkpoint_id, \
          schema_version, checkpoint_json, digest, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .bind(checkpoint.loop_id.to_string())
    .bind(sql_i64("checkpoint version", checkpoint.version)?)
    .bind(checkpoint.id.to_string())
    .bind(sql_i64(
        "aggregate revision",
        checkpoint.aggregate_revision,
    )?)
    .bind(checkpoint.parent.map(|parent| parent.to_string()))
    .bind(CHECKPOINT_SCHEMA_VERSION)
    .bind(checkpoint_json)
    .bind(digest)
    .bind(format_timestamp(now)?)
    .execute(connection)
    .await?;

    Ok(())
}

/// Projects the Episode, Runtime Session, Work Unit, and Attempt the commit
/// carries into their typed tables, enforcing the §10 lifecycle invariants
/// that have no port-level error variant.
async fn write_lifecycle(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
    state: &AggregateState,
) -> Result<(), SqliteStoreError> {
    if let Some(episode) = state.episode.as_ref() {
        write_episode(connection, loop_id, episode).await?;
    }
    if let Some(session) = state.session.as_ref() {
        write_session(connection, session).await?;
    }
    if let Some(work_unit) = state.work_unit.as_ref() {
        write_work_unit(connection, loop_id, work_unit).await?;
    }
    if let Some(attempt) = state.attempt.as_ref() {
        write_attempt(connection, attempt).await?;
    }
    Ok(())
}

/// Upserts the Episode, refusing to leave the Loop with two live Episodes
/// (§10: "one Loop has at most one active Episode").
async fn write_episode(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
    episode: &Episode,
) -> Result<(), SqliteStoreError> {
    if !episode.status.is_terminal() {
        let rows = sqlx::query(
            "SELECT episode_id, status FROM episodes WHERE loop_id = ?1 AND episode_id <> ?2",
        )
        .bind(loop_id.to_string())
        .bind(episode.id.to_string())
        .fetch_all(&mut *connection)
        .await?;

        for row in rows {
            let label = row.try_get::<&str, _>(1)?;
            let status = episode_status(loop_id, label)?;
            if !status.is_terminal() {
                let other = row.try_get::<&str, _>(0)?;
                return Err(SqliteStoreError::ActiveEpisodeExists {
                    loop_id,
                    episode_id: other.parse::<EpisodeId>().map_err(|_| {
                        SqliteStoreError::InconsistentRequest {
                            loop_id,
                            detail: format!("stored episode id {other} is malformed"),
                        }
                    })?,
                });
            }
        }
    }

    // Episode ordinals are the store's own numbering: `Episode` carries no
    // ordinal, but `episodes` is UNIQUE on `(loop_id, ordinal)`, so an
    // Episode keeps the ordinal it was first written with and a new one
    // takes the next.
    let ordinal = match sqlx::query("SELECT ordinal FROM episodes WHERE episode_id = ?1")
        .bind(episode.id.to_string())
        .fetch_optional(&mut *connection)
        .await?
    {
        Some(row) => row.try_get::<i64, _>(0)?,
        None => {
            sqlx::query("SELECT COALESCE(MAX(ordinal), 0) + 1 FROM episodes WHERE loop_id = ?1")
                .bind(loop_id.to_string())
                .fetch_one(&mut *connection)
                .await?
                .try_get::<i64, _>(0)?
        }
    };

    sqlx::query(
        "INSERT INTO episodes \
         (episode_id, loop_id, ordinal, status, base_checkpoint_version, \
          terminal_checkpoint_version, trace_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT(episode_id) DO UPDATE SET \
             status = excluded.status, \
             base_checkpoint_version = excluded.base_checkpoint_version, \
             terminal_checkpoint_version = excluded.terminal_checkpoint_version, \
             trace_id = excluded.trace_id",
    )
    .bind(episode.id.to_string())
    .bind(loop_id.to_string())
    .bind(ordinal)
    .bind(episode.status.label())
    .bind(sql_i64(
        "base checkpoint version",
        episode.base_checkpoint_version,
    )?)
    .bind(
        episode
            .terminal_checkpoint_version
            .map(|version| sql_i64("terminal checkpoint version", version))
            .transpose()?,
    )
    .bind(episode.trace_id.clone())
    .execute(&mut *connection)
    .await?;

    Ok(())
}

/// Upserts the Runtime Session generation, refusing to reopen one that has
/// already ended (§10: "closed Sessions cannot reopen").
async fn write_session(
    connection: &mut SqliteConnection,
    session: &RuntimeSession,
) -> Result<(), SqliteStoreError> {
    if let Some(row) = sqlx::query("SELECT status FROM runtime_sessions WHERE session_id = ?1")
        .bind(session.id.to_string())
        .fetch_optional(&mut *connection)
        .await?
    {
        let stored = session_status(session.loop_id, row.try_get::<&str, _>(0)?)?;
        if stored.is_terminal() && !session.status.is_terminal() {
            return Err(SqliteStoreError::ClosedSessionReopened {
                session_id: session.id,
                stored: stored.label(),
                requested: session.status.label(),
            });
        }
    }

    let runner_ref =
        serde_json::to_string(&session.runner).map_err(|source| SqliteStoreError::Encode {
            what: "runner reference",
            source,
        })?;

    sqlx::query(
        "INSERT INTO runtime_sessions \
         (session_id, episode_id, generation, status, runner_ref, boot_manifest_uri, \
          external_session_ref) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT(session_id) DO UPDATE SET \
             status = excluded.status, \
             runner_ref = excluded.runner_ref, \
             boot_manifest_uri = excluded.boot_manifest_uri, \
             external_session_ref = excluded.external_session_ref",
    )
    .bind(session.id.to_string())
    .bind(session.episode_id.to_string())
    .bind(i64::from(session.generation))
    .bind(session.status.label())
    .bind(runner_ref)
    .bind(session.boot_manifest_ref.uri.clone())
    .bind(
        session
            .external_session_ref
            .as_ref()
            .map(|reference| reference.0.clone()),
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

/// Upserts the current Work Unit.
async fn write_work_unit(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
    work_unit: &WorkUnit,
) -> Result<(), SqliteStoreError> {
    let work_unit_json =
        serde_json::to_string(work_unit).map_err(|source| SqliteStoreError::Encode {
            what: "work unit",
            source,
        })?;

    sqlx::query(
        "INSERT INTO work_units (work_unit_id, loop_id, status, schema_version, work_unit_json) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(work_unit_id) DO UPDATE SET \
             status = excluded.status, \
             schema_version = excluded.schema_version, \
             work_unit_json = excluded.work_unit_json",
    )
    .bind(work_unit.id.to_string())
    .bind(loop_id.to_string())
    .bind(work_unit_status_label(work_unit.status))
    .bind(WORK_UNIT_SCHEMA_VERSION)
    .bind(work_unit_json)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

/// Upserts the current Work Unit Attempt.
///
/// A retry never overwrites a prior Attempt — it has its own identity and
/// ordinal — so this only ever rewrites the observation fields of the
/// Attempt it names.
async fn write_attempt(
    connection: &mut SqliteConnection,
    attempt: &WorkUnitAttempt,
) -> Result<(), SqliteStoreError> {
    let capabilities = serde_json::to_string(&attempt.capabilities_snapshot).map_err(|source| {
        SqliteStoreError::Encode {
            what: "capability snapshot",
            source,
        }
    })?;

    sqlx::query(
        "INSERT INTO work_unit_attempts \
         (attempt_id, work_unit_id, episode_id, ordinal, status, idempotency_key, \
          capabilities_json, boot_package_uri, external_execution_ref, result_uri) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
         ON CONFLICT(attempt_id) DO UPDATE SET \
             status = excluded.status, \
             capabilities_json = excluded.capabilities_json, \
             external_execution_ref = excluded.external_execution_ref, \
             result_uri = excluded.result_uri",
    )
    .bind(attempt.id.to_string())
    .bind(attempt.work_unit_id.to_string())
    .bind(attempt.episode_id.to_string())
    .bind(i64::from(attempt.ordinal))
    .bind(attempt.status.label())
    .bind(attempt.idempotency_key.clone())
    .bind(capabilities)
    .bind(attempt.boot_package_ref.uri.clone())
    .bind(
        attempt
            .external_execution_ref
            .as_ref()
            .map(|reference| reference.0.clone()),
    )
    .bind(
        attempt
            .result_ref
            .as_ref()
            .map(|reference| reference.uri.clone()),
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

/// Records this Checkpoint's resource edges, rejecting any resource that is
/// not durably finalized (§10: "unfinalized resources cannot enter a
/// Checkpoint").
async fn write_resource_edges(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
    state: &AggregateState,
) -> Result<(), SqliteStoreError> {
    let checkpoint_version = sql_i64("checkpoint version", state.execution.checkpoint_version)?;

    for (uri, relation) in referenced_resources(state) {
        let finalized = sqlx::query("SELECT finalized FROM resources WHERE resource_uri = ?1")
            .bind(uri.clone())
            .fetch_optional(&mut *connection)
            .await?;
        match finalized {
            None => return Err(SqliteStoreError::UnknownResource { uri }),
            Some(row) if row.try_get::<i64, _>(0)? != 1 => {
                return Err(SqliteStoreError::ResourceNotFinalized { uri });
            }
            Some(_) => {}
        }

        sqlx::query(
            "INSERT INTO resource_edges (loop_id, checkpoint_version, resource_uri, relation) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(loop_id, checkpoint_version, resource_uri, relation) DO NOTHING",
        )
        .bind(loop_id.to_string())
        .bind(checkpoint_version)
        .bind(uri)
        .bind(relation)
        .execute(&mut *connection)
        .await?;
    }

    Ok(())
}

/// Every resource this projection binds the Checkpoint to, with the
/// relation it is bound by. Deduplicated, in a stable order.
fn referenced_resources(state: &AggregateState) -> Vec<(String, &'static str)> {
    let mut edges: Vec<(String, &'static str)> = Vec::new();

    if let Some(episode) = state.episode.as_ref()
        && let Some(manifest) = episode.hydrated_manifest.as_ref()
    {
        push_edge(&mut edges, &manifest.uri, "hydrated_manifest");
    }
    if let Some(session) = state.session.as_ref() {
        push_edge(&mut edges, &session.boot_manifest_ref.uri, "boot_manifest");
    }
    if let Some(work_unit) = state.work_unit.as_ref() {
        for input in &work_unit.inputs {
            push_edge(&mut edges, &input.uri, "input");
        }
    }
    if let Some(attempt) = state.attempt.as_ref() {
        push_edge(&mut edges, &attempt.boot_package_ref.uri, "boot_package");
        if let Some(result) = attempt.result_ref.as_ref() {
            push_edge(&mut edges, &result.uri, "result");
        }
    }
    edges
}

/// Adds one `(uri, relation)` edge unless it is already present.
fn push_edge(edges: &mut Vec<(String, &'static str)>, uri: &str, relation: &'static str) {
    if !edges
        .iter()
        .any(|(existing, kind)| existing == uri && *kind == relation)
    {
        edges.push((uri.to_owned(), relation));
    }
}

/// Parses a stored Episode status label back into the domain enumeration.
fn episode_status(loop_id: LoopId, label: &str) -> Result<EpisodeStatus, SqliteStoreError> {
    EpisodeStatus::ALL
        .iter()
        .copied()
        .find(|status| status.label() == label)
        .ok_or_else(|| SqliteStoreError::InconsistentRequest {
            loop_id,
            detail: format!("stored episode status {label} is not a known status"),
        })
}

/// Parses a stored Runtime Session status label back into the domain
/// enumeration.
fn session_status(loop_id: LoopId, label: &str) -> Result<SessionStatus, SqliteStoreError> {
    SessionStatus::ALL
        .iter()
        .copied()
        .find(|status| status.label() == label)
        .ok_or_else(|| SqliteStoreError::InconsistentRequest {
            loop_id,
            detail: format!("stored session status {label} is not a known status"),
        })
}
