//! The [`LoopStore`] port: durable persistence for one Loop's projection,
//! event journal, and lease (`docs/03-durable-state-and-transactions.md`).
//!
//! Every write is fenced by an expected `aggregate_revision` (or
//! `checkpoint_version`, for Checkpoint commits) and a lease
//! `fencing_token`, so a driver that lost its lease and resumed after a
//! pause can never commit (§7, §10). `loopward-runtime` depends only on this
//! trait; the concrete `SQLite`-backed implementation lives in a separate
//! adapter crate.
//!
//! [`LoopStore`] deliberately has no separate "create a Loop" method: Phase
//! One's only durable events are lifecycle facts `loopward-core` decided
//! (`loopward_core::LoopEvent` has no `LoopCreated` variant), and the
//! genesis Checkpoint is itself just a Checkpoint commit with no parent
//! (`docs/02-core-domain-model.md` §7: "absent only for the genesis
//! Checkpoint"). A real implementation MUST therefore treat `load` against
//! an unknown [`LoopId`] as [`StoreError::LoopNotFound`], and MUST accept a
//! [`LoopStore::commit_checkpoint`] against an unknown `loop_id` as genesis
//! creation exactly when both `expected_revision` and
//! `expected_checkpoint_version` are `0` — any other combination against an
//! unknown `loop_id` stays a [`StoreError::LoopNotFound`]. This is not just
//! asserted here: the fake `InMemoryLoopStore` in
//! `tests/port_contract.rs` implements exactly this rule, and
//! `commit_checkpoint_creates_a_fresh_loop_and_then_updates_it` drives a
//! brand-new [`LoopId`] through it with no other seam.

use std::fmt;
use std::str::FromStr;

use async_trait::async_trait;
use loopward_api::ids::{EventId, IdParseError, LoopId, Timestamp};
use loopward_core::{AggregateState, Checkpoint, LoopEvent};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The durable identity of one `loopward-runtime` process instance.
///
/// Backs [`LoopLease::holder_id`]: fencing needs to name *which* runtime
/// instance holds a lease, distinct from any Loop-domain identifier defined
/// in `loopward_api::ids`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeInstanceId(Uuid);

impl RuntimeInstanceId {
    /// Generates a new, time-ordered instance identity (`UUIDv7`).
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing [`Uuid`] as a runtime instance identity.
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying [`Uuid`].
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for RuntimeInstanceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RuntimeInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for RuntimeInstanceId {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self).map_err(|source| IdParseError {
            type_name: "RuntimeInstanceId",
            source,
        })
    }
}

/// The event journal schema version this runtime writes, matching
/// `docs/schemas/event-v0.1.yaml`'s `schema_version: loopward.event/v0.1`
/// and the same `loopward.<thing>/vX.Y` convention `loopward-api` already
/// uses for `PROTOCOL_VERSION` and `loopward_api::event::LoopEvent`'s own
/// `schema_version` field.
pub const EVENT_SCHEMA_VERSION: &str = "loopward.event/v0.1";

/// One durable event as recorded in the Loop's append-only journal
/// (`docs/03-durable-state-and-transactions.md` §6): the domain fact
/// [`loopward_core::decide`] produced, plus the journal metadata the
/// runtime assigns when it durably commits the event. `loopward-core`
/// deliberately leaves these fields out of [`LoopEvent`] itself, because
/// Core has no clock and does not assign storage positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedEvent {
    /// This event's durable identity.
    pub event_id: EventId,
    /// The event's position in the Loop's journal. Gap-free per committed
    /// transaction (§10).
    pub sequence: u64,
    /// The aggregate revision this event advanced the Loop to.
    pub aggregate_revision: u64,
    /// The event schema version this record conforms to. Unlike trace/span
    /// ids, §6 states schema version is recorded unconditionally
    /// ("Each event records Loop sequence, aggregate revision, schema
    /// version, timestamp, and current trace/span IDs *when available*") —
    /// see [`EVENT_SCHEMA_VERSION`].
    pub schema_version: String,
    /// When the runtime durably recorded this event.
    pub recorded_at: Timestamp,
    /// The domain fact.
    pub event: LoopEvent,
}

/// A Loop's durable projection and recorded journal, as returned by every
/// [`LoopStore`] operation that loads or mutates it.
///
/// Phase One stores "current projections + immutable Checkpoints +
/// append-only lifecycle Event Journal", not full event sourcing (§1):
/// `state` is that current projection, and `events` is the journal slice the
/// store chooses to return with this read or write (an in-memory store can
/// return everything it has; a real backend may page). Recovery replays
/// from `state.checkpoint` forward, not by re-folding `events` from
/// genesis (§1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLoop {
    /// The Loop's current durable projection.
    pub state: AggregateState,
    /// The recorded journal events returned with this read or write.
    pub events: Vec<RecordedEvent>,
}

/// A request to atomically commit an authoritative Checkpoint (§5).
///
/// Fenced the same way as [`LoopStore::append`] (`expected_revision`,
/// `fencing_token`), plus the Checkpoint version the caller last observed,
/// since Checkpoint commit is the only mutation that also advances
/// `checkpoint_version` (§2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCommitRequest {
    /// The Loop the Checkpoint commits against.
    pub loop_id: LoopId,
    /// The aggregate revision the caller last observed. The commit is
    /// rejected if the store's current revision does not match (§5, §10).
    pub expected_revision: u64,
    /// The checkpoint version the caller last observed. The commit is
    /// rejected if the store's current checkpoint version does not match
    /// (§5, §10).
    pub expected_checkpoint_version: u64,
    /// The lease fencing token the caller last acquired or renewed. A stale
    /// token cannot commit (§7, §10).
    pub fencing_token: u64,
    /// The immutable Checkpoint being committed.
    pub checkpoint: Checkpoint,
    /// The domain events this commit durably records: at minimum
    /// `checkpoint.committed`, plus any terminal Work Unit, Episode, or
    /// Session events the commit closes out (§5).
    pub events: Vec<LoopEvent>,
    /// The Loop's projection as it stands immediately after this commit.
    ///
    /// `loopward-runtime` computes this by folding `events` through
    /// [`loopward_core::evolve`] before calling
    /// [`LoopStore::commit_checkpoint`]; the store's job is to persist it
    /// atomically with everything else in this request, not to re-derive it
    /// (§5: "One `SQLite` transaction writes ... current Loop projection
    /// ..."). Partial commits are forbidden.
    pub state: AggregateState,
}

/// A request to acquire the exclusive driving lease for a Loop (§7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquireLease {
    /// The Loop to acquire a lease for.
    pub loop_id: LoopId,
    /// The requesting runtime instance.
    pub holder_id: RuntimeInstanceId,
    /// How long the lease should be valid for from acquisition.
    pub ttl: time::Duration,
}

/// The exclusive right to mutate one Loop (§7).
///
/// Reacquisition — including by a different `holder_id` after expiry, or by
/// the same holder after losing and regaining the lease — increments
/// `fencing_token`. Every [`LoopStore`] write validates it, so a driver that
/// resumes after losing its lease can never commit even if it is still
/// running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopLease {
    /// The leased Loop.
    pub loop_id: LoopId,
    /// The runtime instance currently holding the lease.
    pub holder_id: RuntimeInstanceId,
    /// Strictly increases on every acquisition; the token every write is
    /// fenced against.
    pub fencing_token: u64,
    /// When this lease was acquired or last renewed.
    pub acquired_at: Timestamp,
    /// When this lease expires absent renewal.
    pub expires_at: Timestamp,
}

/// An error returned by a [`LoopStore`] operation.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// No Loop exists with the given identity.
    #[error("no loop found for {0}")]
    LoopNotFound(LoopId),
    /// The caller's expected aggregate revision did not match the store's
    /// current revision (§2, §10: "aggregate revisions increase
    /// monotonically").
    #[error("aggregate revision conflict for {loop_id}: expected {expected}, found {actual}")]
    RevisionConflict {
        /// The Loop whose revision conflicted.
        loop_id: LoopId,
        /// The revision the caller expected.
        expected: u64,
        /// The store's current revision.
        actual: u64,
    },
    /// The caller's expected checkpoint version did not match the store's
    /// current version (§5, §10: "checkpoint versions increase
    /// monotonically").
    #[error("checkpoint version conflict for {loop_id}: expected {expected}, found {actual}")]
    CheckpointVersionConflict {
        /// The Loop whose checkpoint version conflicted.
        loop_id: LoopId,
        /// The checkpoint version the caller expected.
        expected: u64,
        /// The store's current checkpoint version.
        actual: u64,
    },
    /// The caller's fencing token is no longer current: a later acquisition
    /// has already superseded it, so this write is rejected (§7, §10:
    /// "stale fencing tokens cannot write").
    #[error(
        "stale fencing token for {loop_id}: {given} is no longer current (current is {current})"
    )]
    StaleFencingToken {
        /// The Loop the write targeted.
        loop_id: LoopId,
        /// The fencing token the caller presented.
        given: u64,
        /// The store's current fencing token.
        current: u64,
    },
    /// No lease is currently held for the given Loop.
    #[error("no active lease for {0}")]
    LeaseNotHeld(LoopId),
    /// A different runtime instance already holds this Loop's lease.
    ///
    /// Returned by [`LoopStore::acquire_lease`] when the requested
    /// `holder_id` differs from the current holder's; the same holder MAY
    /// re-acquire (e.g. after restarting with a stable identity), which
    /// still bumps `fencing_token` (§7: "reacquisition increments the
    /// fencing token").
    #[error("lease for {0} is held by another runtime instance")]
    LeaseConflict(LoopId),
    /// The underlying storage backend failed in a way this port does not
    /// otherwise model.
    #[error("storage backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Durable persistence for one Loop's projection, event journal, and lease.
///
/// `loopward-runtime` depends only on this trait: it never opens a `SQLite`
/// connection or a file itself. The concrete implementation lives in a
/// separate adapter crate that depends on this trait, never the reverse.
#[async_trait]
pub trait LoopStore: Send + Sync {
    /// Loads a Loop's current projection and journal.
    async fn load(&self, loop_id: LoopId) -> Result<StoredLoop, StoreError>;

    /// Durably appends `events` to a Loop, fenced by `expected_revision` and
    /// `fencing_token` (§2, §7, §10).
    async fn append(
        &self,
        loop_id: LoopId,
        expected_revision: u64,
        fencing_token: u64,
        events: Vec<LoopEvent>,
    ) -> Result<StoredLoop, StoreError>;

    /// Atomically commits an authoritative Checkpoint (§5): the immutable
    /// Checkpoint, the current Loop projection, and every terminal state
    /// change the commit implies, in one transaction. Partial commits are
    /// forbidden.
    async fn commit_checkpoint(
        &self,
        request: CheckpointCommitRequest,
    ) -> Result<StoredLoop, StoreError>;

    /// Acquires the exclusive driving lease for a Loop, incrementing the
    /// fencing token (§7). Rejected with [`StoreError::LeaseConflict`] if a
    /// different runtime instance currently holds an unexpired lease.
    async fn acquire_lease(&self, request: AcquireLease) -> Result<LoopLease, StoreError>;

    /// Renews an already-held lease, extending its expiry without changing
    /// its fencing token.
    async fn renew_lease(&self, lease: &LoopLease) -> Result<LoopLease, StoreError>;
}
