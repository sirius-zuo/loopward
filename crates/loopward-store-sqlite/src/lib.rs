#![forbid(unsafe_code)]

//! The `SQLite` and filesystem adapters behind `loopward-runtime`'s
//! [`LoopStore`](loopward_runtime::ports::LoopStore) and
//! [`ResourceBroker`](loopward_runtime::ports::ResourceBroker) ports
//! (`docs/03-durable-state-and-transactions.md`).
//!
//! This crate is where the guarantees the ports only *specify* become real:
//!
//! * [`SqliteLoopStore`] persists one Loop's current projection, its
//!   immutable Checkpoints, and its append-only event journal, with every
//!   revision, Checkpoint-version, and lease fencing check performed inside
//!   the same `BEGIN IMMEDIATE` transaction that performs the write — never
//!   as a separate check-then-act. Partial commits are impossible: a failed
//!   transaction rolls back to no trace at all (§5, §9, §10).
//! * [`FileResourceBroker`] keeps large resource content out of `SQLite`,
//!   content-addressed by `SHA-256` under a configured resource root, and
//!   only ever publishes content through an atomic rename (§3, §10).
//!
//! The dependency direction is inward: this crate implements the port
//! traits `loopward-runtime` defines over the domain types `loopward-core`
//! and `loopward-api` define. Nothing here is redefined, and nothing here
//! is depended on by the runtime.
//!
//! # Storage model
//!
//! Lifecycle query fields use typed columns; domain state and versioned
//! event payloads are stored as JSON (§3). Concretely:
//!
//! * `loops.projection_json` is the serialized
//!   [`AggregateState`](loopward_core::AggregateState) and is authoritative
//!   for reads; `loops.status`, `loops.aggregate_revision`, and
//!   `loops.checkpoint_version` are query columns the same transaction
//!   keeps in lockstep with it.
//! * `checkpoints.checkpoint_json` is the serialized
//!   [`Checkpoint`](loopward_core::Checkpoint), and `checkpoints.digest` is
//!   `SHA-256` over exactly those stored bytes (§5).
//! * `loop_events.payload_json` is the serialized domain fact. Because
//!   [`loopward_core::LoopEvent`] is deliberately a pure domain type with no
//!   `serde` derives, this crate owns its own durable encoding of it rather
//!   than pushing a storage concern back into the domain.
//!
//! # Genesis
//!
//! [`LoopStore`](loopward_runtime::ports::LoopStore) has no "create a Loop"
//! method. [`SqliteLoopStore::load`] against an unknown Loop is
//! `LoopNotFound`, and [`SqliteLoopStore::commit_checkpoint`] against an
//! unknown `loop_id` is accepted as genesis creation exactly when both
//! `expected_revision` and `expected_checkpoint_version` are `0`; any other
//! expectation against an unknown `loop_id` stays `LoopNotFound`.

mod checkpoint;
mod error;
mod event;
mod lease;
mod resource;
mod store;

pub use error::SqliteStoreError;
pub use resource::FileResourceBroker;
pub use store::{SqliteLoopStore, SqliteStoreConfig, Synchronous};

/// The Checkpoint schema version this adapter records against every stored
/// Checkpoint, matching `docs/schemas/checkpoint-v0.1.yaml`.
///
/// Pinned to `loopward_core::Checkpoint`'s exact field set by
/// `tests/schema_version_pinning.rs`'s
/// `checkpoint_schema_version_matches_the_checkpoint_shape_it_names` — bump
/// both together.
pub const CHECKPOINT_SCHEMA_VERSION: &str = "loopward.checkpoint/v0.1";

/// The Work Unit schema version this adapter records against every stored
/// Work Unit, matching `docs/schemas/work-unit-v0.1.yaml`.
///
/// Pinned to `loopward_api::work_unit::WorkUnit`'s exact field set by
/// `tests/schema_version_pinning.rs`'s
/// `work_unit_schema_version_matches_the_work_unit_shape_it_names` — bump
/// both together.
pub const WORK_UNIT_SCHEMA_VERSION: &str = "loopward.work-unit/v0.1";

/// The embedded, file-based schema migrations for this adapter.
///
/// Embedded at compile time by [`sqlx::migrate!`], so building this crate
/// never requires a live database; [`SqliteLoopStore::open`] runs them, and
/// callers that manage their own pool can run them with [`run_migrations`].
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Applies every pending schema migration to `pool`.
///
/// [`SqliteLoopStore::open`] does this for the pool it creates; this is the
/// seam for a caller that opened its own [`sqlx::SqlitePool`] and shares it
/// with, for example, a [`FileResourceBroker`].
///
/// # Errors
///
/// Returns [`SqliteStoreError::Migrate`] if a migration cannot be applied.
pub async fn run_migrations(pool: &sqlx::SqlitePool) -> Result<(), SqliteStoreError> {
    MIGRATOR.run(pool).await.map_err(SqliteStoreError::Migrate)
}
