//! Runtime ports: the trait boundaries `loopward-runtime` drives the pure
//! `loopward-core` reducer against, plus the [`RuntimePorts`] bundle that
//! composes one concrete implementation of each for a running driver.
//!
//! This module defines contracts only. `loopward-runtime` depends on these
//! traits and nothing else outside `loopward-core`, `loopward-api`, and
//! `loopward-observability`: no `SQLx`, no filesystem, no network client.
//! Concrete adapters — a real `SQLite`-backed [`LoopStore`], a real
//! filesystem- or object-storage-backed [`ResourceBroker`] — live in
//! separate crates that depend on these traits, never the reverse.

pub mod clock;
pub mod id_generator;
pub mod resource;
pub mod store;

use std::sync::Arc;

pub use clock::Clock;
pub use id_generator::IdGenerator;
pub use resource::{ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle};
pub use store::{
    AcquireLease, CheckpointCommitRequest, EVENT_SCHEMA_VERSION, LoopLease, LoopStore,
    RecordedEvent, RuntimeInstanceId, StoreError, StoredLoop,
};

/// One concrete set of ports a running driver uses to advance Loops.
///
/// Every field is a trait object behind an [`Arc`] so the same
/// `RuntimePorts` can be cloned cheaply and shared across concurrently
/// driven Loops, and so tests can substitute deterministic fakes for every
/// port without touching the driver logic that consumes them.
#[derive(Clone)]
pub struct RuntimePorts {
    /// Durable persistence for Loop projections, journals, and leases.
    pub store: Arc<dyn LoopStore>,
    /// The boundary for large resource content.
    pub resources: Arc<dyn ResourceBroker>,
    /// The runtime's source of wall-clock time.
    pub clock: Arc<dyn Clock>,
    /// The runtime's source of new durable identifiers.
    pub ids: Arc<dyn IdGenerator>,
    /// The observability-owned telemetry sink; disabled and no-op by
    /// default (`docs/08-tracing-and-observability.md` §2, §3).
    pub telemetry: Arc<dyn loopward_observability::TelemetrySink>,
}
