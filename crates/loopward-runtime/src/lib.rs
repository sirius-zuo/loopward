#![forbid(unsafe_code)]

//! Loopward's async coordination layer: drives the pure `loopward-core`
//! reducer against pluggable ports for storage, resources, the clock, ID
//! generation, and telemetry.
//!
//! This crate defines and depends on port *traits* only
//! (`docs/03-durable-state-and-transactions.md`). It never depends on a
//! concrete adapter: no `SQLx`, no filesystem calls, no network client. The
//! real `SQLite`- and filesystem-backed adapters live in separate crates
//! that depend on these traits, never the reverse; the controller that
//! actually drives an Episode forward is built on top of them separately.

pub mod context;
pub mod controller;
pub mod ports;
pub mod rotation;
pub mod verification;

pub use context::{ContextHydrator, HydrationError, HydrationReport, HydrationRequest};
pub use controller::{
    ContextCandidateProvider, ControllerError, LoopController, OperationOutcome,
    RuntimeLoopService, VerificationPlanResolver, VerificationPlanTemplate,
};
pub use ports::{
    AcquireLease, CheckpointCommitRequest, Clock, EVENT_SCHEMA_VERSION, IdGenerator, LoopLease,
    LoopStore, RecordedEvent, ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle,
    RuntimeInstanceId, RuntimePorts, StoreError, StoredLoop,
};
pub use rotation::{RotationContext, RotationDecision, RotationPolicy, RotationSignal};
pub use verification::{VerificationEngine, VerificationFinding};
