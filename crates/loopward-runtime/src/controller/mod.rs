//! The Loop Controller: the coordinator that drives one Episode at a time
//! through the pure `loopward-core` reducer, the runtime ports, context
//! hydration, a `WorkUnitRunner`, and verification
//! (`docs/07-session-rotation.md` §6, §14).
//!
//! [`advance::LoopController`] performs at most one durable Episode
//! transition per [`advance::LoopController::advance`] call; every external
//! effect (hydration, spawning the runner, verification's own resource
//! resolution) happens outside a database transaction, and every durable
//! mutation is fenced by a lease and an expected revision. [`service`]
//! adapts the controller to the public `LoopService` contract.

pub mod advance;
pub mod service;

pub use advance::{
    ContextCandidateProvider, ControllerError, LoopController, OperationOutcome,
    VerificationPlanResolver, VerificationPlanTemplate,
};
pub use service::RuntimeLoopService;
