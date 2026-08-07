//! Stable diagnostic event names (`docs/08-tracing-and-observability.md` §9).
//!
//! These are trace-attached diagnostic copies only. The durable event
//! journal (`docs/03-durable-state-and-transactions.md` §6) remains
//! authoritative for anything recovery depends on; nothing in this crate
//! reads these names back out of a trace.

/// Stable, dotted event names significant enough to attach to a span (or
/// record durably alongside the journal when a caller chooses to).
pub mod event_name {
    /// A Session Rotation was requested.
    pub const ROTATION_REQUESTED: &str = "rotation.requested";
    /// A runner reported that it performed its own internal compaction.
    pub const RUNNER_COMPACTION_REPORTED: &str = "runner.compaction.reported";
    /// A structured handoff was received from a runner.
    pub const HANDOFF_RECEIVED: &str = "handoff.received";
    /// Verification produced one finding.
    pub const VERIFICATION_FINDING: &str = "verification.finding";
    /// A Checkpoint was committed.
    pub const CHECKPOINT_COMMITTED: &str = "checkpoint.committed";
    /// Execution was suspended.
    pub const EXECUTION_SUSPENDED: &str = "execution.suspended";
    /// Cancellation was requested.
    pub const EXECUTION_CANCEL_REQUESTED: &str = "execution.cancel.requested";
    /// Recovery detected an ambiguous external effect it could not safely
    /// resolve.
    pub const RECOVERY_AMBIGUITY_DETECTED: &str = "recovery.ambiguity_detected";
}
