//! The `WorkUnitRunner` protocol: the outbound execution boundary through
//! which Loopward invokes an external executor, per
//! `docs/05-work-unit-and-runner-protocol.md`.
//!
//! A runner's result is a proposal, never an accepted outcome: `collect`
//! returns a [`crate::work_unit::WorkUnitResult`] that cannot itself commit
//! a Checkpoint, activate a decision, satisfy an acceptance criterion, or
//! declare the Loop complete. Loopward validates and verifies the result
//! before applying any mutation.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::event::EventCursor;
use crate::ids::{AttemptId, Timestamp};
use crate::resource::OpaqueRef;
use crate::trace::TraceRef;
use crate::work_unit::WorkUnitResult;

pub use crate::context::ContextCapabilities;

/// A runner's declared, versioned protocol and lifecycle capabilities.
///
/// Capabilities are snapshotted onto the Work Unit Attempt they were
/// observed for; a later runner upgrade must not rewrite that historical
/// record.
///
/// Each flag is an independently declared, orthogonal capability rather than
/// a state machine: `struct_excessive_bools` is intentionally allowed here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct RunnerCapabilities {
    /// The `WorkUnitRunner` protocol versions this runner accepts.
    pub protocol_versions: Vec<String>,
    /// Whether a suspended run can be resumed.
    pub resumable: bool,
    /// Whether `reconcile` is supported.
    pub supports_reconcile: bool,
    /// Whether `events` is supported.
    pub supports_events: bool,
    /// Whether `suspend` is supported.
    pub supports_suspend: bool,
    /// Whether `cancel` is supported.
    pub supports_cancel: bool,
    /// Whether the runner reports model token usage.
    pub reports_token_usage: bool,
    /// Whether the runner reports its own internal trace context.
    pub reports_internal_trace_context: bool,
    /// The runner's context-management capabilities.
    pub context: ContextCapabilities,
}

/// An opaque handle to one started runner execution.
///
/// Returned by `WorkUnitRunner::start` and passed back into every
/// subsequent call for the same attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunHandle {
    /// The Work Unit Attempt this run corresponds to.
    pub attempt_id: AttemptId,
    /// The runner-assigned external execution reference, when the runner
    /// returned one. Absent when a crash occurred between `start` returning
    /// and the reference being persisted (see
    /// `docs/05-work-unit-and-runner-protocol.md` §9).
    pub external_execution_ref: Option<OpaqueRef>,
}

/// The coarse-grained current state of a run, as reported by `status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    /// Accepted by the runner but not yet executing.
    Pending,
    /// Actively executing.
    Running,
    /// Finished executing; a result is ready to be collected.
    AwaitingCollection,
    /// Suspended and not currently executing.
    Suspended,
    /// Finished and collected.
    Completed,
    /// Finished unsuccessfully.
    Failed,
    /// Cancelled.
    Cancelled,
    /// The runner cannot currently determine its own state.
    Ambiguous,
}

/// A runner's current-status report, as of the moment it was polled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStatus {
    /// The coarse-grained current state.
    pub state: RunState,
    /// An optional human-readable detail (e.g. last observed progress).
    pub detail: Option<String>,
    /// When this status was observed, when the runner reports it.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub observed_at: Option<Timestamp>,
}

/// The outcome of reconciling a run after a restart, per
/// `docs/05-work-unit-and-runner-protocol.md` §10.
///
/// `Unknown` is not treated as failure: if repeating the external effect
/// could be unsafe and idempotency cannot be proven, the Attempt becomes
/// ambiguous and the Loop suspends for policy or human resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileOutcome {
    /// The runner never received this attempt.
    NotStarted,
    /// The runner is actively executing this attempt.
    Running,
    /// The runner has suspended this attempt.
    Suspended,
    /// The runner completed this attempt; a result is available.
    Completed,
    /// The runner failed this attempt.
    Failed,
    /// The runner cancelled this attempt.
    Cancelled,
    /// The runner cannot determine the attempt's state.
    Unknown,
}

/// One ephemeral, runner-reported progress event.
///
/// Ephemeral events are best-effort and may be sampled or unavailable after
/// execution; they are not part of the durable event journal (see
/// [`crate::event::LoopEvent`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerEvent {
    /// This event's cursor position within the runner's event stream.
    pub cursor: EventCursor,
    /// When the runner reports the event occurred, when known.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub occurred_at: Option<Timestamp>,
    /// An open, runner-defined event kind.
    pub kind: String,
    /// A short human-readable message, when supplied.
    pub message: Option<String>,
    /// Additional structured, runner-defined attributes.
    #[serde(default)]
    pub attributes: serde_json::Value,
}

/// One page of ephemeral runner events, with a cursor for continued
/// pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerEventPage {
    /// The events in this page, in the runner's reported order.
    pub events: Vec<RunnerEvent>,
    /// A cursor for fetching the next page, absent when exhausted.
    pub next_cursor: Option<EventCursor>,
}

/// Why Loopward is asking a runner to suspend an active run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuspensionReason {
    /// A Runtime Session rotation is in progress.
    Rotation,
    /// A context or resource budget was exceeded.
    BudgetExceeded,
    /// Verification requires human or policy input before continuing.
    VerificationPending,
    /// A policy decision is required before continuing.
    PolicyDecision,
    /// An operator explicitly requested suspension.
    OperatorRequest,
    /// Suspended as part of crash recovery.
    Recovery,
}

/// Why Loopward is asking a runner to cancel an active run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationReason {
    /// The owning Loop was cancelled.
    LoopCancelled,
    /// This attempt was superseded by remediation or retry.
    Superseded,
    /// The run violated policy and must stop.
    PolicyViolation,
    /// The run exceeded its allotted time.
    Timeout,
    /// An operator explicitly requested cancellation.
    OperatorRequest,
}

/// A stable, typed error code for `WorkUnitRunner` protocol failures, per
/// `docs/05-work-unit-and-runner-protocol.md` §12.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerErrorCode {
    /// The runner violated the wire protocol.
    ProtocolError,
    /// The requested operation is not supported by this runner.
    UnsupportedCapability,
    /// The runner could not authenticate the caller.
    AuthenticationError,
    /// The caller is not authorized for the requested operation.
    AuthorizationError,
    /// A referenced resource could not be resolved or is invalid.
    ResourceError,
    /// The runner is temporarily unavailable.
    Unavailable,
    /// The operation exceeded its allotted time.
    Timeout,
    /// The runner's execution failed.
    ExecutionFailed,
    /// The operation was cancelled.
    Cancelled,
    /// The external execution's state cannot be determined safely.
    AmbiguousExternalState,
    /// The runner returned a result that failed structural validation.
    InvalidResult,
}

impl fmt::Display for RunnerErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::ProtocolError => "protocol_error",
            Self::UnsupportedCapability => "unsupported_capability",
            Self::AuthenticationError => "authentication_error",
            Self::AuthorizationError => "authorization_error",
            Self::ResourceError => "resource_error",
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::ExecutionFailed => "execution_failed",
            Self::Cancelled => "cancelled",
            Self::AmbiguousExternalState => "ambiguous_external_state",
            Self::InvalidResult => "invalid_result",
        };
        f.write_str(label)
    }
}

/// A typed, sanitized `WorkUnitRunner` protocol error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct RunnerError {
    /// The stable error code.
    pub code: RunnerErrorCode,
    /// A sanitized, non-sensitive human-readable message.
    pub message: String,
    /// Whether retrying the operation may succeed.
    pub retryable: bool,
    /// A trace correlation reference, when available.
    pub trace_ref: Option<TraceRef>,
}

/// The public outbound execution boundary Loopward uses to invoke an
/// external executor for one Work Unit.
///
/// Implementations may be a coding agent, research service, command, human
/// process, experiment batch, or another framework; Loopward Core does not
/// classify or inspect the implementation behind this trait. Not every
/// runner supports every optional operation: unsupported behavior must be
/// declared truthfully in [`RunnerCapabilities`] and returns
/// [`RunnerErrorCode::UnsupportedCapability`].
#[async_trait::async_trait]
pub trait WorkUnitRunner: Send + Sync {
    /// Reports this runner's protocol and lifecycle capabilities.
    async fn capabilities(&self) -> Result<RunnerCapabilities, RunnerError>;

    /// Starts execution of a Work Unit and returns a handle for subsequent
    /// calls. The caller durably records intent before calling this method
    /// and durably persists the returned handle immediately after.
    async fn start(
        &self,
        request: crate::work_unit::WorkUnitRequest,
    ) -> Result<RunHandle, RunnerError>;

    /// Reconciles a run's state after a restart, without assuming the
    /// external effect failed merely because it was not observed.
    async fn reconcile(&self, handle: &RunHandle) -> Result<ReconcileOutcome, RunnerError>;

    /// Polls the run's current status.
    async fn status(&self, handle: &RunHandle) -> Result<RunStatus, RunnerError>;

    /// Fetches a page of ephemeral progress events, starting after `cursor`.
    async fn events(
        &self,
        handle: &RunHandle,
        cursor: Option<EventCursor>,
    ) -> Result<RunnerEventPage, RunnerError>;

    /// Requests the runner suspend the run. Cancellation and suspension are
    /// cooperative unless the runner guarantees stronger semantics.
    async fn suspend(
        &self,
        handle: &RunHandle,
        reason: SuspensionReason,
    ) -> Result<(), RunnerError>;

    /// Requests the runner cancel the run.
    async fn cancel(
        &self,
        handle: &RunHandle,
        reason: CancellationReason,
    ) -> Result<(), RunnerError>;

    /// Collects the runner's proposed result. The returned
    /// [`WorkUnitResult`] is a proposal boundary only; it cannot itself
    /// commit a Checkpoint, activate a decision, satisfy an acceptance
    /// criterion, or declare the Loop complete.
    async fn collect(&self, handle: &RunHandle) -> Result<WorkUnitResult, RunnerError>;
}
