//! The inbound `LoopService` protocol: the durable execution service
//! Loopward exposes to callers, per `docs/09-public-api.md`.
//!
//! `LoopService` does not conceal unbounded work: `advance` performs at most
//! one durable Episode transition, `run_until` requires explicit finite
//! limits, and every mutating command carries an `expected_revision` for
//! optimistic concurrency plus lease fencing at the implementation layer.
//! Long operations return an [`OperationHandle`] rather than blocking or
//! silently running an unbounded loop.

use serde::{Deserialize, Serialize};

use crate::error::LoopwardError;
use crate::event::{EventPage, EventQuery};
use crate::ids::{EpisodeId, LoopId, OperationId, Timestamp, WorkUnitId};
use crate::resource::{PhaseRef, ResourceRef, SchemaRef};
use crate::trace::TraceContext;
use crate::work_unit::WorkUnitKind;

/// An open, domain- or workflow-defined Loop kind (e.g. `domain.example`).
///
/// Loopward Core does not classify or inspect Loop kinds; this is not a
/// closed enumeration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LoopKind(pub String);

impl LoopKind {
    /// Wraps a raw Loop kind string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for LoopKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// The caller-supplied statement of what a Loop must accomplish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Goal {
    /// A natural-language statement of the Loop's objective.
    pub statement: String,
}

/// Whether a Loop's success criterion has been satisfied.
///
/// Phase One documentation (`docs/schemas/checkpoint-v0.1.yaml`) shows only
/// a `pending` example; this closed set is a conservative interpretation
/// pending refinement by the verification pipeline (`loopward-core`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuccessCriterionStatus {
    /// Not yet evaluated.
    Pending,
    /// Verified as satisfied.
    Satisfied,
    /// Verified and found unsatisfied.
    Failed,
}

/// One measurable condition a Loop must meet to be considered complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuccessCriterion {
    /// A stable identifier for this criterion, scoped to the Loop.
    pub id: String,
    /// A natural-language statement of the criterion.
    pub statement: String,
    /// Whether this criterion has been evaluated and satisfied.
    pub status: SuccessCriterionStatus,
}

/// Who or what originated a pinned constraint.
///
/// This closed set is a conservative interpretation: Phase One documentation
/// (`docs/schemas/checkpoint-v0.1.yaml`) shows only a `user` example.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintSource {
    /// Supplied directly by the requesting user or caller.
    User,
    /// Imposed by Loopward itself.
    System,
    /// Imposed by a deployment-level policy.
    Policy,
    /// Reported by a runner.
    Runner,
}

/// The scope over which a pinned constraint applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintScope {
    /// Applies for the lifetime of the Loop.
    Loop,
    /// Applies for the current Episode only.
    Episode,
    /// Applies for the current Work Unit only.
    WorkUnit,
}

/// A pinned constraint: an active user constraint, safety requirement, or
/// applicable invariant that must survive hydration and drain losslessly
/// (`docs/04-context-lifecycle.md` §2, §4.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Constraint {
    /// A stable identifier for this constraint, scoped to the Loop.
    pub id: String,
    /// A natural-language statement of the constraint.
    pub statement: String,
    /// Who or what originated this constraint.
    pub source: ConstraintSource,
    /// The scope over which this constraint applies.
    pub scope: ConstraintScope,
}

/// Loop-level operational policies.
///
/// Phase One does not yet enumerate a closed set of policy fields
/// (`docs/09-public-api.md` §5 names the field but not its shape). This is
/// an open, forward-compatible bag of named policy values so
/// `CreateLoopRequest` can carry policy configuration today without a
/// schema-breaking change once policies are specified.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LoopPolicies {
    /// Named policy values, open and forward-compatible.
    #[serde(flatten, default)]
    pub values: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Caller identity attached to a mutating command, when the deployment
/// enforces authentication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalRef {
    /// The principal's opaque identifier.
    pub id: String,
    /// A human-readable display name, when available.
    pub display_name: Option<String>,
}

/// Metadata attached to every `LoopService` command.
///
/// Mutating commands are idempotent within their documented scope:
/// `idempotency_key` allows safe retry, and `expected_revision` provides
/// optimistic concurrency control (`docs/09-public-api.md` §4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandMetadata {
    /// A caller-supplied identifier for this specific request, for
    /// correlation and diagnostics.
    pub request_id: String,
    /// A stable idempotency key allowing safe retry of this command.
    pub idempotency_key: String,
    /// The aggregate revision the caller expects the Loop to be at. Absent
    /// only when no prior revision could be known (e.g. `create`).
    pub expected_revision: Option<u64>,
    /// The identity of the caller issuing this command, when known.
    pub principal: Option<PrincipalRef>,
    /// The propagated trace context for this command.
    pub trace_context: TraceContext,
}

/// A request to create a new Loop.
///
/// Creation validates schemas and policies before committing the initial
/// Checkpoint (`docs/09-public-api.md` §5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateLoopRequest {
    /// Command metadata.
    pub metadata: CommandMetadata,
    /// The service protocol version this request was built against.
    pub protocol_version: String,
    /// An open, domain-defined kind classification for the new Loop.
    pub kind: LoopKind,
    /// The Loop's objective.
    pub goal: Goal,
    /// Measurable conditions the Loop must meet to be considered complete.
    #[serde(default)]
    pub success_criteria: Vec<SuccessCriterion>,
    /// Constraints pinned for the lifetime of the Loop (or a narrower
    /// declared scope) that must survive hydration and drain losslessly.
    #[serde(default)]
    pub pinned_constraints: Vec<Constraint>,
    /// The schema the Loop's domain state must satisfy.
    pub domain_schema: SchemaRef,
    /// The Loop's initial, schema-declared domain state.
    pub initial_domain_state: serde_json::Value,
    /// Operational policies for this Loop.
    pub policies: LoopPolicies,
}

/// A handle to a newly created Loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopHandle {
    /// The new Loop's durable identity.
    pub loop_id: LoopId,
    /// The aggregate revision produced by creation (the initial Checkpoint).
    pub aggregate_revision: u64,
}

/// A request to advance a Loop by at most one durable Episode transition to
/// its next safe boundary (`docs/09-public-api.md` §3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceLoopCommand {
    /// Command metadata, including the expected aggregate revision.
    pub metadata: CommandMetadata,
    /// The Loop to advance.
    pub loop_id: LoopId,
}

/// The condition under which a bounded `run_until` sequence of advances
/// should stop, independent of the finite resource limits in [`RunLimits`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunCondition {
    /// Stop once the Loop reaches any terminal outcome (completed, failed,
    /// or cancelled).
    UntilTerminal,
    /// Stop once the Loop reaches a terminal outcome or suspends, whichever
    /// comes first.
    UntilTerminalOrSuspended,
}

/// A bound on cumulative resource expenditure across a `run_until` sequence
/// of advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResourceCost {
    /// A bound on the number of Work Unit Attempts started.
    pub work_unit_attempts: Option<u32>,
    /// A bound on estimated tokens processed, when token accounting is
    /// available.
    pub estimated_tokens: Option<u64>,
}

/// Explicit, finite limits bounding a `run_until` sequence of advances.
///
/// At least one finite limit is required: an unbounded hidden execution
/// loop is not part of the public API (`docs/09-public-api.md` §3,
/// `docs/07-session-rotation.md` §14).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RunLimits {
    /// The maximum number of Episodes to advance through.
    pub max_episodes: Option<u32>,
    /// A wall-clock deadline after which no further advance is attempted.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub deadline: Option<Timestamp>,
    /// A bound on cumulative resource cost across the sequence.
    pub max_resource_cost: Option<ResourceCost>,
}

/// A request to repeatedly advance a Loop until `condition` is met or a
/// limit in `limits` is reached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunUntilCommand {
    /// Command metadata, including the expected aggregate revision.
    pub metadata: CommandMetadata,
    /// The Loop to advance.
    pub loop_id: LoopId,
    /// The condition under which the sequence should stop.
    pub condition: RunCondition,
    /// Explicit, finite limits bounding the sequence.
    pub limits: RunLimits,
}

/// A request to suspend an active Loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspendLoopCommand {
    /// Command metadata, including the expected aggregate revision.
    pub metadata: CommandMetadata,
    /// The Loop to suspend.
    pub loop_id: LoopId,
    /// A caller-supplied reason for the suspension, when given.
    pub reason: Option<String>,
}

/// A request to resume a suspended Loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeLoopCommand {
    /// Command metadata, including the expected aggregate revision.
    pub metadata: CommandMetadata,
    /// The Loop to resume.
    pub loop_id: LoopId,
}

/// A request to cancel a Loop. Cancellation is explicit and never implied by
/// a client disconnecting (`docs/09-public-api.md` §3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelLoopCommand {
    /// Command metadata, including the expected aggregate revision.
    pub metadata: CommandMetadata,
    /// The Loop to cancel.
    pub loop_id: LoopId,
    /// A caller-supplied reason for the cancellation, when given.
    pub reason: Option<String>,
}

/// An opaque handle to one asynchronous `LoopService` mutation.
///
/// `LoopService` does not expose a separate per-operation status call in
/// Phase One: callers observe the resulting state through
/// [`LoopService::status`] or [`LoopService::events`], correlating by
/// `loop_id` and, where recorded, `operation_id`. `accepted_at_revision`
/// records the aggregate revision the mutation was accepted against, tying
/// the handle to the optimistic-concurrency check performed on
/// `CommandMetadata::expected_revision`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationHandle {
    /// This operation's identity, for correlation with events and logs.
    pub operation_id: OperationId,
    /// The Loop this operation applies to.
    pub loop_id: LoopId,
    /// The aggregate revision the operation was accepted against.
    pub accepted_at_revision: u64,
}

/// A lightweight reference to the currently active Episode, for
/// [`LoopStatusView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeRef {
    /// The active Episode's identity.
    pub episode_id: EpisodeId,
    /// The Episode's ordinal position within the Loop.
    pub ordinal: u32,
}

/// A lightweight reference to the currently active Work Unit, for
/// [`LoopStatusView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkUnitRef {
    /// The active Work Unit's identity.
    pub work_unit_id: WorkUnitId,
    /// The active Work Unit's open, domain-defined kind.
    pub kind: WorkUnitKind,
}

/// A compact view of why and since when a Loop is currently suspended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspensionView {
    /// An open, domain- or policy-defined suspension reason.
    pub reason: String,
    /// When the suspension took effect.
    #[serde(with = "time::serde::rfc3339")]
    pub since: Timestamp,
    /// Additional human-readable detail, when available.
    pub detail: Option<String>,
}

/// The kind of terminal outcome a Loop reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopOutcomeKind {
    /// The Loop's success criteria were satisfied.
    Completed,
    /// The Loop failed and cannot proceed further.
    Failed,
    /// The Loop was cancelled.
    Cancelled,
}

/// A Loop's terminal outcome, once it has reached one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopOutcome {
    /// The kind of terminal outcome reached.
    pub kind: LoopOutcomeKind,
    /// A short human-readable summary, when available.
    pub summary: Option<String>,
}

/// The coarse-grained, publicly observable status of a Loop.
///
/// The full internal lifecycle state machine (Episode, Runtime Session, and
/// Work Unit Attempt states) is defined in `loopward-core` per
/// `docs/07-session-rotation.md`; this is the closed, wire-stable
/// projection callers observe through [`LoopStatusView`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopStatus {
    /// The Loop is actively progressing (or eligible to be advanced).
    Active,
    /// The Loop is suspended and requires resolution before it can advance.
    Suspended,
    /// The Loop reached a terminal outcome and cannot advance further.
    Terminal,
}

/// A compact, default status view for one Loop.
///
/// Intentionally does not return the complete Checkpoint, full event
/// history, or sensitive resource content (`docs/09-public-api.md` §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopStatusView {
    /// The Loop's durable identity.
    pub loop_id: LoopId,
    /// The Loop's coarse-grained public status.
    pub status: LoopStatus,
    /// The Loop's current, domain-defined phase.
    pub phase: PhaseRef,
    /// The current aggregate revision, for optimistic concurrency.
    pub aggregate_revision: u64,
    /// The version of the most recently committed Checkpoint.
    pub checkpoint_version: u64,
    /// The currently active Episode, when the Loop is active.
    pub active_episode: Option<EpisodeRef>,
    /// The currently active Work Unit, when one is in progress.
    pub active_work_unit: Option<WorkUnitRef>,
    /// Suspension detail, when the Loop is currently suspended.
    pub suspension: Option<SuspensionView>,
    /// The Loop's terminal outcome, once it has reached one.
    pub terminal_outcome: Option<LoopOutcome>,
}

/// The final, durable result of a completed Loop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoopResult {
    /// The Loop this result belongs to.
    pub loop_id: LoopId,
    /// The Loop's terminal outcome.
    pub outcome: LoopOutcome,
    /// The Loop's final, schema-declared domain state.
    pub final_domain_state: serde_json::Value,
    /// References to durable outputs produced over the Loop's lifetime.
    #[serde(default)]
    pub outputs: Vec<ResourceRef>,
}

/// Loopward's inbound durable execution service.
///
/// Exposes Loop lifecycle control as finite, observable operations: `advance`
/// performs at most one durable Episode transition; `run_until` requires
/// explicit limits; long operations return an [`OperationHandle`]; clients
/// observe status or events using cursors; cancellation is always explicit
/// (`docs/09-public-api.md` §3). It does not expose runner internals,
/// Runtime Session conversation history, or domain-specific state as
/// unversioned implementation details.
#[async_trait::async_trait]
pub trait LoopService: Send + Sync {
    /// Creates a new Loop, validating schemas and policies before committing
    /// the initial Checkpoint.
    async fn create(&self, request: CreateLoopRequest) -> Result<LoopHandle, LoopwardError>;

    /// Advances a Loop by at most one durable Episode transition to its next
    /// safe boundary.
    async fn advance(&self, command: AdvanceLoopCommand) -> Result<OperationHandle, LoopwardError>;

    /// Repeatedly advances a Loop until `condition` is met or a limit in
    /// `limits` is reached.
    async fn run_until(&self, command: RunUntilCommand) -> Result<OperationHandle, LoopwardError>;

    /// Returns a compact, default status view for a Loop.
    async fn status(&self, loop_id: LoopId) -> Result<LoopStatusView, LoopwardError>;

    /// Queries the durable or ephemeral event history for a Loop.
    async fn events(&self, query: EventQuery) -> Result<EventPage, LoopwardError>;

    /// Requests a Loop suspend at its next safe boundary.
    async fn suspend(&self, command: SuspendLoopCommand) -> Result<OperationHandle, LoopwardError>;

    /// Requests a suspended Loop resume.
    async fn resume(&self, command: ResumeLoopCommand) -> Result<OperationHandle, LoopwardError>;

    /// Requests a Loop be explicitly cancelled.
    async fn cancel(&self, command: CancelLoopCommand) -> Result<OperationHandle, LoopwardError>;

    /// Returns the final result of a Loop, once it has reached a terminal
    /// outcome, or `None` if it has not.
    async fn result(&self, loop_id: LoopId) -> Result<Option<LoopResult>, LoopwardError>;
}
