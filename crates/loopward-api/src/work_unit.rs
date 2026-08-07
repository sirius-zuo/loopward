//! Work Unit DTOs: the durable-shaped description of one externally
//! observable unit of progress, and the request/result wire types exchanged
//! with a `WorkUnitRunner`, per
//! `docs/05-work-unit-and-runner-protocol.md` and
//! `docs/02-core-domain-model.md`.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::context::{
    BootPackageManifest, ContinuationProposal, DecisionProposal, ObservationProposal,
    OpenIssueProposal, ResourceGrant, StateAssertionProposal,
};
use crate::ids::{AttemptId, EpisodeId, LoopId, SessionId, Timestamp, WorkUnitId};
use crate::resource::{ConstraintRef, OutputContract, ResourceRef, RunnerRef, VerificationPlanRef};
use crate::runner::RunnerErrorCode;
use crate::trace::{TraceContext, TraceRef};

/// An open, domain- or workflow-defined Work Unit kind (e.g.
/// `domain.example`).
///
/// Loopward Core does not classify or inspect Work Unit kinds; this is not a
/// closed enumeration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkUnitKind(pub String);

impl WorkUnitKind {
    /// Wraps a raw Work Unit kind string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for WorkUnitKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The durable status of a Work Unit. Runner-internal status is not durable
/// and is represented separately by [`crate::runner::RunState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkUnitStatus {
    /// Not yet started.
    Pending,
    /// Currently executing.
    Active,
    /// Execution finished; awaiting verification of the produced result.
    AwaitingVerification,
    /// The result was verified and accepted.
    Accepted,
    /// The Work Unit failed.
    Failed,
    /// The Work Unit is suspended.
    Suspended,
    /// The Work Unit was cancelled.
    Cancelled,
}

/// The maximum number of Attempts permitted for a Work Unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AttemptBudget {
    /// The maximum number of Attempts, when bounded.
    pub maximum: Option<u32>,
}

/// The resource budget attached to a Work Unit or Work Unit Request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResourceBudget {
    /// A requested context/token budget, resolved later by the Context
    /// Manager.
    pub context: Option<crate::context::ContextBudgetRequest>,
    /// A bound on the number of Attempts.
    pub attempts: Option<AttemptBudget>,
}

/// Which runner error codes are safe to retry, and whether reconciliation
/// must run before a retry is attempted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RetryPolicy {
    /// Runner error codes considered safe to retry.
    #[serde(default)]
    pub retryable_codes: Vec<RunnerErrorCode>,
    /// Whether `reconcile` must be called and resolved before retrying.
    pub require_reconciliation_before_retry: bool,
}

/// One externally observable unit of progress: not necessarily one model
/// call, one agent run, or one external workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkUnit {
    /// This Work Unit's durable identity.
    pub id: WorkUnitId,
    /// An open, domain-defined kind classification.
    pub kind: WorkUnitKind,
    /// A natural-language statement of what this Work Unit must accomplish.
    pub objective: String,
    /// The Work Unit's durable status.
    pub status: WorkUnitStatus,
    /// Inputs available to the runner for this Work Unit.
    #[serde(default)]
    pub inputs: Vec<ResourceRef>,
    /// Constraints applicable to this Work Unit.
    #[serde(default)]
    pub constraints: Vec<ConstraintRef>,
    /// The schema the produced result must satisfy.
    pub output_contract: OutputContract,
    /// The versioned verification plan bound to this Work Unit.
    pub verification_plan: VerificationPlanRef,
    /// The runner that should execute this Work Unit.
    pub runner: RunnerRef,
    /// The resource budget for this Work Unit.
    pub budget: ResourceBudget,
    /// The retry policy for this Work Unit's Attempts.
    pub retry_policy: RetryPolicy,
}

/// The full request sent to a `WorkUnitRunner::start` call.
///
/// Carries references and grants, not unrestricted access to the Loop
/// store: the runner sees exactly what this request contains.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkUnitRequest {
    /// The Work Unit request/result protocol version.
    pub protocol_version: String,
    /// The owning Loop.
    pub loop_id: LoopId,
    /// The owning Episode.
    pub episode_id: EpisodeId,
    /// The Runtime Session executing this request.
    pub session_id: SessionId,
    /// The Runtime Session's generation.
    pub session_generation: u32,
    /// The Work Unit being executed.
    pub work_unit: WorkUnit,
    /// This execution Attempt's durable identity.
    pub attempt_id: AttemptId,
    /// A stable idempotency key for this Attempt, allowing safe retry of
    /// the external call.
    pub idempotency_key: String,
    /// The Boot Package manifest hydrated for this Attempt.
    pub boot_package: BootPackageManifest,
    /// Scoped grants permitting on-demand resource retrieval.
    #[serde(default)]
    pub resource_grants: Vec<ResourceGrant>,
    /// The schema the produced result must satisfy for this request.
    pub output_contract: OutputContract,
    /// The resource budget for this specific request.
    pub budget: ResourceBudget,
    /// A deadline by which the runner must complete, when bounded.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub deadline: Option<Timestamp>,
    /// The propagated trace context.
    pub trace_context: TraceContext,
}

/// What a runner itself can assert about the outcome of its own execution.
///
/// This intentionally excludes acceptance-pipeline states (validated,
/// verified, accepted, rejected, stale) from
/// `docs/06-verification-and-recovery.md` §2: those are Loopward's own
/// later determinations, never something a runner sets directly. A runner
/// result is a proposal, not an accepted outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkUnitOutcome {
    /// The runner produced a candidate result.
    Produced,
    /// The runner failed to produce a result.
    Failed,
    /// The runner produced a partial result (e.g. interrupted).
    Partial,
}

/// A report of token/resource usage, distinguishing known, estimated, and
/// unavailable values rather than fabricating a precise figure for an
/// unavailable vendor metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct UsageReport {
    /// Metrics known with certainty (e.g. directly reported by the runner).
    #[serde(default)]
    pub known: std::collections::BTreeMap<String, f64>,
    /// Metrics derived by estimation, with the estimation method left to
    /// accompanying context (e.g. an [`crate::context::EstimateMethod`]).
    #[serde(default)]
    pub estimated: std::collections::BTreeMap<String, f64>,
    /// Metric names that could not be determined at all.
    #[serde(default)]
    pub unavailable: Vec<String>,
}

/// The runner's proposed result for one Work Unit Attempt.
///
/// This is a proposal boundary: it cannot commit a Checkpoint, activate a
/// decision, mark an acceptance criterion satisfied, or declare the Loop
/// complete. Loopward validates and verifies the result before applying any
/// mutation (`docs/05-work-unit-and-runner-protocol.md` §7).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkUnitResult {
    /// The Work Unit request/result protocol version.
    pub protocol_version: String,
    /// The Attempt this result was produced for.
    pub attempt_id: AttemptId,
    /// What the runner itself asserts about its own execution outcome.
    pub outcome: WorkUnitOutcome,
    /// Finalized, digest-bound outputs produced by the runner.
    #[serde(default)]
    pub outputs: Vec<ResourceRef>,
    /// Proposed durable-state assertions. Not applied until validated.
    #[serde(default)]
    pub state_proposals: Vec<StateAssertionProposal>,
    /// Proposed observations. Not applied until validated.
    #[serde(default)]
    pub observation_proposals: Vec<ObservationProposal>,
    /// Proposed decisions. Not applied until validated.
    #[serde(default)]
    pub decision_proposals: Vec<DecisionProposal>,
    /// Evidence offered in support of verification.
    #[serde(default)]
    pub verification_evidence: Vec<ResourceRef>,
    /// Issues the runner reports as still open.
    #[serde(default)]
    pub unresolved: Vec<OpenIssueProposal>,
    /// The runner's recommended next action, when it has one.
    pub recommended_continuation: Option<ContinuationProposal>,
    /// Token/resource usage, distinguishing known, estimated, and
    /// unavailable values.
    #[serde(default)]
    pub usage: UsageReport,
    /// Opaque references to any trace or span the runner owns.
    #[serde(default)]
    pub runner_trace_refs: Vec<TraceRef>,
}
