//! The durable domain model: the entities Loopward Core reasons about and
//! the [`AggregateState`] the reducer folds commands over.
//!
//! Shapes follow `docs/02-core-domain-model.md`; the status enumerations
//! follow the state machines in `docs/07-session-rotation.md` §3–§5. Nothing
//! here performs I/O, reads a clock, or knows about orchestration, DAGs,
//! workflow topology, or an agent registry.
//!
//! Two conventions are worth stating once:
//!
//! * **Current projection, not an event log.** `AggregateState` holds the
//!   *current* Episode, Runtime Session, Work Unit, and Attempt. A terminal
//!   entity stays in the projection until a successor replaces it, so
//!   "no active Episode" is expressed by
//!   [`LoopExecution::active_episode`] being `None`, not by the `episode`
//!   field being `None` (`docs/03-durable-state-and-transactions.md` §1).
//! * **Durable boundaries only.** The state machines contain a few states
//!   that Phase One's command surface never durably observes (a Runtime
//!   Session's `Created`/`Starting`, an Attempt's `Planned`/`Starting`).
//!   They are the in-flight window between the intent transaction and the
//!   observation transaction (`docs/03` §4). They remain representable so
//!   recovery can reconstruct them, and the transition tables still reject
//!   every undocumented edge out of them.

use loopward_api::ids::{AttemptId, CheckpointId, EpisodeId, LoopId, SessionId, WorkUnitId};
use loopward_api::resource::{OpaqueRef, PhaseRef, ResourceRef, RunnerRef};
use loopward_api::runner::RunnerCapabilities;
use loopward_api::work_unit::{ResourceBudget, WorkUnit, WorkUnitStatus};
use serde::{Deserialize, Serialize};

pub use loopward_api::context::ContextBudget;
pub use loopward_api::loop_service::LoopKind;

/// The lifecycle status of a whole Loop.
///
/// **Derived, not quoted.** `docs/02-core-domain-model.md` §2 names
/// `LoopStatus` on [`LoopExecution`] but never enumerates it, and
/// `docs/07-session-rotation.md` only tabulates the Episode, Runtime
/// Session, and Attempt machines. These variants are derived from the
/// invariants that *are* stated — "terminal Loops cannot advance", "Session
/// closure does not terminate the Loop" — from the Loop-level commands the
/// reducer accepts ([`crate::LifecycleCommand::Suspend`], `Complete`,
/// `Fail`), from `kernel.status: active` in
/// `docs/schemas/checkpoint-v0.1.yaml`, and from the terminal outcomes the
/// public API must be able to project
/// (`loopward_api::loop_service::LoopOutcomeKind`).
///
/// The documented Loop transition table is therefore:
///
/// ```text
/// Active    --> Suspended     (Suspend)
/// Active    --> Completed     (Complete)
/// Active    --> Failed        (Fail)
/// Suspended --> Failed        (Fail)
/// Completed | Failed | Cancelled --> [*]
/// ```
///
/// Two edges are deliberately absent from Phase One's *command* surface
/// rather than from the model: `Suspended --> Active` (resumption) and
/// `Active --> Cancelled` (explicit cancellation). Both are exposed by
/// `LoopService` (`docs/09-public-api.md` §3) and will need their own
/// lifecycle commands; [`LoopStatus::Cancelled`] exists so a cancelled Loop
/// is representable and covered by the terminal-state invariants in the
/// meantime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopStatus {
    /// The Loop is durable and eligible to advance.
    Active,
    /// The Loop is suspended and requires resolution before it can advance.
    Suspended,
    /// The Loop's success criteria were satisfied. Terminal.
    Completed,
    /// The Loop failed and cannot proceed. Terminal.
    Failed,
    /// The Loop was explicitly cancelled. Terminal.
    Cancelled,
}

impl LoopStatus {
    /// Every variant, for exhaustive transition-table checks.
    pub const ALL: &'static [Self] = &[
        Self::Active,
        Self::Suspended,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
    ];

    /// The Loop transition table derived above.
    ///
    /// `Suspended -> Active` (resumption) and `Active -> Cancelled`
    /// (explicit cancellation) are absent because no Phase One lifecycle
    /// command produces them; they join this table when their commands do.
    pub const DOCUMENTED_EDGES: &'static [(Self, Self)] = &[
        (Self::Active, Self::Suspended),
        (Self::Active, Self::Completed),
        (Self::Active, Self::Failed),
        (Self::Suspended, Self::Failed),
    ];

    /// Whether the Loop has reached a terminal outcome and cannot advance.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// The durable identity of one long-running task
/// (`docs/02-core-domain-model.md` §2).
///
/// A Loop outlives any single Runtime Session: closing a Session does not
/// terminate it, and its continuation is determined from durable state
/// alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopExecution {
    /// The Loop's durable identity.
    pub id: LoopId,
    /// An open, domain-defined kind classification.
    pub kind: LoopKind,
    /// The Loop's lifecycle status.
    pub status: LoopStatus,
    /// The Loop's current, domain-defined phase.
    pub phase: PhaseRef,
    /// Increments on every durable lifecycle transition; the optimistic
    /// concurrency token every write is fenced against
    /// (`docs/03-durable-state-and-transactions.md` §2).
    pub aggregate_revision: u64,
    /// Increments only on authoritative Checkpoint commit.
    pub checkpoint_version: u64,
    /// The active Episode, of which there is at most one in Phase One.
    pub active_episode: Option<EpisodeId>,
    /// The active Work Unit, of which there is at most one in Phase One.
    pub active_work_unit: Option<WorkUnitId>,
    /// The number of Episodes this Loop has ever started. Increments in
    /// [`evolve`] on every `episode.started` event
    /// (`docs/07-session-rotation.md` §3), giving
    /// [`AggregateState::active_episode`] a durable ordinal instead of only
    /// an identity — the public API's `EpisodeRef::ordinal` is projected
    /// from this.
    pub episode_count: u32,
    /// The Loop's resource budget.
    pub budget: ResourceBudget,
}

/// What an Episode is being run for.
///
/// Phase One's vocabulary, drawn from `docs/07-session-rotation.md` §8
/// (a fresh Session per Work Unit and a fresh Session for verification) and
/// `docs/06-verification-and-recovery.md` §10 and §12 (remediation and
/// recovery each create their own Episode). Only [`Self::Execution`] and
/// [`Self::Verification`] are derivable from the current command surface;
/// the other two are set by the runtime when it starts a remediation or
/// recovery Episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeKind {
    /// Advancing the Loop by executing Work Units.
    Execution,
    /// Verifying a produced result in a context assembled for review.
    Verification,
    /// Remediating a rejected or inconclusive result.
    Remediation,
    /// Recovering after a crash, ambiguity, or runner failure.
    Recovery,
}

/// The Episode state machine from `docs/07-session-rotation.md` §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeStatus {
    /// Created; no Boot Package has been hydrated yet.
    Preparing,
    /// Hydrating a Boot Package from the base Checkpoint.
    Hydrating,
    /// A Runtime Session is executing work for this Episode.
    Executing,
    /// Admission has stopped; the runner is being drained.
    Draining,
    /// The drained handoff is being validated and committed.
    Committing,
    /// Recovering from a runner, session, or ambiguity failure.
    Recovering,
    /// Completed normally. Terminal.
    Completed,
    /// Suspended awaiting resolution. Terminal for this Episode.
    Suspended,
    /// Aborted. Terminal.
    Aborted,
}

impl EpisodeStatus {
    /// Every variant, for exhaustive transition-table checks.
    pub const ALL: &'static [Self] = &[
        Self::Preparing,
        Self::Hydrating,
        Self::Executing,
        Self::Draining,
        Self::Committing,
        Self::Recovering,
        Self::Completed,
        Self::Suspended,
        Self::Aborted,
    ];

    /// Every transition documented by `docs/07-session-rotation.md` §3.
    pub const DOCUMENTED_EDGES: &'static [(Self, Self)] = &[
        (Self::Preparing, Self::Hydrating),
        (Self::Hydrating, Self::Executing),
        (Self::Executing, Self::Draining),
        (Self::Draining, Self::Committing),
        (Self::Committing, Self::Completed),
        (Self::Preparing, Self::Suspended),
        (Self::Hydrating, Self::Suspended),
        (Self::Executing, Self::Suspended),
        (Self::Draining, Self::Suspended),
        (Self::Committing, Self::Suspended),
        (Self::Executing, Self::Recovering),
        (Self::Draining, Self::Recovering),
        (Self::Committing, Self::Recovering),
        (Self::Recovering, Self::Hydrating),
        (Self::Recovering, Self::Suspended),
        (Self::Recovering, Self::Aborted),
        (Self::Preparing, Self::Aborted),
        (Self::Hydrating, Self::Aborted),
        (Self::Executing, Self::Aborted),
        (Self::Draining, Self::Aborted),
        (Self::Committing, Self::Aborted),
    ];

    /// Whether this Episode can no longer transition.
    ///
    /// Every terminal Episode state produces a committed Checkpoint or
    /// references an already committed terminal Checkpoint.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Suspended | Self::Aborted)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Hydrating => "hydrating",
            Self::Executing => "executing",
            Self::Draining => "draining",
            Self::Committing => "committing",
            Self::Recovering => "recovering",
            Self::Completed => "completed",
            Self::Suspended => "suspended",
            Self::Aborted => "aborted",
        }
    }
}

/// One attempt to advance from a committed Checkpoint to the next durable
/// semantic state (`docs/02-core-domain-model.md` §3).
///
/// An Episode is also the `OpenTelemetry` trace boundary: a long Loop is a
/// sequence of trace-linked Episodes, not one indefinitely open trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Episode {
    /// This Episode's durable identity.
    pub id: EpisodeId,
    /// The owning Loop.
    pub loop_id: LoopId,
    /// What this Episode is being run for.
    pub kind: EpisodeKind,
    /// The Episode's lifecycle status.
    pub status: EpisodeStatus,
    /// The Checkpoint version this Episode was hydrated from.
    pub base_checkpoint_version: u64,
    /// The Checkpoint version this Episode terminated at, once committed.
    pub terminal_checkpoint_version: Option<u64>,
    /// The Runtime Session currently executing this Episode, if any.
    pub active_session: Option<SessionId>,
    /// The Boot Package manifest hydrated for this Episode.
    ///
    /// Not in the `docs/02` §3 sketch: the Episode has to carry the manifest
    /// recorded by `context.hydrated` so the next Runtime Session generation
    /// can be started from it (`docs/07-session-rotation.md` §6).
    pub hydrated_manifest: Option<ResourceRef>,
    /// The W3C trace id of this Episode's top-level trace, when tracing is
    /// active.
    pub trace_id: Option<String>,
}

/// The Runtime Session state machine from `docs/07-session-rotation.md` §4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    /// Created but not yet starting.
    Created,
    /// Starting up.
    Starting,
    /// Executing work.
    Running,
    /// No longer admitting work; draining the runner and collecting handoff.
    Draining,
    /// Closed cleanly. Terminal; never reopened.
    Closed,
    /// Failed. Terminal; never reopened.
    Failed,
}

impl SessionStatus {
    /// Every variant, for exhaustive transition-table checks.
    pub const ALL: &'static [Self] = &[
        Self::Created,
        Self::Starting,
        Self::Running,
        Self::Draining,
        Self::Closed,
        Self::Failed,
    ];

    /// Every transition documented by `docs/07-session-rotation.md` §4.
    pub const DOCUMENTED_EDGES: &'static [(Self, Self)] = &[
        (Self::Created, Self::Starting),
        (Self::Starting, Self::Running),
        (Self::Running, Self::Draining),
        (Self::Draining, Self::Closed),
        (Self::Starting, Self::Failed),
        (Self::Running, Self::Failed),
        (Self::Draining, Self::Failed),
    ];

    /// Whether this Session generation has ended. A closed or failed
    /// generation is never reopened; continuation creates the next one.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Failed)
    }

    /// Whether this generation holds live execution resources.
    ///
    /// [`Self::Created`] deliberately does not: nothing has been started,
    /// no runner exists, and there is nothing to drain or close. That
    /// matters because a terminal Loop must have no *live* children, and
    /// the documented Session machine draws no edge out of `Created` other
    /// than `Starting`.
    #[must_use]
    pub fn is_live(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Draining)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }
}

/// A bounded, replaceable execution environment
/// (`docs/02-core-domain-model.md` §4).
///
/// A Session is disposable by construction: it is bounded by a context
/// budget, identified by a monotonically increasing generation within its
/// Episode, and never reopened once closed or failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSession {
    /// This Session generation's durable identity.
    pub id: SessionId,
    /// The owning Loop.
    pub loop_id: LoopId,
    /// The owning Episode.
    pub episode_id: EpisodeId,
    /// This generation's ordinal within the Loop. Strictly increasing.
    pub generation: u32,
    /// The Session's lifecycle status.
    pub status: SessionStatus,
    /// The runner backing this Session.
    pub runner: RunnerRef,
    /// The resolved context budget, once the Context Manager has resolved
    /// one for this generation.
    pub context_budget: Option<ContextBudget>,
    /// The Boot Package manifest this Session was hydrated from.
    pub boot_manifest_ref: ResourceRef,
    /// The runner-assigned external session reference, when the runner
    /// issued one.
    pub external_session_ref: Option<OpaqueRef>,
    /// The recorded rotation intent, once rotation has been requested.
    ///
    /// Phase One's in-projection form of the `RotationRecord` in
    /// `docs/07-session-rotation.md` §9: rotation records intent *before*
    /// admission stops, so a crash mid-rotation is recoverable.
    pub rotation_intent: Option<RotationReason>,
}

/// The Work Unit Attempt state machine from `docs/07-session-rotation.md`
/// §5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    /// Planned but not yet durably prepared.
    Planned,
    /// Intent persisted with a stable idempotency key; not yet started.
    Prepared,
    /// The external call is in flight.
    Starting,
    /// The runner is executing.
    Running,
    /// The runner finished; the result is being collected.
    Collecting,
    /// A result was collected. Terminal for the Attempt — and *not* Work
    /// Unit acceptance.
    Succeeded,
    /// The Attempt failed. Terminal.
    Failed,
    /// The Attempt was cancelled. Terminal.
    Cancelled,
    /// The external effect's state cannot be determined safely. Terminal.
    Ambiguous,
    /// The Attempt is suspended and not currently executing.
    Suspended,
}

impl AttemptStatus {
    /// Every variant, for exhaustive transition-table checks.
    pub const ALL: &'static [Self] = &[
        Self::Planned,
        Self::Prepared,
        Self::Starting,
        Self::Running,
        Self::Collecting,
        Self::Succeeded,
        Self::Failed,
        Self::Cancelled,
        Self::Ambiguous,
        Self::Suspended,
    ];

    /// Every transition documented by `docs/07-session-rotation.md` §5.
    pub const DOCUMENTED_EDGES: &'static [(Self, Self)] = &[
        (Self::Planned, Self::Prepared),
        (Self::Prepared, Self::Starting),
        (Self::Starting, Self::Running),
        (Self::Running, Self::Collecting),
        (Self::Collecting, Self::Succeeded),
        (Self::Starting, Self::Failed),
        (Self::Running, Self::Failed),
        (Self::Collecting, Self::Failed),
        (Self::Starting, Self::Ambiguous),
        (Self::Running, Self::Ambiguous),
        (Self::Planned, Self::Cancelled),
        (Self::Prepared, Self::Cancelled),
        (Self::Starting, Self::Cancelled),
        (Self::Running, Self::Cancelled),
        (Self::Running, Self::Suspended),
        (Self::Suspended, Self::Running),
    ];

    /// Whether this Attempt has finished. Success means a result was
    /// collected, never that the Work Unit was accepted.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Ambiguous
        )
    }

    /// Whether an Attempt in this state must carry the runner's external
    /// execution reference.
    ///
    /// `Ambiguous` deliberately does not: it is exactly the state produced
    /// when a crash occurs after the external call but before the reference
    /// was persisted (`docs/03-durable-state-and-transactions.md` §9).
    #[must_use]
    pub fn requires_external_execution_ref(self) -> bool {
        matches!(self, Self::Running | Self::Collecting | Self::Succeeded)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Prepared => "prepared",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Collecting => "collecting",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Ambiguous => "ambiguous",
            Self::Suspended => "suspended",
        }
    }
}

/// Whether repeating an Attempt's external effect is safe.
///
/// Named but not enumerated by `docs/02-core-domain-model.md` §6; these
/// variants are the distinctions `docs/03-durable-state-and-transactions.md`
/// §4 requires: an unsafe unknown side effect becomes
/// [`AttemptStatus::Ambiguous`] and suspends the Loop rather than being
/// blindly repeated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectClass {
    /// No externally observable side effect; safe to repeat.
    Pure,
    /// Externally observable, but idempotent under the Attempt's
    /// idempotency key.
    Idempotent,
    /// Externally observable and unsafe to repeat without proof the first
    /// effect did not occur.
    NonRepeatable,
}

/// One durable execution attempt for a Work Unit
/// (`docs/02-core-domain-model.md` §6).
///
/// A retry never overwrites a prior Attempt; it creates a new one with its
/// own identity, ordinal, and idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkUnitAttempt {
    /// This Attempt's durable identity.
    pub id: AttemptId,
    /// The Work Unit being attempted.
    pub work_unit_id: WorkUnitId,
    /// The Episode this Attempt belongs to.
    pub episode_id: EpisodeId,
    /// The Runtime Session generation that issued this Attempt's external
    /// call.
    ///
    /// Not in the `docs/02` §6 sketch, but `WorkUnitRequest` already carries
    /// it on the wire: an Attempt executes in exactly one generation, and
    /// both the fresh-Session-per-Work-Unit policy and post-rotation
    /// reconciliation need to know which one.
    pub session_id: SessionId,
    /// This Attempt's ordinal within its Work Unit, starting at 1.
    pub ordinal: u32,
    /// The Attempt's lifecycle status.
    pub status: AttemptStatus,
    /// The stable idempotency key the runner is called with, allowing safe
    /// retry and reconciliation of the external effect.
    pub idempotency_key: String,
    /// The runner capabilities observed for this Attempt, snapshotted in the
    /// observation transaction. A later runner upgrade must not rewrite it.
    pub capabilities_snapshot: Option<RunnerCapabilities>,
    /// The Boot Package this Attempt was started from.
    pub boot_package_ref: ResourceRef,
    /// The runner-assigned external execution reference, when observed.
    pub external_execution_ref: Option<OpaqueRef>,
    /// Whether repeating this Attempt's external effect is safe.
    pub side_effect_class: SideEffectClass,
    /// The collected result. A proposal, never an accepted outcome.
    pub result_ref: Option<ResourceRef>,
    /// The verification outcome recorded against this Attempt's result.
    ///
    /// Not in the `docs/02` §6 sketch. Core needs it in the projection to
    /// enforce the invariant that a Work Unit is only ever accepted on the
    /// back of an accepting verification outcome; the full durable
    /// `verification_runs` records live in the store
    /// (`docs/03-durable-state-and-transactions.md` §3).
    pub verification_outcome: Option<VerificationOutcome>,
}

/// The outcome of running a Work Unit's verification plan against a
/// collected result (`docs/06-verification-and-recovery.md` §2).
///
/// Only [`Self::Accepted`] can produce accepted Work Unit completion
/// mutations. A runner cannot set this: it is Loopward's own determination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationOutcome {
    /// Every required gate passed against current target bindings.
    Accepted,
    /// A required gate failed.
    Rejected,
    /// The target changed after verification, so the finding no longer
    /// binds. A stale pass cannot satisfy an acceptance criterion.
    Stale,
    /// A gate requires human or policy input before it can conclude.
    Suspended,
    /// Verification could not reach a conclusion.
    Inconclusive,
}

impl VerificationOutcome {
    /// Every variant, for exhaustive checks.
    pub const ALL: &'static [Self] = &[
        Self::Accepted,
        Self::Rejected,
        Self::Stale,
        Self::Suspended,
        Self::Inconclusive,
    ];

    /// Whether this outcome may accept a Work Unit's result.
    #[must_use]
    pub fn is_accepting(self) -> bool {
        matches!(self, Self::Accepted)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Stale => "stale",
            Self::Suspended => "suspended",
            Self::Inconclusive => "inconclusive",
        }
    }
}

/// Why a Runtime Session is being rotated
/// (`docs/07-session-rotation.md` §7).
///
/// Rotation is a first-class lifecycle event, not an emergency response to
/// an almost-full context window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RotationReason {
    /// The current Work Unit finished.
    WorkUnitBoundary,
    /// The Loop crossed a phase boundary.
    PhaseBoundary,
    /// Verification needs an independent context.
    VerificationBoundary,
    /// Remediation needs an independent context.
    RemediationBoundary,
    /// The local objective moved elsewhere.
    GoalLocalityShift,
    /// The soft context budget recommends rotating at the next boundary.
    SoftContextBudget,
    /// The hard context budget terminated admission.
    HardContextBudget,
    /// Pinned state or context integrity failed validation.
    ContextIntegrityFailure,
    /// Context entropy signals suggest a degraded working context.
    ContextEntropySignal,
    /// The runner reported internal compaction.
    RunnerCompaction,
    /// The runner failed.
    RunnerFailure,
    /// Rotation is part of crash recovery.
    Recovery,
    /// A deployment policy requires rotation.
    Policy,
    /// An operator requested rotation.
    OperatorRequest,
}

impl RotationReason {
    /// Whether this reason means the current generation cannot be drained
    /// cleanly and must be closed as failed
    /// (`docs/07-session-rotation.md` §8: mandatory closure after runner
    /// failure).
    #[must_use]
    pub fn is_runner_failure(self) -> bool {
        matches!(self, Self::RunnerFailure)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::WorkUnitBoundary => "work_unit_boundary",
            Self::PhaseBoundary => "phase_boundary",
            Self::VerificationBoundary => "verification_boundary",
            Self::RemediationBoundary => "remediation_boundary",
            Self::GoalLocalityShift => "goal_locality_shift",
            Self::SoftContextBudget => "soft_context_budget",
            Self::HardContextBudget => "hard_context_budget",
            Self::ContextIntegrityFailure => "context_integrity_failure",
            Self::ContextEntropySignal => "context_entropy_signal",
            Self::RunnerCompaction => "runner_compaction",
            Self::RunnerFailure => "runner_failure",
            Self::Recovery => "recovery",
            Self::Policy => "policy",
            Self::OperatorRequest => "operator_request",
        }
    }
}

/// A versioned, authoritative snapshot: the durable continuation boundary
/// and the trace segmentation boundary
/// (`docs/02-core-domain-model.md` §7, `docs/07-session-rotation.md` §13).
///
/// This is Core's *identity and versioning* view of a Checkpoint — what the
/// reducer must check to keep commits monotonic, parent-linked, and fenced
/// against a stale revision. The full content model (kernel, goal, pinned,
/// work, observations, decisions, domain, resources, verification,
/// continuation, telemetry, integrity) is materialized by the Checkpoint
/// Manager and stored as `docs/schemas/checkpoint-v0.1.yaml`; Core does not
/// need to interpret it to decide a transition.
///
/// A Checkpoint is a safe recovery point, not necessarily a success point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// This Checkpoint's immutable identity.
    pub id: CheckpointId,
    /// The Loop this Checkpoint belongs to.
    pub loop_id: LoopId,
    /// The Checkpoint version. Increases monotonically per Loop.
    pub version: u64,
    /// The aggregate revision this Checkpoint was built against.
    pub aggregate_revision: u64,
    /// The Checkpoint this one continues from, absent only for the genesis
    /// Checkpoint committed at Loop creation.
    pub parent: Option<CheckpointId>,
    /// The Episode this Checkpoint terminated, when it terminated one.
    pub episode_id: Option<EpisodeId>,
    /// `SHA-256` of the stored serialized Checkpoint bytes, when computed.
    pub integrity_digest: Option<String>,
}

/// Whether a Work Unit can no longer transition.
///
/// [`WorkUnitStatus`] is `loopward-api`'s type (it is identical to the
/// `docs/02-core-domain-model.md` §5 shape and is already on the wire in
/// `WorkUnitRequest`), so this predicate is a free function rather than an
/// inherent method.
#[must_use]
pub fn work_unit_status_is_terminal(status: WorkUnitStatus) -> bool {
    matches!(
        status,
        WorkUnitStatus::Accepted | WorkUnitStatus::Failed | WorkUnitStatus::Cancelled
    )
}

/// The stable `snake_case` label for a Work Unit status.
#[must_use]
pub fn work_unit_status_label(status: WorkUnitStatus) -> &'static str {
    match status {
        WorkUnitStatus::Pending => "pending",
        WorkUnitStatus::Active => "active",
        WorkUnitStatus::AwaitingVerification => "awaiting_verification",
        WorkUnitStatus::Accepted => "accepted",
        WorkUnitStatus::Failed => "failed",
        WorkUnitStatus::Suspended => "suspended",
        WorkUnitStatus::Cancelled => "cancelled",
    }
}

/// The complete durable state one Loop's reducer folds commands over.
///
/// Phase One holds at most one Episode, Runtime Session, Work Unit, and
/// Attempt at a time. Each is the *current* one: a terminal entity stays in
/// the projection until a successor replaces it, which is what makes
/// "a closed Session is never reopened" checkable without consulting
/// history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateState {
    /// The Loop's durable execution record.
    pub execution: LoopExecution,
    /// The current Episode, if the Loop has started one.
    pub episode: Option<Episode>,
    /// The current Runtime Session generation, if one has been started.
    pub session: Option<RuntimeSession>,
    /// The current Work Unit, if one has been planned.
    pub work_unit: Option<WorkUnit>,
    /// The current Work Unit Attempt, if one has been prepared.
    pub attempt: Option<WorkUnitAttempt>,
    /// The most recently committed Checkpoint.
    pub checkpoint: Checkpoint,
}

impl AggregateState {
    /// The current Episode if it has not reached a terminal state.
    #[must_use]
    pub fn active_episode(&self) -> Option<&Episode> {
        self.episode
            .as_ref()
            .filter(|episode| !episode.status.is_terminal())
    }

    /// The current Runtime Session generation if it is still open.
    #[must_use]
    pub fn open_session(&self) -> Option<&RuntimeSession> {
        self.session
            .as_ref()
            .filter(|session| !session.status.is_terminal())
    }

    /// The current Attempt if it has not reached a terminal state.
    #[must_use]
    pub fn open_attempt(&self) -> Option<&WorkUnitAttempt> {
        self.attempt
            .as_ref()
            .filter(|attempt| !attempt.status.is_terminal())
    }

    /// The highest Runtime Session generation this Loop has used.
    #[must_use]
    pub fn latest_generation(&self) -> u32 {
        self.session
            .as_ref()
            .map_or(0, |session| session.generation)
    }
}
