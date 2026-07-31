//! [`LoopController`]: the coordinator that advances one Loop by at most one
//! durable Episode transition per call (`docs/07-session-rotation.md` §14).
//!
//! # The happy-path Episode sequence
//!
//! ```text
//! acquire lease
//! → load Checkpoint
//! → start Episode
//! → hydrate
//! → create Session
//! → persist Attempt intent
//! → start runner
//! → collect
//! → drain
//! → verify
//! → commit Checkpoint
//! → close Session and Episode
//! ```
//!
//! Every external effect (hydration resolving resources, spawning the
//! runner, verification resolving evidence) happens outside a database
//! transaction: only [`decide`]/[`evolve`]'s resulting events ever cross into
//! [`LoopStore::append`] or [`LoopStore::commit_checkpoint`].
//!
//! # Two Episodes per Work Unit, not one
//!
//! Reading the sequence above as *one* Episode is tempting but wrong for
//! Phase One. `docs/07` §8's "fresh Runtime Session for verification"
//! default cannot be satisfied within one Episode: [`EpisodeStatus`]'s
//! documented edges (`docs/07` §3) let a Session start only while the
//! Episode is `Hydrating`, and there is no edge from `Executing` back to
//! `Hydrating` — only `Recovering` reaches `Hydrating` again, and that path
//! is for crash recovery, not routine rotation. `docs/07` §6's own "Normal
//! Rotation Sequence" confirms this: it ends with "Create next Episode and
//! Session generation" and "Hydrate a new Boot Package". So a fresh
//! verification Session is necessarily a fresh **Episode**, of
//! [`EpisodeKind::Verification`] (already derived by
//! `loopward_core::reducer::next_episode_kind` from the Work Unit's
//! `awaiting_verification` status). [`LoopController::advance`] therefore
//! runs the sequence above in two calls: one Execution Episode (hydrate
//! through commit, ending with the Work Unit `awaiting_verification`), then
//! one Verification Episode (hydrate a fresh Boot Package, start a fresh
//! Session, verify, commit). Each call still performs at most one durable
//! Episode transition, honoring `docs/07` §14 exactly.
//!
//! # A found gap in `LoopStore::append`
//!
//! Neither `loopward-store-sqlite`'s `SqliteLoopStore` nor
//! `port_contract.rs`'s `InMemoryLoopStore` fake folds appended events into
//! the projection fields (`episode`, `session`, `work_unit`, `attempt`) that
//! `append` returns or persists — both implementations only advance
//! `aggregate_revision` and round-trip the rest of the projection unchanged
//! (neither calls `loopward_core::evolve`). A `store.load()` performed
//! between two `append` calls, without an intervening `commit_checkpoint`,
//! would therefore observe a stale projection. [`LoopController`] works
//! around this by never re-loading mid-Episode: every helper below folds
//! events into its own locally computed [`AggregateState`] via `evolve` and
//! carries that value forward for the rest of the call, and each Episode
//! always ends with [`LoopStore::commit_checkpoint`], which *does* take a
//! caller-computed `state` and persists it verbatim — so the next
//! `advance()` call's `store.load()` is always correct. This is safe for
//! Phase One's synchronous, single-writer-per-lease design, but it is a real
//! defect: any future path that calls `store.load()` mid-Episode (crash
//! recovery restarting a partially advanced Episode — Task 10's concern)
//! would observe the staleness and needs `LoopStore::append` fixed first.

use std::collections::BTreeMap;
use std::sync::Arc;

use loopward_api::context::{BootPackageManifest, ContextBudget, ContextBudgetRequest};
use loopward_api::ids::{EpisodeId, LoopId};
use loopward_api::loop_service::{RunCondition, RunLimits};
use loopward_api::resource::{OpaqueRef, ResourceRef, VerificationPlanRef};
use loopward_api::runner::{RunnerError, SuspensionReason, WorkUnitRunner};
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{WorkUnit, WorkUnitRequest, WorkUnitResult};
use loopward_core::{
    AggregateState, Checkpoint, DomainError, EffectivePolicy, EpisodeKind, LifecycleCommand,
    LoopEvent, LoopStatus, RotationReason, decide, evolve, validate_invariants,
    work_unit_status_is_terminal,
};

use crate::context::ContextCandidate;
use crate::ports::{
    AcquireLease, CheckpointCommitRequest, LoopLease, ResourceBeginHint, ResourceError,
    RuntimeInstanceId, RuntimePorts, StoreError,
};
use crate::rotation::{RotationContext, RotationPolicy};
use crate::verification::{
    AggregationPolicy, Digest, FindingLineage, RemediationPolicy, VerificationEngine,
    VerificationError, VerificationGateSpec, VerificationPlan, VerificationPlanId,
    VerificationRequest, VerificationTarget,
};
use crate::{HydrationError, HydrationReport, HydrationRequest};

/// Supplies the Checkpoint Manager's projected context candidates for one
/// Boot Package hydration.
///
/// No prior task builds a Checkpoint Manager that projects
/// `docs/schemas/checkpoint-v0.1.yaml`'s `pinned`, `decisions.active`,
/// `observations.verified`, and `resources` sections into
/// [`ContextCandidate`]s (`loopward-runtime/src/context/hydrator.rs`'s own
/// doc comment names this as the hydrator's expected input, supplied by a
/// component this plan has not built yet). This is [`LoopController`]'s seam
/// for one, mirroring [`VerificationPlanResolver`]: a deployment plugs in a
/// real Checkpoint Manager here; the default
/// [`LoopController::new`] configuration supplies no candidates at all,
/// which still hydrates correctly for a Work Unit with no pinned
/// constraints (the hydrator synthesizes required candidates for declared
/// inputs on its own).
pub trait ContextCandidateProvider: Send + Sync {
    /// Returns the candidates to offer the hydrator for `work_unit`, judged
    /// against `checkpoint`.
    fn candidates(&self, work_unit: &WorkUnit, checkpoint: &Checkpoint) -> Vec<ContextCandidate>;
}

/// A [`ContextCandidateProvider`] that never offers any candidate.
struct NoContextCandidates;

impl ContextCandidateProvider for NoContextCandidates {
    fn candidates(&self, _work_unit: &WorkUnit, _checkpoint: &Checkpoint) -> Vec<ContextCandidate> {
        Vec::new()
    }
}

/// Everything a [`VerificationPlan`] needs except the [`VerificationTarget`]
/// it judges.
///
/// The target cannot be part of the template: it binds to the exact result
/// a runner just collected (`docs/06-verification-and-recovery.md` §6),
/// which does not exist until the Attempt has run. [`LoopController`] pairs
/// this template with a freshly computed target to build the
/// [`VerificationPlan`] it hands to the [`VerificationEngine`].
#[derive(Debug, Clone)]
pub struct VerificationPlanTemplate {
    /// This plan's identity.
    pub id: VerificationPlanId,
    /// This plan's version. Part of the target binding.
    pub version: u64,
    /// The gates to evaluate.
    pub gates: Vec<VerificationGateSpec>,
    /// How findings fold into one outcome.
    pub aggregation: AggregationPolicy,
    /// The bounds remediation runs under.
    pub remediation_policy: RemediationPolicy,
}

/// Resolves a Work Unit's [`VerificationPlanRef`] to the gates, aggregation,
/// and remediation policy [`LoopController`] should evaluate against a
/// collected result.
///
/// No prior task defines a durable plan registry, so this is
/// [`LoopController`]'s own seam for one: a deployment-specific
/// implementation (backed by configuration, a database, or a fixed table)
/// plugs in here without `LoopController` needing to know how plans are
/// stored.
pub trait VerificationPlanResolver: Send + Sync {
    /// Resolves `reference` to a plan template, or `None` if no plan is
    /// registered for it.
    fn resolve(&self, reference: &VerificationPlanRef) -> Option<VerificationPlanTemplate>;
}

/// The result of one [`LoopController::advance`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationOutcome {
    /// One Episode reached a safe terminal boundary and committed a
    /// Checkpoint.
    EpisodeCompleted {
        /// The Episode that completed.
        episode_id: EpisodeId,
        /// The Checkpoint version it committed.
        checkpoint_version: u64,
    },
    /// The Loop suspended before or during this call.
    Suspended,
    /// The Loop reached a terminal outcome (completed, failed, or
    /// cancelled).
    Terminal {
        /// The terminal status reached.
        status: LoopStatus,
    },
    /// Nothing was advanced: the Loop was already terminal or suspended, or
    /// this call completed the Loop without running an Episode (no Work
    /// Unit remained to attempt).
    NoOp,
}

/// An error raised while advancing a Loop.
#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    /// The durable store rejected an operation.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The reducer refused a command or rejected a resulting state.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// Boot Package hydration failed.
    #[error(transparent)]
    Hydration(#[from] HydrationError),
    /// Verification could not be run at all.
    #[error(transparent)]
    Verification(#[from] VerificationError),
    /// The runner reported a protocol-level failure.
    #[error("runner call failed: {0}")]
    Runner(#[from] RunnerError),
    /// A resource could not be written, finalized, or resolved.
    #[error(transparent)]
    Resource(#[from] ResourceError),
    /// A durable artifact could not be serialized or deserialized.
    #[error("could not (de)serialize a durable artifact: {0}")]
    Serialization(#[from] serde_json::Error),
    /// No [`VerificationPlanTemplate`] is registered for the Work Unit's
    /// declared plan reference.
    #[error("no verification plan is registered for `{0}`")]
    UnknownVerificationPlan(String),
    /// The Loop has no Work Unit assigned, so there is nothing to advance.
    ///
    /// Assigning a Work Unit to a Loop has no `LifecycleCommand`: Core
    /// treats it as caller-supplied state (`docs/02-core-domain-model.md`
    /// §11), and no earlier task defines how one gets there. Callers of
    /// this controller are expected to have already placed a Work Unit in
    /// the Loop's projection before calling `advance`.
    #[error("loop {0} has no work unit assigned")]
    NoWorkUnit(LoopId),
    /// An Episode was already active when `advance` was called: a prior call
    /// or process was interrupted mid-Episode.
    ///
    /// Reconciling and resuming an in-flight Episode is Task 10's concern
    /// (Reconciliation and Crash Recovery); this controller only ever starts
    /// and fully finishes one Episode per `advance` call, so it refuses
    /// rather than guessing how to resume someone else's partial work.
    #[error(
        "loop {0} has an episode already in flight; recovery is required before it can advance"
    )]
    RecoveryRequired(LoopId),
    /// `run_until` was called without any finite limit.
    #[error("run_until requires at least one of max_episodes, deadline, or max_resource_cost")]
    UnboundedRunUntil,
    /// A structural invariant `LoopController` relies on, but does not
    /// itself re-validate every branch of, did not hold.
    #[error("internal invariant violated: {0}")]
    Invariant(String),
}

/// The coordinator that advances one Loop by at most one durable Episode
/// transition per [`Self::advance`] call.
///
/// See the module documentation for the exact sequence and for why one Work
/// Unit takes two `advance` calls (an Execution Episode, then a Verification
/// Episode) rather than one.
#[derive(Clone)]
pub struct LoopController {
    ports: RuntimePorts,
    hydrator: crate::ContextHydrator,
    runner: Arc<dyn WorkUnitRunner>,
    verification: VerificationEngine,
    plans: Arc<dyn VerificationPlanResolver>,
    candidates: Arc<dyn ContextCandidateProvider>,
    policy: EffectivePolicy,
    holder_id: RuntimeInstanceId,
    context_rule_version: String,
    default_budget: ContextBudget,
    lease_ttl: time::Duration,
}

impl std::fmt::Debug for LoopController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopController")
            .field("holder_id", &self.holder_id)
            .field("context_rule_version", &self.context_rule_version)
            .finish_non_exhaustive()
    }
}

impl LoopController {
    /// Builds a controller over `ports`, executing Work Units through
    /// `runner` and resolving verification plans through `plans`.
    ///
    /// Uses Phase One's default policy (fresh Runtime Session per Work Unit
    /// and for verification), a conservative default context budget, and a
    /// 60 second lease TTL; use the `with_*` builders to override them.
    #[must_use]
    pub fn new(
        ports: RuntimePorts,
        runner: Arc<dyn WorkUnitRunner>,
        plans: Arc<dyn VerificationPlanResolver>,
    ) -> Self {
        let hydrator = crate::ContextHydrator::new(Arc::clone(&ports.resources));
        Self {
            ports,
            hydrator,
            runner,
            verification: VerificationEngine::new(),
            plans,
            candidates: Arc::new(NoContextCandidates),
            policy: EffectivePolicy::default(),
            holder_id: RuntimeInstanceId::new(),
            context_rule_version: "loopward.context-rules/v0.1".to_owned(),
            default_budget: ContextBudget {
                model_window_tokens: None,
                soft_input_limit_tokens: 8_000,
                hard_input_limit_tokens: 16_000,
                reserved_output_tokens: 2_000,
                reserved_tool_tokens: 1_000,
                safety_margin_tokens: 500,
            },
            lease_ttl: time::Duration::seconds(60),
        }
    }

    /// Overrides the effective policy (default: Phase One's fresh-session
    /// deterministic policy).
    #[must_use]
    pub fn with_policy(mut self, policy: EffectivePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Overrides the recorded context selection rule version.
    #[must_use]
    pub fn with_context_rule_version(mut self, version: impl Into<String>) -> Self {
        self.context_rule_version = version.into();
        self
    }

    /// Overrides the default resolved context budget used when a Work Unit
    /// does not request one.
    #[must_use]
    pub fn with_default_budget(mut self, budget: ContextBudget) -> Self {
        self.default_budget = budget;
        self
    }

    /// Overrides the lease TTL acquired for each `advance` call.
    #[must_use]
    pub fn with_lease_ttl(mut self, ttl: time::Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Overrides the verification engine (default: every Phase One gate
    /// kind registered).
    #[must_use]
    pub fn with_verification_engine(mut self, engine: VerificationEngine) -> Self {
        self.verification = engine;
        self
    }

    /// Overrides the context candidate provider (default: none, so a Boot
    /// Package hydrates from only the Work Unit's own declared inputs).
    #[must_use]
    pub fn with_candidate_provider(
        mut self,
        candidates: Arc<dyn ContextCandidateProvider>,
    ) -> Self {
        self.candidates = candidates;
        self
    }

    /// The runtime ports this controller drives Loops with.
    #[must_use]
    pub fn ports(&self) -> &RuntimePorts {
        &self.ports
    }

    /// The stable runtime instance identity this controller acquires leases
    /// as.
    #[must_use]
    pub fn holder_id(&self) -> RuntimeInstanceId {
        self.holder_id
    }

    /// Requests `loop_id` suspend at its current safe boundary
    /// (`docs/07-session-rotation.md` §12), through the reducer's
    /// [`LifecycleCommand::Suspend`], committing a Checkpoint if doing so
    /// leaves an Episode terminal and not yet checkpointed.
    ///
    /// A no-op when the Loop is already terminal or already suspended.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when the lease cannot be acquired or the
    /// reducer refuses the command.
    pub async fn suspend(
        &self,
        loop_id: LoopId,
        reason: SuspensionReason,
    ) -> Result<(), ControllerError> {
        let lease = self
            .ports
            .store
            .acquire_lease(AcquireLease {
                loop_id,
                holder_id: self.holder_id,
                ttl: self.lease_ttl,
            })
            .await?;
        let stored = self.ports.store.load(loop_id).await?;
        let state = stored.state;
        if state.execution.status.is_terminal() || state.execution.status == LoopStatus::Suspended {
            return Ok(());
        }

        let suspended = self
            .apply(
                loop_id,
                &lease,
                &state,
                LifecycleCommand::Suspend { reason },
            )
            .await?;
        let newly_terminal_episode_id = suspended.episode.as_ref().and_then(|episode| {
            (episode.status.is_terminal() && episode.terminal_checkpoint_version.is_none())
                .then_some(episode.id)
        });
        if let Some(episode_id) = newly_terminal_episode_id {
            let checkpoint = self.build_checkpoint(&suspended, episode_id);
            self.commit(loop_id, &lease, &suspended, checkpoint).await?;
        }
        Ok(())
    }

    /// Resumes a suspended Loop.
    ///
    /// `Suspended -> Active` has no `LifecycleCommand`: `loopward-core`
    /// deliberately exposes it as a runtime-level operation
    /// (`loopward_core::model` §`LoopStatus` doc comment), not a reducer
    /// transition. This performs a direct, lease-fenced projection write
    /// through [`LoopStore::commit_checkpoint`] instead — the only store
    /// primitive that accepts an arbitrary caller-computed [`AggregateState`]
    /// — flipping only [`loopward_core::LoopExecution::status`] and leaving
    /// every other entity untouched.
    ///
    /// This is deliberately minimal: if the Episode active when the Loop
    /// suspended left an unreconciled Attempt behind (neither terminal nor
    /// merely suspended-and-safe), the *next* [`Self::advance`] call will
    /// correctly refuse via the reducer's own
    /// `attempt.reconciled_before_next_generation` invariant rather than
    /// silently resuming into an unsafe state. Recognizing that case here
    /// and reconciling it automatically is Task 10's job (Reconciliation and
    /// Crash Recovery), not this one's.
    ///
    /// A no-op when the Loop is not currently suspended.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when the lease cannot be acquired, the
    /// store rejects the write, or the resulting state fails invariant
    /// validation.
    pub async fn resume(&self, loop_id: LoopId) -> Result<(), ControllerError> {
        let lease = self
            .ports
            .store
            .acquire_lease(AcquireLease {
                loop_id,
                holder_id: self.holder_id,
                ttl: self.lease_ttl,
            })
            .await?;
        let stored = self.ports.store.load(loop_id).await?;
        let mut state = stored.state;
        if state.execution.status != LoopStatus::Suspended {
            return Ok(());
        }

        let expected_revision = state.execution.aggregate_revision;
        let expected_checkpoint_version = state.execution.checkpoint_version;
        let checkpoint = Checkpoint {
            id: self.ports.ids.new_checkpoint_id(),
            loop_id,
            version: state.checkpoint.version + 1,
            aggregate_revision: expected_revision + 1,
            parent: Some(state.checkpoint.id),
            episode_id: None,
            integrity_digest: None,
        };
        state.execution.status = LoopStatus::Active;
        state.execution.aggregate_revision = expected_revision + 1;
        state.execution.checkpoint_version = checkpoint.version;
        state.checkpoint = checkpoint.clone();
        validate_invariants(&state)?;

        self.ports
            .store
            .commit_checkpoint(CheckpointCommitRequest {
                loop_id,
                expected_revision,
                expected_checkpoint_version,
                fencing_token: lease.fencing_token,
                checkpoint: checkpoint.clone(),
                events: vec![LoopEvent::CheckpointCommitted { checkpoint }],
                state,
            })
            .await?;
        Ok(())
    }

    /// Explicitly cancels a Loop (`docs/09-public-api.md` §3: cancellation
    /// is always explicit, never implied by a client disconnecting).
    ///
    /// Like [`Self::resume`], `Active -> Cancelled` has no `LifecycleCommand`
    /// and is performed as a direct, lease-fenced projection write. Kept
    /// deliberately minimal: this only flips the status when nothing is
    /// live beneath the Loop (no active Episode, no open Session) — the same
    /// precondition the reducer's own `Complete`/`Fail` commands require —
    /// because forcing a *live* Episode's Session and Attempt to a
    /// consistent terminal shape outside `decide`/`evolve` would duplicate
    /// exactly the reasoning `apply_loop_failed` and
    /// `apply_rotation_requested` already encode, and drifting from it would
    /// be worse than refusing. Cancelling a Loop with a live Episode
    /// requires suspending it first in Phase One.
    ///
    /// A no-op when the Loop is already terminal.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError::Invariant`] when an Episode or Session is
    /// still live, and otherwise whatever [`Self::resume`] can return for
    /// the same category of failure.
    pub async fn cancel(&self, loop_id: LoopId) -> Result<(), ControllerError> {
        let lease = self
            .ports
            .store
            .acquire_lease(AcquireLease {
                loop_id,
                holder_id: self.holder_id,
                ttl: self.lease_ttl,
            })
            .await?;
        let stored = self.ports.store.load(loop_id).await?;
        let mut state = stored.state;
        if state.execution.status.is_terminal() {
            return Ok(());
        }
        if state.active_episode().is_some() || state.open_session().is_some() {
            return Err(ControllerError::Invariant(
                "cannot cancel while an episode or session is in flight; suspend first".to_owned(),
            ));
        }

        let expected_revision = state.execution.aggregate_revision;
        let expected_checkpoint_version = state.execution.checkpoint_version;
        let checkpoint = Checkpoint {
            id: self.ports.ids.new_checkpoint_id(),
            loop_id,
            version: state.checkpoint.version + 1,
            aggregate_revision: expected_revision + 1,
            parent: Some(state.checkpoint.id),
            episode_id: None,
            integrity_digest: None,
        };
        state.execution.status = LoopStatus::Cancelled;
        state.execution.aggregate_revision = expected_revision + 1;
        state.execution.checkpoint_version = checkpoint.version;
        state.checkpoint = checkpoint.clone();
        validate_invariants(&state)?;

        self.ports
            .store
            .commit_checkpoint(CheckpointCommitRequest {
                loop_id,
                expected_revision,
                expected_checkpoint_version,
                fencing_token: lease.fencing_token,
                checkpoint: checkpoint.clone(),
                events: vec![LoopEvent::CheckpointCommitted { checkpoint }],
                state,
            })
            .await?;
        Ok(())
    }

    /// Advances `loop_id` by at most one durable Episode transition to its
    /// next safe boundary (`docs/07-session-rotation.md` §14).
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when the lease cannot be acquired, the
    /// reducer refuses a command, hydration or verification fails outright,
    /// the runner reports a protocol failure that is not otherwise handled
    /// as a rotation, or the Loop has no Work Unit assigned. A collected
    /// runner failure, a hard context budget, or a drain-time integrity
    /// mismatch are not errors: they resolve to
    /// [`OperationOutcome::Suspended`] with a committed Checkpoint recording
    /// the best-known durable state.
    pub async fn advance(&self, loop_id: LoopId) -> Result<OperationOutcome, ControllerError> {
        let lease = self
            .ports
            .store
            .acquire_lease(AcquireLease {
                loop_id,
                holder_id: self.holder_id,
                ttl: self.lease_ttl,
            })
            .await?;

        let stored = self.ports.store.load(loop_id).await?;
        let state = stored.state;

        if state.execution.status.is_terminal() {
            return Ok(OperationOutcome::Terminal {
                status: state.execution.status,
            });
        }
        if state.execution.status == LoopStatus::Suspended {
            return Ok(OperationOutcome::NoOp);
        }
        if state.active_episode().is_some() {
            return Err(ControllerError::RecoveryRequired(loop_id));
        }

        match &state.work_unit {
            None => Err(ControllerError::NoWorkUnit(loop_id)),
            Some(work_unit) if work_unit_status_is_terminal(work_unit.status) => {
                // Phase One's controller carries exactly one Work Unit per
                // Loop; once it is terminal, nothing remains to attempt.
                let completed = self
                    .apply(loop_id, &lease, &state, LifecycleCommand::Complete)
                    .await?;
                Ok(OperationOutcome::Terminal {
                    status: completed.execution.status,
                })
            }
            Some(_) => {
                let episode_id = self.ports.ids.new_episode_id();
                let state = self
                    .apply(
                        loop_id,
                        &lease,
                        &state,
                        LifecycleCommand::StartEpisode { episode_id },
                    )
                    .await?;
                let kind = state.episode.as_ref().map(|episode| episode.kind);
                if kind == Some(EpisodeKind::Verification) {
                    Box::pin(self.run_verification_episode(loop_id, &lease, state, episode_id))
                        .await
                } else {
                    Box::pin(self.run_execution_episode(loop_id, &lease, state, episode_id)).await
                }
            }
        }
    }

    /// Repeatedly calls [`Self::advance`] until `condition` is met or a
    /// limit in `limits` is reached.
    ///
    /// Loops by calling `advance`; this is not a second lifecycle path, and
    /// it performs no hidden unbounded work
    /// (`docs/07-session-rotation.md` §14).
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError::UnboundedRunUntil`] when `limits` names no
    /// finite bound at all, and otherwise anything [`Self::advance`] can
    /// return.
    pub async fn run_until(
        &self,
        loop_id: LoopId,
        condition: RunCondition,
        limits: RunLimits,
    ) -> Result<Vec<OperationOutcome>, ControllerError> {
        if limits.max_episodes.is_none()
            && limits.deadline.is_none()
            && limits.max_resource_cost.is_none()
        {
            return Err(ControllerError::UnboundedRunUntil);
        }

        let mut outcomes = Vec::new();
        let mut episodes = 0u32;
        loop {
            if let Some(max) = limits.max_episodes
                && episodes >= max
            {
                break;
            }
            if let Some(deadline) = limits.deadline
                && self.ports.clock.now() >= deadline
            {
                break;
            }

            let outcome = Box::pin(self.advance(loop_id)).await?;
            let stop = matches!(
                (&outcome, condition),
                (
                    OperationOutcome::Terminal { .. } | OperationOutcome::NoOp,
                    _
                ) | (
                    OperationOutcome::Suspended,
                    RunCondition::UntilTerminalOrSuspended
                )
            );
            if matches!(outcome, OperationOutcome::EpisodeCompleted { .. }) {
                episodes += 1;
            }
            outcomes.push(outcome);
            if stop {
                break;
            }
        }
        Ok(outcomes)
    }

    // -----------------------------------------------------------------
    // Episode branches
    // -----------------------------------------------------------------

    #[allow(clippy::too_many_lines)]
    async fn run_execution_episode(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
    ) -> Result<OperationOutcome, ControllerError> {
        let work_unit = state
            .work_unit
            .clone()
            .ok_or(ControllerError::NoWorkUnit(loop_id))?;

        let capabilities = self.runner.capabilities().await?;
        let budget = self.resolve_context_budget(work_unit.budget.context);
        let session_generation = state.latest_generation() + 1;

        let hydration_request = HydrationRequest {
            checkpoint: state.checkpoint.clone(),
            episode_id,
            session_generation,
            work_unit: work_unit.clone(),
            capabilities,
            budget,
            context_rule_version: self.context_rule_version.clone(),
            candidates: self.candidates.candidates(&work_unit, &state.checkpoint),
        };

        let report = self.hydrator.hydrate(hydration_request.clone()).await?;
        let selected_tokens = report.selected_tokens();
        let budget_decision = RotationPolicy::evaluate(&RotationContext {
            hard_context_budget: selected_tokens >= budget.hard_input_limit_tokens,
            soft_context_budget: selected_tokens >= budget.soft_input_limit_tokens,
            ..RotationContext::default()
        });

        if budget_decision.primary_reason == Some(RotationReason::HardContextBudget) {
            // The hard context budget stops admission before any model work
            // is started: no Session, no Attempt, no runner call
            // (`docs/07` §10).
            let state = self.mark_hydrated(loop_id, lease, &state, &report).await?;
            self.ensure_episode_terminal_and_commit(
                loop_id,
                lease,
                state,
                episode_id,
                SuspensionReason::BudgetExceeded,
            )
            .await?;
            return Ok(OperationOutcome::Suspended);
        }

        let state = self.mark_hydrated(loop_id, lease, &state, &report).await?;

        let session_id = self.ports.ids.new_session_id();
        let state = self
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::StartSession {
                    session_id,
                    generation: session_generation,
                },
            )
            .await?;

        let attempt_id = self.ports.ids.new_attempt_id();
        let idempotency_key = format!("attempt:{attempt_id}");
        let state = self
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::PrepareAttempt {
                    attempt_id,
                    idempotency_key: idempotency_key.clone(),
                },
            )
            .await?;

        let request = WorkUnitRequest {
            protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
            loop_id,
            episode_id,
            session_id,
            session_generation,
            work_unit: work_unit.clone(),
            attempt_id,
            idempotency_key,
            boot_package: report.manifest.clone(),
            resource_grants: Vec::new(),
            output_contract: work_unit.output_contract.clone(),
            budget: work_unit.budget,
            deadline: None,
            trace_context: TraceContext::default(),
        };

        let Ok(handle) = self.runner.start(request).await else {
            let state = self
                .apply(
                    loop_id,
                    lease,
                    &state,
                    LifecycleCommand::RequestRotation {
                        reason: RotationReason::RunnerFailure,
                    },
                )
                .await?;
            self.ensure_episode_terminal_and_commit(
                loop_id,
                lease,
                state,
                episode_id,
                SuspensionReason::Recovery,
            )
            .await?;
            return Ok(OperationOutcome::Suspended);
        };

        let external_ref = handle
            .external_execution_ref
            .clone()
            .unwrap_or_else(|| OpaqueRef::new(format!("attempt:{attempt_id}")));
        let state = self
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::RecordRunnerStarted { external_ref },
            )
            .await?;

        let Ok(collected) = self.runner.collect(&handle).await else {
            let state = self
                .apply(
                    loop_id,
                    lease,
                    &state,
                    LifecycleCommand::RequestRotation {
                        reason: RotationReason::RunnerFailure,
                    },
                )
                .await?;
            self.ensure_episode_terminal_and_commit(
                loop_id,
                lease,
                state,
                episode_id,
                SuspensionReason::Recovery,
            )
            .await?;
            return Ok(OperationOutcome::Suspended);
        };

        // Drain-time integrity re-check: pinned constraints must survive
        // hydration and drain losslessly. Re-hydrating the same request and
        // comparing pinned digests detects pinned state that moved
        // underneath the run; a divergence means the drained result cannot
        // commit (`docs/07` §11, Global Constraints).
        let redrawn = self.hydrator.hydrate(hydration_request).await?;
        if redrawn.manifest.pinned_digest != report.manifest.pinned_digest {
            let state = self
                .apply(
                    loop_id,
                    lease,
                    &state,
                    LifecycleCommand::RequestRotation {
                        reason: RotationReason::ContextIntegrityFailure,
                    },
                )
                .await?;
            self.ensure_episode_terminal_and_commit(
                loop_id,
                lease,
                state,
                episode_id,
                SuspensionReason::Recovery,
            )
            .await?;
            return Ok(OperationOutcome::Suspended);
        }

        let result_ref = self.finalize_result(&collected).await?;
        let state = self
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::RecordResult { result: result_ref },
            )
            .await?;

        let reason = if budget_decision.primary_reason == Some(RotationReason::SoftContextBudget) {
            RotationReason::SoftContextBudget
        } else {
            RotationReason::WorkUnitBoundary
        };
        let committed = self
            .rotate_and_commit(loop_id, lease, state, episode_id, reason)
            .await?;

        Ok(OperationOutcome::EpisodeCompleted {
            episode_id,
            checkpoint_version: committed.checkpoint.version,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run_verification_episode(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
    ) -> Result<OperationOutcome, ControllerError> {
        let work_unit = state
            .work_unit
            .clone()
            .ok_or(ControllerError::NoWorkUnit(loop_id))?;
        let attempt = state.attempt.clone().ok_or_else(|| {
            ControllerError::Invariant(
                "a verification episode started with no Attempt in the projection".to_owned(),
            )
        })?;
        let result_ref = attempt.result_ref.clone().ok_or_else(|| {
            ControllerError::Invariant(
                "a verification episode started with an Attempt that carries no result".to_owned(),
            )
        })?;

        let capabilities = self.runner.capabilities().await?;
        let budget = self.resolve_context_budget(work_unit.budget.context);
        let session_generation = state.latest_generation() + 1;

        let hydration_request = HydrationRequest {
            checkpoint: state.checkpoint.clone(),
            episode_id,
            session_generation,
            work_unit: work_unit.clone(),
            capabilities,
            budget,
            context_rule_version: self.context_rule_version.clone(),
            candidates: self.candidates.candidates(&work_unit, &state.checkpoint),
        };
        let report = self.hydrator.hydrate(hydration_request).await?;
        let state = self.mark_hydrated(loop_id, lease, &state, &report).await?;

        let session_id = self.ports.ids.new_session_id();
        let state = self
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::StartSession {
                    session_id,
                    generation: session_generation,
                },
            )
            .await?;

        let template = self
            .plans
            .resolve(&work_unit.verification_plan)
            .ok_or_else(|| {
                ControllerError::UnknownVerificationPlan(
                    work_unit.verification_plan.reference.clone(),
                )
            })?;

        let result_bytes = self.ports.resources.resolve(&result_ref).await?;
        let result: WorkUnitResult = serde_json::from_slice(&result_bytes)?;

        let target = VerificationTarget {
            work_unit_id: work_unit.id,
            result_digest: Digest::new(result_ref.digest.clone().unwrap_or_default()),
            resource_digests: BTreeMap::new(),
            workspace_revision: None,
            plan_version: template.version,
            constraint_digest: None,
        };
        let plan = VerificationPlan {
            id: template.id,
            version: template.version,
            target,
            gates: template.gates,
            aggregation: template.aggregation,
            remediation_policy: template.remediation_policy,
        };
        let plan_id = plan.id.clone();
        let plan_version = plan.version;

        let lineage = FindingLineage {
            loop_id,
            work_unit_id: work_unit.id,
            attempt_id: attempt.id,
            plan_id,
            plan_version,
            checkpoint_version: state.checkpoint.version,
        };

        let run = self
            .verification
            .evaluate(VerificationRequest {
                plan,
                result: Arc::new(result),
                lineage,
                prior_findings: Vec::new(),
                resources: Arc::clone(&self.ports.resources),
                observed_at: self.ports.clock.now(),
            })
            .await?;

        let state = self
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::RecordVerification {
                    outcome: run.outcome,
                },
            )
            .await?;

        if state.execution.status == LoopStatus::Suspended {
            let committed = self
                .ensure_episode_terminal_and_commit(
                    loop_id,
                    lease,
                    state,
                    episode_id,
                    SuspensionReason::VerificationPending,
                )
                .await?;
            let _ = committed;
            return Ok(OperationOutcome::Suspended);
        }

        let committed = self
            .rotate_and_commit(
                loop_id,
                lease,
                state,
                episode_id,
                RotationReason::VerificationBoundary,
            )
            .await?;
        Ok(OperationOutcome::EpisodeCompleted {
            episode_id,
            checkpoint_version: committed.checkpoint.version,
        })
    }

    // -----------------------------------------------------------------
    // Shared step helpers
    // -----------------------------------------------------------------

    fn resolve_context_budget(&self, requested: Option<ContextBudgetRequest>) -> ContextBudget {
        let requested = requested.unwrap_or_default();
        ContextBudget {
            model_window_tokens: self.default_budget.model_window_tokens,
            soft_input_limit_tokens: requested
                .soft_input_limit_tokens
                .unwrap_or(self.default_budget.soft_input_limit_tokens),
            hard_input_limit_tokens: requested
                .hard_input_limit_tokens
                .unwrap_or(self.default_budget.hard_input_limit_tokens),
            reserved_output_tokens: self.default_budget.reserved_output_tokens,
            reserved_tool_tokens: self.default_budget.reserved_tool_tokens,
            safety_margin_tokens: self.default_budget.safety_margin_tokens,
        }
    }

    async fn mark_hydrated(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: &AggregateState,
        report: &HydrationReport,
    ) -> Result<AggregateState, ControllerError> {
        let manifest_ref = self.finalize_manifest(&report.manifest).await?;
        self.apply(
            loop_id,
            lease,
            state,
            LifecycleCommand::MarkHydrated {
                manifest: manifest_ref,
            },
        )
        .await
    }

    async fn finalize_manifest(
        &self,
        manifest: &BootPackageManifest,
    ) -> Result<ResourceRef, ControllerError> {
        let bytes = serde_json::to_vec(manifest)?;
        let handle = self
            .ports
            .resources
            .begin(ResourceBeginHint {
                media_type: Some("application/json".to_owned()),
                summary: Some("boot package manifest".to_owned()),
            })
            .await?;
        self.ports.resources.write(&handle, &bytes).await?;
        let mut resource_ref = self.ports.resources.finalize(handle).await?;
        resource_ref.digest = Some(crate::context::content_digest(&bytes));
        Ok(resource_ref)
    }

    async fn finalize_result(
        &self,
        result: &WorkUnitResult,
    ) -> Result<ResourceRef, ControllerError> {
        let bytes = serde_json::to_vec(result)?;
        let handle = self
            .ports
            .resources
            .begin(ResourceBeginHint {
                media_type: Some("application/json".to_owned()),
                summary: Some("work unit result".to_owned()),
            })
            .await?;
        self.ports.resources.write(&handle, &bytes).await?;
        let mut resource_ref = self.ports.resources.finalize(handle).await?;
        resource_ref.digest = Some(crate::context::content_digest(&bytes));
        Ok(resource_ref)
    }

    /// Decides and evolves `command` against `state`, then durably appends
    /// the resulting events. Returns the locally folded state rather than
    /// the store's returned projection — see the module documentation's
    /// note on the `LoopStore::append` projection gap.
    async fn apply(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: &AggregateState,
        command: LifecycleCommand,
    ) -> Result<AggregateState, ControllerError> {
        let events = decide(state, &command, &self.policy)?;
        let expected_revision = state.execution.aggregate_revision;
        let mut next = state.clone();
        for event in &events {
            next = evolve(next, event)?;
        }
        validate_invariants(&next)?;
        self.ports
            .store
            .append(loop_id, expected_revision, lease.fencing_token, events)
            .await?;
        Ok(next)
    }

    /// Decides and evolves a [`LifecycleCommand::CommitCheckpoint`] against
    /// `state`, then atomically commits it.
    async fn commit(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: &AggregateState,
        checkpoint: Checkpoint,
    ) -> Result<AggregateState, ControllerError> {
        let command = LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint.clone(),
        };
        let events = decide(state, &command, &self.policy)?;
        let expected_revision = state.execution.aggregate_revision;
        let expected_checkpoint_version = state.execution.checkpoint_version;
        let mut next = state.clone();
        for event in &events {
            next = evolve(next, event)?;
        }
        validate_invariants(&next)?;
        self.ports
            .store
            .commit_checkpoint(CheckpointCommitRequest {
                loop_id,
                expected_revision,
                expected_checkpoint_version,
                fencing_token: lease.fencing_token,
                checkpoint,
                events,
                state: next.clone(),
            })
            .await?;
        Ok(next)
    }

    /// Builds the next Checkpoint for `episode_id`, continuing from
    /// `state.checkpoint`.
    fn build_checkpoint(&self, state: &AggregateState, episode_id: EpisodeId) -> Checkpoint {
        let digest = crate::context::content_digest(
            serde_json::to_vec(state).unwrap_or_default().as_slice(),
        );
        Checkpoint {
            id: self.ports.ids.new_checkpoint_id(),
            loop_id: state.execution.id,
            version: state.checkpoint.version + 1,
            aggregate_revision: state.execution.aggregate_revision,
            parent: Some(state.checkpoint.id),
            episode_id: Some(episode_id),
            integrity_digest: Some(digest),
        }
    }

    /// Requests a normal (non-failure) rotation and commits the resulting
    /// terminal Checkpoint for this Episode.
    async fn rotate_and_commit(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        reason: RotationReason,
    ) -> Result<AggregateState, ControllerError> {
        let state = self
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::RequestRotation { reason },
            )
            .await?;
        let checkpoint = self.build_checkpoint(&state, episode_id);
        self.commit(loop_id, lease, &state, checkpoint).await
    }

    /// Ensures the Loop is suspended (issuing [`LifecycleCommand::Suspend`]
    /// only if it is not already), then commits the resulting terminal
    /// Checkpoint for this Episode.
    ///
    /// Used by every non-happy-path exit: a runner failure, a hard context
    /// budget, a drain-time integrity mismatch, or a verification outcome
    /// that itself suspended the Loop. Some of those already suspend the
    /// Loop as part of the event they just applied (embedded in
    /// [`LifecycleCommand::RequestRotation`]'s runner-failure disposition,
    /// or in [`LifecycleCommand::RecordVerification`]'s suspended outcome);
    /// others do not (a runner that never even started, or a normal
    /// rotation whose drain turned out to be compromised). Checking rather
    /// than assuming keeps this one helper correct for both.
    async fn ensure_episode_terminal_and_commit(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        suspension_reason: SuspensionReason,
    ) -> Result<AggregateState, ControllerError> {
        let state = if state.execution.status == LoopStatus::Active {
            self.apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::Suspend {
                    reason: suspension_reason,
                },
            )
            .await?
        } else {
            state
        };
        let checkpoint = self.build_checkpoint(&state, episode_id);
        self.commit(loop_id, lease, &state, checkpoint).await
    }
}
