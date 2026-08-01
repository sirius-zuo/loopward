//! [`RecoveryManager`]: reconciliation and crash recovery after an
//! interrupted Loop advance (`docs/06-verification-and-recovery.md` §12–§13,
//! `docs/10-mvp-and-evaluation.md` §8).
//!
//! [`LoopController::advance`] performs at most one durable Episode
//! transition per call, and every external effect (hydration, spawning the
//! runner, verification) happens outside a database transaction. A process
//! that crashes mid-Episode leaves the durable projection with an Episode
//! that is neither terminal nor freshly startable —
//! [`ControllerError::RecoveryRequired`] is exactly `advance`'s refusal to
//! guess how to resume someone else's partial work. [`RecoveryManager`] is
//! that resumption: it inspects the latest committed Checkpoint, the current
//! aggregate projection, the active Episode/Session/Work Unit/Attempt, and
//! resource integrity, then drives the interrupted Episode to a safe
//! terminal boundary — reattaching to a runner that is still running,
//! collecting one that finished, retrying one that was never actually
//! called, or suspending the Loop as `Ambiguous` when the external effect's
//! outcome cannot be established. It never assumes an external operation
//! failed merely because Loopward did not record its result
//! (`docs/06-verification-and-recovery.md` §12) — the load-bearing
//! consequence of that principle is that retiring a dangling Attempt (this
//! module's [`Self::reconcile_without_active_episode`]) only ever happens on
//! a *confirmed* terminal report from the runner; anything else (`Running`,
//! `Suspended`, `Unknown`, a `reconcile` call that itself errors, or a runner
//! that does not support `reconcile` at all) leaves the Loop suspended and
//! everything untouched.
//!
//! # Reuse over duplication
//!
//! [`RecoveryManager`] wraps a [`LoopController`] and reuses its private
//! `decide -> evolve -> validate_invariants -> single fenced store write`
//! helpers (`apply`, `commit`, `rotate_and_commit`,
//! `ensure_episode_terminal_and_commit`, `mark_hydrated`,
//! `finish_execution_attempt`, `evaluate_verification`, `finalize_result`)
//! through loosened (`pub(crate)`) visibility rather than re-implementing the
//! same fencing and validation a second time. Every reconciling action this
//! module performs that already has a [`loopward_core::LifecycleCommand`]
//! goes through one of those helpers.
//!
//! A handful of transitions this module needs have **no** `LifecycleCommand`
//! at all — not because they are already-documented edges some command
//! simply never issues, but because `loopward_core`'s command surface never
//! reaches them: `AttemptStatus::Suspended -> Cancelled`,
//! `AttemptStatus::Suspended -> Failed`, the same two starting from
//! `AttemptStatus::Ambiguous`, and `WorkUnitStatus::Suspended -> Pending`
//! (or `-> AwaitingVerification`, once a dangling Attempt's result is
//! actually recovered). Checking `loopward_core::AttemptStatus::DOCUMENTED_EDGES`
//! directly: the *only* outgoing edge from `Suspended` it lists is
//! `Suspended -> Running`, and `WorkUnitStatus` has no edge table at all in
//! `loopward_core`. This is exactly the same category of gap
//! [`LoopController::resume`]/`cancel` already document and work around for
//! `LoopStatus::Suspended -> Active` (also absent from
//! `LoopStatus::DOCUMENTED_EDGES`): a direct, lease-fenced
//! [`crate::ports::LoopStore::commit_checkpoint`] write, validated by
//! [`validate_invariants`] before it lands — not a documented-but-uncommanded
//! transition, simply one with no command path whatsoever.

mod reconcile;

use loopward_api::ids::{AttemptId, LoopId};
use loopward_api::resource::ResourceRef;
use loopward_api::runner::{ReconcileOutcome, RunHandle, SuspensionReason};
use loopward_api::work_unit::WorkUnitResult;
use loopward_core::{
    AggregateState, AttemptStatus, Checkpoint, LoopEvent, WorkUnitAttempt, WorkUnitStatus,
    validate_invariants, work_unit_status_is_terminal,
};

use crate::HydrationRequest;
use crate::controller::{ControllerError, LoopController};
use crate::ports::{AcquireLease, CheckpointCommitRequest, LoopLease};

pub use reconcile::EpisodeRecoveryOutcome;

/// The result of one [`RecoveryManager::recover`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// Nothing needed reconciliation: the Loop was already terminal, had no
    /// active Episode, and no leftover unreconciled Attempt or Work Unit.
    NothingToRecover,
    /// A dangling Attempt (and/or a Work Unit left `Suspended` by a prior
    /// verification outcome) left behind by an Episode that already
    /// terminated was reconciled: the runner *confirmed* the external effect
    /// is over and unsuccessful (never started, or a confirmed cancellation
    /// or failure), so the Attempt was retired and the Work Unit returned to
    /// `Pending`, ready for a fresh Attempt.
    WorkUnitReadyForRetry,
    /// A dangling Attempt's collected result was recovered and validated
    /// (the runner confirmed it completed, and the result's pinned-integrity
    /// binding still matches what the Episode originally hydrated): the
    /// Attempt is now `Succeeded` with its result attached, and the Work
    /// Unit is `AwaitingVerification` — the next `advance` verifies it
    /// normally instead of the result being silently discarded.
    AttemptResultRecovered,
    /// Reconciliation could not establish that the runner's external effect
    /// is actually over (it reports still running or suspended, reports
    /// `Unknown`, does not support `reconcile` at all, or the `reconcile`
    /// call itself failed) and so could not safely retire it: nothing was
    /// changed, and the Loop remains suspended.
    StillSuspended {
        /// Why the Loop remains suspended.
        reason: SuspensionReason,
    },
    /// An interrupted Episode was fully reconciled and driven to a normal
    /// completed boundary (the runner turned out to have completed, or
    /// completed once reattached).
    EpisodeRecovered {
        /// The Episode that completed.
        episode_id: loopward_api::ids::EpisodeId,
        /// The Checkpoint version it committed.
        checkpoint_version: u64,
    },
    /// An interrupted Episode reattached to a runner that is still
    /// genuinely executing: nothing was forced, and the Episode remains
    /// open for a later `recover` call to try again.
    Reattached,
    /// An interrupted Episode was closed out as suspended (ambiguous
    /// external state, a confirmed runner failure, a resource integrity
    /// failure, or a preserved runner-side suspension).
    LoopSuspended {
        /// Why the Loop was suspended.
        reason: SuspensionReason,
    },
}

/// What a dangling Attempt's reconciliation established, once the runner
/// was actually asked (or, for a runner that cannot be asked at all, the
/// deliberately conservative default).
enum DanglingAttemptResolution {
    /// The runner cannot confirm the external effect is over: preserve
    /// everything exactly as it is.
    StillLive,
    /// The runner confirms the external effect is over and did not
    /// succeed: safe to retire the Attempt under the given terminal status
    /// and let a fresh Attempt be prepared.
    Retire(AttemptStatus),
    /// The runner confirms the run completed, and the result validated
    /// against the Episode's originally hydrated pinned state: safe to fold
    /// back in as a completed, awaiting-verification Attempt.
    Recovered(ResourceRef),
}

/// Reconciliation and crash recovery for one Loop.
///
/// See the module documentation for the recovery model and for how this
/// reuses [`LoopController`]'s own fenced mutation helpers.
#[derive(Clone)]
pub struct RecoveryManager {
    controller: LoopController,
}

impl std::fmt::Debug for RecoveryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryManager").finish_non_exhaustive()
    }
}

impl RecoveryManager {
    /// Builds a recovery manager over `controller`, reusing its ports,
    /// runner, policy, and configuration.
    #[must_use]
    pub fn new(controller: LoopController) -> Self {
        Self { controller }
    }

    /// The controller this recovery manager reconciles Episodes for.
    #[must_use]
    pub fn controller(&self) -> &LoopController {
        &self.controller
    }

    /// Reconciles and resumes `loop_id` after an interrupted `advance`.
    ///
    /// A no-op (returns [`RecoveryOutcome::NothingToRecover`]) when the Loop
    /// is already terminal, or when its projection is already durably
    /// consistent (no active Episode, no leftover unreconciled Attempt or
    /// suspended Work Unit).
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError::Store`] when the lease cannot be acquired
    /// — on lease loss this manager stops before making any local commit; it
    /// never claims the runner stopped and never cancels it merely because
    /// the lease was lost (`docs/10-mvp-and-evaluation.md` §8, Step 5) — and
    /// otherwise anything the reused controller helpers can return.
    pub async fn recover(&self, loop_id: LoopId) -> Result<RecoveryOutcome, ControllerError> {
        let lease = self
            .controller
            .ports()
            .store
            .acquire_lease(AcquireLease {
                loop_id,
                holder_id: self.controller.holder_id(),
                ttl: self.controller.lease_ttl(),
            })
            .await?;

        let stored = self.controller.ports().store.load(loop_id).await?;
        let state = stored.state;

        if state.execution.status.is_terminal() {
            return Ok(RecoveryOutcome::NothingToRecover);
        }

        if state.active_episode().is_none() {
            return self
                .reconcile_without_active_episode(loop_id, &lease, state)
                .await;
        }

        match Box::pin(self.reconcile_active_episode(loop_id, &lease, state)).await? {
            EpisodeRecoveryOutcome::Reattached => Ok(RecoveryOutcome::Reattached),
            EpisodeRecoveryOutcome::Completed {
                episode_id,
                checkpoint_version,
            } => Ok(RecoveryOutcome::EpisodeRecovered {
                episode_id,
                checkpoint_version,
            }),
            EpisodeRecoveryOutcome::Suspended { reason } => {
                Ok(RecoveryOutcome::LoopSuspended { reason })
            }
        }
    }

    /// Reconciles a Loop whose most recent Episode already terminated
    /// (durably committed) but left the projection with an unreconciled
    /// Attempt and/or a Work Unit stuck at [`WorkUnitStatus::Suspended`].
    ///
    /// Both are the same underlying gap, left deliberately open by Task 9:
    /// [`WorkUnitStatus::Suspended`] has no `LifecycleCommand` that clears
    /// it (a `VerificationOutcome::Suspended` outcome is the only way to
    /// reach it, and nothing un-reaches it), and a non-terminal or
    /// `Ambiguous` Attempt left behind by a mid-Attempt suspend or a
    /// confirmed runner failure blocks every future
    /// `StartEpisode`/`StartSession` via the reducer's own
    /// `attempt.reconciled_before_next_generation` invariant — that
    /// invariant treats `Ambiguous` as unreconciled *even though it is
    /// terminal*, so an `Ambiguous` Attempt is just as much a dead end here
    /// as a dangling `Suspended` one, and gets the identical treatment.
    ///
    /// Recovery asks the runner before assuming anything is safe to retire:
    /// only a *confirmed* terminal report (the runner never received it, or
    /// definitely cancelled/failed/completed it) is acted on. `Unknown`, a
    /// `reconcile` call that itself errors, a runner that does not support
    /// `reconcile` at all, or a report that the effect may still be live
    /// leave everything untouched and the Loop suspended.
    async fn reconcile_without_active_episode(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: AggregateState,
    ) -> Result<RecoveryOutcome, ControllerError> {
        let unreconciled_attempt = state.open_attempt().cloned().or_else(|| {
            state
                .attempt
                .as_ref()
                .filter(|attempt| attempt.status == AttemptStatus::Ambiguous)
                .cloned()
        });
        let work_unit_stuck = state
            .work_unit
            .as_ref()
            .is_some_and(|work_unit| work_unit.status == WorkUnitStatus::Suspended);

        if unreconciled_attempt.is_none() && !work_unit_stuck {
            return Ok(RecoveryOutcome::NothingToRecover);
        }

        if let Some(attempt) = &unreconciled_attempt {
            return match self.reconcile_dangling_attempt(&state, attempt).await? {
                DanglingAttemptResolution::StillLive => Ok(RecoveryOutcome::StillSuspended {
                    reason: SuspensionReason::Recovery,
                }),
                DanglingAttemptResolution::Retire(status) => {
                    self.retire_dangling_attempt(loop_id, lease, state, attempt.id, status)
                        .await
                }
                DanglingAttemptResolution::Recovered(result_ref) => {
                    self.recover_dangling_result(loop_id, lease, state, attempt.id, result_ref)
                        .await
                }
            };
        }

        self.unstick_work_unit(loop_id, lease, state).await
    }

    /// Asks the runner what actually happened to a dangling Attempt,
    /// mapping its answer to the one safe disposition
    /// (`docs/06-verification-and-recovery.md` §13, the brief's
    /// `ReconcileOutcome` table). A runner that does not declare
    /// `supports_reconcile` is never asked at all — there would be nothing
    /// truthful for it to report — and is treated the same as an
    /// unresolvable answer: still possibly live.
    async fn reconcile_dangling_attempt(
        &self,
        state: &AggregateState,
        attempt: &WorkUnitAttempt,
    ) -> Result<DanglingAttemptResolution, ControllerError> {
        let capabilities = self.controller.runner().capabilities().await?;
        if !capabilities.supports_reconcile {
            return Ok(DanglingAttemptResolution::StillLive);
        }

        let handle = RunHandle {
            attempt_id: attempt.id,
            external_execution_ref: attempt.external_execution_ref.clone(),
        };
        match self.controller.runner().reconcile(&handle).await {
            // Positive confirmation nothing ever started, or that it was
            // cancelled: safe to retire and let a fresh Attempt run.
            Ok(ReconcileOutcome::NotStarted | ReconcileOutcome::Cancelled) => {
                Ok(DanglingAttemptResolution::Retire(AttemptStatus::Cancelled))
            }
            // A confirmed failure is just as safe to retire as a confirmed
            // cancellation — the external effect is over either way.
            Ok(ReconcileOutcome::Failed) => {
                Ok(DanglingAttemptResolution::Retire(AttemptStatus::Failed))
            }
            // Confirmed complete: collect and validate rather than
            // discarding the result — but only trust it if it survives the
            // same pinned-integrity binding a live drain would have
            // enforced. Any failure to collect or validate falls back to
            // the conservative default: leave it alone.
            Ok(ReconcileOutcome::Completed) => {
                let Ok(collected) = self.controller.runner().collect(&handle).await else {
                    return Ok(DanglingAttemptResolution::StillLive);
                };
                match self.validate_dangling_result(state, &collected).await {
                    Ok(Some(result_ref)) => Ok(DanglingAttemptResolution::Recovered(result_ref)),
                    Ok(None) | Err(_) => Ok(DanglingAttemptResolution::StillLive),
                }
            }
            // Still running, itself suspended, unknowable, or the
            // `reconcile` call itself failed: none of these confirm the
            // effect is over, so none of these are safe to act on.
            Ok(
                ReconcileOutcome::Running | ReconcileOutcome::Suspended | ReconcileOutcome::Unknown,
            )
            | Err(_) => Ok(DanglingAttemptResolution::StillLive),
        }
    }

    /// Validates a dangling Attempt's freshly collected result against the
    /// Episode's *originally* hydrated manifest (resolved from its durably
    /// recorded `hydrated_manifest` reference, not re-derived) before
    /// trusting it, mirroring the drain-time pinned-integrity re-check a
    /// live Episode performs. Returns `Ok(None)` (never trust it) rather
    /// than propagating an error for anything that keeps this from being
    /// confirmed safe — a transient resource-backend failure is exactly as
    /// untrustworthy here as tampered pinned content.
    async fn validate_dangling_result(
        &self,
        state: &AggregateState,
        collected: &WorkUnitResult,
    ) -> Result<Option<ResourceRef>, ControllerError> {
        let Some(episode) = &state.episode else {
            return Ok(None);
        };
        let Some(hydrated_manifest_ref) = &episode.hydrated_manifest else {
            return Ok(None);
        };
        let Some(work_unit) = &state.work_unit else {
            return Ok(None);
        };

        let Ok(original_bytes) = self
            .controller
            .ports()
            .resources
            .resolve(hydrated_manifest_ref)
            .await
        else {
            return Ok(None);
        };
        let Ok(original_manifest) =
            serde_json::from_slice::<loopward_api::context::BootPackageManifest>(&original_bytes)
        else {
            return Ok(None);
        };

        let session_generation = state
            .session
            .as_ref()
            .map_or_else(|| state.latest_generation(), |session| session.generation);
        let capabilities = self.controller.runner().capabilities().await?;
        let budget = self
            .controller
            .resolve_context_budget(work_unit.budget.context);
        let hydration_request = HydrationRequest {
            checkpoint: state.checkpoint.clone(),
            episode_id: episode.id,
            session_generation,
            work_unit: work_unit.clone(),
            capabilities,
            budget,
            context_rule_version: self.controller.context_rule_version().to_owned(),
            candidates: self
                .controller
                .candidates()
                .candidates(work_unit, &state.checkpoint),
        };
        let Ok(redrawn) = self.controller.hydrator().hydrate(hydration_request).await else {
            return Ok(None);
        };
        if redrawn.manifest.pinned_digest != original_manifest.pinned_digest {
            return Ok(None);
        }

        let result_ref = self.controller.finalize_result(collected).await?;
        Ok(Some(result_ref))
    }

    /// Retires a confirmed-dead Attempt as `status` and, if the Work Unit
    /// was left `Suspended`, returns it to `Pending`, ready for a fresh
    /// Attempt.
    async fn retire_dangling_attempt(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        mut state: AggregateState,
        attempt_id: AttemptId,
        status: AttemptStatus,
    ) -> Result<RecoveryOutcome, ControllerError> {
        if let Some(attempt) = state.attempt.as_mut()
            && attempt.id == attempt_id
        {
            attempt.status = status;
            attempt.result_ref = None;
        }
        if let Some(work_unit) = state.work_unit.as_mut()
            && !work_unit_status_is_terminal(work_unit.status)
        {
            work_unit.status = WorkUnitStatus::Pending;
        }
        self.commit_raw_projection(loop_id, lease, state).await?;
        Ok(RecoveryOutcome::WorkUnitReadyForRetry)
    }

    /// Folds a dangling Attempt's validated, collected result back in as a
    /// completed, awaiting-verification Attempt rather than discarding it.
    async fn recover_dangling_result(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        mut state: AggregateState,
        attempt_id: AttemptId,
        result_ref: ResourceRef,
    ) -> Result<RecoveryOutcome, ControllerError> {
        if let Some(attempt) = state.attempt.as_mut()
            && attempt.id == attempt_id
        {
            attempt.status = AttemptStatus::Succeeded;
            attempt.result_ref = Some(result_ref);
        }
        if let Some(work_unit) = state.work_unit.as_mut()
            && !work_unit_status_is_terminal(work_unit.status)
        {
            work_unit.status = WorkUnitStatus::AwaitingVerification;
        }
        self.commit_raw_projection(loop_id, lease, state).await?;
        Ok(RecoveryOutcome::AttemptResultRecovered)
    }

    /// Returns a Work Unit stuck at [`WorkUnitStatus::Suspended`] (with no
    /// dangling Attempt of its own — that case is handled by
    /// [`Self::retire_dangling_attempt`]) to `Pending`.
    async fn unstick_work_unit(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        mut state: AggregateState,
    ) -> Result<RecoveryOutcome, ControllerError> {
        if let Some(work_unit) = state.work_unit.as_mut()
            && work_unit.status == WorkUnitStatus::Suspended
        {
            work_unit.status = WorkUnitStatus::Pending;
        }
        self.commit_raw_projection(loop_id, lease, state).await?;
        Ok(RecoveryOutcome::WorkUnitReadyForRetry)
    }

    /// Directly commits `state` (already mutated by the caller into its
    /// reconciled shape) through
    /// [`crate::ports::LoopStore::commit_checkpoint`] rather than
    /// `decide`/`evolve` — see the module documentation for why this is the
    /// correct tool for the transitions this module needs. Mints a fresh
    /// Checkpoint purely to satisfy `commit_checkpoint`'s requirement of a
    /// monotonic version and a matching `CheckpointCommitted` event, and
    /// validates the result with [`validate_invariants`] before writing it —
    /// a state loaded from storage, rebuilt by recovery, or produced by a
    /// future command path is checked by the same predicate, so this cannot
    /// bypass an invariant by inventing a new transition.
    async fn commit_raw_projection(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        mut state: AggregateState,
    ) -> Result<(), ControllerError> {
        let expected_revision = state.execution.aggregate_revision;
        let expected_checkpoint_version = state.execution.checkpoint_version;

        let checkpoint = Checkpoint {
            id: self.controller.ports().ids.new_checkpoint_id(),
            loop_id,
            version: state.checkpoint.version + 1,
            aggregate_revision: expected_revision + 1,
            parent: Some(state.checkpoint.id),
            episode_id: None,
            integrity_digest: None,
        };
        state.execution.aggregate_revision = expected_revision + 1;
        state.execution.checkpoint_version = checkpoint.version;
        state.checkpoint = checkpoint.clone();
        validate_invariants(&state)?;

        self.controller
            .ports()
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
}
