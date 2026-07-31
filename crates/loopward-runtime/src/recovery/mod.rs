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
//! (`docs/06-verification-and-recovery.md` §12).
//!
//! # Reuse over duplication
//!
//! [`RecoveryManager`] wraps a [`LoopController`] and reuses its private
//! `decide -> evolve -> validate_invariants -> single fenced store write`
//! helpers (`apply`, `commit`, `rotate_and_commit`,
//! `ensure_episode_terminal_and_commit`, `mark_hydrated`,
//! `finish_execution_attempt`, `evaluate_verification`) through loosened
//! (`pub(crate)`) visibility rather than re-implementing the same fencing
//! and validation a second time. Every reconciling action this module
//! performs that already has a [`loopward_core::LifecycleCommand`] goes
//! through one of those helpers; a handful of transitions
//! `loopward_core::model::AttemptStatus`'s own documented edge table allows
//! but no `LifecycleCommand` currently produces (`Suspended -> Active` is
//! the analogous, already-established precedent: see
//! [`LoopController::resume`]) are performed as direct, lease-fenced
//! [`crate::ports::LoopStore::commit_checkpoint`] writes, exactly mirroring
//! how `resume`/`cancel` already handle the same category of gap.

mod reconcile;

use loopward_api::ids::LoopId;
use loopward_api::runner::SuspensionReason;
use loopward_core::{
    AggregateState, AttemptStatus, Checkpoint, LoopEvent, WorkUnitStatus, validate_invariants,
    work_unit_status_is_terminal,
};

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
    /// terminated was reconciled: the Attempt was retired as `Cancelled`
    /// and the Work Unit returned to `Pending`, ready for a fresh Attempt.
    WorkUnitReadyForRetry,
    /// Reconciliation found the runner's external effect may still be live
    /// (still running, or itself suspended) and could not be safely
    /// retired: nothing was changed, and the Loop remains suspended.
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
    /// reach it, and nothing un-reaches it), and a non-terminal Attempt left
    /// behind by [`LoopStatus::Suspended`][loopward_core::LoopStatus::Suspended]
    /// (`Running -> Suspended` via a mid-Attempt suspend) blocks every
    /// future `StartEpisode`/`StartSession` via the reducer's own
    /// `attempt.reconciled_before_next_generation` invariant. Recovery asks
    /// the runner before assuming either is safe to retire: if the runner
    /// reports the external effect may still be live, nothing is touched
    /// and the Loop remains suspended.
    async fn reconcile_without_active_episode(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: AggregateState,
    ) -> Result<RecoveryOutcome, ControllerError> {
        let open_attempt = state.open_attempt().cloned();
        let work_unit_stuck = state
            .work_unit
            .as_ref()
            .is_some_and(|work_unit| work_unit.status == WorkUnitStatus::Suspended);

        if open_attempt.is_none() && !work_unit_stuck {
            return Ok(RecoveryOutcome::NothingToRecover);
        }

        if let Some(attempt) = &open_attempt {
            let handle = loopward_api::runner::RunHandle {
                attempt_id: attempt.id,
                external_execution_ref: attempt.external_execution_ref.clone(),
            };
            let reconciled = self.controller.runner().reconcile(&handle).await;
            let still_live = matches!(
                reconciled,
                Ok(loopward_api::runner::ReconcileOutcome::Running
                    | loopward_api::runner::ReconcileOutcome::Suspended)
            );
            if still_live {
                // The external effect may still be running (or itself
                // suspended); starting a fresh Attempt for the same Work
                // Unit now would risk an unsafe concurrent repeat. Leave
                // everything exactly as it is.
                return Ok(RecoveryOutcome::StillSuspended {
                    reason: SuspensionReason::Recovery,
                });
            }
        }

        self.retire_dangling_state(
            loop_id,
            lease,
            state,
            open_attempt.map(|attempt| attempt.id),
            work_unit_stuck,
        )
        .await
    }

    /// Directly writes a reconciled projection retiring `attempt_id` (when
    /// present) as [`AttemptStatus::Cancelled`] and/or returning a stuck
    /// Work Unit to [`WorkUnitStatus::Pending`], through
    /// [`crate::ports::LoopStore::commit_checkpoint`] rather than
    /// `decide`/`evolve`.
    ///
    /// Both transitions (`AttemptStatus::Suspended -> Cancelled`,
    /// `WorkUnitStatus::Suspended -> Pending`) have no
    /// [`loopward_core::LifecycleCommand`] today — exactly the same gap
    /// `LoopController::resume` and `LoopController::cancel` already
    /// document and work around for `LoopStatus::Suspended -> Active`. This
    /// mints a fresh Checkpoint purely to satisfy
    /// [`crate::ports::LoopStore::commit_checkpoint`]'s requirement of a
    /// monotonic version and a matching `CheckpointCommitted` event, exactly
    /// mirroring that precedent, and validates the result with
    /// [`validate_invariants`] before writing it — a state loaded from
    /// storage, rebuilt by recovery, or produced by a future command path is
    /// checked by the same predicate, so this cannot bypass an invariant by
    /// inventing a new transition.
    async fn retire_dangling_state(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        mut state: AggregateState,
        attempt_id: Option<loopward_api::ids::AttemptId>,
        work_unit_stuck: bool,
    ) -> Result<RecoveryOutcome, ControllerError> {
        let expected_revision = state.execution.aggregate_revision;
        let expected_checkpoint_version = state.execution.checkpoint_version;

        if let Some(attempt_id) = attempt_id
            && let Some(attempt) = state.attempt.as_mut()
            && attempt.id == attempt_id
        {
            attempt.status = AttemptStatus::Cancelled;
            attempt.result_ref = None;
        }
        if work_unit_stuck
            && let Some(work_unit) = state.work_unit.as_mut()
            && !work_unit_status_is_terminal(work_unit.status)
        {
            work_unit.status = WorkUnitStatus::Pending;
        }

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
        Ok(RecoveryOutcome::WorkUnitReadyForRetry)
    }
}
