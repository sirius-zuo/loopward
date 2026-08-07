//! Reconciliation of one in-flight (non-terminal) Episode: the heart of
//! [`super::RecoveryManager::recover`].
//!
//! Every branch below dispatches on exactly which durable fact was last
//! recorded (`episode.status`, the Session, the Attempt, the Work Unit) —
//! the only granularity a crash can actually interrupt at, since
//! [`crate::ports::LoopStore::append`] and
//! [`crate::ports::LoopStore::commit_checkpoint`] each commit their whole
//! event batch atomically (`docs/03-durable-state-and-transactions.md` §10).
//! `AttemptStatus::Starting` and `AttemptStatus::Collecting` never persist as
//! observable states for the same reason: `loopward_core::reducer`'s
//! `record_runner_started`/`record_result` always pair them with the event
//! that follows in one atomic append.
//!
//! Mapping to `docs/10-mvp-and-evaluation.md` §8's eight failure points:
//!
//! ```text
//! 1. before runner start              -> Attempt `Prepared`, reconcile -> NotStarted
//! 2. after external start before      -> Attempt `Prepared`, reconcile -> Running/Completed
//!    handle persistence                  (ambiguous locally; only the runner can tell)
//! 3. during execution                 -> Attempt `Running`, reconcile -> Running/Completed
//! 4. after result materialization     -> Attempt `Running`, reconcile -> Completed
//! 5. during verification              -> Episode kind `Verification`, `verification.completed`
//!                                         not yet recorded
//! 6. before Checkpoint transaction    -> the terminal decision (`RecordResult` /
//!                                         `RecordVerification`) is recorded but
//!                                         `RequestRotation` is not yet applied
//! 7. after Checkpoint commit before    -> no active Episode at all: `recover` is a
//!    acknowledgement                     no-op, see `super::RecoveryManager::recover`
//! 8. during Session drain             -> `RequestRotation` is recorded (Episode
//!                                         `Draining`) but the Checkpoint is not
//! ```

use loopward_api::context::BootPackageManifest;
use loopward_api::ids::EpisodeId;
use loopward_api::resource::{OpaqueRef, ResourceRef};
use loopward_api::runner::{ReconcileOutcome, RunHandle, SuspensionReason};
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{WorkUnit, WorkUnitRequest, WorkUnitResult};
use loopward_core::{
    AggregateState, AttemptStatus, EpisodeKind, EpisodeStatus, LifecycleCommand, LoopStatus,
    RotationReason, WorkUnitAttempt, WorkUnitStatus,
};

use crate::HydrationRequest;
use crate::context::resource_grants_for_manifest;
use crate::controller::{ControllerError, OperationOutcome};
use crate::ports::LoopLease;

use super::RecoveryManager;

/// The result of reconciling one active (non-terminal) Episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpisodeRecoveryOutcome {
    /// The Episode reached a normal completed boundary.
    Completed {
        /// The Episode that completed.
        episode_id: EpisodeId,
        /// The Checkpoint version it committed.
        checkpoint_version: u64,
    },
    /// The runner is still genuinely executing: nothing was forced, and the
    /// Episode remains open for a later `recover` call.
    Reattached,
    /// The Episode was closed out as suspended.
    Suspended {
        /// Why it was suspended.
        reason: SuspensionReason,
    },
}

impl RecoveryManager {
    /// Reconciles the Loop's current active Episode to a safe boundary.
    pub(crate) async fn reconcile_active_episode(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        if let Some(outcome) = self
            .check_resource_integrity(loop_id, lease, &state)
            .await?
        {
            return Ok(outcome);
        }

        let episode = state.episode.clone().ok_or_else(|| {
            ControllerError::Invariant(
                "recovering an Episode absent from the projection".to_owned(),
            )
        })?;
        let episode_id = episode.id;

        match episode.status {
            EpisodeStatus::Preparing | EpisodeStatus::Hydrating => {
                Box::pin(self.recover_before_session(loop_id, lease, state, episode_id)).await
            }
            EpisodeStatus::Recovering => {
                self.controller
                    .ensure_episode_terminal_and_commit(
                        loop_id,
                        lease,
                        state,
                        episode_id,
                        SuspensionReason::Recovery,
                    )
                    .await?;
                Ok(EpisodeRecoveryOutcome::Suspended {
                    reason: SuspensionReason::Recovery,
                })
            }
            EpisodeStatus::Executing => {
                Box::pin(self.recover_executing(loop_id, lease, state, episode_id, episode.kind))
                    .await
            }
            EpisodeStatus::Draining => {
                self.recover_draining(loop_id, lease, state, episode_id)
                    .await
            }
            other => Err(ControllerError::Invariant(format!(
                "recovery observed Episode {episode_id} in an unexpected in-flight status `{}`",
                other.label()
            ))),
        }
    }

    /// Resolves every resource an in-flight Episode currently references
    /// (the hydrated Boot Package manifest, the Session's boot manifest, the
    /// Work Unit's declared inputs, the Attempt's Boot Package, and its
    /// collected result, when present) and suspends the Loop rather than
    /// proceeding if any can no longer be resolved.
    async fn check_resource_integrity(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: &AggregateState,
    ) -> Result<Option<EpisodeRecoveryOutcome>, ControllerError> {
        let mut refs: Vec<ResourceRef> = Vec::new();
        if let Some(episode) = &state.episode
            && let Some(manifest_ref) = &episode.hydrated_manifest
        {
            refs.push(manifest_ref.clone());
        }
        if let Some(session) = &state.session {
            refs.push(session.boot_manifest_ref.clone());
        }
        if let Some(work_unit) = &state.work_unit {
            refs.extend(work_unit.inputs.iter().cloned());
        }
        if let Some(attempt) = &state.attempt {
            refs.push(attempt.boot_package_ref.clone());
            if let Some(result_ref) = &attempt.result_ref {
                refs.push(result_ref.clone());
            }
        }

        let mut broken = false;
        for reference in &refs {
            if self
                .controller
                .ports()
                .resources
                .resolve(reference)
                .await
                .is_err()
            {
                broken = true;
                break;
            }
        }
        if !broken {
            return Ok(None);
        }

        let episode_id = state
            .episode
            .as_ref()
            .map(|episode| episode.id)
            .ok_or_else(|| {
                ControllerError::Invariant(
                    "resource integrity check found no active Episode to suspend".to_owned(),
                )
            })?;
        let state = self
            .controller
            .apply(
                loop_id,
                lease,
                state,
                LifecycleCommand::Suspend {
                    reason: SuspensionReason::Recovery,
                },
            )
            .await?;
        self.controller
            .ensure_episode_terminal_and_commit(
                loop_id,
                lease,
                state,
                episode_id,
                SuspensionReason::Recovery,
            )
            .await?;
        Ok(Some(EpisodeRecoveryOutcome::Suspended {
            reason: SuspensionReason::Recovery,
        }))
    }

    /// A crash before `session.started` committed: `context.hydrated` may or
    /// may not already be recorded (`Preparing` vs. `Hydrating`), but no
    /// Runtime Session exists yet. Redoes hydration only if it is missing
    /// (re-applying `MarkHydrated` when it already succeeded would be
    /// rejected by the reducer's own `Preparing | Recovering` precondition),
    /// then starts the Session and continues into the Executing phase.
    async fn recover_before_session(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        let work_unit = state
            .work_unit
            .clone()
            .ok_or(ControllerError::NoWorkUnit(loop_id))?;
        let session_generation = state.latest_generation() + 1;
        // Captured before any further reconciliation reassigns `state`:
        // `episode.kind` never changes across `MarkHydrated`/`StartSession`,
        // but a Verification Episode that crashes this early (a real
        // window: `advance` applies `StartEpisode`, hydrates, then
        // `StartSession`, all in one call) must still be recovered as one —
        // hardcoding `Execution` here would durably misattribute its
        // rotation reason.
        let kind = state
            .episode
            .as_ref()
            .map(|episode| episode.kind)
            .ok_or_else(|| {
                ControllerError::Invariant(
                    "recovering an Episode with no kind on record".to_owned(),
                )
            })?;

        let needs_hydration = state
            .episode
            .as_ref()
            .is_some_and(|episode| episode.hydrated_manifest.is_none());
        let state = if needs_hydration {
            let capabilities = self.controller.runner().capabilities().await?;
            let budget = self
                .controller
                .resolve_context_budget(work_unit.budget.context);
            let hydration_request = HydrationRequest {
                checkpoint: state.checkpoint.clone(),
                episode_id,
                session_generation,
                work_unit: work_unit.clone(),
                capabilities,
                budget,
                context_rule_version: self.controller.context_rule_version().to_owned(),
                candidates: self
                    .controller
                    .candidates()
                    .candidates(&work_unit, &state.checkpoint),
            };
            let report = self
                .controller
                .hydrator()
                .hydrate(hydration_request)
                .await?;
            self.controller
                .mark_hydrated(loop_id, lease, &state, &report)
                .await?
        } else {
            state
        };

        let session_id = self.controller.ports().ids.new_session_id();
        let state = self
            .controller
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

        Box::pin(self.recover_executing(loop_id, lease, state, episode_id, kind)).await
    }

    /// The Episode's Runtime Session is live (`Executing`); dispatches on
    /// what kind of Episode this is.
    async fn recover_executing(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        kind: EpisodeKind,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        let work_unit = state
            .work_unit
            .clone()
            .ok_or(ControllerError::NoWorkUnit(loop_id))?;
        if kind == EpisodeKind::Verification {
            self.recover_verification(loop_id, lease, state, episode_id, work_unit)
                .await
        } else {
            Box::pin(self.recover_execution(loop_id, lease, state, episode_id, work_unit)).await
        }
    }

    /// An Execution Episode with a live Session. Dispatches on the current
    /// Attempt, if any.
    async fn recover_execution(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        work_unit: WorkUnit,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        match state.attempt.clone() {
            None => {
                // The Session started, but `attempt.prepared` never
                // committed: nothing has been attempted yet at all.
                let attempt_id = self.controller.ports().ids.new_attempt_id();
                let idempotency_key = format!("attempt:{attempt_id}");
                let state = self
                    .controller
                    .apply(
                        loop_id,
                        lease,
                        &state,
                        LifecycleCommand::PrepareAttempt {
                            attempt_id,
                            idempotency_key,
                        },
                    )
                    .await?;
                Box::pin(self.recover_execution(loop_id, lease, state, episode_id, work_unit)).await
            }
            Some(attempt) if attempt.status == AttemptStatus::Succeeded => {
                // `attempt.completed` already committed; `rotation.requested`
                // is not (Episode status is still `Executing`) — failure
                // point 8 ("during Session drain") one step earlier than the
                // `Draining` case below.
                self.finish_rotation(
                    loop_id,
                    lease,
                    state,
                    episode_id,
                    RotationReason::WorkUnitBoundary,
                )
                .await
            }
            Some(attempt) if !attempt.status.is_terminal() => {
                Box::pin(self.reconcile_execution_attempt(
                    loop_id, lease, state, episode_id, work_unit, attempt,
                ))
                .await
            }
            Some(_) => {
                // Terminal but non-`Succeeded` (`Failed`/`Cancelled`/
                // `Ambiguous`) with the Episode never closed out: no
                // graceful path leaves this behind, but close it
                // conservatively rather than getting stuck.
                self.controller
                    .ensure_episode_terminal_and_commit(
                        loop_id,
                        lease,
                        state,
                        episode_id,
                        SuspensionReason::Recovery,
                    )
                    .await?;
                Ok(EpisodeRecoveryOutcome::Suspended {
                    reason: SuspensionReason::Recovery,
                })
            }
        }
    }

    /// Reconciles a non-terminal Attempt (`Prepared` with no observed
    /// external reference, or `Running` with one) against the runner's own
    /// account of it, per `docs/06-verification-and-recovery.md` §13 and the
    /// brief's `ReconcileOutcome` mapping. Never assumes the external effect
    /// failed merely because Loopward did not record its result.
    async fn reconcile_execution_attempt(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        work_unit: WorkUnit,
        attempt: WorkUnitAttempt,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        let handle = RunHandle {
            attempt_id: attempt.id,
            external_execution_ref: attempt.external_execution_ref.clone(),
        };
        let reconciled = self.controller.runner().reconcile(&handle).await;

        match reconciled {
            // Intent recorded, runner never called: safe to start under the
            // same idempotency key (failure point 1).
            Ok(ReconcileOutcome::NotStarted) => {
                Box::pin(
                    self.start_and_finish(loop_id, lease, state, episode_id, work_unit, attempt),
                )
                .await
            }
            // The runner is actively executing (or claims it already
            // finished, ambiguous vs. failure point 1 until asked — failure
            // point 2): reattach if we never durably recorded the external
            // reference, then try to observe it.
            Ok(ReconcileOutcome::Running) => {
                let (state, handle) = self
                    .ensure_reattached(loop_id, lease, state, &attempt, handle)
                    .await?;
                match self.controller.runner().collect(&handle).await {
                    Ok(collected) => {
                        Box::pin(self.finish_collected(
                            loop_id, lease, state, episode_id, work_unit, collected,
                        ))
                        .await
                    }
                    // Genuinely still running: leave it open rather than
                    // forcing a terminal outcome. A later `recover` call
                    // tries again.
                    Err(_) => Ok(EpisodeRecoveryOutcome::Reattached),
                }
            }
            Ok(ReconcileOutcome::Completed) => {
                let (state, handle) = self
                    .ensure_reattached(loop_id, lease, state, &attempt, handle)
                    .await?;
                let collected = self.controller.runner().collect(&handle).await?;
                Box::pin(
                    self.finish_collected(loop_id, lease, state, episode_id, work_unit, collected),
                )
                .await
            }
            // The runner itself suspended the run: preserve that suspension
            // rather than treating it as a failure.
            Ok(ReconcileOutcome::Suspended) => {
                let state = self
                    .controller
                    .apply(
                        loop_id,
                        lease,
                        &state,
                        LifecycleCommand::Suspend {
                            reason: SuspensionReason::Recovery,
                        },
                    )
                    .await?;
                self.controller
                    .ensure_episode_terminal_and_commit(
                        loop_id,
                        lease,
                        state,
                        episode_id,
                        SuspensionReason::Recovery,
                    )
                    .await?;
                Ok(EpisodeRecoveryOutcome::Suspended {
                    reason: SuspensionReason::Recovery,
                })
            }
            Ok(ReconcileOutcome::Failed | ReconcileOutcome::Cancelled) => {
                // Confirmed terminal: the external effect is over. Record
                // the actual status *before* requesting rotation, so the
                // reducer's `apply_rotation_requested` (whose `other =>
                // other` arm preserves an already-terminal status) does not
                // fall back to its conservative `Ambiguous` mapping for a
                // pre-crash `Running`/`Prepared` attempt — that mapping
                // exists for effects that genuinely cannot be observed,
                // which this is not. `Failed`/`Cancelled` is not blocking
                // for `require_reconciled_attempt`, so this self-heals on
                // the next `recover()` the same way a dangling Attempt
                // reconciled after Episode termination already does.
                let confirmed_status = if matches!(reconciled, Ok(ReconcileOutcome::Cancelled)) {
                    loopward_core::AttemptStatus::Cancelled
                } else {
                    loopward_core::AttemptStatus::Failed
                };
                let mut state = state;
                if let Some(recorded) = state.attempt.as_mut()
                    && recorded.id == attempt.id
                {
                    recorded.status = confirmed_status;
                    recorded.result_ref = None;
                }
                let state = self
                    .controller
                    .apply(
                        loop_id,
                        lease,
                        &state,
                        LifecycleCommand::RequestRotation {
                            reason: RotationReason::RunnerFailure,
                        },
                    )
                    .await?;
                self.controller
                    .ensure_episode_terminal_and_commit(
                        loop_id,
                        lease,
                        state,
                        episode_id,
                        SuspensionReason::Recovery,
                    )
                    .await?;
                Ok(EpisodeRecoveryOutcome::Suspended {
                    reason: SuspensionReason::Recovery,
                })
            }
            // `Unknown`, or the reconcile call itself failing: neither
            // confirms the external effect is over, so neither can be
            // safely distinguished from an unobservable effect — this is
            // the case the reducer's `Ambiguous` mapping exists for.
            Ok(ReconcileOutcome::Unknown) | Err(_) => {
                let state = self
                    .controller
                    .apply(
                        loop_id,
                        lease,
                        &state,
                        LifecycleCommand::RequestRotation {
                            reason: RotationReason::RunnerFailure,
                        },
                    )
                    .await?;
                self.controller
                    .ensure_episode_terminal_and_commit(
                        loop_id,
                        lease,
                        state,
                        episode_id,
                        SuspensionReason::Recovery,
                    )
                    .await?;
                Ok(EpisodeRecoveryOutcome::Suspended {
                    reason: SuspensionReason::Recovery,
                })
            }
        }
    }

    /// Durably records `attempt.started` if it was not already observed
    /// (the Attempt is `Prepared` with no external reference), synthesizing
    /// a reference the same way [`crate::controller::LoopController`]'s own
    /// normal path does when a runner does not hand one back.
    async fn ensure_reattached(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        attempt: &WorkUnitAttempt,
        handle: RunHandle,
    ) -> Result<(AggregateState, RunHandle), ControllerError> {
        if attempt.external_execution_ref.is_some() {
            return Ok((state, handle));
        }
        let external_ref = OpaqueRef::new(format!("attempt:{}", attempt.id));
        let state = self
            .controller
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::RecordRunnerStarted {
                    external_ref: external_ref.clone(),
                },
            )
            .await?;
        Ok((
            state,
            RunHandle {
                attempt_id: handle.attempt_id,
                external_execution_ref: Some(external_ref),
            },
        ))
    }

    /// The runner never received this Attempt at all: safe to start it
    /// fresh under the same idempotency key, rebuilding the exact request a
    /// normal Execution Episode would have sent.
    async fn start_and_finish(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        work_unit: WorkUnit,
        attempt: WorkUnitAttempt,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        let session = state.session.clone().ok_or_else(|| {
            ControllerError::Invariant("recovering an Attempt with no Runtime Session".to_owned())
        })?;
        let manifest_bytes = self
            .controller
            .ports()
            .resources
            .resolve(&attempt.boot_package_ref)
            .await?;
        let boot_package: BootPackageManifest = serde_json::from_slice(&manifest_bytes)?;
        let resource_grants = resource_grants_for_manifest(work_unit.id, &boot_package)
            .into_iter()
            .collect();

        let request = WorkUnitRequest {
            protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
            loop_id,
            episode_id,
            session_id: session.id,
            session_generation: session.generation,
            work_unit: work_unit.clone(),
            attempt_id: attempt.id,
            idempotency_key: attempt.idempotency_key.clone(),
            boot_package,
            resource_grants,
            output_contract: work_unit.output_contract.clone(),
            budget: work_unit.budget,
            deadline: None,
            trace_context: TraceContext::default(),
        };

        let Ok(handle) = self.controller.runner().start(request).await else {
            let state = self
                .controller
                .apply(
                    loop_id,
                    lease,
                    &state,
                    LifecycleCommand::RequestRotation {
                        reason: RotationReason::RunnerFailure,
                    },
                )
                .await?;
            self.controller
                .ensure_episode_terminal_and_commit(
                    loop_id,
                    lease,
                    state,
                    episode_id,
                    SuspensionReason::Recovery,
                )
                .await?;
            return Ok(EpisodeRecoveryOutcome::Suspended {
                reason: SuspensionReason::Recovery,
            });
        };

        let external_ref = handle
            .external_execution_ref
            .clone()
            .unwrap_or_else(|| OpaqueRef::new(format!("attempt:{}", attempt.id)));
        let state = self
            .controller
            .apply(
                loop_id,
                lease,
                &state,
                LifecycleCommand::RecordRunnerStarted { external_ref },
            )
            .await?;

        let Ok(collected) = self.controller.runner().collect(&handle).await else {
            let state = self
                .controller
                .apply(
                    loop_id,
                    lease,
                    &state,
                    LifecycleCommand::RequestRotation {
                        reason: RotationReason::RunnerFailure,
                    },
                )
                .await?;
            self.controller
                .ensure_episode_terminal_and_commit(
                    loop_id,
                    lease,
                    state,
                    episode_id,
                    SuspensionReason::Recovery,
                )
                .await?;
            return Ok(EpisodeRecoveryOutcome::Suspended {
                reason: SuspensionReason::Recovery,
            });
        };

        Box::pin(self.finish_collected(loop_id, lease, state, episode_id, work_unit, collected))
            .await
    }

    /// A result has been collected (freshly, or via reconciliation): redoes
    /// the drain-time pinned-integrity re-check against the *originally*
    /// hydrated manifest (resolved from the Episode's durably recorded
    /// `hydrated_manifest` reference, not re-derived) and finishes through
    /// [`crate::controller::LoopController::finish_execution_attempt`].
    async fn finish_collected(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        work_unit: WorkUnit,
        collected: WorkUnitResult,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        let hydrated_manifest_ref = state
            .episode
            .as_ref()
            .and_then(|episode| episode.hydrated_manifest.clone())
            .ok_or_else(|| {
                ControllerError::Invariant(
                    "finishing a collected Attempt with no hydrated Boot Package on record"
                        .to_owned(),
                )
            })?;
        let original_bytes = self
            .controller
            .ports()
            .resources
            .resolve(&hydrated_manifest_ref)
            .await?;
        let original_manifest: BootPackageManifest = serde_json::from_slice(&original_bytes)?;

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
            episode_id,
            session_generation,
            work_unit: work_unit.clone(),
            capabilities,
            budget,
            context_rule_version: self.controller.context_rule_version().to_owned(),
            candidates: self
                .controller
                .candidates()
                .candidates(&work_unit, &state.checkpoint),
        };

        let outcome = self
            .controller
            .finish_execution_attempt(
                loop_id,
                lease,
                state,
                episode_id,
                hydration_request,
                original_manifest.pinned_digest,
                collected,
                false,
            )
            .await?;
        match outcome {
            OperationOutcome::EpisodeCompleted {
                episode_id,
                checkpoint_version,
                ..
            } => Ok(EpisodeRecoveryOutcome::Completed {
                episode_id,
                checkpoint_version,
            }),
            OperationOutcome::Suspended => Ok(EpisodeRecoveryOutcome::Suspended {
                reason: SuspensionReason::Recovery,
            }),
            other => Err(ControllerError::Invariant(format!(
                "finish_execution_attempt returned an unexpected outcome during recovery: {other:?}"
            ))),
        }
    }

    /// The terminal decision for this Episode (`attempt.completed` or
    /// `verification.completed`) is already durably recorded, but
    /// `rotation.requested` is not yet applied (Episode status is still
    /// `Executing`): finishes the rotation and commit the same way a
    /// completed `advance` call would have (failure point 8, or the
    /// `Executing` half of failure point 6).
    async fn finish_rotation(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        reason: RotationReason,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        if state.execution.status == LoopStatus::Suspended {
            self.controller
                .ensure_episode_terminal_and_commit(
                    loop_id,
                    lease,
                    state,
                    episode_id,
                    SuspensionReason::Recovery,
                )
                .await?;
            return Ok(EpisodeRecoveryOutcome::Suspended {
                reason: SuspensionReason::Recovery,
            });
        }
        let committed = self
            .controller
            .rotate_and_commit(loop_id, lease, state, episode_id, reason)
            .await?;
        Ok(EpisodeRecoveryOutcome::Completed {
            episode_id,
            checkpoint_version: committed.checkpoint.version,
        })
    }

    /// `rotation.requested` is already durably recorded (Episode status is
    /// `Draining`): only the final Checkpoint commit is missing (failure
    /// point 6's `Draining` half, and failure point 8 literally: "during
    /// Session drain").
    async fn recover_draining(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        if state.execution.status == LoopStatus::Suspended {
            self.controller
                .ensure_episode_terminal_and_commit(
                    loop_id,
                    lease,
                    state,
                    episode_id,
                    SuspensionReason::Recovery,
                )
                .await?;
            return Ok(EpisodeRecoveryOutcome::Suspended {
                reason: SuspensionReason::Recovery,
            });
        }
        let checkpoint = self.controller.build_checkpoint(&state, episode_id);
        // The rotation reason was already durably recorded pre-crash (the
        // Episode is `Draining`, meaning `RequestRotation` already
        // committed); it is not re-derived here, so this commit's span
        // carries no `loopward.rotation.reason` — an accepted, narrow gap
        // distinct from the routine paths this fix round addresses.
        let committed = self
            .controller
            .commit(loop_id, lease, &state, checkpoint, None)
            .await?;
        Ok(EpisodeRecoveryOutcome::Completed {
            episode_id,
            checkpoint_version: committed.checkpoint.version,
        })
    }

    /// A Verification Episode with a live Session: redoes Loopward's own
    /// verification judgement if `verification.completed` is not yet
    /// recorded (failure point 5 — always safe, since verification is a
    /// pure computation over durable inputs, unlike a runner's external
    /// effect), then finishes the rotation exactly as
    /// [`Self::finish_rotation`] does for an Execution Episode.
    async fn recover_verification(
        &self,
        loop_id: loopward_api::ids::LoopId,
        lease: &LoopLease,
        state: AggregateState,
        episode_id: EpisodeId,
        work_unit: WorkUnit,
    ) -> Result<EpisodeRecoveryOutcome, ControllerError> {
        let attempt = state.attempt.clone().ok_or_else(|| {
            ControllerError::Invariant(
                "a verification Episode has no Attempt in the projection".to_owned(),
            )
        })?;

        let verification_pending = work_unit.status == WorkUnitStatus::AwaitingVerification
            && attempt.verification_outcome.is_none();
        if verification_pending {
            let result_ref = attempt.result_ref.clone().ok_or_else(|| {
                ControllerError::Invariant(
                    "a verification Episode's Attempt carries no result".to_owned(),
                )
            })?;
            let outcome = self
                .controller
                .evaluate_verification(
                    loop_id,
                    state.checkpoint.version,
                    &work_unit,
                    &attempt,
                    &result_ref,
                    None,
                )
                .await?;
            let state = self
                .controller
                .apply(
                    loop_id,
                    lease,
                    &state,
                    LifecycleCommand::RecordVerification { outcome },
                )
                .await?;
            return self
                .finish_rotation(
                    loop_id,
                    lease,
                    state,
                    episode_id,
                    RotationReason::VerificationBoundary,
                )
                .await;
        }

        self.finish_rotation(
            loop_id,
            lease,
            state,
            episode_id,
            RotationReason::VerificationBoundary,
        )
        .await
    }
}
