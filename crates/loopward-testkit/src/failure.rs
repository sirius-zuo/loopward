//! [`FailureInjector`]: durably seeds the exact projection a crash at one of
//! Phase One's eight named failure-injection points would leave behind
//! (`docs/10-mvp-and-evaluation.md` §8), so a test can hand that fixture to
//! a real `loopward_runtime::RecoveryManager` and observe genuine
//! reconciliation rather than a simulation of it.
//!
//! Ported from `loopward-runtime/tests/recovery_matrix.rs`'s fixture
//! builders (`build_prepared_attempt`, `build_running_attempt`,
//! `build_verification_episode_hydrated`, `build_verification_pending`),
//! which drive `loopward_core::decide`/`evolve` directly up to the exact
//! durable fact a crash would have left behind, then seed a fake store with
//! the result. This does the identical thing against *any* real
//! [`loopward_runtime::ports::LoopStore`] — durably, through
//! [`loopward_runtime::ports::LoopStore::append`]/`commit_checkpoint`, the
//! same two operations a real `LoopController` would have used — rather
//! than a fake's in-memory `seed`. Because `append`/`commit_checkpoint`
//! each commit one whole event batch atomically, this is the only
//! granularity a real crash could actually interrupt at (see
//! `loopward_runtime::recovery::reconcile`'s module doc).
//!
//! Every crash point starts a *fresh* Loop: [`FailureInjector::seed`] is a
//! self-contained fixture builder, not a way to interrupt an
//! already-in-flight scenario. A conformance test that wants to prove
//! [`loopward_runtime::RecoveryManager`] handles all eight documented cases
//! calls [`FailureInjector::seed`] once per [`CrashPoint`], against its own
//! isolated `loop_id`, and asserts the recovery outcome that point's
//! documentation promises.

use loopward_api::context::{
    ContextBudget as ApiContextBudget, ContextCapabilities, ContextControlLevel,
};
use loopward_api::ids::{EpisodeId, LoopId, SessionId};
use loopward_api::loop_service::LoopKind;
use loopward_api::resource::{OpaqueRef, PhaseRef, ResourceRef};
use loopward_api::runner::RunnerCapabilities;
use loopward_api::work_unit::{
    ResourceBudget, UsageReport, WorkUnit, WorkUnitOutcome, WorkUnitResult,
};
use loopward_core::{
    AggregateState, Checkpoint, EffectivePolicy, LifecycleCommand, LoopEvent, LoopExecution,
    LoopStatus, RotationReason, VerificationOutcome, decide, evolve, validate_invariants,
};
use loopward_runtime::{
    AcquireLease, CheckpointCommitRequest, ContextHydrator, ControllerError, HydrationRequest,
    LoopLease, ResourceBeginHint, RuntimeInstanceId, RuntimePorts,
};

/// The eight failure-injection points `docs/10-mvp-and-evaluation.md` §8
/// names, verbatim, in declaration order.
///
/// Each carries the recovery outcome `docs/06-verification-and-recovery.md`
/// §12-§13's Recovery Decisions table promises for it — see
/// [`CrashPoint::expected_recovery`] — which is what
/// [`loopward_runtime::RecoveryManager::recover`] must actually produce when
/// handed a fixture this type seeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrashPoint {
    /// Before runner start: an Attempt's intent is durable
    /// (`attempt.prepared`), but the runner was never called at all.
    BeforeRunnerStart,
    /// After external start but before handle persistence: locally
    /// identical to [`Self::BeforeRunnerStart`] — the runner truthfully
    /// reports it already started, and only that answer distinguishes the
    /// two.
    AfterExternalStartBeforeHandlePersistence,
    /// During runner execution: `attempt.started` is durable and the
    /// runner is still genuinely executing.
    DuringRunnerExecution,
    /// After result materialization: locally identical to
    /// [`Self::DuringRunnerExecution`] — the runner reports the Attempt is
    /// definitively complete.
    AfterResultMaterialization,
    /// During verification: a fresh Verification Episode's Session is
    /// live, but no verdict has been recorded yet.
    DuringVerification,
    /// Before Checkpoint transaction: `verification.completed` is
    /// durable, but the closing rotation and Checkpoint commit are not.
    BeforeCheckpointTransaction,
    /// After Checkpoint commit but before process acknowledgement: the
    /// Episode-closing Checkpoint transaction already fully committed; only
    /// the caller who invoked it crashed before observing that.
    AfterCheckpointCommitBeforeAcknowledgement,
    /// During Session drain: `rotation.requested` is durable (the
    /// Episode is `Draining`), but the Checkpoint commit that would close it
    /// is not.
    DuringSessionDrain,
}

impl CrashPoint {
    /// Every crash point, in `docs/10` §8's declaration order.
    pub const ALL: [Self; 8] = [
        Self::BeforeRunnerStart,
        Self::AfterExternalStartBeforeHandlePersistence,
        Self::DuringRunnerExecution,
        Self::AfterResultMaterialization,
        Self::DuringVerification,
        Self::BeforeCheckpointTransaction,
        Self::AfterCheckpointCommitBeforeAcknowledgement,
        Self::DuringSessionDrain,
    ];

    /// The stable label `docs/10-mvp-and-evaluation.md` §8 names this point
    /// with, verbatim.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::BeforeRunnerStart => "before runner start",
            Self::AfterExternalStartBeforeHandlePersistence => {
                "after external start but before handle persistence"
            }
            Self::DuringRunnerExecution => "during runner execution",
            Self::AfterResultMaterialization => "after result materialization",
            Self::DuringVerification => "during verification",
            Self::BeforeCheckpointTransaction => "before Checkpoint transaction",
            Self::AfterCheckpointCommitBeforeAcknowledgement => {
                "after Checkpoint commit but before process acknowledgement"
            }
            Self::DuringSessionDrain => "during Session drain",
        }
    }

    /// The recovery outcome `docs/06-verification-and-recovery.md` §12-§13's
    /// Recovery Decisions table promises for a crash at this point, and
    /// which [`loopward_runtime::RecoveryOutcome`] variant demonstrates it —
    /// exactly what "each case must have an expected reconciliation and
    /// recovery outcome" (`docs/10` §8) requires be documented.
    #[must_use]
    pub fn expected_recovery(self) -> &'static str {
        match self {
            Self::BeforeRunnerStart => {
                "intent recorded, runner never called: start under the same idempotency key \
                 (`RecoveryOutcome::EpisodeRecovered`, one `start` call)"
            }
            Self::AfterExternalStartBeforeHandlePersistence => {
                "runner confirms it already started: reattach without starting a second time, \
                 then collect (`RecoveryOutcome::EpisodeRecovered`, zero `start` calls)"
            }
            Self::DuringRunnerExecution => {
                "runner reports still running: reattach and leave the Episode open for a later \
                 recovery attempt (`RecoveryOutcome::Reattached`)"
            }
            Self::AfterResultMaterialization => {
                "runner reports completed: collect and validate the result, then finish the \
                 Episode normally (`RecoveryOutcome::EpisodeRecovered`)"
            }
            Self::DuringVerification => {
                "redo Loopward's own verification judgement (safe: no external effect to \
                 repeat) and finish the Episode (`RecoveryOutcome::EpisodeRecovered`, the \
                 runner is never consulted)"
            }
            Self::BeforeCheckpointTransaction => {
                "verification already durable: finish the rotation and commit without \
                 re-deciding verification (`RecoveryOutcome::EpisodeRecovered`, exactly one \
                 Checkpoint commit)"
            }
            Self::AfterCheckpointCommitBeforeAcknowledgement => {
                "the Episode already fully terminated: recovery is a no-op, and a subsequent \
                 `advance` continues from the next Episode (`RecoveryOutcome::NothingToRecover`)"
            }
            Self::DuringSessionDrain => {
                "rotation already durable: commit directly without re-deciding it \
                 (`RecoveryOutcome::EpisodeRecovered`, zero re-issued `rotation.requested` \
                 events)"
            }
        }
    }
}

/// Everything a seeded crash-point fixture leaves behind, for a test to
/// assert against and to hand to `RecoveryManager::recover`.
#[derive(Debug, Clone)]
pub struct CrashFixture {
    /// The crash point this fixture was seeded for.
    pub point: CrashPoint,
    /// The fixture's Loop.
    pub loop_id: LoopId,
    /// The Work Unit the fixture was seeded with.
    pub work_unit: WorkUnit,
    /// The Attempt the crash left behind.
    pub attempt_id: loopward_api::ids::AttemptId,
    /// The Execution Episode this fixture opened.
    pub execution_episode_id: EpisodeId,
    /// The Execution Episode's Runtime Session.
    pub execution_session_id: SessionId,
    /// The Verification Episode this fixture opened, once the crash point is
    /// at or past [`CrashPoint::DuringVerification`].
    pub verification_episode_id: Option<EpisodeId>,
    /// The Verification Episode's Runtime Session.
    pub verification_session_id: Option<SessionId>,
    /// The exact durable projection the crash left behind.
    pub state: AggregateState,
}

/// Seeds the durable fixture one of Phase One's eight named crash points
/// would leave behind, and nothing more.
pub struct FailureInjector {
    ports: RuntimePorts,
    hydrator: ContextHydrator,
    holder_id: RuntimeInstanceId,
    lease_ttl: time::Duration,
}

impl FailureInjector {
    /// Builds an injector over `ports`, acquiring leases under `holder_id`.
    ///
    /// `holder_id` should be the same [`RuntimeInstanceId`] the
    /// `LoopController`/`RecoveryManager` this fixture will later be handed
    /// to uses (its own `holder_id()` accessor) — a *different* holder
    /// racing to acquire the same Loop's lease while it is still held (real
    /// stores fence on wall-clock TTL expiry, not the injected `Clock`)
    /// would otherwise see `LeaseConflict` instead of the fixture it asked
    /// for.
    #[must_use]
    pub fn new(ports: RuntimePorts, holder_id: RuntimeInstanceId) -> Self {
        let hydrator = ContextHydrator::new(std::sync::Arc::clone(&ports.resources));
        Self {
            ports,
            hydrator,
            holder_id,
            lease_ttl: time::Duration::seconds(120),
        }
    }

    /// The ports this injector seeds fixtures into.
    #[must_use]
    pub fn ports(&self) -> &RuntimePorts {
        &self.ports
    }

    /// Overrides the lease TTL acquired while seeding (default: 120s, ample
    /// for a synchronous seeding sequence).
    #[must_use]
    pub fn with_lease_ttl(mut self, ttl: time::Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Seeds the exact durable fixture a crash at `point` would leave
    /// behind for `work_unit`, in a fresh Loop identified by `loop_id`.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when the underlying store, hydration, or
    /// domain reducer rejects any step of building the fixture — the same
    /// errors a real `LoopController` could raise performing the identical
    /// sequence.
    pub async fn seed(
        &self,
        point: CrashPoint,
        loop_id: LoopId,
        work_unit: WorkUnit,
    ) -> Result<CrashFixture, ControllerError> {
        match point {
            CrashPoint::BeforeRunnerStart
            | CrashPoint::AfterExternalStartBeforeHandlePersistence => {
                self.seed_prepared_attempt(point, loop_id, work_unit).await
            }
            CrashPoint::DuringRunnerExecution | CrashPoint::AfterResultMaterialization => {
                self.seed_running_attempt(point, loop_id, work_unit).await
            }
            CrashPoint::DuringVerification => {
                Box::pin(self.seed_verification_pending(point, loop_id, work_unit)).await
            }
            CrashPoint::BeforeCheckpointTransaction => {
                Box::pin(self.seed_before_checkpoint_transaction(point, loop_id, work_unit)).await
            }
            CrashPoint::AfterCheckpointCommitBeforeAcknowledgement => {
                Box::pin(self.seed_after_checkpoint_commit(point, loop_id, work_unit)).await
            }
            CrashPoint::DuringSessionDrain => {
                Box::pin(self.seed_during_session_drain(point, loop_id, work_unit)).await
            }
        }
    }

    /// A fresh, unused `loop_id`, for a caller that wants an isolated
    /// fixture per crash point without minting its own identities.
    #[must_use]
    pub fn fresh_loop_id() -> LoopId {
        LoopId::new()
    }

    // -----------------------------------------------------------------
    // Genesis and the shared `decide -> evolve -> validate -> append` step
    // -----------------------------------------------------------------

    /// Genesis-commits a fresh Loop with `work_unit` already its active Work
    /// Unit — durably working around the documented gap that no
    /// `LifecycleCommand` assigns one (`loopward_runtime::ControllerError::NoWorkUnit`'s
    /// own doc comment).
    ///
    /// Public because a conformance scenario, not only a crash-point
    /// fixture, needs this same seam to get a Work Unit onto a Loop at all
    /// — see `crates/loopward-cli/tests/e2e_phase_one.rs`.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when `loop_id` already exists, or when
    /// the underlying store rejects the genesis commit.
    pub async fn genesis(
        &self,
        loop_id: LoopId,
        work_unit: &WorkUnit,
    ) -> Result<AggregateState, ControllerError> {
        let checkpoint = Checkpoint {
            id: self.ports.ids.new_checkpoint_id(),
            loop_id,
            version: 1,
            aggregate_revision: 1,
            parent: None,
            episode_id: None,
            integrity_digest: None,
        };
        let state = AggregateState {
            execution: LoopExecution {
                id: loop_id,
                kind: LoopKind::new("loopward.testkit/failure-injector"),
                status: LoopStatus::Active,
                phase: PhaseRef::new("initial"),
                aggregate_revision: 1,
                checkpoint_version: 1,
                active_episode: None,
                active_work_unit: Some(work_unit.id),
                budget: ResourceBudget::default(),
            },
            episode: None,
            session: None,
            work_unit: Some(work_unit.clone()),
            attempt: None,
            checkpoint: checkpoint.clone(),
        };
        let stored = self
            .ports
            .store
            .commit_checkpoint(CheckpointCommitRequest {
                loop_id,
                expected_revision: 0,
                expected_checkpoint_version: 0,
                fencing_token: 0,
                checkpoint: checkpoint.clone(),
                events: vec![LoopEvent::CheckpointCommitted { checkpoint }],
                state,
            })
            .await?;
        Ok(stored.state)
    }

    /// Acquires a fresh lease for `loop_id` under this injector's holder.
    async fn acquire(&self, loop_id: LoopId) -> Result<LoopLease, ControllerError> {
        Ok(self
            .ports
            .store
            .acquire_lease(AcquireLease {
                loop_id,
                holder_id: self.holder_id,
                ttl: self.lease_ttl,
            })
            .await?)
    }

    /// Decides, evolves, validates, and durably appends one command's
    /// events — the identical sequence
    /// `loopward_runtime::LoopController::apply` performs, reimplemented
    /// here because that helper is `pub(crate)` to the runtime crate.
    async fn step(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: &AggregateState,
        command: LifecycleCommand,
    ) -> Result<AggregateState, ControllerError> {
        let events = decide(state, &command, &EffectivePolicy::default())?;
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

    /// Decides, evolves, validates, and durably commits a
    /// [`LifecycleCommand::CommitCheckpoint`] through
    /// [`loopward_runtime::ports::LoopStore::commit_checkpoint`] rather than
    /// [`Self::step`]'s `append` — the identical split
    /// `loopward_runtime::LoopController::apply`/`commit` draws, and not a
    /// cosmetic one: only `commit_checkpoint` populates the typed Episode/
    /// Session/Work Unit/Attempt lifecycle tables a real `SqliteLoopStore`
    /// projects (`append` only advances the JSON projection row), so an
    /// Episode boundary seeded through `step` instead of this method would
    /// leave a later Attempt's `episode_id` foreign key dangling.
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
        let events = decide(state, &command, &EffectivePolicy::default())?;
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

    /// Finalizes `bytes` through the resource broker.
    async fn finalize_bytes(&self, bytes: Vec<u8>) -> Result<ResourceRef, ControllerError> {
        let handle = self
            .ports
            .resources
            .begin(ResourceBeginHint::default())
            .await?;
        self.ports.resources.write(&handle, &bytes).await?;
        Ok(self.ports.resources.finalize(handle).await?)
    }

    /// Hydrates a Boot Package for `episode_id`/`session_generation` and
    /// durably records `context.hydrated`.
    async fn hydrate_and_mark(
        &self,
        loop_id: LoopId,
        lease: &LoopLease,
        state: &AggregateState,
        episode_id: EpisodeId,
        session_generation: u32,
        work_unit: &WorkUnit,
    ) -> Result<AggregateState, ControllerError> {
        let request = HydrationRequest {
            checkpoint: state.checkpoint.clone(),
            episode_id,
            session_generation,
            work_unit: work_unit.clone(),
            capabilities: test_capabilities(),
            budget: generous_budget(),
            context_rule_version: "loopward.context-rules/v0.1".to_owned(),
            candidates: Vec::new(),
        };
        let report = self.hydrator.hydrate(request).await?;
        let bytes = serde_json::to_vec(&report.manifest)?;
        let manifest_ref = self.finalize_bytes(bytes).await?;
        self.step(
            loop_id,
            lease,
            state,
            LifecycleCommand::MarkHydrated {
                manifest: manifest_ref,
            },
        )
        .await
    }

    // -----------------------------------------------------------------
    // Fixture tiers, each extending the previous by exactly the events a
    // crash at the next point later would add.
    // -----------------------------------------------------------------

    /// Failure points 1 and 2's shared fixture: an Attempt exists,
    /// `Prepared`, with no externally observed reference.
    async fn seed_prepared_attempt(
        &self,
        point: CrashPoint,
        loop_id: LoopId,
        work_unit: WorkUnit,
    ) -> Result<CrashFixture, ControllerError> {
        let state = self.genesis(loop_id, &work_unit).await?;
        let lease = self.acquire(loop_id).await?;

        let episode_id = self.ports.ids.new_episode_id();
        let session_id = self.ports.ids.new_session_id();
        let attempt_id = self.ports.ids.new_attempt_id();

        let state = self
            .step(
                loop_id,
                &lease,
                &state,
                LifecycleCommand::StartEpisode { episode_id },
            )
            .await?;
        let state = self
            .hydrate_and_mark(loop_id, &lease, &state, episode_id, 1, &work_unit)
            .await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &state,
                LifecycleCommand::StartSession {
                    session_id,
                    generation: 1,
                },
            )
            .await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &state,
                LifecycleCommand::PrepareAttempt {
                    attempt_id,
                    idempotency_key: format!("attempt:{attempt_id}"),
                },
            )
            .await?;

        Ok(CrashFixture {
            point,
            loop_id,
            work_unit,
            attempt_id,
            execution_episode_id: episode_id,
            execution_session_id: session_id,
            verification_episode_id: None,
            verification_session_id: None,
            state,
        })
    }

    /// Failure points 3 and 4's shared fixture: as
    /// [`Self::seed_prepared_attempt`], continued to `attempt.started`
    /// (`Running`, with a durably observed external reference).
    async fn seed_running_attempt(
        &self,
        point: CrashPoint,
        loop_id: LoopId,
        work_unit: WorkUnit,
    ) -> Result<CrashFixture, ControllerError> {
        let prepared = self
            .seed_prepared_attempt(point, loop_id, work_unit)
            .await?;
        let lease = self.acquire(loop_id).await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &prepared.state,
                LifecycleCommand::RecordRunnerStarted {
                    external_ref: OpaqueRef::new(format!("external-ref-{}", prepared.attempt_id)),
                },
            )
            .await?;
        Ok(CrashFixture { state, ..prepared })
    }

    /// Failure point 5's fixture: a fresh Verification Episode's Session is
    /// live (`session.started` recorded), but `verification.completed` has
    /// not been.
    async fn seed_verification_pending(
        &self,
        point: CrashPoint,
        loop_id: LoopId,
        work_unit: WorkUnit,
    ) -> Result<CrashFixture, ControllerError> {
        let running = self
            .seed_running_attempt(CrashPoint::DuringRunnerExecution, loop_id, work_unit)
            .await?;
        let lease = self.acquire(loop_id).await?;

        let result_bytes = serde_json::to_vec(&default_result(running.attempt_id))?;
        let result_ref = self.finalize_bytes(result_bytes).await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &running.state,
                LifecycleCommand::RecordResult { result: result_ref },
            )
            .await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &state,
                LifecycleCommand::RequestRotation {
                    reason: RotationReason::WorkUnitBoundary,
                },
            )
            .await?;
        let checkpoint1 = Checkpoint {
            id: self.ports.ids.new_checkpoint_id(),
            loop_id,
            version: state.checkpoint.version + 1,
            aggregate_revision: state.execution.aggregate_revision,
            parent: Some(state.checkpoint.id),
            episode_id: Some(running.execution_episode_id),
            integrity_digest: None,
        };
        let state = self.commit(loop_id, &lease, &state, checkpoint1).await?;

        let verification_episode_id = self.ports.ids.new_episode_id();
        let state = self
            .step(
                loop_id,
                &lease,
                &state,
                LifecycleCommand::StartEpisode {
                    episode_id: verification_episode_id,
                },
            )
            .await?;
        let state = self
            .hydrate_and_mark(
                loop_id,
                &lease,
                &state,
                verification_episode_id,
                2,
                &running.work_unit,
            )
            .await?;
        let verification_session_id = self.ports.ids.new_session_id();
        let state = self
            .step(
                loop_id,
                &lease,
                &state,
                LifecycleCommand::StartSession {
                    session_id: verification_session_id,
                    generation: 2,
                },
            )
            .await?;

        Ok(CrashFixture {
            point,
            verification_episode_id: Some(verification_episode_id),
            verification_session_id: Some(verification_session_id),
            state,
            ..running
        })
    }

    /// Failure point 6's fixture: `verification.completed` (`Accepted`) is
    /// durable, but the closing rotation and Checkpoint are not.
    async fn seed_before_checkpoint_transaction(
        &self,
        point: CrashPoint,
        loop_id: LoopId,
        work_unit: WorkUnit,
    ) -> Result<CrashFixture, ControllerError> {
        let pending = Box::pin(self.seed_verification_pending(
            CrashPoint::DuringVerification,
            loop_id,
            work_unit,
        ))
        .await?;
        let lease = self.acquire(loop_id).await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &pending.state,
                LifecycleCommand::RecordVerification {
                    outcome: VerificationOutcome::Accepted,
                },
            )
            .await?;
        Ok(CrashFixture {
            point,
            state,
            ..pending
        })
    }

    /// Failure point 7's fixture: the Episode-closing Checkpoint transaction
    /// already fully committed.
    async fn seed_after_checkpoint_commit(
        &self,
        point: CrashPoint,
        loop_id: LoopId,
        work_unit: WorkUnit,
    ) -> Result<CrashFixture, ControllerError> {
        let verified = Box::pin(self.seed_before_checkpoint_transaction(
            CrashPoint::BeforeCheckpointTransaction,
            loop_id,
            work_unit,
        ))
        .await?;
        let lease = self.acquire(loop_id).await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &verified.state,
                LifecycleCommand::RequestRotation {
                    reason: RotationReason::VerificationBoundary,
                },
            )
            .await?;
        let checkpoint2 = Checkpoint {
            id: self.ports.ids.new_checkpoint_id(),
            loop_id,
            version: state.checkpoint.version + 1,
            aggregate_revision: state.execution.aggregate_revision,
            parent: Some(state.checkpoint.id),
            episode_id: verified.verification_episode_id,
            integrity_digest: None,
        };
        let state = self.commit(loop_id, &lease, &state, checkpoint2).await?;
        Ok(CrashFixture {
            point,
            state,
            ..verified
        })
    }

    /// Failure point 8's fixture: `rotation.requested` is durable (the
    /// Verification Episode is `Draining`), but the Checkpoint commit that
    /// would close it is not.
    async fn seed_during_session_drain(
        &self,
        point: CrashPoint,
        loop_id: LoopId,
        work_unit: WorkUnit,
    ) -> Result<CrashFixture, ControllerError> {
        let verified = Box::pin(self.seed_before_checkpoint_transaction(
            CrashPoint::BeforeCheckpointTransaction,
            loop_id,
            work_unit,
        ))
        .await?;
        let lease = self.acquire(loop_id).await?;
        let state = self
            .step(
                loop_id,
                &lease,
                &verified.state,
                LifecycleCommand::RequestRotation {
                    reason: RotationReason::VerificationBoundary,
                },
            )
            .await?;
        Ok(CrashFixture {
            point,
            state,
            ..verified
        })
    }
}

/// Generous enough that no admission or drain-time budget signal fires
/// unless a test deliberately sets one up to.
fn generous_budget() -> ApiContextBudget {
    ApiContextBudget {
        model_window_tokens: None,
        soft_input_limit_tokens: 100_000,
        hard_input_limit_tokens: 200_000,
        reserved_output_tokens: 0,
        reserved_tool_tokens: 0,
        safety_margin_tokens: 0,
    }
}

/// Capabilities matching [`crate::fake_runner::FakeRunner`]'s own defaults.
fn test_capabilities() -> RunnerCapabilities {
    RunnerCapabilities {
        protocol_versions: vec![loopward_api::PROTOCOL_VERSION.to_owned()],
        resumable: false,
        supports_reconcile: true,
        supports_events: false,
        supports_suspend: false,
        supports_cancel: false,
        reports_token_usage: false,
        reports_internal_trace_context: false,
        context: ContextCapabilities {
            control_level: ContextControlLevel::ManagedBoot,
            reports_usage: false,
            reports_compaction: false,
            supports_resource_fetch: false,
            supports_structured_handoff: false,
            supports_resume: false,
        },
    }
}

/// The permissive default result a seeded Attempt is recorded with.
fn default_result(attempt_id: loopward_api::ids::AttemptId) -> WorkUnitResult {
    WorkUnitResult {
        protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
        attempt_id,
        outcome: WorkUnitOutcome::Produced,
        outputs: Vec::new(),
        state_proposals: Vec::new(),
        observation_proposals: Vec::new(),
        decision_proposals: Vec::new(),
        verification_evidence: Vec::new(),
        unresolved: Vec::new(),
        recommended_continuation: None,
        usage: UsageReport::default(),
        runner_trace_refs: Vec::new(),
    }
}
