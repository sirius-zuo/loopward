//! [`RuntimeLoopService`]: the public `LoopService` adapter over
//! [`LoopController`] (`docs/09-public-api.md`).
//!
//! Every mutating method validates the caller's `expected_revision` against
//! the store's current aggregate revision before doing anything else, and
//! replays a previously seen `idempotency_key` rather than re-invoking the
//! controller — Phase One's answer to "mutating commands are idempotent
//! within their documented scope" (`docs/09` §4). No prior task defines a
//! durable `Operation` record store, so this idempotency ledger is
//! in-process only (a `Mutex<HashMap>`), not durable across a process
//! restart; a production deployment wanting cross-restart idempotency needs
//! a dedicated `LoopStore` extension, which is future work.
//!
//! `status`, `events`, and `result` return public views projected from the
//! durable [`AggregateState`] and journal, never the store rows or a
//! complete Checkpoint (`docs/09` §6).

use std::collections::HashMap;
use std::sync::Mutex;

use loopward_api::error::{LoopwardError, LoopwardErrorCode};
use loopward_api::event::{
    EventCorrelation, EventCursor, EventDurability, EventDurabilityFilter, EventPage, EventQuery,
    LoopEvent as ApiLoopEvent,
};
use loopward_api::ids::LoopId;
use loopward_api::loop_service::{
    AdvanceLoopCommand, CancelLoopCommand, CreateLoopRequest, EpisodeRef, LoopHandle, LoopOutcome,
    LoopOutcomeKind, LoopResult, LoopStatus as ApiLoopStatus, LoopStatusView, OperationHandle,
    ResumeLoopCommand, RunUntilCommand, SuspendLoopCommand, SuspensionView, WorkUnitRef,
};
use loopward_api::resource::PhaseRef;
use loopward_api::runner::SuspensionReason;
use loopward_core::{AggregateState, LoopStatus as CoreLoopStatus, work_unit_status_is_terminal};

use crate::controller::{ControllerError, LoopController};
use crate::ports::{RecordedEvent, StoreError};
use crate::recovery::{RecoveryManager, RecoveryOutcome};

/// The public `LoopService` adapter over [`LoopController`].
pub struct RuntimeLoopService {
    controller: LoopController,
    recovery: RecoveryManager,
    operations: Mutex<HashMap<String, OperationHandle>>,
}

impl std::fmt::Debug for RuntimeLoopService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeLoopService").finish_non_exhaustive()
    }
}

impl RuntimeLoopService {
    /// Builds a service adapter over `controller`.
    #[must_use]
    pub fn new(controller: LoopController) -> Self {
        Self {
            recovery: RecoveryManager::new(controller.clone()),
            controller,
            operations: Mutex::new(HashMap::new()),
        }
    }

    /// The controller this service delegates to.
    #[must_use]
    pub fn controller(&self) -> &LoopController {
        &self.controller
    }

    /// Returns a previously recorded [`OperationHandle`] for
    /// `idempotency_key`, if this exact command was already accepted.
    fn previously_accepted(&self, idempotency_key: &str) -> Option<OperationHandle> {
        self.operations
            .lock()
            .expect("operations lock poisoned")
            .get(idempotency_key)
            .cloned()
    }

    /// Records the [`OperationHandle`] a command with `idempotency_key`
    /// produced, so a retried command replays it instead of re-executing.
    fn record_accepted(&self, idempotency_key: &str, handle: OperationHandle) {
        self.operations
            .lock()
            .expect("operations lock poisoned")
            .insert(idempotency_key.to_owned(), handle);
    }

    /// Loads the Loop's current aggregate revision and rejects a mismatched
    /// `expected_revision`.
    async fn check_expected_revision(
        &self,
        loop_id: LoopId,
        expected: Option<u64>,
    ) -> Result<u64, LoopwardError> {
        let stored = self
            .controller
            .ports()
            .store
            .load(loop_id)
            .await
            .map_err(|error| store_error(&error))?;
        let actual = stored.state.execution.aggregate_revision;
        if let Some(expected) = expected
            && expected != actual
        {
            return Err(LoopwardError {
                code: LoopwardErrorCode::Conflict,
                message: format!("expected aggregate revision {expected}, found {actual}"),
                retryable: true,
                trace_ref: None,
            });
        }
        Ok(actual)
    }
}

#[async_trait::async_trait]
impl loopward_api::loop_service::LoopService for RuntimeLoopService {
    /// # Known limitation: most of `CreateLoopRequest` is validated, then
    /// discarded
    ///
    /// `request.goal`, `success_criteria`, `pinned_constraints`,
    /// `domain_schema`, `initial_domain_state`, and `policies` are checked by
    /// [`validate_create_request`] but never stored anywhere durable. This is
    /// not an oversight this method is hiding: `loopward_core::Checkpoint` is
    /// deliberately Core's identity-and-versioning view only (id, `loop_id`,
    /// version, aggregate revision, parent, episode — see its own doc
    /// comment) and has no field for a goal, success criteria, pinned
    /// constraints, or domain state. The full Checkpoint content model
    /// (`docs/schemas/checkpoint-v0.1.yaml`) has no Rust type in this crate
    /// yet, a known drift from Tasks 3/4 this task does not have the file
    /// scope to close.
    ///
    /// `pinned_constraints` is the most concerning field to drop silently,
    /// given the Global Constraint that pinned state must survive hydration
    /// and drain losslessly — but note that Phase One's `create()` also
    /// produces a Loop with `work_unit: None` (no `LifecycleCommand` assigns
    /// one; see [`ControllerError::NoWorkUnit`]'s doc comment), so there is
    /// nowhere to attach them yet even in memory: the controller's default
    /// candidate provider (`DefaultContextCandidates`) projects
    /// `WorkUnit::constraints`, not `CreateLoopRequest::pinned_constraints`,
    /// and today those are two different, currently disconnected lists.
    /// Whether a Loop's creation-time `pinned_constraints` should
    /// automatically populate every future Work Unit's `constraints` is a
    /// real design question for whoever builds Work Unit assignment — this
    /// method does not answer it, only avoids silently pretending the
    /// question does not exist.
    async fn create(&self, request: CreateLoopRequest) -> Result<LoopHandle, LoopwardError> {
        if let Some(existing) = self.previously_accepted(&request.metadata.idempotency_key) {
            return Ok(LoopHandle {
                loop_id: existing.loop_id,
                aggregate_revision: existing.accepted_at_revision,
            });
        }
        validate_create_request(&request)?;

        let ports = self.controller.ports();
        let loop_id = ports.ids.new_loop_id();

        let checkpoint = loopward_core::Checkpoint {
            id: ports.ids.new_checkpoint_id(),
            loop_id,
            version: 1,
            aggregate_revision: 1,
            parent: None,
            episode_id: None,
            integrity_digest: None,
        };
        let state = AggregateState {
            execution: loopward_core::LoopExecution {
                id: loop_id,
                kind: request.kind.clone(),
                status: CoreLoopStatus::Active,
                phase: PhaseRef::new("initial"),
                aggregate_revision: 1,
                checkpoint_version: 1,
                active_episode: None,
                active_work_unit: None,
                budget: loopward_api::work_unit::ResourceBudget::default(),
            },
            episode: None,
            session: None,
            work_unit: None,
            attempt: None,
            checkpoint: checkpoint.clone(),
        };

        // Genesis first, fenced by `0` — an uncreated Loop has no lease row
        // and a current fencing token of `0`
        // (`loopward-store-sqlite/src/lease.rs`'s own module doc says this
        // precisely: "exactly the token a genesis `commit_checkpoint` must
        // present"). Acquiring a lease *before* the Loop exists would fail
        // outright against the real store (`acquire_lease` requires the Loop
        // to already exist), and even a lenient fake that let it through
        // would hand back fencing token `1`, which a genesis commit must
        // *not* present. The lease is acquired only once the Loop is
        // durable, immediately after — closing the window a caller with
        // knowledge of `loop_id` could otherwise use to write with the
        // still-current token `0`.
        let stored = ports
            .store
            .commit_checkpoint(crate::ports::CheckpointCommitRequest {
                loop_id,
                expected_revision: 0,
                expected_checkpoint_version: 0,
                fencing_token: 0,
                checkpoint: checkpoint.clone(),
                events: vec![loopward_core::LoopEvent::CheckpointCommitted { checkpoint }],
                state,
            })
            .await
            .map_err(|error| store_error(&error))?;

        ports
            .store
            .acquire_lease(crate::ports::AcquireLease {
                loop_id,
                holder_id: self.controller.holder_id(),
                ttl: time::Duration::seconds(60),
            })
            .await
            .map_err(|error| store_error(&error))?;

        let handle = LoopHandle {
            loop_id,
            aggregate_revision: stored.state.execution.aggregate_revision,
        };
        self.record_accepted(
            &request.metadata.idempotency_key,
            OperationHandle {
                operation_id: ports.ids.new_operation_id(),
                loop_id,
                accepted_at_revision: handle.aggregate_revision,
            },
        );
        Ok(handle)
    }

    async fn advance(&self, command: AdvanceLoopCommand) -> Result<OperationHandle, LoopwardError> {
        if let Some(existing) = self.previously_accepted(&command.metadata.idempotency_key) {
            return Ok(existing);
        }
        let accepted_at_revision = self
            .check_expected_revision(command.loop_id, command.metadata.expected_revision)
            .await?;

        Box::pin(self.controller.advance(command.loop_id))
            .await
            .map_err(controller_error)?;

        let handle = OperationHandle {
            operation_id: self.controller.ports().ids.new_operation_id(),
            loop_id: command.loop_id,
            accepted_at_revision,
        };
        self.record_accepted(&command.metadata.idempotency_key, handle.clone());
        Ok(handle)
    }

    async fn run_until(&self, command: RunUntilCommand) -> Result<OperationHandle, LoopwardError> {
        if let Some(existing) = self.previously_accepted(&command.metadata.idempotency_key) {
            return Ok(existing);
        }
        let accepted_at_revision = self
            .check_expected_revision(command.loop_id, command.metadata.expected_revision)
            .await?;

        Box::pin(
            self.controller
                .run_until(command.loop_id, command.condition, command.limits),
        )
        .await
        .map_err(controller_error)?;

        let handle = OperationHandle {
            operation_id: self.controller.ports().ids.new_operation_id(),
            loop_id: command.loop_id,
            accepted_at_revision,
        };
        self.record_accepted(&command.metadata.idempotency_key, handle.clone());
        Ok(handle)
    }

    async fn status(&self, loop_id: LoopId) -> Result<LoopStatusView, LoopwardError> {
        let stored = self
            .controller
            .ports()
            .store
            .load(loop_id)
            .await
            .map_err(|error| store_error(&error))?;
        Ok(project_status(&stored.state, &stored.events))
    }

    async fn events(&self, query: EventQuery) -> Result<EventPage, LoopwardError> {
        if matches!(query.durability, EventDurabilityFilter::Ephemeral) {
            // Phase One records no ephemeral runner-progress events of its
            // own; only the durable journal is queryable here.
            return Ok(EventPage {
                events: Vec::new(),
                next_cursor: None,
            });
        }

        let stored = self
            .controller
            .ports()
            .store
            .load(query.loop_id)
            .await
            .map_err(|error| store_error(&error))?;

        let after = query
            .after
            .as_ref()
            .and_then(|cursor| cursor.0.parse::<u64>().ok())
            .unwrap_or(0);
        let limit = usize::try_from(query.limit.max(1)).unwrap_or(usize::MAX);

        let mut matching: Vec<&RecordedEvent> = stored
            .events
            .iter()
            .filter(|recorded| recorded.sequence > after)
            .collect();
        matching.sort_by_key(|recorded| recorded.sequence);

        let next_cursor = matching
            .get(limit)
            .map(|_| matching[limit - 1].sequence)
            .filter(|_| matching.len() > limit)
            .map(|sequence| EventCursor::new(sequence.to_string()));
        matching.truncate(limit);

        let events = matching
            .into_iter()
            .map(|recorded| project_event(query.loop_id, recorded))
            .collect();
        Ok(EventPage {
            events,
            next_cursor,
        })
    }

    async fn suspend(&self, command: SuspendLoopCommand) -> Result<OperationHandle, LoopwardError> {
        if let Some(existing) = self.previously_accepted(&command.metadata.idempotency_key) {
            return Ok(existing);
        }
        let accepted_at_revision = self
            .check_expected_revision(command.loop_id, command.metadata.expected_revision)
            .await?;

        self.controller
            .suspend(command.loop_id, SuspensionReason::OperatorRequest)
            .await
            .map_err(controller_error)?;

        let handle = OperationHandle {
            operation_id: self.controller.ports().ids.new_operation_id(),
            loop_id: command.loop_id,
            accepted_at_revision,
        };
        self.record_accepted(&command.metadata.idempotency_key, handle.clone());
        Ok(handle)
    }

    /// Resumes a suspended Loop, then reconciles any leftover unreconciled
    /// state before returning.
    ///
    /// [`LoopController::resume`] is deliberately a bare status flip
    /// (`Suspended -> Active`) and does not itself reconcile what the
    /// suspended Episode left behind. Two gaps that flip alone cannot close
    /// (Task 9's own flagged scope for Task 10):
    ///
    /// 1. a Work Unit left at `WorkUnitStatus::Suspended` by a prior
    ///    `VerificationOutcome::Suspended` outcome has no `LifecycleCommand`
    ///    that clears it, so the next `advance` would find a Work Unit
    ///    status `prepare_attempt` never accepts and error permanently;
    /// 2. an unreconciled Attempt left non-terminal by the same suspension
    ///    would make the next `StartEpisode`/`StartSession` fail the
    ///    reducer's `attempt.reconciled_before_next_generation` invariant.
    ///
    /// Calling [`RecoveryManager::recover`] immediately after the flip
    /// closes both: it reconciles the dangling Attempt (asking the runner
    /// before assuming the external effect is safe to retire) and returns
    /// the Work Unit to `Pending`, so the Loop this call hands back is
    /// actually ready to `advance`, not merely `Active` in name.
    ///
    /// Recovery can also legitimately conclude the opposite —
    /// [`RecoveryOutcome::StillSuspended`], when the runner cannot confirm
    /// the old external effect is actually over. Reporting bare success in
    /// that case would be a lie: the Loop would sit `Active` with a
    /// non-terminal Attempt, and every future `advance` would fail the
    /// reducer's `attempt.reconciled_before_next_generation` invariant
    /// permanently. This re-suspends the Loop before returning and reports
    /// the resume as refused (`LoopwardErrorCode::RecoveryRequired`) rather
    /// than a bare `Ok`, so a caller cannot mistake "resumed" for "actually
    /// advanceable".
    async fn resume(&self, command: ResumeLoopCommand) -> Result<OperationHandle, LoopwardError> {
        if let Some(existing) = self.previously_accepted(&command.metadata.idempotency_key) {
            return Ok(existing);
        }
        let accepted_at_revision = self
            .check_expected_revision(command.loop_id, command.metadata.expected_revision)
            .await?;

        self.controller
            .resume(command.loop_id)
            .await
            .map_err(controller_error)?;
        let recovery_outcome = Box::pin(self.recovery.recover(command.loop_id))
            .await
            .map_err(controller_error)?;

        if let RecoveryOutcome::StillSuspended { reason } = recovery_outcome {
            self.controller
                .suspend(command.loop_id, reason)
                .await
                .map_err(controller_error)?;
            return Err(LoopwardError {
                code: LoopwardErrorCode::RecoveryRequired,
                message: format!(
                    "loop {} could not be resumed: a reconciled attempt's external effect may still be live",
                    command.loop_id
                ),
                retryable: true,
                trace_ref: None,
            });
        }

        let handle = OperationHandle {
            operation_id: self.controller.ports().ids.new_operation_id(),
            loop_id: command.loop_id,
            accepted_at_revision,
        };
        self.record_accepted(&command.metadata.idempotency_key, handle.clone());
        Ok(handle)
    }

    async fn cancel(&self, command: CancelLoopCommand) -> Result<OperationHandle, LoopwardError> {
        if let Some(existing) = self.previously_accepted(&command.metadata.idempotency_key) {
            return Ok(existing);
        }
        let accepted_at_revision = self
            .check_expected_revision(command.loop_id, command.metadata.expected_revision)
            .await?;

        self.controller
            .cancel(command.loop_id)
            .await
            .map_err(controller_error)?;

        let handle = OperationHandle {
            operation_id: self.controller.ports().ids.new_operation_id(),
            loop_id: command.loop_id,
            accepted_at_revision,
        };
        self.record_accepted(&command.metadata.idempotency_key, handle.clone());
        Ok(handle)
    }

    async fn result(&self, loop_id: LoopId) -> Result<Option<LoopResult>, LoopwardError> {
        let stored = self
            .controller
            .ports()
            .store
            .load(loop_id)
            .await
            .map_err(|error| store_error(&error))?;
        let state = &stored.state;
        if !state.execution.status.is_terminal() {
            return Ok(None);
        }
        let kind = match state.execution.status {
            CoreLoopStatus::Completed => LoopOutcomeKind::Completed,
            CoreLoopStatus::Failed => LoopOutcomeKind::Failed,
            CoreLoopStatus::Cancelled => LoopOutcomeKind::Cancelled,
            CoreLoopStatus::Active | CoreLoopStatus::Suspended => {
                unreachable!("guarded by the `is_terminal` check above")
            }
        };
        let outputs = state
            .attempt
            .as_ref()
            .and_then(|attempt| attempt.result_ref.clone())
            .into_iter()
            .collect();
        Ok(Some(LoopResult {
            loop_id,
            outcome: LoopOutcome {
                kind,
                summary: None,
            },
            // Phase One's `AggregateState` carries no schema-declared domain
            // state field of its own (`docs/02-core-domain-model.md` §11
            // treats it as caller-defined); a real domain wrapper owns and
            // projects it. `null` is an honest placeholder, not a fabricated
            // value.
            final_domain_state: serde_json::Value::Null,
            outputs,
        }))
    }
}

fn validate_create_request(request: &CreateLoopRequest) -> Result<(), LoopwardError> {
    let mut problems = Vec::new();
    if request.protocol_version.trim().is_empty() {
        problems.push("protocol_version must not be empty");
    }
    if request.kind.0.trim().is_empty() {
        problems.push("kind must not be empty");
    }
    if request.goal.statement.trim().is_empty() {
        problems.push("goal.statement must not be empty");
    }
    if request.domain_schema.0.trim().is_empty() {
        problems.push("domain_schema must not be empty");
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(LoopwardError {
            code: LoopwardErrorCode::InvalidRequest,
            message: problems.join("; "),
            retryable: false,
            trace_ref: None,
        })
    }
}

fn project_status(state: &AggregateState, events: &[RecordedEvent]) -> LoopStatusView {
    let status = match state.execution.status {
        CoreLoopStatus::Active => ApiLoopStatus::Active,
        CoreLoopStatus::Suspended => ApiLoopStatus::Suspended,
        CoreLoopStatus::Completed | CoreLoopStatus::Failed | CoreLoopStatus::Cancelled => {
            ApiLoopStatus::Terminal
        }
    };
    let active_episode = state.active_episode().map(|episode| EpisodeRef {
        episode_id: episode.id,
        // `AggregateState` tracks only the *current* Episode, not a
        // running ordinal across the Loop's lifetime; a dedicated counter
        // is future work, so this is always reported as `0` for now.
        ordinal: 0,
    });
    let active_work_unit = state
        .work_unit
        .as_ref()
        .filter(|work_unit| !work_unit_status_is_terminal(work_unit.status))
        .map(|work_unit| WorkUnitRef {
            work_unit_id: work_unit.id,
            kind: work_unit.kind.clone(),
        });
    let suspension = find_suspension(state, events);
    let terminal_outcome = match state.execution.status {
        CoreLoopStatus::Completed => Some(LoopOutcome {
            kind: LoopOutcomeKind::Completed,
            summary: None,
        }),
        CoreLoopStatus::Failed => Some(LoopOutcome {
            kind: LoopOutcomeKind::Failed,
            summary: None,
        }),
        CoreLoopStatus::Cancelled => Some(LoopOutcome {
            kind: LoopOutcomeKind::Cancelled,
            summary: None,
        }),
        CoreLoopStatus::Active | CoreLoopStatus::Suspended => None,
    };

    LoopStatusView {
        loop_id: state.execution.id,
        status,
        phase: state.execution.phase.clone(),
        aggregate_revision: state.execution.aggregate_revision,
        checkpoint_version: state.execution.checkpoint_version,
        active_episode,
        active_work_unit,
        suspension,
        terminal_outcome,
    }
}

/// Finds the most recent `loop.suspended` journal event, for
/// [`LoopStatusView::suspension`].
///
/// `AggregateState` itself carries no durable `SuspensionReason` field —
/// only the journal event does — so this is reconstructed from the same
/// `events` the store already returned alongside the projection, rather than
/// widening Core's projection just to answer this one status field.
fn find_suspension(state: &AggregateState, events: &[RecordedEvent]) -> Option<SuspensionView> {
    if state.execution.status != CoreLoopStatus::Suspended {
        return None;
    }
    events
        .iter()
        .rev()
        .find_map(|recorded| match &recorded.event {
            loopward_core::LoopEvent::LoopSuspended { reason } => Some(SuspensionView {
                reason: suspension_reason_label(*reason).to_owned(),
                since: recorded.recorded_at,
                detail: None,
            }),
            _ => None,
        })
}

fn suspension_reason_label(reason: SuspensionReason) -> &'static str {
    match reason {
        SuspensionReason::Rotation => "rotation",
        SuspensionReason::BudgetExceeded => "budget_exceeded",
        SuspensionReason::VerificationPending => "verification_pending",
        SuspensionReason::PolicyDecision => "policy_decision",
        SuspensionReason::OperatorRequest => "operator_request",
        SuspensionReason::Recovery => "recovery",
    }
}

/// Projects one durable journal record into the wire-level
/// [`ApiLoopEvent`].
///
/// `loopward_core::LoopEvent` has no `Serialize` implementation (Core is
/// pure and has no wire concerns), so there is no generic way yet to turn
/// one into a structured `payload`. This is a real, scoped simplification:
/// the wire event carries its type, correlation-free (Phase One's
/// `RecordedEvent` does not track per-event Episode/Session/Attempt
/// correlation either), and an approximate integrity digest computed over
/// its `Debug` rendering rather than a canonical payload encoding. A future
/// task that needs faithful event payloads on the wire should add a
/// `LoopEvent -> serde_json::Value` projection instead of widening this
/// function further.
fn project_event(loop_id: LoopId, recorded: &RecordedEvent) -> ApiLoopEvent {
    let debug_repr = format!("{:?}", recorded.event);
    ApiLoopEvent {
        schema_version: recorded.schema_version.clone(),
        event_id: recorded.event_id,
        journal_position: recorded.sequence,
        loop_id,
        aggregate_revision: recorded.aggregate_revision,
        occurred_at: recorded.recorded_at,
        recorded_at: recorded.recorded_at,
        kind: recorded.event.kind().to_owned(),
        durability: EventDurability::Durable,
        correlation: EventCorrelation::default(),
        payload: serde_json::Value::Null,
        integrity: loopward_api::event::EventIntegrity {
            payload_digest: crate::context::content_digest(debug_repr.as_bytes()),
        },
    }
}

fn store_error(error: &StoreError) -> LoopwardError {
    match error {
        StoreError::LoopNotFound(_) => LoopwardError {
            code: LoopwardErrorCode::NotFound,
            message: error.to_string(),
            retryable: false,
            trace_ref: None,
        },
        StoreError::RevisionConflict { .. } | StoreError::CheckpointVersionConflict { .. } => {
            LoopwardError {
                code: LoopwardErrorCode::Conflict,
                message: error.to_string(),
                retryable: true,
                trace_ref: None,
            }
        }
        StoreError::StaleFencingToken { .. } | StoreError::LeaseConflict(_) => LoopwardError {
            code: LoopwardErrorCode::LeaseUnavailable,
            message: error.to_string(),
            retryable: true,
            trace_ref: None,
        },
        StoreError::LeaseNotHeld(_) => LoopwardError {
            code: LoopwardErrorCode::LeaseUnavailable,
            message: error.to_string(),
            retryable: false,
            trace_ref: None,
        },
        StoreError::Backend(_) => LoopwardError {
            code: LoopwardErrorCode::Internal,
            message: error.to_string(),
            retryable: true,
            trace_ref: None,
        },
    }
}

fn controller_error(error: ControllerError) -> LoopwardError {
    match error {
        ControllerError::Store(source) => store_error(&source),
        ControllerError::Domain(source) => LoopwardError {
            code: LoopwardErrorCode::InvalidStateTransition,
            message: source.to_string(),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::Hydration(source) => LoopwardError {
            code: LoopwardErrorCode::HydrationFailed,
            message: source.to_string(),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::Verification(source) => LoopwardError {
            code: LoopwardErrorCode::VerificationFailed,
            message: source.to_string(),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::Runner(source) => LoopwardError {
            code: LoopwardErrorCode::RunnerFailed,
            message: source.to_string(),
            retryable: source.retryable,
            trace_ref: source.trace_ref,
        },
        ControllerError::Resource(source) => LoopwardError {
            code: LoopwardErrorCode::Internal,
            message: source.to_string(),
            retryable: true,
            trace_ref: None,
        },
        ControllerError::Serialization(source) => LoopwardError {
            code: LoopwardErrorCode::Internal,
            message: source.to_string(),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::UnknownVerificationPlan(reference) => LoopwardError {
            code: LoopwardErrorCode::InvalidRequest,
            message: format!("no verification plan is registered for `{reference}`"),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::NoWorkUnit(loop_id) => LoopwardError {
            code: LoopwardErrorCode::InvalidRequest,
            message: format!("loop {loop_id} has no work unit assigned"),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::RecoveryRequired(loop_id) => LoopwardError {
            code: LoopwardErrorCode::RecoveryRequired,
            message: format!("loop {loop_id} requires recovery before it can advance"),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::UnboundedRunUntil => LoopwardError {
            code: LoopwardErrorCode::InvalidRequest,
            message: ControllerError::UnboundedRunUntil.to_string(),
            retryable: false,
            trace_ref: None,
        },
        ControllerError::Invariant(detail) => LoopwardError {
            code: LoopwardErrorCode::InvalidStateTransition,
            message: detail,
            retryable: false,
            trace_ref: None,
        },
    }
}
