//! The pure transition interface: [`decide`] and [`evolve`].
//!
//! `decide` answers *what would happen*: it validates a command against the
//! current state and the effective policy and returns the durable events it
//! would produce, or refuses. `evolve` answers *what the state becomes*: it
//! applies one already-decided event. Neither performs I/O, reads a clock,
//! allocates an identifier, or mutates in place — `evolve` takes the state
//! by value and returns the successor, so a refused transition cannot leave
//! a half-applied aggregate behind.
//!
//! Two rules shape almost every branch below:
//!
//! * **A runner result is a proposal.** [`LifecycleCommand::RecordResult`]
//!   can only move a Work Unit to `AwaitingVerification`. Nothing but
//!   [`LifecycleCommand::RecordVerification`] with
//!   [`VerificationOutcome::Accepted`] can accept it
//!   (`docs/06-verification-and-recovery.md` §2).
//! * **Only documented edges.** Where a command spans two documented
//!   transitions — draining then committing, running then collecting — it
//!   returns *both* events rather than inventing a shortcut edge that
//!   `docs/07-session-rotation.md` does not draw.

use loopward_api::ids::{EpisodeId, SessionId};
use loopward_api::resource::{OpaqueRef, ResourceRef};
use loopward_api::runner::SuspensionReason;
use loopward_api::work_unit::WorkUnitStatus;

use crate::command::LifecycleCommand;
use crate::event::{LoopEvent, RotationDisposition, SessionCloseOutcome};
use crate::invariants::{DomainError, Entity};
use crate::model::{
    AggregateState, AttemptStatus, Checkpoint, Episode, EpisodeKind, EpisodeStatus, LoopStatus,
    RotationReason, RuntimeSession, SessionStatus, SideEffectClass, VerificationOutcome,
    WorkUnitAttempt, work_unit_status_is_terminal, work_unit_status_label,
};
use crate::policy::{EffectivePolicy, SessionReusePolicy};

/// Decides which durable events a command produces against a given state.
///
/// # Errors
///
/// Returns a [`DomainError`] if the command is not admissible: an
/// undocumented transition, a violated invariant, a missing entity, a stale
/// revision, or a policy refusal. A refusal produces no events at all.
pub fn decide(
    state: &AggregateState,
    command: &LifecycleCommand,
    policy: &EffectivePolicy,
) -> Result<Vec<LoopEvent>, DomainError> {
    check_loop_gate(state, command)?;

    match command {
        LifecycleCommand::StartEpisode { episode_id } => start_episode(state, *episode_id),
        LifecycleCommand::MarkHydrated { manifest } => mark_hydrated(state, manifest),
        LifecycleCommand::StartSession {
            session_id,
            generation,
        } => start_session(state, *session_id, *generation),
        LifecycleCommand::PrepareAttempt {
            attempt_id,
            idempotency_key,
        } => prepare_attempt(state, policy, *attempt_id, idempotency_key),
        LifecycleCommand::RecordRunnerStarted { external_ref } => {
            record_runner_started(state, external_ref)
        }
        LifecycleCommand::RecordResult { result } => record_result(state, result),
        LifecycleCommand::RecordVerification { outcome } => record_verification(state, *outcome),
        LifecycleCommand::RequestRotation { reason } => request_rotation(state, policy, *reason),
        LifecycleCommand::CommitCheckpoint { checkpoint } => commit_checkpoint(state, checkpoint),
        LifecycleCommand::Suspend { reason } => {
            Ok(vec![LoopEvent::LoopSuspended { reason: *reason }])
        }
        LifecycleCommand::Complete => complete(state),
        LifecycleCommand::Fail { code } => Ok(vec![LoopEvent::LoopFailed { code: code.clone() }]),
    }
}

/// Applies one decided event, returning the successor state.
///
/// # Errors
///
/// Returns [`DomainError::UnapplicableEvent`] if the event does not refer to
/// the entities present in `state`. This is a consistency check on a journal
/// replay, not a second policy gate: an event produced by [`decide`] against
/// the same state always applies.
pub fn evolve(state: AggregateState, event: &LoopEvent) -> Result<AggregateState, DomainError> {
    let mut next = state;
    match event {
        LoopEvent::EpisodeStarted {
            episode_id,
            kind,
            base_checkpoint_version,
        } => {
            apply_episode_started(&mut next, *episode_id, *kind, *base_checkpoint_version);
            Ok(())
        }
        LoopEvent::ContextHydrated {
            episode_id,
            manifest,
        } => apply_context_hydrated(&mut next, *episode_id, manifest),
        LoopEvent::SessionStarted {
            session_id,
            episode_id,
            generation,
            boot_manifest_ref,
        } => apply_session_started(
            &mut next,
            *session_id,
            *episode_id,
            *generation,
            boot_manifest_ref,
        ),
        LoopEvent::AttemptPrepared {
            attempt_id,
            work_unit_id,
            episode_id,
            ordinal,
            idempotency_key,
        } => apply_attempt_prepared(
            &mut next,
            &AttemptRecord {
                attempt_id: *attempt_id,
                work_unit_id: *work_unit_id,
                episode_id: *episode_id,
                ordinal: *ordinal,
                idempotency_key,
            },
            event,
        ),
        LoopEvent::AttemptStarting { attempt_id } => {
            apply_attempt_status(&mut next, event, *attempt_id, AttemptStatus::Starting)
        }
        LoopEvent::AttemptStarted {
            attempt_id,
            external_ref,
        } => apply_attempt_started(&mut next, *attempt_id, external_ref),
        LoopEvent::AttemptCollecting { attempt_id } => {
            apply_attempt_status(&mut next, event, *attempt_id, AttemptStatus::Collecting)
        }
        LoopEvent::AttemptCompleted {
            attempt_id,
            work_unit_id,
            result,
        } => apply_attempt_completed(&mut next, *attempt_id, *work_unit_id, result, event),
        LoopEvent::VerificationCompleted {
            work_unit_id,
            attempt_id,
            outcome,
        } => apply_verification(&mut next, *attempt_id, *work_unit_id, *outcome, event),
        LoopEvent::RotationRequested {
            session_id,
            reason,
            disposition,
            ..
        } => apply_rotation_requested(&mut next, *session_id, *reason, *disposition, event),
        LoopEvent::SessionClosed {
            session_id,
            outcome,
        } => apply_session_closed(&mut next, *session_id, *outcome, event),
        LoopEvent::EpisodeCommitting { episode_id } => {
            let episode = require_episode_mut(&mut next, event.kind(), *episode_id)?;
            episode.status = EpisodeStatus::Committing;
            Ok(())
        }
        LoopEvent::CheckpointCommitted { checkpoint } => {
            apply_checkpoint_committed(&mut next, checkpoint);
            Ok(())
        }
        LoopEvent::LoopSuspended { .. } => {
            apply_loop_suspended(&mut next);
            Ok(())
        }
        LoopEvent::LoopCompleted => {
            next.execution.status = LoopStatus::Completed;
            Ok(())
        }
        LoopEvent::LoopFailed { .. } => {
            apply_loop_failed(&mut next);
            Ok(())
        }
    }?;

    next.execution.aggregate_revision = next.execution.aggregate_revision.saturating_add(1);
    resync_projection(&mut next);
    Ok(next)
}

// ---------------------------------------------------------------------------
// Decision helpers
// ---------------------------------------------------------------------------

fn check_loop_gate(state: &AggregateState, command: &LifecycleCommand) -> Result<(), DomainError> {
    let status = state.execution.status;
    if command.permitted_loop_statuses().contains(&status) {
        return Ok(());
    }
    Err(DomainError::InvalidTransition {
        entity: Entity::Loop,
        from: status.label(),
        to: loop_transition_target(command),
    })
}

fn loop_transition_target(command: &LifecycleCommand) -> &'static str {
    match command {
        LifecycleCommand::Suspend { .. } => LoopStatus::Suspended.label(),
        LifecycleCommand::Complete => LoopStatus::Completed.label(),
        LifecycleCommand::Fail { .. } => LoopStatus::Failed.label(),
        _ => LoopStatus::Active.label(),
    }
}

fn missing(entity: Entity, command: &'static str) -> DomainError {
    DomainError::MissingEntity { entity, command }
}

/// Refuses to open a new Episode or Runtime Session generation while an
/// Attempt from the previous one is still unreconciled.
///
/// Recovery reconciles the active Attempt by identity and idempotency key
/// before resuming (`docs/06-verification-and-recovery.md` §12); starting a
/// fresh generation on top of an in-flight external effect would silently
/// abandon it.
///
/// [`AttemptStatus::Ambiguous`] counts as unreconciled even though it is
/// terminal: it is precisely the state in which Loopward cannot tell whether
/// the external effect happened, so continuing would risk repeating it. It
/// is cleared by reconciliation, which is not part of Phase One's command
/// surface — an ambiguous Attempt therefore halts the Loop rather than
/// being retried past.
fn require_reconciled_attempt(state: &AggregateState) -> Result<(), DomainError> {
    let unreconciled = state.open_attempt().or_else(|| {
        state
            .attempt
            .as_ref()
            .filter(|attempt| attempt.status == AttemptStatus::Ambiguous)
    });
    if let Some(attempt) = unreconciled {
        return Err(DomainError::InvariantViolation {
            invariant: "attempt.reconciled_before_next_generation",
            detail: format!(
                "Attempt {} is `{}` and must be reconciled first",
                attempt.id,
                attempt.status.label()
            ),
        });
    }
    Ok(())
}

fn start_episode(
    state: &AggregateState,
    episode_id: EpisodeId,
) -> Result<Vec<LoopEvent>, DomainError> {
    if let Some(active) = state.active_episode() {
        return Err(DomainError::InvariantViolation {
            invariant: "loop.at_most_one_active_episode",
            detail: format!(
                "Episode {} is `{}`; it must reach a terminal state first",
                active.id,
                active.status.label()
            ),
        });
    }
    require_reconciled_attempt(state)?;
    // The rotation sequence closes the current generation *before* creating
    // the next Episode (`docs/07-session-rotation.md` §6).
    if let Some(session) = state.open_session() {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Session,
            from: session.status.label(),
            to: SessionStatus::Closed.label(),
        });
    }
    if let Some(previous) = &state.episode {
        if previous.id == episode_id {
            return Err(DomainError::IdentityMismatch {
                entity: Entity::Episode,
                detail: format!("Episode {episode_id} has already run"),
            });
        }
        if previous.terminal_checkpoint_version.is_none() {
            return Err(DomainError::InvariantViolation {
                invariant: "episode.terminal_state_produces_checkpoint",
                detail: format!(
                    "Episode {} ended `{}` without a committed Checkpoint",
                    previous.id,
                    previous.status.label()
                ),
            });
        }
    }
    Ok(vec![LoopEvent::EpisodeStarted {
        episode_id,
        kind: next_episode_kind(state),
        base_checkpoint_version: state.checkpoint.version,
    }])
}

/// Picks the kind of the Episode a [`LifecycleCommand::StartEpisode`] opens.
///
/// Verification runs in a distinct, bounded context assembled for review and
/// must not inherit the implementation trajectory
/// (`docs/06-verification-and-recovery.md` §8), so a Work Unit awaiting
/// verification opens a verification Episode. Remediation and recovery
/// Episodes are started by the runtime, which knows why it is recovering.
fn next_episode_kind(state: &AggregateState) -> EpisodeKind {
    match state.work_unit.as_ref().map(|work_unit| work_unit.status) {
        Some(WorkUnitStatus::AwaitingVerification) => EpisodeKind::Verification,
        _ => EpisodeKind::Execution,
    }
}

fn mark_hydrated(
    state: &AggregateState,
    manifest: &ResourceRef,
) -> Result<Vec<LoopEvent>, DomainError> {
    let episode = state
        .episode
        .as_ref()
        .ok_or_else(|| missing(Entity::Episode, "mark_hydrated"))?;
    if !matches!(
        episode.status,
        EpisodeStatus::Preparing | EpisodeStatus::Recovering
    ) {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Episode,
            from: episode.status.label(),
            to: EpisodeStatus::Hydrating.label(),
        });
    }
    Ok(vec![LoopEvent::ContextHydrated {
        episode_id: episode.id,
        manifest: manifest.clone(),
    }])
}

fn start_session(
    state: &AggregateState,
    session_id: SessionId,
    generation: u32,
) -> Result<Vec<LoopEvent>, DomainError> {
    require_reconciled_attempt(state)?;
    if let Some(session) = &state.session {
        // A closed or failed generation is never reopened, and an open one
        // is not replaced underneath itself.
        if session.id == session_id || !session.status.is_terminal() {
            return Err(DomainError::InvalidTransition {
                entity: Entity::Session,
                from: session.status.label(),
                to: SessionStatus::Running.label(),
            });
        }
        if generation <= session.generation {
            return Err(DomainError::InvariantViolation {
                invariant: "session.generation_strictly_increases",
                detail: format!(
                    "generation {generation} does not follow generation {}",
                    session.generation
                ),
            });
        }
    }

    let episode = state
        .episode
        .as_ref()
        .ok_or_else(|| missing(Entity::Episode, "start_session"))?;
    if episode.status != EpisodeStatus::Hydrating {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Episode,
            from: episode.status.label(),
            to: EpisodeStatus::Executing.label(),
        });
    }
    let boot_manifest_ref =
        episode
            .hydrated_manifest
            .clone()
            .ok_or_else(|| DomainError::InvariantViolation {
                invariant: "session.requires_hydrated_boot_package",
                detail: format!("Episode {} has no hydrated Boot Package", episode.id),
            })?;

    Ok(vec![LoopEvent::SessionStarted {
        session_id,
        episode_id: episode.id,
        generation,
        boot_manifest_ref,
    }])
}

fn prepare_attempt(
    state: &AggregateState,
    policy: &EffectivePolicy,
    attempt_id: loopward_api::ids::AttemptId,
    idempotency_key: &str,
) -> Result<Vec<LoopEvent>, DomainError> {
    let session = state
        .session
        .as_ref()
        .ok_or_else(|| missing(Entity::Session, "prepare_attempt"))?;
    if session.status != SessionStatus::Running {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Session,
            from: session.status.label(),
            to: SessionStatus::Running.label(),
        });
    }
    let episode = state
        .episode
        .as_ref()
        .ok_or_else(|| missing(Entity::Episode, "prepare_attempt"))?;
    if episode.status != EpisodeStatus::Executing {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Episode,
            from: episode.status.label(),
            to: EpisodeStatus::Executing.label(),
        });
    }
    let work_unit = state
        .work_unit
        .as_ref()
        .ok_or_else(|| missing(Entity::WorkUnit, "prepare_attempt"))?;
    if !matches!(
        work_unit.status,
        WorkUnitStatus::Pending | WorkUnitStatus::Active
    ) {
        return Err(DomainError::InvalidTransition {
            entity: Entity::WorkUnit,
            from: work_unit_status_label(work_unit.status),
            to: work_unit_status_label(WorkUnitStatus::Active),
        });
    }

    let mut ordinal = 1;
    if let Some(previous) = &state.attempt {
        if previous.status == AttemptStatus::Ambiguous {
            // Never repeat an external effect whose outcome is unknown.
            return Err(DomainError::InvariantViolation {
                invariant: "attempt.reconciled_before_next_generation",
                detail: format!(
                    "Attempt {} is `ambiguous` and must be reconciled before a retry",
                    previous.id
                ),
            });
        }
        if !previous.status.is_terminal() {
            return Err(DomainError::InvalidTransition {
                entity: Entity::Attempt,
                from: previous.status.label(),
                to: AttemptStatus::Prepared.label(),
            });
        }
        if previous.id == attempt_id {
            return Err(DomainError::IdentityMismatch {
                entity: Entity::Attempt,
                detail: format!("Attempt {attempt_id} has already run"),
            });
        }
        if policy.session_reuse == SessionReusePolicy::FreshPerWorkUnit
            && previous.session_id == session.id
        {
            return Err(DomainError::PolicyViolation {
                policy: "session.fresh_per_work_unit",
                detail: format!(
                    "Session generation {} already ran Attempt {}; rotate first",
                    session.generation, previous.id
                ),
            });
        }
        if previous.work_unit_id == work_unit.id {
            ordinal = previous.ordinal.saturating_add(1);
        }
    }
    if let Some(maximum) = policy.max_attempts_per_work_unit
        && ordinal > maximum
    {
        return Err(DomainError::PolicyViolation {
            policy: "work_unit.max_attempts",
            detail: format!("Attempt {ordinal} exceeds the maximum of {maximum}"),
        });
    }
    if idempotency_key.is_empty() {
        return Err(DomainError::InvariantViolation {
            invariant: "attempt.requires_idempotency_key",
            detail: "the idempotency key must be stable and non-empty".to_owned(),
        });
    }

    Ok(vec![LoopEvent::AttemptPrepared {
        attempt_id,
        work_unit_id: work_unit.id,
        episode_id: episode.id,
        ordinal,
        idempotency_key: idempotency_key.to_owned(),
    }])
}

fn record_runner_started(
    state: &AggregateState,
    external_ref: &OpaqueRef,
) -> Result<Vec<LoopEvent>, DomainError> {
    let session = state
        .session
        .as_ref()
        .ok_or_else(|| missing(Entity::Session, "record_runner_started"))?;
    if session.status != SessionStatus::Running {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Session,
            from: session.status.label(),
            to: SessionStatus::Running.label(),
        });
    }
    let attempt = state
        .attempt
        .as_ref()
        .ok_or_else(|| missing(Entity::Attempt, "record_runner_started"))?;
    let mut events = Vec::with_capacity(2);
    match attempt.status {
        AttemptStatus::Prepared => events.push(LoopEvent::AttemptStarting {
            attempt_id: attempt.id,
        }),
        AttemptStatus::Starting => {}
        other => {
            return Err(DomainError::InvalidTransition {
                entity: Entity::Attempt,
                from: other.label(),
                to: AttemptStatus::Running.label(),
            });
        }
    }
    events.push(LoopEvent::AttemptStarted {
        attempt_id: attempt.id,
        external_ref: external_ref.clone(),
    });
    Ok(events)
}

fn record_result(
    state: &AggregateState,
    result: &ResourceRef,
) -> Result<Vec<LoopEvent>, DomainError> {
    let session = state
        .session
        .as_ref()
        .ok_or_else(|| missing(Entity::Session, "record_result"))?;
    // Collecting a result is the normal path both while running and while
    // draining for rotation (`docs/07-session-rotation.md` §6).
    if !matches!(
        session.status,
        SessionStatus::Running | SessionStatus::Draining
    ) {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Session,
            from: session.status.label(),
            to: SessionStatus::Draining.label(),
        });
    }
    let attempt = state
        .attempt
        .as_ref()
        .ok_or_else(|| missing(Entity::Attempt, "record_result"))?;
    let work_unit = state
        .work_unit
        .as_ref()
        .ok_or_else(|| missing(Entity::WorkUnit, "record_result"))?;

    let mut events = Vec::with_capacity(2);
    match attempt.status {
        AttemptStatus::Running => events.push(LoopEvent::AttemptCollecting {
            attempt_id: attempt.id,
        }),
        AttemptStatus::Collecting => {}
        other => {
            return Err(DomainError::InvalidTransition {
                entity: Entity::Attempt,
                from: other.label(),
                to: AttemptStatus::Collecting.label(),
            });
        }
    }
    events.push(LoopEvent::AttemptCompleted {
        attempt_id: attempt.id,
        work_unit_id: work_unit.id,
        result: result.clone(),
    });
    Ok(events)
}

fn record_verification(
    state: &AggregateState,
    outcome: VerificationOutcome,
) -> Result<Vec<LoopEvent>, DomainError> {
    let work_unit = state
        .work_unit
        .as_ref()
        .ok_or_else(|| missing(Entity::WorkUnit, "record_verification"))?;
    if work_unit.status != WorkUnitStatus::AwaitingVerification {
        return Err(DomainError::InvalidTransition {
            entity: Entity::WorkUnit,
            from: work_unit_status_label(work_unit.status),
            to: work_unit_status_label(verified_work_unit_status(outcome)),
        });
    }
    let attempt = state
        .attempt
        .as_ref()
        .ok_or_else(|| missing(Entity::Attempt, "record_verification"))?;
    if attempt.status != AttemptStatus::Succeeded || attempt.result_ref.is_none() {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Attempt,
            from: attempt.status.label(),
            to: AttemptStatus::Succeeded.label(),
        });
    }

    let mut events = vec![LoopEvent::VerificationCompleted {
        work_unit_id: work_unit.id,
        attempt_id: attempt.id,
        outcome,
    }];
    // A human or policy gate that cannot conclude suspends the Loop with a
    // structured request (`docs/06-verification-and-recovery.md` §9).
    if outcome == VerificationOutcome::Suspended {
        events.push(LoopEvent::LoopSuspended {
            reason: SuspensionReason::VerificationPending,
        });
    }
    Ok(events)
}

/// The Work Unit status a verification outcome produces.
///
/// A stale finding leaves the Work Unit awaiting verification: the target
/// changed, so the run is re-evaluated against the current target rather
/// than being credited or blamed
/// (`docs/06-verification-and-recovery.md` §6, §13).
fn verified_work_unit_status(outcome: VerificationOutcome) -> WorkUnitStatus {
    match outcome {
        VerificationOutcome::Accepted => WorkUnitStatus::Accepted,
        VerificationOutcome::Rejected | VerificationOutcome::Inconclusive => {
            WorkUnitStatus::Pending
        }
        VerificationOutcome::Stale => WorkUnitStatus::AwaitingVerification,
        VerificationOutcome::Suspended => WorkUnitStatus::Suspended,
    }
}

fn request_rotation(
    state: &AggregateState,
    policy: &EffectivePolicy,
    reason: RotationReason,
) -> Result<Vec<LoopEvent>, DomainError> {
    let session = state
        .session
        .as_ref()
        .ok_or_else(|| missing(Entity::Session, "request_rotation"))?;
    let episode = state
        .episode
        .as_ref()
        .ok_or_else(|| missing(Entity::Episode, "request_rotation"))?;

    // Runner failure mandates closure: the generation cannot be drained, so
    // it fails and the Episode recovers (`docs/07-session-rotation.md` §8).
    let close_as_failed = reason.is_runner_failure() && policy.close_session_on_runner_failure;
    if close_as_failed {
        if !matches!(
            session.status,
            SessionStatus::Starting | SessionStatus::Running | SessionStatus::Draining
        ) {
            return Err(DomainError::InvalidTransition {
                entity: Entity::Session,
                from: session.status.label(),
                to: SessionStatus::Failed.label(),
            });
        }
        if !matches!(
            episode.status,
            EpisodeStatus::Executing | EpisodeStatus::Draining | EpisodeStatus::Committing
        ) {
            return Err(DomainError::InvalidTransition {
                entity: Entity::Episode,
                from: episode.status.label(),
                to: EpisodeStatus::Recovering.label(),
            });
        }
        let mut events = vec![
            LoopEvent::RotationRequested {
                session_id: session.id,
                from_generation: session.generation,
                reason,
                disposition: RotationDisposition::AbandonGeneration,
            },
            LoopEvent::SessionClosed {
                session_id: session.id,
                outcome: SessionCloseOutcome::Failed,
            },
        ];
        // An external effect whose outcome cannot be observed suspends the
        // Loop: "unsafe unknown side effects produce AMBIGUOUS and suspend
        // the Loop" (`docs/03-durable-state-and-transactions.md` §4) and
        // "start outcome uncertain -> reconcile; suspend if ambiguity
        // remains" (`docs/06-verification-and-recovery.md` §13). Without
        // this, the next generation could silently retry an external effect
        // that may already have happened.
        if state.attempt.as_ref().is_some_and(|attempt| {
            matches!(
                attempt.status,
                AttemptStatus::Starting | AttemptStatus::Running
            )
        }) {
            events.push(LoopEvent::LoopSuspended {
                reason: SuspensionReason::Recovery,
            });
        }
        return Ok(events);
    }

    if session.status != SessionStatus::Running {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Session,
            from: session.status.label(),
            to: SessionStatus::Draining.label(),
        });
    }
    if episode.status != EpisodeStatus::Executing {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Episode,
            from: episode.status.label(),
            to: EpisodeStatus::Draining.label(),
        });
    }
    Ok(vec![LoopEvent::RotationRequested {
        session_id: session.id,
        from_generation: session.generation,
        reason,
        disposition: RotationDisposition::Drain,
    }])
}

fn commit_checkpoint(
    state: &AggregateState,
    checkpoint: &Checkpoint,
) -> Result<Vec<LoopEvent>, DomainError> {
    let episode = state
        .episode
        .as_ref()
        .ok_or_else(|| missing(Entity::Episode, "commit_checkpoint"))?;
    let already_checkpointed = episode.terminal_checkpoint_version.is_some();
    let commits_normally = matches!(
        episode.status,
        EpisodeStatus::Draining | EpisodeStatus::Committing
    );
    // Every terminal Episode state produces a committed Checkpoint, so a
    // suspended or aborted Episode may still record its own — once.
    let records_terminal_checkpoint = episode.status.is_terminal() && !already_checkpointed;
    if !commits_normally && !records_terminal_checkpoint {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Episode,
            from: episode.status.label(),
            to: EpisodeStatus::Completed.label(),
        });
    }
    if let Some(session) = &state.session
        && !session.status.is_terminal()
        && session.status != SessionStatus::Draining
    {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Session,
            from: session.status.label(),
            to: SessionStatus::Closed.label(),
        });
    }

    if checkpoint.loop_id != state.execution.id {
        return Err(DomainError::IdentityMismatch {
            entity: Entity::Checkpoint,
            detail: format!(
                "Checkpoint belongs to Loop {} but this is Loop {}",
                checkpoint.loop_id, state.execution.id
            ),
        });
    }
    if checkpoint.version <= state.checkpoint.version {
        return Err(DomainError::NonMonotonicCheckpoint {
            committed: state.checkpoint.version,
            proposed: checkpoint.version,
        });
    }
    if checkpoint.aggregate_revision != state.execution.aggregate_revision {
        return Err(DomainError::RevisionConflict {
            expected: state.execution.aggregate_revision,
            found: checkpoint.aggregate_revision,
        });
    }
    if checkpoint.parent != Some(state.checkpoint.id) {
        return Err(DomainError::InvariantViolation {
            invariant: "checkpoint.parent_links_to_committed",
            detail: format!(
                "proposed parent {:?} is not the committed Checkpoint {}",
                checkpoint.parent, state.checkpoint.id
            ),
        });
    }

    let mut events = Vec::with_capacity(3);
    if episode.status == EpisodeStatus::Draining {
        events.push(LoopEvent::EpisodeCommitting {
            episode_id: episode.id,
        });
    }
    events.push(LoopEvent::CheckpointCommitted {
        checkpoint: checkpoint.clone(),
    });
    if let Some(session) = &state.session
        && session.status == SessionStatus::Draining
    {
        events.push(LoopEvent::SessionClosed {
            session_id: session.id,
            outcome: SessionCloseOutcome::Closed,
        });
    }
    Ok(events)
}

fn complete(state: &AggregateState) -> Result<Vec<LoopEvent>, DomainError> {
    // The completion gate: no required Work Unit remains active, and no
    // Episode is still in flight (`docs/06-verification-and-recovery.md`
    // §11).
    if let Some(work_unit) = &state.work_unit
        && !matches!(
            work_unit.status,
            WorkUnitStatus::Accepted | WorkUnitStatus::Cancelled
        )
    {
        return Err(DomainError::InvalidTransition {
            entity: Entity::WorkUnit,
            from: work_unit_status_label(work_unit.status),
            to: work_unit_status_label(WorkUnitStatus::Accepted),
        });
    }
    if let Some(attempt) = state.open_attempt() {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Attempt,
            from: attempt.status.label(),
            to: AttemptStatus::Succeeded.label(),
        });
    }
    if let Some(session) = state.open_session() {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Session,
            from: session.status.label(),
            to: SessionStatus::Closed.label(),
        });
    }
    if let Some(episode) = state.active_episode() {
        return Err(DomainError::InvalidTransition {
            entity: Entity::Episode,
            from: episode.status.label(),
            to: EpisodeStatus::Completed.label(),
        });
    }
    Ok(vec![LoopEvent::LoopCompleted])
}

// ---------------------------------------------------------------------------
// Evolution helpers
// ---------------------------------------------------------------------------

fn unapplicable(event: &LoopEvent, detail: &str) -> DomainError {
    DomainError::UnapplicableEvent {
        event: event.kind(),
        detail: detail.to_owned(),
    }
}

fn require_session<'a>(
    state: &'a AggregateState,
    event: &'static str,
) -> Result<&'a RuntimeSession, DomainError> {
    state
        .session
        .as_ref()
        .ok_or_else(|| DomainError::UnapplicableEvent {
            event,
            detail: "no Runtime Session in the projection".to_owned(),
        })
}

fn require_episode_mut<'a>(
    state: &'a mut AggregateState,
    event: &'static str,
    episode_id: EpisodeId,
) -> Result<&'a mut Episode, DomainError> {
    match state.episode.as_mut() {
        Some(episode) if episode.id == episode_id => Ok(episode),
        _ => Err(DomainError::UnapplicableEvent {
            event,
            detail: format!("Episode {episode_id} is not the current Episode"),
        }),
    }
}

fn require_attempt_mut<'a>(
    state: &'a mut AggregateState,
    event: &'static str,
    attempt_id: loopward_api::ids::AttemptId,
) -> Result<&'a mut WorkUnitAttempt, DomainError> {
    match state.attempt.as_mut() {
        Some(attempt) if attempt.id == attempt_id => Ok(attempt),
        _ => Err(DomainError::UnapplicableEvent {
            event,
            detail: format!("Attempt {attempt_id} is not the current Attempt"),
        }),
    }
}

fn apply_episode_started(
    state: &mut AggregateState,
    episode_id: EpisodeId,
    kind: EpisodeKind,
    base_checkpoint_version: u64,
) {
    state.episode = Some(Episode {
        id: episode_id,
        loop_id: state.execution.id,
        kind,
        status: EpisodeStatus::Preparing,
        base_checkpoint_version,
        terminal_checkpoint_version: None,
        active_session: None,
        hydrated_manifest: None,
        trace_id: None,
    });
    // The previous generation's Session and Attempt records stay in the
    // projection: Session generations increase strictly across the whole
    // Loop, and Attempt ordinals accumulate per Work Unit, so both must
    // survive an Episode boundary. Both are terminal by construction here.
}

fn apply_context_hydrated(
    state: &mut AggregateState,
    episode_id: EpisodeId,
    manifest: &ResourceRef,
) -> Result<(), DomainError> {
    let episode = require_episode_mut(state, "context.hydrated", episode_id)?;
    episode.status = EpisodeStatus::Hydrating;
    episode.hydrated_manifest = Some(manifest.clone());
    Ok(())
}

fn apply_session_started(
    state: &mut AggregateState,
    session_id: SessionId,
    episode_id: EpisodeId,
    generation: u32,
    boot_manifest_ref: &ResourceRef,
) -> Result<(), DomainError> {
    let runner = state
        .work_unit
        .as_ref()
        .map_or_else(default_runner, |work_unit| work_unit.runner.clone());
    let loop_id = state.execution.id;
    let episode = require_episode_mut(state, "session.started", episode_id)?;
    episode.status = EpisodeStatus::Executing;
    episode.active_session = Some(session_id);
    state.session = Some(RuntimeSession {
        id: session_id,
        loop_id,
        episode_id,
        generation,
        status: SessionStatus::Running,
        runner,
        context_budget: None,
        boot_manifest_ref: boot_manifest_ref.clone(),
        external_session_ref: None,
        rotation_intent: None,
    });
    Ok(())
}

fn default_runner() -> loopward_api::resource::RunnerRef {
    loopward_api::resource::RunnerRef {
        name: "unassigned".to_owned(),
        profile: None,
    }
}

/// The fields carried by an `attempt.prepared` event.
struct AttemptRecord<'a> {
    attempt_id: loopward_api::ids::AttemptId,
    work_unit_id: loopward_api::ids::WorkUnitId,
    episode_id: EpisodeId,
    ordinal: u32,
    idempotency_key: &'a str,
}

fn apply_attempt_prepared(
    state: &mut AggregateState,
    record: &AttemptRecord<'_>,
    event: &LoopEvent,
) -> Result<(), DomainError> {
    let session = require_session(state, event.kind())?;
    let session_id = session.id;
    let boot_package_ref = session.boot_manifest_ref.clone();
    let work_unit = state
        .work_unit
        .as_mut()
        .ok_or_else(|| unapplicable(event, "no Work Unit"))?;
    if work_unit.id != record.work_unit_id {
        return Err(unapplicable(event, "Work Unit identity mismatch"));
    }
    work_unit.status = WorkUnitStatus::Active;
    state.attempt = Some(WorkUnitAttempt {
        id: record.attempt_id,
        work_unit_id: record.work_unit_id,
        episode_id: record.episode_id,
        session_id,
        ordinal: record.ordinal,
        status: AttemptStatus::Prepared,
        idempotency_key: record.idempotency_key.to_owned(),
        capabilities_snapshot: None,
        boot_package_ref,
        external_execution_ref: None,
        side_effect_class: SideEffectClass::Idempotent,
        result_ref: None,
        verification_outcome: None,
    });
    Ok(())
}

fn apply_attempt_status(
    state: &mut AggregateState,
    event: &LoopEvent,
    attempt_id: loopward_api::ids::AttemptId,
    status: AttemptStatus,
) -> Result<(), DomainError> {
    require_attempt_mut(state, event.kind(), attempt_id)?.status = status;
    Ok(())
}

fn apply_attempt_started(
    state: &mut AggregateState,
    attempt_id: loopward_api::ids::AttemptId,
    external_ref: &OpaqueRef,
) -> Result<(), DomainError> {
    let attempt = require_attempt_mut(state, "attempt.started", attempt_id)?;
    attempt.status = AttemptStatus::Running;
    attempt.external_execution_ref = Some(external_ref.clone());
    Ok(())
}

fn apply_attempt_completed(
    state: &mut AggregateState,
    attempt_id: loopward_api::ids::AttemptId,
    work_unit_id: loopward_api::ids::WorkUnitId,
    result: &ResourceRef,
    event: &LoopEvent,
) -> Result<(), DomainError> {
    let attempt = require_attempt_mut(state, event.kind(), attempt_id)?;
    attempt.status = AttemptStatus::Succeeded;
    attempt.result_ref = Some(result.clone());
    let work_unit = state
        .work_unit
        .as_mut()
        .ok_or_else(|| unapplicable(event, "no Work Unit"))?;
    if work_unit.id != work_unit_id {
        return Err(unapplicable(event, "Work Unit identity mismatch"));
    }
    // The runner succeeded; the result is still only a proposal.
    work_unit.status = WorkUnitStatus::AwaitingVerification;
    Ok(())
}

fn apply_verification(
    state: &mut AggregateState,
    attempt_id: loopward_api::ids::AttemptId,
    work_unit_id: loopward_api::ids::WorkUnitId,
    outcome: VerificationOutcome,
    event: &LoopEvent,
) -> Result<(), DomainError> {
    let attempt = require_attempt_mut(state, event.kind(), attempt_id)?;
    attempt.verification_outcome = Some(outcome);
    let work_unit = state
        .work_unit
        .as_mut()
        .ok_or_else(|| unapplicable(event, "no Work Unit"))?;
    if work_unit.id != work_unit_id {
        return Err(unapplicable(event, "Work Unit identity mismatch"));
    }
    work_unit.status = verified_work_unit_status(outcome);
    Ok(())
}

fn apply_rotation_requested(
    state: &mut AggregateState,
    session_id: SessionId,
    reason: RotationReason,
    disposition: RotationDisposition,
    event: &LoopEvent,
) -> Result<(), DomainError> {
    let failing = disposition == RotationDisposition::AbandonGeneration;
    match state.session.as_mut() {
        Some(session) if session.id == session_id => {
            session.rotation_intent = Some(reason);
            if !failing {
                session.status = SessionStatus::Draining;
            }
        }
        _ => return Err(unapplicable(event, "not the current Runtime Session")),
    }
    if let Some(episode) = state.episode.as_mut() {
        episode.status = if failing {
            EpisodeStatus::Recovering
        } else {
            EpisodeStatus::Draining
        };
    }
    if failing && let Some(attempt) = state.attempt.as_mut() {
        // The generation is gone, so an in-flight Attempt cannot be
        // observed through it. An unobservable external effect is
        // ambiguous, not failed: Loopward never assumes an external
        // operation failed merely because it did not record the result
        // (`docs/03-durable-state-and-transactions.md` §4, §9).
        attempt.status = match attempt.status {
            AttemptStatus::Starting | AttemptStatus::Running => AttemptStatus::Ambiguous,
            AttemptStatus::Collecting => AttemptStatus::Failed,
            AttemptStatus::Planned | AttemptStatus::Prepared => AttemptStatus::Cancelled,
            other => other,
        };
        if attempt.status != AttemptStatus::Succeeded {
            attempt.result_ref = None;
        }
    }
    Ok(())
}

fn apply_session_closed(
    state: &mut AggregateState,
    session_id: SessionId,
    outcome: SessionCloseOutcome,
    event: &LoopEvent,
) -> Result<(), DomainError> {
    match state.session.as_mut() {
        Some(session) if session.id == session_id => {
            session.status = match outcome {
                SessionCloseOutcome::Closed => SessionStatus::Closed,
                SessionCloseOutcome::Failed => SessionStatus::Failed,
            };
        }
        _ => return Err(unapplicable(event, "not the current Runtime Session")),
    }
    if let Some(episode) = state.episode.as_mut() {
        episode.active_session = None;
    }
    Ok(())
}

fn apply_checkpoint_committed(state: &mut AggregateState, checkpoint: &Checkpoint) {
    if let Some(episode) = state.episode.as_mut() {
        if !episode.status.is_terminal() {
            episode.status = EpisodeStatus::Completed;
        }
        episode.terminal_checkpoint_version = Some(checkpoint.version);
    }
    state.execution.checkpoint_version = checkpoint.version;
    state.checkpoint = checkpoint.clone();
}

fn apply_loop_suspended(state: &mut AggregateState) {
    state.execution.status = LoopStatus::Suspended;
    if let Some(episode) = state.episode.as_mut()
        && !episode.status.is_terminal()
    {
        episode.status = EpisodeStatus::Suspended;
    }
    if let Some(session) = state.session.as_mut()
        && session.status == SessionStatus::Running
    {
        session.status = SessionStatus::Draining;
    }
    if let Some(attempt) = state.attempt.as_mut()
        && attempt.status == AttemptStatus::Running
    {
        attempt.status = AttemptStatus::Suspended;
    }
    if let Some(work_unit) = state.work_unit.as_mut()
        && work_unit.status == WorkUnitStatus::Active
    {
        work_unit.status = WorkUnitStatus::Suspended;
    }
}

fn apply_loop_failed(state: &mut AggregateState) {
    state.execution.status = LoopStatus::Failed;
    if let Some(attempt) = state.attempt.as_mut() {
        attempt.status = match attempt.status {
            AttemptStatus::Planned
            | AttemptStatus::Prepared
            | AttemptStatus::Starting
            | AttemptStatus::Running => AttemptStatus::Cancelled,
            AttemptStatus::Collecting => AttemptStatus::Failed,
            other => other,
        };
        if attempt.status != AttemptStatus::Succeeded {
            attempt.result_ref = None;
        }
    }
    if let Some(session) = state.session.as_mut()
        && matches!(
            session.status,
            SessionStatus::Starting | SessionStatus::Running | SessionStatus::Draining
        )
    {
        session.status = SessionStatus::Failed;
    }
    if let Some(episode) = state.episode.as_mut()
        && !episode.status.is_terminal()
    {
        episode.status = EpisodeStatus::Aborted;
    }
    if let Some(work_unit) = state.work_unit.as_mut()
        && !work_unit_status_is_terminal(work_unit.status)
    {
        work_unit.status = WorkUnitStatus::Failed;
    }
}

/// Re-derives the Loop projection fields from the entities themselves so the
/// "at most one active Episode / Work Unit" invariants cannot drift.
fn resync_projection(state: &mut AggregateState) {
    state.execution.active_episode = state
        .episode
        .as_ref()
        .filter(|episode| !episode.status.is_terminal())
        .map(|episode| episode.id);
    state.execution.active_work_unit = state
        .work_unit
        .as_ref()
        .filter(|work_unit| !work_unit_status_is_terminal(work_unit.status))
        .map(|work_unit| work_unit.id);
    if let Some(episode) = state.episode.as_mut() {
        episode.active_session = state
            .session
            .as_ref()
            .filter(|session| !session.status.is_terminal())
            .map(|session| session.id);
    }
}
