//! Contract tests for the pure Loopward Core lifecycle state machines.
//!
//! Every state in these tests is built explicitly through [`StateBuilder`]:
//! no mocks, no hidden transitions, and no I/O. Every event returned by
//! [`decide`] is applied with [`evolve`] and re-validated with
//! [`validate_invariants`] so an illegal intermediate state cannot hide
//! behind a legal final state.

use loopward_api::ids::{AttemptId, CheckpointId, EpisodeId, LoopId, SessionId, WorkUnitId};
use loopward_api::resource::{
    OpaqueRef, OutputContract, PhaseRef, ResourceRef, RunnerRef, VerificationPlanRef,
};
use loopward_api::runner::SuspensionReason;
use loopward_api::work_unit::{
    AttemptBudget, ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitStatus,
};
use loopward_core::{
    AggregateState, AttemptStatus, Checkpoint, DomainError, EffectivePolicy, Entity, Episode,
    EpisodeKind, EpisodeStatus, LifecycleCommand, LoopEvent, LoopExecution, LoopKind, LoopStatus,
    RotationReason, RuntimeSession, SessionReusePolicy, SessionStatus, SideEffectClass,
    VerificationOutcome, WorkUnitAttempt, decide, evolve, validate_invariants,
};

// ---------------------------------------------------------------------------
// Explicit state builders
// ---------------------------------------------------------------------------

fn resource(uri: &str) -> ResourceRef {
    ResourceRef {
        uri: uri.to_owned(),
        media_type: None,
        digest: Some("sha256:0000".to_owned()),
        version: None,
        size: None,
        summary: None,
    }
}

/// Builds an explicit [`AggregateState`] one entity at a time.
///
/// The builder keeps the derived projection fields (`active_episode`,
/// `active_work_unit`, `Episode::active_session`) consistent with the
/// statuses it is given, so every state it produces satisfies
/// [`validate_invariants`] before a command is applied to it.
struct StateBuilder {
    loop_id: LoopId,
    episode_id: EpisodeId,
    work_unit_id: WorkUnitId,
    loop_status: LoopStatus,
    checkpoint_version: u64,
    episode: Option<EpisodeStatus>,
    uncheckpointed_episode: bool,
    session: Option<(SessionId, SessionStatus, u32)>,
    work_unit: Option<WorkUnitStatus>,
    attempt: Option<(AttemptId, AttemptStatus)>,
}

impl StateBuilder {
    fn new() -> Self {
        Self {
            loop_id: LoopId::new(),
            episode_id: EpisodeId::new(),
            work_unit_id: WorkUnitId::new(),
            loop_status: LoopStatus::Active,
            checkpoint_version: 1,
            episode: None,
            uncheckpointed_episode: false,
            session: None,
            work_unit: None,
            attempt: None,
        }
    }

    fn loop_status(mut self, status: LoopStatus) -> Self {
        self.loop_status = status;
        self
    }

    fn episode(mut self, status: EpisodeStatus) -> Self {
        self.episode = Some(status);
        self
    }

    /// Leaves a terminal Episode without its committed Checkpoint, the state
    /// a Loop is in between `Fail`/`Suspend` and the final
    /// `CommitCheckpoint`.
    fn uncheckpointed_episode(mut self) -> Self {
        self.uncheckpointed_episode = true;
        self
    }

    fn session(mut self, status: SessionStatus, generation: u32) -> Self {
        self.session = Some((SessionId::new(), status, generation));
        self
    }

    fn work_unit(mut self, status: WorkUnitStatus) -> Self {
        self.work_unit = Some(status);
        self
    }

    fn attempt(mut self, status: AttemptStatus) -> Self {
        self.attempt = Some((AttemptId::new(), status));
        self
    }

    fn build_episode(&self) -> Option<Episode> {
        self.episode.map(|status| Episode {
            id: self.episode_id,
            loop_id: self.loop_id,
            kind: EpisodeKind::Execution,
            status,
            base_checkpoint_version: self.checkpoint_version,
            terminal_checkpoint_version: (status.is_terminal() && !self.uncheckpointed_episode)
                .then_some(self.checkpoint_version),
            active_session: self
                .session
                .as_ref()
                .filter(|(_, session_status, _)| !session_status.is_terminal())
                .map(|(id, _, _)| *id),
            hydrated_manifest: Some(resource("resource://boot/manifest-1")),
            trace_id: None,
        })
    }

    fn build_session(&self) -> Option<RuntimeSession> {
        self.session.map(|(id, status, generation)| RuntimeSession {
            id,
            loop_id: self.loop_id,
            episode_id: self.episode_id,
            generation,
            status,
            runner: RunnerRef {
                name: "command".to_owned(),
                profile: None,
            },
            context_budget: None,
            boot_manifest_ref: resource("resource://boot/manifest-1"),
            external_session_ref: Some(OpaqueRef::new("session-external-1")),
            rotation_intent: None,
        })
    }

    fn build_work_unit(&self) -> Option<WorkUnit> {
        self.work_unit.map(|status| WorkUnit {
            id: self.work_unit_id,
            kind: WorkUnitKind::new("domain.example"),
            objective: "advance the durable task".to_owned(),
            status,
            inputs: Vec::new(),
            constraints: Vec::new(),
            output_contract: OutputContract {
                schema: "loopward.work-unit-result/v0.1".to_owned(),
            },
            verification_plan: VerificationPlanRef {
                reference: "verification-plan://plan-1".to_owned(),
                version: 1,
            },
            runner: RunnerRef {
                name: "command".to_owned(),
                profile: None,
            },
            budget: ResourceBudget {
                context: None,
                attempts: Some(AttemptBudget { maximum: None }),
            },
            retry_policy: RetryPolicy::default(),
        })
    }

    fn build_attempt(&self, session: Option<&RuntimeSession>) -> Option<WorkUnitAttempt> {
        self.attempt.map(|(id, status)| WorkUnitAttempt {
            id,
            work_unit_id: self.work_unit_id,
            episode_id: self.episode_id,
            session_id: session.map_or_else(SessionId::new, |session| session.id),
            ordinal: 1,
            status,
            idempotency_key: "attempt-1".to_owned(),
            capabilities_snapshot: None,
            boot_package_ref: resource("resource://boot/manifest-1"),
            external_execution_ref: status
                .requires_external_execution_ref()
                .then(|| OpaqueRef::new("run-external-1")),
            side_effect_class: SideEffectClass::Idempotent,
            result_ref: matches!(status, AttemptStatus::Succeeded)
                .then(|| resource("resource://result/1")),
            // A Work Unit is only ever `Accepted` on the back of an
            // Attempt carrying an accepting verification outcome, so a
            // builder asked for that pair must produce it.
            verification_outcome: (matches!(status, AttemptStatus::Succeeded)
                && self.work_unit == Some(WorkUnitStatus::Accepted))
            .then_some(VerificationOutcome::Accepted),
        })
    }

    fn build(self) -> AggregateState {
        let checkpoint = Checkpoint {
            id: CheckpointId::new(),
            loop_id: self.loop_id,
            version: self.checkpoint_version,
            aggregate_revision: 0,
            parent: None,
            episode_id: None,
            integrity_digest: None,
        };
        let episode = self.build_episode();
        let session = self.build_session();
        let work_unit = self.build_work_unit();
        let attempt = self.build_attempt(session.as_ref());

        let active_episode = episode
            .as_ref()
            .filter(|episode| !episode.status.is_terminal())
            .map(|episode| episode.id);
        let active_work_unit = work_unit
            .as_ref()
            .filter(|work_unit| !loopward_core::work_unit_status_is_terminal(work_unit.status))
            .map(|work_unit| work_unit.id);

        let execution = LoopExecution {
            id: self.loop_id,
            kind: LoopKind::new("domain.example"),
            status: self.loop_status,
            phase: PhaseRef::new("implementation"),
            aggregate_revision: 7,
            checkpoint_version: self.checkpoint_version,
            active_episode: if self.loop_status.is_terminal() {
                None
            } else {
                active_episode
            },
            active_work_unit: if self.loop_status.is_terminal() {
                None
            } else {
                active_work_unit
            },
            episode_count: 0,
            budget: ResourceBudget::default(),
        };

        let state = AggregateState {
            execution,
            episode,
            session,
            work_unit,
            attempt,
            checkpoint,
        };
        validate_invariants(&state).expect("builder produces a state that satisfies invariants");
        state
    }
}

/// Applies every decided event in order, validating invariants after each one.
fn apply_all(state: AggregateState, events: &[LoopEvent]) -> AggregateState {
    let mut current = state;
    for event in events {
        current = evolve(current, event).expect("decided event applies to the state it came from");
        validate_invariants(&current).expect("every evolved state satisfies every invariant");
    }
    current
}

fn next_checkpoint(state: &AggregateState) -> Checkpoint {
    Checkpoint {
        id: CheckpointId::new(),
        loop_id: state.execution.id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: state.episode.as_ref().map(|episode| episode.id),
        integrity_digest: Some("sha256:1111".to_owned()),
    }
}

// ---------------------------------------------------------------------------
// Step 1 lifecycle contract tests
// ---------------------------------------------------------------------------

#[test]
fn closed_session_cannot_return_to_running() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Hydrating)
        .session(SessionStatus::Closed, 3)
        .build();
    let closed_session_id = state.session.as_ref().expect("session present").id;

    let error = decide(
        &state,
        &LifecycleCommand::StartSession {
            session_id: closed_session_id,
            generation: 3,
        },
        &policy,
    )
    .expect_err("a closed Runtime Session generation is never reopened");
    assert!(
        matches!(
            error,
            DomainError::InvalidTransition {
                entity: Entity::Session,
                from: "closed",
                to: "running",
            }
        ),
        "unexpected error: {error:?}"
    );

    // The next generation is a different Session, and is admissible.
    let events = decide(
        &state,
        &LifecycleCommand::StartSession {
            session_id: SessionId::new(),
            generation: 4,
        },
        &policy,
    )
    .expect("continuation creates the next generation");
    let next = apply_all(state, &events);
    let session = next.session.as_ref().expect("session present");
    assert_eq!(session.status, SessionStatus::Running);
    assert_eq!(session.generation, 4);
    assert_ne!(session.id, closed_session_id);
}

#[test]
fn terminal_loop_cannot_start_an_episode() {
    let policy = EffectivePolicy::default();
    for status in [
        LoopStatus::Completed,
        LoopStatus::Failed,
        LoopStatus::Cancelled,
    ] {
        let state = StateBuilder::new().loop_status(status).build();
        let error = decide(
            &state,
            &LifecycleCommand::StartEpisode {
                episode_id: EpisodeId::new(),
            },
            &policy,
        )
        .expect_err("a terminal Loop cannot advance");
        assert!(
            matches!(
                error,
                DomainError::InvalidTransition {
                    entity: Entity::Loop,
                    ..
                }
            ),
            "unexpected error for {status:?}: {error:?}"
        );
    }
}

#[test]
fn accepted_attempt_does_not_complete_work_unit_before_verification() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Executing)
        .session(SessionStatus::Running, 1)
        .work_unit(WorkUnitStatus::Active)
        .attempt(AttemptStatus::Running)
        .build();

    let events = decide(
        &state,
        &LifecycleCommand::RecordResult {
            result: resource("resource://result/1"),
        },
        &policy,
    )
    .expect("a running attempt can record its collected result");
    let state = apply_all(state, &events);

    // The runner succeeded, but its result is still only a proposal.
    let attempt = state.attempt.as_ref().expect("attempt present");
    assert_eq!(attempt.status, AttemptStatus::Succeeded);
    assert!(attempt.result_ref.is_some());
    assert!(attempt.verification_outcome.is_none());

    let work_unit = state.work_unit.as_ref().expect("work unit present");
    assert_eq!(
        work_unit.status,
        WorkUnitStatus::AwaitingVerification,
        "a collected result must await verification, never be accepted outright"
    );

    // The completion gate refuses to close a Loop over unverified work.
    let error = decide(&state, &LifecycleCommand::Complete, &policy)
        .expect_err("an unverified Work Unit blocks Loop completion");
    assert!(
        matches!(
            error,
            DomainError::InvalidTransition {
                entity: Entity::WorkUnit,
                from: "awaiting_verification",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );

    // Only an accepting verification outcome may complete the Work Unit.
    let events = decide(
        &state,
        &LifecycleCommand::RecordVerification {
            outcome: VerificationOutcome::Accepted,
        },
        &policy,
    )
    .expect("verification may run against a collected result");
    let state = apply_all(state, &events);
    assert_eq!(
        state.work_unit.as_ref().expect("work unit present").status,
        WorkUnitStatus::Accepted
    );
    assert_eq!(
        state
            .attempt
            .as_ref()
            .expect("attempt present")
            .verification_outcome,
        Some(VerificationOutcome::Accepted)
    );
}

#[test]
fn one_loop_has_at_most_one_active_episode() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Executing)
        .build();

    let error = decide(
        &state,
        &LifecycleCommand::StartEpisode {
            episode_id: EpisodeId::new(),
        },
        &policy,
    )
    .expect_err("a second Episode cannot start while one is active");
    assert!(
        matches!(
            error,
            DomainError::InvariantViolation {
                invariant: "loop.at_most_one_active_episode",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );

    // The same invariant is enforced structurally, not only by `decide`.
    let mut inconsistent = state;
    inconsistent.execution.active_episode = Some(EpisodeId::new());
    let error =
        validate_invariants(&inconsistent).expect_err("projection must match the active Episode");
    assert!(
        matches!(
            error,
            DomainError::InvariantViolation {
                invariant: "loop.at_most_one_active_episode",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn rotation_records_intent_before_closing_the_session() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Executing)
        .session(SessionStatus::Running, 1)
        .work_unit(WorkUnitStatus::Accepted)
        .attempt(AttemptStatus::Succeeded)
        .build();

    let events = decide(
        &state,
        &LifecycleCommand::RequestRotation {
            reason: RotationReason::WorkUnitBoundary,
        },
        &policy,
    )
    .expect("a running session can be asked to rotate");
    let state = apply_all(state, &events);
    assert_eq!(
        state.session.as_ref().expect("session present").status,
        SessionStatus::Draining
    );
    assert_eq!(
        state
            .session
            .as_ref()
            .expect("session present")
            .rotation_intent,
        Some(RotationReason::WorkUnitBoundary)
    );

    let checkpoint = next_checkpoint(&state);
    let events = decide(
        &state,
        &LifecycleCommand::CommitCheckpoint { checkpoint },
        &policy,
    )
    .expect("a drained session commits its terminal checkpoint");
    let state = apply_all(state, &events);
    assert_eq!(
        state.session.as_ref().expect("session present").status,
        SessionStatus::Closed
    );
    assert_eq!(
        state.episode.as_ref().expect("episode present").status,
        EpisodeStatus::Completed
    );
    assert_eq!(state.execution.active_episode, None);
    assert_eq!(state.execution.checkpoint_version, 2);
}

#[test]
fn suspension_drains_the_session_and_suspends_the_episode() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Executing)
        .session(SessionStatus::Running, 1)
        .work_unit(WorkUnitStatus::Active)
        .attempt(AttemptStatus::Running)
        .build();

    let events = decide(
        &state,
        &LifecycleCommand::Suspend {
            reason: SuspensionReason::PolicyDecision,
        },
        &policy,
    )
    .expect("an active Loop can suspend");
    let state = apply_all(state, &events);

    assert_eq!(state.execution.status, LoopStatus::Suspended);
    assert_eq!(
        state.episode.as_ref().expect("episode present").status,
        EpisodeStatus::Suspended
    );
    assert_eq!(
        state.session.as_ref().expect("session present").status,
        SessionStatus::Draining
    );
    assert_eq!(
        state.attempt.as_ref().expect("attempt present").status,
        AttemptStatus::Suspended
    );
}

// ---------------------------------------------------------------------------
// Step 5: property-style transition tables
//
// These tests enumerate the full cross product of every status in every
// state machine against every lifecycle command, and assert two things:
//
//   1. Soundness — no command ever produces a transition the documented
//      state machine does not draw. The Episode, Runtime Session, and Work
//      Unit Attempt tables are transcribed verbatim from the mermaid
//      diagrams in `docs/07-session-rotation.md` §3–§5 and live on the
//      status enums themselves. The Loop table is derived (see the
//      `LoopStatus` docs) because no document enumerates it.
//   2. Closure — the exact set of edges the command surface can produce is
//      pinned, so an accidentally widened state machine fails the build
//      rather than passing silently.
// ---------------------------------------------------------------------------

use std::collections::BTreeSet;

type Edges = BTreeSet<(&'static str, &'static str)>;

#[derive(Default)]
struct EdgeLog {
    loops: Edges,
    episodes: Edges,
    sessions: Edges,
    attempts: Edges,
}

/// One instance of every [`LifecycleCommand`] variant, plus every branching
/// argument (each [`VerificationOutcome`], and rotation both with and
/// without a runner failure).
fn command_catalogue(state: &AggregateState) -> Vec<LifecycleCommand> {
    let mut commands = vec![
        LifecycleCommand::StartEpisode {
            episode_id: EpisodeId::new(),
        },
        LifecycleCommand::MarkHydrated {
            manifest: resource("resource://boot/manifest-next"),
        },
        LifecycleCommand::StartSession {
            session_id: SessionId::new(),
            generation: 99,
        },
        LifecycleCommand::PrepareAttempt {
            attempt_id: AttemptId::new(),
            idempotency_key: "attempt-next".to_owned(),
        },
        LifecycleCommand::RecordRunnerStarted {
            external_ref: OpaqueRef::new("run-external-next"),
        },
        LifecycleCommand::RecordResult {
            result: resource("resource://result/next"),
        },
        LifecycleCommand::RequestRotation {
            reason: RotationReason::WorkUnitBoundary,
        },
        LifecycleCommand::RequestRotation {
            reason: RotationReason::RunnerFailure,
        },
        LifecycleCommand::CommitCheckpoint {
            checkpoint: next_checkpoint(state),
        },
        LifecycleCommand::Suspend {
            reason: SuspensionReason::OperatorRequest,
        },
        LifecycleCommand::Complete,
        LifecycleCommand::Fail {
            code: "terminal_failure".to_owned(),
        },
    ];
    commands.extend(
        VerificationOutcome::ALL
            .iter()
            .map(|outcome| LifecycleCommand::RecordVerification { outcome: *outcome }),
    );
    commands
}

/// The status each *other* entity is put in so the command under test can
/// actually reach the entity being tabulated instead of being refused
/// earlier by an unrelated precondition.
fn permissive_episode_status(command: &LifecycleCommand) -> EpisodeStatus {
    match command {
        LifecycleCommand::MarkHydrated { .. } => EpisodeStatus::Preparing,
        LifecycleCommand::StartSession { .. } => EpisodeStatus::Hydrating,
        LifecycleCommand::CommitCheckpoint { .. } => EpisodeStatus::Draining,
        // The completion gate requires no Episode still in flight.
        LifecycleCommand::StartEpisode { .. } | LifecycleCommand::Complete => {
            EpisodeStatus::Completed
        }
        _ => EpisodeStatus::Executing,
    }
}

fn permissive_session_status(command: &LifecycleCommand) -> SessionStatus {
    match command {
        LifecycleCommand::StartSession { .. }
        | LifecycleCommand::StartEpisode { .. }
        | LifecycleCommand::Complete => SessionStatus::Closed,
        LifecycleCommand::CommitCheckpoint { .. } => SessionStatus::Draining,
        _ => SessionStatus::Running,
    }
}

fn permissive_work_unit_status(command: &LifecycleCommand) -> WorkUnitStatus {
    match command {
        LifecycleCommand::RecordVerification { .. } => WorkUnitStatus::AwaitingVerification,
        LifecycleCommand::PrepareAttempt { .. } => WorkUnitStatus::Pending,
        LifecycleCommand::Complete => WorkUnitStatus::Cancelled,
        _ => WorkUnitStatus::Active,
    }
}

fn permissive_attempt_status(command: &LifecycleCommand) -> Option<AttemptStatus> {
    match command {
        LifecycleCommand::PrepareAttempt { .. } | LifecycleCommand::Complete => None,
        LifecycleCommand::RecordRunnerStarted { .. } => Some(AttemptStatus::Prepared),
        LifecycleCommand::RecordVerification { .. } => Some(AttemptStatus::Succeeded),
        _ => Some(AttemptStatus::Running),
    }
}

/// Records the transition each entity actually made, refusing any that the
/// documented machine does not draw.
///
/// A replaced entity (a new Episode, the next Session generation, a retried
/// Attempt) is a creation, not a transition: its predecessor must have been
/// terminal, and no edge is logged.
fn record_transitions(before: &AggregateState, after: &AggregateState, log: &mut EdgeLog) {
    if before.execution.status != after.execution.status {
        log.loops.insert((
            before.execution.status.label(),
            after.execution.status.label(),
        ));
    }

    match (&before.episode, &after.episode) {
        (Some(old), Some(new)) if old.id == new.id => {
            if old.status != new.status {
                log.episodes
                    .insert((old.status.label(), new.status.label()));
            }
        }
        (Some(old), Some(_)) => assert!(
            old.status.is_terminal(),
            "a new Episode replaced Episode {} while it was `{}`",
            old.id,
            old.status.label()
        ),
        _ => {}
    }

    match (&before.session, &after.session) {
        (Some(old), Some(new)) if old.id == new.id => {
            if old.status != new.status {
                log.sessions
                    .insert((old.status.label(), new.status.label()));
            }
        }
        (Some(old), Some(new)) => {
            assert!(
                old.status.is_terminal(),
                "generation {} was replaced while `{}`",
                old.generation,
                old.status.label()
            );
            assert!(
                new.generation > old.generation,
                "generation must strictly increase"
            );
        }
        _ => {}
    }

    match (&before.attempt, &after.attempt) {
        (Some(old), Some(new)) if old.id == new.id => {
            if old.status != new.status {
                log.attempts
                    .insert((old.status.label(), new.status.label()));
            }
        }
        (Some(old), Some(_)) => assert!(
            old.status.is_terminal(),
            "a new Attempt replaced Attempt {} while it was `{}`",
            old.id,
            old.status.label()
        ),
        _ => {}
    }
}

fn edge_set<S: Copy>(edges: &[(S, S)], label: fn(S) -> &'static str) -> Edges {
    edges
        .iter()
        .map(|(from, to)| (label(*from), label(*to)))
        .collect()
}

/// Runs every command in the catalogue against `state`, applying and
/// validating each decided event, and logging the transitions produced.
fn exercise(state: &AggregateState, log: &mut EdgeLog) {
    for command in command_catalogue(state) {
        let Ok(events) = decide(state, &command, &EffectivePolicy::default()) else {
            // A refusal is always acceptable: this test proves no
            // undocumented transition is ever *produced*.
            continue;
        };
        // Step through the events one at a time: a command that spans two
        // documented edges must be caught producing both, not a shortcut
        // between its endpoints.
        let mut current = state.clone();
        for event in &events {
            let next = evolve(current.clone(), event).expect("decided event applies");
            validate_invariants(&next).expect("every evolved state satisfies every invariant");
            record_transitions(&current, &next, log);
            current = next;
        }
    }
}

fn run_all_tables() -> EdgeLog {
    let mut log = EdgeLog::default();

    // Loop table: every LoopStatus against every command.
    for status in LoopStatus::ALL {
        for command in command_catalogue(&StateBuilder::new().build()) {
            let mut builder = StateBuilder::new().loop_status(*status);
            if !status.is_terminal() {
                builder = builder
                    .episode(permissive_episode_status(&command))
                    .session(permissive_session_status(&command), 1)
                    .work_unit(permissive_work_unit_status(&command));
                if let Some(attempt) = permissive_attempt_status(&command) {
                    builder = builder.attempt(attempt);
                }
            }
            let state = builder.build();
            exercise(&state, &mut log);
        }
    }

    // Episode, Session, and Attempt tables.
    for status in EpisodeStatus::ALL {
        for command in command_catalogue(&StateBuilder::new().build()) {
            let mut builder = StateBuilder::new()
                .episode(*status)
                .session(permissive_session_status(&command), 1)
                .work_unit(permissive_work_unit_status(&command));
            if let Some(attempt) = permissive_attempt_status(&command) {
                builder = builder.attempt(attempt);
            }
            exercise(&builder.build(), &mut log);
        }
    }
    for status in SessionStatus::ALL {
        for command in command_catalogue(&StateBuilder::new().build()) {
            let mut builder = StateBuilder::new()
                .episode(permissive_episode_status(&command))
                .session(*status, 1)
                .work_unit(permissive_work_unit_status(&command));
            if let Some(attempt) = permissive_attempt_status(&command) {
                builder = builder.attempt(attempt);
            }
            exercise(&builder.build(), &mut log);
        }
    }
    for status in AttemptStatus::ALL {
        for command in command_catalogue(&StateBuilder::new().build()) {
            let builder = StateBuilder::new()
                .episode(permissive_episode_status(&command))
                .session(permissive_session_status(&command), 1)
                .work_unit(permissive_work_unit_status(&command))
                .attempt(*status);
            exercise(&builder.build(), &mut log);
        }
    }

    log
}

#[test]
fn no_command_produces_an_undocumented_transition() {
    let log = run_all_tables();

    let documented_loops = edge_set(LoopStatus::DOCUMENTED_EDGES, LoopStatus::label);
    let documented_episodes = edge_set(EpisodeStatus::DOCUMENTED_EDGES, EpisodeStatus::label);
    let documented_sessions = edge_set(SessionStatus::DOCUMENTED_EDGES, SessionStatus::label);
    let documented_attempts = edge_set(AttemptStatus::DOCUMENTED_EDGES, AttemptStatus::label);

    for (name, produced, documented) in [
        ("Loop", &log.loops, &documented_loops),
        ("Episode", &log.episodes, &documented_episodes),
        ("Runtime Session", &log.sessions, &documented_sessions),
        ("Work Unit Attempt", &log.attempts, &documented_attempts),
    ] {
        let undocumented: Vec<_> = produced.difference(documented).collect();
        assert!(
            undocumented.is_empty(),
            "{name} produced undocumented transitions: {undocumented:?}"
        );
    }
}

#[test]
fn the_command_surface_produces_exactly_these_documented_edges() {
    let log = run_all_tables();

    // Pinned so widening a state machine is a deliberate, reviewed change.
    // Edges documented in `docs/07-session-rotation.md` but absent here are
    // reachable only through commands Phase One does not yet expose
    // (resumption, cancellation, explicit reconciliation).
    assert_eq!(
        log.loops.into_iter().collect::<Vec<_>>(),
        vec![
            ("active", "completed"),
            ("active", "failed"),
            ("active", "suspended"),
            ("suspended", "failed"),
        ]
    );
    assert_eq!(
        log.episodes.into_iter().collect::<Vec<_>>(),
        vec![
            ("committing", "aborted"),
            ("committing", "completed"),
            ("committing", "recovering"),
            ("committing", "suspended"),
            ("draining", "aborted"),
            ("draining", "committing"),
            ("draining", "recovering"),
            ("draining", "suspended"),
            ("executing", "aborted"),
            ("executing", "draining"),
            ("executing", "recovering"),
            ("executing", "suspended"),
            ("hydrating", "aborted"),
            ("hydrating", "executing"),
            ("hydrating", "suspended"),
            ("preparing", "aborted"),
            ("preparing", "hydrating"),
            ("preparing", "suspended"),
            ("recovering", "aborted"),
            ("recovering", "hydrating"),
            ("recovering", "suspended"),
        ]
    );
    assert_eq!(
        log.sessions.into_iter().collect::<Vec<_>>(),
        vec![
            ("draining", "closed"),
            ("draining", "failed"),
            ("running", "draining"),
            ("running", "failed"),
            ("starting", "failed"),
        ]
    );
    assert_eq!(
        log.attempts.into_iter().collect::<Vec<_>>(),
        vec![
            ("collecting", "failed"),
            ("collecting", "succeeded"),
            ("planned", "cancelled"),
            ("prepared", "cancelled"),
            ("prepared", "starting"),
            ("running", "ambiguous"),
            ("running", "cancelled"),
            ("running", "collecting"),
            ("running", "suspended"),
            ("starting", "ambiguous"),
            ("starting", "cancelled"),
            ("starting", "running"),
        ]
    );
}

/// A terminal Loop may still record the Checkpoint its terminal Episode owes
/// — and nothing else.
///
/// `docs/07-session-rotation.md` §3 requires every terminal Episode state to
/// produce a committed Checkpoint, and `docs/06-verification-and-recovery.md`
/// §14 lists terminal completion or failure as a thing a Checkpoint may
/// represent. `Fail` and `Suspend` are exactly the paths that terminate an
/// Episode without one, so the final commit has to be admissible after the
/// Loop is already terminal. That is a *record* of the terminal state, not an
/// advance past it (`docs/02-core-domain-model.md` §2), and it is bounded to
/// happen at most once: the commit stamps `terminal_checkpoint_version`, and
/// no further Episode can ever start on a terminal Loop.
#[test]
fn terminal_loop_records_its_final_checkpoint_exactly_once() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .loop_status(LoopStatus::Failed)
        .episode(EpisodeStatus::Aborted)
        .uncheckpointed_episode()
        .session(SessionStatus::Failed, 1)
        .build();
    assert_eq!(
        state
            .episode
            .as_ref()
            .expect("episode present")
            .terminal_checkpoint_version,
        None
    );

    let events = decide(
        &state,
        &LifecycleCommand::CommitCheckpoint {
            checkpoint: next_checkpoint(&state),
        },
        &policy,
    )
    .expect("a terminal Episode still owes its Checkpoint");
    let state = apply_all(state, &events);

    assert_eq!(state.execution.status, LoopStatus::Failed);
    assert_eq!(state.execution.checkpoint_version, 2);
    assert_eq!(
        state
            .episode
            .as_ref()
            .expect("episode present")
            .terminal_checkpoint_version,
        Some(2)
    );

    // Once recorded, it cannot be recorded again: the Loop is now fully
    // drained and can advance no further by any route.
    let error = decide(
        &state,
        &LifecycleCommand::CommitCheckpoint {
            checkpoint: next_checkpoint(&state),
        },
        &policy,
    )
    .expect_err("a checkpointed terminal Episode cannot commit twice");
    assert!(
        matches!(
            error,
            DomainError::InvalidTransition {
                entity: Entity::Episode,
                from: "aborted",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn terminal_loop_refuses_every_command_but_its_final_checkpoint() {
    let policy = EffectivePolicy::default();
    for status in LoopStatus::ALL.iter().filter(|status| status.is_terminal()) {
        // A fixture with real children present, so a command is refused by
        // the Loop gate rather than merely dying on a missing entity.
        let state = StateBuilder::new()
            .loop_status(*status)
            .episode(EpisodeStatus::Aborted)
            .uncheckpointed_episode()
            .session(SessionStatus::Failed, 1)
            .work_unit(WorkUnitStatus::Failed)
            .attempt(AttemptStatus::Cancelled)
            .build();

        for command in command_catalogue(&state) {
            let outcome = decide(&state, &command, &policy);
            if matches!(command, LifecycleCommand::CommitCheckpoint { .. }) {
                outcome.expect("the terminal Episode's own Checkpoint is admissible");
                continue;
            }
            let error = outcome.expect_err(&format!(
                "terminal Loop `{}` accepted `{}`",
                status.label(),
                command.name()
            ));
            assert!(
                matches!(
                    error,
                    DomainError::InvalidTransition {
                        entity: Entity::Loop,
                        ..
                    }
                ),
                "terminal Loop `{}` refused `{}` for the wrong reason: {error:?}",
                status.label(),
                command.name()
            );
        }
    }
}

#[test]
fn terminal_sessions_and_attempts_are_never_resurrected() {
    let policy = EffectivePolicy::default();

    // A closed or failed Session generation is never reopened, whatever the
    // command.
    for status in SessionStatus::ALL
        .iter()
        .filter(|status| status.is_terminal())
    {
        for command in command_catalogue(&StateBuilder::new().build()) {
            let state = StateBuilder::new()
                .episode(permissive_episode_status(&command))
                .session(*status, 1)
                .work_unit(permissive_work_unit_status(&command))
                .build();
            let session_id = state.session.as_ref().expect("session present").id;
            let Ok(events) = decide(&state, &command, &policy) else {
                continue;
            };
            let after = apply_all(state, &events);
            let session = after.session.as_ref().expect("session present");
            assert!(
                session.id != session_id || session.status == *status,
                "`{}` reopened a `{}` Session generation",
                command.name(),
                status.label()
            );
        }
    }

    // A terminal Attempt is never resurrected: a retry is a new Attempt.
    for status in AttemptStatus::ALL
        .iter()
        .filter(|status| status.is_terminal())
    {
        for command in command_catalogue(&StateBuilder::new().build()) {
            let state = StateBuilder::new()
                .episode(permissive_episode_status(&command))
                .session(permissive_session_status(&command), 1)
                .work_unit(permissive_work_unit_status(&command))
                .attempt(*status)
                .build();
            let attempt_id = state.attempt.as_ref().expect("attempt present").id;
            let Ok(events) = decide(&state, &command, &policy) else {
                continue;
            };
            let after = apply_all(state, &events);
            let attempt = after.attempt.as_ref().expect("attempt present");
            assert!(
                attempt.id != attempt_id || attempt.status == *status,
                "`{}` moved a terminal `{}` Attempt to `{}`",
                command.name(),
                status.label(),
                attempt.status.label()
            );
        }
    }
}

#[test]
fn only_an_accepting_verification_outcome_accepts_a_work_unit() {
    let policy = EffectivePolicy::default();
    for outcome in VerificationOutcome::ALL {
        let state = StateBuilder::new()
            .episode(EpisodeStatus::Executing)
            .session(SessionStatus::Running, 1)
            .work_unit(WorkUnitStatus::AwaitingVerification)
            .attempt(AttemptStatus::Succeeded)
            .build();
        let events = decide(
            &state,
            &LifecycleCommand::RecordVerification { outcome: *outcome },
            &policy,
        )
        .expect("a collected result can always be verified");
        let after = apply_all(state, &events);
        let status = after.work_unit.as_ref().expect("work unit present").status;
        if outcome.is_accepting() {
            assert_eq!(status, WorkUnitStatus::Accepted);
        } else {
            assert_ne!(
                status,
                WorkUnitStatus::Accepted,
                "`{}` must not accept a Work Unit",
                outcome.label()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Ambiguity, fencing, and policy coverage
// ---------------------------------------------------------------------------

/// An external effect whose outcome cannot be observed must halt the Loop.
///
/// `docs/03-durable-state-and-transactions.md` §4: "Unsafe unknown side
/// effects produce `AMBIGUOUS` and suspend the Loop."
/// `docs/06-verification-and-recovery.md` §13: "Start outcome uncertain →
/// Reconcile; suspend if ambiguity remains." Retrying past an ambiguous
/// Attempt could repeat an external effect that already happened.
#[test]
fn an_ambiguous_attempt_suspends_the_loop_and_blocks_retry() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Executing)
        .session(SessionStatus::Running, 1)
        .work_unit(WorkUnitStatus::Active)
        .attempt(AttemptStatus::Running)
        .build();

    let events = decide(
        &state,
        &LifecycleCommand::RequestRotation {
            reason: RotationReason::RunnerFailure,
        },
        &policy,
    )
    .expect("a runner failure always forces the generation closed");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, LoopEvent::LoopSuspended { .. })),
        "an in-flight Attempt lost to runner failure must suspend the Loop: {events:?}"
    );
    let state = apply_all(state, &events);

    assert_eq!(
        state.attempt.as_ref().expect("attempt present").status,
        AttemptStatus::Ambiguous,
        "an unobservable external effect is ambiguous, not failed"
    );
    assert_eq!(
        state.session.as_ref().expect("session present").status,
        SessionStatus::Failed
    );
    assert_eq!(state.execution.status, LoopStatus::Suspended);

    // The suspended Loop refuses the whole recovery-and-retry sequence the
    // runtime would otherwise walk straight through.
    for command in [
        LifecycleCommand::MarkHydrated {
            manifest: resource("resource://boot/manifest-2"),
        },
        LifecycleCommand::StartSession {
            session_id: SessionId::new(),
            generation: 2,
        },
        LifecycleCommand::PrepareAttempt {
            attempt_id: AttemptId::new(),
            idempotency_key: "attempt-2".to_owned(),
        },
    ] {
        let error =
            decide(&state, &command, &policy).expect_err("a suspended Loop admits no further work");
        assert!(
            matches!(
                error,
                DomainError::InvalidTransition {
                    entity: Entity::Loop,
                    ..
                }
            ),
            "`{}` was refused for the wrong reason: {error:?}",
            command.name()
        );
    }
}

/// Even on an Active Loop — the state a future `Resume` command would
/// restore — an unreconciled ambiguous Attempt blocks the next generation
/// and any retry.
#[test]
fn an_ambiguous_attempt_is_unreconciled_even_though_it_is_terminal() {
    let policy = EffectivePolicy::default();

    let hydrating = StateBuilder::new()
        .episode(EpisodeStatus::Hydrating)
        .session(SessionStatus::Failed, 1)
        .work_unit(WorkUnitStatus::Active)
        .attempt(AttemptStatus::Ambiguous)
        .build();
    let error = decide(
        &hydrating,
        &LifecycleCommand::StartSession {
            session_id: SessionId::new(),
            generation: 2,
        },
        &policy,
    )
    .expect_err("the next generation must not start over an ambiguous effect");
    assert!(
        matches!(
            error,
            DomainError::InvariantViolation {
                invariant: "attempt.reconciled_before_next_generation",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );

    let executing = StateBuilder::new()
        .episode(EpisodeStatus::Executing)
        .session(SessionStatus::Running, 1)
        .work_unit(WorkUnitStatus::Active)
        .attempt(AttemptStatus::Ambiguous)
        .build();
    let error = decide(
        &executing,
        &LifecycleCommand::PrepareAttempt {
            attempt_id: AttemptId::new(),
            idempotency_key: "attempt-2".to_owned(),
        },
        &policy,
    )
    .expect_err("an ambiguous external effect must not be repeated");
    assert!(
        matches!(
            error,
            DomainError::InvariantViolation {
                invariant: "attempt.reconciled_before_next_generation",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
}

/// Every Checkpoint commit is fenced: monotonic version, exact expected
/// aggregate revision, parent linkage, and Loop identity
/// (`docs/03-durable-state-and-transactions.md` §2, §5, §10).
#[test]
fn checkpoint_commit_is_fenced_on_version_revision_parent_and_identity() {
    let policy = EffectivePolicy::default();
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Draining)
        .session(SessionStatus::Draining, 1)
        .build();
    let committed = state.checkpoint.version;
    let revision = state.execution.aggregate_revision;

    let commit = |checkpoint: Checkpoint| {
        decide(
            &state,
            &LifecycleCommand::CommitCheckpoint { checkpoint },
            &policy,
        )
    };

    // Non-monotonic version: re-committing the current version, and going
    // backwards, are both refused.
    for proposed in [committed, committed - 1] {
        let error = commit(Checkpoint {
            version: proposed,
            ..next_checkpoint(&state)
        })
        .expect_err("checkpoint versions increase monotonically");
        assert_eq!(
            error,
            DomainError::NonMonotonicCheckpoint {
                committed,
                proposed
            }
        );
    }

    // Stale or fabricated expected revision: the optimistic-concurrency
    // check every durable write is fenced against.
    for found in [revision - 1, revision + 1] {
        let error = commit(Checkpoint {
            aggregate_revision: found,
            ..next_checkpoint(&state)
        })
        .expect_err("a commit must carry the exact expected aggregate revision");
        assert_eq!(
            error,
            DomainError::RevisionConflict {
                expected: revision,
                found
            }
        );
    }

    // Parent linkage: a Checkpoint must continue the committed one.
    for parent in [None, Some(CheckpointId::new())] {
        let error = commit(Checkpoint {
            parent,
            ..next_checkpoint(&state)
        })
        .expect_err("a Checkpoint must link to the Checkpoint it continues");
        assert!(
            matches!(
                error,
                DomainError::InvariantViolation {
                    invariant: "checkpoint.parent_links_to_committed",
                    ..
                }
            ),
            "unexpected error: {error:?}"
        );
    }

    // Identity: another Loop's Checkpoint is never accepted here.
    let error = commit(Checkpoint {
        loop_id: LoopId::new(),
        ..next_checkpoint(&state)
    })
    .expect_err("a Checkpoint belongs to exactly one Loop");
    assert!(
        matches!(
            error,
            DomainError::IdentityMismatch {
                entity: Entity::Checkpoint,
                ..
            }
        ),
        "unexpected error: {error:?}"
    );

    // The conforming commit is accepted and advances both counters.
    let events = commit(next_checkpoint(&state)).expect("a fenced, conforming commit is accepted");
    let after = apply_all(state.clone(), &events);
    assert_eq!(after.execution.checkpoint_version, committed + 1);
    assert!(after.execution.aggregate_revision > revision);
}

/// `session_reuse` and `max_attempts_per_work_unit` are real gates, not
/// decoration (`docs/07-session-rotation.md` §8,
/// `docs/06-verification-and-recovery.md` §10).
#[test]
fn policy_bounds_session_reuse_and_retry_count() {
    let state = StateBuilder::new()
        .episode(EpisodeStatus::Executing)
        .session(SessionStatus::Running, 1)
        .work_unit(WorkUnitStatus::Pending)
        .attempt(AttemptStatus::Failed)
        .build();
    let retry = LifecycleCommand::PrepareAttempt {
        attempt_id: AttemptId::new(),
        idempotency_key: "attempt-2".to_owned(),
    };

    // Phase One's default: a fresh Runtime Session for each Work Unit, so
    // the generation that already ran an Attempt cannot run another.
    let error = decide(&state, &retry, &EffectivePolicy::default())
        .expect_err("the default policy requires rotation between Attempts");
    assert!(
        matches!(
            error,
            DomainError::PolicyViolation {
                policy: "session.fresh_per_work_unit",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );

    // Opting into reuse honors it, and the attempt budget still binds.
    let reuse = EffectivePolicy {
        session_reuse: SessionReusePolicy::AllowReuse,
        max_attempts_per_work_unit: Some(2),
        ..EffectivePolicy::default()
    };
    let events = decide(&state, &retry, &reuse).expect("reuse is opt-in but honored");
    let after = apply_all(state.clone(), &events);
    let attempt = after.attempt.as_ref().expect("attempt present");
    assert_eq!(attempt.ordinal, 2, "a retry gets the next ordinal");
    assert_eq!(attempt.status, AttemptStatus::Prepared);

    let bounded = EffectivePolicy {
        max_attempts_per_work_unit: Some(1),
        ..reuse
    };
    let error =
        decide(&state, &retry, &bounded).expect_err("remediation is bounded by the attempt budget");
    assert!(
        matches!(
            error,
            DomainError::PolicyViolation {
                policy: "work_unit.max_attempts",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
}

/// `close_session_on_runner_failure` selects between mandatory closure and
/// an ordinary drain (`docs/07-session-rotation.md` §8).
#[test]
fn policy_selects_mandatory_closure_or_drain_on_runner_failure() {
    let build = || {
        StateBuilder::new()
            .episode(EpisodeStatus::Executing)
            .session(SessionStatus::Running, 1)
            .work_unit(WorkUnitStatus::Active)
            .attempt(AttemptStatus::Running)
            .build()
    };
    let rotate = LifecycleCommand::RequestRotation {
        reason: RotationReason::RunnerFailure,
    };

    let state = build();
    let events =
        decide(&state, &rotate, &EffectivePolicy::default()).expect("closure is mandatory");
    let closed = apply_all(state, &events);
    assert_eq!(
        closed.session.as_ref().expect("session present").status,
        SessionStatus::Failed
    );
    assert_eq!(
        closed.episode.as_ref().expect("episode present").status,
        EpisodeStatus::Suspended,
        "the recovering Episode is then suspended by the ambiguity"
    );

    let lenient = EffectivePolicy {
        close_session_on_runner_failure: false,
        ..EffectivePolicy::default()
    };
    let state = build();
    let events = decide(&state, &rotate, &lenient).expect("an ordinary drain is attempted instead");
    let drained = apply_all(state, &events);
    assert_eq!(
        drained.session.as_ref().expect("session present").status,
        SessionStatus::Draining
    );
    assert_eq!(
        drained.episode.as_ref().expect("episode present").status,
        EpisodeStatus::Draining
    );
    assert_eq!(drained.execution.status, LoopStatus::Active);
    assert_eq!(
        drained.attempt.as_ref().expect("attempt present").status,
        AttemptStatus::Running
    );
}
