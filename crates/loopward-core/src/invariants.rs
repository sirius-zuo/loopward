//! Domain errors and the structural invariants every state must satisfy.
//!
//! [`validate_invariants`] is deliberately independent of [`crate::decide`]:
//! `decide` rejects illegal *commands*, this rejects illegal *states*. A
//! state loaded from storage, rebuilt by recovery, or produced by a future
//! command path is checked by the same predicate, so an invariant cannot be
//! bypassed by inventing a new transition.

use crate::model::{
    AggregateState, VerificationOutcome, work_unit_status_is_terminal, work_unit_status_label,
};
use loopward_api::work_unit::WorkUnitStatus;

/// Which entity a domain error is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Entity {
    /// The Loop itself.
    Loop,
    /// An Episode.
    Episode,
    /// A Runtime Session generation.
    Session,
    /// A Work Unit.
    WorkUnit,
    /// A Work Unit Attempt.
    Attempt,
    /// A Checkpoint.
    Checkpoint,
}

impl Entity {
    /// The stable `snake_case` label used in diagnostics.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Loop => "loop",
            Self::Episode => "episode",
            Self::Session => "session",
            Self::WorkUnit => "work_unit",
            Self::Attempt => "attempt",
            Self::Checkpoint => "checkpoint",
        }
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Why a command was refused or a state rejected.
///
/// Every variant is a *refusal*, never a partial application: `decide`
/// returns one of these instead of events, and `evolve` returns one instead
/// of a half-mutated state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    /// The requested transition is not part of this entity's documented
    /// state machine.
    #[error("{entity} cannot transition from `{from}` to `{to}`")]
    InvalidTransition {
        /// The entity the transition applies to.
        entity: Entity,
        /// The entity's current status label.
        from: &'static str,
        /// The requested status label.
        to: &'static str,
    },
    /// A structural invariant of the aggregate would be, or has been,
    /// violated.
    #[error("invariant `{invariant}` violated: {detail}")]
    InvariantViolation {
        /// The stable invariant name.
        invariant: &'static str,
        /// What specifically was inconsistent.
        detail: String,
    },
    /// The command requires an entity the aggregate does not currently have.
    #[error("{entity} is required for `{command}` but is absent")]
    MissingEntity {
        /// The absent entity.
        entity: Entity,
        /// The command that required it.
        command: &'static str,
    },
    /// The command targets an entity other than the current one.
    #[error("{entity} identity mismatch: {detail}")]
    IdentityMismatch {
        /// The entity whose identity did not match.
        entity: Entity,
        /// What specifically did not match.
        detail: String,
    },
    /// The effective policy refused the command.
    #[error("policy `{policy}` refused the command: {detail}")]
    PolicyViolation {
        /// The stable policy name.
        policy: &'static str,
        /// Why the policy refused.
        detail: String,
    },
    /// The command was built against a stale aggregate revision.
    #[error("expected aggregate revision {expected}, found {found}")]
    RevisionConflict {
        /// The revision the aggregate is actually at.
        expected: u64,
        /// The revision the command was built against.
        found: u64,
    },
    /// The proposed Checkpoint does not advance the committed version.
    #[error("checkpoint version {proposed} does not advance committed version {committed}")]
    NonMonotonicCheckpoint {
        /// The currently committed Checkpoint version.
        committed: u64,
        /// The proposed Checkpoint version.
        proposed: u64,
    },
    /// The event cannot be applied to the state it was handed.
    #[error("event `{event}` is not applicable: {detail}")]
    UnapplicableEvent {
        /// The event's journal type.
        event: &'static str,
        /// Why it could not be applied.
        detail: String,
    },
}

impl DomainError {
    fn invariant(invariant: &'static str, detail: impl Into<String>) -> Self {
        Self::InvariantViolation {
            invariant,
            detail: detail.into(),
        }
    }
}

/// Validates every structural invariant of an [`AggregateState`].
///
/// The invariants come from `docs/02-core-domain-model.md` §2 and
/// `docs/03-durable-state-and-transactions.md` §10. They are checked in a
/// fixed order so a given inconsistency always produces the same error.
///
/// # Errors
///
/// Returns [`DomainError::InvariantViolation`] naming the first invariant
/// that does not hold.
#[allow(clippy::too_many_lines)]
pub fn validate_invariants(state: &AggregateState) -> Result<(), DomainError> {
    let execution = &state.execution;

    // At most one active Episode, and the projection must name it.
    let active_episode = state.active_episode().map(|episode| episode.id);
    if execution.active_episode != active_episode {
        return Err(DomainError::invariant(
            "loop.at_most_one_active_episode",
            format!(
                "projection names {:?} but the current Episode is {:?}",
                execution.active_episode, active_episode
            ),
        ));
    }

    // At most one active Work Unit, and the projection must name it.
    let active_work_unit = state
        .work_unit
        .as_ref()
        .filter(|work_unit| !work_unit_status_is_terminal(work_unit.status))
        .map(|work_unit| work_unit.id);
    if execution.active_work_unit != active_work_unit {
        return Err(DomainError::invariant(
            "loop.at_most_one_active_work_unit",
            format!(
                "projection names {:?} but the current Work Unit is {:?}",
                execution.active_work_unit, active_work_unit
            ),
        ));
    }

    // Terminal Loops cannot advance, so nothing beneath them may be live.
    if execution.status.is_terminal() {
        if let Some(episode) = state.active_episode() {
            return Err(DomainError::invariant(
                "loop.terminal_has_no_live_children",
                format!(
                    "Loop is `{}` but Episode {} is `{}`",
                    execution.status.label(),
                    episode.id,
                    episode.status.label()
                ),
            ));
        }
        if let Some(session) = state.session.as_ref().filter(|s| s.status.is_live()) {
            return Err(DomainError::invariant(
                "loop.terminal_has_no_live_children",
                format!(
                    "Loop is `{}` but Session {} is `{}`",
                    execution.status.label(),
                    session.id,
                    session.status.label()
                ),
            ));
        }
    }

    // Every entity belongs to this Loop.
    if let Some(episode) = &state.episode
        && episode.loop_id != execution.id
    {
        return Err(DomainError::invariant(
            "entity.belongs_to_loop",
            format!("Episode {} belongs to Loop {}", episode.id, episode.loop_id),
        ));
    }
    if state.checkpoint.loop_id != execution.id {
        return Err(DomainError::invariant(
            "entity.belongs_to_loop",
            format!(
                "Checkpoint {} belongs to Loop {}",
                state.checkpoint.id, state.checkpoint.loop_id
            ),
        ));
    }

    // An *open* Session belongs to the current Episode. A closed or failed
    // generation stays in the projection after its Episode ends — Session
    // generations increase strictly across the whole Loop, so the last one
    // has to remain visible — and is therefore exempt from that check.
    if let Some(session) = &state.session {
        if session.loop_id != execution.id {
            return Err(DomainError::invariant(
                "entity.belongs_to_loop",
                format!("Session {} belongs to Loop {}", session.id, session.loop_id),
            ));
        }
        if !session.status.is_terminal() {
            match &state.episode {
                Some(episode) if episode.id == session.episode_id => {}
                _ => {
                    return Err(DomainError::invariant(
                        "session.belongs_to_episode",
                        format!(
                            "open Session {} names Episode {} which is not the current Episode",
                            session.id, session.episode_id
                        ),
                    ));
                }
            }
        }
        let expected_active = (!session.status.is_terminal()).then_some(session.id);
        let named_active = state
            .episode
            .as_ref()
            .and_then(|episode| episode.active_session);
        if named_active != expected_active {
            return Err(DomainError::invariant(
                "session.active_matches_status",
                format!(
                    "Episode names active Session {named_active:?} but Session {} is `{}`",
                    session.id,
                    session.status.label()
                ),
            ));
        }
    } else if let Some(episode) = &state.episode
        && episode.active_session.is_some()
    {
        return Err(DomainError::invariant(
            "session.active_matches_status",
            format!(
                "Episode {} names an active Session that is not in the projection",
                episode.id
            ),
        ));
    }

    // An Attempt belongs to the current Work Unit, Episode, and Session.
    if let Some(attempt) = &state.attempt {
        let Some(work_unit) = &state.work_unit else {
            return Err(DomainError::invariant(
                "attempt.belongs_to_work_unit",
                format!("Attempt {} has no Work Unit in the projection", attempt.id),
            ));
        };
        if attempt.work_unit_id != work_unit.id {
            return Err(DomainError::invariant(
                "attempt.belongs_to_work_unit",
                format!(
                    "Attempt {} names Work Unit {} but the current Work Unit is {}",
                    attempt.id, attempt.work_unit_id, work_unit.id
                ),
            ));
        }
        // As with Sessions, only an *open* Attempt must belong to the
        // current Episode and generation: a terminal Attempt stays in the
        // projection so ordinals keep accumulating across Episodes.
        if !attempt.status.is_terminal() {
            match &state.episode {
                Some(episode) if episode.id == attempt.episode_id => {}
                _ => {
                    return Err(DomainError::invariant(
                        "attempt.belongs_to_episode",
                        format!(
                            "open Attempt {} names Episode {} which is not the current Episode",
                            attempt.id, attempt.episode_id
                        ),
                    ));
                }
            }
            match &state.session {
                Some(session) if session.id == attempt.session_id => {}
                _ => {
                    return Err(DomainError::invariant(
                        "attempt.belongs_to_session",
                        format!(
                            "open Attempt {} names Session {} which is not the current Session",
                            attempt.id, attempt.session_id
                        ),
                    ));
                }
            }
        }
        if attempt.ordinal == 0 {
            return Err(DomainError::invariant(
                "attempt.ordinal_starts_at_one",
                format!("Attempt {} has ordinal 0", attempt.id),
            ));
        }
        if attempt.status.requires_external_execution_ref()
            && attempt.external_execution_ref.is_none()
        {
            return Err(DomainError::invariant(
                "attempt.observed_run_has_external_ref",
                format!(
                    "Attempt {} is `{}` without an external execution reference",
                    attempt.id,
                    attempt.status.label()
                ),
            ));
        }
        if attempt.result_ref.is_some() && attempt.status != crate::model::AttemptStatus::Succeeded
        {
            return Err(DomainError::invariant(
                "attempt.result_requires_success",
                format!(
                    "Attempt {} is `{}` but carries a result",
                    attempt.id,
                    attempt.status.label()
                ),
            ));
        }
    }

    // A runner result is a proposal: acceptance requires a verified result.
    if let Some(work_unit) = &state.work_unit
        && work_unit.status == WorkUnitStatus::Accepted
    {
        let verified = state.attempt.as_ref().is_some_and(|attempt| {
            attempt.result_ref.is_some()
                && attempt
                    .verification_outcome
                    .is_some_and(VerificationOutcome::is_accepting)
        });
        if !verified {
            return Err(DomainError::invariant(
                "work_unit.acceptance_requires_accepted_verification",
                format!(
                    "Work Unit {} is `{}` without an Attempt carrying an accepted verification outcome",
                    work_unit.id,
                    work_unit_status_label(work_unit.status)
                ),
            ));
        }
    }

    // Checkpoint versioning is monotonic and matches the projection.
    if execution.checkpoint_version != state.checkpoint.version {
        return Err(DomainError::invariant(
            "checkpoint.version_matches_projection",
            format!(
                "Loop is at checkpoint version {} but the committed Checkpoint is version {}",
                execution.checkpoint_version, state.checkpoint.version
            ),
        ));
    }
    if state.checkpoint.aggregate_revision > execution.aggregate_revision {
        return Err(DomainError::invariant(
            "checkpoint.revision_not_ahead_of_loop",
            format!(
                "Checkpoint was built at revision {} but the Loop is at revision {}",
                state.checkpoint.aggregate_revision, execution.aggregate_revision
            ),
        ));
    }
    if let Some(episode) = &state.episode {
        if episode.base_checkpoint_version > execution.checkpoint_version {
            return Err(DomainError::invariant(
                "episode.base_checkpoint_is_committed",
                format!(
                    "Episode {} hydrated from version {} but only version {} is committed",
                    episode.id, episode.base_checkpoint_version, execution.checkpoint_version
                ),
            ));
        }
        if let Some(terminal) = episode.terminal_checkpoint_version
            && !episode.status.is_terminal()
        {
            return Err(DomainError::invariant(
                "episode.terminal_checkpoint_requires_terminal_status",
                format!(
                    "Episode {} is `{}` but records terminal checkpoint version {}",
                    episode.id,
                    episode.status.label(),
                    terminal
                ),
            ));
        }
    }

    Ok(())
}
