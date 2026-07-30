//! The lifecycle commands the pure reducer accepts.
//!
//! A command is a *request* to transition, never a transition itself:
//! [`crate::decide`] validates it against the current state and the
//! effective policy and answers with the events it would produce, or a
//! [`crate::DomainError`]. Commands carry no timestamps and no I/O handles;
//! the runtime supplies those when it durably journals the resulting events.

use loopward_api::ids::{AttemptId, EpisodeId, SessionId};
use loopward_api::resource::{OpaqueRef, ResourceRef};
use loopward_api::runner::SuspensionReason;

use crate::model::{Checkpoint, LoopStatus, RotationReason, VerificationOutcome};

/// One requested durable lifecycle transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleCommand {
    /// Start a new Episode from the committed Checkpoint.
    StartEpisode {
        /// The new Episode's identity.
        episode_id: EpisodeId,
    },
    /// Record that a Boot Package was hydrated for the current Episode.
    MarkHydrated {
        /// The hydrated Boot Package manifest.
        manifest: ResourceRef,
    },
    /// Start the next Runtime Session generation for the current Episode.
    StartSession {
        /// The new Session generation's identity.
        session_id: SessionId,
        /// The new generation's ordinal. Must strictly increase.
        generation: u32,
    },
    /// Durably record the intent to attempt the current Work Unit, before
    /// any external effect occurs
    /// (`docs/03-durable-state-and-transactions.md` §4).
    PrepareAttempt {
        /// The new Attempt's identity.
        attempt_id: AttemptId,
        /// The stable idempotency key the runner will be called with.
        idempotency_key: String,
    },
    /// Record the observation that the runner accepted the external call.
    RecordRunnerStarted {
        /// The runner-assigned external execution reference.
        external_ref: OpaqueRef,
    },
    /// Record the result collected from the runner. A proposal only: this
    /// cannot accept the Work Unit.
    RecordResult {
        /// The collected result resource.
        result: ResourceRef,
    },
    /// Record the outcome of the Work Unit's verification plan.
    RecordVerification {
        /// The verification outcome.
        outcome: VerificationOutcome,
    },
    /// Record rotation intent and stop admitting work to the current
    /// Runtime Session generation.
    RequestRotation {
        /// The primary rotation reason.
        reason: RotationReason,
    },
    /// Commit the authoritative Checkpoint that terminates the current
    /// Episode.
    CommitCheckpoint {
        /// The Checkpoint to commit.
        checkpoint: Checkpoint,
    },
    /// Suspend the Loop at its current safe boundary.
    Suspend {
        /// Why the Loop is suspending.
        reason: SuspensionReason,
    },
    /// Complete the Loop. Gated on there being no unaccepted Work Unit and
    /// no active Episode (`docs/06-verification-and-recovery.md` §11).
    Complete,
    /// Fail the Loop terminally.
    Fail {
        /// A stable, non-sensitive failure code.
        code: String,
    },
}

impl LifecycleCommand {
    /// The stable `snake_case` name of this command, for diagnostics.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::StartEpisode { .. } => "start_episode",
            Self::MarkHydrated { .. } => "mark_hydrated",
            Self::StartSession { .. } => "start_session",
            Self::PrepareAttempt { .. } => "prepare_attempt",
            Self::RecordRunnerStarted { .. } => "record_runner_started",
            Self::RecordResult { .. } => "record_result",
            Self::RecordVerification { .. } => "record_verification",
            Self::RequestRotation { .. } => "request_rotation",
            Self::CommitCheckpoint { .. } => "commit_checkpoint",
            Self::Suspend { .. } => "suspend",
            Self::Complete => "complete",
            Self::Fail { .. } => "fail",
        }
    }

    /// The Loop statuses this command may be issued in.
    ///
    /// Terminal Loops cannot advance (`docs/02-core-domain-model.md` §2), so
    /// every advancing command requires [`LoopStatus::Active`].
    /// [`Self::CommitCheckpoint`] is the sole exception: every terminal
    /// Episode state must still produce a committed Checkpoint
    /// (`docs/07-session-rotation.md` §3), and recording that Checkpoint is
    /// not an advance. Its Episode-level preconditions in
    /// [`crate::decide`] keep it from doing anything else.
    #[must_use]
    pub fn permitted_loop_statuses(&self) -> &'static [LoopStatus] {
        const ACTIVE: &[LoopStatus] = &[LoopStatus::Active];
        const ACTIVE_OR_SUSPENDED: &[LoopStatus] = &[LoopStatus::Active, LoopStatus::Suspended];
        match self {
            Self::CommitCheckpoint { .. } => LoopStatus::ALL,
            Self::Fail { .. } => ACTIVE_OR_SUSPENDED,
            _ => ACTIVE,
        }
    }
}
