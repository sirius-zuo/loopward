//! The durable lifecycle events the reducer produces.
//!
//! An event is a fact: it has already been decided, and [`crate::evolve`]
//! applies it without re-deciding anything. Each variant maps to one of the
//! journal event types listed in
//! `docs/03-durable-state-and-transactions.md` §6, exposed through
//! [`LoopEvent::kind`] so the runtime can write the journal without a second
//! mapping table.
//!
//! Events carry no timestamps, no journal position, and no aggregate
//! revision: those are assigned by the runtime when it commits the event,
//! and Core has no clock. This type is Core's *domain* event; the wire-level
//! journal record callers page through is
//! `loopward_api::event::LoopEvent`.

use loopward_api::ids::{AttemptId, EpisodeId, SessionId, WorkUnitId};
use loopward_api::resource::{OpaqueRef, ResourceRef};
use loopward_api::runner::SuspensionReason;

use crate::model::{Checkpoint, EpisodeKind, RotationReason, VerificationOutcome};

/// How a Runtime Session generation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionCloseOutcome {
    /// The generation drained cleanly and closed.
    Closed,
    /// The generation ended without a clean drain.
    Failed,
}

/// What a rotation does to the generation it rotates out.
///
/// This is part of the *event* rather than something [`crate::evolve`]
/// re-derives from the [`RotationReason`], because the choice depends on the
/// effective policy at decision time
/// (`EffectivePolicy::close_session_on_runner_failure`). An event is a fact;
/// re-deriving it would let the reducer disagree with the decision that
/// produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RotationDisposition {
    /// Stop admitting work and drain the runner for a structured handoff.
    Drain,
    /// The generation cannot be drained and is abandoned; the Episode
    /// recovers instead.
    AbandonGeneration,
}

/// One decided durable lifecycle fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoopEvent {
    /// A new Episode started from the committed Checkpoint.
    EpisodeStarted {
        /// The new Episode's identity.
        episode_id: EpisodeId,
        /// What the Episode is being run for.
        kind: EpisodeKind,
        /// The Checkpoint version the Episode is hydrating from.
        base_checkpoint_version: u64,
    },
    /// A Boot Package was hydrated for the current Episode.
    ContextHydrated {
        /// The Episode the Boot Package was hydrated for.
        episode_id: EpisodeId,
        /// The hydrated Boot Package manifest.
        manifest: ResourceRef,
    },
    /// A new Runtime Session generation started.
    SessionStarted {
        /// The new Session generation's identity.
        session_id: SessionId,
        /// The Episode the Session executes.
        episode_id: EpisodeId,
        /// The generation ordinal.
        generation: u32,
        /// The Boot Package manifest the Session was hydrated from.
        boot_manifest_ref: ResourceRef,
    },
    /// Intent to attempt a Work Unit was durably recorded, before any
    /// external effect.
    AttemptPrepared {
        /// The new Attempt's identity.
        attempt_id: AttemptId,
        /// The Work Unit being attempted.
        work_unit_id: WorkUnitId,
        /// The Episode the Attempt belongs to.
        episode_id: EpisodeId,
        /// The Attempt's ordinal within its Work Unit.
        ordinal: u32,
        /// The stable idempotency key for the external call.
        idempotency_key: String,
    },
    /// The external call for an Attempt is in flight.
    AttemptStarting {
        /// The Attempt whose external call was issued.
        attempt_id: AttemptId,
    },
    /// The runner accepted the external call and returned a reference.
    AttemptStarted {
        /// The Attempt that started.
        attempt_id: AttemptId,
        /// The runner-assigned external execution reference.
        external_ref: OpaqueRef,
    },
    /// The runner finished and its result is being collected.
    AttemptCollecting {
        /// The Attempt being collected from.
        attempt_id: AttemptId,
    },
    /// A result was collected from the runner. Still a proposal.
    AttemptCompleted {
        /// The Attempt that produced the result.
        attempt_id: AttemptId,
        /// The Work Unit the result belongs to.
        work_unit_id: WorkUnitId,
        /// The collected result resource.
        result: ResourceRef,
    },
    /// A verification plan concluded against a collected result.
    VerificationCompleted {
        /// The Work Unit that was verified.
        work_unit_id: WorkUnitId,
        /// The Attempt whose result was verified.
        attempt_id: AttemptId,
        /// The verification outcome.
        outcome: VerificationOutcome,
    },
    /// Rotation intent was recorded and admission stopped.
    RotationRequested {
        /// The Session generation being rotated out.
        session_id: SessionId,
        /// The generation being rotated out.
        from_generation: u32,
        /// The primary rotation reason.
        reason: RotationReason,
        /// What happens to the generation being rotated out.
        disposition: RotationDisposition,
    },
    /// A Runtime Session generation ended. It is never reopened.
    SessionClosed {
        /// The Session generation that ended.
        session_id: SessionId,
        /// How it ended.
        outcome: SessionCloseOutcome,
    },
    /// The drained handoff was accepted and the Episode entered its commit
    /// phase.
    EpisodeCommitting {
        /// The Episode entering its commit phase.
        episode_id: EpisodeId,
    },
    /// An authoritative Checkpoint was committed, terminating the Episode.
    CheckpointCommitted {
        /// The committed Checkpoint.
        checkpoint: Checkpoint,
    },
    /// The Loop suspended.
    LoopSuspended {
        /// Why the Loop suspended.
        reason: SuspensionReason,
    },
    /// The Loop completed.
    LoopCompleted,
    /// The Loop failed terminally.
    LoopFailed {
        /// A stable, non-sensitive failure code.
        code: String,
    },
}

impl LoopEvent {
    /// The durable journal event type for this event
    /// (`docs/03-durable-state-and-transactions.md` §6).
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::EpisodeStarted { .. } => "episode.started",
            Self::ContextHydrated { .. } => "context.hydrated",
            Self::SessionStarted { .. } => "session.started",
            Self::AttemptPrepared { .. } => "attempt.prepared",
            Self::AttemptStarting { .. } => "attempt.starting",
            Self::AttemptStarted { .. } => "attempt.started",
            Self::AttemptCollecting { .. } => "attempt.collecting",
            Self::AttemptCompleted { .. } => "attempt.completed",
            Self::VerificationCompleted { .. } => "verification.completed",
            Self::RotationRequested { .. } => "rotation.requested",
            Self::SessionClosed { .. } => "session.closed",
            Self::EpisodeCommitting { .. } => "episode.committing",
            Self::CheckpointCommitted { .. } => "checkpoint.committed",
            Self::LoopSuspended { .. } => "loop.suspended",
            Self::LoopCompleted => "loop.completed",
            Self::LoopFailed { .. } => "loop.failed",
        }
    }
}
