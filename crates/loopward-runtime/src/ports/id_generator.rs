//! The [`IdGenerator`] port: the runtime's only source of new durable
//! identifiers.
//!
//! `loopward-core` never manufactures identifiers itself — every
//! [`loopward_core::LoopEvent`] variant that introduces a new entity
//! (`EpisodeStarted::episode_id`, `AttemptPrepared::attempt_id`, ...)
//! carries an identifier the caller already decided, because Core is pure
//! and has no source of randomness or uniqueness. The Runtime mints those
//! identifiers through this port instead, so tests can inject deterministic,
//! reproducible ID sequences instead of random `UUIDv7`s.

use loopward_api::ids::{
    AttemptId, CheckpointId, EpisodeId, EventId, LoopId, OperationId, SessionId, WorkUnitId,
};

/// A source of new durable identifiers for every ID type Loopward mints at
/// runtime.
pub trait IdGenerator: Send + Sync {
    /// Mints a new [`LoopId`].
    fn new_loop_id(&self) -> LoopId;
    /// Mints a new [`EpisodeId`].
    fn new_episode_id(&self) -> EpisodeId;
    /// Mints a new [`SessionId`].
    fn new_session_id(&self) -> SessionId;
    /// Mints a new [`WorkUnitId`].
    fn new_work_unit_id(&self) -> WorkUnitId;
    /// Mints a new [`AttemptId`].
    fn new_attempt_id(&self) -> AttemptId;
    /// Mints a new [`OperationId`].
    fn new_operation_id(&self) -> OperationId;
    /// Mints a new [`EventId`].
    fn new_event_id(&self) -> EventId;
    /// Mints a new [`CheckpointId`].
    fn new_checkpoint_id(&self) -> CheckpointId;
}
