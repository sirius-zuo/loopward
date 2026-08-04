//! Deterministic [`Clock`] and [`IdGenerator`] test doubles.
//!
//! Ported from the near-identical fakes three `loopward-runtime` test files
//! (`advance_episode.rs`, `recovery_matrix.rs`, `port_contract.rs`) each
//! independently defined, so conformance tests across the workspace share
//! one implementation instead of five. [`FixedClock`] never advances on its
//! own — a test that wants time to move forward calls
//! [`FixedClock::advance`] or [`FixedClock::set`] explicitly, keeping every
//! recorded [`Timestamp`] fully reproducible.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use loopward_api::ids::{
    AttemptId, CheckpointId, EpisodeId, EventId, LoopId, OperationId, SessionId, Timestamp,
    WorkUnitId,
};
use loopward_runtime::{Clock, IdGenerator};
use uuid::Uuid;

/// A [`Clock`] whose [`Clock::now`] never changes except when the test
/// explicitly asks it to.
///
/// Every timestamp a deterministic conformance test records must be
/// reproducible; wall-clock time is never an acceptable source for one.
#[derive(Debug)]
pub struct FixedClock(Mutex<Timestamp>);

impl FixedClock {
    /// Builds a clock fixed at `initial`.
    #[must_use]
    pub fn new(initial: Timestamp) -> Self {
        Self(Mutex::new(initial))
    }

    /// The current fixed timestamp.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (only possible if a prior
    /// call panicked while holding it).
    #[must_use]
    pub fn current(&self) -> Timestamp {
        *self.0.lock().expect("fixed clock lock poisoned")
    }

    /// Moves the clock forward by `duration`. The only way this clock's time
    /// ever changes.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn advance(&self, duration: time::Duration) {
        let mut guard = self.0.lock().expect("fixed clock lock poisoned");
        *guard += duration;
    }

    /// Sets the clock to an exact timestamp, regardless of its current value.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn set(&self, at: Timestamp) {
        *self.0.lock().expect("fixed clock lock poisoned") = at;
    }
}

impl Default for FixedClock {
    /// Fixed at the Unix epoch, matching the runtime crate's own fakes.
    fn default() -> Self {
        Self::new(Timestamp::UNIX_EPOCH)
    }
}

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        self.current()
    }
}

/// A deterministic [`IdGenerator`]: every identity is a sequentially
/// numbered `UUID`, so two runs of the same scripted scenario mint the exact
/// same identities in the exact same order.
#[derive(Debug, Default)]
pub struct SequentialIdGenerator {
    next: AtomicU64,
}

impl SequentialIdGenerator {
    /// Builds a generator whose first minted id is `1`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// The next sequential `UUID`, shared by every `new_*` method below.
    fn next_uuid(&self) -> Uuid {
        let value = self.next.fetch_add(1, Ordering::SeqCst);
        Uuid::from_u128(u128::from(value))
    }
}

impl IdGenerator for SequentialIdGenerator {
    fn new_loop_id(&self) -> LoopId {
        LoopId::from_uuid(self.next_uuid())
    }

    fn new_episode_id(&self) -> EpisodeId {
        EpisodeId::from_uuid(self.next_uuid())
    }

    fn new_session_id(&self) -> SessionId {
        SessionId::from_uuid(self.next_uuid())
    }

    fn new_work_unit_id(&self) -> WorkUnitId {
        WorkUnitId::from_uuid(self.next_uuid())
    }

    fn new_attempt_id(&self) -> AttemptId {
        AttemptId::from_uuid(self.next_uuid())
    }

    fn new_operation_id(&self) -> OperationId {
        OperationId::from_uuid(self.next_uuid())
    }

    fn new_event_id(&self) -> EventId {
        EventId::from_uuid(self.next_uuid())
    }

    fn new_checkpoint_id(&self) -> CheckpointId {
        CheckpointId::from_uuid(self.next_uuid())
    }
}
