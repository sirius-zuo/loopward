//! The [`Clock`] port: the runtime's only source of wall-clock time.
//!
//! `loopward-core` is pure and synchronous and has no clock of its own; the
//! Runtime reads the current time exclusively through this port so tests and
//! recovery logic can inject a fixed or scripted timestamp instead of
//! reaching for `OffsetDateTime::now_utc()` directly.

use loopward_api::ids::Timestamp;

/// A source of the current wall-clock time.
///
/// Phase One timestamps are wall-clock, not monotonic instants
/// (`docs/03-durable-state-and-transactions.md` §6: every event records a
/// timestamp); implementations are not required to guarantee strict
/// monotonicity, only that the runtime never reads the system clock
/// directly.
pub trait Clock: Send + Sync {
    /// Returns the current time.
    fn now(&self) -> Timestamp;
}
