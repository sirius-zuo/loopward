//! Event DTOs: the durable lifecycle event journal exposed through
//! `LoopService::events`, and the opaque cursor used to page through it.
//!
//! Loopward distinguishes durable lifecycle events (monotonically ordered,
//! used for audit and recovery) from ephemeral runner progress events, which
//! may be sampled or unavailable after execution (see
//! `docs/09-public-api.md` §7 and `docs/schemas/event-v0.1.yaml`).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ids::{AttemptId, EpisodeId, EventId, LoopId, SessionId, Timestamp, WorkUnitId};

/// An opaque, forward-only pagination cursor for durable or runner-reported
/// events.
///
/// Callers must treat this as opaque and pass it back unmodified; Loopward
/// does not guarantee any particular internal encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventCursor(pub String);

impl EventCursor {
    /// Wraps a raw cursor string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for EventCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Whether an event is part of the durable, recovery-relevant journal or an
/// ephemeral, best-effort progress signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDurability {
    /// Recorded in the append-only lifecycle event journal.
    Durable,
    /// A best-effort progress signal that may be sampled or unavailable
    /// after execution.
    Ephemeral,
}

/// A filter selecting which durability class of events a query should
/// return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDurabilityFilter {
    /// Only durable journal events.
    Durable,
    /// Only ephemeral progress events.
    Ephemeral,
    /// Both durable and ephemeral events.
    All,
}

/// Identifiers correlating an event to the Loop entities active when it
/// occurred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EventCorrelation {
    /// The active Episode, when applicable.
    pub episode_id: Option<EpisodeId>,
    /// The active Runtime Session, when applicable.
    pub session_id: Option<SessionId>,
    /// The active Work Unit, when applicable.
    pub work_unit_id: Option<WorkUnitId>,
    /// The active Work Unit Attempt, when applicable.
    pub attempt_id: Option<AttemptId>,
    /// The W3C trace id active when the event occurred, when available.
    pub trace_id: Option<String>,
}

/// Integrity binding for a durable event's payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventIntegrity {
    /// A digest of the event payload.
    pub payload_digest: String,
}

/// One durable lifecycle event from the append-only Loop event journal, per
/// `docs/schemas/event-v0.1.yaml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopEvent {
    /// The event schema version, e.g. `loopward.event/v0.1`.
    pub schema_version: String,
    /// This event's durable identity.
    pub event_id: EventId,
    /// This event's position in the Loop's append-only journal.
    pub journal_position: u64,
    /// The Loop this event belongs to.
    pub loop_id: LoopId,
    /// The aggregate revision produced by this event.
    pub aggregate_revision: u64,
    /// When the underlying lifecycle transition occurred.
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: Timestamp,
    /// When the event was durably recorded, which may lag `occurred_at`.
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: Timestamp,
    /// The event type, e.g. `checkpoint.committed`. Open and
    /// extensible; Phase One does not close this set in the wire contract.
    #[serde(rename = "type")]
    pub kind: String,
    /// Whether this event is durable or ephemeral.
    pub durability: EventDurability,
    /// Entities active when this event occurred.
    pub correlation: EventCorrelation,
    /// The event's type-specific payload.
    pub payload: serde_json::Value,
    /// Integrity binding for the payload.
    pub integrity: EventIntegrity,
}

/// A query for durable or ephemeral Loop events, per
/// `docs/09-public-api.md` §7.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventQuery {
    /// The Loop to query events for.
    pub loop_id: LoopId,
    /// Return only events after this cursor, when supplied.
    pub after: Option<EventCursor>,
    /// The maximum number of events to return.
    pub limit: u32,
    /// Which durability class of events to return.
    pub durability: EventDurabilityFilter,
}

/// One page of Loop events, with a cursor for continued pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventPage {
    /// The events in this page, in journal order.
    pub events: Vec<LoopEvent>,
    /// A cursor for fetching the next page, absent when exhausted.
    pub next_cursor: Option<EventCursor>,
}
