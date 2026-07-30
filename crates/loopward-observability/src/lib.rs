#![forbid(unsafe_code)]

//! Loopward's observability boundary.
//!
//! This crate owns the [`TelemetrySink`] contract `loopward-runtime` emits
//! lifecycle telemetry through, and the [`NoopTelemetrySink`] that keeps the
//! runtime correct with tracing disabled and no telemetry backend
//! configured (`docs/08-tracing-and-observability.md` §2, §3: "does not
//! ship or configure a telemetry backend").
//!
//! Telemetry storage, querying, visualization, and alerting are explicitly
//! *not* Loopward's concern (§2) — this crate defines the emission contract
//! only. A real OpenTelemetry-backed sink belongs in the separate
//! `loopward-observability-otel` crate; `loopward-runtime` depends on this
//! crate's trait and must never redeclare it.

/// One structured fact the runtime wants to record as telemetry.
///
/// Deliberately generic rather than modeling full `OpenTelemetry` spans:
/// Phase One "supports `OpenTelemetry`-compatible tracing" (§3) through a
/// dedicated adapter crate, but the contract a correctness-critical runtime
/// depends on must stay small enough that [`NoopTelemetrySink`] can satisfy
/// it with no observable effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryEvent {
    /// A stable, dotted event name (e.g. `"episode.started"`), matching the
    /// journal event kinds in `docs/03-durable-state-and-transactions.md`
    /// §6 where one exists.
    pub name: &'static str,
    /// Ordered key/value attributes describing the event.
    pub attributes: Vec<(&'static str, String)>,
}

impl TelemetryEvent {
    /// Starts a new event with no attributes.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            attributes: Vec::new(),
        }
    }

    /// Adds one attribute and returns the event for chaining.
    #[must_use]
    pub fn with_attribute(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.attributes.push((key, value.into()));
        self
    }
}

/// The runtime's telemetry emission boundary.
///
/// Runtime correctness never depends on a [`TelemetrySink`] implementation
/// doing anything observable: every call site treats emission as
/// best-effort and side-channel only, and [`NoopTelemetrySink`] is a
/// legitimate production configuration.
pub trait TelemetrySink: Send + Sync {
    /// Records one telemetry event.
    fn record(&self, event: TelemetryEvent);
}

/// A [`TelemetrySink`] that discards every event.
///
/// The default sink when tracing is disabled and no telemetry backend is
/// configured; also useful in tests that want deterministic behavior with
/// no telemetry side effects at all.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoopTelemetrySink;

impl TelemetrySink for NoopTelemetrySink {
    fn record(&self, _event: TelemetryEvent) {}
}

#[cfg(test)]
mod tests {
    use super::{NoopTelemetrySink, TelemetryEvent, TelemetrySink};

    #[test]
    fn noop_sink_accepts_any_event_and_has_no_observable_effect() {
        let sink = NoopTelemetrySink;
        sink.record(TelemetryEvent::new("loop.created").with_attribute("loop_id", "abc"));
        // No assertion beyond "did not panic": the point of the noop sink is
        // that recording is unobservable.
    }

    #[test]
    fn telemetry_event_builder_accumulates_attributes_in_order() {
        let event = TelemetryEvent::new("episode.started")
            .with_attribute("loop_id", "loop-1")
            .with_attribute("episode_id", "episode-1");
        assert_eq!(event.name, "episode.started");
        assert_eq!(
            event.attributes,
            vec![
                ("loop_id", "loop-1".to_owned()),
                ("episode_id", "episode-1".to_owned()),
            ]
        );
    }
}
