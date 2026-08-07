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
//!
//! # From one flat event to spans
//!
//! Task 4 established [`TelemetryEvent`]: one flat, spanless fact, with its
//! own doc comment already naming the tradeoff — "deliberately generic
//! rather than modeling full `OpenTelemetry` spans". Task 11 needs real span
//! semantics: start/end pairs, parent/child nesting, and the causal links
//! `docs/08` §7 names (`continuation_of`, `retry_of`, `recovery_of`,
//! `remediation_of`, `external_execution`) — none of which a one-shot event
//! can express. Rather than replacing [`TelemetryEvent`] (still the right
//! shape for a bare diagnostic fact with no span of its own — see [`events`]
//! for the stable names `docs/08` §9 assigns them), this crate *extends*
//! [`TelemetrySink`] with span methods alongside it: [`spans::SpanStart`]/
//! [`spans::SpanEnd`] describe one span's start and end, [`spans::SpanLink`]
//! carries a causal link, and [`spans::Span`] is an RAII guard that lets
//! `loopward-runtime` wrap an *existing* call sequence in a span without
//! restructuring its control flow (`?` and early returns still end the span
//! exactly once, via `Drop`).
//!
//! Every new trait method carries a default implementation that does
//! nothing and returns the cheapest possible handle
//! ([`spans::SpanHandle::disabled`]) — so [`NoopTelemetrySink`] did not need
//! to change at all to pick them up, and stays what it always was: a
//! legitimate, zero-cost production configuration with tracing disabled.

pub mod attributes;
pub mod events;
pub mod recorder;
pub mod spans;

pub use attributes::attribute;
pub use events::event_name;

use spans::{SpanEnd, SpanHandle, SpanStart};

/// One structured fact the runtime wants to record as telemetry, with no
/// span of its own.
///
/// Deliberately generic rather than modeling full `OpenTelemetry` spans:
/// Phase One "supports `OpenTelemetry`-compatible tracing" (§3) through a
/// dedicated adapter crate, but the contract a correctness-critical runtime
/// depends on must stay small enough that [`NoopTelemetrySink`] can satisfy
/// it with no observable effect. See [`spans`] for span-shaped telemetry
/// (start/end, nesting, causal links).
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
///
/// The span methods all carry default implementations that do nothing and
/// return [`SpanHandle::disabled`]: an implementor that only ever cared
/// about flat events (like [`NoopTelemetrySink`]) needs no changes at all,
/// and picks up "tracing disabled" behavior for free.
pub trait TelemetrySink: Send + Sync {
    /// Records one telemetry event with no span of its own.
    fn record(&self, event: TelemetryEvent);

    /// Starts a new span, returning a handle for [`Self::end_span`] and for
    /// use as another span's parent or link target.
    ///
    /// Must never block or panic: a telemetry outage must not stop Loop
    /// progress (`docs/08-tracing-and-observability.md` §13).
    fn start_span(&self, start: SpanStart) -> SpanHandle {
        let _ = start;
        SpanHandle::disabled()
    }

    /// Ends a span previously returned by [`Self::start_span`].
    ///
    /// Must never block or panic, for the same reason as [`Self::start_span`].
    fn end_span(&self, handle: SpanHandle, end: SpanEnd) {
        let _ = (handle, end);
    }

    /// Attaches one diagnostic event to a currently open span
    /// (`docs/08` §9). The durable event journal remains authoritative;
    /// this is a diagnostic copy only.
    fn span_event(&self, handle: &SpanHandle, event: TelemetryEvent) {
        let _ = (handle, event);
    }

    /// Resolves the W3C trace context to propagate across a runner boundary
    /// for the span `handle` names (`docs/08` §8), for attaching to a
    /// `WorkUnitRequest`. Returns an empty context when tracing is disabled
    /// or the sink does not support propagation.
    fn trace_context(&self, handle: &SpanHandle) -> loopward_api::trace::TraceContext {
        let _ = handle;
        loopward_api::trace::TraceContext::default()
    }
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
