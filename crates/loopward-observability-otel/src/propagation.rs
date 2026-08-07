//! W3C Trace Context propagation between a real `OpenTelemetry`
//! [`opentelemetry::trace::SpanContext`] and Loopward's wire-level
//! [`loopward_api::trace::TraceContext`] (`docs/08-tracing-and-observability.md`
//! §8).
//!
//! This module owns only the translation. Span creation and export are
//! [`crate::exporter`]'s concern.

use std::collections::HashMap;

use loopward_api::trace::TraceContext;
use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;

/// A minimal, allocation-only [`Injector`]/[`Extractor`] backed by a
/// `HashMap`, so this module never has to touch a real HTTP header map to
/// exercise W3C propagation.
#[derive(Debug, Default)]
struct MapCarrier(HashMap<String, String>);

impl Injector for MapCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

impl Extractor for MapCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Formats `span_context` as a Loopward [`TraceContext`] (a `traceparent`,
/// and a `tracestate` when the context carries one) suitable for attaching
/// to a `WorkUnitRequest` crossing the runner boundary.
///
/// Baggage is never populated here: `docs/08` §8 requires baggage keys to be
/// allow-listed by the caller, which this translation-only function has no
/// visibility into.
#[must_use]
pub fn to_wire_trace_context(span_context: &SpanContext) -> TraceContext {
    if !span_context.is_valid() {
        return TraceContext::default();
    }

    let propagator = TraceContextPropagator::new();
    let cx = Context::new().with_remote_span_context(span_context.clone());
    let mut carrier = MapCarrier::default();
    propagator.inject_context(&cx, &mut carrier);

    TraceContext {
        traceparent: carrier.0.remove("traceparent"),
        tracestate: carrier.0.remove("tracestate"),
        baggage: Vec::new(),
    }
}

/// Parses a Loopward [`TraceContext`] back into a real `OpenTelemetry`
/// [`SpanContext`], for a runner (or test) that received a `traceparent` and
/// wants to continue the trace it names.
///
/// Returns [`SpanContext::empty_context`] (invalid) when `wire` carries no
/// `traceparent`, or one that does not parse as valid W3C Trace Context.
#[must_use]
pub fn from_wire_trace_context(wire: &TraceContext) -> SpanContext {
    let propagator = TraceContextPropagator::new();
    let mut carrier = MapCarrier::default();
    if let Some(traceparent) = &wire.traceparent {
        carrier
            .0
            .insert("traceparent".to_owned(), traceparent.clone());
    }
    if let Some(tracestate) = &wire.tracestate {
        carrier
            .0
            .insert("tracestate".to_owned(), tracestate.clone());
    }
    let cx = propagator.extract(&carrier);
    cx.span().span_context().clone()
}

#[cfg(test)]
mod tests {
    use super::{from_wire_trace_context, to_wire_trace_context};
    use loopward_api::trace::TraceContext;
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};

    fn sample_span_context() -> SpanContext {
        SpanContext::new(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        )
    }

    #[test]
    fn round_trips_a_valid_span_context_through_the_wire_shape() {
        let original = sample_span_context();
        let wire = to_wire_trace_context(&original);
        let traceparent = wire
            .traceparent
            .clone()
            .expect("a valid context yields a traceparent");
        assert!(traceparent.contains("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert!(traceparent.contains("00f067aa0ba902b7"));

        let recovered = from_wire_trace_context(&wire);
        assert_eq!(recovered.trace_id(), original.trace_id());
        assert_eq!(recovered.span_id(), original.span_id());
    }

    #[test]
    fn an_invalid_span_context_yields_no_traceparent() {
        let wire = to_wire_trace_context(&SpanContext::empty_context());
        assert!(wire.traceparent.is_none());
        assert!(wire.tracestate.is_none());
    }

    #[test]
    fn a_trace_context_with_no_traceparent_extracts_as_invalid() {
        let recovered = from_wire_trace_context(&TraceContext::default());
        assert!(!recovered.is_valid());
    }
}
