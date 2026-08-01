//! An in-memory [`TelemetrySink`] recorder for tests
//! (`docs/08-tracing-and-observability.md` §14: "Phase One instrumentation
//! tests use ... an in-memory span exporter or recorder ... No external
//! telemetry service is required for conformance tests").
//!
//! This is a plain, dependency-free recorder scoped to this crate's own
//! semantic-convention and redaction tests. `loopward-observability-otel`
//! has its own, separate in-memory exporter backed by the real
//! `OpenTelemetry` SDK for testing the adapter itself end to end; this one
//! exists so `loopward-observability` can test its own contract without
//! depending on that adapter crate.

use std::sync::Mutex;

use crate::spans::{SpanContext, SpanEnd, SpanHandle, SpanLink, SpanStart, SpanStatus};
use crate::{TelemetryEvent, TelemetrySink};

/// One span this recorder observed, from start to end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedSpan {
    /// This span's own context.
    pub context: SpanContext,
    /// The parent span's context, if any.
    pub parent: Option<SpanContext>,
    /// The span name it started with.
    pub name: &'static str,
    /// Attributes recorded at start.
    pub start_attributes: Vec<(&'static str, String)>,
    /// Causal links recorded at start.
    pub links: Vec<SpanLink>,
    /// Attributes recorded at end.
    pub end_attributes: Vec<(&'static str, String)>,
    /// The status it ended with.
    pub status: SpanStatus,
}

impl RecordedSpan {
    /// All attributes, start and end, in recorded order.
    #[must_use]
    pub fn all_attributes(&self) -> Vec<(&'static str, String)> {
        self.start_attributes
            .iter()
            .chain(self.end_attributes.iter())
            .cloned()
            .collect()
    }
}

/// One span this recorder has seen started but not yet ended.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenSpan {
    context: SpanContext,
    name: &'static str,
    attributes: Vec<(&'static str, String)>,
    links: Vec<SpanLink>,
    parent: Option<SpanContext>,
}

#[derive(Debug, Default)]
struct State {
    next_id: u64,
    open: Vec<OpenSpan>,
    finished: Vec<RecordedSpan>,
    events: Vec<(Option<SpanContext>, TelemetryEvent)>,
}

/// A [`TelemetrySink`] that records every span and event in memory rather
/// than exporting anywhere.
#[derive(Debug, Default)]
pub struct RecordingSink {
    state: Mutex<State>,
}

impl RecordingSink {
    /// Builds an empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// All spans that have ended so far, in end order.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (only possible if a prior
    /// call panicked while holding it).
    #[must_use]
    pub fn finished_spans(&self) -> Vec<RecordedSpan> {
        self.state
            .lock()
            .expect("recorder lock poisoned")
            .finished
            .clone()
    }

    /// All span-scoped and spanless events recorded so far.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn events(&self) -> Vec<(Option<SpanContext>, TelemetryEvent)> {
        self.state
            .lock()
            .expect("recorder lock poisoned")
            .events
            .clone()
    }
}

impl TelemetrySink for RecordingSink {
    fn record(&self, event: TelemetryEvent) {
        self.state
            .lock()
            .expect("recorder lock poisoned")
            .events
            .push((None, event));
    }

    fn start_span(&self, start: SpanStart) -> SpanHandle {
        let mut state = self.state.lock().expect("recorder lock poisoned");
        state.next_id += 1;
        let id = state.next_id;
        let trace_id = start
            .parent
            .as_ref()
            .map_or_else(|| format!("trace-{id}"), |parent| parent.trace_id.clone());
        let context = SpanContext::new(trace_id, format!("span-{id}"));
        state.open.push(OpenSpan {
            context: context.clone(),
            name: start.name,
            attributes: start.attributes,
            links: start.links,
            parent: start.parent,
        });
        SpanHandle::new(context)
    }

    fn end_span(&self, handle: SpanHandle, end: SpanEnd) {
        let Some(context) = handle.context().cloned() else {
            return;
        };
        let mut state = self.state.lock().expect("recorder lock poisoned");
        let Some(index) = state.open.iter().position(|open| open.context == context) else {
            return;
        };
        let open = state.open.remove(index);
        state.finished.push(RecordedSpan {
            context: open.context,
            parent: open.parent,
            name: open.name,
            start_attributes: open.attributes,
            links: open.links,
            end_attributes: end.attributes,
            status: end.status,
        });
    }

    fn span_event(&self, handle: &SpanHandle, event: TelemetryEvent) {
        self.state
            .lock()
            .expect("recorder lock poisoned")
            .events
            .push((handle.context().cloned(), event));
    }

    fn trace_context(&self, handle: &SpanHandle) -> loopward_api::trace::TraceContext {
        handle
            .context()
            .map(|context| loopward_api::trace::TraceContext {
                traceparent: Some(format!("00-{}-{}-01", context.trace_id, context.span_id)),
                tracestate: None,
                baggage: Vec::new(),
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::RecordingSink;
    use crate::TelemetrySink;
    use crate::spans::{SpanEnd, SpanStart};

    #[test]
    fn records_a_span_from_start_to_end() {
        let sink = RecordingSink::new();
        let handle = sink.start_span(SpanStart::new("loopward.episode").with_attribute("k", "v"));
        sink.end_span(
            handle,
            SpanEnd::ok().with_attribute("loopward.outcome", "completed"),
        );

        let spans = sink.finished_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "loopward.episode");
        assert_eq!(
            spans[0].all_attributes(),
            vec![
                ("k", "v".to_owned()),
                ("loopward.outcome", "completed".to_owned())
            ]
        );
    }

    #[test]
    fn child_spans_share_their_parents_trace_id() {
        let sink = RecordingSink::new();
        let root = sink.start_span(SpanStart::new("root"));
        let root_context = root.context().unwrap().clone();
        let child = sink.start_span(SpanStart::new("child").with_parent(root_context.clone()));
        assert_eq!(child.context().unwrap().trace_id, root_context.trace_id);
        sink.end_span(child, SpanEnd::ok());
        sink.end_span(root, SpanEnd::ok());
    }
}
