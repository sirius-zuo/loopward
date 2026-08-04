//! [`TraceRecorder`]: an in-memory [`TelemetrySink`] a conformance test can
//! plug into [`loopward_runtime::ports::RuntimePorts`] and later query for
//! the spans and causal links a scenario produced.
//!
//! Builds directly on [`RecordingSink`] (`loopward-observability`, Task 11)
//! rather than reimplementing an in-memory span recorder from scratch — this
//! module is a thin, scenario-shaped query layer over it: top-level traces
//! (one root span per Episode, `docs/08-tracing-and-observability.md` §4),
//! and the [`SpanLinkKind::ContinuationOf`] links between them.

use std::sync::Arc;

use loopward_observability::TelemetrySink;
use loopward_observability::recorder::{RecordedSpan, RecordingSink};
use loopward_observability::spans::{SpanLink, SpanLinkKind};

/// Records every span and event a scenario's telemetry produces, and
/// answers the queries a conformance test asserts against: which spans were
/// top-level traces, which carried a given causal link, and so on.
#[derive(Debug, Default)]
pub struct TraceRecorder {
    sink: Arc<RecordingSink>,
}

impl TraceRecorder {
    /// Builds an empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sink: Arc::new(RecordingSink::new()),
        }
    }

    /// The underlying [`TelemetrySink`], to plug into
    /// `RuntimePorts::telemetry`.
    #[must_use]
    pub fn sink(&self) -> Arc<dyn TelemetrySink> {
        Arc::clone(&self.sink) as Arc<dyn TelemetrySink>
    }

    /// Every span that has ended so far, in end order.
    #[must_use]
    pub fn spans(&self) -> Vec<RecordedSpan> {
        self.sink.finished_spans()
    }

    /// Every span with no parent — one top-level trace root per Episode
    /// (`docs/08` §4: "one top-level trace per Episode").
    #[must_use]
    pub fn top_level_spans(&self) -> Vec<RecordedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.parent.is_none())
            .collect()
    }

    /// Every recorded span named `name`, in end order.
    #[must_use]
    pub fn spans_named(&self, name: &str) -> Vec<RecordedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.name == name)
            .collect()
    }

    /// Every `(span, link)` pair among the recorded spans whose link carries
    /// `kind` — e.g. every [`SpanLinkKind::ContinuationOf`] a scenario's
    /// Episode traces recorded, so a test can assert each Episode after the
    /// first links back to its predecessor's trace.
    #[must_use]
    pub fn links_of_kind(&self, kind: SpanLinkKind) -> Vec<(RecordedSpan, SpanLink)> {
        self.spans()
            .into_iter()
            .flat_map(|span| {
                let matching: Vec<SpanLink> = span
                    .links
                    .iter()
                    .filter(|link| link.kind == kind)
                    .cloned()
                    .collect();
                matching
                    .into_iter()
                    .map(move |link| (span.clone(), link))
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}
