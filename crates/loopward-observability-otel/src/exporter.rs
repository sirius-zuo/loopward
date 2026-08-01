//! [`OtelTelemetrySink`]: a real `OpenTelemetry`-backed
//! [`loopward_observability::TelemetrySink`] (`docs/08-tracing-and-observability.md`
//! §3).
//!
//! Translates Loopward's backend-neutral [`SpanStart`]/[`SpanEnd`]/
//! [`SpanLink`] into real `OpenTelemetry` spans through an
//! [`opentelemetry_sdk::trace::SdkTracer`], and back into a W3C
//! [`TraceContext`] for propagation across the runner boundary (§8). Never
//! configures a telemetry backend or UI itself (§3): callers supply an
//! already-built [`SdkTracerProvider`] (an in-memory-exporter-backed one for
//! tests, or [`OtelTelemetrySink::from_env`]'s OTLP-configured one for a real
//! deployment).

use std::sync::Mutex;

use loopward_api::trace::TraceContext;
use loopward_observability::TelemetrySink;
use loopward_observability::spans::{
    SpanContext as LoopwardSpanContext, SpanEnd, SpanHandle, SpanStart, SpanStatus,
};
use loopward_observability::{TelemetryEvent, attribute};
use opentelemetry::trace::{
    Link, Span as _, SpanBuilder, SpanContext as OtelSpanContext, SpanId, SpanKind, Status,
    TraceContextExt, TraceFlags, TraceId, TraceState, Tracer, TracerProvider,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider, Span as OtelSpan};

use crate::propagation::to_wire_trace_context;

/// The environment variable Phase One reads an OTLP endpoint from
/// (`docs/08` §3). Its absence means "do not export": this adapter never
/// falls back to a default local address.
pub const OTEL_EXPORTER_OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// A real `OpenTelemetry`-backed [`TelemetrySink`].
///
/// Every span this sink starts holds its live `OpenTelemetry` span behind a
/// mutex until it ends — [`TelemetrySink::start_span`]/`end_span` are
/// synchronous by contract, so this sink never awaits anything on the
/// runtime's call path; whatever the underlying [`SdkTracerProvider`]'s span
/// processor does with a finished span (batch it, export it, drop it on a
/// telemetry outage) happens on its own schedule and can never block or
/// corrupt Loop progress (§13).
pub struct OtelTelemetrySink {
    tracer: SdkTracer,
    open: Mutex<Vec<(LoopwardSpanContext, OtelSpan)>>,
}

impl std::fmt::Debug for OtelTelemetrySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelTelemetrySink").finish_non_exhaustive()
    }
}

impl OtelTelemetrySink {
    /// Builds a sink that starts spans through `provider`'s
    /// `"loopward"`-scoped tracer.
    ///
    /// This is the seam tests use with an in-memory-exporter-backed
    /// provider (`docs/08` §14), and that a real deployment uses with an
    /// OTLP-exporter-backed one (see [`Self::from_env`]).
    #[must_use]
    pub fn new(provider: &SdkTracerProvider) -> Self {
        Self {
            tracer: provider.tracer("loopward"),
            open: Mutex::new(Vec::new()),
        }
    }

    /// Builds a sink whose spans export over OTLP/gRPC to the endpoint
    /// named by [`OTEL_EXPORTER_OTLP_ENDPOINT_ENV`].
    ///
    /// Returns `None` when the variable is unset or empty: this adapter
    /// never supplies a default local endpoint and never configures a
    /// telemetry backend on its own (`docs/08` §3) — an absent variable
    /// means the caller should fall back to
    /// [`loopward_observability::NoopTelemetrySink`] instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the variable is set but the OTLP exporter could
    /// not be built (e.g. an unparseable endpoint).
    pub fn from_env() -> Result<Option<Self>, opentelemetry_otlp::ExporterBuildError> {
        let endpoint = std::env::var(OTEL_EXPORTER_OTLP_ENDPOINT_ENV).ok();
        let Some(endpoint) = endpoint.filter(|value| !value.is_empty()) else {
            return Ok(None);
        };

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()?;
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build();
        Ok(Some(Self::new(&provider)))
    }
}

/// Reconstructs a real `OpenTelemetry` [`OtelSpanContext`] from Loopward's
/// opaque [`LoopwardSpanContext`], for use as a parent when starting a
/// child span.
fn otel_span_context(context: &LoopwardSpanContext) -> OtelSpanContext {
    let trace_id = TraceId::from_hex(&context.trace_id).unwrap_or(TraceId::INVALID);
    let span_id = SpanId::from_hex(&context.span_id).unwrap_or(SpanId::INVALID);
    OtelSpanContext::new(
        trace_id,
        span_id,
        TraceFlags::SAMPLED,
        false,
        TraceState::default(),
    )
}

/// Wraps a real `OpenTelemetry` [`OtelSpanContext`] as Loopward's opaque
/// [`LoopwardSpanContext`], so `loopward-runtime` can hold and later link to
/// it without depending on this crate.
fn loopward_span_context(context: &OtelSpanContext) -> LoopwardSpanContext {
    LoopwardSpanContext::new(
        format!("{:032x}", context.trace_id()),
        format!("{:016x}", context.span_id()),
    )
}

/// Builds a real `OpenTelemetry` [`Link`] out of one Loopward
/// [`loopward_observability::spans::SpanLink`].
///
/// `docs/08` §7's link kinds (`continuation_of`, `retry_of`, ...) have no
/// native `OpenTelemetry` counterpart, so the kind travels as an attribute
/// (`loopward.link.kind`) on the link itself — the same approach
/// `OpenTelemetry`'s own ecosystem uses for anything beyond parent/child.
/// When `trace_ref` is not a real 32-hex-digit trace ID (an opaque runner
/// reference, or a test recorder's own synthetic ID), the link's span
/// context is a stable placeholder and the raw reference is preserved
/// verbatim as `loopward.link.trace_ref` instead of being silently dropped.
fn otel_link(link: &loopward_observability::spans::SpanLink) -> Link {
    let mut attributes = vec![KeyValue::new("loopward.link.kind", link.kind.as_str())];
    let trace_id = match TraceId::from_hex(&link.trace_ref) {
        Ok(id) if id != TraceId::INVALID => id,
        _ => {
            attributes.push(KeyValue::new(
                "loopward.link.trace_ref",
                link.trace_ref.clone(),
            ));
            TraceId::from_hex("00000000000000000000000000000001").unwrap_or(TraceId::INVALID)
        }
    };
    attributes.extend(
        link.attributes
            .iter()
            .map(|(key, value)| KeyValue::new(*key, value.clone())),
    );
    let span_context = OtelSpanContext::new(
        trace_id,
        SpanId::from_hex("0000000000000001").unwrap_or(SpanId::INVALID),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    Link::new(span_context, attributes, 0)
}

impl TelemetrySink for OtelTelemetrySink {
    fn record(&self, event: TelemetryEvent) {
        // A bare, spanless event has no natural home on a real OpenTelemetry
        // trace (it names no span); Loopward's durable journal remains
        // authoritative for it (`docs/08` §9), so this adapter intentionally
        // drops it rather than inventing an artificial span to carry it on.
        let _ = event;
    }

    fn start_span(&self, start: SpanStart) -> SpanHandle {
        let parent_cx = start.parent.as_ref().map_or_else(Context::new, |parent| {
            Context::new().with_remote_span_context(otel_span_context(parent))
        });

        let attributes: Vec<KeyValue> = start
            .attributes
            .iter()
            .map(|(key, value)| KeyValue::new(*key, value.clone()))
            .collect();
        let links: Vec<Link> = start.links.iter().map(otel_link).collect();

        let builder = SpanBuilder::from_name(start.name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes)
            .with_links(links);
        let span = self.tracer.build_with_context(builder, &parent_cx);
        let context = loopward_span_context(span.span_context());

        self.open
            .lock()
            .expect("otel sink lock poisoned")
            .push((context.clone(), span));
        SpanHandle::new(context)
    }

    fn end_span(&self, handle: SpanHandle, end: SpanEnd) {
        let Some(context) = handle.context() else {
            return;
        };
        let mut open = self.open.lock().expect("otel sink lock poisoned");
        let Some(index) = open.iter().position(|(existing, _)| existing == context) else {
            return;
        };
        let (_, mut span) = open.remove(index);
        drop(open);

        for (key, value) in end.attributes {
            span.set_attribute(KeyValue::new(key, value));
        }
        match end.status {
            SpanStatus::Ok => span.set_status(Status::Ok),
            SpanStatus::Error { code, message } => {
                span.set_attribute(KeyValue::new(attribute::FAILURE_CODE, code));
                span.set_status(Status::error(message.unwrap_or_else(|| code.to_owned())));
            }
        }
        span.end();
    }

    fn span_event(&self, handle: &SpanHandle, event: TelemetryEvent) {
        let Some(context) = handle.context() else {
            return;
        };
        let mut open = self.open.lock().expect("otel sink lock poisoned");
        let Some((_, span)) = open.iter_mut().find(|(existing, _)| existing == context) else {
            return;
        };
        let attributes: Vec<KeyValue> = event
            .attributes
            .iter()
            .map(|(key, value)| KeyValue::new(*key, value.clone()))
            .collect();
        span.add_event(event.name, attributes);
    }

    fn trace_context(&self, handle: &SpanHandle) -> TraceContext {
        let Some(context) = handle.context() else {
            return TraceContext::default();
        };
        to_wire_trace_context(&otel_span_context(context))
    }
}
