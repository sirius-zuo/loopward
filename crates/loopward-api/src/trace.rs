//! Trace propagation carriers.
//!
//! Loopward owns the trace of its own public execution boundary and
//! coordinates W3C Trace Context with runners that support it (see
//! `docs/08-tracing-and-observability.md`). This module contains only the
//! wire carrier types; span creation and export are the concern of
//! `loopward-observability` and `loopward-observability-otel`.

use serde::{Deserialize, Serialize};

/// A single W3C `tracestate`-adjacent baggage entry propagated to a runner.
///
/// Only allow-listed baggage keys may cross the runner boundary; sensitive
/// Loop state, prompt content, credentials, and unrestricted identifiers
/// must never be placed in baggage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaggageItem {
    /// The baggage key. Callers are responsible for allow-listing.
    pub key: String,
    /// The baggage value.
    pub value: String,
}

/// The W3C Trace Context propagated across the `WorkUnitRunner` boundary.
///
/// Carried on every `WorkUnitRequest` so a runner that supports context
/// propagation can emit compatible child or linked spans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TraceContext {
    /// The W3C `traceparent` header value, when tracing is active.
    pub traceparent: Option<String>,
    /// The W3C `tracestate` header value, when present.
    pub tracestate: Option<String>,
    /// Allow-listed baggage entries propagated alongside the trace context.
    #[serde(default)]
    pub baggage: Vec<BaggageItem>,
}

/// An opaque reference to a trace or span owned by a runner, recorded by
/// Loopward without claiming ownership of the external spans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRef {
    /// The runner-provided opaque trace or span reference.
    pub reference: String,
    /// An optional human-readable description of what the reference points
    /// at (e.g. a vendor console URL or trace id).
    pub description: Option<String>,
}

/// How much of Loopward's propagated trace context a runner actually
/// honors, per `docs/08-tracing-and-observability.md` §8.
///
/// This must reflect the runner's *actual* observed capability, never an
/// aspiration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceCoordinationLevel {
    /// Loopward traces the call, but the runner does not propagate context.
    BoundaryOnly,
    /// The runner returns an external trace reference.
    Correlated,
    /// The runner accepts propagated context and emits compatible child or
    /// linked spans.
    Continuous,
}
