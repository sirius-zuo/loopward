#![forbid(unsafe_code)]

//! An optional, real `OpenTelemetry`-backed [`loopward_observability::TelemetrySink`]
//! (`docs/08-tracing-and-observability.md` §3).
//!
//! This crate is never a dependency of `loopward-runtime` — the runtime
//! depends only on `loopward-observability`'s trait, and operates correctly
//! (Global Constraints: "MUST operate with tracing disabled and without a
//! telemetry backend") with no implementation from this crate wired in at
//! all. A deployment that wants real `OpenTelemetry` export constructs an
//! [`exporter::OtelTelemetrySink`] and passes it wherever the runtime
//! expects an `Arc<dyn loopward_observability::TelemetrySink>`.
//!
//! This crate never ships or configures a telemetry backend or UI itself:
//! [`exporter::OtelTelemetrySink::from_env`] only ever reads standard
//! `OpenTelemetry` environment variables
//! (`OTEL_EXPORTER_OTLP_ENDPOINT`) and never supplies a default local
//! address.

pub mod exporter;
pub mod propagation;

pub use exporter::OtelTelemetrySink;
