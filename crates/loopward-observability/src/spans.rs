//! Backend-neutral span semantics: names, links, and the [`Span`] guard
//! `loopward-runtime` instruments its lifecycle with
//! (`docs/08-tracing-and-observability.md` §5, §7).
//!
//! Nothing here models a full `OpenTelemetry` SDK: [`SpanContext`] is an
//! opaque pair of caller-meaningful identifier strings, not a real trace/span
//! ID pair with sampling flags, and [`TelemetrySink::start_span`]/
//! [`TelemetrySink::end_span`] are synchronous, infallible, and best-effort
//! (`docs/08` §13 — "Tracing is best-effort. A telemetry outage must not
//! stop Loop progress"). A real `OpenTelemetry`-backed implementation lives
//! in `loopward-observability-otel` and is responsible for turning this
//! shape into actual trace/span IDs, sampling, and export.

use crate::TelemetrySink;
use std::sync::Arc;

/// Stable, low-cardinality span names (`docs/08` §5). Identifiers belong in
/// attributes, never in the name itself.
pub mod span_name {
    /// One Episode's top-level trace.
    pub const EPISODE: &str = "loopward.episode";
    /// Boot Package hydration.
    pub const CONTEXT_HYDRATE: &str = "loopward.context.hydrate";
    /// A Runtime Session starting.
    pub const SESSION_START: &str = "loopward.session.start";
    /// A Runtime Session actively running a Work Unit Attempt.
    pub const SESSION_RUN: &str = "loopward.session.run";
    /// A Runtime Session draining before rotation.
    pub const SESSION_DRAIN: &str = "loopward.session.drain";
    /// A Runtime Session closing.
    pub const SESSION_CLOSE: &str = "loopward.session.close";
    /// The runner boundary call that starts a Work Unit Attempt.
    pub const WORK_UNIT_EXECUTE: &str = "loopward.work_unit.execute";
    /// The runner boundary call that collects a Work Unit Attempt's result.
    pub const WORK_UNIT_COLLECT: &str = "loopward.work_unit.collect";
    /// Verification judging a collected result.
    pub const VERIFICATION_EVALUATE: &str = "loopward.verification.evaluate";
    /// A Checkpoint commit.
    pub const CHECKPOINT_COMMIT: &str = "loopward.checkpoint.commit";
    /// A recovery/reconciliation operation.
    pub const RECOVERY_RECONCILE: &str = "loopward.recovery.reconcile";
    /// Resolving one resource through a Loopward boundary.
    pub const RESOURCE_RESOLVE: &str = "loopward.resource.resolve";
}

/// An opaque, caller-meaningful identity for one span.
///
/// Deliberately just two strings, not a real `OpenTelemetry` trace/span ID:
/// this crate's contract stays backend-neutral, and a real adapter (like
/// `loopward-observability-otel`) is free to put actual W3C-shaped
/// identifiers here, while [`crate::NoopTelemetrySink`] and test recorders
/// are free to put anything stable enough to correlate start/end and
/// parent/child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanContext {
    /// The identifier of the trace this span belongs to.
    pub trace_id: String,
    /// This span's own identifier, unique within its trace.
    pub span_id: String,
}

impl SpanContext {
    /// Builds a context from its two identifiers.
    #[must_use]
    pub fn new(trace_id: impl Into<String>, span_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
        }
    }
}

/// A handle to one started span, returned by
/// [`TelemetrySink::start_span`] and required by
/// [`TelemetrySink::end_span`].
///
/// Carries no context when tracing is disabled or the sink chose not to
/// start a real span — [`SpanHandle::disabled`] is the zero-cost value
/// [`crate::NoopTelemetrySink`] and the trait's default methods hand back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanHandle(Option<SpanContext>);

impl SpanHandle {
    /// The handle a disabled or no-op sink returns: carries no context and
    /// costs nothing to construct or drop.
    #[must_use]
    pub fn disabled() -> Self {
        Self(None)
    }

    /// Wraps a real span context.
    #[must_use]
    pub fn new(context: SpanContext) -> Self {
        Self(Some(context))
    }

    /// This handle's context, when the span it names is real.
    #[must_use]
    pub fn context(&self) -> Option<&SpanContext> {
        self.0.as_ref()
    }
}

/// Which causal relation a [`SpanLink`] expresses
/// (`docs/08` §7) — never parent/child containment, which
/// [`SpanStart::parent`] already carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanLinkKind {
    /// The root span of a new Episode, linking to the prior Episode's
    /// trace.
    ContinuationOf,
    /// A retried operation, linking to the attempt it retries.
    RetryOf,
    /// A recovery operation, linking to the interrupted trace it resumes.
    RecoveryOf,
    /// A remediation run, linking to the finding or plan that triggered it.
    RemediationOf,
    /// An opaque external execution trace a runner returned, recorded
    /// without Loopward claiming ownership of it.
    ExternalExecution,
}

impl SpanLinkKind {
    /// This link kind's stable, dotted label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContinuationOf => "continuation_of",
            Self::RetryOf => "retry_of",
            Self::RecoveryOf => "recovery_of",
            Self::RemediationOf => "remediation_of",
            Self::ExternalExecution => "external_execution",
        }
    }
}

/// One causal link from a span being started to another trace or span
/// (`docs/08` §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanLink {
    /// The causal relation this link expresses.
    pub kind: SpanLinkKind,
    /// An opaque reference to the linked trace or span: another
    /// [`SpanContext`]'s `trace_id` for an internal link, or a runner's own
    /// opaque reference for [`SpanLinkKind::ExternalExecution`].
    pub trace_ref: String,
    /// Additional attributes describing the link.
    pub attributes: Vec<(&'static str, String)>,
}

impl SpanLink {
    /// Builds a link with no additional attributes.
    #[must_use]
    pub fn new(kind: SpanLinkKind, trace_ref: impl Into<String>) -> Self {
        Self {
            kind,
            trace_ref: trace_ref.into(),
            attributes: Vec::new(),
        }
    }

    /// Adds one attribute and returns the link for chaining.
    #[must_use]
    pub fn with_attribute(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.attributes.push((key, value.into()));
        self
    }
}

/// A span's outcome status, distinct from the business outcome recorded as
/// an attribute (`docs/08` §10). Only a genuine transport/protocol failure
/// (a runner protocol error, a store failure) should ever produce
/// [`SpanStatus::Error`]; a business rejection or suspension is a
/// successful span carrying `loopward.outcome=...`.
///
/// [`SpanStatus::Unset`] — not [`SpanStatus::Ok`] — is the default: a
/// [`Span`] that ends via [`Drop`] without an explicit outcome (any `?` or
/// `let ... else { return ... }` inside a wrapped call that this crate's
/// "wrap, don't restructure" instrumentation style deliberately never
/// intercepts) must never be recorded as an affirmative success.
/// `OpenTelemetry`'s own status semantics agree: `Status::Ok` is an
/// application's explicit assertion that an operation succeeded, not a
/// neutral "nothing went wrong" — `Status::Unset` is that neutral state, and
/// is what most spans that never fail are expected to carry.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SpanStatus {
    /// No outcome was explicitly recorded for this span.
    #[default]
    Unset,
    /// The operation this span represents completed, whatever its business
    /// outcome.
    Ok,
    /// A transport- or protocol-level failure occurred.
    Error {
        /// A stable, sanitized failure code (never a raw error message).
        code: &'static str,
        /// An optional sanitized description.
        message: Option<String>,
    },
}

/// Describes one span about to start.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SpanStart {
    /// The stable span name (see [`span_name`]).
    pub name: &'static str,
    /// Attributes known at start time.
    pub attributes: Vec<(&'static str, String)>,
    /// Causal links to other traces or spans.
    pub links: Vec<SpanLink>,
    /// The parent span's context, when this span is not a trace root.
    pub parent: Option<SpanContext>,
}

impl SpanStart {
    /// Starts building a span named `name` with no attributes, links, or
    /// parent (a trace root).
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            attributes: Vec::new(),
            links: Vec::new(),
            parent: None,
        }
    }

    /// Adds one attribute and returns the builder for chaining.
    #[must_use]
    pub fn with_attribute(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.attributes.push((key, value.into()));
        self
    }

    /// Adds one causal link and returns the builder for chaining.
    #[must_use]
    pub fn with_link(mut self, link: SpanLink) -> Self {
        self.links.push(link);
        self
    }

    /// Sets this span's parent, making it a child span rather than a trace
    /// root.
    #[must_use]
    pub fn with_parent(mut self, parent: SpanContext) -> Self {
        self.parent = Some(parent);
        self
    }
}

/// Describes how a span concluded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SpanEnd {
    /// Attributes known only once the span concludes (e.g.
    /// `loopward.outcome`, `loopward.checkpoint.version`).
    pub attributes: Vec<(&'static str, String)>,
    /// The span's transport-level status.
    pub status: SpanStatus,
}

impl SpanEnd {
    /// An explicit successful conclusion with no additional attributes.
    ///
    /// Distinct from [`Self::default`] (used by [`Span`]'s silent `Drop`
    /// path), which is [`SpanStatus::Unset`], not `Ok` — see
    /// [`SpanStatus`]'s doc comment for why an unexamined span must never
    /// default to an affirmative success.
    #[must_use]
    pub fn ok() -> Self {
        Self {
            attributes: Vec::new(),
            status: SpanStatus::Ok,
        }
    }

    /// A transport/protocol-error conclusion.
    #[must_use]
    pub fn error(code: &'static str) -> Self {
        Self {
            attributes: Vec::new(),
            status: SpanStatus::Error {
                code,
                message: None,
            },
        }
    }

    /// Attaches a sanitized error message (`docs/08` §10: raw error
    /// messages may be sensitive; prefer public codes and sanitized
    /// descriptions).
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        if let SpanStatus::Error { message: slot, .. } = &mut self.status {
            *slot = Some(message.into());
        }
        self
    }

    /// Adds one attribute and returns the builder for chaining.
    #[must_use]
    pub fn with_attribute(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.attributes.push((key, value.into()));
        self
    }
}

/// An RAII guard around one open span.
///
/// Wraps an existing call sequence without requiring it to be restructured:
/// start a [`Span`] before a sequence of calls that may return early through
/// `?` or a `let ... else`, and the span still ends exactly once, with
/// whatever outcome was last recorded on it (defaulting to
/// [`SpanStatus::Unset`] with no extra attributes — never an unearned
/// `Ok`) — the same "wrap, don't rewrite" shape `docs/08`'s testability
/// section expects instrumentation to have.
///
/// Ending is always best-effort and synchronous
/// (`docs/08` §13): [`Drop::drop`] cannot run async code, so
/// [`TelemetrySink::end_span`] must never block or fail observably.
pub struct Span {
    sink: Arc<dyn TelemetrySink>,
    handle: SpanHandle,
    end: SpanEnd,
    finished: bool,
}

impl Span {
    /// Starts a new span through `sink`.
    #[must_use]
    pub fn start(sink: &Arc<dyn TelemetrySink>, start: SpanStart) -> Self {
        let handle = sink.start_span(start);
        Self {
            sink: Arc::clone(sink),
            handle,
            end: SpanEnd::default(),
            finished: false,
        }
    }

    /// This span's context, when tracing is active.
    #[must_use]
    pub fn context(&self) -> Option<&SpanContext> {
        self.handle.context()
    }

    /// This span's handle, for use as another span's parent or link target.
    #[must_use]
    pub fn handle(&self) -> &SpanHandle {
        &self.handle
    }

    /// Adds one attribute to be recorded when this span ends.
    pub fn set_end_attribute(&mut self, key: &'static str, value: impl Into<String>) {
        self.end.attributes.push((key, value.into()));
    }

    /// Marks this span as a transport/protocol-level error
    /// (`docs/08` §10) — never for a business rejection or suspension,
    /// which stays [`SpanStatus::Unset`] (or an explicitly recorded
    /// [`SpanStatus::Ok`]) with a `loopward.outcome` attribute instead.
    pub fn set_error(&mut self, code: &'static str, message: impl Into<String>) {
        self.end.status = SpanStatus::Error {
            code,
            message: Some(message.into()),
        };
    }

    /// Attaches one diagnostic event to this span (`docs/08` §9).
    pub fn event(&self, event: crate::TelemetryEvent) {
        self.sink.span_event(&self.handle, event);
    }

    /// Ends the span immediately with `end`, superseding any attributes or
    /// error already recorded on it.
    pub fn finish(mut self, end: SpanEnd) {
        self.finished = true;
        self.sink.end_span(
            std::mem::replace(&mut self.handle, SpanHandle::disabled()),
            end,
        );
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if !self.finished {
            self.sink.end_span(
                std::mem::replace(&mut self.handle, SpanHandle::disabled()),
                std::mem::take(&mut self.end),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SpanEnd, SpanLinkKind, SpanStart, span_name};

    #[test]
    fn span_link_kind_labels_match_the_stable_vocabulary() {
        assert_eq!(SpanLinkKind::ContinuationOf.as_str(), "continuation_of");
        assert_eq!(SpanLinkKind::RetryOf.as_str(), "retry_of");
        assert_eq!(SpanLinkKind::RecoveryOf.as_str(), "recovery_of");
        assert_eq!(SpanLinkKind::RemediationOf.as_str(), "remediation_of");
        assert_eq!(
            SpanLinkKind::ExternalExecution.as_str(),
            "external_execution"
        );
    }

    #[test]
    fn span_start_builder_accumulates_attributes_and_links() {
        let start = SpanStart::new(span_name::EPISODE).with_attribute("loopward.loop.id", "l-1");
        assert_eq!(start.name, span_name::EPISODE);
        assert_eq!(
            start.attributes,
            vec![("loopward.loop.id", "l-1".to_owned())]
        );
    }

    #[test]
    fn span_end_error_carries_a_sanitized_message() {
        let end = SpanEnd::error("runner_failure").with_message("sanitized detail");
        match end.status {
            super::SpanStatus::Error { code, message } => {
                assert_eq!(code, "runner_failure");
                assert_eq!(message.as_deref(), Some("sanitized detail"));
            }
            super::SpanStatus::Ok | super::SpanStatus::Unset => {
                panic!("expected an error status")
            }
        }
    }

    #[test]
    fn span_end_default_is_unset_not_ok() {
        assert_eq!(super::SpanEnd::default().status, super::SpanStatus::Unset);
        assert_eq!(super::SpanEnd::ok().status, super::SpanStatus::Ok);
    }
}
