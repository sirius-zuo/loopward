//! Semantic-convention and redaction tests for `loopward-observability`
//! (Task 11, Step 1; `docs/08-tracing-and-observability.md` §5, §6, §12).

use loopward_observability::attribute;
use loopward_observability::attributes::FORBIDDEN_ATTRIBUTE_SUBSTRINGS;
use loopward_observability::recorder::RecordingSink;
use loopward_observability::spans::{SpanEnd, SpanLink, SpanLinkKind, SpanStart, span_name};
use loopward_observability::{TelemetryEvent, TelemetrySink};

/// The 12 stable span names `docs/08` §5 lists, verbatim.
const EXPECTED_SPAN_NAMES: &[&str] = &[
    "loopward.episode",
    "loopward.context.hydrate",
    "loopward.session.start",
    "loopward.session.run",
    "loopward.session.drain",
    "loopward.session.close",
    "loopward.work_unit.execute",
    "loopward.work_unit.collect",
    "loopward.verification.evaluate",
    "loopward.checkpoint.commit",
    "loopward.recovery.reconcile",
    "loopward.resource.resolve",
];

#[test]
fn span_name_constants_match_the_stable_vocabulary_exactly() {
    let actual = [
        span_name::EPISODE,
        span_name::CONTEXT_HYDRATE,
        span_name::SESSION_START,
        span_name::SESSION_RUN,
        span_name::SESSION_DRAIN,
        span_name::SESSION_CLOSE,
        span_name::WORK_UNIT_EXECUTE,
        span_name::WORK_UNIT_COLLECT,
        span_name::VERIFICATION_EVALUATE,
        span_name::CHECKPOINT_COMMIT,
        span_name::RECOVERY_RECONCILE,
        span_name::RESOURCE_RESOLVE,
    ];
    assert_eq!(actual, EXPECTED_SPAN_NAMES);
}

#[test]
fn required_attribute_keys_are_all_dotted_under_the_loopward_namespace() {
    let required = [
        attribute::LOOP_ID,
        attribute::EPISODE_ID,
        attribute::EPISODE_ORDINAL,
        attribute::SESSION_ID,
        attribute::SESSION_GENERATION,
        attribute::WORK_UNIT_ID,
        attribute::ATTEMPT_ID,
        attribute::RUNNER_NAME,
        attribute::RUNNER_PROTOCOL_VERSION,
        attribute::CHECKPOINT_VERSION,
        attribute::PHASE,
        attribute::OPERATION,
        attribute::OUTCOME,
    ];
    for key in required {
        assert!(
            key.starts_with("loopward."),
            "attribute key `{key}` is not namespaced under `loopward.`"
        );
    }
    assert_eq!(required.len(), 13);
}

#[test]
fn conditional_attribute_keys_match_the_documented_set() {
    let conditional = [
        attribute::ROTATION_REASON,
        attribute::ROTATION_SIGNALS,
        attribute::SUSPENSION_CODE,
        attribute::FAILURE_CODE,
        attribute::RECOVERY_OUTCOME,
        attribute::CONTEXT_RULE_VERSION,
        attribute::CONTEXT_ESTIMATED_TOKENS,
        attribute::CONTEXT_ACTUAL_TOKENS,
        attribute::CONTEXT_ESTIMATE_METHOD,
        attribute::VERIFICATION_GATE_KIND,
        attribute::VERIFICATION_OUTCOME,
    ];
    assert_eq!(
        conditional,
        [
            "loopward.rotation.reason",
            "loopward.rotation.signals",
            "loopward.suspension.code",
            "loopward.failure.code",
            "loopward.recovery.outcome",
            "loopward.context.rule_version",
            "loopward.context.estimated_tokens",
            "loopward.context.actual_tokens",
            "loopward.context.estimate_method",
            "loopward.verification.gate_kind",
            "loopward.verification.outcome",
        ]
    );
}

/// §12: "traces contain no full prompt or resource payload by default" —
/// none of this crate's own attribute *keys* name a forbidden concept. This
/// cannot prove a caller never stuffs a whole prompt into a value, only that
/// the crate's own constants never invite it.
#[test]
fn no_built_in_attribute_key_names_a_forbidden_payload_concept() {
    let all_keys = [
        attribute::LOOP_ID,
        attribute::EPISODE_ID,
        attribute::EPISODE_ORDINAL,
        attribute::SESSION_ID,
        attribute::SESSION_GENERATION,
        attribute::WORK_UNIT_ID,
        attribute::ATTEMPT_ID,
        attribute::RUNNER_NAME,
        attribute::RUNNER_PROTOCOL_VERSION,
        attribute::CHECKPOINT_VERSION,
        attribute::PHASE,
        attribute::OPERATION,
        attribute::OUTCOME,
        attribute::ROTATION_REASON,
        attribute::SUSPENSION_CODE,
        attribute::FAILURE_CODE,
        attribute::RECOVERY_OUTCOME,
        attribute::CONTEXT_RULE_VERSION,
        attribute::CONTEXT_ESTIMATED_TOKENS,
        attribute::CONTEXT_ACTUAL_TOKENS,
        attribute::CONTEXT_ESTIMATE_METHOD,
        attribute::VERIFICATION_GATE_KIND,
        attribute::VERIFICATION_OUTCOME,
    ];
    for key in all_keys {
        for forbidden in FORBIDDEN_ATTRIBUTE_SUBSTRINGS {
            assert!(
                !key.contains(forbidden),
                "attribute key `{key}` unexpectedly names forbidden concept `{forbidden}`"
            );
        }
    }
}

/// Exercises this crate's own [`RecordingSink`] plumbing (start/end,
/// attribute accumulation) with attribute *values* this test chose itself.
/// This proves the recorder correctly reports back whatever it was given —
/// it does **not** prove real runtime instrumentation never puts sensitive
/// content into a value. That end-to-end guarantee (`docs/08` §12/§15: "no
/// full prompt or resource payload by default") is proven against a real,
/// running `LoopController` and a real `OpenTelemetry` exporter in
/// `loopward-observability-otel`'s
/// `no_span_or_event_attribute_leaks_marked_sensitive_content` test, which
/// this crate cannot depend on (that crate depends on this one, not the
/// reverse).
#[test]
fn a_recorded_episode_span_carries_no_forbidden_payload_content() {
    let sink = RecordingSink::new();
    let handle = sink.start_span(
        SpanStart::new(span_name::EPISODE)
            .with_attribute(attribute::LOOP_ID, "loop-1")
            .with_attribute(attribute::EPISODE_ID, "episode-1")
            .with_attribute(attribute::CHECKPOINT_VERSION, "3"),
    );
    sink.end_span(
        handle,
        SpanEnd::ok().with_attribute(attribute::OUTCOME, "completed"),
    );

    let spans = sink.finished_spans();
    assert_eq!(spans.len(), 1);
    for (_, value) in spans[0].all_attributes() {
        for forbidden in FORBIDDEN_ATTRIBUTE_SUBSTRINGS {
            assert!(
                !value.contains(forbidden),
                "recorded attribute value `{value}` unexpectedly contains `{forbidden}`"
            );
        }
        // The redaction guarantee is about full payloads, not size per se,
        // but a bounded identifier/outcome value is never anywhere near
        // prompt- or Boot-Package-sized.
        assert!(
            value.len() < 256,
            "attribute value `{value}` looks payload-sized"
        );
    }
}

/// As above: proves the [`RecordingSink`]'s event plumbing round-trips
/// attribute values correctly, not that real instrumentation never emits a
/// sensitive one — see `no_span_or_event_attribute_leaks_marked_sensitive_content`
/// in `loopward-observability-otel` for the real end-to-end proof.
#[test]
fn span_events_stay_bounded_and_never_carry_raw_prompt_text() {
    let sink = RecordingSink::new();
    let handle = sink.start_span(SpanStart::new(span_name::WORK_UNIT_EXECUTE));
    sink.span_event(
        &handle,
        TelemetryEvent::new("rotation.requested")
            .with_attribute("loopward.rotation.reason", "work_unit_boundary"),
    );
    sink.end_span(handle, SpanEnd::ok());

    let events = sink.events();
    assert_eq!(events.len(), 1);
    let (_, event) = &events[0];
    for (_, value) in &event.attributes {
        for forbidden in FORBIDDEN_ATTRIBUTE_SUBSTRINGS {
            assert!(!value.contains(forbidden));
        }
    }
}

#[test]
fn span_links_name_the_five_documented_causal_relations() {
    let kinds = [
        SpanLinkKind::ContinuationOf,
        SpanLinkKind::RetryOf,
        SpanLinkKind::RecoveryOf,
        SpanLinkKind::RemediationOf,
        SpanLinkKind::ExternalExecution,
    ];
    let labels: Vec<&str> = kinds.iter().map(|kind| kind.as_str()).collect();
    assert_eq!(
        labels,
        vec![
            "continuation_of",
            "retry_of",
            "recovery_of",
            "remediation_of",
            "external_execution",
        ]
    );

    // Each is constructible as an actual link with an opaque reference,
    // matching how a continuation link or an external runner reference is
    // recorded in practice.
    let link = SpanLink::new(SpanLinkKind::ContinuationOf, "trace-1")
        .with_attribute(attribute::CHECKPOINT_VERSION, "2");
    assert_eq!(link.kind.as_str(), "continuation_of");
    assert_eq!(link.trace_ref, "trace-1");
}
