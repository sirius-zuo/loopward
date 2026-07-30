//! Round-trip and typed-error contract tests for `loopward-api`.
//!
//! These tests pin down the wire shape of the public contracts: they must
//! fail to compile until the DTOs exist, and they must fail at runtime if a
//! future change silently loses a field or widens an enum to accept unknown
//! variants.

use loopward_api::context::{
    BootPackageManifest, ContextBudget, ContextClass, ContextItem, ContextPriority, Fidelity,
    Sensitivity, Validity,
};
use loopward_api::ids::{AttemptId, EpisodeId, LoopId, SessionId, WorkUnitId};
use loopward_api::resource::{
    CheckpointRef, OutputContract, ResourceRef, RunnerRef, VerificationPlanRef,
};
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{
    AttemptBudget, ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitRequest,
    WorkUnitStatus,
};

fn sample_request() -> WorkUnitRequest {
    let loop_id = LoopId::new();
    let episode_id = EpisodeId::new();
    let session_id = SessionId::new();
    let work_unit_id = WorkUnitId::new();
    let attempt_id = AttemptId::new();

    let boot_package = BootPackageManifest {
        schema_version: "loopward.boot-package-manifest/v0.1".to_string(),
        loop_id,
        episode_id,
        session_generation: 3,
        work_unit_id,
        checkpoint_ref: CheckpointRef::new("checkpoint://loop-001/1"),
        context_rule_version: "deterministic-v0.1".to_string(),
        budget: ContextBudget {
            model_window_tokens: None,
            soft_input_limit_tokens: 24_000,
            hard_input_limit_tokens: 48_000,
            reserved_output_tokens: 8_000,
            reserved_tool_tokens: 8_000,
            safety_margin_tokens: 4_000,
        },
        items: vec![ContextItem {
            id: "context-item-001".to_string(),
            kind: "constraint".to_string(),
            source: ResourceRef {
                uri: "constraint://constraint-001".to_string(),
                media_type: None,
                digest: None,
                version: None,
                size: None,
                summary: None,
            },
            classification: ContextClass::Pinned,
            priority: ContextPriority::Required,
            fidelity: Fidelity::Verbatim,
            sensitivity: Sensitivity::Internal,
            validity: Validity::Current,
            estimated_tokens: Some(12),
            estimate_method: None,
            estimate_confidence: Some(1.0),
            digest: "sha256:item".to_string(),
            selection_reason: "Active loop-scoped user constraint".to_string(),
        }],
        pinned_digest: "sha256:pinned".to_string(),
        manifest_digest: "sha256:manifest".to_string(),
    };

    let work_unit = WorkUnit {
        id: work_unit_id,
        kind: WorkUnitKind::new("domain.example"),
        objective: "Produce one externally observable unit of progress.".to_string(),
        status: WorkUnitStatus::Pending,
        inputs: vec![],
        constraints: vec![],
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_string(),
        },
        verification_plan: VerificationPlanRef {
            reference: "verification-plan://plan-017".to_string(),
            version: 1,
        },
        runner: RunnerRef {
            name: "command".to_string(),
            profile: Some("default".to_string()),
        },
        budget: ResourceBudget {
            context: None,
            attempts: Some(AttemptBudget { maximum: Some(2) }),
        },
        retry_policy: RetryPolicy {
            retryable_codes: vec![],
            require_reconciliation_before_retry: true,
        },
    };

    WorkUnitRequest {
        protocol_version: loopward_api::PROTOCOL_VERSION.to_string(),
        loop_id,
        episode_id,
        session_id,
        session_generation: 3,
        work_unit,
        attempt_id,
        idempotency_key: "attempt-017-1".to_string(),
        boot_package,
        resource_grants: vec![],
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_string(),
        },
        budget: ResourceBudget {
            context: None,
            attempts: None,
        },
        deadline: None,
        trace_context: TraceContext {
            traceparent: Some(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            tracestate: None,
            baggage: vec![],
        },
    }
}

#[test]
fn work_unit_request_round_trips_through_json_losslessly() {
    let request = sample_request();

    let encoded = serde_json::to_string(&request).expect("serialize WorkUnitRequest");
    let decoded: WorkUnitRequest =
        serde_json::from_str(&encoded).expect("deserialize WorkUnitRequest");

    assert_eq!(decoded.loop_id, request.loop_id);
    assert_eq!(decoded.session_generation, 3);
    assert_eq!(
        decoded.trace_context.traceparent.as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );
    assert_eq!(decoded.boot_package.pinned_digest, "sha256:pinned");

    // Full structural equality: nothing else was lost either.
    assert_eq!(decoded, request);
}

#[test]
fn unknown_work_unit_status_variant_fails_deserialization_with_a_typed_error() {
    let json = serde_json::json!({
        "id": WorkUnitId::new().to_string(),
        "kind": "domain.example",
        "objective": "Produce one externally observable unit of progress.",
        "status": "not_a_real_status",
        "inputs": [],
        "constraints": [],
        "output_contract": { "schema": "loopward.work-unit-result/v0.1" },
        "verification_plan": { "ref": "verification-plan://plan-017", "version": 1 },
        "runner": { "name": "command", "profile": "default" },
        "budget": { "context": null, "attempts": null },
        "retry_policy": { "retryable_codes": [], "require_reconciliation_before_retry": true }
    });

    let result: Result<WorkUnit, serde_json::Error> = serde_json::from_value(json);

    let err = result.expect_err("unknown WorkUnitStatus variant must not deserialize");
    // A typed serde error, not a silent fallback to a known variant.
    assert!(err.to_string().contains("not_a_real_status") || err.is_data());
}

#[test]
fn unknown_context_class_variant_fails_deserialization_with_a_typed_error() {
    let json = serde_json::json!("not_a_real_class");
    let result: Result<ContextClass, serde_json::Error> = serde_json::from_value(json);
    assert!(
        result.is_err(),
        "unknown ContextClass variant must not deserialize"
    );
}
