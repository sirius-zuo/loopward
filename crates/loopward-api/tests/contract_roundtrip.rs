//! Round-trip and typed-error contract tests for `loopward-api`.
//!
//! These tests pin down the wire shape of the public contracts: they must
//! fail to compile until the DTOs exist, and they must fail at runtime if a
//! future change silently loses a field or widens an enum to accept unknown
//! variants.

use loopward_api::context::{
    BootPackageManifest, ContextBudget, ContextClass, ContextItem, ContextPriority,
    ContinuationProposal, DecisionProposal, Fidelity, HandoffProposal, ObservationProposal,
    OpenIssueProposal, Sensitivity, StateAssertionProposal, Validity, VerificationStatus,
};
use loopward_api::ids::{AttemptId, EpisodeId, LoopId, SessionId, Timestamp, WorkUnitId};
use loopward_api::loop_service::{
    CommandMetadata, Constraint, ConstraintScope, ConstraintSource, CreateLoopRequest, EpisodeRef,
    Goal, LoopKind, LoopOutcome, LoopOutcomeKind, LoopPolicies, LoopStatus, LoopStatusView,
    SuccessCriterion, SuccessCriterionStatus, SuspensionView,
};
use loopward_api::resource::{
    CheckpointRef, OutputContract, PhaseRef, ResourceRef, RunnerRef, SchemaRef, VerificationPlanRef,
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

fn sample_handoff_proposal() -> HandoffProposal {
    HandoffProposal {
        completed_work: vec![StateAssertionProposal {
            statement: "Implemented the identifier newtypes.".to_string(),
            evidence: vec![ResourceRef {
                uri: "resource://diff/001".to_string(),
                media_type: None,
                digest: Some("sha256:diff".to_string()),
                version: None,
                size: None,
                summary: None,
            }],
            confidence: Some(0.9),
        }],
        active_work: vec![StateAssertionProposal {
            statement: "Implementing the runner protocol.".to_string(),
            evidence: vec![],
            confidence: None,
        }],
        unresolved: vec![OpenIssueProposal {
            statement: "LoopPolicies shape is still open.".to_string(),
            severity: Some("advisory".to_string()),
            evidence: vec![],
        }],
        observations: vec![ObservationProposal {
            statement: "The contract round-trips losslessly.".to_string(),
            verification_status: VerificationStatus::Verified,
            evidence: vec![],
        }],
        decision_proposals: vec![DecisionProposal {
            statement: "Keep proposal types append-only.".to_string(),
            rationale: Some("Preserves drain-side losslessness.".to_string()),
            alternatives_considered: vec!["Mutate in place".to_string()],
        }],
        relevant_resources: vec![ResourceRef {
            uri: "resource://output/017".to_string(),
            media_type: None,
            digest: Some("sha256:output".to_string()),
            version: None,
            size: None,
            summary: None,
        }],
        recommended_next_action: Some(ContinuationProposal {
            kind: "verify".to_string(),
            detail: None,
        }),
        pinned_digest_seen: "sha256:pinned".to_string(),
    }
}

#[test]
fn handoff_proposal_round_trips_through_json_losslessly() {
    let proposal = sample_handoff_proposal();

    let encoded = serde_json::to_string(&proposal).expect("serialize HandoffProposal");
    let decoded: HandoffProposal =
        serde_json::from_str(&encoded).expect("deserialize HandoffProposal");

    assert_eq!(decoded.pinned_digest_seen, "sha256:pinned");
    assert_eq!(decoded, proposal);
}

fn sample_create_loop_request() -> CreateLoopRequest {
    let mut policy_values = std::collections::BTreeMap::new();
    policy_values.insert(
        "allow_unverified_completion".to_string(),
        serde_json::json!(false),
    );
    policy_values.insert("max_retries".to_string(), serde_json::json!(3));

    CreateLoopRequest {
        metadata: CommandMetadata {
            request_id: "request-001".to_string(),
            idempotency_key: "create-loop-001".to_string(),
            expected_revision: None,
            principal: None,
            trace_context: TraceContext {
                traceparent: Some(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
                ),
                tracestate: None,
                baggage: vec![],
            },
        },
        protocol_version: loopward_api::PROTOCOL_VERSION.to_string(),
        kind: LoopKind::new("domain.example"),
        goal: Goal {
            statement: "Complete the durable task.".to_string(),
        },
        success_criteria: vec![SuccessCriterion {
            id: "criterion-001".to_string(),
            statement: "All required verification gates pass.".to_string(),
            status: SuccessCriterionStatus::Pending,
        }],
        pinned_constraints: vec![Constraint {
            id: "constraint-001".to_string(),
            statement: "Preserve the public contract.".to_string(),
            source: ConstraintSource::User,
            scope: ConstraintScope::Loop,
        }],
        domain_schema: SchemaRef::new("loopward.domain.example/v0.1"),
        initial_domain_state: serde_json::json!({}),
        policies: LoopPolicies {
            values: policy_values,
        },
    }
}

#[test]
fn create_loop_request_round_trips_through_json_losslessly() {
    let request = sample_create_loop_request();

    let encoded = serde_json::to_string(&request).expect("serialize CreateLoopRequest");
    let decoded: CreateLoopRequest =
        serde_json::from_str(&encoded).expect("deserialize CreateLoopRequest");

    assert_eq!(decoded.kind, request.kind);
    assert_eq!(decoded.goal, request.goal);
    assert_eq!(decoded.success_criteria, request.success_criteria);
    assert_eq!(decoded.pinned_constraints, request.pinned_constraints);
    assert_eq!(decoded.domain_schema, request.domain_schema);
    // The flattened `LoopPolicies` bag is the one construct in this crate
    // whose round-trip can't be verified by inspection alone: confirm both
    // the flattened keys and their values survive exactly.
    assert_eq!(decoded.policies.values, request.policies.values);
    assert_eq!(decoded.policies.values.len(), 2);
    assert_eq!(decoded.policies.values["max_retries"], serde_json::json!(3));

    assert_eq!(decoded, request);
}

#[test]
fn loop_status_view_round_trips_through_json_losslessly() {
    let view = LoopStatusView {
        loop_id: LoopId::new(),
        status: LoopStatus::Active,
        phase: PhaseRef::new("implementation"),
        aggregate_revision: 7,
        checkpoint_version: 1,
        active_episode: Some(EpisodeRef {
            episode_id: EpisodeId::new(),
            ordinal: 3,
        }),
        active_work_unit: Some(loopward_api::loop_service::WorkUnitRef {
            work_unit_id: WorkUnitId::new(),
            kind: WorkUnitKind::new("domain.example"),
        }),
        suspension: Some(SuspensionView {
            reason: "verification_pending".to_string(),
            since: Timestamp::from_unix_timestamp(1_800_000_000).expect("valid unix timestamp"),
            detail: Some("Awaiting human review.".to_string()),
        }),
        terminal_outcome: Some(LoopOutcome {
            kind: LoopOutcomeKind::Completed,
            summary: Some("All success criteria satisfied.".to_string()),
        }),
    };

    let encoded = serde_json::to_string(&view).expect("serialize LoopStatusView");
    let decoded: LoopStatusView =
        serde_json::from_str(&encoded).expect("deserialize LoopStatusView");

    assert_eq!(decoded, view);
}

#[test]
fn unknown_loop_status_variant_fails_deserialization_with_a_typed_error() {
    let json = serde_json::json!("not_a_real_loop_status");
    let result: Result<LoopStatus, serde_json::Error> = serde_json::from_value(json);
    assert!(
        result.is_err(),
        "unknown LoopStatus variant must not deserialize"
    );
}
