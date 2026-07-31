//! Contract tests for verification gates, ordered aggregation, and the Loop
//! completion gate (`docs/06-verification-and-recovery.md` §2-§11).
//!
//! The property under test throughout is the one stated in
//! `docs/06-verification-and-recovery.md` §1: *a runner result is not
//! accepted merely because the runner reports success*. Every test drives
//! [`VerificationEngine`] through its public surface with a real
//! [`ResourceBroker`] fake, and the deterministic-command gate really does
//! spawn `sh`, so "the gate passed" always means a subprocess actually ran
//! and actually exited zero.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::ids::{AttemptId, CheckpointId, LoopId, WorkUnitId};
use loopward_api::resource::{OutputContract, ResourceRef, SchemaRef};
use loopward_api::work_unit::{UsageReport, WorkUnitOutcome, WorkUnitResult};
use loopward_core::{Checkpoint, VerificationOutcome};
use loopward_runtime::ports::{ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle};
use loopward_runtime::verification::{
    AgenticGateConfig, AggregationPolicy, CompletionObstacle, CriterionId, DeterministicGateConfig,
    Digest, EvidenceGap, FindingLineage, GateConfig, GateEnforcement, GateId, GateOutcome,
    GateRequest, HumanGateConfig, IntegrityGateConfig, PolicyGateConfig, RemediationPolicy,
    SchemaGateConfig, VerificationEngine, VerificationError, VerificationFinding, VerificationGate,
    VerificationGateKind, VerificationGateSpec, VerificationPlan, VerificationPlanId,
    VerificationRequest, VerificationTarget, assess_completion,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Test doubles and fixtures
// ---------------------------------------------------------------------------

/// An in-memory [`ResourceBroker`]. Content registered up front is served
/// verbatim; anything else resolves to deterministic synthetic bytes so a
/// gate that resolves an incidental reference does not fail the test.
#[derive(Default)]
struct InMemoryBroker {
    content: Mutex<HashMap<String, Vec<u8>>>,
    open: Mutex<HashMap<String, (ResourceBeginHint, Vec<u8>)>>,
    next: AtomicU64,
}

impl InMemoryBroker {
    fn with(entries: &[(&str, &str)]) -> Arc<Self> {
        let broker = Self::default();
        {
            let mut content = broker.content.lock().expect("content lock poisoned");
            for (uri, body) in entries {
                content.insert((*uri).to_owned(), (*body).as_bytes().to_vec());
            }
        }
        Arc::new(broker)
    }

    fn finalized(&self, uri: &str) -> Option<Vec<u8>> {
        self.content
            .lock()
            .expect("content lock poisoned")
            .get(uri)
            .cloned()
    }
}

#[async_trait]
impl ResourceBroker for InMemoryBroker {
    async fn begin(&self, hint: ResourceBeginHint) -> Result<ResourceHandle, ResourceError> {
        let ordinal = self.next.fetch_add(1, Ordering::SeqCst);
        let handle = ResourceHandle::new(format!("handle-{ordinal}"));
        self.open
            .lock()
            .expect("open lock poisoned")
            .insert(handle.as_str().to_owned(), (hint, Vec::new()));
        Ok(handle)
    }

    async fn write(&self, handle: &ResourceHandle, bytes: &[u8]) -> Result<(), ResourceError> {
        let mut open = self.open.lock().expect("open lock poisoned");
        let entry = open
            .get_mut(handle.as_str())
            .ok_or_else(|| ResourceError::UnknownHandle(handle.as_str().to_owned()))?;
        entry.1.extend_from_slice(bytes);
        Ok(())
    }

    async fn finalize(&self, handle: ResourceHandle) -> Result<ResourceRef, ResourceError> {
        let (hint, bytes) = self
            .open
            .lock()
            .expect("open lock poisoned")
            .remove(handle.as_str())
            .ok_or_else(|| ResourceError::UnknownHandle(handle.as_str().to_owned()))?;
        let uri = format!("resource://verification/{}", handle.as_str());
        self.content
            .lock()
            .expect("content lock poisoned")
            .insert(uri.clone(), bytes.clone());
        Ok(ResourceRef {
            uri,
            media_type: hint.media_type,
            digest: Some(sha256_of(&bytes)),
            version: None,
            size: Some(bytes.len() as u64),
            summary: hint.summary,
        })
    }

    async fn abort(&self, handle: ResourceHandle) -> Result<(), ResourceError> {
        self.open
            .lock()
            .expect("open lock poisoned")
            .remove(handle.as_str());
        Ok(())
    }

    async fn resolve(&self, reference: &ResourceRef) -> Result<Vec<u8>, ResourceError> {
        self.content
            .lock()
            .expect("content lock poisoned")
            .get(&reference.uri)
            .cloned()
            .ok_or_else(|| ResourceError::UnresolvableReference(reference.uri.clone()))
    }
}

fn sha256_of(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .fold(String::from("sha256:"), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// A gate that records the order in which the engine invoked it and then
/// returns a fixed outcome.
struct RecordingGate {
    kind: VerificationGateKind,
    outcome: GateOutcome,
    log: Arc<Mutex<Vec<VerificationGateKind>>>,
}

#[async_trait]
impl VerificationGate for RecordingGate {
    fn kind(&self) -> VerificationGateKind {
        self.kind
    }

    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError> {
        self.log.lock().expect("log lock poisoned").push(self.kind);
        Ok(request.finding(self.outcome, "test.recorded", "recorded by the test gate"))
    }
}

fn recording_engine(log: &Arc<Mutex<Vec<VerificationGateKind>>>) -> VerificationEngine {
    let mut engine = VerificationEngine::new();
    for kind in VerificationGateKind::EVALUATION_ORDER {
        engine = engine.with_gate(Arc::new(RecordingGate {
            kind: *kind,
            outcome: GateOutcome::Passed,
            log: Arc::clone(log),
        }));
    }
    engine
}

fn output_ref(uri: &str, body: &str) -> ResourceRef {
    ResourceRef {
        uri: uri.to_owned(),
        media_type: Some("application/json".to_owned()),
        digest: Some(sha256_of(body.as_bytes())),
        version: None,
        size: Some(body.len() as u64),
        summary: None,
    }
}

const RESULT_BODY: &str = r#"{"summary":"done","artifact":"report.md"}"#;

fn produced_result(outputs: Vec<ResourceRef>) -> WorkUnitResult {
    WorkUnitResult {
        protocol_version: "loopward.work-unit-result/v0.1".to_owned(),
        attempt_id: AttemptId::from_uuid(Uuid::from_u128(0x11)),
        // The runner asserts it succeeded. That assertion is a proposal.
        outcome: WorkUnitOutcome::Produced,
        outputs,
        state_proposals: Vec::new(),
        observation_proposals: Vec::new(),
        decision_proposals: Vec::new(),
        verification_evidence: Vec::new(),
        unresolved: Vec::new(),
        recommended_continuation: None,
        usage: UsageReport::default(),
        runner_trace_refs: Vec::new(),
    }
}

fn target_for(outputs: &[ResourceRef], plan_version: u64) -> VerificationTarget {
    let mut resource_digests = BTreeMap::new();
    for output in outputs {
        resource_digests.insert(
            output.uri.clone(),
            Digest::new(output.digest.clone().unwrap_or_default()),
        );
    }
    VerificationTarget {
        work_unit_id: WorkUnitId::from_uuid(Uuid::from_u128(0x22)),
        result_digest: Digest::new(sha256_of(RESULT_BODY.as_bytes())),
        resource_digests,
        workspace_revision: Some("workspace-rev-1".to_owned()),
        plan_version,
        constraint_digest: Some(Digest::new(sha256_of(b"constraints"))),
    }
}

fn spec(
    id: &str,
    kind: VerificationGateKind,
    enforcement: GateEnforcement,
    config: GateConfig,
) -> VerificationGateSpec {
    VerificationGateSpec {
        id: GateId::new(id),
        kind,
        enforcement,
        criterion: None,
        config,
    }
}

fn spec_with_criterion(
    id: &str,
    kind: VerificationGateKind,
    enforcement: GateEnforcement,
    criterion: &str,
    config: GateConfig,
) -> VerificationGateSpec {
    VerificationGateSpec {
        id: GateId::new(id),
        kind,
        enforcement,
        criterion: Some(CriterionId::new(criterion)),
        config,
    }
}

fn command_config(script: &str) -> GateConfig {
    GateConfig::Deterministic(DeterministicGateConfig {
        program: "sh".to_owned(),
        args: vec!["-c".to_owned(), script.to_owned()],
        ..DeterministicGateConfig::default()
    })
}

fn plan(target: VerificationTarget, gates: Vec<VerificationGateSpec>) -> VerificationPlan {
    VerificationPlan {
        id: VerificationPlanId::new("verification-plan-001"),
        version: target.plan_version,
        target,
        gates,
        aggregation: AggregationPolicy::default(),
        remediation_policy: RemediationPolicy::default(),
    }
}

fn lineage(checkpoint_version: u64) -> FindingLineage {
    FindingLineage {
        loop_id: LoopId::from_uuid(Uuid::from_u128(0x33)),
        work_unit_id: WorkUnitId::from_uuid(Uuid::from_u128(0x22)),
        attempt_id: AttemptId::from_uuid(Uuid::from_u128(0x11)),
        plan_id: VerificationPlanId::new("verification-plan-001"),
        plan_version: 1,
        checkpoint_version,
    }
}

fn request(
    plan: VerificationPlan,
    result: WorkUnitResult,
    broker: Arc<InMemoryBroker>,
) -> VerificationRequest {
    VerificationRequest {
        plan,
        result: Arc::new(result),
        lineage: lineage(4),
        prior_findings: Vec::new(),
        resources: broker,
        observed_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn checkpoint(version: u64) -> Checkpoint {
    Checkpoint {
        id: CheckpointId::from_uuid(Uuid::from_u128(0x44)),
        loop_id: LoopId::from_uuid(Uuid::from_u128(0x33)),
        version,
        aggregate_revision: version * 2,
        parent: None,
        episode_id: None,
        integrity_digest: None,
    }
}

fn finding(
    gate: &str,
    outcome: GateOutcome,
    enforcement: GateEnforcement,
    criterion: Option<&str>,
    target_digest: &Digest,
    checkpoint_version: u64,
) -> VerificationFinding {
    VerificationFinding {
        gate_id: GateId::new(gate),
        kind: VerificationGateKind::Deterministic,
        enforcement,
        outcome,
        code: "test.finding".to_owned(),
        statement: "recorded by the test".to_owned(),
        evidence: Vec::new(),
        target_digest: target_digest.clone(),
        criterion: criterion.map(CriterionId::new),
        lineage: lineage(checkpoint_version),
        input_request: None,
        observed_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

// ---------------------------------------------------------------------------
// Gate ordering (docs/06 §4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cheap_conclusive_gates_run_before_expensive_semantic_gates() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let engine = recording_engine(&log);
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);

    // Declared deliberately out of order: the plan lists the expensive
    // semantic gates first and the cheap conclusive gates last.
    let gates = vec![
        spec(
            "human",
            VerificationGateKind::Human,
            GateEnforcement::Required,
            GateConfig::Human(HumanGateConfig::default()),
        ),
        spec(
            "agentic",
            VerificationGateKind::Agentic,
            GateEnforcement::Required,
            GateConfig::Agentic(AgenticGateConfig::default()),
        ),
        spec(
            "policy",
            VerificationGateKind::Policy,
            GateEnforcement::Required,
            GateConfig::Policy(PolicyGateConfig::default()),
        ),
        spec(
            "deterministic",
            VerificationGateKind::Deterministic,
            GateEnforcement::Required,
            command_config("exit 0"),
        ),
        spec(
            "integrity",
            VerificationGateKind::Integrity,
            GateEnforcement::Required,
            GateConfig::Integrity(IntegrityGateConfig::default()),
        ),
        spec(
            "schema",
            VerificationGateKind::Schema,
            GateEnforcement::Required,
            GateConfig::Schema(SchemaGateConfig::default()),
        ),
    ];

    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);
    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(
        *log.lock().expect("log lock poisoned"),
        vec![
            VerificationGateKind::Schema,
            VerificationGateKind::Integrity,
            VerificationGateKind::Deterministic,
            VerificationGateKind::Policy,
            VerificationGateKind::Agentic,
            VerificationGateKind::Human,
        ],
        "schema and integrity must run before deterministic, policy, and semantic gates"
    );
    assert_eq!(run.outcome, VerificationOutcome::Accepted);
    let recorded: Vec<_> = run.findings.iter().map(|f| f.kind).collect();
    assert_eq!(
        recorded,
        vec![
            VerificationGateKind::Schema,
            VerificationGateKind::Integrity,
            VerificationGateKind::Deterministic,
            VerificationGateKind::Policy,
            VerificationGateKind::Agentic,
            VerificationGateKind::Human,
        ],
        "findings are recorded in evaluation order"
    );
}

#[tokio::test]
async fn a_failed_cheap_gate_short_circuits_the_expensive_ones() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut engine = recording_engine(&log);
    engine = engine.with_gate(Arc::new(RecordingGate {
        kind: VerificationGateKind::Schema,
        outcome: GateOutcome::Failed,
        log: Arc::clone(&log),
    }));

    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![
        spec(
            "schema",
            VerificationGateKind::Schema,
            GateEnforcement::Required,
            GateConfig::Schema(SchemaGateConfig::default()),
        ),
        spec(
            "agentic",
            VerificationGateKind::Agentic,
            GateEnforcement::Required,
            GateConfig::Agentic(AgenticGateConfig::default()),
        ),
    ];

    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);
    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(
        *log.lock().expect("log lock poisoned"),
        vec![VerificationGateKind::Schema],
        "the expensive semantic gate must not run after a required cheap gate failed"
    );
    assert_eq!(run.outcome, VerificationOutcome::Rejected);
}

// ---------------------------------------------------------------------------
// Required vs advisory aggregation (docs/06 §7)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_required_failed_gate_rejects_the_result() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "unit-tests",
        VerificationGateKind::Deterministic,
        GateEnforcement::Required,
        command_config("exit 1"),
    )];

    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);
    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Rejected);
    assert!(!run.outcome.is_accepting());
    let finding = run
        .findings
        .iter()
        .find(|f| f.gate_id == GateId::new("unit-tests"))
        .expect("the required gate produced a finding");
    assert_eq!(finding.outcome, GateOutcome::Failed);
    assert!(finding.is_blocking(), "a failed required finding blocks");
}

#[tokio::test]
async fn an_advisory_failed_gate_does_not_override_a_required_pass() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![
        spec(
            "unit-tests",
            VerificationGateKind::Deterministic,
            GateEnforcement::Required,
            command_config("exit 0"),
        ),
        spec(
            "style-lint",
            VerificationGateKind::Deterministic,
            GateEnforcement::Advisory,
            command_config("exit 3"),
        ),
    ];

    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);
    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(
        run.outcome,
        VerificationOutcome::Accepted,
        "an advisory failure must not reject a run whose required gates passed"
    );

    let advisory = run
        .findings
        .iter()
        .find(|f| f.gate_id == GateId::new("style-lint"))
        .expect("the advisory failure is still recorded as a durable finding");
    assert_eq!(advisory.outcome, GateOutcome::Failed);
    assert_eq!(advisory.enforcement, GateEnforcement::Advisory);
    assert!(
        !advisory.is_blocking(),
        "an advisory finding is never blocking"
    );

    let required = run
        .findings
        .iter()
        .find(|f| f.gate_id == GateId::new("unit-tests"))
        .expect("the required pass is recorded too");
    assert_eq!(
        required.outcome,
        GateOutcome::Passed,
        "the advisory failure must not hide the required pass"
    );
}

// ---------------------------------------------------------------------------
// Target binding and staleness (docs/06 §6, §13)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_target_digest_change_marks_prior_findings_stale() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let old_target = target_for(&outputs, 1);
    let old_digest = old_target.binding_digest();

    let prior = finding(
        "unit-tests",
        GateOutcome::Passed,
        GateEnforcement::Required,
        Some("criterion-001"),
        &old_digest,
        4,
    );

    // The workspace moved on: same outputs, new revision, so the binding
    // digest changes.
    let mut new_target = target_for(&outputs, 1);
    new_target.workspace_revision = Some("workspace-rev-2".to_owned());
    assert_ne!(new_target.binding_digest(), old_digest);

    let gates = vec![spec(
        "unit-tests",
        VerificationGateKind::Deterministic,
        GateEnforcement::Required,
        command_config("exit 0"),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);
    let mut req = request(
        plan(new_target.clone(), gates),
        produced_result(outputs),
        broker,
    );
    req.prior_findings = vec![prior];

    let run = engine.evaluate(req).await.expect("evaluation completes");

    let carried = run
        .findings
        .iter()
        .find(|f| f.target_digest == old_digest)
        .expect("the prior finding is carried forward");
    assert_eq!(
        carried.outcome,
        GateOutcome::Stale,
        "a prior pass whose target moved is stale, not still passing"
    );
    assert!(
        !carried.is_current_for(&new_target),
        "a stale finding does not bind to the current target"
    );

    // The freshly evaluated gate binds to the new target.
    let fresh = run
        .findings
        .iter()
        .find(|f| f.target_digest == new_target.binding_digest())
        .expect("the re-evaluated gate binds to the new target");
    assert_eq!(fresh.outcome, GateOutcome::Passed);
    assert_eq!(run.outcome, VerificationOutcome::Accepted);
}

#[test]
fn a_stale_pass_cannot_satisfy_an_acceptance_criterion() {
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let stale = finding(
        "unit-tests",
        GateOutcome::Stale,
        GateEnforcement::Required,
        Some("criterion-001"),
        &target.binding_digest(),
        4,
    );

    let assessment = assess_completion(&checkpoint(4), std::slice::from_ref(&stale));
    assert!(!assessment.permitted);
    assert!(
        assessment
            .obstacles
            .contains(&CompletionObstacle::CriterionUnsupported {
                criterion: CriterionId::new("criterion-001"),
                gap: EvidenceGap::Stale,
            })
    );
}

// ---------------------------------------------------------------------------
// A runner cannot accept its own work (docs/06 §1, §3, §16)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runner_reported_success_cannot_complete_a_work_unit() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);

    // The runner says `Produced` — its strongest possible self-report — but
    // the required deterministic gate disagrees.
    let gates = vec![spec(
        "unit-tests",
        VerificationGateKind::Deterministic,
        GateEnforcement::Required,
        command_config("echo 'FAIL: 2 tests failed' >&2; exit 1"),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);
    let result = produced_result(outputs.clone());
    assert_eq!(result.outcome, WorkUnitOutcome::Produced);

    let run = engine
        .evaluate(request(plan(target, gates), result, broker))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Rejected);
    assert!(!run.outcome.is_accepting());
}

#[tokio::test]
async fn a_plan_without_a_required_gate_cannot_accept_anything() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "style-lint",
        VerificationGateKind::Deterministic,
        GateEnforcement::Advisory,
        command_config("exit 0"),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);

    let error = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect_err("a plan with no required gate is not a verification plan");
    assert!(matches!(error, VerificationError::NoRequiredGate { .. }));
}

// ---------------------------------------------------------------------------
// Concrete Phase One gates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_schema_gate_fails_a_result_missing_a_required_field() {
    let engine = VerificationEngine::new();
    let body = r#"{"summary":"done"}"#;
    let outputs = vec![output_ref("resource://output/1", body)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "schema",
        VerificationGateKind::Schema,
        GateEnforcement::Required,
        GateConfig::Schema(SchemaGateConfig {
            contract: OutputContract {
                schema: "loopward.domain.example/v0.1".to_owned(),
            },
            expected_protocol_version: Some("loopward.work-unit-result/v0.1".to_owned()),
            required_pointers: vec!["/summary".to_owned(), "/artifact".to_owned()],
            minimum_outputs: 1,
        }),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", body)]);

    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Rejected);
    assert_eq!(run.findings[0].outcome, GateOutcome::Failed);
    assert_eq!(run.findings[0].code, "verification.schema.missing_field");
    assert!(run.findings[0].statement.contains("/artifact"));
}

#[tokio::test]
async fn the_schema_gate_accepts_a_non_json_output_when_nothing_is_configured_to_check() {
    let engine = VerificationEngine::new();
    // A plain-Markdown artifact, not JSON at all. Nothing in the plan below
    // names a `required_pointers` entry, so the schema gate has nothing to
    // check inside the bytes and must not fail parsing content it was never
    // configured to look at.
    let body = "# Report\n\nEverything looks good.\n";
    let outputs = vec![output_ref("resource://output/1", body)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "schema",
        VerificationGateKind::Schema,
        GateEnforcement::Required,
        GateConfig::Schema(SchemaGateConfig {
            contract: OutputContract {
                schema: "loopward.domain.markdown-report/v0.1".to_owned(),
            },
            ..SchemaGateConfig::default()
        }),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", body)]);

    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Accepted);
    assert_eq!(run.findings[0].outcome, GateOutcome::Passed);
}

#[tokio::test]
async fn the_integrity_gate_fails_when_content_does_not_hash_to_its_declared_digest() {
    let engine = VerificationEngine::new();
    // The declared digest is over `RESULT_BODY`, but the broker serves
    // something else: the bytes were replaced after the runner declared them.
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "integrity",
        VerificationGateKind::Integrity,
        GateEnforcement::Required,
        GateConfig::Integrity(IntegrityGateConfig {
            require_output_digests: true,
            verify_content_digests: true,
            required_evidence: Vec::new(),
        }),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", "tampered")]);

    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Rejected);
    assert_eq!(
        run.findings[0].code,
        "verification.integrity.digest_mismatch"
    );
}

#[tokio::test]
async fn the_deterministic_gate_records_real_command_output_as_evidence() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "unit-tests",
        VerificationGateKind::Deterministic,
        GateEnforcement::Required,
        GateConfig::Deterministic(DeterministicGateConfig {
            program: "sh".to_owned(),
            args: vec!["-c".to_owned(), "echo loopward-gate-ran; exit 0".to_owned()],
            required_stdout_substring: Some("loopward-gate-ran".to_owned()),
            ..DeterministicGateConfig::default()
        }),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);

    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            Arc::clone(&broker),
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Accepted);
    let evidence = run.findings[0]
        .evidence
        .first()
        .expect("the command's captured output is durable evidence");
    let bytes = broker
        .finalized(&evidence.uri)
        .expect("evidence was finalized through the broker");
    let text = String::from_utf8(bytes).expect("captured output is utf-8");
    assert!(
        text.contains("loopward-gate-ran"),
        "a real subprocess ran and its output was captured: {text}"
    );
}

#[tokio::test]
async fn the_deterministic_gate_fails_when_required_output_is_absent() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "unit-tests",
        VerificationGateKind::Deterministic,
        GateEnforcement::Required,
        GateConfig::Deterministic(DeterministicGateConfig {
            program: "sh".to_owned(),
            args: vec!["-c".to_owned(), "echo nothing-useful".to_owned()],
            required_stdout_substring: Some("ALL TESTS PASSED".to_owned()),
            ..DeterministicGateConfig::default()
        }),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);

    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Rejected);
    assert_eq!(
        run.findings[0].code,
        "verification.deterministic.missing_output"
    );
}

#[tokio::test]
async fn the_policy_gate_rejects_a_runner_outcome_the_policy_does_not_admit() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "policy",
        VerificationGateKind::Policy,
        GateEnforcement::Required,
        GateConfig::Policy(PolicyGateConfig {
            allowed_outcomes: vec![WorkUnitOutcome::Produced],
            ..PolicyGateConfig::default()
        }),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);
    let mut result = produced_result(outputs);
    result.outcome = WorkUnitOutcome::Partial;

    let run = engine
        .evaluate(request(plan(target, gates), result, broker))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Rejected);
    assert_eq!(
        run.findings[0].code,
        "verification.policy.outcome_not_admitted"
    );
}

#[tokio::test]
async fn the_human_gate_suspends_with_a_structured_input_request() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "approval",
        VerificationGateKind::Human,
        GateEnforcement::Required,
        GateConfig::Human(HumanGateConfig {
            code: "release.approval".to_owned(),
            prompt: "Approve the release?".to_owned(),
            expected_schema: SchemaRef::new("loopward.approval/v0.1"),
        }),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);

    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Suspended);
    let requested = run.findings[0]
        .input_request
        .as_ref()
        .expect("a human gate suspends the Loop with a structured request");
    assert_eq!(requested.code, "release.approval");
    assert_eq!(requested.prompt, "Approve the release?");
    assert_eq!(
        requested.evidence.len(),
        1,
        "the request carries the evidence a reviewer needs"
    );
}

#[tokio::test]
async fn the_agentic_gate_suspends_rather_than_asserting_authority() {
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let gates = vec![spec(
        "semantic-review",
        VerificationGateKind::Agentic,
        GateEnforcement::Required,
        GateConfig::Agentic(AgenticGateConfig::default()),
    )];
    let broker = InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]);

    let run = engine
        .evaluate(request(
            plan(target, gates),
            produced_result(outputs),
            broker,
        ))
        .await
        .expect("evaluation completes");

    assert_eq!(run.outcome, VerificationOutcome::Suspended);
    assert_eq!(run.findings[0].outcome, GateOutcome::Suspended);
}

// ---------------------------------------------------------------------------
// Loop completion gate (docs/06 §11)
// ---------------------------------------------------------------------------

#[test]
fn loop_completion_fails_while_a_required_criterion_lacks_current_evidence() {
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let target = target_for(&outputs, 1);
    let digest = target.binding_digest();

    let findings = vec![
        finding(
            "criterion-001-gate",
            GateOutcome::Passed,
            GateEnforcement::Required,
            Some("criterion-001"),
            &digest,
            7,
        ),
        finding(
            "criterion-002-gate",
            GateOutcome::Inconclusive,
            GateEnforcement::Required,
            Some("criterion-002"),
            &digest,
            7,
        ),
    ];

    let assessment = assess_completion(&checkpoint(7), &findings);

    assert!(
        !assessment.permitted,
        "the Loop cannot terminate while a required criterion is unsupported"
    );
    assert_eq!(
        assessment.satisfied,
        vec![CriterionId::new("criterion-001")]
    );
    assert!(
        assessment
            .obstacles
            .contains(&CompletionObstacle::CriterionUnsupported {
                criterion: CriterionId::new("criterion-002"),
                gap: EvidenceGap::Inconclusive,
            })
    );
}

#[test]
fn loop_completion_fails_when_evidence_predates_the_current_checkpoint() {
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let digest = target_for(&outputs, 1).binding_digest();

    // The pass was recorded against Checkpoint 6; the Loop is now at 7.
    let findings = vec![finding(
        "criterion-001-gate",
        GateOutcome::Passed,
        GateEnforcement::Required,
        Some("criterion-001"),
        &digest,
        6,
    )];

    let assessment = assess_completion(&checkpoint(7), &findings);
    assert!(!assessment.permitted);
    assert!(
        assessment
            .obstacles
            .contains(&CompletionObstacle::CriterionUnsupported {
                criterion: CriterionId::new("criterion-001"),
                gap: EvidenceGap::NotCurrent,
            })
    );
}

#[test]
fn loop_completion_is_permitted_when_every_required_criterion_has_current_evidence() {
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let digest = target_for(&outputs, 1).binding_digest();

    let findings = vec![
        finding(
            "criterion-001-gate",
            GateOutcome::Passed,
            GateEnforcement::Required,
            Some("criterion-001"),
            &digest,
            7,
        ),
        finding(
            "criterion-002-gate",
            GateOutcome::Passed,
            GateEnforcement::Required,
            Some("criterion-002"),
            &digest,
            7,
        ),
        // An advisory failure is recorded but never blocks completion.
        finding(
            "style-lint",
            GateOutcome::Failed,
            GateEnforcement::Advisory,
            Some("criterion-003"),
            &digest,
            7,
        ),
    ];

    let assessment = assess_completion(&checkpoint(7), &findings);
    assert!(
        assessment.permitted,
        "obstacles: {:?}",
        assessment.obstacles
    );
    assert_eq!(
        assessment.satisfied,
        vec![
            CriterionId::new("criterion-001"),
            CriterionId::new("criterion-002")
        ]
    );
    assert!(assessment.obstacles.is_empty());
}

#[tokio::test]
async fn completion_stays_permitted_as_verification_evidence_accumulates_across_checkpoints() {
    // `VerificationRun.findings` is durable and ever-growing: every prior
    // run's findings are carried forward (superseded to `Stale` once their
    // target moves), and every fresh finding is appended. Feeding that
    // accumulated, realistic history into `assess_completion` must not make
    // completion *less* likely over time — a criterion the Loop's current
    // evidence satisfies stays satisfied no matter how much now-superseded
    // history sits alongside it.
    let engine = VerificationEngine::new();
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let gates = vec![spec_with_criterion(
        "unit-tests",
        VerificationGateKind::Deterministic,
        GateEnforcement::Required,
        "criterion-001",
        command_config("exit 0"),
    )];

    // First verification run, recorded against Checkpoint 7.
    let target_v1 = target_for(&outputs, 1);
    let mut first_req = request(
        plan(target_v1, gates.clone()),
        produced_result(outputs.clone()),
        InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]),
    );
    first_req.lineage.checkpoint_version = 7;
    let first_run = engine
        .evaluate(first_req)
        .await
        .expect("first evaluation completes");
    assert_eq!(first_run.outcome, VerificationOutcome::Accepted);

    // The Loop advances to Checkpoint 8, and the workspace moved on too, so
    // the target binding changes and the Work Unit is re-verified there.
    let mut target_v2 = target_for(&outputs, 1);
    target_v2.workspace_revision = Some("workspace-rev-2".to_owned());
    let mut second_req = request(
        plan(target_v2, gates),
        produced_result(outputs),
        InMemoryBroker::with(&[("resource://output/1", RESULT_BODY)]),
    );
    second_req.lineage.checkpoint_version = 8;
    second_req.prior_findings = first_run.findings;
    let second_run = engine
        .evaluate(second_req)
        .await
        .expect("second evaluation completes");
    assert_eq!(second_run.outcome, VerificationOutcome::Accepted);

    // `second_run.findings` now durably carries both the Checkpoint-7 pass
    // (stale w.r.t. Checkpoint 8's target, and recorded at an earlier
    // Checkpoint version besides) and the fresh Checkpoint-8 pass. Neither
    // the out-of-date version nor the superseded target may block a Loop
    // that is, in fact, fully verified right now.
    let assessment = assess_completion(&checkpoint(8), &second_run.findings);
    assert!(
        assessment.permitted,
        "obstacles: {:?}",
        assessment.obstacles
    );
    assert_eq!(
        assessment.satisfied,
        vec![CriterionId::new("criterion-001")]
    );
}

#[test]
fn loop_completion_fails_without_any_required_evidence_at_all() {
    let assessment = assess_completion(&checkpoint(7), &[]);
    assert!(!assessment.permitted);
    assert_eq!(
        assessment.obstacles,
        vec![CompletionObstacle::NoRequiredEvidence]
    );
}

#[test]
fn loop_completion_ignores_findings_from_another_loop() {
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let digest = target_for(&outputs, 1).binding_digest();
    let mut foreign = finding(
        "criterion-001-gate",
        GateOutcome::Passed,
        GateEnforcement::Required,
        Some("criterion-001"),
        &digest,
        7,
    );
    foreign.lineage.loop_id = LoopId::from_uuid(Uuid::from_u128(0x99));

    let assessment = assess_completion(&checkpoint(7), std::slice::from_ref(&foreign));
    assert!(!assessment.permitted);
    assert!(
        assessment
            .obstacles
            .iter()
            .any(|obstacle| matches!(obstacle, CompletionObstacle::ForeignFinding { .. })),
        "a finding from another Loop cannot support this Loop's completion"
    );
}

#[test]
fn a_blocking_required_finding_bars_completion_even_with_a_satisfied_criterion() {
    let outputs = vec![output_ref("resource://output/1", RESULT_BODY)];
    let digest = target_for(&outputs, 1).binding_digest();
    let findings = vec![
        finding(
            "criterion-001-gate",
            GateOutcome::Passed,
            GateEnforcement::Required,
            Some("criterion-001"),
            &digest,
            7,
        ),
        // A required gate that supports no particular criterion but is
        // nonetheless unresolved.
        finding(
            "final-integrity",
            GateOutcome::Failed,
            GateEnforcement::Required,
            None,
            &digest,
            7,
        ),
    ];

    let assessment = assess_completion(&checkpoint(7), &findings);
    assert!(!assessment.permitted);
    assert!(
        assessment
            .obstacles
            .iter()
            .any(|obstacle| matches!(obstacle, CompletionObstacle::BlockingFinding { .. })),
        "no blocking finding may remain unresolved at completion"
    );
}
