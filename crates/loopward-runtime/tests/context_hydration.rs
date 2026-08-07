//! Contract tests for deterministic context hydration
//! (`docs/04-context-lifecycle.md` §6, §7, §12).
//!
//! These tests exercise [`ContextHydrator`] against an in-memory
//! [`ResourceBroker`] fake that records every resolution it is asked for, so
//! a test can assert not only *that* hydration failed but that it failed
//! before any downstream work was attempted — a failed hydration must never
//! start a Work Unit Attempt.
//!
//! The stress fixture at `tests/fixtures/scenario/large-history.json` (repo
//! root) is a synthetic prior history of roughly 100,000 estimated tokens.
//! Hydrating it for a final verification Work Unit must produce a Boot
//! Package at or below 30,000 estimated tokens
//! (`docs/10-mvp-and-evaluation.md` §7).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::context::{
    ContextBudget, ContextCapabilities, ContextClass, ContextControlLevel, ContextPriority,
    EstimateMethod, Fidelity, Sensitivity, Validity, VerificationStatus,
};
use loopward_api::ids::{CheckpointId, EpisodeId, LoopId, WorkUnitId};
use loopward_api::resource::{
    ConstraintRef, OutputContract, ResourceRef, RunnerRef, VerificationPlanRef,
};
use loopward_api::runner::RunnerCapabilities;
use loopward_api::work_unit::{
    ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitStatus,
};
use loopward_core::Checkpoint;
use loopward_runtime::context::{
    CandidateDisposition, ContextCandidate, ContextHydrator, ExclusionReason, HydrationError,
    HydrationReport, HydrationRequest, REFERENCE_ONLY_TOKENS, SelectionBucket,
    available_boot_tokens,
};
use loopward_runtime::ports::{ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Recording in-memory resource broker
// ---------------------------------------------------------------------------

/// A [`ResourceBroker`] that serves deterministic synthetic bytes for any
/// URI, except those explicitly registered as missing, and records the URI of
/// every `resolve` call in order.
struct RecordingBroker {
    explicit: HashMap<String, Vec<u8>>,
    missing: HashSet<String>,
    resolved: Mutex<Vec<String>>,
}

impl RecordingBroker {
    fn new() -> Self {
        Self {
            explicit: HashMap::new(),
            missing: HashSet::new(),
            resolved: Mutex::new(Vec::new()),
        }
    }

    fn missing(uris: &[&str]) -> Self {
        let mut broker = Self::new();
        broker.missing = uris.iter().map(|uri| (*uri).to_owned()).collect();
        broker
    }

    /// The deterministic bytes this broker serves for `uri` when no explicit
    /// content was registered.
    fn synthetic(uri: &str) -> Vec<u8> {
        format!("loopward-test-content::{uri}").into_bytes()
    }

    fn content(&self, uri: &str) -> Vec<u8> {
        self.explicit
            .get(uri)
            .cloned()
            .unwrap_or_else(|| Self::synthetic(uri))
    }

    fn resolved_uris(&self) -> Vec<String> {
        self.resolved
            .lock()
            .expect("resolve log is not poisoned")
            .clone()
    }
}

#[async_trait]
impl ResourceBroker for RecordingBroker {
    async fn begin(&self, _hint: ResourceBeginHint) -> Result<ResourceHandle, ResourceError> {
        Err(ResourceError::UnknownHandle(
            "write not supported".to_owned(),
        ))
    }

    async fn write(&self, handle: &ResourceHandle, _bytes: &[u8]) -> Result<(), ResourceError> {
        Err(ResourceError::UnknownHandle(handle.as_str().to_owned()))
    }

    async fn finalize(&self, handle: ResourceHandle) -> Result<ResourceRef, ResourceError> {
        Err(ResourceError::UnknownHandle(handle.as_str().to_owned()))
    }

    async fn abort(&self, handle: ResourceHandle) -> Result<(), ResourceError> {
        Err(ResourceError::UnknownHandle(handle.as_str().to_owned()))
    }

    async fn resolve(&self, reference: &ResourceRef) -> Result<Vec<u8>, ResourceError> {
        self.resolved
            .lock()
            .expect("resolve log is not poisoned")
            .push(reference.uri.clone());
        if self.missing.contains(&reference.uri) {
            return Err(ResourceError::UnresolvableReference(reference.uri.clone()));
        }
        Ok(self.content(&reference.uri))
    }
}

fn digest_of(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(hex, "{byte:02x}");
    }
    format!("sha256:{hex}")
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn resource(uri: &str, digest: Option<&str>) -> ResourceRef {
    ResourceRef {
        uri: uri.to_owned(),
        media_type: Some("text/plain".to_owned()),
        digest: digest.map(str::to_owned),
        version: None,
        size: None,
        summary: None,
    }
}

fn checkpoint() -> Checkpoint {
    Checkpoint {
        id: CheckpointId::from_uuid(Uuid::from_u128(0x01)),
        loop_id: LoopId::from_uuid(Uuid::from_u128(0x02)),
        version: 7,
        aggregate_revision: 21,
        parent: None,
        episode_id: None,
        integrity_digest: Some("sha256:checkpoint".to_owned()),
    }
}

fn work_unit(constraints: &[&str], inputs: &[&str]) -> WorkUnit {
    WorkUnit {
        id: WorkUnitId::from_uuid(Uuid::from_u128(0x11)),
        kind: WorkUnitKind::new("domain.final-verification"),
        objective: "Verify the target revision.".to_owned(),
        status: WorkUnitStatus::Active,
        inputs: inputs.iter().map(|uri| resource(uri, None)).collect(),
        constraints: constraints
            .iter()
            .map(|reference| ConstraintRef {
                reference: (*reference).to_owned(),
            })
            .collect(),
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_owned(),
        },
        verification_plan: VerificationPlanRef {
            reference: "verification-plan://final".to_owned(),
            version: 3,
        },
        runner: RunnerRef {
            name: "command".to_owned(),
            profile: None,
        },
        budget: ResourceBudget::default(),
        retry_policy: RetryPolicy::default(),
    }
}

fn capabilities(supports_resource_fetch: bool) -> RunnerCapabilities {
    RunnerCapabilities {
        protocol_versions: vec!["loopward.work-unit/v0.1".to_owned()],
        resumable: false,
        supports_reconcile: false,
        supports_events: false,
        supports_suspend: false,
        supports_cancel: false,
        reports_token_usage: false,
        reports_internal_trace_context: false,
        context: ContextCapabilities {
            control_level: ContextControlLevel::ManagedBoot,
            reports_usage: false,
            reports_compaction: false,
            supports_resource_fetch,
            supports_structured_handoff: false,
            supports_resume: false,
        },
    }
}

fn budget(hard: u64, output: u64, tool: u64, margin: u64) -> ContextBudget {
    ContextBudget {
        model_window_tokens: Some(200_000),
        soft_input_limit_tokens: hard / 2,
        hard_input_limit_tokens: hard,
        reserved_output_tokens: output,
        reserved_tool_tokens: tool,
        safety_margin_tokens: margin,
    }
}

fn candidate(
    id: &str,
    uri: &str,
    classification: ContextClass,
    priority: ContextPriority,
    tokens: Option<u64>,
) -> ContextCandidate {
    let mut item = ContextCandidate::new(
        id,
        "test-item",
        resource(uri, None),
        classification,
        priority,
    );
    item.estimated_tokens = tokens;
    item.estimate_method = tokens.map(|_| EstimateMethod::Tokenizer);
    item.estimate_confidence = tokens.map(|_| 1.0);
    item.selection_reason = format!("selected for {id}");
    item
}

fn request(
    work_unit: WorkUnit,
    budget: ContextBudget,
    candidates: Vec<ContextCandidate>,
    supports_resource_fetch: bool,
) -> HydrationRequest {
    HydrationRequest {
        checkpoint: checkpoint(),
        episode_id: EpisodeId::from_uuid(Uuid::from_u128(0x03)),
        session_generation: 3,
        work_unit,
        capabilities: capabilities(supports_resource_fetch),
        budget,
        context_rule_version: "deterministic-v0.1".to_owned(),
        candidates,
    }
}

async fn hydrate(
    broker: Arc<RecordingBroker>,
    request: HydrationRequest,
) -> Result<HydrationReport, HydrationError> {
    ContextHydrator::new(broker).hydrate(request).await
}

// ---------------------------------------------------------------------------
// Budget arithmetic
// ---------------------------------------------------------------------------

#[test]
fn available_boot_tokens_subtracts_every_reservation() {
    let available = available_boot_tokens(&budget(48_000, 8_000, 6_000, 4_000))
        .expect("reservations fit inside the hard limit");
    assert_eq!(available, 30_000);
}

#[test]
fn available_boot_tokens_rejects_a_budget_that_underflows() {
    let error = available_boot_tokens(&budget(10_000, 8_000, 6_000, 4_000))
        .expect_err("reservations exceed the hard limit");
    assert!(matches!(error, HydrationError::InvalidBudget));
}

// ---------------------------------------------------------------------------
// Pinned integrity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn required_pinned_items_are_present_verbatim() {
    let broker = Arc::new(RecordingBroker::new());
    let candidates = vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(400),
        ),
        candidate(
            "sup-001",
            "resource://history/001",
            ContextClass::Evidence,
            ContextPriority::Supplemental,
            Some(400),
        ),
    ];
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            candidates,
            true,
        ),
    )
    .await
    .expect("hydration succeeds");

    let pinned = report
        .manifest
        .items
        .iter()
        .find(|item| item.id == "pin-001")
        .expect("required pinned item is present in the manifest");
    assert_eq!(pinned.fidelity, Fidelity::Verbatim);
    assert_eq!(pinned.classification, ContextClass::Pinned);
    assert_eq!(
        pinned.digest,
        digest_of(&RecordingBroker::synthetic("constraint://policy-001")),
        "the manifest binds the pinned item to the exact resolved bytes"
    );
    assert_eq!(
        report.manifest.items[0].id, "pin-001",
        "pinned items sort first"
    );
    assert_ne!(
        report.manifest.pinned_digest,
        report.manifest.manifest_digest
    );
    assert!(report.manifest.pinned_digest.starts_with("sha256:"));
}

#[tokio::test]
async fn missing_pinned_resource_fails_before_any_optional_resolution() {
    let broker = Arc::new(RecordingBroker::missing(&["constraint://policy-001"]));
    let candidates = vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(400),
        ),
        // No declared estimate: this candidate *would* be resolved if
        // hydration ever reached the selection phase.
        candidate(
            "sup-001",
            "resource://history/001",
            ContextClass::Evidence,
            ContextPriority::Supplemental,
            None,
        ),
    ];
    let error = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            candidates,
            true,
        ),
    )
    .await
    .expect_err("a missing required pinned resource fails hydration");

    assert!(matches!(
        error,
        HydrationError::MissingRequiredResource { ref id, .. } if id == "pin-001"
    ));
    assert!(
        !broker
            .resolved_uris()
            .iter()
            .any(|uri| uri == "resource://history/001"),
        "hydration aborted during pinned validation, before selecting anything"
    );
}

#[tokio::test]
async fn immutable_digest_mismatch_fails_hydration() {
    let broker = Arc::new(RecordingBroker::new());
    let mut pinned = candidate(
        "pin-001",
        "constraint://policy-001",
        ContextClass::Pinned,
        ContextPriority::Required,
        Some(400),
    );
    pinned.source.digest = Some("sha256:0000000000000000".to_owned());
    let error = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            vec![pinned],
            true,
        ),
    )
    .await
    .expect_err("a required item whose immutable digest does not match fails hydration");

    assert!(matches!(
        error,
        HydrationError::DigestMismatch { ref id, .. } if id == "pin-001"
    ));
}

#[tokio::test]
async fn a_work_unit_constraint_without_a_pinned_candidate_fails() {
    let broker = Arc::new(RecordingBroker::new());
    let error = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001", "constraint://user-002"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            vec![candidate(
                "pin-001",
                "constraint://policy-001",
                ContextClass::Pinned,
                ContextPriority::Required,
                Some(400),
            )],
            true,
        ),
    )
    .await
    .expect_err("an unrepresented Work Unit constraint fails hydration");

    assert!(matches!(
        error,
        HydrationError::MissingPinnedConstraint { ref reference } if reference == "constraint://user-002"
    ));
}

#[tokio::test]
async fn required_pinned_content_may_never_be_summarized() {
    let broker = Arc::new(RecordingBroker::new());
    let mut pinned = candidate(
        "pin-001",
        "constraint://policy-001",
        ContextClass::Pinned,
        ContextPriority::Required,
        Some(400),
    );
    pinned.fidelity = Fidelity::Summary;
    let error = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            vec![pinned],
            true,
        ),
    )
    .await
    .expect_err("a lossy summary can never replace a pinned constraint");

    assert!(matches!(
        error,
        HydrationError::LossyPinnedFidelity { ref id, .. } if id == "pin-001"
    ));
}

// ---------------------------------------------------------------------------
// Budget fitting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn required_content_exceeding_the_hard_limit_fails() {
    let broker = Arc::new(RecordingBroker::new());
    let candidates = vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(3_000),
        ),
        candidate(
            "cw-001",
            "resource://work/input-001",
            ContextClass::CurrentWork,
            ContextPriority::Required,
            Some(3_000),
        ),
    ];
    let error = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(5_000, 2_000, 1_000, 1_000),
            candidates,
            true,
        ),
    )
    .await
    .expect_err("required content larger than the boot budget fails hydration");

    assert!(matches!(
        error,
        HydrationError::RequiredContentExceedsBudget {
            required_tokens: 6_000,
            available_tokens: 1_000,
        }
    ));
}

#[tokio::test]
async fn supplemental_items_are_omitted_before_important_items() {
    let broker = Arc::new(RecordingBroker::new());
    let candidates = vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(200),
        ),
        candidate(
            "sup-001",
            "resource://history/001",
            ContextClass::Evidence,
            ContextPriority::Supplemental,
            Some(700),
        ),
        candidate(
            "imp-001",
            "resource://work/input-001",
            ContextClass::CurrentWork,
            ContextPriority::Important,
            Some(700),
        ),
    ];
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(4_000, 1_500, 1_000, 500),
            candidates,
            true,
        ),
    )
    .await
    .expect("hydration succeeds within budget");

    let ids: Vec<&str> = report
        .manifest
        .items
        .iter()
        .map(|item| item.id.as_str())
        .collect();
    assert_eq!(ids, vec!["pin-001", "imp-001"]);
    assert_eq!(
        report.exclusion_reason("sup-001"),
        Some(ExclusionReason::BudgetExhausted),
        "the supplemental item is dropped so the important item fits"
    );
    assert_eq!(
        report.bucket("imp-001"),
        Some(SelectionBucket::ImportantCurrentInput)
    );
}

/// `Important` material whose class has no dedicated bucket (an unverified
/// observation) shares [`SelectionBucket::Supplemental`] with genuinely
/// supplemental material. Declared priority must still decide inside that
/// bucket, even when the supplemental item sorts earlier by identifier.
#[tokio::test]
async fn important_material_outranks_supplemental_inside_a_shared_bucket() {
    let broker = Arc::new(RecordingBroker::new());
    let mut unverified = candidate(
        "zzz-important-unverified",
        "resource://evidence/unverified-001",
        ContextClass::Evidence,
        ContextPriority::Important,
        Some(700),
    );
    unverified.verification_status = Some(VerificationStatus::Unverified);
    let candidates = vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(200),
        ),
        candidate(
            "aaa-supplemental",
            "resource://history/001",
            ContextClass::Evidence,
            ContextPriority::Supplemental,
            Some(700),
        ),
        unverified,
    ];
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(4_000, 1_500, 1_000, 500),
            candidates,
            true,
        ),
    )
    .await
    .expect("hydration succeeds within budget");

    assert_eq!(
        report.bucket("aaa-supplemental"),
        Some(SelectionBucket::Supplemental)
    );
    assert_eq!(
        report.bucket("zzz-important-unverified"),
        Some(SelectionBucket::Supplemental),
        "the shared-bucket path is the one under test"
    );
    assert_eq!(
        report.disposition("zzz-important-unverified"),
        Some(&CandidateDisposition::Included),
        "important material is offered budget before supplemental material"
    );
    assert_eq!(
        report.exclusion_reason("aaa-supplemental"),
        Some(ExclusionReason::BudgetExhausted)
    );
}

#[tokio::test]
async fn on_demand_references_cost_a_bounded_reference_fee_when_the_runner_can_fetch() {
    let broker = Arc::new(RecordingBroker::new());
    let candidates = vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(200),
        ),
        candidate(
            "od-001",
            "resource://archive/episode-001",
            ContextClass::Reference,
            ContextPriority::OnDemand,
            Some(9_000),
        ),
    ];
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(4_000, 1_500, 1_000, 500),
            candidates.clone(),
            true,
        ),
    )
    .await
    .expect("hydration succeeds");
    let reference = report
        .manifest
        .items
        .iter()
        .find(|item| item.id == "od-001")
        .expect("an on-demand reference fits when the runner can fetch it");
    assert_eq!(reference.fidelity, Fidelity::ReferenceOnly);
    assert_eq!(reference.estimated_tokens, Some(REFERENCE_ONLY_TOKENS));

    // The same candidate must be fully materialized (and therefore dropped,
    // at 9,000 tokens) for a runner that cannot fetch on demand.
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(4_000, 1_500, 1_000, 500),
            candidates,
            false,
        ),
    )
    .await
    .expect("hydration succeeds");
    assert_eq!(
        report.exclusion_reason("od-001"),
        Some(ExclusionReason::BudgetExhausted)
    );
}

#[tokio::test]
async fn work_unit_inputs_without_candidates_are_hydrated_as_required_current_work() {
    let broker = Arc::new(RecordingBroker::new());
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &["resource://work/input-001"]),
            budget(48_000, 8_000, 6_000, 4_000),
            vec![candidate(
                "pin-001",
                "constraint://policy-001",
                ContextClass::Pinned,
                ContextPriority::Required,
                Some(400),
            )],
            true,
        ),
    )
    .await
    .expect("hydration succeeds");

    let input = report
        .manifest
        .items
        .iter()
        .find(|item| item.source.uri == "resource://work/input-001")
        .expect("an undeclared Work Unit input is hydrated as required current work");
    assert_eq!(input.priority, ContextPriority::Required);
    assert_eq!(input.classification, ContextClass::CurrentWork);
    assert_eq!(input.estimate_method, Some(EstimateMethod::Heuristic));
}

/// A projection that *downgrades* a declared Work Unit input defeats the
/// synthesis guard just as thoroughly as one that omits it: the input must
/// never be droppable for budget reasons.
#[tokio::test]
async fn a_work_unit_input_covered_by_a_lower_priority_candidate_is_promoted() {
    let broker = Arc::new(RecordingBroker::new());
    let candidates = vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(200),
        ),
        // Sorts before the input candidate, and would consume the budget
        // first if the input were left at supplemental priority.
        candidate(
            "aaa-filler",
            "resource://history/001",
            ContextClass::Evidence,
            ContextPriority::Supplemental,
            Some(700),
        ),
        candidate(
            "zzz-declared-input",
            "resource://work/input-001",
            ContextClass::CurrentWork,
            ContextPriority::Supplemental,
            Some(700),
        ),
    ];
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &["resource://work/input-001"]),
            budget(4_000, 1_500, 1_000, 500),
            candidates,
            true,
        ),
    )
    .await
    .expect("hydration succeeds within budget");

    let input = report
        .manifest
        .items
        .iter()
        .find(|item| item.source.uri == "resource://work/input-001")
        .expect("a declared Work Unit input survives even when projected as supplemental");
    assert_eq!(
        input.id, "zzz-declared-input",
        "the covering candidate is promoted, not duplicated"
    );
    assert_eq!(input.priority, ContextPriority::Required);
    assert_eq!(
        report.bucket("zzz-declared-input"),
        Some(SelectionBucket::RequiredCurrentWork)
    );
    assert!(
        input.selection_reason.contains("promoted to required"),
        "the promotion is recorded in the item's selection reason, saw {:?}",
        input.selection_reason
    );
    assert_eq!(
        report.exclusion_reason("aaa-filler"),
        Some(ExclusionReason::BudgetExhausted),
        "the required input takes budget ahead of supplemental material"
    );
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

fn determinism_candidates() -> Vec<ContextCandidate> {
    vec![
        candidate(
            "pin-001",
            "constraint://policy-001",
            ContextClass::Pinned,
            ContextPriority::Required,
            Some(400),
        ),
        candidate(
            "dec-001",
            "resource://decision/001",
            ContextClass::Decision,
            ContextPriority::Important,
            Some(500),
        ),
        candidate(
            "ev-001",
            "resource://evidence/001",
            ContextClass::Evidence,
            ContextPriority::Important,
            Some(600),
        ),
        candidate(
            "sup-001",
            "resource://history/001",
            ContextClass::Evidence,
            ContextPriority::Supplemental,
            Some(700),
        ),
    ]
}

#[tokio::test]
async fn the_same_input_and_rule_version_produce_the_same_manifest_digest() {
    let broker = Arc::new(RecordingBroker::new());
    let build = |candidates| {
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            candidates,
            true,
        )
    };

    let first = hydrate(Arc::clone(&broker), build(determinism_candidates()))
        .await
        .expect("hydration succeeds");
    let second = hydrate(Arc::clone(&broker), build(determinism_candidates()))
        .await
        .expect("hydration succeeds");
    assert_eq!(first.manifest, second.manifest);
    assert_eq!(
        first.manifest.manifest_digest,
        second.manifest.manifest_digest
    );

    // Candidate order is an input artifact, not a selection input.
    let mut shuffled = determinism_candidates();
    shuffled.reverse();
    let reordered = hydrate(Arc::clone(&broker), build(shuffled))
        .await
        .expect("hydration succeeds");
    assert_eq!(
        reordered.manifest.manifest_digest, first.manifest.manifest_digest,
        "candidate input order must not change the manifest"
    );

    // A different rule version is a different manifest.
    let mut other_rules = build(determinism_candidates());
    other_rules.context_rule_version = "deterministic-v0.2".to_owned();
    let other = hydrate(Arc::clone(&broker), other_rules)
        .await
        .expect("hydration succeeds");
    assert_ne!(
        other.manifest.manifest_digest,
        first.manifest.manifest_digest
    );
}

#[tokio::test]
async fn selection_follows_the_documented_bucket_order() {
    let broker = Arc::new(RecordingBroker::new());
    let mut candidates = determinism_candidates();
    candidates.push(candidate(
        "od-001",
        "resource://archive/001",
        ContextClass::Reference,
        ContextPriority::OnDemand,
        Some(100),
    ));
    candidates.push(candidate(
        "ro-001",
        "resource://outcome/001",
        ContextClass::RecentOutcome,
        ContextPriority::Important,
        Some(100),
    ));
    candidates.push(candidate(
        "cw-001",
        "resource://work/001",
        ContextClass::CurrentWork,
        ContextPriority::Required,
        Some(100),
    ));
    for item in &mut candidates {
        if item.id == "ev-001" {
            item.verification_status = Some(VerificationStatus::Verified);
        }
    }

    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["constraint://policy-001"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            candidates,
            true,
        ),
    )
    .await
    .expect("hydration succeeds");

    let ids: Vec<&str> = report
        .manifest
        .items
        .iter()
        .map(|item| item.id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec![
            "pin-001", "cw-001", "dec-001", "ev-001", "ro-001", "sup-001", "od-001"
        ]
    );
}

// ---------------------------------------------------------------------------
// 100,000-token stress fixture
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Fixture {
    context_rule_version: String,
    budget: ContextBudget,
    work_unit: FixtureWorkUnit,
    candidates: Vec<FixtureCandidate>,
}

#[derive(Deserialize)]
struct FixtureWorkUnit {
    constraints: Vec<String>,
    inputs: Vec<String>,
}

#[derive(Deserialize)]
struct FixtureCandidate {
    id: String,
    kind: String,
    uri: String,
    digest: Option<String>,
    classification: ContextClass,
    priority: ContextPriority,
    fidelity: Fidelity,
    sensitivity: Sensitivity,
    validity: Validity,
    verification_status: Option<VerificationStatus>,
    estimated_tokens: Option<u64>,
    estimate_method: Option<EstimateMethod>,
    estimate_confidence: Option<f32>,
    selection_reason: String,
}

impl From<FixtureCandidate> for ContextCandidate {
    fn from(value: FixtureCandidate) -> Self {
        Self {
            id: value.id,
            kind: value.kind,
            source: resource(&value.uri, value.digest.as_deref()),
            classification: value.classification,
            priority: value.priority,
            fidelity: value.fidelity,
            sensitivity: value.sensitivity,
            validity: value.validity,
            verification_status: value.verification_status,
            estimated_tokens: value.estimated_tokens,
            estimate_method: value.estimate_method,
            estimate_confidence: value.estimate_confidence,
            selection_reason: value.selection_reason,
        }
    }
}

fn load_fixture() -> Fixture {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/scenario/large-history.json"
    );
    let text = std::fs::read_to_string(path).expect("the stress fixture is readable");
    serde_json::from_str(&text).expect("the stress fixture parses")
}

#[tokio::test]
async fn the_hundred_thousand_token_history_yields_a_bounded_verification_manifest() {
    let fixture = load_fixture();
    let declared_history: u64 = fixture
        .candidates
        .iter()
        .filter_map(|candidate| candidate.estimated_tokens)
        .sum();
    assert!(
        declared_history >= 100_000,
        "the stress fixture must represent a history larger than 100,000 tokens, saw {declared_history}"
    );

    let available =
        available_boot_tokens(&fixture.budget).expect("the fixture budget is well formed");
    assert_eq!(available, 30_000);

    let constraints: Vec<&str> = fixture
        .work_unit
        .constraints
        .iter()
        .map(String::as_str)
        .collect();
    let inputs: Vec<&str> = fixture
        .work_unit
        .inputs
        .iter()
        .map(String::as_str)
        .collect();
    let candidates: Vec<ContextCandidate> = fixture
        .candidates
        .into_iter()
        .map(ContextCandidate::from)
        .collect();

    let broker = Arc::new(RecordingBroker::new());
    let mut hydration = request(
        work_unit(&constraints, &inputs),
        fixture.budget,
        candidates,
        true,
    );
    hydration.context_rule_version = fixture.context_rule_version;

    let report = hydrate(Arc::clone(&broker), hydration)
        .await
        .expect("the final verification Work Unit hydrates successfully");

    let selected = report.selected_tokens();
    assert!(
        selected <= 30_000,
        "a final verification Boot Package must stay at or below 30,000 estimated tokens, saw {selected}"
    );
    assert!(
        selected <= available,
        "the Boot Package must fit the available boot budget"
    );
    assert_eq!(
        selected, 29_792,
        "the fixture's selected size is a deterministic regression anchor"
    );

    assert_final_verification_selection(&report);
}

/// The selection properties the `docs/10-mvp-and-evaluation.md` §7 stress
/// scenario requires of a final verification Boot Package.
fn assert_final_verification_selection(report: &HydrationReport) {
    // Everything required, and every applicable decision, verified
    // observation, important input, and recent accepted outcome survives.
    for id in [
        "pin-001-system-policy",
        "pin-002-user-constraint",
        "pin-003-acceptance-criteria",
        "pin-004-safety-requirement",
        "cw-001-target-revision",
        "cw-002-verification-plan",
        "cs-001-accepted-state",
        "dec-001-active",
        "dec-006-active",
        "ev-001-verified",
        "ev-008-verified",
        "ci-004-input",
        "ro-005-accepted",
    ] {
        assert_eq!(
            report.disposition(id),
            Some(&CandidateDisposition::Included),
            "{id} must survive hydration"
        );
    }

    // Archived history stays reachable as bounded references, not payload.
    for index in 1..=12 {
        let id = format!("od-{index:03}-archive");
        let item = report
            .manifest
            .items
            .iter()
            .find(|item| item.id == id)
            .unwrap_or_else(|| panic!("{id} is referenced in the manifest"));
        assert_eq!(item.fidelity, Fidelity::ReferenceOnly);
        assert_eq!(item.estimated_tokens, Some(REFERENCE_ONLY_TOKENS));
    }

    // Bulk history is dropped, and superseded decisions never compete at all.
    for id in ["sup-001-history", "sup-020-history"] {
        assert_eq!(
            report.exclusion_reason(id),
            Some(ExclusionReason::BudgetExhausted),
            "{id} is genuinely supplemental and must yield the remaining budget"
        );
    }
    assert_eq!(
        report.exclusion_reason("dsup-001-superseded"),
        Some(ExclusionReason::Superseded)
    );

    // Important-priority material demoted into the supplemental bucket
    // (unverified observations) still outranks genuinely supplemental
    // material sharing that bucket.
    for id in ["uev-001-unverified", "uev-002-unverified"] {
        assert_eq!(
            report.disposition(id),
            Some(&CandidateDisposition::Included),
            "{id} is important-priority and outranks every sup-* candidate"
        );
        assert_eq!(report.bucket(id), Some(SelectionBucket::Supplemental));
    }

    // Every candidate considered is accounted for, with an estimation method.
    assert_eq!(report.considered.len(), 72);
    assert!(
        report
            .considered
            .iter()
            .all(|outcome| outcome.estimate_method.is_some()),
        "every considered candidate records how its tokens were estimated"
    );
}
