//! Phase One's end-to-end conformance scenario (`docs/10-mvp-and-evaluation.md`
//! §6): `Inspect → Implement → Verify rejected → Remediate → Final Verify
//! accepted → Complete`, driven entirely through the public
//! [`LoopService`] surface (`RuntimeLoopService`, over a real
//! `SqliteLoopStore`/`FileResourceBroker`) with `loopward-testkit`'s
//! deterministic fakes standing in for wall-clock time, identity, and the
//! external runner.
//!
//! # Five Episodes, one Work Unit
//!
//! `loopward_runtime::controller::advance`'s own module doc is explicit:
//! "Phase One's controller carries exactly one Work Unit per Loop; once it
//! is terminal, nothing remains to attempt" — no `LifecycleCommand` ever
//! assigns or replaces a Loop's Work Unit
//! (`ControllerError::NoWorkUnit`'s doc comment says the same). So the
//! reference scenario's five named phases
//! (`tests/fixtures/scenario/retry-loop.json`) are not five distinct
//! `WorkUnitId`s chained by the runtime — that would be exactly the
//! orchestration/workflow-topology concept the Global Constraints forbid
//! Loopward Core from containing. They are five real Episodes against *one*
//! Work Unit's lifecycle:
//!
//! ```text
//! Episode 1 (Execution)   Inspect       — the runner fails outright;
//!                                         the Loop suspends and resumes,
//!                                         reconciling the interrupted
//!                                         Attempt as never started.
//! Episode 2 (Execution)   Implement     — a fresh Attempt succeeds.
//! Episode 3 (Verification) Verify       — the deterministic gate REJECTS.
//! Episode 4 (Execution)   Remediate     — a third Attempt lands the fix
//!                                         (same Work Unit: rejection sends
//!                                         it back to `Pending`, not to a
//!                                         new identity).
//! Episode 5 (Verification) Final Verify — the deterministic gate ACCEPTS.
//! ```
//!
//! `Complete` then terminates the Loop — a sixth durable transition, but not
//! a sixth Episode.
//!
//! Because a `create()` Loop never gets a Work Unit either (the very same
//! documented gap), this test seeds the Work Unit directly through
//! `loopward_testkit::FailureInjector::genesis` rather than `LoopService::create`
//! — the same seam `crates/loopward-cli/src/commands.rs`'s `NullRunner` doc
//! comment and `RuntimeLoopService::create`'s own "Known limitation" doc
//! comment both already name. Every other step — `advance`, `resume`,
//! `status`, `events`, `result` — goes through the real `LoopService` trait.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;

use loopward_api::context::ContextClass;
use loopward_api::event::{EventDurabilityFilter, EventQuery};
use loopward_api::ids::{LoopId, WorkUnitId};
use loopward_api::loop_service::{
    AdvanceLoopCommand, CommandMetadata, LoopOutcomeKind, LoopService, LoopStatus as ApiLoopStatus,
    ResumeLoopCommand,
};
use loopward_api::resource::{ConstraintRef, OutputContract, RunnerRef, VerificationPlanRef};
use loopward_api::runner::{ReconcileOutcome, WorkUnitRunner};
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{
    ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitStatus,
};
use loopward_core::LoopEvent;
use loopward_observability::spans::SpanLinkKind;
use loopward_runtime::ports::{LoopStore, ResourceBroker};
use loopward_runtime::verification::{
    AggregationPolicy, DeterministicGateConfig, GateConfig, GateEnforcement, GateId,
    RemediationPolicy, VerificationGateKind, VerificationGateSpec, VerificationPlanId,
};
use loopward_runtime::{
    LoopController, RecoveryManager, RecoveryOutcome, RuntimeLoopService, RuntimePorts,
    VerificationPlanResolver, VerificationPlanTemplate,
};
use loopward_store_sqlite::{FileResourceBroker, SqliteLoopStore, SqliteStoreConfig};
use loopward_testkit::failure::CrashPoint;
use loopward_testkit::{
    AttemptScript, FailureInjector, FakeRunner, FixedClock, SequentialIdGenerator, TraceRecorder,
};

// ---------------------------------------------------------------------------
// The reference fixture (`tests/fixtures/scenario/retry-loop.json`)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ScenarioFixture {
    #[serde(rename = "loop")]
    loop_spec: LoopSpec,
    pinned_constraints: Vec<PinnedConstraint>,
    work_units: Vec<WorkUnitPhase>,
}

#[derive(Debug, Deserialize)]
struct LoopSpec {
    goal: String,
}

#[derive(Debug, Deserialize)]
struct PinnedConstraint {
    id: String,
    statement: String,
}

#[derive(Debug, Deserialize)]
struct WorkUnitPhase {
    ordinal: u32,
    phase: String,
    #[serde(default)]
    expected_verification_outcome: Option<String>,
}

fn load_fixture() -> ScenarioFixture {
    let raw = include_str!("../../../tests/fixtures/scenario/retry-loop.json");
    serde_json::from_str(raw).expect("retry-loop.json is valid ScenarioFixture json")
}

fn load_expected_event_kinds() -> Vec<String> {
    let raw = include_str!("../../../tests/fixtures/scenario/expected-events.json");
    serde_json::from_str(raw).expect("expected-events.json is a valid array of event kind strings")
}

// ---------------------------------------------------------------------------
// Verification plan: one required Deterministic gate checking for a marker
// file the "Remediate" Attempt creates.
// ---------------------------------------------------------------------------

struct MarkerVerificationPlan {
    marker: std::path::PathBuf,
}

impl VerificationPlanResolver for MarkerVerificationPlan {
    fn resolve(&self, reference: &VerificationPlanRef) -> Option<VerificationPlanTemplate> {
        Some(VerificationPlanTemplate {
            id: VerificationPlanId::new(reference.reference.clone()),
            version: u64::from(reference.version),
            gates: vec![VerificationGateSpec {
                id: GateId::new("deterministic-check"),
                kind: VerificationGateKind::Deterministic,
                enforcement: GateEnforcement::Required,
                criterion: None,
                config: GateConfig::Deterministic(DeterministicGateConfig {
                    program: "test".to_owned(),
                    args: vec!["-f".to_owned(), self.marker.display().to_string()],
                    working_directory: None,
                    env: BTreeMap::new(),
                    expected_exit_code: 0,
                    required_stdout_substring: None,
                    forbidden_stdout_substring: None,
                    timeout: Some(std::time::Duration::from_secs(30)),
                    max_captured_output_bytes: 4 * 1024,
                }),
            }],
            aggregation: AggregationPolicy::default(),
            remediation_policy: RemediationPolicy::default(),
        })
    }
}

// ---------------------------------------------------------------------------
// Composition root: the same shape as `loopward-cli/src/commands.rs`'s
// `build_service`, but with `loopward-testkit`'s deterministic fakes for
// the clock, identity, and runner, and a `TraceRecorder` in place of
// `NoopTelemetrySink`/an OTLP sink — proving the runtime operates fully
// with tracing captured in-process and without any telemetry backend
// (`docs/10` §13).
// ---------------------------------------------------------------------------

fn command_metadata(expected_revision: Option<u64>) -> CommandMetadata {
    CommandMetadata {
        request_id: uuid::Uuid::now_v7().to_string(),
        idempotency_key: uuid::Uuid::now_v7().to_string(),
        expected_revision,
        principal: None,
        trace_context: TraceContext::default(),
    }
}

async fn expected_revision(service: &RuntimeLoopService, loop_id: LoopId) -> u64 {
    service
        .status(loop_id)
        .await
        .expect("status succeeds")
        .aggregate_revision
}

/// Finalizes `statement` as real, resolvable resource content and returns
/// its URI.
///
/// The default context candidate provider projects
/// [`WorkUnit::constraints`] into *required* pinned candidates
/// (`loopward_runtime::controller::advance::DefaultContextCandidates`), and
/// a required candidate must actually resolve through the `ResourceBroker`
/// — an opaque, never-finalized `constraint://...` URI is not a real
/// pinned constraint, it is an unresolvable reference, and hydration
/// correctly refuses to fabricate content for it.
async fn finalize_constraint_resource(
    ports: &RuntimePorts,
    statement: &str,
) -> loopward_api::resource::ResourceRef {
    let handle = ports
        .resources
        .begin(loopward_runtime::ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    ports
        .resources
        .write(&handle, statement.as_bytes())
        .await
        .expect("write succeeds");
    ports
        .resources
        .finalize(handle)
        .await
        .expect("finalize succeeds")
}

fn work_unit(id: WorkUnitId, constraint_uri: &str) -> WorkUnit {
    WorkUnit {
        constraints: vec![ConstraintRef {
            reference: constraint_uri.to_owned(),
        }],
        ..bare_work_unit(id)
    }
}

/// As [`work_unit`], but with no declared constraints — for fixtures (like
/// the forced-interruption demonstration below) that exist only to prove
/// `RecoveryManager` reconciles a crash correctly, not to exercise pinned
/// constraint hydration: `ContextHydrator::hydrate` requires every declared
/// constraint resolve to a real candidate
/// (`loopward_runtime::HydrationError::MissingPinnedConstraint`), which
/// `loopward_testkit::FailureInjector`'s own hydration step deliberately
/// offers no candidates for (it is not a `ContextCandidateProvider`).
fn bare_work_unit(id: WorkUnitId) -> WorkUnit {
    WorkUnit {
        id,
        kind: WorkUnitKind::new("loopward.reference-scenario/rust-maintenance"),
        objective: "stabilize the flaky `retry_backoff` unit test".to_owned(),
        status: WorkUnitStatus::Pending,
        inputs: Vec::new(),
        constraints: Vec::new(),
        output_contract: OutputContract {
            schema: "loopward.reference-scenario-output/v0.1".to_owned(),
        },
        verification_plan: VerificationPlanRef {
            reference: "loopward.reference-scenario/deterministic-check".to_owned(),
            version: 1,
        },
        runner: RunnerRef {
            name: "loopward-testkit/fake".to_owned(),
            profile: None,
        },
        budget: ResourceBudget::default(),
        retry_policy: RetryPolicy::default(),
    }
}

/// The full reference scenario, end to end, over the real durable stack.
///
/// Deliberately one long, linear sequence rather than several smaller test
/// functions: every step depends on the durable state the previous one left
/// behind (the whole point is proving the stack survives *across* a
/// suspend/resume, a rejection, and a remediation in one continuous run),
/// so splitting it apart would only hide that continuity, not clarify it.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn phase_one_reference_scenario_completes_after_one_rejection_and_remediation() {
    let fixture = load_fixture();
    assert!(
        !fixture.loop_spec.goal.trim().is_empty(),
        "the fixture must declare a Loop goal"
    );
    assert_eq!(
        fixture.work_units.len(),
        5,
        "the fixture declares five phases"
    );
    assert_eq!(
        fixture
            .work_units
            .iter()
            .map(|phase| phase.ordinal)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5],
        "the fixture's phases must be ordered"
    );
    assert_eq!(
        fixture
            .work_units
            .iter()
            .map(|phase| phase.phase.as_str())
            .collect::<Vec<_>>(),
        vec![
            "inspect",
            "implement",
            "verify",
            "remediate",
            "final_verify"
        ],
        "the fixture must name Inspect -> Implement -> Verify -> Remediate -> Final Verify"
    );
    let verify_phase = &fixture.work_units[2];
    let final_verify_phase = &fixture.work_units[4];
    assert_eq!(
        verify_phase.expected_verification_outcome.as_deref(),
        Some("rejected")
    );
    assert_eq!(
        final_verify_phase.expected_verification_outcome.as_deref(),
        Some("accepted")
    );
    let constraint = &fixture.pinned_constraints[0];
    assert!(!constraint.id.trim().is_empty());

    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("remediation-landed.marker");
    assert!(!marker.exists(), "the marker must not pre-exist");

    let store = SqliteLoopStore::open(SqliteStoreConfig::new(temp.path().join("loopward.sqlite3")))
        .await
        .expect("store opens");
    let pool = store.pool().clone();
    let broker: Arc<dyn ResourceBroker> = Arc::new(
        FileResourceBroker::new(pool, temp.path().join("resources")).expect("broker opens"),
    );
    let store: Arc<dyn LoopStore> = Arc::new(store);
    let trace_recorder = TraceRecorder::new();
    let ports = RuntimePorts {
        store,
        resources: broker,
        clock: Arc::new(FixedClock::default()),
        ids: Arc::new(SequentialIdGenerator::new()),
        telemetry: trace_recorder.sink(),
    };

    let runner = Arc::new(FakeRunner::new());
    // Attempt 1 ("Inspect"): the runner fails outright. `reconcile` truthfully
    // answers `NotStarted` so `resume` (which reconciles before returning,
    // see `RuntimeLoopService::resume`'s own doc comment) retires it cleanly
    // rather than leaving the Loop stuck `RecoveryRequired`.
    runner.script_ordinal(
        1,
        AttemptScript {
            fail_start: true,
            reconcile: Some(ReconcileOutcome::NotStarted),
            ..AttemptScript::default()
        },
    );
    // Attempt 3 ("Remediate") is the one that lands the fix: creating the
    // marker file the pinned Deterministic gate checks for.
    let marker_for_hook = marker.clone();
    runner.script_ordinal(
        3,
        AttemptScript::default().with_on_collect(move || {
            std::fs::write(&marker_for_hook, b"remediated").expect("marker writes");
        }),
    );

    let plans: Arc<dyn VerificationPlanResolver> = Arc::new(MarkerVerificationPlan {
        marker: marker.clone(),
    });
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        plans,
    );
    let holder_id = controller.holder_id();
    let service = RuntimeLoopService::new(controller);

    // Seed the Loop with its one Work Unit directly (see the module doc:
    // `LoopService::create` never assigns one).
    let injector = FailureInjector::new(ports.clone(), holder_id);
    let loop_id = LoopId::new();
    let constraint_resource = finalize_constraint_resource(&ports, &constraint.statement).await;
    let wu = work_unit(WorkUnitId::new(), &constraint_resource.uri);
    injector
        .genesis(loop_id, &wu)
        .await
        .expect("genesis succeeds");

    // --- Episode 1 (Execution, "Inspect"): the runner fails, the Loop
    // suspends with a committed Checkpoint. ---
    let revision = expected_revision(&service, loop_id).await;
    service
        .advance(AdvanceLoopCommand {
            metadata: command_metadata(Some(revision)),
            loop_id,
        })
        .await
        .expect("advance accepts (a collected runner failure is not a service error)");
    let status = service.status(loop_id).await.expect("status succeeds");
    assert_eq!(
        status.status,
        ApiLoopStatus::Suspended,
        "a runner start failure must suspend the Loop, not error the call"
    );

    // --- Resume: reconciles the interrupted Attempt (never actually
    // started) and returns the Loop to Active, Work Unit `Pending`. ---
    let revision = expected_revision(&service, loop_id).await;
    service
        .resume(ResumeLoopCommand {
            metadata: command_metadata(Some(revision)),
            loop_id,
        })
        .await
        .expect("resume succeeds: the runner confirmed it never started");
    let status = service.status(loop_id).await.expect("status succeeds");
    assert_eq!(status.status, ApiLoopStatus::Active);

    // --- Episode 2 (Execution, "Implement"): a fresh Attempt succeeds. ---
    let revision = expected_revision(&service, loop_id).await;
    service
        .advance(AdvanceLoopCommand {
            metadata: command_metadata(Some(revision)),
            loop_id,
        })
        .await
        .expect("advance succeeds");

    // --- Episode 3 (Verification, "Verify"): the marker does not exist yet,
    // so the pinned Deterministic gate rejects. ---
    let revision = expected_revision(&service, loop_id).await;
    service
        .advance(AdvanceLoopCommand {
            metadata: command_metadata(Some(revision)),
            loop_id,
        })
        .await
        .expect("advance succeeds (a rejected verification is not a service error)");

    // --- Episode 4 (Execution, "Remediate"): the rejected Work Unit
    // returned to `Pending`, so a third Attempt of the *same* Work Unit
    // runs and lands the fix. ---
    let revision = expected_revision(&service, loop_id).await;
    service
        .advance(AdvanceLoopCommand {
            metadata: command_metadata(Some(revision)),
            loop_id,
        })
        .await
        .expect("advance succeeds");

    // --- Episode 5 (Verification, "Final Verify"): the marker now exists,
    // so the gate accepts. ---
    let revision = expected_revision(&service, loop_id).await;
    service
        .advance(AdvanceLoopCommand {
            metadata: command_metadata(Some(revision)),
            loop_id,
        })
        .await
        .expect("advance succeeds");

    // --- Complete: the Work Unit is terminal (`Accepted`), so this final
    // `advance` completes the Loop — a durable transition, but not a sixth
    // Episode. ---
    let revision = expected_revision(&service, loop_id).await;
    service
        .advance(AdvanceLoopCommand {
            metadata: command_metadata(Some(revision)),
            loop_id,
        })
        .await
        .expect("advance succeeds");

    let status = service.status(loop_id).await.expect("status succeeds");
    assert_eq!(status.status, ApiLoopStatus::Terminal);
    let result = service
        .result(loop_id)
        .await
        .expect("result succeeds")
        .expect("a terminal Loop has a result");
    assert_eq!(result.outcome.kind, LoopOutcomeKind::Completed);

    // =======================================================================
    // Assertions
    // =======================================================================

    let stored = ports.store.load(loop_id).await.expect("load succeeds");

    // -- five Episodes, five top-level traces --
    let episode_starts: Vec<_> = stored
        .events
        .iter()
        .filter(|recorded| matches!(recorded.event, LoopEvent::EpisodeStarted { .. }))
        .collect();
    assert_eq!(
        episode_starts.len(),
        5,
        "exactly five Episodes must have run"
    );

    let top_level = trace_recorder.top_level_spans();
    let episode_spans: Vec<_> = top_level
        .iter()
        .filter(|span| span.name == loopward_observability::spans::span_name::EPISODE)
        .collect();
    assert_eq!(
        episode_spans.len(),
        5,
        "exactly five top-level Episode traces must have been recorded"
    );

    // -- every Episode after the first links back to its predecessor --
    let continuations = trace_recorder.links_of_kind(SpanLinkKind::ContinuationOf);
    let episode_continuations: usize = continuations
        .iter()
        .filter(|(span, _)| span.name == loopward_observability::spans::span_name::EPISODE)
        .count();
    assert_eq!(
        episode_continuations, 4,
        "Episodes 2-5 must each link `continuation_of` their predecessor's trace"
    );

    // -- fresh Session generation for each Episode --
    let generations: Vec<u32> = stored
        .events
        .iter()
        .filter_map(|recorded| match &recorded.event {
            LoopEvent::SessionStarted { generation, .. } => Some(*generation),
            _ => None,
        })
        .collect();
    assert_eq!(
        generations,
        vec![1, 2, 3, 4, 5],
        "every Episode must open a strictly new Runtime Session generation"
    );

    // -- at least one rejected result --
    let verification_outcomes: Vec<_> = stored
        .events
        .iter()
        .filter_map(|recorded| match &recorded.event {
            LoopEvent::VerificationCompleted { outcome, .. } => Some(*outcome),
            _ => None,
        })
        .collect();
    assert_eq!(
        verification_outcomes.len(),
        2,
        "two verification runs occurred"
    );
    assert_eq!(
        verification_outcomes[0],
        loopward_core::VerificationOutcome::Rejected,
        "the first verification (`Verify`) must be rejected"
    );
    assert_eq!(
        verification_outcomes[1],
        loopward_core::VerificationOutcome::Accepted,
        "the second verification (`Final Verify`) must be accepted -- final success criteria \
         are supported by current evidence"
    );

    // -- one remediation lineage: the rejected and the accepted verification
    // both judge the *same* Work Unit, and the Attempt collected between
    // them (the remediation Attempt) is a later Attempt of that Work Unit,
    // not a different Work Unit -- exactly what "a rejected result does not
    // silently restart the same Work Unit [under a different identity]"
    // (`docs/06-verification-and-recovery.md` §10) requires be traceable. --
    let verified_work_units: Vec<_> = stored
        .events
        .iter()
        .filter_map(|recorded| match &recorded.event {
            LoopEvent::VerificationCompleted { work_unit_id, .. } => Some(*work_unit_id),
            _ => None,
        })
        .collect();
    assert_eq!(verified_work_units, vec![wu.id, wu.id]);
    let attempt_ordinals: Vec<_> = stored
        .events
        .iter()
        .filter_map(|recorded| match &recorded.event {
            LoopEvent::AttemptPrepared { ordinal, .. } => Some(*ordinal),
            _ => None,
        })
        .collect();
    assert_eq!(
        attempt_ordinals,
        vec![1, 2, 3],
        "three Attempts of the one Work Unit: Inspect (retired), Implement, Remediate"
    );

    // -- required constraints unchanged across every rotation --
    let manifests = hydrated_manifests(&ports, &stored.events).await;
    assert_eq!(manifests.len(), 5, "one Boot Package per Episode");
    let first_pinned_digest = manifests[0].pinned_digest.clone();
    for manifest in &manifests {
        assert_eq!(
            manifest.pinned_digest, first_pinned_digest,
            "the pinned constraint set must never change across rotation"
        );
        let carries_constraint = manifest.items.iter().any(|item| {
            item.classification == ContextClass::Pinned
                && item.source.uri == constraint_resource.uri
        });
        assert!(
            carries_constraint,
            "every Boot Package must carry the pinned public-contract constraint"
        );
    }

    // -- no full history in the final Boot Package --
    let final_manifest = manifests.last().expect("five manifests");
    let final_tokens: u64 = final_manifest
        .items
        .iter()
        .filter_map(|item| item.estimated_tokens)
        .sum();
    assert!(
        final_tokens < 5_000,
        "the final Boot Package ({final_tokens} estimated tokens) must not carry the Loop's \
         full five-Episode history"
    );

    // -- the expected durable event sequence --
    let page = service
        .events(EventQuery {
            loop_id,
            after: None,
            limit: 1_000,
            durability: EventDurabilityFilter::Durable,
        })
        .await
        .expect("events succeeds");
    let actual_kinds: Vec<String> = page.events.iter().map(|event| event.kind.clone()).collect();
    let expected_kinds = load_expected_event_kinds();
    assert_eq!(
        actual_kinds, expected_kinds,
        "the durable event journal must match tests/fixtures/scenario/expected-events.json"
    );

    // =======================================================================
    // At least one forced process interruption, reconciled by a genuine
    // RecoveryManager -- against the same composition root (ports, holder),
    // an isolated Loop so it cannot perturb the scenario above.
    // =======================================================================
    let manager = RecoveryManager::new(service.controller().clone());
    let interruption_loop_id = LoopId::new();
    let interruption_work_unit = bare_work_unit(WorkUnitId::new());
    let crash_fixture = injector
        .seed(
            CrashPoint::DuringRunnerExecution,
            interruption_loop_id,
            interruption_work_unit,
        )
        .await
        .expect("seeding the forced interruption fixture succeeds");
    runner.script_attempt(
        crash_fixture.attempt_id,
        AttemptScript {
            fail_collect: true,
            reconcile: Some(ReconcileOutcome::Running),
            ..AttemptScript::default()
        },
    );
    let recovery_outcome = manager
        .recover(interruption_loop_id)
        .await
        .expect("recover succeeds");
    assert_eq!(
        recovery_outcome,
        RecoveryOutcome::Reattached,
        "the forced process interruption must reattach without forcing a result: {}",
        CrashPoint::DuringRunnerExecution.expected_recovery()
    );
}

/// Resolves every `context.hydrated` event's manifest, in order, through the
/// same `ResourceBroker` the scenario ran against.
async fn hydrated_manifests(
    ports: &RuntimePorts,
    events: &[loopward_runtime::RecordedEvent],
) -> Vec<loopward_api::context::BootPackageManifest> {
    let mut manifests = Vec::new();
    for recorded in events {
        if let LoopEvent::ContextHydrated { manifest, .. } = &recorded.event {
            let bytes = ports
                .resources
                .resolve(manifest)
                .await
                .expect("boot package manifest resolves");
            manifests
                .push(serde_json::from_slice(&bytes).expect("boot package manifest deserializes"));
        }
    }
    manifests
}
