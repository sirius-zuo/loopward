//! Proves [`FailureInjector`] against a *real* `SqliteLoopStore`: every one
//! of Phase One's eight named failure-injection points
//! (`docs/10-mvp-and-evaluation.md` §8) recovers exactly as
//! `docs/06-verification-and-recovery.md` §12-§13's Recovery Decisions table
//! promises, via the genuine `loopward_runtime::RecoveryManager` — not a
//! simulation of it.
//!
//! `loopward-runtime/tests/recovery_matrix.rs` already covers this ground in
//! depth against an in-memory fake store; this file exists to prove the
//! *reusable* `loopward-testkit` component reproduces the same eight
//! outcomes against a real durable store, which is the whole point of a
//! shared conformance testkit (`docs/10` §9.1, §13: "all eight
//! failure-injection scenarios recover as specified").

use std::sync::Arc;

use loopward_api::ids::WorkUnitId;
use loopward_api::resource::{OutputContract, VerificationPlanRef};
use loopward_api::runner::{ReconcileOutcome, WorkUnitRunner};
use loopward_api::work_unit::{
    ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitStatus,
};
use loopward_observability::NoopTelemetrySink;
use loopward_runtime::ports::{LoopStore, ResourceBroker};
use loopward_runtime::verification::{
    AggregationPolicy, GateConfig, GateEnforcement, GateId, RemediationPolicy, SchemaGateConfig,
    VerificationGateKind, VerificationGateSpec, VerificationPlanId,
};
use loopward_runtime::{
    LoopController, RecoveryManager, RecoveryOutcome, RuntimePorts, VerificationPlanResolver,
    VerificationPlanTemplate,
};
use loopward_store_sqlite::{FileResourceBroker, SqliteLoopStore, SqliteStoreConfig};
use loopward_testkit::failure::CrashPoint;
use loopward_testkit::{
    AttemptScript, FailureInjector, FakeRunner, FixedClock, SequentialIdGenerator,
};

/// One required Schema gate, trivially satisfied — verification's own
/// outcome is never exercised by these fixtures, only whether the runner is
/// (or is not) consulted while recovering.
struct AlwaysAcceptPlan;

impl VerificationPlanResolver for AlwaysAcceptPlan {
    fn resolve(&self, reference: &VerificationPlanRef) -> Option<VerificationPlanTemplate> {
        Some(VerificationPlanTemplate {
            id: VerificationPlanId::new(reference.reference.clone()),
            version: u64::from(reference.version),
            gates: vec![VerificationGateSpec {
                id: GateId::new("schema"),
                kind: VerificationGateKind::Schema,
                enforcement: GateEnforcement::Required,
                criterion: None,
                config: GateConfig::Schema(SchemaGateConfig::default()),
            }],
            aggregation: AggregationPolicy::default(),
            remediation_policy: RemediationPolicy::default(),
        })
    }
}

fn test_work_unit() -> WorkUnit {
    WorkUnit {
        id: WorkUnitId::new(),
        kind: WorkUnitKind::new("test.work-unit"),
        objective: "prove one failure-injection point recovers as documented".to_owned(),
        status: WorkUnitStatus::Pending,
        inputs: Vec::new(),
        constraints: Vec::new(),
        output_contract: OutputContract {
            schema: "loopward.test-output/v0.1".to_owned(),
        },
        verification_plan: VerificationPlanRef {
            reference: "test-plan".to_owned(),
            version: 1,
        },
        runner: loopward_api::resource::RunnerRef {
            name: "fake".to_owned(),
            profile: None,
        },
        budget: ResourceBudget::default(),
        retry_policy: RetryPolicy::default(),
    }
}

async fn build_ports() -> (RuntimePorts, tempfile::TempDir) {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = SqliteLoopStore::open(SqliteStoreConfig::new(temp.path().join("loopward.sqlite3")))
        .await
        .expect("store opens");
    let pool = store.pool().clone();
    let broker: Arc<dyn ResourceBroker> = Arc::new(
        FileResourceBroker::new(pool, temp.path().join("resources")).expect("broker opens"),
    );
    let store: Arc<dyn LoopStore> = Arc::new(store);
    let ports = RuntimePorts {
        store,
        resources: broker,
        clock: Arc::new(FixedClock::default()),
        ids: Arc::new(SequentialIdGenerator::new()),
        telemetry: Arc::new(NoopTelemetrySink),
    };
    (ports, temp)
}

/// Scripts `runner` with the answer `point` needs before recovery is asked
/// to reconcile it (a no-op for points that never consult the runner).
fn script_runner(point: CrashPoint, runner: &FakeRunner, attempt_id: loopward_api::ids::AttemptId) {
    match point {
        CrashPoint::BeforeRunnerStart => {
            runner.script_attempt(
                attempt_id,
                AttemptScript::default().with_reconcile(ReconcileOutcome::NotStarted),
            );
        }
        CrashPoint::AfterExternalStartBeforeHandlePersistence => {
            runner.script_attempt(
                attempt_id,
                AttemptScript::default().with_reconcile(ReconcileOutcome::Running),
            );
        }
        CrashPoint::DuringRunnerExecution => {
            runner.script_attempt(
                attempt_id,
                AttemptScript {
                    fail_collect: true,
                    reconcile: Some(ReconcileOutcome::Running),
                    ..AttemptScript::default()
                },
            );
        }
        CrashPoint::AfterResultMaterialization => {
            runner.script_attempt(
                attempt_id,
                AttemptScript::default().with_reconcile(ReconcileOutcome::Completed),
            );
        }
        CrashPoint::DuringVerification
        | CrashPoint::BeforeCheckpointTransaction
        | CrashPoint::AfterCheckpointCommitBeforeAcknowledgement
        | CrashPoint::DuringSessionDrain => {
            // Recovery never consults the runner for these points; no script
            // needed.
        }
    }
}

/// Asserts `outcome` matches the recovery [`CrashPoint::expected_recovery`]
/// promises, plus whatever extra shape that outcome variant carries
/// (checkpoint version deltas for the two rotation-adjacent points).
fn assert_recovered_as_documented(
    point: CrashPoint,
    checkpoint_version_before: u64,
    outcome: RecoveryOutcome,
) {
    match point {
        CrashPoint::BeforeRunnerStart
        | CrashPoint::AfterExternalStartBeforeHandlePersistence
        | CrashPoint::AfterResultMaterialization
        | CrashPoint::DuringVerification => {
            assert!(
                matches!(outcome, RecoveryOutcome::EpisodeRecovered { .. }),
                "`{}`: expected EpisodeRecovered, got {outcome:?}",
                point.label()
            );
        }
        CrashPoint::DuringRunnerExecution => {
            assert_eq!(
                outcome,
                RecoveryOutcome::Reattached,
                "`{}`: still genuinely running, nothing may be forced",
                point.label()
            );
        }
        CrashPoint::BeforeCheckpointTransaction | CrashPoint::DuringSessionDrain => match outcome {
            RecoveryOutcome::EpisodeRecovered {
                checkpoint_version, ..
            } => assert_eq!(checkpoint_version, checkpoint_version_before + 1),
            other => panic!(
                "`{}`: expected EpisodeRecovered, got {other:?}",
                point.label()
            ),
        },
        CrashPoint::AfterCheckpointCommitBeforeAcknowledgement => {
            assert_eq!(
                outcome,
                RecoveryOutcome::NothingToRecover,
                "`{}`: the Episode already fully terminated",
                point.label()
            );
        }
    }
}

/// Every one of Phase One's eight named failure-injection points, seeded
/// against a real `SqliteLoopStore` and recovered through a genuine
/// `RecoveryManager`, produces exactly the outcome
/// `docs/06-verification-and-recovery.md` §12-§13 promises for it.
#[tokio::test]
async fn all_eight_crash_points_recover_as_documented() {
    let (ports, _temp) = build_ports().await;

    let runner = Arc::new(FakeRunner::new());
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    );
    let holder_id = controller.holder_id();
    let manager = RecoveryManager::new(controller);
    let injector = FailureInjector::new(ports.clone(), holder_id);

    for point in CrashPoint::ALL {
        let loop_id = FailureInjector::fresh_loop_id();
        let work_unit = test_work_unit();

        let fixture = injector
            .seed(point, loop_id, work_unit)
            .await
            .unwrap_or_else(|error| panic!("seeding `{}` failed: {error}", point.label()));
        let start_calls_before = runner.start_calls();
        let reconcile_calls_before = runner.reconcile_calls();

        script_runner(point, &runner, fixture.attempt_id);
        let outcome = manager.recover(loop_id).await.expect("recover succeeds");

        match point {
            CrashPoint::BeforeRunnerStart => {
                assert_eq!(
                    runner.start_calls(),
                    start_calls_before + 1,
                    "`{}`: the runner must be started once",
                    point.label()
                );
            }
            CrashPoint::AfterExternalStartBeforeHandlePersistence => {
                assert_eq!(
                    runner.start_calls(),
                    start_calls_before,
                    "`{}`: an already-started Attempt must never be started a second time",
                    point.label()
                );
            }
            CrashPoint::DuringVerification => {
                assert_eq!(
                    runner.reconcile_calls(),
                    reconcile_calls_before,
                    "`{}`: verification never consults the runner",
                    point.label()
                );
            }
            _ => {}
        }
        assert_recovered_as_documented(point, fixture.state.execution.checkpoint_version, outcome);
    }
}

/// Every crash point's `label`/`expected_recovery` documentation is
/// non-empty and distinct, matching `docs/10` §8's requirement that "each
/// case must have an expected reconciliation and recovery outcome".
#[test]
fn every_crash_point_documents_its_expected_recovery() {
    let mut labels = std::collections::BTreeSet::new();
    for point in CrashPoint::ALL {
        assert!(!point.label().is_empty());
        assert!(!point.expected_recovery().is_empty());
        assert!(
            labels.insert(point.label()),
            "duplicate label: {}",
            point.label()
        );
    }
    assert_eq!(labels.len(), 8);
}
