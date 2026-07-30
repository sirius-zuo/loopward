//! Contract tests for the generic command runner: the Phase One reference
//! `WorkUnitRunner` that executes an arbitrary command as a Work Unit
//! Attempt (`docs/05-work-unit-and-runner-protocol.md` §13-15).
//!
//! Every test drives the runner through its public trait surface only, so
//! the assertions describe the protocol a command author programs against:
//! an isolated attempt directory, documented environment variables, a
//! result file that is a *proposal*, bounded log capture, and cooperative
//! cancellation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use loopward_api::context::{
    BootPackageManifest, ContextBudget, ContextClass, ContextItem, ContextPriority, Fidelity,
    Sensitivity, Validity,
};
use loopward_api::ids::{AttemptId, EpisodeId, LoopId, SessionId, WorkUnitId};
use loopward_api::resource::{
    CheckpointRef, OutputContract, ResourceRef, RunnerRef, VerificationPlanRef,
};
use loopward_api::runner::{
    CancellationReason, RunState, RunnerErrorCode, SuspensionReason, WorkUnitRunner,
};
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{
    ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitOutcome, WorkUnitRequest,
    WorkUnitStatus,
};
use loopward_runner_command::{
    CommandRunner, CommandRunnerConfig, TerminationObservation, protocol,
};
use loopward_runtime::{ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle};

// ---------------------------------------------------------------------------
// Test doubles and fixtures
// ---------------------------------------------------------------------------

/// A [`ResourceBroker`] backed by in-memory buffers. It carries the begin
/// hint through onto the finalized [`ResourceRef`] exactly as a real broker
/// would, so a test can observe what the runner declared about the bytes it
/// finalized.
#[derive(Default)]
struct InMemoryResourceBroker {
    open: Mutex<HashMap<String, (ResourceBeginHint, Vec<u8>)>>,
    finalized: Mutex<HashMap<String, Vec<u8>>>,
    next: AtomicU64,
}

#[async_trait]
impl ResourceBroker for InMemoryResourceBroker {
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
        let uri = format!("resource://{}", handle.as_str());
        let size = bytes.len() as u64;
        self.finalized
            .lock()
            .expect("finalized lock poisoned")
            .insert(uri.clone(), bytes);
        Ok(ResourceRef {
            uri,
            media_type: hint.media_type,
            digest: Some(format!("fake-sha256:{size}")),
            version: None,
            size: Some(size),
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
        self.finalized
            .lock()
            .expect("finalized lock poisoned")
            .get(&reference.uri)
            .cloned()
            .ok_or_else(|| ResourceError::UnresolvableReference(reference.uri.clone()))
    }
}

/// Resolves one of the repo-root runner fixture scripts.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/runner")
        .join(name)
}

fn sample_request(attempt_id: AttemptId) -> WorkUnitRequest {
    let loop_id = LoopId::new();
    let episode_id = EpisodeId::new();
    let work_unit_id = WorkUnitId::new();

    let boot_package = BootPackageManifest {
        schema_version: "loopward.boot-package-manifest/v0.1".to_owned(),
        loop_id,
        episode_id,
        session_generation: 7,
        work_unit_id,
        checkpoint_ref: CheckpointRef::new("checkpoint://loop-001/1"),
        context_rule_version: "deterministic-v0.1".to_owned(),
        budget: ContextBudget {
            model_window_tokens: None,
            soft_input_limit_tokens: 24_000,
            hard_input_limit_tokens: 48_000,
            reserved_output_tokens: 8_000,
            reserved_tool_tokens: 8_000,
            safety_margin_tokens: 4_000,
        },
        items: vec![ContextItem {
            id: "context-item-001".to_owned(),
            kind: "constraint".to_owned(),
            source: ResourceRef {
                uri: "constraint://constraint-001".to_owned(),
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
            digest: "sha256:item".to_owned(),
            selection_reason: "Active loop-scoped user constraint".to_owned(),
        }],
        pinned_digest: "sha256:pinned".to_owned(),
        manifest_digest: "sha256:manifest".to_owned(),
    };

    let work_unit = WorkUnit {
        id: work_unit_id,
        kind: WorkUnitKind::new("domain.example"),
        objective: "Produce one externally observable unit of progress.".to_owned(),
        status: WorkUnitStatus::Active,
        inputs: vec![],
        constraints: vec![],
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_owned(),
        },
        verification_plan: VerificationPlanRef {
            reference: "verification-plan://plan-017".to_owned(),
            version: 1,
        },
        runner: RunnerRef {
            name: "command".to_owned(),
            profile: Some("default".to_owned()),
        },
        budget: ResourceBudget::default(),
        retry_policy: RetryPolicy::default(),
    };

    WorkUnitRequest {
        protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
        loop_id,
        episode_id,
        session_id: SessionId::new(),
        session_generation: 7,
        work_unit,
        attempt_id,
        idempotency_key: "attempt-017-1".to_owned(),
        boot_package,
        resource_grants: vec![],
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_owned(),
        },
        budget: ResourceBudget::default(),
        deadline: None,
        trace_context: TraceContext {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned()),
            tracestate: None,
            baggage: vec![],
        },
    }
}

/// Polls a captured log until it contains `marker`.
///
/// Cancellation tests must not race the shell's own startup: a `SIGTERM`
/// delivered before the fixture installs its trap is handled by the default
/// disposition, which would make the test's outcome depend on process
/// scheduling. Waiting for the fixture's readiness marker removes that race
/// without lengthening the test — the marker normally appears in single-digit
/// milliseconds.
async fn wait_for_log_marker(path: &Path, marker: &str) {
    for _ in 0..1_000 {
        if std::fs::read_to_string(path).is_ok_and(|log| log.contains(marker)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("`{marker}` never appeared in `{}`", path.display());
}

/// A config whose `run_root` lives inside the test's temp directory.
fn config_in(dir: &tempfile::TempDir, program: PathBuf) -> CommandRunnerConfig {
    CommandRunnerConfig::new(program, dir.path().join("runs"))
}

// ---------------------------------------------------------------------------
// Protocol materialization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_materializes_the_request_and_boot_package_in_an_isolated_attempt_directory() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let runner = CommandRunner::new(config_in(&dir, fixture("success.sh")), broker);

    let attempt_id = AttemptId::new();
    let request = sample_request(attempt_id);
    let handle = runner.start(request.clone()).await.expect("start succeeds");

    assert_eq!(handle.attempt_id, attempt_id);
    assert!(
        handle.external_execution_ref.is_some(),
        "the runner must return an external execution reference for the spawned process"
    );

    let attempt_dir = runner.attempt_dir(attempt_id);
    assert_eq!(
        attempt_dir,
        dir.path().join("runs").join(attempt_id.to_string())
    );

    // Wait for the command to finish so the whole layout is observable.
    runner.collect(&handle).await.expect("collect succeeds");

    let request_json = std::fs::read_to_string(attempt_dir.join(protocol::REQUEST_FILE_NAME))
        .expect("request.json is materialized");
    let materialized: WorkUnitRequest =
        serde_json::from_str(&request_json).expect("request.json round-trips");
    assert_eq!(materialized, request);

    let boot_json = std::fs::read_to_string(attempt_dir.join(protocol::BOOT_PACKAGE_FILE_NAME))
        .expect("boot-package.json is materialized");
    let boot: BootPackageManifest =
        serde_json::from_str(&boot_json).expect("boot-package.json round-trips");
    assert_eq!(boot, request.boot_package);

    assert!(attempt_dir.join(protocol::INPUTS_DIR_NAME).is_dir());
    assert!(attempt_dir.join(protocol::OUTPUTS_DIR_NAME).is_dir());
    assert!(attempt_dir.join(protocol::STDOUT_LOG_FILE_NAME).is_file());
    assert!(attempt_dir.join(protocol::STDERR_LOG_FILE_NAME).is_file());
    assert!(attempt_dir.join(protocol::RESULT_FILE_NAME).is_file());
}

#[tokio::test]
async fn start_exports_the_documented_environment_variables() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let runner = CommandRunner::new(config_in(&dir, fixture("success.sh")), broker);

    let attempt_id = AttemptId::new();
    let mut request = sample_request(attempt_id);
    // `traceparent` is present; `tracestate` is deliberately absent.
    request.trace_context.tracestate = None;

    let handle = runner.start(request.clone()).await.expect("start succeeds");
    runner.collect(&handle).await.expect("collect succeeds");

    let attempt_dir = runner.attempt_dir(attempt_id);
    let stdout = std::fs::read_to_string(attempt_dir.join(protocol::STDOUT_LOG_FILE_NAME))
        .expect("stdout.log is captured");

    let expected = [
        format!("LOOPWARD_LOOP_ID={}", request.loop_id),
        format!("LOOPWARD_EPISODE_ID={}", request.episode_id),
        format!("LOOPWARD_SESSION_ID={}", request.session_id),
        format!("LOOPWARD_SESSION_GENERATION={}", request.session_generation),
        format!("LOOPWARD_WORK_UNIT_ID={}", request.work_unit.id),
        format!("LOOPWARD_ATTEMPT_ID={attempt_id}"),
        format!(
            "LOOPWARD_REQUEST_PATH={}",
            attempt_dir.join(protocol::REQUEST_FILE_NAME).display()
        ),
        format!(
            "LOOPWARD_RESULT_PATH={}",
            attempt_dir.join(protocol::RESULT_FILE_NAME).display()
        ),
        format!(
            "LOOPWARD_RESOURCE_DIR={}",
            attempt_dir.join(protocol::OUTPUTS_DIR_NAME).display()
        ),
        "TRACEPARENT=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
    ];
    for line in expected {
        assert!(
            stdout.lines().any(|observed| observed == line),
            "expected environment line `{line}` in captured stdout:\n{stdout}"
        );
    }

    assert!(
        stdout.lines().any(|line| line == "TRACESTATE=<unset>"),
        "TRACESTATE must not be exported when the request does not carry it:\n{stdout}"
    );
}

#[tokio::test]
async fn start_rejects_an_incompatible_request_protocol_major_version_before_executing() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let runner = CommandRunner::new(config_in(&dir, fixture("success.sh")), broker);

    let attempt_id = AttemptId::new();
    let mut request = sample_request(attempt_id);
    request.protocol_version = "loopward.api/v9.0".to_owned();

    let error = runner
        .start(request)
        .await
        .expect_err("an incompatible major version is rejected before execution");
    assert_eq!(error.code, RunnerErrorCode::ProtocolError);
    assert!(
        !runner.attempt_dir(attempt_id).exists(),
        "nothing may be materialized for a rejected request"
    );
}

// ---------------------------------------------------------------------------
// Result collection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn collect_returns_the_commands_proposed_result() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let runner = CommandRunner::new(config_in(&dir, fixture("success.sh")), broker);

    let attempt_id = AttemptId::new();
    let handle = runner
        .start(sample_request(attempt_id))
        .await
        .expect("start succeeds");

    let result = runner.collect(&handle).await.expect("collect succeeds");
    assert_eq!(result.attempt_id, attempt_id);
    assert_eq!(result.outcome, WorkUnitOutcome::Produced);
    assert_eq!(result.protocol_version, loopward_api::PROTOCOL_VERSION);

    let status = runner.status(&handle).await.expect("status succeeds");
    assert_eq!(status.state, RunState::Completed);
}

#[tokio::test]
async fn collect_reports_invalid_result_when_the_command_writes_no_result_file() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    // `true` exits successfully without ever writing a result file.
    let runner = CommandRunner::new(config_in(&dir, PathBuf::from("/usr/bin/true")), broker);

    let handle = runner
        .start(sample_request(AttemptId::new()))
        .await
        .expect("start succeeds");

    let error = runner
        .collect(&handle)
        .await
        .expect_err("a missing result file is not collectable");
    assert_eq!(error.code, RunnerErrorCode::InvalidResult);
}

#[tokio::test]
async fn collect_reports_invalid_result_when_the_result_json_is_malformed() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let runner = CommandRunner::new(config_in(&dir, fixture("invalid-result.sh")), broker);

    let handle = runner
        .start(sample_request(AttemptId::new()))
        .await
        .expect("start succeeds");

    let error = runner
        .collect(&handle)
        .await
        .expect_err("malformed result JSON is not collectable");
    assert_eq!(error.code, RunnerErrorCode::InvalidResult);
}

#[tokio::test]
async fn collect_rejects_a_result_file_above_the_configured_maximum_size() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let config = config_in(&dir, fixture("success.sh")).with_max_result_bytes(16);
    let runner = CommandRunner::new(config, broker);

    let handle = runner
        .start(sample_request(AttemptId::new()))
        .await
        .expect("start succeeds");

    let error = runner
        .collect(&handle)
        .await
        .expect_err("an oversized result file is rejected");
    assert_eq!(error.code, RunnerErrorCode::InvalidResult);
    assert!(
        error.message.contains("maximum"),
        "the error should explain the size limit, got: {}",
        error.message
    );
}

#[tokio::test]
async fn collect_rejects_a_result_with_an_incompatible_protocol_major_version() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let config = config_in(&dir, fixture("success.sh")).with_args(["wrong-version"]);
    let runner = CommandRunner::new(config, broker);

    let handle = runner
        .start(sample_request(AttemptId::new()))
        .await
        .expect("start succeeds");

    let error = runner
        .collect(&handle)
        .await
        .expect_err("an incompatible result protocol version is rejected");
    assert_eq!(error.code, RunnerErrorCode::ProtocolError);
}

// ---------------------------------------------------------------------------
// Bounded capture
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stdout_and_stderr_are_finalized_as_resources_with_bounded_capture() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let config = config_in(&dir, fixture("success.sh"))
        .with_args(["flood"])
        .with_max_capture_bytes(512);
    let runner = CommandRunner::new(config, broker.clone());

    let attempt_id = AttemptId::new();
    let handle = runner
        .start(sample_request(attempt_id))
        .await
        .expect("start succeeds");
    let result = runner.collect(&handle).await.expect("collect succeeds");

    let attempt_dir = runner.attempt_dir(attempt_id);
    for name in [
        protocol::STDOUT_LOG_FILE_NAME,
        protocol::STDERR_LOG_FILE_NAME,
    ] {
        let len = std::fs::metadata(attempt_dir.join(name))
            .expect("log file exists")
            .len();
        assert_eq!(
            len, 512,
            "`{name}` must be capped at the configured capture limit, observed {len} bytes"
        );
    }

    let logs: Vec<&ResourceRef> = result
        .outputs
        .iter()
        .filter(|output| {
            output
                .summary
                .as_deref()
                .is_some_and(|summary| summary.contains("stdout") || summary.contains("stderr"))
        })
        .collect();
    assert_eq!(
        logs.len(),
        2,
        "both captured streams must be finalized as resources, got: {:?}",
        result.outputs
    );
    for log in logs {
        assert_eq!(log.size, Some(512));
        assert!(
            log.summary
                .as_deref()
                .is_some_and(|summary| summary.contains("truncated")),
            "a truncated capture must say so, got: {:?}",
            log.summary
        );
        // The finalized bytes actually went through the broker.
        let bytes = broker.resolve(log).await.expect("resource resolves");
        assert_eq!(bytes.len(), 512);
    }
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_observes_cooperative_termination_within_the_grace_period() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let config =
        config_in(&dir, fixture("slow.sh")).with_cancellation_grace(Duration::from_secs(5));
    let runner = CommandRunner::new(config, broker);

    let attempt_id = AttemptId::new();
    let handle = runner
        .start(sample_request(attempt_id))
        .await
        .expect("start succeeds");
    wait_for_log_marker(
        &runner
            .attempt_dir(attempt_id)
            .join(protocol::STDOUT_LOG_FILE_NAME),
        "slow runner started",
    )
    .await;
    assert_eq!(
        runner.status(&handle).await.expect("status succeeds").state,
        RunState::Running
    );

    runner
        .cancel(&handle, CancellationReason::OperatorRequest)
        .await
        .expect("cancel succeeds");

    assert_eq!(
        runner.termination_observation(&handle),
        Some(TerminationObservation::ExitedWithinGrace),
        "a command that honours SIGTERM must be observed exiting within the grace period"
    );
    let status = runner.status(&handle).await.expect("status succeeds");
    assert_eq!(status.state, RunState::Cancelled);
    assert!(
        status
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("grace")),
        "status must report how termination was observed, got: {:?}",
        status.detail
    );
}

#[tokio::test]
async fn cancel_force_kills_and_reports_it_when_the_grace_period_expires() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let config = config_in(&dir, fixture("slow.sh"))
        .with_args(["ignore-term"])
        .with_cancellation_grace(Duration::from_millis(250));
    let runner = CommandRunner::new(config, broker);

    let attempt_id = AttemptId::new();
    let handle = runner
        .start(sample_request(attempt_id))
        .await
        .expect("start succeeds");
    wait_for_log_marker(
        &runner
            .attempt_dir(attempt_id)
            .join(protocol::STDOUT_LOG_FILE_NAME),
        "slow runner started",
    )
    .await;

    runner
        .cancel(&handle, CancellationReason::Timeout)
        .await
        .expect("cancel succeeds");

    assert_eq!(
        runner.termination_observation(&handle),
        Some(TerminationObservation::ForceKilled),
        "a command that ignores SIGTERM must be reported as force-killed, not as a clean exit"
    );
    let status = runner.status(&handle).await.expect("status succeeds");
    assert_eq!(status.state, RunState::Cancelled);
}

// ---------------------------------------------------------------------------
// Declared capabilities
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capabilities_are_declared_truthfully_and_unsupported_operations_say_so() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let runner = CommandRunner::new(config_in(&dir, fixture("success.sh")), broker);

    let capabilities = runner.capabilities().await.expect("capabilities succeed");
    assert!(
        capabilities
            .protocol_versions
            .iter()
            .any(|version| version == loopward_api::PROTOCOL_VERSION)
    );
    assert!(capabilities.supports_cancel);
    assert!(!capabilities.supports_suspend);
    assert!(!capabilities.supports_events);

    let handle = runner
        .start(sample_request(AttemptId::new()))
        .await
        .expect("start succeeds");

    let error = runner
        .suspend(&handle, SuspensionReason::Rotation)
        .await
        .expect_err("suspend is not supported");
    assert_eq!(error.code, RunnerErrorCode::UnsupportedCapability);

    let error = runner
        .events(&handle, None)
        .await
        .expect_err("events are not supported");
    assert_eq!(error.code, RunnerErrorCode::UnsupportedCapability);

    runner.collect(&handle).await.expect("collect succeeds");
}

#[tokio::test]
async fn reconcile_reports_not_started_for_an_attempt_this_runner_never_saw() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let broker = Arc::new(InMemoryResourceBroker::default());
    let runner = CommandRunner::new(config_in(&dir, fixture("success.sh")), broker);

    let handle = loopward_api::runner::RunHandle {
        attempt_id: AttemptId::new(),
        external_execution_ref: None,
    };
    let outcome = runner.reconcile(&handle).await.expect("reconcile succeeds");
    assert_eq!(
        outcome,
        loopward_api::runner::ReconcileOutcome::NotStarted,
        "an attempt with no in-memory entry and no attempt directory was never sent"
    );
}
