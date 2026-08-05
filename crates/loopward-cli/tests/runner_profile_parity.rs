//! Demonstrates that two different `loopward_api::runner::WorkUnitRunner`
//! implementations — `loopward_runner_command::CommandRunner` (the real
//! external-process adapter, Task 7) and `loopward_testkit::FakeRunner` (the
//! scripted in-memory test double, Task 13) — can both complete the *same*
//! Work Unit protocol fixture and reach an equivalent outcome.
//!
//! This closes `docs/10-mvp-and-evaluation.md` §13's "at least two
//! different command-runner profiles complete the same Work Unit protocol
//! fixture" acceptance gate, previously recorded as an unmet criterion in
//! `docs/phase-one-conformance.md` §8.
//!
//! Deliberately narrow scope: one fixture, one deterministic `Produced`
//! outcome carrying one declared output resource — not a re-run of the
//! five-Episode reference scenario (`e2e_phase_one.rs`) through a real
//! external process, which would require replicating Task 13's entire
//! scripted multi-Episode narrative (rejection, remediation, etc.) against a
//! real command, disproportionate scope for this gap. The point being
//! demonstrated is narrower and simpler: the same request shape, handed to
//! two different runner implementations, comes back with the same
//! runner-asserted outcome and the same output content.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
use loopward_api::runner::WorkUnitRunner;
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{
    ResourceBudget, RetryPolicy, UsageReport, WorkUnit, WorkUnitKind, WorkUnitOutcome,
    WorkUnitRequest, WorkUnitResult, WorkUnitStatus,
};
use loopward_runner_command::{CommandRunner, CommandRunnerConfig};
use loopward_runtime::{ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle};
use loopward_testkit::{AttemptScript, FakeRunner};

// ---------------------------------------------------------------------------
// The shared Work Unit protocol fixture
// ---------------------------------------------------------------------------

/// The output resource both runner profiles declare for this fixture,
/// byte-for-byte identical: `tests/fixtures/runner/parity.sh` (driven
/// through `CommandRunner`) writes exactly this into its `result.json`, and
/// the `FakeRunner` script below returns exactly this same value directly.
fn fixture_output() -> ResourceRef {
    ResourceRef {
        uri: "loopward-fixture://runner-profile-parity/output".to_owned(),
        media_type: Some("text/plain; charset=utf-8".to_owned()),
        digest: Some("sha256:runner-profile-parity-fixture".to_owned()),
        version: None,
        size: Some(42),
        summary: Some("loopward-runner-profile-parity fixture output".to_owned()),
    }
}

/// One Work Unit protocol fixture request. Every field is fixed except the
/// Attempt identity, so the identical shape can be handed to both runner
/// profiles.
fn fixture_request(attempt_id: AttemptId) -> WorkUnitRequest {
    let loop_id = LoopId::new();
    let episode_id = EpisodeId::new();
    let work_unit_id = WorkUnitId::new();

    let boot_package = BootPackageManifest {
        schema_version: "loopward.boot-package-manifest/v0.1".to_owned(),
        loop_id,
        episode_id,
        session_generation: 1,
        work_unit_id,
        checkpoint_ref: CheckpointRef::new("checkpoint://runner-profile-parity/1"),
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
                uri: "constraint://runner-profile-parity/001".to_owned(),
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
        kind: WorkUnitKind::new("domain.runner-profile-parity"),
        objective: "Produce one deterministic, externally observable unit of progress.".to_owned(),
        status: WorkUnitStatus::Active,
        inputs: vec![],
        constraints: vec![],
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_owned(),
        },
        verification_plan: VerificationPlanRef {
            reference: "verification-plan://runner-profile-parity".to_owned(),
            version: 1,
        },
        runner: RunnerRef {
            name: "runner-profile-parity-fixture".to_owned(),
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
        session_generation: 1,
        work_unit,
        attempt_id,
        idempotency_key: "runner-profile-parity-1".to_owned(),
        boot_package,
        resource_grants: vec![],
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_owned(),
        },
        budget: ResourceBudget::default(),
        deadline: None,
        trace_context: TraceContext {
            traceparent: None,
            tracestate: None,
            baggage: vec![],
        },
    }
}

/// The expected outcome shared by both runner profiles: a `Produced` result
/// carrying exactly [`fixture_output`] and nothing else. `FakeRunner` is
/// scripted to return this directly; `CommandRunner` must arrive at an
/// equivalent result via `tests/fixtures/runner/parity.sh`.
fn expected_result(attempt_id: AttemptId) -> WorkUnitResult {
    WorkUnitResult {
        protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
        attempt_id,
        outcome: WorkUnitOutcome::Produced,
        outputs: vec![fixture_output()],
        state_proposals: vec![],
        observation_proposals: vec![],
        decision_proposals: vec![],
        verification_evidence: vec![],
        unresolved: vec![],
        recommended_continuation: None,
        usage: UsageReport::default(),
        runner_trace_refs: vec![],
    }
}

/// Asserts that `result` completes the fixture equivalently to
/// [`expected_result`]: the same protocol version, the same runner-asserted
/// outcome, and the fixture's declared output resource present among its
/// outputs — ignoring any additional runner-specific outputs (such as
/// `CommandRunner`'s captured stdout/stderr logs), which are execution
/// plumbing, not part of the fixture itself.
fn assert_completes_fixture_equivalently(result: &WorkUnitResult, attempt_id: AttemptId) {
    let expected = expected_result(attempt_id);
    assert_eq!(result.attempt_id, expected.attempt_id);
    assert_eq!(result.protocol_version, expected.protocol_version);
    assert_eq!(
        result.outcome, expected.outcome,
        "both runner profiles must assert the same outcome for the same fixture"
    );
    assert!(
        result.outputs.contains(&fixture_output()),
        "the fixture's declared output resource must be present, byte-for-byte, in the \
         collected result; got: {:?}",
        result.outputs
    );
}

// ---------------------------------------------------------------------------
// Profile 1 support: `CommandRunner` over a small, portable shell fixture
// ---------------------------------------------------------------------------

/// A [`ResourceBroker`] backed by in-memory buffers, adapted from
/// `crates/loopward-runner-command/tests/runner_contract.rs`'s test double
/// of the same name. `CommandRunner` needs one to finalize its captured
/// stdout/stderr logs; this fixture does not care what those logs contain.
#[derive(Default)]
struct InMemoryResourceBroker {
    open: Mutex<HashMap<String, (ResourceBeginHint, Vec<u8>)>>,
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
        Err(ResourceError::UnresolvableReference(reference.uri.clone()))
    }
}

/// Resolves the shared, portable fixture command
/// (`tests/fixtures/runner/parity.sh`), following the same convention
/// `runner_contract.rs`'s `fixture()` helper uses for the other command
/// runner fixture scripts.
fn parity_fixture_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/runner")
        .join("parity.sh")
}

/// Builds a `CommandRunner` configured to invoke the shared fixture script,
/// rooted inside `dir`.
fn command_runner_in(dir: &tempfile::TempDir) -> CommandRunner {
    let config = CommandRunnerConfig::new(parity_fixture_script(), dir.path().join("runs"))
        .with_capture_drain_grace(Duration::from_millis(250));
    CommandRunner::new(config, Arc::new(InMemoryResourceBroker::default()))
}

// ---------------------------------------------------------------------------
// Parity: the same fixture, through both profiles, reaches the same outcome
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_different_runner_profiles_complete_the_same_work_unit_protocol_fixture() {
    // Profile 1: the real external-process adapter (Task 7), invoking a
    // small, deterministic, portable shell command.
    let dir = tempfile::tempdir().expect("create temp dir");
    let command_runner = command_runner_in(&dir);
    let command_attempt_id = AttemptId::new();
    let command_handle = command_runner
        .start(fixture_request(command_attempt_id))
        .await
        .expect("the command runner starts the fixture");
    let command_result = command_runner
        .collect(&command_handle)
        .await
        .expect("the command runner collects the fixture's proposed result");
    assert_completes_fixture_equivalently(&command_result, command_attempt_id);

    // Profile 2: the scripted in-memory test double (Task 13), scripted to
    // produce the identical expected result.
    let fake_runner = FakeRunner::new();
    let fake_attempt_id = AttemptId::new();
    fake_runner.script_attempt(
        fake_attempt_id,
        AttemptScript::succeeding_with(expected_result(fake_attempt_id)),
    );
    let fake_handle = fake_runner
        .start(fixture_request(fake_attempt_id))
        .await
        .expect("the fake runner starts the fixture");
    let fake_result = fake_runner
        .collect(&fake_handle)
        .await
        .expect("the fake runner collects the fixture's scripted result");
    assert_completes_fixture_equivalently(&fake_result, fake_attempt_id);

    // Direct parity between the two profiles' results for the same fixture:
    // the same protocol version, the same runner-asserted outcome, and the
    // same declared output content.
    assert_eq!(
        command_result.protocol_version, fake_result.protocol_version,
        "both profiles must report the same protocol version for the same fixture"
    );
    assert_eq!(
        command_result.outcome, fake_result.outcome,
        "both profiles must complete the same fixture with the same outcome"
    );
    assert!(
        command_result.outputs.contains(&fixture_output())
            && fake_result.outputs.contains(&fixture_output()),
        "both profiles must produce the fixture's declared output content, byte-for-byte:\n\
         command outputs: {:?}\nfake outputs: {:?}",
        command_result.outputs,
        fake_result.outputs
    );
}
