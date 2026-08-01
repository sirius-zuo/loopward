//! Episode-link conformance test for the `OpenTelemetry` adapter (Task 11,
//! Step 2; `docs/08-tracing-and-observability.md` §4, §7, §8).
//!
//! Drives a real [`LoopController`] through the same two-`advance()`-call
//! sequence `loopward-runtime`'s own `advance_episode.rs` tests use for one
//! Work Unit (an Execution Episode, then a Verification Episode — Task 9
//! established that a Work Unit takes two Episodes because
//! `EpisodeStatus` has no `Executing -> Hydrating` edge within one Episode
//! instance), with an [`OtelTelemetrySink`] backed by
//! `opentelemetry_sdk`'s in-memory span exporter (`docs/08` §14: "an
//! in-memory span exporter or recorder ... No external telemetry service is
//! required"), and asserts:
//!
//! - two separate root traces exist (one per Episode);
//! - Episode 2's root span carries a `continuation_of` link to Episode 1's
//!   trace;
//! - both root spans record the correct Loop ID and Checkpoint version;
//! - the runner boundary span (`loopward.work_unit.execute`) carries
//!   propagated W3C context.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::context::{ContextCapabilities, ContextControlLevel};
use loopward_api::ids::{
    AttemptId, CheckpointId, EpisodeId, EventId, LoopId, OperationId, SessionId, Timestamp,
    WorkUnitId,
};
use loopward_api::loop_service::LoopKind;
use loopward_api::resource::{
    ConstraintRef, OutputContract, PhaseRef, ResourceRef, RunnerRef, VerificationPlanRef,
};
use loopward_api::runner::{
    CancellationReason, ReconcileOutcome, RunHandle, RunState, RunStatus, RunnerCapabilities,
    RunnerError, RunnerEventPage, SuspensionReason, WorkUnitRunner,
};
use loopward_api::work_unit::{
    ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitOutcome, WorkUnitRequest,
    WorkUnitResult, WorkUnitStatus,
};
use loopward_core::{AggregateState, Checkpoint, LoopEvent, LoopExecution, LoopStatus, evolve};
use loopward_observability_otel::OtelTelemetrySink;
use loopward_runtime::context::ContextCandidate;
use loopward_runtime::ports::{
    AcquireLease, CheckpointCommitRequest, Clock, EVENT_SCHEMA_VERSION, IdGenerator, LoopLease,
    LoopStore, RecordedEvent, ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle,
    RuntimePorts, StoreError, StoredLoop,
};
use loopward_runtime::verification::{
    AggregationPolicy, GateConfig, GateEnforcement, GateId, RemediationPolicy, SchemaGateConfig,
    VerificationGateKind, VerificationGateSpec, VerificationPlanId,
};
use loopward_runtime::{
    ContextCandidateProvider, LoopController, VerificationPlanResolver, VerificationPlanTemplate,
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Minimal deterministic fakes (same shape as `loopward-runtime`'s own
// `advance_episode.rs`, condensed to just what a two-Episode happy path
// needs).
// ---------------------------------------------------------------------------

struct FixedClock(Timestamp);

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

struct SequentialIdGenerator {
    next: AtomicU64,
}

impl SequentialIdGenerator {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    fn next_uuid(&self) -> Uuid {
        let value = self.next.fetch_add(1, Ordering::SeqCst);
        Uuid::from_u128(u128::from(value))
    }
}

impl IdGenerator for SequentialIdGenerator {
    fn new_loop_id(&self) -> LoopId {
        LoopId::from_uuid(self.next_uuid())
    }
    fn new_episode_id(&self) -> EpisodeId {
        EpisodeId::from_uuid(self.next_uuid())
    }
    fn new_session_id(&self) -> SessionId {
        SessionId::from_uuid(self.next_uuid())
    }
    fn new_work_unit_id(&self) -> WorkUnitId {
        WorkUnitId::from_uuid(self.next_uuid())
    }
    fn new_attempt_id(&self) -> AttemptId {
        AttemptId::from_uuid(self.next_uuid())
    }
    fn new_operation_id(&self) -> OperationId {
        OperationId::from_uuid(self.next_uuid())
    }
    fn new_event_id(&self) -> EventId {
        EventId::from_uuid(self.next_uuid())
    }
    fn new_checkpoint_id(&self) -> CheckpointId {
        CheckpointId::from_uuid(self.next_uuid())
    }
}

struct InMemoryResourceBroker {
    open: Mutex<HashMap<String, Vec<u8>>>,
    finalized: Mutex<HashMap<String, Vec<u8>>>,
    next: AtomicU64,
}

impl InMemoryResourceBroker {
    fn new() -> Self {
        Self {
            open: Mutex::new(HashMap::new()),
            finalized: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl ResourceBroker for InMemoryResourceBroker {
    async fn begin(&self, _hint: ResourceBeginHint) -> Result<ResourceHandle, ResourceError> {
        let ordinal = self.next.fetch_add(1, Ordering::SeqCst);
        let handle = ResourceHandle::new(format!("handle-{ordinal}"));
        self.open
            .lock()
            .expect("open resource lock poisoned")
            .insert(handle.as_str().to_owned(), Vec::new());
        Ok(handle)
    }

    async fn write(&self, handle: &ResourceHandle, bytes: &[u8]) -> Result<(), ResourceError> {
        let mut open = self.open.lock().expect("open resource lock poisoned");
        let buffer = open
            .get_mut(handle.as_str())
            .ok_or_else(|| ResourceError::UnknownHandle(handle.as_str().to_owned()))?;
        buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finalize(&self, handle: ResourceHandle) -> Result<ResourceRef, ResourceError> {
        let bytes = self
            .open
            .lock()
            .expect("open resource lock poisoned")
            .remove(handle.as_str())
            .ok_or_else(|| ResourceError::UnknownHandle(handle.as_str().to_owned()))?;
        let uri = format!("resource://{}", handle.as_str());
        let size = bytes.len() as u64;
        self.finalized
            .lock()
            .expect("finalized resource lock poisoned")
            .insert(uri.clone(), bytes);
        Ok(ResourceRef {
            uri,
            media_type: None,
            digest: None,
            version: None,
            size: Some(size),
            summary: None,
        })
    }

    async fn abort(&self, handle: ResourceHandle) -> Result<(), ResourceError> {
        self.open
            .lock()
            .expect("open resource lock poisoned")
            .remove(handle.as_str())
            .ok_or_else(|| ResourceError::UnknownHandle(handle.as_str().to_owned()))?;
        Ok(())
    }

    async fn resolve(&self, reference: &ResourceRef) -> Result<Vec<u8>, ResourceError> {
        self.finalized
            .lock()
            .expect("finalized resource lock poisoned")
            .get(&reference.uri)
            .cloned()
            .ok_or_else(|| ResourceError::UnresolvableReference(reference.uri.clone()))
    }
}

/// An in-memory [`LoopStore`] that folds every appended event through
/// `loopward_core::evolve`, mirroring the real store (see
/// `loopward-runtime`'s `controller/advance.rs` module doc for why a fake
/// that skips this is a correctness gap for this controller).
struct InMemoryLoopStore {
    loops: Mutex<HashMap<LoopId, StoredLoop>>,
    leases: Mutex<HashMap<LoopId, LoopLease>>,
    fencing_tokens: Mutex<HashMap<LoopId, u64>>,
    event_ids: SequentialIdGenerator,
}

impl InMemoryLoopStore {
    fn new() -> Self {
        Self {
            loops: Mutex::new(HashMap::new()),
            leases: Mutex::new(HashMap::new()),
            fencing_tokens: Mutex::new(HashMap::new()),
            event_ids: SequentialIdGenerator::new(),
        }
    }

    fn seed(&self, loop_id: LoopId, state: AggregateState) {
        self.loops.lock().expect("loops lock poisoned").insert(
            loop_id,
            StoredLoop {
                state,
                events: Vec::new(),
            },
        );
    }

    fn current_token(&self, loop_id: LoopId) -> u64 {
        *self
            .fencing_tokens
            .lock()
            .expect("fencing token lock poisoned")
            .get(&loop_id)
            .unwrap_or(&0)
    }
}

#[async_trait]
impl LoopStore for InMemoryLoopStore {
    async fn load(&self, loop_id: LoopId) -> Result<StoredLoop, StoreError> {
        self.loops
            .lock()
            .expect("loops lock poisoned")
            .get(&loop_id)
            .cloned()
            .ok_or(StoreError::LoopNotFound(loop_id))
    }

    async fn append(
        &self,
        loop_id: LoopId,
        expected_revision: u64,
        fencing_token: u64,
        events: Vec<LoopEvent>,
    ) -> Result<StoredLoop, StoreError> {
        let current_token = self.current_token(loop_id);
        if fencing_token != current_token {
            return Err(StoreError::StaleFencingToken {
                loop_id,
                given: fencing_token,
                current: current_token,
            });
        }

        let mut loops = self.loops.lock().expect("loops lock poisoned");
        let stored = loops
            .get_mut(&loop_id)
            .ok_or(StoreError::LoopNotFound(loop_id))?;
        let actual_revision = stored.state.execution.aggregate_revision;
        if actual_revision != expected_revision {
            return Err(StoreError::RevisionConflict {
                loop_id,
                expected: expected_revision,
                actual: actual_revision,
            });
        }

        let mut state = stored.state.clone();
        let starting_sequence = stored.events.len() as u64;
        for (offset, event) in events.into_iter().enumerate() {
            state = evolve(state, &event)
                .expect("test fake: an event decide() produced must apply to its own state");
            stored.events.push(RecordedEvent {
                event_id: self.event_ids.new_event_id(),
                sequence: starting_sequence + offset as u64 + 1,
                aggregate_revision: state.execution.aggregate_revision,
                schema_version: EVENT_SCHEMA_VERSION.to_owned(),
                recorded_at: Timestamp::UNIX_EPOCH,
                event,
            });
        }
        stored.state = state;
        Ok(stored.clone())
    }

    async fn commit_checkpoint(
        &self,
        request: CheckpointCommitRequest,
    ) -> Result<StoredLoop, StoreError> {
        let current_token = self.current_token(request.loop_id);
        if request.fencing_token != current_token {
            return Err(StoreError::StaleFencingToken {
                loop_id: request.loop_id,
                given: request.fencing_token,
                current: current_token,
            });
        }

        let mut loops = self.loops.lock().expect("loops lock poisoned");
        let existing = loops
            .get(&request.loop_id)
            .ok_or(StoreError::LoopNotFound(request.loop_id))?;

        if existing.state.execution.aggregate_revision != request.expected_revision {
            return Err(StoreError::RevisionConflict {
                loop_id: request.loop_id,
                expected: request.expected_revision,
                actual: existing.state.execution.aggregate_revision,
            });
        }
        if existing.state.execution.checkpoint_version != request.expected_checkpoint_version {
            return Err(StoreError::CheckpointVersionConflict {
                loop_id: request.loop_id,
                expected: request.expected_checkpoint_version,
                actual: existing.state.execution.checkpoint_version,
            });
        }

        let stored = loops
            .get_mut(&request.loop_id)
            .expect("checked present above");
        for event in request.events {
            stored.events.push(RecordedEvent {
                event_id: self.event_ids.new_event_id(),
                sequence: stored.events.len() as u64 + 1,
                aggregate_revision: request.state.execution.aggregate_revision,
                schema_version: EVENT_SCHEMA_VERSION.to_owned(),
                recorded_at: Timestamp::UNIX_EPOCH,
                event,
            });
        }
        stored.state = request.state;
        stored.state.checkpoint = request.checkpoint;
        Ok(stored.clone())
    }

    async fn acquire_lease(&self, request: AcquireLease) -> Result<LoopLease, StoreError> {
        if !self
            .loops
            .lock()
            .expect("loops lock poisoned")
            .contains_key(&request.loop_id)
        {
            return Err(StoreError::LoopNotFound(request.loop_id));
        }
        let mut leases = self.leases.lock().expect("leases lock poisoned");
        let mut tokens = self
            .fencing_tokens
            .lock()
            .expect("fencing token lock poisoned");
        let next_token = tokens.get(&request.loop_id).copied().unwrap_or(0) + 1;
        tokens.insert(request.loop_id, next_token);

        let now = Timestamp::UNIX_EPOCH;
        let lease = LoopLease {
            loop_id: request.loop_id,
            holder_id: request.holder_id,
            fencing_token: next_token,
            acquired_at: now,
            expires_at: now + request.ttl,
        };
        leases.insert(request.loop_id, lease);
        Ok(lease)
    }

    async fn renew_lease(&self, lease: &LoopLease) -> Result<LoopLease, StoreError> {
        Ok(*lease)
    }
}

/// A [`WorkUnitRunner`] that always succeeds, recording every request's
/// propagated `trace_context` so the test can assert it was populated.
struct FakeRunner {
    start_calls: AtomicUsize,
    last_traceparent: Mutex<Option<String>>,
}

impl FakeRunner {
    fn new() -> Self {
        Self {
            start_calls: AtomicUsize::new(0),
            last_traceparent: Mutex::new(None),
        }
    }

    fn last_traceparent(&self) -> Option<String> {
        self.last_traceparent
            .lock()
            .expect("traceparent lock poisoned")
            .clone()
    }
}

#[async_trait]
impl WorkUnitRunner for FakeRunner {
    async fn capabilities(&self) -> Result<RunnerCapabilities, RunnerError> {
        Ok(RunnerCapabilities {
            protocol_versions: vec![loopward_api::PROTOCOL_VERSION.to_owned()],
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
                supports_resource_fetch: false,
                supports_structured_handoff: false,
                supports_resume: false,
            },
        })
    }

    async fn start(&self, request: WorkUnitRequest) -> Result<RunHandle, RunnerError> {
        self.start_calls.fetch_add(1, Ordering::SeqCst);
        self.last_traceparent
            .lock()
            .expect("traceparent lock poisoned")
            .clone_from(&request.trace_context.traceparent);
        Ok(RunHandle {
            attempt_id: request.attempt_id,
            external_execution_ref: Some(loopward_api::resource::OpaqueRef::new("fake-run")),
        })
    }

    async fn reconcile(&self, _handle: &RunHandle) -> Result<ReconcileOutcome, RunnerError> {
        Ok(ReconcileOutcome::Unknown)
    }

    async fn status(&self, _handle: &RunHandle) -> Result<RunStatus, RunnerError> {
        Ok(RunStatus {
            state: RunState::Completed,
            detail: None,
            observed_at: None,
        })
    }

    async fn events(
        &self,
        _handle: &RunHandle,
        _cursor: Option<loopward_api::event::EventCursor>,
    ) -> Result<RunnerEventPage, RunnerError> {
        Ok(RunnerEventPage {
            events: Vec::new(),
            next_cursor: None,
        })
    }

    async fn suspend(
        &self,
        _handle: &RunHandle,
        _reason: SuspensionReason,
    ) -> Result<(), RunnerError> {
        Ok(())
    }

    async fn cancel(
        &self,
        _handle: &RunHandle,
        _reason: CancellationReason,
    ) -> Result<(), RunnerError> {
        Ok(())
    }

    async fn collect(&self, handle: &RunHandle) -> Result<WorkUnitResult, RunnerError> {
        Ok(WorkUnitResult {
            protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
            attempt_id: handle.attempt_id,
            outcome: WorkUnitOutcome::Produced,
            outputs: Vec::new(),
            state_proposals: Vec::new(),
            observation_proposals: Vec::new(),
            decision_proposals: Vec::new(),
            verification_evidence: Vec::new(),
            unresolved: Vec::new(),
            recommended_continuation: None,
            usage: loopward_api::work_unit::UsageReport::default(),
            runner_trace_refs: Vec::new(),
        })
    }
}

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

struct NoCandidates;

impl ContextCandidateProvider for NoCandidates {
    fn candidates(&self, _work_unit: &WorkUnit, _checkpoint: &Checkpoint) -> Vec<ContextCandidate> {
        Vec::new()
    }
}

fn test_work_unit(id: WorkUnitId) -> WorkUnit {
    WorkUnit {
        id,
        kind: WorkUnitKind::new("test.work-unit"),
        objective: "do the test thing".to_owned(),
        status: WorkUnitStatus::Pending,
        inputs: Vec::new(),
        constraints: Vec::<ConstraintRef>::new(),
        output_contract: OutputContract {
            schema: "loopward.test-output/v0.1".to_owned(),
        },
        verification_plan: VerificationPlanRef {
            reference: "test-plan".to_owned(),
            version: 1,
        },
        runner: RunnerRef {
            name: "fake".to_owned(),
            profile: None,
        },
        budget: ResourceBudget::default(),
        retry_policy: RetryPolicy::default(),
    }
}

fn genesis_state(loop_id: LoopId, work_unit: WorkUnit) -> AggregateState {
    AggregateState {
        execution: LoopExecution {
            id: loop_id,
            kind: LoopKind::new("domain.example"),
            status: LoopStatus::Active,
            phase: PhaseRef::new("implementation"),
            aggregate_revision: 0,
            checkpoint_version: 0,
            active_episode: None,
            active_work_unit: Some(work_unit.id),
            budget: ResourceBudget::default(),
        },
        episode: None,
        session: None,
        work_unit: Some(work_unit),
        attempt: None,
        checkpoint: Checkpoint {
            id: CheckpointId::new(),
            loop_id,
            version: 0,
            aggregate_revision: 0,
            parent: None,
            episode_id: None,
            integrity_digest: None,
        },
    }
}

fn generous_budget() -> loopward_api::context::ContextBudget {
    loopward_api::context::ContextBudget {
        model_window_tokens: None,
        soft_input_limit_tokens: 100_000,
        hard_input_limit_tokens: 200_000,
        reserved_output_tokens: 0,
        reserved_tool_tokens: 0,
        safety_margin_tokens: 0,
    }
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn two_episodes_produce_linked_traces_with_propagated_runner_context() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let telemetry = Arc::new(OtelTelemetrySink::new(&provider));

    let ids = Arc::new(SequentialIdGenerator::new());
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(WorkUnitId::new());
    let store = Arc::new(InMemoryLoopStore::new());
    store.seed(loop_id, genesis_state(loop_id, work_unit));

    let ports = RuntimePorts {
        store,
        resources: Arc::new(InMemoryResourceBroker::new()),
        clock: Arc::new(FixedClock(Timestamp::UNIX_EPOCH)),
        ids,
        telemetry: telemetry as Arc<dyn loopward_observability::TelemetrySink>,
    };

    let runner = Arc::new(FakeRunner::new());
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget())
    .with_candidate_provider(Arc::new(NoCandidates));

    // Episode 1: Execution.
    let outcome_one = controller
        .advance(loop_id)
        .await
        .expect("execution episode advances");
    let loopward_runtime::OperationOutcome::EpisodeCompleted {
        checkpoint_version: checkpoint_one,
        ..
    } = outcome_one
    else {
        panic!("expected the execution episode to complete, got {outcome_one:?}");
    };

    // Episode 2: Verification.
    let outcome_two = controller
        .advance(loop_id)
        .await
        .expect("verification episode advances");
    let loopward_runtime::OperationOutcome::EpisodeCompleted {
        checkpoint_version: checkpoint_two,
        ..
    } = outcome_two
    else {
        panic!("expected the verification episode to complete, got {outcome_two:?}");
    };
    assert!(checkpoint_two > checkpoint_one);

    provider.force_flush().expect("flush succeeds");
    let spans = exporter
        .get_finished_spans()
        .expect("exporter is not shut down");

    let episode_spans: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "loopward.episode")
        .collect();
    assert_eq!(
        episode_spans.len(),
        2,
        "expected exactly one `loopward.episode` root span per Episode"
    );

    // Two separate root traces: neither Episode span is any other's parent.
    let trace_ids: std::collections::HashSet<_> = episode_spans
        .iter()
        .map(|span| span.span_context.trace_id())
        .collect();
    assert_eq!(trace_ids.len(), 2, "expected two distinct root traces");
    for span in &episode_spans {
        assert!(
            span.parent_span_id == opentelemetry::trace::SpanId::INVALID
                || span.parent_span_is_remote,
            "an Episode span must be its own trace root, not a child span"
        );
    }

    // Order episodes by their recorded `loopward.checkpoint.version` end
    // attribute (recorded once the Episode completes) so "Episode 1" and
    // "Episode 2" below refer to the Execution and Verification Episodes
    // respectively, regardless of export order.
    // The Episode span records `loopward.checkpoint.version` twice: once at
    // start (the base version it hydrated from) and once at end (the
    // version it committed) — the *last* occurrence is always the end
    // value, since `OtelTelemetrySink::end_span` appends end attributes
    // after whatever the span already carried from its start attributes.
    let checkpoint_version_of = |span: &opentelemetry_sdk::trace::SpanData| -> u64 {
        span.attributes
            .iter()
            .rev()
            .find(|kv| kv.key.as_str() == "loopward.checkpoint.version")
            .and_then(|kv| kv.value.as_str().parse::<u64>().ok())
            .expect("episode span records a checkpoint version")
    };
    let (episode_one, episode_two) =
        if checkpoint_version_of(episode_spans[0]) < checkpoint_version_of(episode_spans[1]) {
            (episode_spans[0], episode_spans[1])
        } else {
            (episode_spans[1], episode_spans[0])
        };

    // Both record the correct Loop ID and their own Checkpoint version.
    for (span, expected_checkpoint) in
        [(episode_one, checkpoint_one), (episode_two, checkpoint_two)]
    {
        let loop_id_attr = span
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == "loopward.loop.id")
            .map(|kv| kv.value.as_str().to_string());
        assert_eq!(loop_id_attr.as_deref(), Some(loop_id.to_string().as_str()));
        assert_eq!(checkpoint_version_of(span), expected_checkpoint);
    }

    // Episode 2 carries a `continuation_of` link to Episode 1's trace.
    assert_eq!(
        episode_two.links.iter().count(),
        1,
        "the second Episode's root span must carry exactly one link"
    );
    let link = episode_two.links.iter().next().expect("one link present");
    let link_kind = link
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "loopward.link.kind")
        .map(|kv| kv.value.as_str().to_string());
    assert_eq!(link_kind.as_deref(), Some("continuation_of"));
    assert_eq!(
        link.span_context.trace_id(),
        episode_one.span_context.trace_id(),
        "the continuation link must reference Episode 1's actual trace ID"
    );

    // The runner boundary span carries propagated W3C context: the fake
    // runner observed a real `traceparent`, and it names the execute span's
    // own (child) trace ID — which is Episode 1's trace ID, since the
    // runner call happens during the Execution Episode.
    let traceparent = runner
        .last_traceparent()
        .expect("the runner observed a propagated traceparent");
    assert!(traceparent.contains(&format!("{:032x}", episode_one.span_context.trace_id())));

    let execute_spans: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "loopward.work_unit.execute")
        .collect();
    assert_eq!(execute_spans.len(), 1);
    assert_eq!(
        execute_spans[0].span_context.trace_id(),
        episode_one.span_context.trace_id(),
        "the runner boundary span must nest under the Execution Episode's trace"
    );
}
