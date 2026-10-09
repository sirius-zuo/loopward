//! Episode advance and rotation-boundary tests for `LoopController` (Task 9).
//!
//! Every fake below implements one real port or protocol trait end to end —
//! the same pattern `port_contract.rs` establishes — plus a
//! [`InMemoryLoopStore::append`] that (unlike that file's simpler fake, and
//! unlike the real `SqliteLoopStore`; see `controller/advance.rs`'s module
//! documentation) actually folds events through `loopward_core::evolve`, so
//! `store.load()` between test assertions reflects a fully-projected state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::context::{
    ContextCapabilities, ContextClass, ContextControlLevel, ContextPriority, Fidelity, Sensitivity,
    Validity,
};
use loopward_api::event::EventCursor;
use loopward_api::ids::{
    AttemptId, CheckpointId, EpisodeId, EventId, LoopId, OperationId, SessionId, Timestamp,
    WorkUnitId,
};
use loopward_api::loop_service::{
    CommandMetadata, CreateLoopRequest, Goal, LoopKind, LoopPolicies, LoopService, RunCondition,
    RunLimits,
};
use loopward_api::resource::{
    ConstraintRef, OutputContract, PhaseRef, ResourceRef, RunnerRef, SchemaRef, VerificationPlanRef,
};
use loopward_api::runner::{
    CancellationReason, ReconcileOutcome, RunHandle, RunState, RunStatus, RunnerCapabilities,
    RunnerError, RunnerErrorCode, RunnerEventPage, SuspensionReason, WorkUnitRunner,
};
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{
    ResourceBudget, RetryPolicy, WorkUnit, WorkUnitKind, WorkUnitOutcome, WorkUnitRequest,
    WorkUnitResult, WorkUnitStatus,
};
use loopward_core::{
    AggregateState, Checkpoint, DomainError, EffectivePolicy, Episode, EpisodeKind, EpisodeStatus,
    LifecycleCommand, LoopEvent, LoopExecution, LoopStatus, RuntimeSession, SessionStatus, decide,
    evolve,
};
use loopward_observability::NoopTelemetrySink;
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
    ContextCandidateProvider, ControllerError, LoopController, OperationOutcome,
    RuntimeLoopService, VerificationPlanResolver, VerificationPlanTemplate,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixed clock
// ---------------------------------------------------------------------------

struct FixedClock(Timestamp);

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Deterministic ID generator
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// In-memory resource broker (with a test-only overwrite seam)
// ---------------------------------------------------------------------------

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

    /// Test-only seam simulating content changing underneath a run: replaces
    /// the bytes behind an already-finalized `uri`.
    fn overwrite(&self, uri: &str, bytes: Vec<u8>) {
        self.finalized
            .lock()
            .expect("finalized resource lock poisoned")
            .insert(uri.to_owned(), bytes);
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

// ---------------------------------------------------------------------------
// In-memory loop store: correctly folds events (see module doc for why this
// matters and how `LoopController` stays correct even against a store that
// does not).
// ---------------------------------------------------------------------------

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

        let Some(existing) = loops.get(&request.loop_id) else {
            if request.expected_revision != 0 || request.expected_checkpoint_version != 0 {
                return Err(StoreError::LoopNotFound(request.loop_id));
            }
            let mut events = Vec::new();
            for (ordinal, event) in request.events.into_iter().enumerate() {
                let position = ordinal as u64 + 1;
                events.push(RecordedEvent {
                    event_id: self.event_ids.new_event_id(),
                    sequence: position,
                    aggregate_revision: position,
                    schema_version: EVENT_SCHEMA_VERSION.to_owned(),
                    recorded_at: Timestamp::UNIX_EPOCH,
                    event,
                });
            }
            let mut state = request.state;
            state.checkpoint = request.checkpoint;
            let stored_loop = StoredLoop { state, events };
            loops.insert(request.loop_id, stored_loop.clone());
            return Ok(stored_loop);
        };

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
        // Mirrors a real store: `loop_leases` is a foreign key onto `loops`,
        // so an uncreated Loop cannot be leased yet — only genesis-committed
        // (fenced by the token `0` every uncreated Loop presents).
        if !self
            .loops
            .lock()
            .expect("loops lock poisoned")
            .contains_key(&request.loop_id)
        {
            return Err(StoreError::LoopNotFound(request.loop_id));
        }

        let mut leases = self.leases.lock().expect("leases lock poisoned");
        if let Some(existing) = leases.get(&request.loop_id)
            && existing.holder_id != request.holder_id
        {
            return Err(StoreError::LeaseConflict(request.loop_id));
        }

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
        let mut leases = self.leases.lock().expect("leases lock poisoned");
        let held = leases
            .get(&lease.loop_id)
            .ok_or(StoreError::LeaseNotHeld(lease.loop_id))?;
        if held.fencing_token != lease.fencing_token {
            return Err(StoreError::StaleFencingToken {
                loop_id: lease.loop_id,
                given: lease.fencing_token,
                current: held.fencing_token,
            });
        }
        let renewed = LoopLease {
            expires_at: held.expires_at + time::Duration::seconds(60),
            ..*held
        };
        leases.insert(lease.loop_id, renewed);
        Ok(renewed)
    }

    async fn release_lease(&self, lease: &LoopLease) -> Result<(), StoreError> {
        // Only the current lease is released; `fencing_tokens` is kept
        // separately, so the next acquisition still increments the token.
        let mut leases = self.leases.lock().expect("leases lock poisoned");
        if leases.get(&lease.loop_id).is_some_and(|held| {
            held.holder_id == lease.holder_id && held.fencing_token == lease.fencing_token
        }) {
            leases.remove(&lease.loop_id);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fake WorkUnitRunner
// ---------------------------------------------------------------------------

struct FakeRunner {
    start_calls: AtomicUsize,
    collect_calls: AtomicUsize,
    fail_start: bool,
    fail_collect: bool,
    mutate_on_collect: Option<(String, Vec<u8>)>,
    broker: Arc<InMemoryResourceBroker>,
}

impl FakeRunner {
    fn new(broker: Arc<InMemoryResourceBroker>) -> Self {
        Self {
            start_calls: AtomicUsize::new(0),
            collect_calls: AtomicUsize::new(0),
            fail_start: false,
            fail_collect: false,
            mutate_on_collect: None,
            broker,
        }
    }

    fn failing_start(broker: Arc<InMemoryResourceBroker>) -> Self {
        Self {
            fail_start: true,
            ..Self::new(broker)
        }
    }

    fn failing_collect(broker: Arc<InMemoryResourceBroker>) -> Self {
        Self {
            fail_collect: true,
            ..Self::new(broker)
        }
    }

    fn mutating_pinned_resource_on_collect(
        broker: Arc<InMemoryResourceBroker>,
        uri: String,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            mutate_on_collect: Some((uri, bytes)),
            ..Self::new(broker)
        }
    }

    fn start_calls(&self) -> usize {
        self.start_calls.load(Ordering::SeqCst)
    }

    fn collect_calls(&self) -> usize {
        self.collect_calls.load(Ordering::SeqCst)
    }
}

fn fake_runner_error() -> RunnerError {
    RunnerError {
        code: RunnerErrorCode::ExecutionFailed,
        message: "the fake runner was told to fail".to_owned(),
        retryable: false,
        trace_ref: None,
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
        if self.fail_start {
            return Err(fake_runner_error());
        }
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
        _cursor: Option<EventCursor>,
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
        self.collect_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_collect {
            return Err(fake_runner_error());
        }
        if let Some((uri, bytes)) = &self.mutate_on_collect {
            self.broker.overwrite(uri, bytes.clone());
        }
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

// ---------------------------------------------------------------------------
// Fake verification plan resolver: one required Schema gate with a default
// (trivially satisfied) config, so every collected result is accepted.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Fake candidate provider: offers one required pinned candidate.
// ---------------------------------------------------------------------------

struct PinnedCandidateProvider {
    resource: ResourceRef,
}

impl ContextCandidateProvider for PinnedCandidateProvider {
    fn candidates(&self, _work_unit: &WorkUnit, _checkpoint: &Checkpoint) -> Vec<ContextCandidate> {
        vec![ContextCandidate {
            estimated_tokens: Some(10),
            selection_reason: "pinned test constraint".to_owned(),
            sensitivity: Sensitivity::Internal,
            validity: Validity::Current,
            fidelity: Fidelity::Verbatim,
            ..ContextCandidate::new(
                "pinned-constraint",
                "constraint",
                self.resource.clone(),
                ContextClass::Pinned,
                ContextPriority::Required,
            )
        }]
    }
}

/// A candidate provider offering one required-current-work candidate with an
/// explicit token estimate, covering a Work Unit's declared input so its
/// token cost is exactly controllable (rather than depending on the
/// hydrator's byte-length heuristic).
struct FixedCostCandidateProvider {
    resource: ResourceRef,
    tokens: u64,
}

impl ContextCandidateProvider for FixedCostCandidateProvider {
    fn candidates(&self, _work_unit: &WorkUnit, _checkpoint: &Checkpoint) -> Vec<ContextCandidate> {
        vec![ContextCandidate {
            estimated_tokens: Some(self.tokens),
            selection_reason: "fixed-cost test input".to_owned(),
            ..ContextCandidate::new(
                "fixed-cost-input",
                "work-unit-input",
                self.resource.clone(),
                ContextClass::CurrentWork,
                ContextPriority::Required,
            )
        }]
    }
}

// ---------------------------------------------------------------------------
// Test fixture builders
// ---------------------------------------------------------------------------

fn test_work_unit(
    id: WorkUnitId,
    inputs: Vec<ResourceRef>,
    constraints: Vec<ConstraintRef>,
) -> WorkUnit {
    WorkUnit {
        id,
        kind: WorkUnitKind::new("test.work-unit"),
        objective: "do the test thing".to_owned(),
        status: WorkUnitStatus::Pending,
        inputs,
        constraints,
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

fn genesis_state(loop_id: LoopId, work_unit: Option<WorkUnit>) -> AggregateState {
    AggregateState {
        execution: LoopExecution {
            id: loop_id,
            kind: LoopKind::new("domain.example"),
            status: LoopStatus::Active,
            phase: PhaseRef::new("implementation"),
            aggregate_revision: 0,
            checkpoint_version: 0,
            active_episode: None,
            active_work_unit: work_unit.as_ref().map(|work_unit| work_unit.id),
            episode_count: 0,
            budget: ResourceBudget::default(),
        },
        episode: None,
        session: None,
        work_unit,
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

/// Builds a fresh [`RuntimePorts`] with one Loop seeded at genesis, carrying
/// `work_unit` if given, over a fresh [`InMemoryResourceBroker`].
fn build_ports(work_unit: Option<WorkUnit>) -> (RuntimePorts, LoopId, Arc<InMemoryResourceBroker>) {
    build_ports_with_broker(work_unit, Arc::new(InMemoryResourceBroker::new()))
}

/// As [`build_ports`], but over an already-constructed broker — for tests
/// that need to populate resource content (to learn its assigned URI) before
/// building the Work Unit that references it.
fn build_ports_with_broker(
    work_unit: Option<WorkUnit>,
    broker: Arc<InMemoryResourceBroker>,
) -> (RuntimePorts, LoopId, Arc<InMemoryResourceBroker>) {
    let ids = Arc::new(SequentialIdGenerator::new());
    let loop_id = ids.new_loop_id();

    let store = Arc::new(InMemoryLoopStore::new());
    store.seed(loop_id, genesis_state(loop_id, work_unit));

    let ports = RuntimePorts {
        store,
        resources: Arc::clone(&broker) as Arc<dyn ResourceBroker>,
        clock: Arc::new(FixedClock(Timestamp::UNIX_EPOCH)),
        ids,
        telemetry: Arc::new(NoopTelemetrySink),
    };
    (ports, loop_id, broker)
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
// Episode tests
// ---------------------------------------------------------------------------

/// One `advance` call stops at one committed durable boundary: the Execution
/// Episode runs hydrate → session → attempt → runner → collect → drain →
/// commit in a single call and returns exactly one committed Checkpoint.
#[tokio::test]
async fn one_advance_call_stops_at_one_committed_boundary() {
    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, Vec::new(), Vec::new());
    let (ports, loop_id, broker) = build_ports(Some(work_unit));
    let runner = Arc::new(FakeRunner::new(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget());

    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    match outcome {
        OperationOutcome::EpisodeCompleted {
            checkpoint_version, ..
        } => assert_eq!(checkpoint_version, 1),
        other => panic!("expected EpisodeCompleted, got {other:?}"),
    }

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.checkpoint_version, 1);
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::AwaitingVerification)
    );
    assert_eq!(runner.start_calls(), 1);
    assert_eq!(runner.collect_calls(), 1);

    // Exactly one `checkpoint.committed` event landed in the journal, not
    // two: one `advance` call performs at most one durable Episode
    // transition (`docs/07-session-rotation.md` §14).
    let checkpoint_commits = stored
        .events
        .iter()
        .filter(|recorded| matches!(recorded.event, LoopEvent::CheckpointCommitted { .. }))
        .count();
    assert_eq!(checkpoint_commits, 1);
}

/// A fresh Session generation is created for the Execution Episode, and
/// verification uses a fresh Session of its own (a later, higher
/// generation) rather than reusing the execution generation.
#[tokio::test]
async fn fresh_session_generation_per_work_unit_and_for_verification() {
    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, Vec::new(), Vec::new());
    let (ports, loop_id, broker) = build_ports(Some(work_unit));
    let runner = Arc::new(FakeRunner::new(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget());

    controller
        .advance(loop_id)
        .await
        .expect("execution episode advances");
    let after_execution = ports.store.load(loop_id).await.expect("load succeeds");
    let execution_session = after_execution
        .state
        .session
        .clone()
        .expect("execution episode started a session");
    assert_eq!(execution_session.generation, 1);
    assert_eq!(execution_session.status, SessionStatus::Closed);

    controller
        .advance(loop_id)
        .await
        .expect("verification episode advances");
    let after_verification = ports.store.load(loop_id).await.expect("load succeeds");
    let verification_session = after_verification
        .state
        .session
        .expect("verification episode started a session");
    assert_eq!(verification_session.generation, 2);
    assert_ne!(verification_session.id, execution_session.id);
    assert_eq!(
        after_verification.state.work_unit.map(|wu| wu.status),
        Some(WorkUnitStatus::Accepted)
    );
}

/// A closed Runtime Session generation is never reopened: the reducer
/// rejects both restarting the exact same generation and reusing its
/// identity, driven directly against `loopward_core::decide` (no controller
/// needed — this is a property of the pure reducer itself).
#[test]
fn closed_session_generation_never_reopens() {
    let loop_id = LoopId::new();
    let episode_id = EpisodeId::new();
    let closed_session_id = SessionId::new();

    let state = AggregateState {
        execution: LoopExecution {
            id: loop_id,
            kind: LoopKind::new("domain.example"),
            status: LoopStatus::Active,
            phase: PhaseRef::new("implementation"),
            aggregate_revision: 5,
            checkpoint_version: 1,
            active_episode: Some(episode_id),
            active_work_unit: None,
            episode_count: 0,
            budget: ResourceBudget::default(),
        },
        episode: Some(Episode {
            id: episode_id,
            loop_id,
            kind: EpisodeKind::Execution,
            status: EpisodeStatus::Hydrating,
            base_checkpoint_version: 1,
            terminal_checkpoint_version: None,
            active_session: None,
            hydrated_manifest: Some(ResourceRef {
                uri: "resource://boot-package".to_owned(),
                media_type: None,
                digest: None,
                version: None,
                size: None,
                summary: None,
            }),
            trace_id: None,
        }),
        session: Some(RuntimeSession {
            id: closed_session_id,
            loop_id,
            episode_id,
            generation: 2,
            status: SessionStatus::Closed,
            runner: RunnerRef {
                name: "fake".to_owned(),
                profile: None,
            },
            context_budget: None,
            boot_manifest_ref: ResourceRef {
                uri: "resource://boot-package".to_owned(),
                media_type: None,
                digest: None,
                version: None,
                size: None,
                summary: None,
            },
            external_session_ref: None,
            rotation_intent: None,
        }),
        work_unit: None,
        attempt: None,
        checkpoint: Checkpoint {
            id: CheckpointId::new(),
            loop_id,
            version: 1,
            aggregate_revision: 5,
            parent: None,
            episode_id: None,
            integrity_digest: None,
        },
    };

    // Restarting the exact same closed generation identity is rejected.
    let same_identity = decide(
        &state,
        &LifecycleCommand::StartSession {
            session_id: closed_session_id,
            generation: 3,
        },
        &EffectivePolicy::default(),
    );
    assert!(matches!(
        same_identity,
        Err(DomainError::InvalidTransition { .. })
    ));

    // A new identity at a generation that does not strictly advance is
    // rejected too.
    let stale_generation = decide(
        &state,
        &LifecycleCommand::StartSession {
            session_id: SessionId::new(),
            generation: 2,
        },
        &EffectivePolicy::default(),
    );
    assert!(matches!(
        stale_generation,
        Err(DomainError::InvariantViolation {
            invariant: "session.generation_strictly_increases",
            ..
        })
    ));
}

/// A soft context budget signal recommends rotation but does not force
/// anything: the Episode still completes normally, recording
/// `SoftContextBudget` as the rotation reason instead of the default
/// `WorkUnitBoundary`.
#[tokio::test]
async fn soft_budget_rotates_at_the_next_safe_boundary() {
    let broker = Arc::new(InMemoryResourceBroker::new());
    let handle = broker
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    broker
        .write(&handle, b"some input content")
        .await
        .expect("write succeeds");
    let input_ref = broker.finalize(handle).await.expect("finalize succeeds");

    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, vec![input_ref.clone()], Vec::new());
    let (ports, loop_id, broker) = build_ports_with_broker(Some(work_unit), broker);

    let runner = Arc::new(FakeRunner::new(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_candidate_provider(Arc::new(FixedCostCandidateProvider {
        resource: input_ref,
        tokens: 150,
    }))
    .with_default_budget(loopward_api::context::ContextBudget {
        model_window_tokens: None,
        soft_input_limit_tokens: 100,
        hard_input_limit_tokens: 100_000,
        reserved_output_tokens: 0,
        reserved_tool_tokens: 0,
        safety_margin_tokens: 0,
    });

    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    assert!(matches!(outcome, OperationOutcome::EpisodeCompleted { .. }));

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    let rotation_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::RotationRequested { reason, .. } => Some(*reason),
            _ => None,
        });
    // `RotationPolicy::PRECEDENCE` ranks `WorkUnitBoundary` (mandatory)
    // strictly above `SoftContextBudget` (advisory) — a soft budget signal
    // rides along as a contributing signal but never displaces the primary
    // reason a mandatory boundary already supplies. This Attempt just
    // finished (a genuine Work Unit boundary) at the same time the soft
    // budget was crossed, so the durably recorded reason is
    // `WorkUnitBoundary`, not `SoftContextBudget` — proving the controller
    // actually defers to `RotationPolicy::evaluate`'s precedence rather than
    // a hand-rolled shortcut that could disagree with it.
    assert_eq!(
        rotation_reason,
        Some(loopward_core::RotationReason::WorkUnitBoundary)
    );
    assert_eq!(
        runner.start_calls(),
        1,
        "soft budget still admits the attempt"
    );
}

/// A hard context budget stops admission before any model work starts: no
/// Session, no Attempt, no runner call, and the Loop suspends with a
/// committed Checkpoint recording the best-known durable state.
#[tokio::test]
async fn hard_budget_stops_new_model_work_and_initiates_drain() {
    let broker = Arc::new(InMemoryResourceBroker::new());
    let seed_handle = broker
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    broker
        .write(&seed_handle, b"some input content")
        .await
        .expect("write succeeds");
    let input_ref = broker
        .finalize(seed_handle)
        .await
        .expect("finalize succeeds");

    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, vec![input_ref.clone()], Vec::new());
    let (ports, loop_id, broker) = build_ports_with_broker(Some(work_unit), broker);

    let runner = Arc::new(FakeRunner::new(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_candidate_provider(Arc::new(FixedCostCandidateProvider {
        resource: input_ref,
        tokens: 150,
    }))
    .with_default_budget(loopward_api::context::ContextBudget {
        model_window_tokens: None,
        soft_input_limit_tokens: 100,
        hard_input_limit_tokens: 150,
        reserved_output_tokens: 0,
        reserved_tool_tokens: 0,
        safety_margin_tokens: 0,
    });

    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    assert_eq!(outcome, OperationOutcome::Suspended);
    assert_eq!(
        runner.start_calls(),
        0,
        "the hard budget must stop admission before any model work starts"
    );

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    assert_eq!(stored.state.execution.checkpoint_version, 1);
    let suspended_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::LoopSuspended { reason } => Some(*reason),
            _ => None,
        });
    assert_eq!(suspended_reason, Some(SuspensionReason::BudgetExceeded));
}

/// A drained result cannot commit when the pinned digest it was hydrated
/// against no longer matches: content that changed underneath the run is
/// caught at drain time, and the Loop suspends without ever recording the
/// (possibly tainted) result.
#[tokio::test]
async fn drain_result_cannot_commit_when_pinned_digest_differs() {
    let broker = Arc::new(InMemoryResourceBroker::new());
    let handle = broker
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    broker
        .write(&handle, b"pinned-content-v1")
        .await
        .expect("write succeeds");
    let pinned_ref = broker.finalize(handle).await.expect("finalize succeeds");

    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(
        work_unit_id,
        Vec::new(),
        vec![ConstraintRef {
            reference: pinned_ref.uri.clone(),
        }],
    );
    let (ports, loop_id, broker) = build_ports_with_broker(Some(work_unit), broker);

    let runner = Arc::new(FakeRunner::mutating_pinned_resource_on_collect(
        Arc::clone(&broker),
        pinned_ref.uri.clone(),
        b"pinned-content-v2".to_vec(),
    ));
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_candidate_provider(Arc::new(PinnedCandidateProvider {
        resource: pinned_ref,
    }))
    .with_default_budget(generous_budget());

    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    assert_eq!(outcome, OperationOutcome::Suspended);
    assert_eq!(runner.start_calls(), 1);
    assert_eq!(runner.collect_calls(), 1);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    // The tainted result never became a durable fact.
    assert!(
        stored
            .state
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.result_ref.is_none())
    );

    let rotation_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::RotationRequested { reason, .. } => Some(*reason),
            _ => None,
        });
    assert_eq!(
        rotation_reason,
        Some(loopward_core::RotationReason::ContextIntegrityFailure)
    );
    let suspended_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::LoopSuspended { reason } => Some(*reason),
            _ => None,
        });
    assert_eq!(suspended_reason, Some(SuspensionReason::Recovery));
}

/// The *default* `ContextCandidateProvider` (no `with_candidate_provider`
/// call at all) already projects a Work Unit's declared constraints into
/// required pinned candidates, so the pinned-digest integrity check is live
/// out of the box rather than only when a caller happens to wire in a
/// special provider.
#[tokio::test]
async fn default_candidate_provider_makes_the_pinned_integrity_check_live() {
    let broker = Arc::new(InMemoryResourceBroker::new());
    let handle = broker
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    broker
        .write(&handle, b"pinned-content-v1")
        .await
        .expect("write succeeds");
    let pinned_ref = broker.finalize(handle).await.expect("finalize succeeds");

    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(
        work_unit_id,
        Vec::new(),
        vec![ConstraintRef {
            reference: pinned_ref.uri.clone(),
        }],
    );
    let (ports, loop_id, broker) = build_ports_with_broker(Some(work_unit), broker);

    let runner = Arc::new(FakeRunner::mutating_pinned_resource_on_collect(
        Arc::clone(&broker),
        pinned_ref.uri.clone(),
        b"pinned-content-v2".to_vec(),
    ));
    // No `.with_candidate_provider(...)` here: this is the controller's
    // out-of-the-box configuration.
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget());

    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    assert_eq!(outcome, OperationOutcome::Suspended);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    let rotation_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::RotationRequested { reason, .. } => Some(*reason),
            _ => None,
        });
    assert_eq!(
        rotation_reason,
        Some(loopward_core::RotationReason::ContextIntegrityFailure)
    );
}

// ---------------------------------------------------------------------------
// run_until and RuntimeLoopService
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_until_requires_at_least_one_finite_limit() {
    let (ports, loop_id, broker) = build_ports(None);
    let runner = Arc::new(FakeRunner::new(broker));
    let controller = LoopController::new(
        ports,
        runner as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    );

    let result = controller
        .run_until(loop_id, RunCondition::UntilTerminal, RunLimits::default())
        .await;
    assert!(matches!(result, Err(ControllerError::UnboundedRunUntil)));
}

#[tokio::test]
async fn run_until_stops_after_max_episodes() {
    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, Vec::new(), Vec::new());
    let (ports, loop_id, broker) = build_ports(Some(work_unit));
    let runner = Arc::new(FakeRunner::new(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports,
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget());

    let outcomes = controller
        .run_until(
            loop_id,
            RunCondition::UntilTerminal,
            RunLimits {
                max_episodes: Some(1),
                deadline: None,
                max_resource_cost: None,
            },
        )
        .await
        .expect("run_until succeeds");
    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        outcomes[0],
        OperationOutcome::EpisodeCompleted { .. }
    ));
}

/// A `max_resource_cost` that sets `work_unit_attempts` is a real, enforced
/// bound: with only one Attempt permitted, `run_until` stops after the
/// Execution Episode starts it, even though the Verification Episode
/// (`started_attempt: false`) would otherwise be free to run too.
#[tokio::test]
async fn run_until_stops_after_max_work_unit_attempts() {
    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, Vec::new(), Vec::new());
    let (ports, loop_id, broker) = build_ports(Some(work_unit));
    let runner = Arc::new(FakeRunner::new(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports,
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget());

    let outcomes = controller
        .run_until(
            loop_id,
            RunCondition::UntilTerminal,
            RunLimits {
                max_episodes: None,
                deadline: None,
                max_resource_cost: Some(loopward_api::loop_service::ResourceCost {
                    work_unit_attempts: Some(1),
                    estimated_tokens: None,
                }),
            },
        )
        .await
        .expect("run_until succeeds");
    // Exactly the one Execution Episode that started the bounded Attempt —
    // not the Verification Episode that would otherwise follow it.
    assert_eq!(outcomes.len(), 1);
    assert_eq!(runner.start_calls(), 1);
}

/// A `max_resource_cost` that sets only `estimated_tokens` — a limit this
/// controller has no port to account for yet — must not be accepted as
/// satisfying "at least one limit": accepting it would leave the loop
/// genuinely unbounded despite `run_until` looking bounded to the caller.
#[tokio::test]
async fn run_until_rejects_a_resource_cost_that_sets_only_unenforced_fields() {
    let (ports, loop_id, broker) = build_ports(None);
    let runner = Arc::new(FakeRunner::new(broker));
    let controller = LoopController::new(
        ports,
        runner as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    );

    let result = controller
        .run_until(
            loop_id,
            RunCondition::UntilTerminal,
            RunLimits {
                max_episodes: None,
                deadline: None,
                max_resource_cost: Some(loopward_api::loop_service::ResourceCost {
                    work_unit_attempts: None,
                    estimated_tokens: Some(1_000),
                }),
            },
        )
        .await;
    assert!(matches!(result, Err(ControllerError::UnboundedRunUntil)));
}

#[tokio::test]
async fn runtime_loop_service_creates_and_advances_a_loop() {
    let (ports, _seeded_loop_id, broker) = build_ports(None);
    let runner = Arc::new(FakeRunner::new(broker));
    let controller = LoopController::new(
        ports,
        runner as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    );
    let service = RuntimeLoopService::new(controller);

    let create_request = CreateLoopRequest {
        metadata: CommandMetadata {
            request_id: "req-1".to_owned(),
            idempotency_key: "create-1".to_owned(),
            expected_revision: None,
            principal: None,
            trace_context: TraceContext::default(),
        },
        protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
        kind: LoopKind::new("domain.example"),
        goal: Goal {
            statement: "finish the test".to_owned(),
        },
        success_criteria: Vec::new(),
        pinned_constraints: Vec::new(),
        domain_schema: SchemaRef::new("loopward.test-domain/v0.1"),
        initial_domain_state: serde_json::Value::Null,
        policies: LoopPolicies::default(),
    };
    let handle = service
        .create(create_request.clone())
        .await
        .expect("create succeeds");
    assert_eq!(handle.aggregate_revision, 1);

    // Idempotent replay: the same idempotency key returns the same handle
    // rather than creating a second Loop.
    let replayed = service
        .create(create_request)
        .await
        .expect("replay succeeds");
    assert_eq!(replayed.loop_id, handle.loop_id);

    let status = service
        .status(handle.loop_id)
        .await
        .expect("status succeeds");
    assert_eq!(
        status.status,
        loopward_api::loop_service::LoopStatus::Active
    );
    assert_eq!(status.aggregate_revision, 1);

    // No Work Unit is assigned yet (Phase One's `create` carries none), so
    // `advance` reports it rather than silently doing nothing.
    let advance_result = service
        .advance(loopward_api::loop_service::AdvanceLoopCommand {
            metadata: CommandMetadata {
                request_id: "req-2".to_owned(),
                idempotency_key: "advance-1".to_owned(),
                expected_revision: Some(1),
                principal: None,
                trace_context: TraceContext::default(),
            },
            loop_id: handle.loop_id,
        })
        .await;
    assert!(advance_result.is_err());
}

// ---------------------------------------------------------------------------
// Mandatory closure after runner failure (`docs/07-session-rotation.md` §8)
// ---------------------------------------------------------------------------

/// A runner that fails to even accept the start call never reaches
/// `RecordRunnerStarted`; the Loop still suspends with a committed
/// Checkpoint rather than being left mid-Episode.
#[tokio::test]
async fn runner_start_failure_suspends_the_loop_and_commits() {
    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, Vec::new(), Vec::new());
    let (ports, loop_id, broker) = build_ports(Some(work_unit));
    let runner = Arc::new(FakeRunner::failing_start(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget());

    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    assert_eq!(outcome, OperationOutcome::Suspended);
    assert_eq!(runner.start_calls(), 1);
    assert_eq!(runner.collect_calls(), 0);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    assert_eq!(stored.state.execution.checkpoint_version, 1);
    let rotation_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::RotationRequested { reason, .. } => Some(*reason),
            _ => None,
        });
    assert_eq!(
        rotation_reason,
        Some(loopward_core::RotationReason::RunnerFailure)
    );
}

/// A runner that accepts the start call but fails to produce a collectible
/// result also suspends the Loop with a committed Checkpoint: the
/// generation cannot be drained, so it is abandoned rather than silently
/// retried.
#[tokio::test]
async fn runner_collect_failure_suspends_the_loop_and_commits() {
    let work_unit_id = WorkUnitId::new();
    let work_unit = test_work_unit(work_unit_id, Vec::new(), Vec::new());
    let (ports, loop_id, broker) = build_ports(Some(work_unit));
    let runner = Arc::new(FakeRunner::failing_collect(Arc::clone(&broker)));
    let controller = LoopController::new(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget());

    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    assert_eq!(outcome, OperationOutcome::Suspended);
    assert_eq!(runner.start_calls(), 1);
    assert_eq!(runner.collect_calls(), 1);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    assert_eq!(stored.state.execution.checkpoint_version, 1);
    let rotation_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::RotationRequested { reason, .. } => Some(*reason),
            _ => None,
        });
    assert_eq!(
        rotation_reason,
        Some(loopward_core::RotationReason::RunnerFailure)
    );
}

// ---------------------------------------------------------------------------
// Lease hand-back (`docs/03-durable-state-and-transactions.md` §7)
// ---------------------------------------------------------------------------

/// Builds a controller over `ports` with its own, freshly minted runtime
/// instance identity — as a separate `loopward` CLI process would.
fn another_runtime_instance(
    ports: &RuntimePorts,
    broker: &Arc<InMemoryResourceBroker>,
) -> LoopController {
    LoopController::new(
        ports.clone(),
        Arc::new(FakeRunner::new(Arc::clone(broker))) as Arc<dyn WorkUnitRunner>,
        Arc::new(AlwaysAcceptPlan),
    )
    .with_default_budget(generous_budget())
}

/// Every lease-acquiring `LoopController` operation hands its lease back
/// when it returns, so a *different* runtime instance can drive the same
/// Loop straight afterwards instead of being refused `LeaseConflict` until
/// the TTL runs out. (`InMemoryLoopStore` does not model TTL expiry at all,
/// so without the hand-back the second instance would be refused forever.)
#[tokio::test]
async fn a_different_runtime_instance_can_drive_a_loop_right_after_each_operation() {
    let work_unit = test_work_unit(WorkUnitId::new(), Vec::new(), Vec::new());
    let (ports, loop_id, broker) = build_ports(Some(work_unit));
    let first = another_runtime_instance(&ports, &broker);
    let second = another_runtime_instance(&ports, &broker);
    assert_ne!(first.holder_id(), second.holder_id());

    first
        .advance(loop_id)
        .await
        .expect("first instance advances");
    second
        .advance(loop_id)
        .await
        .expect("second instance advances right after the first");
    first
        .suspend(loop_id, SuspensionReason::OperatorRequest)
        .await
        .expect("first instance suspends right after the second");
    second
        .resume(loop_id)
        .await
        .expect("second instance resumes right after the first");
    first
        .cancel(loop_id)
        .await
        .expect("first instance cancels right after the second");

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Cancelled);
}

/// An operation that fails after acquiring its lease still hands it back:
/// the next runtime instance sees the same domain failure, not a lease
/// conflict.
#[tokio::test]
async fn a_failed_operation_still_hands_its_lease_back() {
    let (ports, loop_id, broker) = build_ports(None);
    let first = another_runtime_instance(&ports, &broker);
    let second = another_runtime_instance(&ports, &broker);

    let result = first.advance(loop_id).await;
    assert!(matches!(result, Err(ControllerError::NoWorkUnit(id)) if id == loop_id));

    let result = second.advance(loop_id).await;
    assert!(
        matches!(result, Err(ControllerError::NoWorkUnit(id)) if id == loop_id),
        "expected NoWorkUnit, got {result:?}"
    );
}

/// `RuntimeLoopService::create` and `resume` (which runs recovery under its
/// own lease acquisition) hand their leases back too.
#[tokio::test]
async fn runtime_loop_service_operations_hand_their_leases_back() {
    let (ports, _seeded_loop_id, broker) = build_ports(None);
    let service = RuntimeLoopService::new(another_runtime_instance(&ports, &broker));
    let other = another_runtime_instance(&ports, &broker);
    let metadata = |key: &str, expected_revision: Option<u64>| CommandMetadata {
        request_id: format!("req-{key}"),
        idempotency_key: key.to_owned(),
        expected_revision,
        principal: None,
        trace_context: TraceContext::default(),
    };

    let handle = service
        .create(CreateLoopRequest {
            metadata: metadata("create", None),
            protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
            kind: LoopKind::new("domain.example"),
            goal: Goal {
                statement: "hand leases back".to_owned(),
            },
            success_criteria: Vec::new(),
            pinned_constraints: Vec::new(),
            domain_schema: SchemaRef::new("loopward.test-domain/v0.1"),
            initial_domain_state: serde_json::Value::Null,
            policies: LoopPolicies::default(),
        })
        .await
        .expect("create succeeds");
    let loop_id = handle.loop_id;

    other
        .suspend(loop_id, SuspensionReason::OperatorRequest)
        .await
        .expect("another instance suspends right after create");

    service
        .resume(loopward_api::loop_service::ResumeLoopCommand {
            metadata: metadata("resume", None),
            loop_id,
        })
        .await
        .expect("service resumes right after another instance suspended");

    other
        .cancel(loop_id)
        .await
        .expect("another instance cancels right after the service resumed");
    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Cancelled);
}
