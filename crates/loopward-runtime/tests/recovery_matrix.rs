//! `RecoveryManager` crash-recovery tests (Task 10), covering the eight
//! failure points in `docs/10-mvp-and-evaluation.md` §8, plus the two scope
//! items Task 9's report flagged for this task: a Work Unit left stuck at
//! `Suspended` by a prior verification outcome, and a dangling unreconciled
//! Attempt left behind by a mid-Attempt suspend.
//!
//! Every fixture is built by driving `loopward_core::decide`/`evolve`
//! directly up to the exact durable fact a crash would have left behind —
//! never further — then seeding an `InMemoryLoopStore` with the result. This
//! mirrors `advance_episode.rs`'s `closed_session_generation_never_reopens`
//! pattern and, because `LoopStore::append`/`commit_checkpoint` each commit
//! one whole event batch atomically, is the only granularity a real crash
//! could actually interrupt at (see `recovery/reconcile.rs`'s module doc).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::context::{
    ContextBudget as ApiContextBudget, ContextCapabilities, ContextControlLevel,
};
use loopward_api::error::LoopwardErrorCode;
use loopward_api::event::EventCursor;
use loopward_api::ids::{
    AttemptId, CheckpointId, EpisodeId, EventId, LoopId, OperationId, SessionId, Timestamp,
    WorkUnitId,
};
use loopward_api::loop_service::{CommandMetadata, LoopKind, ResumeLoopCommand};
use loopward_api::resource::{
    OpaqueRef, OutputContract, PhaseRef, ResourceRef, RunnerRef, VerificationPlanRef,
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
    AggregateState, AttemptStatus, Checkpoint, EffectivePolicy, EpisodeKind, EpisodeStatus,
    LifecycleCommand, LoopEvent, LoopExecution, LoopStatus, RotationReason, SessionStatus,
    VerificationOutcome, decide, evolve,
};
use loopward_observability::NoopTelemetrySink;
use loopward_runtime::context::ContextHydrator;
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
    HydrationRequest, LoopController, OperationOutcome, RecoveryManager, RecoveryOutcome,
    RuntimeLoopService, VerificationPlanResolver, VerificationPlanTemplate,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixed clock / deterministic ids (same pattern as advance_episode.rs)
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

// ---------------------------------------------------------------------------
// In-memory resource broker, with a test-only removal seam for the resource
// integrity failure test.
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

    /// Test-only seam simulating a lost or corrupted resource: removes the
    /// finalized content behind `uri` so a later `resolve` fails.
    fn remove(&self, uri: &str) {
        self.finalized
            .lock()
            .expect("finalized resource lock poisoned")
            .remove(uri);
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

async fn finalize_bytes(resources: &Arc<dyn ResourceBroker>, bytes: Vec<u8>) -> ResourceRef {
    let handle = resources
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    resources
        .write(&handle, &bytes)
        .await
        .expect("write succeeds");
    resources.finalize(handle).await.expect("finalize succeeds")
}

// ---------------------------------------------------------------------------
// In-memory Loop store: identical shape to advance_episode.rs's fake — folds
// every event through `evolve` so `store.load()` mid-Episode is accurate.
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
}

// ---------------------------------------------------------------------------
// Configurable fake WorkUnitRunner: every crash-recovery scenario needs a
// different `reconcile`/`start`/`collect` behavior, so this fake is
// configured per test rather than specialized like advance_episode.rs's.
// ---------------------------------------------------------------------------

struct ConfigurableRunner {
    start_calls: AtomicUsize,
    collect_calls: AtomicUsize,
    reconcile_calls: AtomicUsize,
    reconcile_outcome: Mutex<Result<ReconcileOutcome, ()>>,
    fail_start: bool,
    collect_ok: bool,
}

impl ConfigurableRunner {
    fn new(_attempt_id: AttemptId, reconcile_outcome: Result<ReconcileOutcome, ()>) -> Self {
        Self {
            start_calls: AtomicUsize::new(0),
            collect_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            reconcile_outcome: Mutex::new(reconcile_outcome),
            fail_start: false,
            collect_ok: true,
        }
    }

    fn failing_to_collect(mut self) -> Self {
        self.collect_ok = false;
        self
    }

    fn failing_to_start(mut self) -> Self {
        self.fail_start = true;
        self
    }

    /// Changes what a later `reconcile` call reports, for tests that need
    /// the *same* runtime instance (lease holder) to observe two different
    /// runner answers across two separate `recover`/`resume` calls.
    fn set_reconcile_outcome(&self, outcome: Result<ReconcileOutcome, ()>) {
        *self
            .reconcile_outcome
            .lock()
            .expect("reconcile lock poisoned") = outcome;
    }

    fn start_calls(&self) -> usize {
        self.start_calls.load(Ordering::SeqCst)
    }

    fn collect_calls(&self) -> usize {
        self.collect_calls.load(Ordering::SeqCst)
    }

    fn reconcile_calls(&self) -> usize {
        self.reconcile_calls.load(Ordering::SeqCst)
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

fn empty_result(attempt_id: AttemptId) -> WorkUnitResult {
    WorkUnitResult {
        protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
        attempt_id,
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
    }
}

#[async_trait]
impl WorkUnitRunner for ConfigurableRunner {
    async fn capabilities(&self) -> Result<RunnerCapabilities, RunnerError> {
        Ok(RunnerCapabilities {
            protocol_versions: vec![loopward_api::PROTOCOL_VERSION.to_owned()],
            resumable: false,
            supports_reconcile: true,
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
            external_execution_ref: Some(OpaqueRef::new("fresh-start-ref")),
        })
    }

    async fn reconcile(&self, _handle: &RunHandle) -> Result<ReconcileOutcome, RunnerError> {
        self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
        match *self
            .reconcile_outcome
            .lock()
            .expect("reconcile lock poisoned")
        {
            Ok(outcome) => Ok(outcome),
            Err(()) => Err(fake_runner_error()),
        }
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
        if !self.collect_ok {
            return Err(fake_runner_error());
        }
        Ok(empty_result(handle.attempt_id))
    }
}

// ---------------------------------------------------------------------------
// Verification plan resolver: one required Schema gate, trivially satisfied.
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
// Fixture builders
// ---------------------------------------------------------------------------

fn test_work_unit(id: WorkUnitId) -> WorkUnit {
    WorkUnit {
        id,
        kind: WorkUnitKind::new("test.work-unit"),
        objective: "do the test thing".to_owned(),
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

/// Decides and folds `command` against `state`, exactly mirroring
/// `LoopController::apply` but driven directly against the pure reducer, for
/// hand-building a fixture up to an exact durable fact.
#[allow(clippy::needless_pass_by_value)]
fn step(state: &AggregateState, command: LifecycleCommand) -> AggregateState {
    let events = decide(state, &command, &EffectivePolicy::default())
        .unwrap_or_else(|error| panic!("decide({command:?}) failed: {error}"));
    let mut next = state.clone();
    for event in &events {
        next = evolve(next, event).expect("evolve succeeds for a just-decided event");
    }
    next
}

fn generous_budget() -> ApiContextBudget {
    ApiContextBudget {
        model_window_tokens: None,
        soft_input_limit_tokens: 100_000,
        hard_input_limit_tokens: 200_000,
        reserved_output_tokens: 0,
        reserved_tool_tokens: 0,
        safety_margin_tokens: 0,
    }
}

fn test_capabilities() -> RunnerCapabilities {
    RunnerCapabilities {
        protocol_versions: vec![loopward_api::PROTOCOL_VERSION.to_owned()],
        resumable: false,
        supports_reconcile: true,
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
    }
}

/// Builds a fresh set of ports and returns everything a fixture builder
/// needs, including the concrete store (`ports.store` is only visible as
/// `Arc<dyn LoopStore>`, which has no `seed`) so tests can seed a
/// hand-built [`AggregateState`] directly.
fn build_ports() -> (
    RuntimePorts,
    Arc<InMemoryResourceBroker>,
    Arc<SequentialIdGenerator>,
    Arc<InMemoryLoopStore>,
) {
    let ids = Arc::new(SequentialIdGenerator::new());
    let broker = Arc::new(InMemoryResourceBroker::new());
    let store = Arc::new(InMemoryLoopStore::new());
    let ports = RuntimePorts {
        store: Arc::clone(&store) as Arc<dyn LoopStore>,
        resources: Arc::clone(&broker) as Arc<dyn ResourceBroker>,
        clock: Arc::new(FixedClock(Timestamp::UNIX_EPOCH)),
        ids: Arc::clone(&ids) as Arc<dyn IdGenerator>,
        telemetry: Arc::new(NoopTelemetrySink),
    };
    (ports, broker, ids, store)
}

async fn hydrate_and_mark(
    ports: &RuntimePorts,
    state: &AggregateState,
    episode_id: EpisodeId,
    session_generation: u32,
    work_unit: &WorkUnit,
) -> AggregateState {
    let hydrator = ContextHydrator::new(Arc::clone(&ports.resources));
    let request = HydrationRequest {
        checkpoint: state.checkpoint.clone(),
        episode_id,
        session_generation,
        work_unit: work_unit.clone(),
        capabilities: test_capabilities(),
        budget: generous_budget(),
        context_rule_version: "loopward.context-rules/v0.1".to_owned(),
        candidates: Vec::new(),
    };
    let report = hydrator.hydrate(request).await.expect("hydrate succeeds");
    let bytes = serde_json::to_vec(&report.manifest).expect("manifest serializes");
    let manifest_ref = finalize_bytes(&ports.resources, bytes).await;
    step(
        state,
        LifecycleCommand::MarkHydrated {
            manifest: manifest_ref,
        },
    )
}

/// Builds an Execution Episode up to `attempt.prepared` (an Attempt exists,
/// `Prepared`, with no observed external reference): failure points 1 and 2
/// both start here — only the runner's own account of it differs.
async fn build_prepared_attempt(
    ports: &RuntimePorts,
    loop_id: LoopId,
    work_unit: &WorkUnit,
    episode_id: EpisodeId,
    session_id: SessionId,
    attempt_id: AttemptId,
) -> AggregateState {
    let state = genesis_state(loop_id, work_unit.clone());
    let state = step(&state, LifecycleCommand::StartEpisode { episode_id });
    let state = hydrate_and_mark(ports, &state, episode_id, 1, work_unit).await;
    let state = step(
        &state,
        LifecycleCommand::StartSession {
            session_id,
            generation: 1,
        },
    );
    step(
        &state,
        LifecycleCommand::PrepareAttempt {
            attempt_id,
            idempotency_key: format!("attempt:{attempt_id}"),
        },
    )
}

/// As [`build_prepared_attempt`], continued to `attempt.started` (`Running`,
/// with a durably observed external reference): failure points 3 and 4 both
/// start here.
async fn build_running_attempt(
    ports: &RuntimePorts,
    loop_id: LoopId,
    work_unit: &WorkUnit,
    episode_id: EpisodeId,
    session_id: SessionId,
    attempt_id: AttemptId,
) -> AggregateState {
    let state = build_prepared_attempt(
        ports, loop_id, work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    step(
        &state,
        LifecycleCommand::RecordRunnerStarted {
            external_ref: OpaqueRef::new("external-ref-1"),
        },
    )
}

/// Fully closes an Execution Episode (through `attempt.completed`,
/// `rotation.requested`, and a committed Checkpoint) starting from a
/// `Running` Attempt, then starts a fresh Verification Episode and hydrates
/// it — `session.started` has *not* been recorded yet. A crash exactly here
/// (`StartEpisode` and `MarkHydrated` committed, `StartSession` not) is a
/// real window: `advance` applies all three in one call.
#[allow(clippy::too_many_arguments)]
async fn build_verification_episode_hydrated(
    ports: &RuntimePorts,
    loop_id: LoopId,
    work_unit: &WorkUnit,
    execution_episode_id: EpisodeId,
    execution_session_id: SessionId,
    attempt_id: AttemptId,
    verification_episode_id: EpisodeId,
) -> AggregateState {
    let state = build_running_attempt(
        ports,
        loop_id,
        work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
    )
    .await;

    let result_bytes = serde_json::to_vec(&empty_result(attempt_id)).expect("result serializes");
    let result_ref = finalize_bytes(&ports.resources, result_bytes).await;
    let state = step(
        &state,
        LifecycleCommand::RecordResult { result: result_ref },
    );
    let state = step(
        &state,
        LifecycleCommand::RequestRotation {
            reason: RotationReason::WorkUnitBoundary,
        },
    );
    let checkpoint1 = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: Some(execution_episode_id),
        integrity_digest: None,
    };
    let state = step(
        &state,
        LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint1,
        },
    );
    assert_eq!(
        state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::AwaitingVerification),
        "the execution episode must close with the Work Unit awaiting verification"
    );

    let state = step(
        &state,
        LifecycleCommand::StartEpisode {
            episode_id: verification_episode_id,
        },
    );
    assert_eq!(
        state.episode.as_ref().map(|episode| episode.kind),
        Some(EpisodeKind::Verification)
    );
    hydrate_and_mark(ports, &state, verification_episode_id, 2, work_unit).await
}

/// As [`build_verification_episode_hydrated`], continued through
/// `session.started` — a `verification.completed` decision has *not* been
/// recorded yet. This is failure point 5's fixture directly.
#[allow(clippy::too_many_arguments)]
async fn build_verification_pending(
    ports: &RuntimePorts,
    loop_id: LoopId,
    work_unit: &WorkUnit,
    execution_episode_id: EpisodeId,
    execution_session_id: SessionId,
    attempt_id: AttemptId,
    verification_episode_id: EpisodeId,
    verification_session_id: SessionId,
) -> AggregateState {
    let state = build_verification_episode_hydrated(
        ports,
        loop_id,
        work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
        verification_episode_id,
    )
    .await;
    step(
        &state,
        LifecycleCommand::StartSession {
            session_id: verification_session_id,
            generation: 2,
        },
    )
}

fn controller(ports: RuntimePorts, runner: Arc<dyn WorkUnitRunner>) -> LoopController {
    LoopController::new(ports, runner, Arc::new(AlwaysAcceptPlan))
        .with_default_budget(generous_budget())
}

// ===========================================================================
// The eight failure points (`docs/10-mvp-and-evaluation.md` §8)
// ===========================================================================

/// 1. Before runner start: `attempt.prepared` is durable, but the runner was
/// never called at all. `reconcile` truthfully reports `NotStarted`, so
/// recovery starts it under the same idempotency key and finishes the
/// Episode. The runner *is* called (for the first time).
#[tokio::test]
async fn failure_point_1_before_runner_start() {
    let (ports, broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_prepared_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    assert_eq!(
        state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Prepared)
    );
    assert!(
        state
            .attempt
            .as_ref()
            .unwrap()
            .external_execution_ref
            .is_none()
    );
    store.seed(loop_id, state);
    let _ = &broker;

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::NotStarted),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::EpisodeRecovered { .. }));
    assert_eq!(runner.reconcile_calls(), 1);
    assert_eq!(
        runner.start_calls(),
        1,
        "the runner must be called: it never received this attempt"
    );
    assert_eq!(runner.collect_calls(), 1);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Active);
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::AwaitingVerification)
    );
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Succeeded)
    );
    assert!(
        stored.state.active_episode().is_none(),
        "the recovered episode must be terminal"
    );
}

/// 2. After external start, before handle persistence: locally identical to
/// failure point 1 (`attempt.prepared`, no reference), but the runner
/// truthfully reports it already started. Recovery must *not* call `start`
/// again — it reattaches (recording a synthesized reference) and collects.
#[tokio::test]
async fn failure_point_2_after_external_start_before_handle_persistence() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_prepared_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Running),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::EpisodeRecovered { .. }));
    assert_eq!(
        runner.start_calls(),
        0,
        "the runner already received this attempt; it must never be started a second time"
    );
    assert_eq!(runner.collect_calls(), 1);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    let attempt = stored
        .state
        .attempt
        .as_ref()
        .expect("attempt in projection");
    assert_eq!(attempt.status, AttemptStatus::Succeeded);
    assert!(
        attempt.external_execution_ref.is_some(),
        "reattaching must durably record a reference, synthesized if the runner gave none"
    );
}

/// 3. During execution: `attempt.started` is durable (`Running`, with a
/// reference); the runner is still genuinely executing. Recovery reattaches
/// (a no-op here, the reference already exists) but must not force a
/// terminal outcome: the Episode stays open for a later `recover` call.
#[tokio::test]
async fn failure_point_3_during_execution() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    store.seed(loop_id, state);

    let runner = Arc::new(
        ConfigurableRunner::new(attempt_id, Ok(ReconcileOutcome::Running)).failing_to_collect(),
    );
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert_eq!(
        outcome,
        RecoveryOutcome::Reattached,
        "still genuinely running: nothing may be forced"
    );
    assert_eq!(
        runner.collect_calls(),
        1,
        "the runner may be called again to check for completion"
    );

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Active);
    assert!(
        stored.state.active_episode().is_some(),
        "the episode must remain open, not forced terminal"
    );
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Running)
    );
}

/// 4. After result materialization: the runner reports the Attempt is
/// definitively `Completed`. Recovery collects and finishes the Episode.
#[tokio::test]
async fn failure_point_4_after_result_materialization() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Completed),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::EpisodeRecovered { .. }));
    assert_eq!(
        runner.collect_calls(),
        1,
        "the runner may be called again: it must be collected from"
    );

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::AwaitingVerification)
    );
    assert!(stored.state.active_episode().is_none());
}

/// 5. During verification: a fresh Verification Episode's Session is live,
/// but `verification.completed` was never recorded. Redoing Loopward's own
/// verification judgement is always safe (no external effect to repeat), so
/// recovery re-evaluates and finishes normally. The runner is never called.
#[tokio::test]
async fn failure_point_5_during_verification() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let execution_episode_id = ids.new_episode_id();
    let execution_session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();
    let verification_episode_id = ids.new_episode_id();
    let verification_session_id = ids.new_session_id();

    let state = build_verification_pending(
        &ports,
        loop_id,
        &work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
        verification_episode_id,
        verification_session_id,
    )
    .await;
    assert!(
        state
            .attempt
            .as_ref()
            .unwrap()
            .verification_outcome
            .is_none()
    );
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Unknown),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::EpisodeRecovered { .. }));
    assert_eq!(
        runner.reconcile_calls(),
        0,
        "verification never consults the runner"
    );
    assert_eq!(runner.start_calls(), 0);
    assert_eq!(runner.collect_calls(), 0);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Accepted)
    );
    assert!(stored.state.active_episode().is_none());
}

/// 6. Before Checkpoint transaction: `verification.completed` is durable,
/// but `rotation.requested` and the Checkpoint commit are not. Recovery
/// finishes the rotation and commits — no verification is redone.
#[tokio::test]
async fn failure_point_6_before_checkpoint_transaction() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let execution_episode_id = ids.new_episode_id();
    let execution_session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();
    let verification_episode_id = ids.new_episode_id();
    let verification_session_id = ids.new_session_id();

    let state = build_verification_pending(
        &ports,
        loop_id,
        &work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
        verification_episode_id,
        verification_session_id,
    )
    .await;
    let state = step(
        &state,
        LifecycleCommand::RecordVerification {
            outcome: VerificationOutcome::Accepted,
        },
    );
    assert_eq!(
        state.episode.as_ref().map(|e| e.status),
        Some(EpisodeStatus::Executing)
    );
    let checkpoint_version_before = state.execution.checkpoint_version;
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Unknown),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    match outcome {
        RecoveryOutcome::EpisodeRecovered {
            checkpoint_version, ..
        } => {
            assert_eq!(checkpoint_version, checkpoint_version_before + 1);
        }
        other => panic!("expected EpisodeRecovered, got {other:?}"),
    }
    assert_eq!(runner.reconcile_calls(), 0);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Accepted)
    );
    assert!(stored.state.active_episode().is_none());
    let commits = stored
        .events
        .iter()
        .filter(|recorded| matches!(recorded.event, LoopEvent::CheckpointCommitted { .. }))
        .count();
    assert_eq!(
        commits, 1,
        "recovery must commit exactly once, not duplicate the boundary"
    );
}

/// 7. After Checkpoint commit, before acknowledgement: the Checkpoint
/// transaction that terminates an Episode already fully committed; only the
/// caller who invoked the original operation crashed before observing it.
/// `recover` is a no-op, and a subsequent `advance` proceeds normally —
/// "continue from next Episode" (`docs/06-verification-and-recovery.md`
/// §13).
#[tokio::test]
async fn failure_point_7_after_checkpoint_commit_before_acknowledgement() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let execution_episode_id = ids.new_episode_id();
    let execution_session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();
    let verification_episode_id = ids.new_episode_id();
    let verification_session_id = ids.new_session_id();

    let state = build_verification_pending(
        &ports,
        loop_id,
        &work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
        verification_episode_id,
        verification_session_id,
    )
    .await;
    let state = step(
        &state,
        LifecycleCommand::RecordVerification {
            outcome: VerificationOutcome::Accepted,
        },
    );
    let state = step(
        &state,
        LifecycleCommand::RequestRotation {
            reason: RotationReason::VerificationBoundary,
        },
    );
    let checkpoint2 = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: Some(verification_episode_id),
        integrity_digest: None,
    };
    let state = step(
        &state,
        LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint2,
        },
    );
    assert!(
        state.active_episode().is_none(),
        "the episode already fully terminated"
    );
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Unknown),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert_eq!(outcome, RecoveryOutcome::NothingToRecover);
    assert_eq!(runner.reconcile_calls(), 0);
    assert_eq!(runner.start_calls(), 0);
    assert_eq!(runner.collect_calls(), 0);

    // Recovery wrote nothing at all: a subsequent `advance` still finds the
    // Loop already terminal (this Work Unit is the Loop's only one and it is
    // now `Accepted`), proving recovery did not disturb the already
    // acknowledged-in-substance state.
    let controller = manager.controller();
    let outcome = controller.advance(loop_id).await.expect("advance succeeds");
    assert!(matches!(outcome, OperationOutcome::Terminal { .. }));
}

/// 8. During Session drain: `rotation.requested` is durable (Episode
/// `Draining`), but the Checkpoint commit is not. Recovery commits directly
/// — `RequestRotation` must not be re-applied (the Session is no longer
/// `Running`, so re-deciding it would be rejected).
#[tokio::test]
async fn failure_point_8_during_session_drain() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let execution_episode_id = ids.new_episode_id();
    let execution_session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();
    let verification_episode_id = ids.new_episode_id();
    let verification_session_id = ids.new_session_id();

    let state = build_verification_pending(
        &ports,
        loop_id,
        &work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
        verification_episode_id,
        verification_session_id,
    )
    .await;
    let state = step(
        &state,
        LifecycleCommand::RecordVerification {
            outcome: VerificationOutcome::Accepted,
        },
    );
    let state = step(
        &state,
        LifecycleCommand::RequestRotation {
            reason: RotationReason::VerificationBoundary,
        },
    );
    assert_eq!(
        state.episode.as_ref().map(|e| e.status),
        Some(EpisodeStatus::Draining)
    );
    assert_eq!(
        state.session.as_ref().map(|s| s.status),
        Some(SessionStatus::Draining)
    );
    let checkpoint_version_before = state.execution.checkpoint_version;
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Unknown),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    match outcome {
        RecoveryOutcome::EpisodeRecovered {
            checkpoint_version, ..
        } => {
            assert_eq!(checkpoint_version, checkpoint_version_before + 1);
        }
        other => panic!("expected EpisodeRecovered, got {other:?}"),
    }

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert!(stored.state.active_episode().is_none());
    let rotations = stored
        .events
        .iter()
        .filter(|recorded| matches!(recorded.event, LoopEvent::RotationRequested { .. }))
        .count();
    assert_eq!(
        rotations, 0,
        "rotation was already durably recorded before the crash; recovery must not re-decide it"
    );
    let commits = stored
        .events
        .iter()
        .filter(|recorded| matches!(recorded.event, LoopEvent::CheckpointCommitted { .. }))
        .count();
    assert_eq!(commits, 1);
}

// ===========================================================================
// Resource integrity (Step 3)
// ===========================================================================

/// A resource the in-flight Episode depends on (here, the Attempt's Boot
/// Package) can no longer be resolved: recovery suspends rather than
/// proceeding on unverifiable state, and never calls the runner at all.
#[tokio::test]
async fn a_missing_resource_suspends_recovery_without_consulting_the_runner() {
    let (ports, broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    let boot_package_uri = state.attempt.as_ref().unwrap().boot_package_ref.uri.clone();
    broker.remove(&boot_package_uri);
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Completed),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::LoopSuspended { .. }));
    assert_eq!(
        runner.reconcile_calls(),
        0,
        "an integrity failure must be caught before consulting the runner"
    );

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
}

// ===========================================================================
// The two scope items Task 9's report flagged for this task
// ===========================================================================

/// A `VerificationOutcome::Suspended` outcome leaves the Work Unit stuck at
/// `WorkUnitStatus::Suspended` — a status `prepare_attempt` never accepts,
/// and no `LifecycleCommand` clears. `RuntimeLoopService::resume` must call
/// into recovery so the Loop it hands back is actually able to `advance`,
/// not merely `Active` in name.
#[tokio::test]
async fn resume_reconciles_a_work_unit_stuck_suspended_by_a_verification_outcome() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let execution_episode_id = ids.new_episode_id();
    let execution_session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();
    let verification_episode_id = ids.new_episode_id();
    let verification_session_id = ids.new_session_id();

    let state = build_verification_pending(
        &ports,
        loop_id,
        &work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
        verification_episode_id,
        verification_session_id,
    )
    .await;
    // `RecordVerification` with `Suspended` durably produces both
    // `verification.completed` and `loop.suspended` together.
    let state = step(
        &state,
        LifecycleCommand::RecordVerification {
            outcome: VerificationOutcome::Suspended,
        },
    );
    assert_eq!(state.execution.status, LoopStatus::Suspended);
    assert_eq!(
        state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Suspended)
    );
    // The verification Episode is now terminal (`Suspended`) but not yet
    // checkpointed: commit its terminal Checkpoint, exactly as
    // `ensure_episode_terminal_and_commit` would.
    let checkpoint2 = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: Some(verification_episode_id),
        integrity_digest: None,
    };
    let state = step(
        &state,
        LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint2,
        },
    );
    assert!(state.active_episode().is_none());
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Unknown),
    ));
    let service = RuntimeLoopService::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    resume_loop(&service, loop_id).await;

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Active);
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Pending),
        "the stuck Work Unit must be returned to Pending, not left Suspended forever"
    );

    // The dead end Task 9 flagged is gone: a fresh Execution Episode can now
    // start and run to completion.
    let advance_outcome = service
        .controller()
        .advance(loop_id)
        .await
        .expect("advance no longer hits the dead end");
    assert!(matches!(
        advance_outcome,
        OperationOutcome::EpisodeCompleted { .. }
    ));
}

/// A Loop suspended mid-Attempt (e.g. an operator suspend while the runner
/// was executing) leaves a non-terminal Attempt behind. The reducer's own
/// `attempt.reconciled_before_next_generation` invariant then permanently
/// refuses `StartEpisode`/`StartSession` unless something reconciles it
/// first. `resume` must call into recovery to reconcile it (asking the
/// runner before assuming the effect stopped) rather than resuming into an
/// unsafe state. The runner here gives a *confirmed* answer (`NotStarted`):
/// this is the safe half of the boundary — see
/// `resume_refuses_when_recovery_cannot_confirm_a_dangling_attempt_is_safe`
/// for the unsafe half, where an unconfirmed answer must NOT be retired.
#[tokio::test]
async fn resume_reconciles_a_dangling_attempt_left_by_a_mid_attempt_suspend() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    // An operator (or the mandatory-closure path) suspends the Loop while
    // the Attempt is still `Running`.
    let state = step(
        &state,
        LifecycleCommand::Suspend {
            reason: SuspensionReason::OperatorRequest,
        },
    );
    assert_eq!(
        state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Suspended)
    );
    assert!(!state.attempt.as_ref().unwrap().status.is_terminal());
    let checkpoint2 = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: Some(episode_id),
        integrity_digest: None,
    };
    let state = step(
        &state,
        LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint2,
        },
    );
    assert!(state.active_episode().is_none());

    // Confirm the dead end is real before recovery touches anything: a bare
    // resume (no reconciliation) would leave the reducer's own invariant
    // blocking every future StartEpisode/StartSession.
    let unreconciled = decide(
        &{
            let mut resumed = state.clone();
            resumed.execution.status = LoopStatus::Active;
            resumed
        },
        &LifecycleCommand::StartEpisode {
            episode_id: EpisodeId::new(),
        },
        &EffectivePolicy::default(),
    );
    assert!(
        unreconciled.is_err(),
        "a bare status flip alone must still be blocked"
    );

    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::NotStarted),
    ));
    let service = RuntimeLoopService::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    resume_loop(&service, loop_id).await;

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Active);
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Cancelled)
    );
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Pending)
    );

    let advance_outcome = service
        .controller()
        .advance(loop_id)
        .await
        .expect("the reconciled attempt no longer blocks advance");
    assert!(matches!(
        advance_outcome,
        OperationOutcome::EpisodeCompleted { .. }
    ));
}

/// Resuming while the runner reports the dangling Attempt's external effect
/// may still be live must not retire it: recovery leaves everything exactly
/// as it is rather than risking an unsafe concurrent repeat.
#[tokio::test]
async fn resume_preserves_a_dangling_attempt_the_runner_reports_as_still_live() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    let state = step(
        &state,
        LifecycleCommand::Suspend {
            reason: SuspensionReason::OperatorRequest,
        },
    );
    let checkpoint2 = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: Some(episode_id),
        integrity_digest: None,
    };
    let state = step(
        &state,
        LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint2,
        },
    );
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Running),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::StillSuspended { .. }));

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Suspended),
        "still possibly live: the Attempt must not be retired"
    );
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Suspended),
        "the Work Unit must not be reopened while the old effect might still be running"
    );
}

/// Important #2: `resume` must not report success while leaving the Loop in
/// a state every subsequent `advance` will reject. When recovery cannot
/// confirm a dangling Attempt's external effect is over (here: `Unknown`),
/// `resume` re-suspends the Loop and refuses, rather than returning a bare
/// `Ok` for a Loop that is `Active` in name only.
#[tokio::test]
async fn resume_refuses_when_recovery_cannot_confirm_a_dangling_attempt_is_safe() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    let state = step(
        &state,
        LifecycleCommand::Suspend {
            reason: SuspensionReason::OperatorRequest,
        },
    );
    let checkpoint2 = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: Some(episode_id),
        integrity_digest: None,
    };
    let state = step(
        &state,
        LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint2,
        },
    );
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Unknown),
    ));
    let service = RuntimeLoopService::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let result = resume_loop_result(&service, loop_id).await;
    let error =
        result.expect_err("resume must refuse rather than report success it cannot back up");
    assert_eq!(error.code, LoopwardErrorCode::RecoveryRequired);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(
        stored.state.execution.status,
        LoopStatus::Suspended,
        "the loop must be left Suspended, not Active with an unreconciled attempt"
    );
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Suspended)
    );
}

/// A dangling Attempt whose runner confirms it actually `Completed` is not
/// simply discarded (unlike a confirmed failure/cancellation): recovery
/// collects the result, validates it against the Episode's originally
/// hydrated pinned state, and folds it back in as a completed Attempt
/// awaiting verification.
#[tokio::test]
async fn resume_recovers_a_dangling_attempts_completed_result_instead_of_discarding_it() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    let state = step(
        &state,
        LifecycleCommand::Suspend {
            reason: SuspensionReason::OperatorRequest,
        },
    );
    let checkpoint2 = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: state.checkpoint.version + 1,
        aggregate_revision: state.execution.aggregate_revision,
        parent: Some(state.checkpoint.id),
        episode_id: Some(episode_id),
        integrity_digest: None,
    };
    let state = step(
        &state,
        LifecycleCommand::CommitCheckpoint {
            checkpoint: checkpoint2,
        },
    );
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Completed),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert_eq!(outcome, RecoveryOutcome::AttemptResultRecovered);
    assert_eq!(runner.collect_calls(), 1);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Succeeded)
    );
    assert!(stored.state.attempt.as_ref().unwrap().result_ref.is_some());
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::AwaitingVerification)
    );
}

/// The runner itself suspended the run (still inside a live Episode):
/// recovery preserves that suspension through the ordinary `Suspend`
/// command rather than treating it as a failure.
#[tokio::test]
async fn in_episode_reconcile_suspended_preserves_the_runners_own_suspension() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Suspended),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::LoopSuspended { .. }));

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Suspended)
    );
    assert!(stored.state.active_episode().is_none());
}

/// A confirmed runner failure (inside a live Episode) mandates closure:
/// Core's only reachable terminal status from `Running` here is
/// `Ambiguous`, and the Loop suspends. Per Important #3's fix, that
/// `Ambiguous` Attempt is not a dead end: a later `resume` reconciles it
/// too, asking the runner again before retiring it.
#[tokio::test]
async fn in_episode_reconcile_failed_marks_ambiguous_and_a_later_resume_clears_it() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Failed),
    ));
    // The same controller (and so the same lease-holder identity) drives
    // both phases below: `InMemoryLoopStore::acquire_lease` — like the real
    // store — rejects a second holder while the first holder's lease is
    // still current, and this fake does not model TTL expiry.
    let built_controller = controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    );
    let manager = RecoveryManager::new(built_controller.clone());

    let outcome = manager
        .recover(loop_id)
        .await
        .expect("first recover suspends the loop");
    assert!(matches!(outcome, RecoveryOutcome::LoopSuspended { .. }));

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Ambiguous),
        "Core's mechanical closure of a confirmed failure inside a live episode is Ambiguous"
    );
    assert!(stored.state.active_episode().is_none());

    // A later `resume` (through the service, not `RecoveryManager` directly)
    // reconciles the now-`Ambiguous` Attempt exactly as it would a dangling
    // `Suspended` one, once the runner gives a confirmed answer.
    runner.set_reconcile_outcome(Ok(ReconcileOutcome::Cancelled));
    let service = RuntimeLoopService::new(built_controller);
    resume_loop(&service, loop_id).await;

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Active);
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Cancelled),
        "the Ambiguous Attempt must no longer be a dead end"
    );
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Pending)
    );

    let advance_outcome = service
        .controller()
        .advance(loop_id)
        .await
        .expect("the reconciled attempt no longer blocks advance");
    assert!(matches!(
        advance_outcome,
        OperationOutcome::EpisodeCompleted { .. }
    ));
}

/// A confirmed runner-side cancellation (inside a live Episode) is treated
/// the same as a confirmed failure: Core's only reachable terminal status
/// here is `Ambiguous`, and the Loop suspends.
#[tokio::test]
async fn in_episode_reconcile_cancelled_marks_ambiguous_and_suspends() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_running_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Cancelled),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::LoopSuspended { .. }));

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(
        stored.state.attempt.as_ref().map(|a| a.status),
        Some(AttemptStatus::Ambiguous)
    );
}

/// The runner truthfully reports the Attempt was never started
/// (`NotStarted`), but the fresh `start` call recovery itself issues fails:
/// mandatory closure applies exactly as a graceful in-process runner
/// failure would (`ConfigurableRunner::fail_start`, previously unexercised
/// by any test).
#[tokio::test]
async fn recovery_start_failure_suspends_the_loop_and_commits() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let episode_id = ids.new_episode_id();
    let session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();

    let state = build_prepared_attempt(
        &ports, loop_id, &work_unit, episode_id, session_id, attempt_id,
    )
    .await;
    store.seed(loop_id, state);

    let runner = Arc::new(
        ConfigurableRunner::new(attempt_id, Ok(ReconcileOutcome::NotStarted)).failing_to_start(),
    );
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::LoopSuspended { .. }));
    assert_eq!(runner.start_calls(), 1);

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(stored.state.execution.status, LoopStatus::Suspended);
    let rotation_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::RotationRequested { reason, .. } => Some(*reason),
            _ => None,
        });
    assert_eq!(rotation_reason, Some(RotationReason::RunnerFailure));
}

/// A Verification Episode that crashes before `session.started` (a real
/// window: `advance` applies `StartEpisode`, hydrates, then `StartSession`,
/// all in one call) must be recovered *as* a Verification Episode — not
/// mis-attributed as an Execution Episode's `WorkUnitBoundary` rotation.
#[tokio::test]
async fn verification_episode_crash_before_session_start_is_recovered_as_verification() {
    let (ports, _broker, ids, store) = build_ports();
    let loop_id = ids.new_loop_id();
    let work_unit = test_work_unit(ids.new_work_unit_id());
    let execution_episode_id = ids.new_episode_id();
    let execution_session_id = ids.new_session_id();
    let attempt_id = ids.new_attempt_id();
    let verification_episode_id = ids.new_episode_id();

    let state = build_verification_episode_hydrated(
        &ports,
        loop_id,
        &work_unit,
        execution_episode_id,
        execution_session_id,
        attempt_id,
        verification_episode_id,
    )
    .await;
    assert_eq!(
        state.episode.as_ref().map(|episode| episode.status),
        Some(EpisodeStatus::Hydrating)
    );
    assert_eq!(
        state.episode.as_ref().map(|episode| episode.kind),
        Some(EpisodeKind::Verification)
    );
    store.seed(loop_id, state);

    let runner = Arc::new(ConfigurableRunner::new(
        attempt_id,
        Ok(ReconcileOutcome::Unknown),
    ));
    let manager = RecoveryManager::new(controller(
        ports.clone(),
        Arc::clone(&runner) as Arc<dyn WorkUnitRunner>,
    ));

    let outcome = manager.recover(loop_id).await.expect("recover succeeds");
    assert!(matches!(outcome, RecoveryOutcome::EpisodeRecovered { .. }));
    assert_eq!(
        runner.reconcile_calls(),
        0,
        "verification never consults the runner"
    );

    let stored = ports.store.load(loop_id).await.expect("load succeeds");
    let rotation_reason = stored
        .events
        .iter()
        .find_map(|recorded| match &recorded.event {
            LoopEvent::RotationRequested { reason, .. } => Some(*reason),
            _ => None,
        });
    assert_eq!(
        rotation_reason,
        Some(RotationReason::VerificationBoundary),
        "must be attributed as a verification boundary, not a work unit boundary"
    );
    assert_eq!(
        stored.state.work_unit.as_ref().map(|wu| wu.status),
        Some(WorkUnitStatus::Accepted)
    );
}

async fn resume_loop_result(
    service: &RuntimeLoopService,
    loop_id: LoopId,
) -> Result<loopward_api::loop_service::OperationHandle, loopward_api::error::LoopwardError> {
    let command = ResumeLoopCommand {
        metadata: CommandMetadata {
            request_id: "req-resume".to_owned(),
            idempotency_key: format!("resume:{loop_id}"),
            expected_revision: None,
            principal: None,
            trace_context: TraceContext::default(),
        },
        loop_id,
    };
    loopward_api::loop_service::LoopService::resume(service, command).await
}

async fn resume_loop(service: &RuntimeLoopService, loop_id: LoopId) {
    resume_loop_result(service, loop_id)
        .await
        .expect("resume succeeds");
}
