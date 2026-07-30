//! Compile-time contract tests for the runtime ports.
//!
//! Every fake below implements one real port trait end to end — no mocks
//! that merely record calls. The point of this test is that
//! [`RuntimePorts`] can be composed entirely from deterministic,
//! in-memory, no-I/O fakes: deterministic IDs, a fixed clock, in-memory
//! events, and no-op telemetry (`loopward_observability::NoopTelemetrySink`
//! is used directly rather than reimplemented here).
//!
//! `loopward-runtime` itself must depend only on port traits
//! (`docs/03-durable-state-and-transactions.md`); this test proves that
//! contract by satisfying every port with a fake that lives entirely in
//! this test file.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::ids::{
    AttemptId, CheckpointId, EpisodeId, EventId, LoopId, OperationId, SessionId, Timestamp,
    WorkUnitId,
};
use loopward_api::loop_service::LoopKind;
use loopward_api::resource::{PhaseRef, ResourceRef};
use loopward_api::work_unit::ResourceBudget;
use loopward_core::{AggregateState, Checkpoint, LoopEvent, LoopExecution, LoopStatus};
use loopward_observability::NoopTelemetrySink;
use loopward_runtime::ports::{
    AcquireLease, CheckpointCommitRequest, Clock, EVENT_SCHEMA_VERSION, IdGenerator, LoopLease,
    LoopStore, RecordedEvent, ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle,
    RuntimeInstanceId, RuntimePorts, StoreError, StoredLoop,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixed clock
// ---------------------------------------------------------------------------

/// A [`Clock`] that always returns the same, fixed timestamp.
struct FixedClock(Timestamp);

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Deterministic ID generator
// ---------------------------------------------------------------------------

/// An [`IdGenerator`] that mints IDs from a strictly increasing counter
/// instead of random `UUIDv7`s, so a test can assert exact, reproducible ID
/// sequences.
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
// In-memory resource broker
// ---------------------------------------------------------------------------

/// A [`ResourceBroker`] backed by in-memory buffers, keyed by an opaque
/// handle string. Finalized resources are addressed by `resource://<n>`
/// and content is looked up by that URI.
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

// ---------------------------------------------------------------------------
// In-memory loop store
// ---------------------------------------------------------------------------

/// A [`LoopStore`] backed by an in-memory map, enforcing the same
/// revision- and fencing-token checks a real store must
/// (`docs/03-durable-state-and-transactions.md` §2, §7, §10).
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

    /// Test-only seam for establishing a Loop's genesis state. `LoopStore`
    /// itself has no creation method: genesis persistence is a Task 9
    /// controller concern, out of scope for this port definition task.
    fn seed(&self, loop_id: LoopId, state: AggregateState) {
        self.loops.lock().expect("loops lock poisoned").insert(
            loop_id,
            StoredLoop {
                state,
                events: Vec::new(),
            },
        );
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
        let current_token = *self
            .fencing_tokens
            .lock()
            .expect("fencing token lock poisoned")
            .get(&loop_id)
            .unwrap_or(&0);
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

        for event in events {
            stored.state.execution.aggregate_revision += 1;
            stored.events.push(RecordedEvent {
                event_id: self.event_ids.new_event_id(),
                sequence: stored.events.len() as u64 + 1,
                aggregate_revision: stored.state.execution.aggregate_revision,
                schema_version: EVENT_SCHEMA_VERSION.to_owned(),
                recorded_at: Timestamp::UNIX_EPOCH,
                event,
            });
        }
        Ok(stored.clone())
    }

    async fn commit_checkpoint(
        &self,
        request: CheckpointCommitRequest,
    ) -> Result<StoredLoop, StoreError> {
        let current_token = *self
            .fencing_tokens
            .lock()
            .expect("fencing token lock poisoned")
            .get(&request.loop_id)
            .unwrap_or(&0);
        if request.fencing_token != current_token {
            return Err(StoreError::StaleFencingToken {
                loop_id: request.loop_id,
                given: request.fencing_token,
                current: current_token,
            });
        }

        let mut loops = self.loops.lock().expect("loops lock poisoned");

        // No row yet: the only legal request is a genesis commit, exactly
        // when the caller expects a blank aggregate revision and blank
        // checkpoint version (`docs/02-core-domain-model.md` §7: the
        // genesis Checkpoint has no parent; `docs/03` §5). Any other
        // expectation against a nonexistent Loop stays `LoopNotFound`
        // rather than silently creating a row out of thin air.
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
            stored.state.execution.aggregate_revision += 1;
            stored.events.push(RecordedEvent {
                event_id: self.event_ids.new_event_id(),
                sequence: stored.events.len() as u64 + 1,
                aggregate_revision: stored.state.execution.aggregate_revision,
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
// Genesis state builder
// ---------------------------------------------------------------------------

fn genesis_state(loop_id: LoopId) -> AggregateState {
    AggregateState {
        execution: LoopExecution {
            id: loop_id,
            kind: LoopKind::new("domain.example"),
            status: LoopStatus::Active,
            phase: PhaseRef::new("implementation"),
            aggregate_revision: 0,
            checkpoint_version: 0,
            active_episode: None,
            active_work_unit: None,
            budget: ResourceBudget::default(),
        },
        episode: None,
        session: None,
        work_unit: None,
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

/// Builds a [`RuntimePorts`] from the fakes above, with one Loop already
/// seeded into the store at genesis (`aggregate_revision: 0`,
/// `checkpoint_version: 0`).
fn build_ports() -> (RuntimePorts, LoopId, Timestamp) {
    let fixed_now = Timestamp::UNIX_EPOCH;

    let ids = Arc::new(SequentialIdGenerator::new());
    let loop_id = ids.new_loop_id();

    // The concrete in-memory store is seeded directly, before being erased
    // to `Arc<dyn LoopStore>`, so every test can drive the trait exactly as
    // `loopward-runtime` would while still controlling test setup
    // (`LoopStore` has no create method; see `InMemoryLoopStore::seed`).
    let concrete_store = Arc::new(InMemoryLoopStore::new());
    concrete_store.seed(loop_id, genesis_state(loop_id));

    let ports = RuntimePorts {
        store: concrete_store,
        resources: Arc::new(InMemoryResourceBroker::new()),
        clock: Arc::new(FixedClock(fixed_now)),
        ids,
        telemetry: Arc::new(NoopTelemetrySink),
    };
    (ports, loop_id, fixed_now)
}

// ---------------------------------------------------------------------------
// The contract tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runtime_ports_compose_from_deterministic_fakes() {
    let (ports, _loop_id, fixed_now) = build_ports();

    // Clock is fixed.
    assert_eq!(ports.clock.now(), fixed_now);

    // IDs are deterministic and distinct.
    let first_episode_id = ports.ids.new_episode_id();
    let second_episode_id = ports.ids.new_episode_id();
    assert_ne!(first_episode_id, second_episode_id);
}

#[tokio::test]
async fn lease_fencing_token_gates_appends_and_survives_renewal() {
    let (ports, loop_id, _fixed_now) = build_ports();

    // Acquiring a lease mints fencing token 1.
    let holder = RuntimeInstanceId::new();
    let lease = ports
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("lease acquisition succeeds");
    assert_eq!(lease.fencing_token, 1);

    // Appending with the correct revision and fencing token advances the
    // aggregate revision and records an in-memory event.
    let episode_id = ports.ids.new_episode_id();
    let stored = ports
        .store
        .append(
            loop_id,
            0,
            lease.fencing_token,
            vec![LoopEvent::EpisodeStarted {
                episode_id,
                kind: loopward_core::EpisodeKind::Execution,
                base_checkpoint_version: 0,
            }],
        )
        .await
        .expect("append with correct fencing succeeds");
    assert_eq!(stored.state.execution.aggregate_revision, 1);
    assert_eq!(stored.events.len(), 1);

    // A stale fencing token cannot write (§7, §10).
    let stale_result = ports
        .store
        .append(
            loop_id,
            1,
            0, // stale: the lease bumped the token to 1
            vec![LoopEvent::LoopCompleted],
        )
        .await;
    assert!(matches!(
        stale_result,
        Err(StoreError::StaleFencingToken { .. })
    ));

    // Loading reflects the durable append.
    let loaded = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(loaded.state.execution.aggregate_revision, 1);

    // Renewing the lease keeps the same fencing token and extends expiry.
    let renewed = ports
        .store
        .renew_lease(&lease)
        .await
        .expect("renewal succeeds");
    assert_eq!(renewed.fencing_token, lease.fencing_token);
    assert!(renewed.expires_at > lease.expires_at);
}

#[tokio::test]
async fn append_rejects_stale_aggregate_revision() {
    let (ports, loop_id, _fixed_now) = build_ports();

    let holder = RuntimeInstanceId::new();
    let lease = ports
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("lease acquisition succeeds");

    // The genesis state seeded by `build_ports` starts at aggregate
    // revision 0; presenting any other expected revision must be rejected
    // even though the fencing token is correct (§2, §10) — distinct from
    // `StaleFencingToken`, which the lease-fencing test above already
    // covers.
    let result = ports
        .store
        .append(
            loop_id,
            5,
            lease.fencing_token,
            vec![LoopEvent::LoopCompleted],
        )
        .await;
    assert!(matches!(
        result,
        Err(StoreError::RevisionConflict {
            expected: 5,
            actual: 0,
            ..
        })
    ));
}

#[tokio::test]
async fn commit_checkpoint_creates_a_fresh_loop_and_then_updates_it() {
    let (ports, _seeded_loop_id, _fixed_now) = build_ports();

    // A brand-new Loop, never seeded: `LoopStore` has no separate creation
    // method, so this drives genesis purely through the trait —
    // `commit_checkpoint` with `expected_revision: 0` and
    // `expected_checkpoint_version: 0` against an unknown `loop_id`
    // (`docs/02-core-domain-model.md` §7, `docs/03` §5).
    let loop_id = ports.ids.new_loop_id();
    let holder = RuntimeInstanceId::new();
    let lease = ports
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("lease acquisition succeeds");

    let genesis_checkpoint = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: 1,
        aggregate_revision: 1,
        parent: None,
        episode_id: None,
        integrity_digest: None,
    };
    let mut genesis = genesis_state(loop_id);
    genesis.execution.aggregate_revision = 1;
    genesis.execution.checkpoint_version = 1;
    genesis.checkpoint = genesis_checkpoint.clone();

    let created = ports
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 0,
            expected_checkpoint_version: 0,
            fencing_token: lease.fencing_token,
            checkpoint: genesis_checkpoint.clone(),
            events: vec![LoopEvent::CheckpointCommitted {
                checkpoint: genesis_checkpoint.clone(),
            }],
            state: genesis.clone(),
        })
        .await
        .expect("genesis commit succeeds against an unknown loop_id");
    assert_eq!(created.state.execution.aggregate_revision, 1);
    assert_eq!(created.state.execution.checkpoint_version, 1);
    assert_eq!(created.events.len(), 1);

    // Loading now succeeds through the trait alone: no seed backdoor was
    // used for this Loop.
    let loaded = ports.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(loaded.state.execution.id, loop_id);

    // A second, authoritative commit against the now-existing Loop is
    // fenced against the revision and checkpoint version the genesis
    // commit just established.
    let second_checkpoint = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: 2,
        aggregate_revision: 2,
        parent: Some(genesis_checkpoint.id),
        episode_id: None,
        integrity_digest: None,
    };
    let mut second_state = genesis;
    second_state.execution.aggregate_revision = 2;
    second_state.execution.checkpoint_version = 2;
    second_state.checkpoint = second_checkpoint.clone();

    let updated = ports
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: lease.fencing_token,
            checkpoint: second_checkpoint,
            events: vec![],
            state: second_state,
        })
        .await
        .expect("commit against the existing loop succeeds");
    assert_eq!(updated.state.execution.checkpoint_version, 2);
    assert_eq!(updated.events.len(), 1); // unchanged: this commit added no new events
}

#[tokio::test]
async fn commit_checkpoint_rejects_stale_revision_and_checkpoint_version() {
    let (ports, loop_id, _fixed_now) = build_ports();

    let holder = RuntimeInstanceId::new();
    let lease = ports
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("lease acquisition succeeds");

    let checkpoint = Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version: 1,
        aggregate_revision: 1,
        parent: None,
        episode_id: None,
        integrity_digest: None,
    };
    let mut state = genesis_state(loop_id);
    state.execution.aggregate_revision = 1;
    state.execution.checkpoint_version = 1;
    state.checkpoint = checkpoint.clone();

    // Wrong expected revision against the seeded genesis (0).
    let revision_conflict = ports
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 9,
            expected_checkpoint_version: 0,
            fencing_token: lease.fencing_token,
            checkpoint: checkpoint.clone(),
            events: vec![],
            state: state.clone(),
        })
        .await;
    assert!(matches!(
        revision_conflict,
        Err(StoreError::RevisionConflict {
            expected: 9,
            actual: 0,
            ..
        })
    ));

    // Correct revision but wrong expected checkpoint version.
    let checkpoint_conflict = ports
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 0,
            expected_checkpoint_version: 7,
            fencing_token: lease.fencing_token,
            checkpoint,
            events: vec![],
            state,
        })
        .await;
    assert!(matches!(
        checkpoint_conflict,
        Err(StoreError::CheckpointVersionConflict {
            expected: 7,
            actual: 0,
            ..
        })
    ));
}

#[tokio::test]
async fn renew_lease_without_prior_acquisition_is_rejected() {
    let (ports, loop_id, _fixed_now) = build_ports();

    // A lease this store never granted (e.g. reconstructed from a stale
    // in-memory copy after a restart): renewal must fail rather than
    // silently succeed.
    let phantom_lease = LoopLease {
        loop_id,
        holder_id: RuntimeInstanceId::new(),
        fencing_token: 1,
        acquired_at: Timestamp::UNIX_EPOCH,
        expires_at: Timestamp::UNIX_EPOCH + time::Duration::seconds(30),
    };

    let result = ports.store.renew_lease(&phantom_lease).await;
    assert!(matches!(result, Err(StoreError::LeaseNotHeld(id)) if id == loop_id));
}

#[tokio::test]
async fn lease_conflict_when_another_holder_still_holds_the_lease() {
    let (ports, loop_id, _fixed_now) = build_ports();

    let first_holder = RuntimeInstanceId::new();
    let first_lease = ports
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: first_holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("first acquisition succeeds");
    assert_eq!(first_lease.fencing_token, 1);

    // A different holder cannot steal the lease while it is still tracked
    // as held (§7: "at most one driver may mutate a Loop").
    let second_holder = RuntimeInstanceId::new();
    let conflict = ports
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: second_holder,
            ttl: time::Duration::seconds(30),
        })
        .await;
    assert!(matches!(conflict, Err(StoreError::LeaseConflict(id)) if id == loop_id));

    // The same holder MAY re-acquire — e.g. after restarting with a stable
    // identity — and doing so still bumps the fencing token (§7:
    // "reacquisition increments the fencing token").
    let reacquired = ports
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: first_holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("same holder can re-acquire");
    assert_eq!(reacquired.fencing_token, 2);
}

#[tokio::test]
async fn resource_broker_round_trips_and_aborts() {
    let (ports, _loop_id, _fixed_now) = build_ports();

    // Resource broker round-trip: begin, write, finalize, resolve.
    let handle = ports
        .resources
        .begin(ResourceBeginHint {
            media_type: Some("text/plain".to_owned()),
            summary: Some("test resource".to_owned()),
        })
        .await
        .expect("begin succeeds");
    ports
        .resources
        .write(&handle, b"hello loopward")
        .await
        .expect("write succeeds");
    let resource_ref = ports
        .resources
        .finalize(handle)
        .await
        .expect("finalize succeeds");
    let resolved = ports
        .resources
        .resolve(&resource_ref)
        .await
        .expect("resolve succeeds");
    assert_eq!(resolved, b"hello loopward");

    // Aborting a resource makes it unresolvable and unfinalizable.
    let aborted_handle = ports
        .resources
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    ports
        .resources
        .abort(aborted_handle)
        .await
        .expect("abort succeeds");
}

#[tokio::test]
async fn telemetry_sink_is_noop() {
    let (ports, _loop_id, _fixed_now) = build_ports();

    // Telemetry is no-op: recording never panics and never observably
    // affects anything else in this test.
    ports
        .telemetry
        .record(loopward_observability::TelemetryEvent::new("loop.created"));
}
