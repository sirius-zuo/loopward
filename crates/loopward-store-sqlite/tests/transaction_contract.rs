//! Transaction contract tests for the `SQLite` adapter.
//!
//! These tests drive [`SqliteLoopStore`] and [`FileResourceBroker`] through
//! the `loopward-runtime` port traits against real, temporary `SQLite`
//! databases and real, temporary resource roots — no fakes, no in-memory
//! shortcuts — and then inspect the database with raw `SQL` to prove the
//! storage invariants in `docs/03-durable-state-and-transactions.md` §10
//! actually hold:
//!
//! * checkpoint versions and aggregate revisions increase monotonically;
//! * one Loop has at most one active Episode;
//! * closed Sessions cannot reopen;
//! * unfinalized resources cannot enter a Checkpoint;
//! * stale fencing tokens cannot write;
//! * event sequence is gap-free per committed transaction;
//! * journal and projection changes are transactionally consistent — a
//!   rolled-back Checkpoint transaction leaves *no* partial trace.

use std::path::{Path, PathBuf};

use loopward_api::ids::{CheckpointId, EpisodeId, LoopId, SessionId};
use loopward_api::loop_service::LoopKind;
use loopward_api::resource::{PhaseRef, ResourceRef, RunnerRef};
use loopward_api::runner::SuspensionReason;
use loopward_api::work_unit::ResourceBudget;
use loopward_core::{
    AggregateState, Checkpoint, Episode, EpisodeKind, EpisodeStatus, LoopEvent, LoopExecution,
    LoopStatus, RuntimeSession, SessionStatus,
};
use loopward_runtime::ports::{
    AcquireLease, CheckpointCommitRequest, LoopStore, ResourceBeginHint, ResourceBroker,
    ResourceError, RuntimeInstanceId, StoreError,
};
use loopward_store_sqlite::{
    FileResourceBroker, SqliteLoopStore, SqliteStoreConfig, SqliteStoreError, Synchronous,
};
use sqlx::Row;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Harness {
    directory: TempDir,
    store: SqliteLoopStore,
    broker: FileResourceBroker,
}

impl Harness {
    async fn open() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store =
            SqliteLoopStore::open(SqliteStoreConfig::new(directory.path().join("loopward.db")))
                .await
                .expect("store opens and migrates");
        let broker =
            FileResourceBroker::new(store.pool().clone(), directory.path().join("resources"))
                .expect("resource broker opens");
        Self {
            directory,
            store,
            broker,
        }
    }

    fn resource_root(&self) -> PathBuf {
        self.directory.path().join("resources")
    }

    async fn count(&self, sql: &'static str, loop_id: LoopId) -> i64 {
        sqlx::query(sql)
            .bind(loop_id.to_string())
            .fetch_one(self.store.pool())
            .await
            .expect("count query succeeds")
            .get::<i64, _>(0)
    }

    async fn event_sequences(&self, loop_id: LoopId) -> Vec<i64> {
        sqlx::query("SELECT sequence FROM loop_events WHERE loop_id = ?1 ORDER BY sequence")
            .bind(loop_id.to_string())
            .fetch_all(self.store.pool())
            .await
            .expect("sequence query succeeds")
            .into_iter()
            .map(|row| row.get::<i64, _>(0))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn genesis_state(loop_id: LoopId, checkpoint: Checkpoint) -> AggregateState {
    AggregateState {
        execution: LoopExecution {
            id: loop_id,
            kind: LoopKind::new("domain.example"),
            status: LoopStatus::Active,
            phase: PhaseRef::new("implementation"),
            aggregate_revision: checkpoint.aggregate_revision,
            checkpoint_version: checkpoint.version,
            active_episode: None,
            active_work_unit: None,
            episode_count: 0,
            budget: ResourceBudget::default(),
        },
        episode: None,
        session: None,
        work_unit: None,
        attempt: None,
        checkpoint,
    }
}

fn checkpoint_at(
    loop_id: LoopId,
    version: u64,
    aggregate_revision: u64,
    parent: Option<CheckpointId>,
) -> Checkpoint {
    Checkpoint {
        id: CheckpointId::new(),
        loop_id,
        version,
        aggregate_revision,
        parent,
        episode_id: None,
        integrity_digest: None,
    }
}

fn episode(loop_id: LoopId, id: EpisodeId, status: EpisodeStatus) -> Episode {
    Episode {
        id,
        loop_id,
        kind: EpisodeKind::Execution,
        status,
        base_checkpoint_version: 1,
        terminal_checkpoint_version: None,
        active_session: None,
        hydrated_manifest: None,
        trace_id: None,
    }
}

fn session(
    loop_id: LoopId,
    id: SessionId,
    episode_id: EpisodeId,
    status: SessionStatus,
    manifest: ResourceRef,
) -> RuntimeSession {
    RuntimeSession {
        id,
        loop_id,
        episode_id,
        generation: 1,
        status,
        runner: RunnerRef {
            name: "command".to_owned(),
            profile: None,
        },
        context_budget: None,
        boot_manifest_ref: manifest,
        external_session_ref: None,
        rotation_intent: None,
    }
}

/// The `checkpoint.committed` journal event every Checkpoint commit must
/// record (`docs/03-durable-state-and-transactions.md` §5; the port
/// documents `events` as recording "at minimum `checkpoint.committed`").
fn records_commit(checkpoint: &Checkpoint) -> Vec<LoopEvent> {
    vec![LoopEvent::CheckpointCommitted {
        checkpoint: checkpoint.clone(),
    }]
}

/// A projection sitting at `checkpoint` whose Episode is executing and
/// whose Runtime Session boots from `manifest`.
fn state_with_session(
    loop_id: LoopId,
    checkpoint: Checkpoint,
    episode_id: EpisodeId,
    session_id: SessionId,
    status: SessionStatus,
    manifest: ResourceRef,
) -> AggregateState {
    let mut state = genesis_state(loop_id, checkpoint);
    state.execution.active_episode = Some(episode_id);
    state.episode = Some(episode(loop_id, episode_id, EpisodeStatus::Executing));
    state.session = Some(session(loop_id, session_id, episode_id, status, manifest));
    state
}

/// Commits the genesis Checkpoint for a brand-new Loop, which is the only
/// way `LoopStore` creates a Loop: `expected_revision` and
/// `expected_checkpoint_version` both `0` against an unknown `loop_id`.
async fn commit_genesis(harness: &Harness, loop_id: LoopId) -> Checkpoint {
    let checkpoint = checkpoint_at(loop_id, 1, 1, None);
    harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 0,
            expected_checkpoint_version: 0,
            fencing_token: 0,
            checkpoint: checkpoint.clone(),
            events: vec![LoopEvent::CheckpointCommitted {
                checkpoint: checkpoint.clone(),
            }],
            state: genesis_state(loop_id, checkpoint.clone()),
        })
        .await
        .expect("genesis commit succeeds against an unknown loop_id");
    checkpoint
}

fn backend_error(error: &StoreError) -> &SqliteStoreError {
    match error {
        StoreError::Backend(source) => source
            .downcast_ref::<SqliteStoreError>()
            .expect("backend errors are SqliteStoreError"),
        other => panic!("expected a backend error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Genesis and loading
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_rejects_an_unknown_loop() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();

    let result = harness.store.load(loop_id).await;

    assert!(matches!(result, Err(StoreError::LoopNotFound(id)) if id == loop_id));
}

#[tokio::test]
async fn commit_checkpoint_creates_a_loop_only_at_genesis_expectations() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let checkpoint = checkpoint_at(loop_id, 1, 1, None);

    // A non-genesis expectation against an unknown Loop stays LoopNotFound
    // rather than conjuring a row.
    let not_found = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 3,
            expected_checkpoint_version: 0,
            fencing_token: 0,
            checkpoint: checkpoint.clone(),
            events: vec![],
            state: genesis_state(loop_id, checkpoint.clone()),
        })
        .await;
    assert!(matches!(not_found, Err(StoreError::LoopNotFound(id)) if id == loop_id));
    assert_eq!(
        harness
            .count("SELECT COUNT(*) FROM loops WHERE loop_id = ?1", loop_id)
            .await,
        0,
        "a rejected genesis commit must not create a loops row"
    );

    // Both expectations at zero: genesis creation.
    let created = commit_genesis(&harness, loop_id).await;
    let loaded = harness.store.load(loop_id).await.expect("loop now loads");
    assert_eq!(loaded.state.execution.id, loop_id);
    assert_eq!(loaded.state.execution.aggregate_revision, 1);
    assert_eq!(loaded.state.execution.checkpoint_version, 1);
    assert_eq!(loaded.state.checkpoint.id, created.id);
    assert_eq!(loaded.events.len(), 1);
    assert_eq!(loaded.events[0].sequence, 1);
    assert_eq!(loaded.events[0].aggregate_revision, 1);
}

// ---------------------------------------------------------------------------
// Fencing: revisions, checkpoint versions, and lease tokens
// ---------------------------------------------------------------------------

#[tokio::test]
async fn append_rejects_a_stale_aggregate_revision() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let lease = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("lease acquisition succeeds");

    let result = harness
        .store
        .append(
            loop_id,
            0, // stale: genesis advanced the revision to 1
            lease.fencing_token,
            vec![LoopEvent::LoopCompleted],
        )
        .await;

    assert!(matches!(
        result,
        Err(StoreError::RevisionConflict {
            expected: 0,
            actual: 1,
            ..
        })
    ));
    assert_eq!(
        harness.event_sequences(loop_id).await,
        vec![1],
        "a rejected append must not write a journal row"
    );
}

#[tokio::test]
async fn append_rejects_a_stale_fencing_token() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let first = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("first acquisition succeeds");

    // The same holder reacquires, superseding the first token.
    let second = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: first.holder_id,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("reacquisition succeeds");
    assert!(second.fencing_token > first.fencing_token);

    let result = harness
        .store
        .append(
            loop_id,
            1,
            first.fencing_token,
            vec![LoopEvent::LoopCompleted],
        )
        .await;

    assert!(matches!(
        result,
        Err(StoreError::StaleFencingToken { given, current, .. })
            if given == first.fencing_token && current == second.fencing_token
    ));

    // The superseding token still writes.
    let stored = harness
        .store
        .append(
            loop_id,
            1,
            second.fencing_token,
            vec![LoopEvent::LoopCompleted],
        )
        .await
        .expect("the current fencing token writes");
    assert_eq!(stored.state.execution.aggregate_revision, 2);
}

/// `append` folds each event through `loopward_core::evolve` before
/// persisting the projection, exactly mirroring what `commit_checkpoint`
/// already does with its caller-supplied `state`. This is the fix for a real
/// defect: a bare `append` used to only advance `aggregate_revision`,
/// leaving `execution.status` (and every other projection field) stale.
/// That silently broke `LoopController::suspend` on an idle Loop — the only
/// durable write there, with no Episode active to also commit a Checkpoint
/// for, is exactly this: a bare `append` of `LoopSuspended`.
#[tokio::test]
async fn append_folds_events_into_the_persisted_projection_not_just_the_revision() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let lease = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("lease acquisition succeeds");

    let appended = harness
        .store
        .append(
            loop_id,
            1,
            lease.fencing_token,
            vec![LoopEvent::LoopSuspended {
                reason: SuspensionReason::OperatorRequest,
            }],
        )
        .await
        .expect("append succeeds");
    assert_eq!(appended.state.execution.status, LoopStatus::Suspended);

    // And it survives a completely fresh `load()`, not just the value this
    // one `append` call happened to return.
    let reloaded = harness.store.load(loop_id).await.expect("load succeeds");
    assert_eq!(reloaded.state.execution.status, LoopStatus::Suspended);
    assert_eq!(reloaded.state.execution.aggregate_revision, 2);
}

#[tokio::test]
async fn commit_checkpoint_rejects_stale_fencing_tokens_and_versions() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    let lease = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("lease acquisition succeeds");

    let next = checkpoint_at(loop_id, 2, 2, Some(genesis.id));
    let state = genesis_state(loop_id, next.clone());

    let stale_token = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: lease.fencing_token + 7,
            checkpoint: next.clone(),
            events: vec![],
            state: state.clone(),
        })
        .await;
    assert!(matches!(
        stale_token,
        Err(StoreError::StaleFencingToken { .. })
    ));

    let stale_revision = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 9,
            expected_checkpoint_version: 1,
            fencing_token: lease.fencing_token,
            checkpoint: next.clone(),
            events: vec![],
            state: state.clone(),
        })
        .await;
    assert!(matches!(
        stale_revision,
        Err(StoreError::RevisionConflict {
            expected: 9,
            actual: 1,
            ..
        })
    ));

    let stale_version = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 7,
            fencing_token: lease.fencing_token,
            checkpoint: next.clone(),
            events: vec![],
            state: state.clone(),
        })
        .await;
    assert!(matches!(
        stale_version,
        Err(StoreError::CheckpointVersionConflict {
            expected: 7,
            actual: 1,
            ..
        })
    ));

    // Nothing above left a trace: still exactly the genesis Checkpoint.
    assert_eq!(
        harness
            .count(
                "SELECT COUNT(*) FROM checkpoints WHERE loop_id = ?1",
                loop_id
            )
            .await,
        1
    );

    // And a well-fenced commit still succeeds afterwards.
    let committed = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: lease.fencing_token,
            checkpoint: next.clone(),
            events: vec![LoopEvent::CheckpointCommitted {
                checkpoint: next.clone(),
            }],
            state,
        })
        .await
        .expect("a well-fenced commit succeeds");
    assert_eq!(committed.state.execution.checkpoint_version, 2);
}

#[tokio::test]
async fn a_commit_must_journal_the_checkpoint_committed_event() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    // §5 lists `checkpoint.committed` among the commit transaction's
    // mandated contents, and the port documents `events` as recording "at
    // minimum `checkpoint.committed`". A commit carrying no events at all
    // would publish a durable Checkpoint whose commit appears nowhere in
    // the journal while the revision advanced regardless.
    let next = checkpoint_at(loop_id, 2, 2, Some(genesis.id));
    let no_events = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: next.clone(),
            events: vec![],
            state: genesis_state(loop_id, next.clone()),
        })
        .await
        .expect_err("a commit journalling no events is rejected");
    assert!(matches!(
        backend_error(&no_events),
        SqliteStoreError::MissingCheckpointCommittedEvent { .. }
    ));

    // Events that are not `checkpoint.committed` do not satisfy the rule
    // either.
    let wrong_event = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: next.clone(),
            events: vec![LoopEvent::LoopCompleted],
            state: genesis_state(loop_id, next.clone()),
        })
        .await
        .expect_err("an unrelated event does not record the commit");
    assert!(matches!(
        backend_error(&wrong_event),
        SqliteStoreError::MissingCheckpointCommittedEvent { .. }
    ));

    // Nor does a `checkpoint.committed` naming a *different* Checkpoint.
    let other = checkpoint_at(loop_id, 2, 2, Some(genesis.id));
    let wrong_checkpoint = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: next.clone(),
            events: records_commit(&other),
            state: genesis_state(loop_id, next.clone()),
        })
        .await
        .expect_err("a checkpoint.committed naming another checkpoint is rejected");
    assert!(matches!(
        backend_error(&wrong_checkpoint),
        SqliteStoreError::MissingCheckpointCommittedEvent { .. }
    ));

    // None of the three rejections left a trace.
    assert_eq!(
        harness
            .count(
                "SELECT COUNT(*) FROM checkpoints WHERE loop_id = ?1",
                loop_id
            )
            .await,
        1
    );
    assert_eq!(harness.event_sequences(loop_id).await, vec![1]);

    // The same commit, properly journalled, succeeds.
    let committed = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            events: records_commit(&next),
            checkpoint: next.clone(),
            state: genesis_state(loop_id, next.clone()),
        })
        .await
        .expect("a commit that records itself succeeds");
    assert_eq!(committed.state.execution.checkpoint_version, 2);
    assert_eq!(
        committed.events.last().expect("journal is non-empty").event,
        LoopEvent::CheckpointCommitted { checkpoint: next }
    );
}

#[tokio::test]
async fn checkpoint_versions_must_increase_monotonically() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    // A Checkpoint whose own version does not advance past the one the
    // caller expected would break §10's monotonicity invariant.
    let regressive = checkpoint_at(loop_id, 1, 2, Some(genesis.id));
    let result = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: regressive.clone(),
            events: vec![],
            state: genesis_state(loop_id, regressive),
        })
        .await
        .expect_err("a non-advancing checkpoint version is rejected");

    assert!(matches!(
        backend_error(&result),
        SqliteStoreError::NonMonotonicCheckpointVersion { .. }
    ));
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_reacquired_lease_has_a_larger_fencing_token() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let holder = RuntimeInstanceId::new();
    let first = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("first acquisition succeeds");
    assert_eq!(first.fencing_token, 1);

    // A different runtime instance cannot steal an unexpired lease.
    let conflict = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await;
    assert!(matches!(conflict, Err(StoreError::LeaseConflict(id)) if id == loop_id));

    // The same holder MAY reacquire, and reacquisition increments the token.
    let second = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("same holder reacquires");
    assert_eq!(second.fencing_token, 2);
    assert!(second.fencing_token > first.fencing_token);

    // Renewal extends expiry without changing the token.
    let renewed = harness
        .store
        .renew_lease(&second)
        .await
        .expect("renewal succeeds");
    assert_eq!(renewed.fencing_token, second.fencing_token);
    assert!(renewed.expires_at >= second.expires_at);

    // Renewing the superseded lease is rejected.
    let stale = harness.store.renew_lease(&first).await;
    assert!(matches!(stale, Err(StoreError::StaleFencingToken { .. })));
}

#[tokio::test]
async fn a_released_lease_frees_the_loop_but_its_token_never_writes_again() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let released = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("first acquisition succeeds");
    harness
        .store
        .release_lease(&released)
        .await
        .expect("release succeeds");
    // Releasing twice is harmless.
    harness
        .store
        .release_lease(&released)
        .await
        .expect("a repeated release succeeds");

    // A different runtime instance no longer has to wait out the 30s TTL,
    // and taking over still increments the fencing token (§7) — release
    // keeps the lease row rather than resetting the token to `0`.
    let taken_over = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("another holder acquires a released lease");
    assert_eq!(taken_over.fencing_token, released.fencing_token + 1);

    // The released lease's token is stale: it can never write again.
    let result = harness
        .store
        .append(
            loop_id,
            1,
            released.fencing_token,
            vec![LoopEvent::LoopCompleted],
        )
        .await;
    assert!(matches!(result, Err(StoreError::StaleFencingToken { .. })));
    assert_eq!(harness.event_sequences(loop_id).await, vec![1]);
}

#[tokio::test]
async fn releasing_a_superseded_lease_leaves_the_current_one_held() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let holder = RuntimeInstanceId::new();
    let superseded = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("first acquisition succeeds");
    let current = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: holder,
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("same holder reacquires");
    assert!(current.fencing_token > superseded.fencing_token);

    // Releasing the old lease must not cut the current one short.
    harness
        .store
        .release_lease(&superseded)
        .await
        .expect("releasing a superseded lease is a no-op");
    let conflict = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await;
    assert!(matches!(conflict, Err(StoreError::LeaseConflict(id)) if id == loop_id));

    // ...and the current holder can still renew it.
    harness
        .store
        .renew_lease(&current)
        .await
        .expect("the current lease is still held");
}

#[tokio::test]
async fn renewing_a_lease_that_was_never_acquired_is_rejected() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let phantom = loopward_runtime::ports::LoopLease {
        loop_id,
        holder_id: RuntimeInstanceId::new(),
        fencing_token: 1,
        acquired_at: time::OffsetDateTime::UNIX_EPOCH,
        expires_at: time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30),
    };

    let result = harness.store.renew_lease(&phantom).await;
    assert!(matches!(result, Err(StoreError::LeaseNotHeld(id)) if id == loop_id));
}

#[tokio::test]
async fn a_lease_cannot_be_acquired_for_a_loop_that_does_not_exist_yet() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();

    // `loop_leases.loop_id` is a foreign key onto `loops(loop_id)`: a Loop
    // becomes leasable once its genesis Checkpoint has been committed, and
    // an uncreated Loop's current fencing token is therefore `0`.
    let result = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await;
    assert!(matches!(result, Err(StoreError::LoopNotFound(id)) if id == loop_id));

    commit_genesis(&harness, loop_id).await;
    let lease = harness
        .store
        .acquire_lease(AcquireLease {
            loop_id,
            holder_id: RuntimeInstanceId::new(),
            ttl: time::Duration::seconds(30),
        })
        .await
        .expect("a created loop is leasable");
    assert_eq!(lease.fencing_token, 1);
}

// ---------------------------------------------------------------------------
// Atomicity: a failed transaction leaves no partial trace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failed_checkpoint_transaction_leaves_the_prior_checkpoint_intact() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    // Force a mid-transaction failure: `checkpoints.checkpoint_id` is
    // UNIQUE, so reusing the genesis Checkpoint's identity fails *after*
    // the events and the projection row have already been written inside
    // the same transaction.
    let duplicate = Checkpoint {
        id: genesis.id,
        loop_id,
        version: 2,
        aggregate_revision: 2,
        parent: Some(genesis.id),
        episode_id: None,
        integrity_digest: None,
    };
    let failure = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: duplicate.clone(),
            events: vec![LoopEvent::CheckpointCommitted {
                checkpoint: duplicate,
            }],
            state: genesis_state(loop_id, checkpoint_at(loop_id, 2, 2, Some(genesis.id))),
        })
        .await;
    assert!(failure.is_err(), "the duplicate commit must fail");

    // The prior Checkpoint is intact and still authoritative.
    let loaded = harness.store.load(loop_id).await.expect("loop still loads");
    assert_eq!(loaded.state.checkpoint.id, genesis.id);
    assert_eq!(loaded.state.execution.checkpoint_version, 1);
    assert_eq!(loaded.state.execution.aggregate_revision, 1);
    assert_eq!(
        harness
            .count(
                "SELECT COUNT(*) FROM checkpoints WHERE loop_id = ?1",
                loop_id
            )
            .await,
        1
    );
}

#[tokio::test]
async fn the_event_journal_and_the_projection_commit_atomically() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    let before_revision = harness
        .count(
            "SELECT aggregate_revision FROM loops WHERE loop_id = ?1",
            loop_id,
        )
        .await;
    let before_sequences = harness.event_sequences(loop_id).await;
    assert_eq!(before_sequences, vec![1]);

    // Three events would be journalled and the projection advanced before
    // the duplicate Checkpoint identity aborts the transaction.
    let duplicate = Checkpoint {
        id: genesis.id,
        loop_id,
        version: 2,
        aggregate_revision: 4,
        parent: Some(genesis.id),
        episode_id: None,
        integrity_digest: None,
    };
    let failure = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: duplicate.clone(),
            events: vec![
                LoopEvent::EpisodeCommitting {
                    episode_id: EpisodeId::new(),
                },
                LoopEvent::LoopCompleted,
                LoopEvent::CheckpointCommitted {
                    checkpoint: duplicate,
                },
            ],
            state: genesis_state(loop_id, checkpoint_at(loop_id, 2, 4, Some(genesis.id))),
        })
        .await;
    assert!(failure.is_err());

    // No orphaned journal rows, and no advanced projection row.
    assert_eq!(
        harness.event_sequences(loop_id).await,
        before_sequences,
        "a rolled-back transaction must leave no journal rows behind"
    );
    assert_eq!(
        harness
            .count(
                "SELECT aggregate_revision FROM loops WHERE loop_id = ?1",
                loop_id
            )
            .await,
        before_revision,
        "a rolled-back transaction must not advance the projection"
    );
    let loaded = harness.store.load(loop_id).await.expect("loop still loads");
    assert_eq!(loaded.state.execution.aggregate_revision, 1);
    assert_eq!(loaded.events.len(), 1);

    // The journal has no gap: the next committed transaction continues at 2.
    let appended = harness
        .store
        .append(loop_id, 1, 0, vec![LoopEvent::LoopCompleted])
        .await
        .expect("append after rollback succeeds");
    assert_eq!(appended.events.len(), 2);
    assert_eq!(harness.event_sequences(loop_id).await, vec![1, 2]);
}

#[tokio::test]
async fn event_sequences_are_gap_free_across_committed_transactions() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    let stored = harness
        .store
        .append(loop_id, 1, 0, vec![LoopEvent::LoopCompleted])
        .await
        .expect("append succeeds");

    assert_eq!(harness.event_sequences(loop_id).await, vec![1, 2]);
    assert_eq!(stored.state.execution.aggregate_revision, 2);
    let revisions: Vec<u64> = stored
        .events
        .iter()
        .map(|event| event.aggregate_revision)
        .collect();
    assert_eq!(revisions, vec![1, 2]);
    // Round-tripped domain facts survive serialization unchanged.
    assert_eq!(stored.events[1].event, LoopEvent::LoopCompleted);
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

#[tokio::test]
async fn finalized_resources_are_content_addressed_and_abort_removes_only_its_own_pending_file() {
    let harness = Harness::open().await;

    let handle = harness
        .broker
        .begin(ResourceBeginHint {
            media_type: Some("text/plain".to_owned()),
            summary: Some("boot manifest".to_owned()),
        })
        .await
        .expect("begin succeeds");
    harness
        .broker
        .write(&handle, b"hello ")
        .await
        .expect("write succeeds");
    harness
        .broker
        .write(&handle, b"loopward")
        .await
        .expect("write succeeds");
    let finalized = harness
        .broker
        .finalize(handle.clone())
        .await
        .expect("finalize succeeds");

    // SHA-256("hello loopward"), computed independently of this crate.
    let digest_hex = "967b5d2b3027995f1be3f3ee8086f0ee62dc973af417500fe98ad0e4f9bd11d8";
    assert_eq!(
        finalized.digest.as_deref(),
        Some(format!("sha256:{digest_hex}").as_str()),
        "content is addressed by the SHA-256 of the bytes actually written"
    );
    assert_eq!(finalized.size, Some(14));
    assert_eq!(finalized.uri, format!("resource://sha256/{digest_hex}"));

    // Content lives under <resource_root>/sha256/<digest>.
    let content_path = harness.resource_root().join("sha256").join(digest_hex);
    assert!(
        content_path.is_file(),
        "content is content-addressed on disk"
    );
    assert_eq!(
        std::fs::read(&content_path).expect("content readable"),
        b"hello loopward"
    );

    // The finalized bytes resolve back through the port.
    assert_eq!(
        harness
            .broker
            .resolve(&finalized)
            .await
            .expect("resolve succeeds"),
        b"hello loopward"
    );

    // The pending file is gone, and the handle is spent.
    assert!(
        !pending_files(&harness.resource_root())
            .iter()
            .any(|path| path.ends_with(handle.as_str().trim_start_matches("resource://pending/"))),
        "finalize consumes its pending file"
    );
    let reused = harness.broker.finalize(handle).await;
    assert!(matches!(reused, Err(ResourceError::UnknownHandle(_))));

    // Aborting a *different* pending write removes only its own file and
    // never touches finalized content.
    let doomed = harness
        .broker
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    harness
        .broker
        .write(&doomed, b"never durable")
        .await
        .expect("write succeeds");
    assert_eq!(pending_files(&harness.resource_root()).len(), 1);
    harness
        .broker
        .abort(doomed.clone())
        .await
        .expect("abort succeeds");
    assert_eq!(pending_files(&harness.resource_root()).len(), 0);
    assert!(
        content_path.is_file(),
        "abort must never touch finalized content"
    );
    assert_eq!(
        harness
            .broker
            .resolve(&finalized)
            .await
            .expect("finalized content still resolves"),
        b"hello loopward"
    );
    assert!(matches!(
        harness.broker.write(&doomed, b"more").await,
        Err(ResourceError::UnknownHandle(_))
    ));
}

fn pending_files(root: &Path) -> Vec<PathBuf> {
    let pending = root.join("pending");
    if !pending.is_dir() {
        return Vec::new();
    }
    std::fs::read_dir(pending)
        .expect("pending directory readable")
        .map(|entry| entry.expect("entry readable").path())
        .collect()
}

#[tokio::test]
async fn unfinalized_resources_cannot_enter_a_checkpoint() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    // A begun-but-not-finalized resource.
    let handle = harness
        .broker
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    harness
        .broker
        .write(&handle, b"work in progress")
        .await
        .expect("write succeeds");
    let pending_ref = ResourceRef {
        uri: handle.as_str().to_owned(),
        media_type: None,
        digest: None,
        version: None,
        size: None,
        summary: None,
    };

    let next = checkpoint_at(loop_id, 2, 2, Some(genesis.id));
    let state = state_with_session(
        loop_id,
        next.clone(),
        EpisodeId::new(),
        SessionId::new(),
        SessionStatus::Running,
        pending_ref,
    );

    let rejected = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: next.clone(),
            events: vec![LoopEvent::CheckpointCommitted {
                checkpoint: next.clone(),
            }],
            state: state.clone(),
        })
        .await
        .expect_err("an unfinalized resource cannot enter a checkpoint");
    assert!(matches!(
        backend_error(&rejected),
        SqliteStoreError::ResourceNotFinalized { .. }
    ));

    // Nothing partial survived the rejection.
    assert_eq!(
        harness
            .count(
                "SELECT COUNT(*) FROM checkpoints WHERE loop_id = ?1",
                loop_id
            )
            .await,
        1
    );
    assert_eq!(harness.event_sequences(loop_id).await, vec![1]);
    assert_eq!(
        harness
            .count(
                "SELECT COUNT(*) FROM resource_edges WHERE loop_id = ?1",
                loop_id
            )
            .await,
        0
    );

    // Finalizing the same content makes the very same commit succeed.
    let finalized = harness
        .broker
        .finalize(handle)
        .await
        .expect("finalize succeeds");
    let mut session_state = state;
    if let Some(open) = session_state.session.as_mut() {
        open.boot_manifest_ref = finalized.clone();
    }
    harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: next.clone(),
            events: vec![LoopEvent::CheckpointCommitted { checkpoint: next }],
            state: session_state,
        })
        .await
        .expect("a finalized resource may enter a checkpoint");
    assert_eq!(
        harness
            .count(
                "SELECT COUNT(*) FROM resource_edges WHERE loop_id = ?1",
                loop_id
            )
            .await,
        1
    );
}

// ---------------------------------------------------------------------------
// Lifecycle invariants projected into typed tables
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_loop_has_at_most_one_active_episode() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    let first_episode = EpisodeId::new();
    let second = checkpoint_at(loop_id, 2, 2, Some(genesis.id));
    let mut state = genesis_state(loop_id, second.clone());
    state.execution.active_episode = Some(first_episode);
    state.episode = Some(episode(loop_id, first_episode, EpisodeStatus::Executing));
    harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: second.clone(),
            events: records_commit(&second),
            state,
        })
        .await
        .expect("the first episode commits");

    // A second, distinct Episode while the first is still non-terminal.
    let third = checkpoint_at(loop_id, 3, 3, Some(second.id));
    let second_episode = EpisodeId::new();
    let mut state = genesis_state(loop_id, third.clone());
    state.execution.active_episode = Some(second_episode);
    state.episode = Some(episode(loop_id, second_episode, EpisodeStatus::Executing));
    let rejected = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 2,
            expected_checkpoint_version: 2,
            fencing_token: 0,
            events: records_commit(&third),
            checkpoint: third,
            state,
        })
        .await
        .expect_err("a second active episode is rejected");
    assert!(matches!(
        backend_error(&rejected),
        SqliteStoreError::ActiveEpisodeExists { .. }
    ));
}

#[tokio::test]
async fn closed_sessions_cannot_reopen() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    let genesis = commit_genesis(&harness, loop_id).await;

    let manifest = finalized_resource(&harness, b"boot manifest").await;
    let episode_id = EpisodeId::new();
    let session_id = SessionId::new();

    let second = checkpoint_at(loop_id, 2, 2, Some(genesis.id));
    harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 1,
            expected_checkpoint_version: 1,
            fencing_token: 0,
            checkpoint: second.clone(),
            events: records_commit(&second),
            state: state_with_session(
                loop_id,
                second.clone(),
                episode_id,
                session_id,
                SessionStatus::Closed,
                manifest.clone(),
            ),
        })
        .await
        .expect("closing the session commits");

    // Reopening the very same Session generation is refused.
    let third = checkpoint_at(loop_id, 3, 3, Some(second.id));
    let reopened = state_with_session(
        loop_id,
        third.clone(),
        episode_id,
        session_id,
        SessionStatus::Running,
        manifest,
    );
    let rejected = harness
        .store
        .commit_checkpoint(CheckpointCommitRequest {
            loop_id,
            expected_revision: 2,
            expected_checkpoint_version: 2,
            fencing_token: 0,
            events: records_commit(&third),
            checkpoint: third,
            state: reopened,
        })
        .await
        .expect_err("a closed session cannot reopen");
    assert!(matches!(
        backend_error(&rejected),
        SqliteStoreError::ClosedSessionReopened { .. }
    ));
}

async fn finalized_resource(harness: &Harness, bytes: &[u8]) -> ResourceRef {
    let handle = harness
        .broker
        .begin(ResourceBeginHint::default())
        .await
        .expect("begin succeeds");
    harness
        .broker
        .write(&handle, bytes)
        .await
        .expect("write succeeds");
    harness
        .broker
        .finalize(handle)
        .await
        .expect("finalize succeeds")
}

// ---------------------------------------------------------------------------
// Connection configuration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_pooled_connection_is_configured_for_durability() {
    let harness = Harness::open().await;

    // Every pooled connection — not merely the first — must come up in WAL
    // mode with foreign keys enforced, the configured busy timeout, and
    // `synchronous = NORMAL` (`docs/03-durable-state-and-transactions.md`
    // §8). Holding all four at once forces the pool to open four *distinct*
    // connections, which is what makes this a claim about the pool's
    // connect options rather than about one lucky connection.
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(
            harness
                .store
                .pool()
                .acquire()
                .await
                .expect("connection acquires"),
        );
    }

    for connection in &mut connections {
        let journal: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(&mut **connection)
            .await
            .expect("journal_mode readable")
            .get(0);
        assert_eq!(journal.to_lowercase(), "wal");

        let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
            .fetch_one(&mut **connection)
            .await
            .expect("foreign_keys readable")
            .get(0);
        assert_eq!(foreign_keys, 1);

        let synchronous: i64 = sqlx::query("PRAGMA synchronous")
            .fetch_one(&mut **connection)
            .await
            .expect("synchronous readable")
            .get(0);
        assert_eq!(synchronous, 1, "NORMAL");

        let busy_timeout: i64 = sqlx::query("PRAGMA busy_timeout")
            .fetch_one(&mut **connection)
            .await
            .expect("busy_timeout readable")
            .get(0);
        assert_eq!(busy_timeout, 5_000, "the configured five second default");
    }
}

#[tokio::test]
async fn the_configured_synchronous_level_reaches_every_connection() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = SqliteLoopStore::open(
        SqliteStoreConfig::new(directory.path().join("loopward.db"))
            .with_synchronous(Synchronous::Full)
            .with_busy_timeout(std::time::Duration::from_millis(250)),
    )
    .await
    .expect("store opens and migrates");

    let mut connection = store.pool().acquire().await.expect("connection acquires");
    let synchronous: i64 = sqlx::query("PRAGMA synchronous")
        .fetch_one(&mut *connection)
        .await
        .expect("synchronous readable")
        .get(0);
    assert_eq!(synchronous, 2, "FULL");

    let busy_timeout: i64 = sqlx::query("PRAGMA busy_timeout")
        .fetch_one(&mut *connection)
        .await
        .expect("busy_timeout readable")
        .get(0);
    assert_eq!(busy_timeout, 250);
}
