# Durable State and Transactions

**Status:** Draft for review  
**Version:** 0.1

## 1. Storage Model

Phase One uses:

```text
current projections
+ immutable Checkpoints
+ append-only lifecycle Event Journal
```

It does not use full event sourcing. Recovery starts from the latest valid Checkpoint and durable execution records, not from replaying all historical events.

## 2. Versioning

Two versions are required:

```text
aggregate_revision
  increments on every durable lifecycle transition

checkpoint_version
  increments only on authoritative Checkpoint commit
```

All writes include expected revision and lease fencing token.

## 3. SQLite Tables

```text
loops
loop_leases
operations
command_idempotency
checkpoints
episodes
runtime_sessions
work_units
work_unit_attempts
verification_runs
suspensions
resources
resource_edges
loop_events
schema_migrations
```

Lifecycle query fields use typed columns. Domain state and versioned event payloads use JSON. Large resource contents are stored outside SQLite.

## 4. Intent-Before-Effect

Every side-effecting runner operation follows:

```text
persist intent
→ perform external effect
→ persist observation
```

### Prepare transaction

Atomically persists:

- Attempt ID;
- stable idempotency key;
- `PREPARED` status;
- `attempt.prepared` event;
- incremented aggregate revision.

### External call

The runner receives Attempt ID, idempotency key, trace context, and bounded request.

### Observation transaction

On success, atomically persists:

- `RUNNING` status;
- opaque external execution reference;
- capability snapshot;
- `attempt.started` event;
- incremented aggregate revision.

If a crash occurs between the call and observation transaction, Recovery MUST reconcile by Attempt ID and idempotency key. Unsafe unknown side effects produce `AMBIGUOUS` and suspend the Loop.

## 5. Checkpoint Commit

Before commit, the Checkpoint Manager validates:

- expected aggregate revision;
- expected Checkpoint version;
- lease fencing token;
- Checkpoint and domain schemas;
- referenced resources and digests;
- pinned-state preservation;
- decision-proposal status;
- verification evidence;
- continuation validity.

One SQLite transaction writes:

```text
immutable Checkpoint
current Loop projection
Work Unit state changes
Episode terminal state
Session terminal state when applicable
verification state
resource edges
checkpoint.committed journal event
aggregate revision
checkpoint version
```

Partial commits are forbidden.

The integrity digest is:

```text
SHA-256(stored serialized Checkpoint bytes)
```

Phase One does not require cross-language canonical JSON.

## 6. Event Journal

Durable events include:

```text
loop.created
episode.started
context.hydrated
session.started
attempt.prepared
attempt.started
attempt.completed
attempt.ambiguous
session.closed
verification.completed
checkpoint.committed
loop.suspended
loop.resumed
loop.completed
```

High-volume progress belongs in tracing or logs unless it materially affects recovery or user decisions.

Each event records Loop sequence, aggregate revision, schema version, timestamp, and current trace/span IDs when available.

## 7. Lease and Fencing

At most one driver may mutate a Loop.

```rust
pub struct LoopLease {
    pub loop_id: LoopId,
    pub holder_id: RuntimeInstanceId,
    pub fencing_token: u64,
    pub acquired_at: Timestamp,
    pub expires_at: Timestamp,
}
```

Reacquisition increments the fencing token. Every write validates it. A stale driver cannot commit even if it resumes after a pause.

External work occurs outside database transactions. The driver renews its lease during long operations. Lease loss stops local commits and begins reconciliation; it does not blindly cancel the runner.

## 8. SQLite Configuration

Phase One defaults:

```text
WAL mode
foreign_keys = ON
busy_timeout configured
synchronous = NORMAL
transactional migrations
```

High-durability deployments MAY choose `synchronous = FULL`.

## 9. Crash Recovery Matrix

| Crash point | Recovery |
|---|---|
| Before runner start | Restart or abort safely |
| After Attempt prepared | Reconcile by idempotency key |
| After start, before external ref persisted | Reconcile; suspend if ambiguous |
| While runner is running | Query runner using external ref |
| After result stored, before Checkpoint | Validate stored result and resume commit |
| During Checkpoint transaction | SQLite rollback preserves prior Checkpoint |
| After Checkpoint, before trace export | Continue; telemetry is non-authoritative |
| During drain | Close failed Session and preserve valid handoff |
| During verification | Restart against the same target digest |

## 10. Storage Invariants

- Checkpoint versions increase monotonically.
- Aggregate revisions increase monotonically.
- One Loop has at most one active Episode.
- Closed Sessions cannot reopen.
- Unfinalized resources cannot enter a Checkpoint.
- Stale fencing tokens cannot write.
- Event sequence is gap-free per committed transaction.
- Journal and projection changes are transactionally consistent.
