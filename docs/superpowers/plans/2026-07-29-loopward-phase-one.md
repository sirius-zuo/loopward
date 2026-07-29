# Loopward Phase One Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Rust Phase One Loopward runtime that advances one durable Loop across bounded Episodes and replaceable Runtime Sessions using structured Checkpoints, deterministic context hydration, verified Work Units, recovery, and backend-neutral tracing.

**Architecture:** A pure `loopward-core` reducer owns lifecycle invariants. `loopward-runtime` coordinates ports for storage, runners, resources, verification, time, identifiers, and telemetry; SQLite and the command runner are concrete adapters. Every Episode begins from a committed Checkpoint, uses a bounded Boot Package, and ends at a new durable boundary.

**Tech Stack:** Rust 1.97.1, edition 2024, Tokio 1.53.1, Serde 1.0.229, SQLx 0.9.0 with SQLite, tracing 0.1.44, OpenTelemetry 0.32.0, Clap 4.6.4.

## Global Constraints

- Loopward Core MUST contain no orchestration, DAG, workflow-topology, agent-registry, or Aether concept.
- Aether MUST remain excluded from Phase One.
- External composition MUST use case-specific wrappers against public APIs.
- A Loop MUST be durable and a Runtime Session MUST be bounded and replaceable.
- Authoritative state MUST live outside runner conversations.
- Context MUST be reconstructed for the current Work Unit.
- Pinned constraints MUST survive hydration and drain losslessly.
- Runner results MUST remain proposals until validation and verification.
- One Episode MUST be one top-level trace; consecutive Episodes MUST be linked through Checkpoints.
- Telemetry storage, visualization, and alerting MUST remain external.
- The runtime MUST operate with tracing disabled and without a telemetry backend.
- The Phase One default MUST be one fresh Runtime Session per Work Unit.
- Rust toolchain MUST be pinned to `1.97.1`; workspace edition MUST be `2024`; workspace `rust-version` MUST be `1.97`.
- Production code MUST forbid unsafe code.
- Every state mutation MUST use optimistic concurrency and lease fencing.
- Every task MUST finish with `cargo fmt --check`, targeted tests, Clippy with warnings denied, and a focused commit.

---

## File and Crate Map

```text
loopward/
├── .github/workflows/ci.yml
├── .gitignore
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── crates/
│   ├── loopward-api/
│   │   └── src/{lib,ids,context,error,event,loop_service,resource,runner,trace,work_unit}.rs
│   ├── loopward-core/
│   │   └── src/{lib,command,event,invariants,model,policy,reducer}.rs
│   ├── loopward-runtime/
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── context/{mod,budget,hydrator,manifest}.rs
│   │       ├── controller/{mod,advance}.rs
│   │       ├── controller/service.rs
│   │       ├── ports/{mod,clock,id_generator,resource,store}.rs
│   │       ├── recovery/{mod,reconcile}.rs
│   │       ├── rotation/{mod,policy}.rs
│   │       └── verification/{mod,engine,gate}.rs
│   ├── loopward-store-sqlite/
│   │   ├── migrations/0001_initial.sql
│   │   └── src/{lib,checkpoint,event,lease,resource,store}.rs
│   ├── loopward-observability/
│   │   └── src/{lib,attributes,events,spans}.rs
│   ├── loopward-observability-otel/
│   │   └── src/{lib,exporter,propagation}.rs
│   ├── loopward-runner-command/
│   │   └── src/{lib,process,protocol}.rs
│   ├── loopward-cli/
│   │   └── src/{main,commands,config}.rs
│   └── loopward-testkit/
│       └── src/{lib,clock,failure,fake_runner,trace_recorder}.rs
├── tests/fixtures/{runner,scenario}/
└── docs/superpowers/plans/2026-07-29-loopward-phase-one.md
```

Responsibility rules:

- `loopward-api` contains stable wire-facing contracts only.
- `loopward-core` is synchronous, pure, and I/O-free.
- `loopward-runtime` depends only on ports, never concrete adapters.
- `loopward-store-sqlite`, `loopward-runner-command`, and `loopward-observability-otel` depend inward on public contracts.
- `loopward-cli` is the composition root.
- `loopward-testkit` supplies deterministic fakes and failure injection.

---

### Task 1: Bootstrap the Rust Workspace and Quality Gates

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `.gitignore`
- Create: `.github/workflows/ci.yml`
- Create: `crates/*/Cargo.toml` for all nine crates
- Create: `crates/*/src/lib.rs` for library crates
- Create: `crates/loopward-cli/src/main.rs`
- Test: `crates/loopward-api/tests/workspace_smoke.rs`

**Interfaces:**
- Consumes: no code interfaces.
- Produces: the nine-crate workspace and shared dependency/version policy used by every later task.

- [ ] **Step 1: Add the pinned toolchain and workspace manifest**

Create `rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.97.1"
components = ["clippy", "rustfmt"]
profile = "minimal"
```

Create the root `Cargo.toml` with all nine members, resolver `3`, edition `2024`, `rust-version = "1.97"`, and these workspace dependencies:

```toml
[workspace]
members = ["crates/*"]
resolver = "3"

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.97"
license = "Apache-2.0"

[workspace.dependencies]
async-trait = "0.1.91"
clap = { version = "4.6.4", features = ["derive"] }
jsonschema = "0.49.2"
opentelemetry = "0.32.0"
opentelemetry-otlp = { version = "0.32.0", features = ["grpc-tonic", "trace"] }
opentelemetry_sdk = { version = "0.32.1", features = ["rt-tokio", "trace"] }
serde = { version = "1.0.229", features = ["derive"] }
serde_json = "1.0.151"
sha2 = "0.11.0"
sqlx = { version = "0.9.0", default-features = false, features = ["runtime-tokio", "sqlite", "migrate"] }
tempfile = "3.27.0"
thiserror = "2.0.19"
time = { version = "0.3.54", features = ["serde", "formatting"] }
tokio = { version = "1.53.1", features = ["macros", "process", "rt-multi-thread", "signal", "sync", "time"] }
tracing = "0.1.44"
tracing-opentelemetry = "0.33.0"
tracing-subscriber = { version = "0.3.23", features = ["env-filter", "fmt", "registry"] }
uuid = { version = "1.24.0", features = ["serde", "v7"] }

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = "deny"
pedantic = "deny"
```

- [ ] **Step 2: Create minimal crate manifests and entry points**

Each crate inherits workspace package fields and lints. Library entry points contain:

```rust
#![forbid(unsafe_code)]
```

The CLI entry point contains:

```rust
fn main() {
    println!("loopward 0.1.0");
}
```

- [ ] **Step 3: Write the workspace smoke test**

Create `crates/loopward-api/tests/workspace_smoke.rs`:

```rust
#[test]
fn workspace_uses_phase_one_protocol_version() {
    assert_eq!(loopward_api::PROTOCOL_VERSION, "loopward.api/v0.1");
}
```

Expose in `crates/loopward-api/src/lib.rs`:

```rust
#![forbid(unsafe_code)]

pub const PROTOCOL_VERSION: &str = "loopward.api/v0.1";
```

- [ ] **Step 4: Run formatting, tests, and Clippy**

Run:

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: all commands exit `0`; the smoke test reports one pass.

- [ ] **Step 5: Add CI**

Create `.github/workflows/ci.yml` that checks out the repository, installs toolchain `1.97.1` with `rustfmt` and `clippy`, then runs the three commands from Step 4.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml .gitignore .github crates
git commit -m "chore: bootstrap Loopward Rust workspace"
```

---

### Task 2: Implement Versioned Public Contracts

**Files:**
- Create: `crates/loopward-api/src/ids.rs`
- Create: `crates/loopward-api/src/context.rs`
- Create: `crates/loopward-api/src/error.rs`
- Create: `crates/loopward-api/src/event.rs`
- Create: `crates/loopward-api/src/loop_service.rs`
- Create: `crates/loopward-api/src/resource.rs`
- Create: `crates/loopward-api/src/runner.rs`
- Create: `crates/loopward-api/src/trace.rs`
- Create: `crates/loopward-api/src/work_unit.rs`
- Modify: `crates/loopward-api/src/lib.rs`
- Test: `crates/loopward-api/tests/contract_roundtrip.rs`

**Interfaces:**
- Consumes: `PROTOCOL_VERSION`.
- Produces: `LoopService`, `WorkUnitRunner`, identifier newtypes, `WorkUnitRequest`, `WorkUnitResult`, `TraceContext`, `ResourceRef`, and typed errors.

- [ ] **Step 1: Write failing round-trip tests**

Test that a `WorkUnitRequest` round-trips through JSON without losing:

```rust
assert_eq!(decoded.loop_id, request.loop_id);
assert_eq!(decoded.session_generation, 3);
assert_eq!(decoded.trace_context.traceparent.as_deref(), Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
assert_eq!(decoded.boot_package.pinned_digest, "sha256:pinned");
```

Test that unknown enum variants fail with a typed serialization error rather than silently becoming a known state.

- [ ] **Step 2: Run the contract test and confirm failure**

Run:

```bash
cargo test -p loopward-api --test contract_roundtrip
```

Expected: compilation fails because `WorkUnitRequest` and its supporting types do not exist.

- [ ] **Step 3: Add identifier and resource types**

Implement UUID-backed newtypes with `new()`, `Display`, `FromStr`, `Serialize`, and `Deserialize`:

```rust
LoopId
EpisodeId
SessionId
WorkUnitId
AttemptId
OperationId
EventId
CheckpointId
```

Implement:

```rust
pub struct ResourceRef {
    pub uri: String,
    pub media_type: Option<String>,
    pub digest: Option<String>,
    pub version: Option<String>,
    pub size: Option<u64>,
    pub summary: Option<String>,
}
```

- [ ] **Step 4: Add context, tracing, Work Unit, and runner DTOs**

Implement the exact public shapes from:

- `docs/04-context-lifecycle.md`;
- `docs/05-work-unit-and-runner-protocol.md`;
- `docs/08-tracing-and-observability.md`;
- `docs/schemas/*.yaml`.

Define the object-safe runner boundary:

```rust
#[async_trait::async_trait]
pub trait WorkUnitRunner: Send + Sync {
    async fn capabilities(&self) -> Result<RunnerCapabilities, RunnerError>;
    async fn start(&self, request: WorkUnitRequest) -> Result<RunHandle, RunnerError>;
    async fn reconcile(&self, handle: &RunHandle) -> Result<ReconcileOutcome, RunnerError>;
    async fn status(&self, handle: &RunHandle) -> Result<RunStatus, RunnerError>;
    async fn events(
        &self,
        handle: &RunHandle,
        cursor: Option<EventCursor>,
    ) -> Result<RunnerEventPage, RunnerError>;
    async fn suspend(
        &self,
        handle: &RunHandle,
        reason: SuspensionReason,
    ) -> Result<(), RunnerError>;
    async fn cancel(
        &self,
        handle: &RunHandle,
        reason: CancellationReason,
    ) -> Result<(), RunnerError>;
    async fn collect(&self, handle: &RunHandle) -> Result<WorkUnitResult, RunnerError>;
}
```

- [ ] **Step 5: Add LoopService commands and error codes**

Implement the `LoopService` signature from `docs/09-public-api.md`, including `CommandMetadata`, `RunLimits`, `LoopStatusView`, `OperationHandle`, and stable `LoopwardErrorCode`.

- [ ] **Step 6: Run API verification**

```bash
cargo fmt --all --check
cargo test -p loopward-api
cargo clippy -p loopward-api --all-targets -- -D warnings
```

Expected: round-trip and error tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-api
git commit -m "feat(api): define Loopward execution contracts"
```

---

### Task 3: Implement the Pure Core State Machines

**Files:**
- Create: `crates/loopward-core/src/model.rs`
- Create: `crates/loopward-core/src/command.rs`
- Create: `crates/loopward-core/src/event.rs`
- Create: `crates/loopward-core/src/policy.rs`
- Create: `crates/loopward-core/src/invariants.rs`
- Create: `crates/loopward-core/src/reducer.rs`
- Modify: `crates/loopward-core/src/lib.rs`
- Test: `crates/loopward-core/tests/lifecycle_contract.rs`

**Interfaces:**
- Consumes: identifiers and DTO primitives from `loopward-api`.
- Produces: `AggregateState`, `LifecycleCommand`, `LoopEvent`, `decide`, `evolve`, and invariant validation.

- [ ] **Step 1: Write failing lifecycle tests**

Cover:

```rust
#[test]
fn closed_session_cannot_return_to_running() { /* assert InvalidTransition */ }

#[test]
fn terminal_loop_cannot_start_an_episode() { /* assert InvalidTransition */ }

#[test]
fn accepted_attempt_does_not_complete_work_unit_before_verification() { /* assert AwaitingVerification */ }

#[test]
fn one_loop_has_at_most_one_active_episode() { /* assert InvariantViolation */ }
```

Use explicit state builders in the test; do not hide transitions behind mocks.

- [ ] **Step 2: Confirm the tests fail**

```bash
cargo test -p loopward-core --test lifecycle_contract
```

Expected: compilation fails because the state model and reducer are absent.

- [ ] **Step 3: Implement the aggregate model and commands**

Implement:

```rust
pub struct AggregateState {
    pub execution: LoopExecution,
    pub episode: Option<Episode>,
    pub session: Option<RuntimeSession>,
    pub work_unit: Option<WorkUnit>,
    pub attempt: Option<WorkUnitAttempt>,
    pub checkpoint: Checkpoint,
}

pub enum LifecycleCommand {
    StartEpisode { episode_id: EpisodeId },
    MarkHydrated { manifest: ResourceRef },
    StartSession { session_id: SessionId, generation: u32 },
    PrepareAttempt { attempt_id: AttemptId, idempotency_key: String },
    RecordRunnerStarted { external_ref: OpaqueRef },
    RecordResult { result: ResourceRef },
    RecordVerification { outcome: VerificationOutcome },
    RequestRotation { reason: RotationReason },
    CommitCheckpoint { checkpoint: Checkpoint },
    Suspend { reason: SuspensionReason },
    Complete,
    Fail { code: String },
}
```

- [ ] **Step 4: Implement pure decision and evolution**

```rust
pub fn decide(
    state: &AggregateState,
    command: &LifecycleCommand,
    policy: &EffectivePolicy,
) -> Result<Vec<LoopEvent>, DomainError>;

pub fn evolve(
    state: AggregateState,
    event: &LoopEvent,
) -> Result<AggregateState, DomainError>;
```

Call `validate_invariants` after every evolved event in tests.

- [ ] **Step 5: Add property-style transition table tests**

Enumerate every `LoopStatus`, `EpisodeStatus`, `SessionStatus`, and `AttemptStatus`; assert that undocumented transitions are rejected. The expected transition tables come directly from `docs/07-session-rotation.md`.

- [ ] **Step 6: Run core verification**

```bash
cargo fmt --all --check
cargo test -p loopward-core
cargo clippy -p loopward-core --all-targets -- -D warnings
```

Expected: all lifecycle and invariant tests pass without Tokio or I/O dependencies.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-core
git commit -m "feat(core): implement pure loop lifecycle state machines"
```

---

### Task 4: Define Runtime Ports and Deterministic Services

**Files:**
- Create: `crates/loopward-runtime/src/ports/mod.rs`
- Create: `crates/loopward-runtime/src/ports/store.rs`
- Create: `crates/loopward-runtime/src/ports/resource.rs`
- Create: `crates/loopward-runtime/src/ports/clock.rs`
- Create: `crates/loopward-runtime/src/ports/id_generator.rs`
- Modify: `crates/loopward-observability/src/lib.rs`
- Modify: `crates/loopward-runtime/src/lib.rs`
- Test: `crates/loopward-runtime/tests/port_contract.rs`

**Interfaces:**
- Consumes: `AggregateState`, `LoopEvent`, public IDs and resources.
- Produces: runtime `LoopStore`, `ResourceBroker`, `Clock`, and `IdGenerator` ports; observability-owned `TelemetrySink`.

- [ ] **Step 1: Write compile-time port contract tests**

Create fakes implementing every port and a test that constructs:

```rust
RuntimePorts {
    store: Arc<dyn LoopStore>,
    resources: Arc<dyn ResourceBroker>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    telemetry: Arc<dyn loopward_observability::TelemetrySink>,
}
```

Expected fake behavior: deterministic IDs, fixed timestamps, in-memory events, and no-op telemetry.

- [ ] **Step 2: Confirm the test fails**

```bash
cargo test -p loopward-runtime --test port_contract
```

Expected: compilation fails because the ports do not exist.

- [ ] **Step 3: Implement `LoopStore`**

Define methods with expected revisions and fencing tokens:

```rust
async fn load(&self, loop_id: LoopId) -> Result<StoredLoop, StoreError>;
async fn append(
    &self,
    loop_id: LoopId,
    expected_revision: u64,
    fencing_token: u64,
    events: Vec<LoopEvent>,
) -> Result<StoredLoop, StoreError>;
async fn commit_checkpoint(
    &self,
    request: CheckpointCommitRequest,
) -> Result<StoredLoop, StoreError>;
async fn acquire_lease(&self, request: AcquireLease) -> Result<LoopLease, StoreError>;
async fn renew_lease(&self, lease: &LoopLease) -> Result<LoopLease, StoreError>;
```

- [ ] **Step 4: Implement resource, clock, ID, and telemetry boundaries**

`ResourceBroker` uses `begin`, `write`, `finalize`, `abort`, and `resolve`. Define `TelemetrySink` and `NoopTelemetrySink` in `loopward-observability`; `loopward-runtime` consumes that crate and does not redeclare the telemetry contract. Runtime correctness never requires telemetry.

- [ ] **Step 5: Run port verification**

```bash
cargo fmt --all --check
cargo test -p loopward-runtime --test port_contract
cargo test -p loopward-observability
cargo clippy -p loopward-runtime -p loopward-observability --all-targets -- -D warnings
```

Expected: fake-port composition passes.

- [ ] **Step 6: Commit**

```bash
git add crates/loopward-runtime crates/loopward-observability
git commit -m "feat(runtime): define durable runtime ports"
```

---

### Task 5: Implement SQLite Durability, Resources, and Lease Fencing

**Files:**
- Create: `crates/loopward-store-sqlite/migrations/0001_initial.sql`
- Create: `crates/loopward-store-sqlite/src/store.rs`
- Create: `crates/loopward-store-sqlite/src/checkpoint.rs`
- Create: `crates/loopward-store-sqlite/src/event.rs`
- Create: `crates/loopward-store-sqlite/src/lease.rs`
- Create: `crates/loopward-store-sqlite/src/resource.rs`
- Modify: `crates/loopward-store-sqlite/src/lib.rs`
- Test: `crates/loopward-store-sqlite/tests/transaction_contract.rs`

**Interfaces:**
- Consumes: runtime `LoopStore` and `ResourceBroker` ports.
- Produces: `SqliteLoopStore` and `FileResourceBroker`.

- [ ] **Step 1: Write failing transaction tests**

Tests must prove:

- stale `aggregate_revision` is rejected;
- stale fencing token is rejected;
- failed Checkpoint transaction leaves the prior Checkpoint intact;
- event journal and projection commit atomically;
- unfinalized resources cannot enter a Checkpoint;
- a reacquired lease has a larger fencing token.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-store-sqlite --test transaction_contract
```

Expected: compilation fails because `SqliteLoopStore` does not exist.

- [ ] **Step 3: Add the initial migration**

Create tables listed in `docs/03-durable-state-and-transactions.md` with:

```sql
PRAGMA foreign_keys = ON;

CREATE TABLE loops (
    loop_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    aggregate_revision INTEGER NOT NULL,
    checkpoint_version INTEGER NOT NULL,
    projection_json TEXT NOT NULL
);

CREATE TABLE loop_events (
    loop_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    aggregate_revision INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (loop_id, sequence)
);

CREATE TABLE loop_leases (
    loop_id TEXT PRIMARY KEY REFERENCES loops(loop_id) ON DELETE CASCADE,
    holder_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE command_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    PRIMARY KEY (scope, idempotency_key)
);

CREATE TABLE checkpoints (
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    checkpoint_version INTEGER NOT NULL,
    checkpoint_id TEXT NOT NULL UNIQUE,
    aggregate_revision INTEGER NOT NULL,
    parent_checkpoint_id TEXT,
    schema_version TEXT NOT NULL,
    checkpoint_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (loop_id, checkpoint_version)
);

CREATE TABLE episodes (
    episode_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    status TEXT NOT NULL,
    base_checkpoint_version INTEGER NOT NULL,
    terminal_checkpoint_version INTEGER,
    trace_id TEXT,
    UNIQUE (loop_id, ordinal)
);

CREATE TABLE runtime_sessions (
    session_id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES episodes(episode_id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    runner_ref TEXT NOT NULL,
    boot_manifest_uri TEXT NOT NULL,
    external_session_ref TEXT,
    UNIQUE (episode_id, generation)
);

CREATE TABLE work_units (
    work_unit_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    work_unit_json TEXT NOT NULL
);

CREATE TABLE work_unit_attempts (
    attempt_id TEXT PRIMARY KEY,
    work_unit_id TEXT NOT NULL REFERENCES work_units(work_unit_id) ON DELETE CASCADE,
    episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
    ordinal INTEGER NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    boot_package_uri TEXT NOT NULL,
    external_execution_ref TEXT,
    result_uri TEXT,
    UNIQUE (work_unit_id, ordinal),
    UNIQUE (work_unit_id, idempotency_key)
);

CREATE TABLE verification_runs (
    verification_run_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    work_unit_id TEXT REFERENCES work_units(work_unit_id),
    plan_version INTEGER NOT NULL,
    target_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    findings_json TEXT NOT NULL
);

CREATE TABLE suspensions (
    suspension_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    reason_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE resources (
    resource_uri TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    media_type TEXT,
    size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    finalized INTEGER NOT NULL CHECK (finalized IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE TABLE resource_edges (
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    checkpoint_version INTEGER NOT NULL,
    resource_uri TEXT NOT NULL REFERENCES resources(resource_uri),
    relation TEXT NOT NULL,
    PRIMARY KEY (loop_id, checkpoint_version, resource_uri, relation),
    FOREIGN KEY (loop_id, checkpoint_version)
        REFERENCES checkpoints(loop_id, checkpoint_version)
);

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
```

Store large bytes under the configured resource root, not in SQLite.

- [ ] **Step 4: Implement store configuration and transactions**

On open:

```text
journal_mode = WAL
foreign_keys = ON
busy_timeout = configured duration
synchronous = NORMAL by default
```

Use `BEGIN IMMEDIATE` for lease mutation and Checkpoint commit. Use dynamic `sqlx::query` so builds do not require a live database.

- [ ] **Step 5: Implement content-addressed resource finalization**

Write pending bytes under a temporary name, calculate SHA-256, atomically rename to `<resource_root>/sha256/<digest>`, then insert metadata. `abort` removes only its pending file.

- [ ] **Step 6: Run store verification**

```bash
cargo fmt --all --check
cargo test -p loopward-store-sqlite
cargo clippy -p loopward-store-sqlite --all-targets -- -D warnings
```

Expected: all transaction, lease, and resource integrity tests pass against temporary SQLite databases.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-store-sqlite
git commit -m "feat(store): add SQLite checkpoints leases and resources"
```

---

### Task 6: Implement Deterministic Context Hydration

**Files:**
- Create: `crates/loopward-runtime/src/context/mod.rs`
- Create: `crates/loopward-runtime/src/context/budget.rs`
- Create: `crates/loopward-runtime/src/context/manifest.rs`
- Create: `crates/loopward-runtime/src/context/hydrator.rs`
- Test: `crates/loopward-runtime/tests/context_hydration.rs`
- Test fixture: `tests/fixtures/scenario/large-history.json`

**Interfaces:**
- Consumes: `Checkpoint`, `WorkUnit`, `RunnerCapabilities`, `ResourceBroker`, `ContextBudget`.
- Produces: `ContextHydrator::hydrate(HydrationRequest) -> BootPackageManifest`.

- [ ] **Step 1: Write failing hydration tests**

Tests must assert:

- required pinned items are present verbatim;
- missing pinned resources fail before runner start;
- immutable digest mismatch fails;
- required content exceeding the hard limit fails;
- supplemental items are omitted before important items;
- the same input and rule version produce the same manifest digest;
- the 100,000-token history fixture produces a final-verification manifest at or below 30,000 estimated tokens.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-runtime --test context_hydration
```

Expected: compilation fails because `ContextHydrator` does not exist.

- [ ] **Step 3: Implement budget arithmetic**

```rust
pub fn available_boot_tokens(budget: &ContextBudget) -> Result<u64, HydrationError> {
    budget
        .hard_input_limit_tokens
        .checked_sub(budget.reserved_output_tokens)
        .and_then(|v| v.checked_sub(budget.reserved_tool_tokens))
        .and_then(|v| v.checked_sub(budget.safety_margin_tokens))
        .ok_or(HydrationError::InvalidBudget)
}
```

- [ ] **Step 4: Implement ordered deterministic selection**

Selection order:

```text
Required Pinned
→ Required Current Work
→ Applicable Active Decisions
→ Verified Observations
→ Important Current Inputs
→ Recent Accepted Outcomes
→ Supplemental
→ On-Demand References
```

Sort ties by stable `ContextItemId`. Record exclusion reason and estimation method for every candidate.

- [ ] **Step 5: Implement manifest integrity**

Calculate:

```text
pinned_digest = SHA-256(stored pinned bytes)
manifest_digest = SHA-256(stored serialized manifest bytes)
```

Do not summarize required pinned content.

- [ ] **Step 6: Run context verification**

```bash
cargo fmt --all --check
cargo test -p loopward-runtime --test context_hydration
cargo clippy -p loopward-runtime --all-targets -- -D warnings
```

Expected: all deterministic and stress-fixture assertions pass.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-runtime/src/context crates/loopward-runtime/tests/context_hydration.rs tests/fixtures/scenario/large-history.json
git commit -m "feat(context): build bounded deterministic boot packages"
```

---

### Task 7: Implement the Generic Command Runner

**Files:**
- Create: `crates/loopward-runner-command/src/protocol.rs`
- Create: `crates/loopward-runner-command/src/process.rs`
- Modify: `crates/loopward-runner-command/src/lib.rs`
- Create: `tests/fixtures/runner/success.sh`
- Create: `tests/fixtures/runner/invalid-result.sh`
- Create: `tests/fixtures/runner/slow.sh`
- Test: `crates/loopward-runner-command/tests/runner_contract.rs`

**Interfaces:**
- Consumes: `WorkUnitRunner`, `WorkUnitRequest`, `WorkUnitResult`, trace carrier fields.
- Produces: `CommandRunner` implementing `WorkUnitRunner`.

- [ ] **Step 1: Write failing runner tests**

Tests must cover:

- request JSON and Boot Package are materialized in an isolated attempt directory;
- documented `LOOPWARD_*`, `TRACEPARENT`, and `TRACESTATE` variables are set;
- valid result JSON is collected;
- missing or invalid result JSON returns `InvalidResult`;
- stdout and stderr become resources with bounded capture;
- cancellation sends cooperative termination, waits for the configured grace period, and reports whether termination was observed.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-runner-command --test runner_contract
```

Expected: compilation fails because `CommandRunner` is absent.

- [ ] **Step 3: Implement protocol materialization**

Use one directory per Attempt:

```text
<run_root>/<attempt_id>/
├── request.json
├── boot-package.json
├── inputs/
├── outputs/
├── stdout.log
├── stderr.log
└── result.json
```

The runner command receives no Loop database credentials.

- [ ] **Step 4: Implement process lifecycle**

Persist the process handle in memory, expose status, and return an opaque Attempt-scoped handle. Set environment identifiers and W3C trace values only when present.

- [ ] **Step 5: Implement result and capture limits**

Reject result files above the configured maximum, validate protocol major version, deserialize with Serde, and finalize logs through `ResourceBroker`.

- [ ] **Step 6: Run runner verification**

```bash
cargo fmt --all --check
cargo test -p loopward-runner-command
cargo clippy -p loopward-runner-command --all-targets -- -D warnings
```

Expected: success, invalid-result, slow, and cancellation fixtures pass.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-runner-command tests/fixtures/runner
git commit -m "feat(runner): add generic command execution boundary"
```

---

### Task 8: Implement Verification Gates and Completion Assessment

**Files:**
- Create: `crates/loopward-runtime/src/verification/mod.rs`
- Create: `crates/loopward-runtime/src/verification/gate.rs`
- Create: `crates/loopward-runtime/src/verification/engine.rs`
- Test: `crates/loopward-runtime/tests/verification_contract.rs`

**Interfaces:**
- Consumes: `VerificationPlan`, `WorkUnitResult`, target digests, `ResourceBroker`.
- Produces: `VerificationEngine::evaluate` and durable `VerificationFinding` values.

- [ ] **Step 1: Write failing verification tests**

Tests must prove:

- schema and integrity gates run before deterministic gates;
- a required failed gate rejects the result;
- an advisory failed gate does not hide or override a required pass;
- a target digest change marks prior findings stale;
- runner-reported success cannot complete a Work Unit;
- Loop completion fails while any required criterion lacks current evidence.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-runtime --test verification_contract
```

Expected: compilation fails because the verification engine is absent.

- [ ] **Step 3: Implement gate interfaces**

```rust
#[async_trait::async_trait]
pub trait VerificationGate: Send + Sync {
    fn kind(&self) -> VerificationGateKind;
    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError>;
}
```

Implement schema, integrity, deterministic-command, and policy gates in Phase One. Agentic and human gates use the same contract but may return `Suspended` through configured runners or clients.

- [ ] **Step 4: Implement ordered aggregation**

Return `Accepted` only when every required gate passes, every target digest is current, and no blocking finding remains.

- [ ] **Step 5: Implement completion assessment**

```rust
pub fn assess_completion(
    checkpoint: &Checkpoint,
    findings: &[VerificationFinding],
) -> CompletionAssessment;
```

Require current evidence for every required success criterion.

- [ ] **Step 6: Run verification**

```bash
cargo fmt --all --check
cargo test -p loopward-runtime --test verification_contract
cargo clippy -p loopward-runtime --all-targets -- -D warnings
```

Expected: all ordering, stale-target, and completion tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-runtime/src/verification crates/loopward-runtime/tests/verification_contract.rs
git commit -m "feat(verification): gate authoritative progress on evidence"
```

---

### Task 9: Implement Rotation Policy and One-Episode Advance

**Files:**
- Create: `crates/loopward-runtime/src/rotation/mod.rs`
- Create: `crates/loopward-runtime/src/rotation/policy.rs`
- Create: `crates/loopward-runtime/src/controller/mod.rs`
- Create: `crates/loopward-runtime/src/controller/advance.rs`
- Create: `crates/loopward-runtime/src/controller/service.rs`
- Test: `crates/loopward-runtime/tests/advance_episode.rs`

**Interfaces:**
- Consumes: Core reducer, ports, hydrator, runner, verification engine.
- Produces: `LoopController::advance(loop_id) -> OperationOutcome`, `RotationPolicy::evaluate`, and `RuntimeLoopService` implementing the public `LoopService`.

- [ ] **Step 1: Write failing Episode tests**

Cover:

- one `advance` call stops at one committed durable boundary;
- a fresh Session generation is created for each Work Unit;
- verification uses a fresh Session;
- closed generations never reopen;
- soft budget rotates at the next safe boundary;
- hard budget stops new model work and initiates drain;
- drain result cannot commit when pinned digest differs.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-runtime --test advance_episode
```

Expected: compilation fails because `LoopController` and `RotationPolicy` are absent.

- [ ] **Step 3: Implement deterministic rotation evaluation**

```rust
pub struct RotationDecision {
    pub rotate: bool,
    pub mandatory: bool,
    pub primary_reason: Option<RotationReason>,
    pub signals: Vec<RotationSignal>,
}
```

Phase One reasons and precedence must match `docs/07-session-rotation.md`.

- [ ] **Step 4: Implement the happy-path Episode coordinator**

Use this exact order:

```text
acquire lease
→ load Checkpoint
→ start Episode
→ hydrate
→ create Session
→ persist Attempt intent
→ start runner
→ collect
→ drain
→ verify
→ commit Checkpoint
→ close Session and Episode
```

Every external effect occurs outside a database transaction.

- [ ] **Step 5: Implement bounded `run_until`**

Reject a command without at least one of:

```text
max_episodes
deadline
max_resource_cost
```

Loop by calling `advance`; do not create a second lifecycle path.

- [ ] **Step 6: Implement the public LoopService adapter**

`RuntimeLoopService` validates command idempotency and expected aggregate revision, persists an `Operation`, then delegates `advance` and `run_until` to `LoopController`. Its `status`, `events`, and `result` methods return public views rather than store rows or complete Checkpoints.

- [ ] **Step 7: Run controller verification**

```bash
cargo fmt --all --check
cargo test -p loopward-runtime --test advance_episode
cargo clippy -p loopward-runtime --all-targets -- -D warnings
```

Expected: all Episode and rotation boundary tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/loopward-runtime/src/controller crates/loopward-runtime/src/rotation crates/loopward-runtime/tests/advance_episode.rs
git commit -m "feat(runtime): advance loops through bounded episodes"
```

---

### Task 10: Implement Reconciliation and Crash Recovery

**Files:**
- Create: `crates/loopward-runtime/src/recovery/mod.rs`
- Create: `crates/loopward-runtime/src/recovery/reconcile.rs`
- Test: `crates/loopward-runtime/tests/recovery_matrix.rs`

**Interfaces:**
- Consumes: latest Checkpoint, journal tail, active Attempt, runner `reconcile`, resource integrity.
- Produces: `RecoveryManager::recover(loop_id) -> RecoveryOutcome`.

- [ ] **Step 1: Write the eight failing recovery tests**

Create one test for each failure point in `docs/10-mvp-and-evaluation.md`:

```text
before runner start
after external start before handle persistence
during execution
after result materialization
during verification
before Checkpoint transaction
after Checkpoint commit before acknowledgement
during Session drain
```

Assert the exact resumed state and whether the runner may be called again.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-runtime --test recovery_matrix
```

Expected: compilation fails because `RecoveryManager` is absent.

- [ ] **Step 3: Implement recovery inspection**

Load:

```text
latest committed Checkpoint
current aggregate revision
durable journal tail
active Episode, Session, Work Unit, and Attempt
resource integrity status
```

- [ ] **Step 4: Implement reconciliation decisions**

Map `ReconcileOutcome` exactly:

```text
NotStarted → start with same idempotency key
Running → reattach
Suspended → preserve suspension
Completed → collect and validate
Failed → apply retry policy
Cancelled → record terminal Attempt
Unknown → mark Ambiguous and suspend
```

- [ ] **Step 5: Implement lease-loss behavior**

On lease loss, stop local commits. Do not claim the runner stopped and do not cancel it unless policy explicitly requests cancellation.

- [ ] **Step 6: Run recovery verification**

```bash
cargo fmt --all --check
cargo test -p loopward-runtime --test recovery_matrix
cargo clippy -p loopward-runtime --all-targets -- -D warnings
```

Expected: all eight crash scenarios pass with no blind repetition of ambiguous effects.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-runtime/src/recovery crates/loopward-runtime/tests/recovery_matrix.rs
git commit -m "feat(recovery): reconcile interrupted loop execution"
```

---

### Task 11: Implement Backend-Neutral Tracing and OpenTelemetry Adapter

**Files:**
- Create: `crates/loopward-observability/src/spans.rs`
- Create: `crates/loopward-observability/src/attributes.rs`
- Create: `crates/loopward-observability/src/events.rs`
- Modify: `crates/loopward-observability/src/lib.rs`
- Create: `crates/loopward-observability-otel/src/exporter.rs`
- Create: `crates/loopward-observability-otel/src/propagation.rs`
- Modify: `crates/loopward-observability-otel/src/lib.rs`
- Test: `crates/loopward-observability/tests/semantic_conventions.rs`
- Test: `crates/loopward-observability-otel/tests/episode_links.rs`

**Interfaces:**
- Consumes: the `TelemetrySink` contract introduced in `loopward-observability` and Episode/execution identifiers from `loopward-api`.
- Produces: test telemetry, stable semantic conventions, optional OTLP exporter, and W3C propagation; `loopward-runtime` continues to depend only on `loopward-observability`.

- [ ] **Step 1: Write failing semantic-convention tests**

Assert stable span names:

```text
loopward.episode
loopward.context.hydrate
loopward.session.start
loopward.session.run
loopward.session.drain
loopward.session.close
loopward.work_unit.execute
loopward.work_unit.collect
loopward.verification.evaluate
loopward.checkpoint.commit
loopward.recovery.reconcile
loopward.resource.resolve
```

Assert full prompt, Boot Package payload, and resource content are absent from recorded attributes.

- [ ] **Step 2: Write the failing Episode-link test**

Using an in-memory exporter, create two Episodes and assert:

- two separate root traces exist;
- Episode 2 has a `continuation_of` link to Episode 1;
- both record the correct Loop ID and Checkpoint version;
- runner boundary span carries propagated W3C context.

- [ ] **Step 3: Confirm failure**

```bash
cargo test -p loopward-observability
cargo test -p loopward-observability-otel
```

Expected: compilation fails because semantic conventions and adapters are absent.

- [ ] **Step 4: Implement backend-neutral instrumentation**

Expose constants and a no-op/test `TelemetrySink`. Keep IDs in trace attributes and prohibit them from default metric labels.

- [ ] **Step 5: Implement the optional OpenTelemetry adapter**

Read exporter configuration from standard OpenTelemetry environment variables, including `OTEL_EXPORTER_OTLP_ENDPOINT`. Do not provide a default local endpoint and do not configure a telemetry backend or UI.

- [ ] **Step 6: Integrate runtime spans**

Instrument hydration, runner boundary, verification, Checkpoint, rotation, and recovery operations. Treat business rejection and suspension as outcomes distinct from transport errors.

- [ ] **Step 7: Run tracing verification**

```bash
cargo fmt --all --check
cargo test -p loopward-observability -p loopward-observability-otel -p loopward-runtime
cargo clippy -p loopward-observability -p loopward-observability-otel --all-targets -- -D warnings
```

Expected: semantic, link, redaction, no-op, and disabled-tracing tests pass without an external service.

- [ ] **Step 8: Commit**

```bash
git add crates/loopward-observability crates/loopward-observability-otel crates/loopward-runtime
git commit -m "feat(observability): trace bounded episodes and runner boundaries"
```

---

### Task 12: Implement the CLI Composition Root

**Files:**
- Create: `crates/loopward-cli/src/config.rs`
- Create: `crates/loopward-cli/src/commands.rs`
- Modify: `crates/loopward-cli/src/main.rs`
- Test: `crates/loopward-cli/tests/cli_contract.rs`

**Interfaces:**
- Consumes: `LoopService`, SQLite store, command runner, telemetry adapters.
- Produces: the Phase One `loopward` operator CLI.

- [ ] **Step 1: Write failing CLI tests**

Using `assert_cmd`, test:

```text
loopward init
loopward create --spec <json>
loopward advance <loop-id>
loopward run <loop-id> --max-episodes 2
loopward status <loop-id> --json
loopward events <loop-id> --json
loopward suspend <loop-id>
loopward resume <loop-id>
loopward cancel <loop-id>
loopward inspect-checkpoint <loop-id>
loopward verify-store
```

Assert `run` without a finite limit fails with `InvalidRequest`.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-cli --test cli_contract
```

Expected: tests fail because the subcommands do not exist.

- [ ] **Step 3: Implement configuration**

Resolve:

```text
database path
resource root
run root
lease duration
runner profile
tracing enabled
```

Tracing defaults to disabled. Enabling OTLP uses standard environment variables and does not start another service.

- [ ] **Step 4: Implement subcommands as LoopService clients**

Each command constructs a versioned request with idempotency metadata. Human-readable output is concise; `--json` emits versioned response DTOs.

- [ ] **Step 5: Run CLI verification**

```bash
cargo fmt --all --check
cargo test -p loopward-cli
cargo clippy -p loopward-cli --all-targets -- -D warnings
```

Expected: all command, JSON, invalid-limit, and tracing-disabled tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/loopward-cli
git commit -m "feat(cli): expose local durable loop operations"
```

---

### Task 13: Build Testkit, Reference Scenario, and Evaluation Harness

**Files:**
- Create: `crates/loopward-testkit/src/clock.rs`
- Create: `crates/loopward-testkit/src/fake_runner.rs`
- Create: `crates/loopward-testkit/src/failure.rs`
- Create: `crates/loopward-testkit/src/trace_recorder.rs`
- Modify: `crates/loopward-testkit/src/lib.rs`
- Create: `tests/fixtures/scenario/retry-loop.json`
- Create: `tests/fixtures/scenario/expected-events.json`
- Create: `crates/loopward-cli/tests/e2e_phase_one.rs`
- Create: `docs/phase-one-evaluation-template.md`

**Interfaces:**
- Consumes: all Phase One public APIs and adapters.
- Produces: deterministic conformance harness, five-Episode reference scenario, and evaluation report template.

- [ ] **Step 1: Write the failing end-to-end test**

The test must execute:

```text
Inspect
→ Implement
→ Verify rejected
→ Remediate
→ Final Verify accepted
→ Complete
```

Assert:

- five Episodes and five top-level traces;
- fresh Session generation for each Work Unit;
- at least one rejected result;
- one remediation lineage;
- required constraints unchanged;
- final success criteria supported by current evidence;
- no full history in the final Boot Package;
- the expected durable event sequence.

- [ ] **Step 2: Confirm failure**

```bash
cargo test -p loopward-cli --test e2e_phase_one
```

Expected: the scenario fails because testkit fakes and fixtures are absent.

- [ ] **Step 3: Implement deterministic testkit components**

`FakeRunner` accepts scripted outcomes by Attempt ordinal. `FailureInjector` exposes the eight named crash points. `TraceRecorder` returns recorded spans and links. `FixedClock` advances only when the test requests it.

- [ ] **Step 4: Implement the reference fixture**

The JSON fixture declares:

- Loop goal and success criteria;
- pinned public-contract constraint;
- five Work Units;
- deterministic verification commands;
- one failed verification finding;
- one remediation Work Unit;
- final accepted result.

- [ ] **Step 5: Add the evaluation template**

The report table records per Episode:

```text
Boot Package estimated tokens and method
reported input/output tokens or unavailable
accepted state transitions
repeated resource reads
rotation reason
verification outcome
recovery events
trace reference
```

- [ ] **Step 6: Run testkit and end-to-end verification**

```bash
cargo fmt --all --check
cargo test -p loopward-testkit
cargo test -p loopward-cli --test e2e_phase_one
cargo clippy -p loopward-testkit --all-targets -- -D warnings
```

Expected: deterministic scenario and all failure injection tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/loopward-testkit crates/loopward-cli/tests/e2e_phase_one.rs tests/fixtures docs/phase-one-evaluation-template.md
git commit -m "test: add Phase One conformance and evaluation harness"
```

---

### Task 14: Run the Phase One Acceptance Gate

**Files:**
- Modify: `docs/README.md`
- Create: `docs/phase-one-conformance.md`
- Modify only if verification finds a mismatch: contract schemas under `docs/schemas/`

**Interfaces:**
- Consumes: the complete Phase One workspace.
- Produces: recorded conformance evidence and a release-ready `0.1.0` baseline.

- [ ] **Step 1: Verify architecture boundaries**

Run:

```bash
rg -n "Aether|OrchestrationProvider|DAG|workflow topology|agent registry" crates
```

Expected: no matches in production crates. Test strings that explicitly assert absence are allowed only when the command is narrowed and reviewed.

- [ ] **Step 2: Verify forbidden observability coupling**

Run:

```bash
rg -n "Grafana|Tempo|Jaeger|localhost|Web Preview" crates .github Cargo.toml
```

Expected: no matches.

- [ ] **Step 3: Run the complete Rust quality gate**

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

Expected: every command exits `0`, no tests fail, and documentation builds without warnings.

- [ ] **Step 4: Run the Phase One behavioral gate**

Run:

```bash
cargo test -p loopward-cli --test e2e_phase_one
cargo test -p loopward-runtime --test recovery_matrix
cargo test -p loopward-runtime --test context_hydration
cargo test -p loopward-observability-otel --test episode_links
```

Expected:

- all eight recovery cases pass;
- final Boot Package fixture is at most 30,000 estimated tokens;
- required constraints remain byte-equivalent;
- each Episode is a separate linked trace;
- tests use no external telemetry service.

- [ ] **Step 5: Record conformance evidence**

Create `docs/phase-one-conformance.md` with:

- commit SHA;
- Rust and Cargo versions;
- dependency lockfile digest;
- commands executed;
- pass/fail counts;
- context stress result;
- recovery matrix result;
- tracing-disabled result;
- known, estimated, and unavailable token measurements.

- [ ] **Step 6: Update the documentation index**

Link the implementation plan, evaluation template, and conformance report from `docs/README.md`. Keep wiki generation outside this implementation phase.

- [ ] **Step 7: Commit**

```bash
git add docs
git commit -m "docs: record Loopward Phase One conformance"
```

---

## Execution Order and Review Gates

Execute Tasks 1–14 in order. Review gates:

```text
Gate A after Task 3:
  Public contracts and pure lifecycle model

Gate B after Task 6:
  Durable store and context lifecycle

Gate C after Task 10:
  Executable Loop, verification, rotation, and recovery

Gate D after Task 13:
  Tracing, CLI, and deterministic end-to-end scenario

Final gate after Task 14:
  Phase One acceptance evidence
```

Do not start the next gate group while the prior group's targeted tests or Clippy checks fail.

## Specification Coverage

| Design document | Implemented by |
|---|---|
| `00-project-charter.md` | Global constraints, Tasks 1 and 14 |
| `01-system-architecture.md` | Tasks 1, 4, 5, 7, 11, and 12 |
| `02-core-domain-model.md` | Tasks 2 and 3 |
| `03-durable-state-and-transactions.md` | Tasks 4, 5, and 10 |
| `04-context-lifecycle.md` | Task 6 and context stress gate in Tasks 13–14 |
| `05-work-unit-and-runner-protocol.md` | Tasks 2 and 7 |
| `06-verification-and-recovery.md` | Tasks 8 and 10 |
| `07-session-rotation.md` | Task 9 |
| `08-tracing-and-observability.md` | Task 11 |
| `09-public-api.md` | Tasks 2, 9, and 12 |
| `10-mvp-and-evaluation.md` | Tasks 13 and 14 |
| `11-risks-and-open-questions.md` | Global constraints, explicit suspension, integrity tests, and final conformance review |
| `docs/schemas/*.yaml` | Contract parity tests in Tasks 2, 5, 6, and 7 |

## Implementation Recommendations

1. Keep `loopward-core` synchronous and free of Tokio. This makes the state machine deterministic and easy to exhaustively test.
2. Use dynamic SQLx queries in Phase One. Compile-time SQL checking would require a build database and adds friction before schema stability.
3. Pin the current Rust toolchain and `Cargo.lock`. Revisit an N-minus compatibility policy only after the first external consumer exists.
4. Use the command runner as the only real execution adapter in Phase One. Vendor-specific agent commands belong in runner profiles, not Core crates.
5. Treat the 30,000-token Boot Package limit as a fixture threshold, not a global default.
6. Keep OpenTelemetry optional and test it in memory. Do not make telemetry availability part of runtime correctness.
7. Prefer explicit suspension over automatic retry whenever external side effects are ambiguous.
