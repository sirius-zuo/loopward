# Public API

## 1. Purpose

The public API exposes Loopward as a durable execution service and exposes Work Unit execution as an outbound boundary.

It does not expose runner internals, Runtime Session conversation history, or domain-specific state as unversioned implementation details.

## 2. Inbound Loop Service

```rust
#[async_trait]
pub trait LoopService: Send + Sync {
    async fn create(
        &self,
        request: CreateLoopRequest,
    ) -> Result<LoopHandle, LoopwardError>;

    async fn advance(
        &self,
        command: AdvanceLoopCommand,
    ) -> Result<OperationHandle, LoopwardError>;

    async fn run_until(
        &self,
        command: RunUntilCommand,
    ) -> Result<OperationHandle, LoopwardError>;

    async fn status(
        &self,
        loop_id: LoopId,
    ) -> Result<LoopStatusView, LoopwardError>;

    async fn events(
        &self,
        query: EventQuery,
    ) -> Result<EventPage, LoopwardError>;

    async fn suspend(
        &self,
        command: SuspendLoopCommand,
    ) -> Result<OperationHandle, LoopwardError>;

    async fn resume(
        &self,
        command: ResumeLoopCommand,
    ) -> Result<OperationHandle, LoopwardError>;

    async fn cancel(
        &self,
        command: CancelLoopCommand,
    ) -> Result<OperationHandle, LoopwardError>;

    async fn result(
        &self,
        loop_id: LoopId,
    ) -> Result<Option<LoopResult>, LoopwardError>;
}
```

## 3. Finite Operations

Public calls do not conceal unbounded work.

- `advance` progresses to the next safe durable boundary.
- `run_until` requires explicit limits.
- long operations return an `OperationHandle`.
- clients observe status or events using cursors.
- cancellation is explicit.

```rust
pub struct RunUntilCommand {
    pub metadata: CommandMetadata,
    pub loop_id: LoopId,
    pub condition: RunCondition,
    pub limits: RunLimits,
}

pub struct RunLimits {
    pub max_episodes: Option<u32>,
    pub deadline: Option<Timestamp>,
    pub max_resource_cost: Option<ResourceCost>,
}
```

At least one finite limit is required.

## 4. Command Metadata

```rust
pub struct CommandMetadata {
    pub request_id: String,
    pub idempotency_key: String,
    pub expected_revision: Option<u64>,
    pub principal: Option<PrincipalRef>,
    pub trace_context: TraceContext,
}
```

Mutating commands are idempotent within their documented scope. `expected_revision` provides optimistic concurrency control.

## 5. Loop Creation

```rust
pub struct CreateLoopRequest {
    pub metadata: CommandMetadata,
    pub protocol_version: String,
    pub kind: LoopKind,
    pub goal: Goal,
    pub success_criteria: Vec<SuccessCriterion>,
    pub pinned_constraints: Vec<Constraint>,
    pub domain_schema: SchemaRef,
    pub initial_domain_state: Value,
    pub policies: LoopPolicies,
}
```

Creation validates schemas and policies before committing the initial Checkpoint.

## 6. Status View

The default status view is intentionally compact:

```rust
pub struct LoopStatusView {
    pub loop_id: LoopId,
    pub status: LoopStatus,
    pub phase: PhaseRef,
    pub aggregate_revision: u64,
    pub checkpoint_version: u64,
    pub active_episode: Option<EpisodeRef>,
    pub active_work_unit: Option<WorkUnitRef>,
    pub suspension: Option<SuspensionView>,
    pub terminal_outcome: Option<LoopOutcome>,
}
```

It does not return the complete Checkpoint, full event history, or sensitive resource content.

## 7. Events

Loopward distinguishes:

- durable lifecycle events used for audit and recovery;
- ephemeral progress events emitted by runners.

Durable events have monotonically ordered journal positions. Ephemeral events may be sampled or unavailable after execution.

```rust
pub struct EventQuery {
    pub loop_id: LoopId,
    pub after: Option<EventCursor>,
    pub limit: u32,
    pub durability: EventDurabilityFilter,
}
```

## 8. Outbound Work Unit Boundary

The outbound API is the `WorkUnitRunner` protocol defined in [Work Unit and Runner Protocol](05-work-unit-and-runner-protocol.md).

Loopward Core does not require the runner to be an agent and does not expose a generic orchestration API.

## 9. Administrative Access

Administrative APIs may expose:

- Checkpoint metadata;
- integrity validation;
- lease status;
- recovery operations;
- redacted Boot Package manifests;
- resource metadata;
- tracing health.

They must be authorization-controlled and separate from normal Loop execution APIs.

## 10. Error Model

```rust
pub enum LoopwardErrorCode {
    InvalidRequest,
    UnsupportedVersion,
    NotFound,
    Conflict,
    LeaseUnavailable,
    InvalidStateTransition,
    BudgetExceeded,
    HydrationFailed,
    RunnerFailed,
    VerificationFailed,
    RecoveryRequired,
    AmbiguousExternalState,
    Suspended,
    Cancelled,
    Internal,
}
```

Errors include a stable code, sanitized message, retry guidance, and trace correlation reference where available.

## 11. Versioning

The following are independently versioned:

- service protocol;
- Checkpoint schema;
- Work Unit request/result protocol;
- Boot Package manifest;
- durable event schema;
- domain state schema.

A Loop records the versions used at creation and at each migration.

## 12. Security

Public APIs enforce:

- caller identity where deployed;
- authorization by Loop and operation;
- resource-grant scoping;
- input size and schema limits;
- redaction of secrets and sensitive payloads;
- audit records for administrative mutation;
- protection against untrusted runner output.

## 13. Wrapper Integration

External systems integrate through public boundaries, case by case.

### 13.1 External system as a Work Unit

A wrapper implements `WorkUnitRunner` and translates between:

```text
Loopward WorkUnitRequest
↔ external public API
↔ Loopward WorkUnitResult
```

Loopward does not need to know whether the external system internally coordinates many executions.

### 13.2 Loopward as an external worker

A wrapper implements the external system's worker contract and invokes `LoopService`.

The external system sees one worker execution. Loopward retains ownership of its Episodes, Runtime Sessions, Checkpoints, verification, and recovery.

### 13.3 State ownership

Each framework owns its authoritative state. Cross-system execution references are opaque. Wrappers map public outcomes, cancellation, suspension, and trace context without copying internal state machines.

### 13.4 Aether

Aether is a possible future example of both wrapper directions. It is not a Loopward dependency, does not appear in Loopward Core types, and is excluded from Phase One.

## 14. Acceptance Criteria

The public API is acceptable when:

- clients can start, advance, observe, suspend, resume, cancel, and collect a Loop;
- all mutating commands are idempotent and concurrency-aware;
- an external runner can integrate without access to the Loop store;
- Loopward can be wrapped as a worker using only its inbound API;
- internal topology and conversation state do not leak across boundaries;
- version negotiation and typed errors are explicit;
- no orchestration concept is required in Loopward Core.

