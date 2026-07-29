# Work Unit and Runner Protocol

## 1. Purpose

A Work Unit is Loopward's public outbound execution boundary.

It is not necessarily one model call, one agent run, or one external workflow. It is:

> One externally observable unit of progress with explicit inputs, constraints, output contract, lifecycle, and verification requirements.

The implementation behind a runner may be a coding agent, research service, command, human process, experiment batch, or another framework. Loopward Core does not classify or inspect that implementation.

## 2. Work Unit Model

```rust
pub struct WorkUnit {
    pub id: WorkUnitId,
    pub kind: WorkUnitKind,
    pub objective: String,
    pub status: WorkUnitStatus,
    pub inputs: Vec<ResourceRef>,
    pub constraints: Vec<ConstraintRef>,
    pub output_contract: OutputContract,
    pub verification_plan: VerificationPlanRef,
    pub runner: RunnerRef,
    pub budget: ResourceBudget,
    pub retry_policy: RetryPolicy,
}
```

```rust
pub enum WorkUnitStatus {
    Pending,
    Active,
    AwaitingVerification,
    Accepted,
    Failed,
    Suspended,
    Cancelled,
}
```

Work Unit status is durable. Runner-internal status is not.

## 3. Work Unit Attempt

Each execution attempt is a separate entity:

```rust
pub struct WorkUnitAttempt {
    pub id: AttemptId,
    pub work_unit_id: WorkUnitId,
    pub ordinal: u32,
    pub runner_ref: RunnerRef,
    pub capabilities_snapshot: RunnerCapabilities,
    pub boot_package_ref: ResourceRef,
    pub idempotency_key: String,
    pub external_execution_ref: Option<OpaqueRef>,
    pub status: AttemptStatus,
    pub result_ref: Option<ResourceRef>,
}
```

A retry never overwrites the prior attempt. It creates a new attempt with lineage.

## 4. Runner Contract

```rust
#[async_trait]
pub trait WorkUnitRunner: Send + Sync {
    async fn capabilities(
        &self,
    ) -> Result<RunnerCapabilities, RunnerError>;

    async fn start(
        &self,
        request: WorkUnitRequest,
    ) -> Result<RunHandle, RunnerError>;

    async fn reconcile(
        &self,
        handle: &RunHandle,
    ) -> Result<ReconcileOutcome, RunnerError>;

    async fn status(
        &self,
        handle: &RunHandle,
    ) -> Result<RunStatus, RunnerError>;

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

    async fn collect(
        &self,
        handle: &RunHandle,
    ) -> Result<WorkUnitResult, RunnerError>;
}
```

Not every runner supports every optional operation. Unsupported behavior is declared in `RunnerCapabilities` and returns a typed `UnsupportedCapability` error.

## 5. Runner Capabilities

```rust
pub struct RunnerCapabilities {
    pub protocol_versions: Vec<String>,
    pub resumable: bool,
    pub supports_reconcile: bool,
    pub supports_events: bool,
    pub supports_suspend: bool,
    pub supports_cancel: bool,
    pub reports_token_usage: bool,
    pub reports_internal_trace_context: bool,
    pub context: ContextCapabilities,
}
```

Capabilities are snapshotted on the attempt. A later runner upgrade must not rewrite the historical capability record.

## 6. Work Unit Request

```rust
pub struct WorkUnitRequest {
    pub protocol_version: String,
    pub loop_id: LoopId,
    pub episode_id: EpisodeId,
    pub session_id: SessionId,
    pub session_generation: u32,
    pub work_unit: WorkUnit,
    pub attempt_id: AttemptId,
    pub idempotency_key: String,
    pub boot_package: BootPackageManifest,
    pub resource_grants: Vec<ResourceGrant>,
    pub output_contract: OutputContract,
    pub budget: ResourceBudget,
    pub deadline: Option<Timestamp>,
    pub trace_context: TraceContext,
}
```

The request carries references and grants, not unrestricted access to the Loop store.

## 7. Work Unit Result

```rust
pub struct WorkUnitResult {
    pub protocol_version: String,
    pub attempt_id: AttemptId,
    pub outcome: WorkUnitOutcome,
    pub outputs: Vec<ResourceRef>,
    pub state_proposals: Vec<StateAssertionProposal>,
    pub observation_proposals: Vec<ObservationProposal>,
    pub decision_proposals: Vec<DecisionProposal>,
    pub verification_evidence: Vec<ResourceRef>,
    pub unresolved: Vec<OpenIssueProposal>,
    pub recommended_continuation: Option<ContinuationProposal>,
    pub usage: UsageReport,
    pub runner_trace_refs: Vec<TraceRef>,
}
```

The result is a proposal boundary:

- it cannot commit a Checkpoint;
- it cannot activate a decision;
- it cannot mark an acceptance criterion satisfied;
- it cannot declare the Loop complete.

Loopward validates and verifies the result before applying a mutation.

## 8. Resource Broker

Runner output is written through a Resource Broker:

```rust
#[async_trait]
pub trait ResourceBroker {
    async fn begin(
        &self,
        metadata: ResourceMetadata,
    ) -> Result<PendingResource, ResourceError>;

    async fn write(
        &self,
        pending: &PendingResource,
        chunk: Bytes,
    ) -> Result<(), ResourceError>;

    async fn finalize(
        &self,
        pending: PendingResource,
    ) -> Result<ResourceRef, ResourceError>;

    async fn abort(
        &self,
        pending: PendingResource,
    ) -> Result<(), ResourceError>;
}
```

Only finalized, digest-bound resources may be referenced by an accepted result.

## 9. Intent Before External Effect

Before calling `start`, Loopward durably records:

- the Work Unit Attempt;
- runner identity and capabilities;
- Boot Package reference;
- idempotency key;
- budget;
- expected aggregate revision.

After `start`, the returned external execution reference is persisted immediately.

This ordering allows recovery to distinguish:

- an attempt never sent;
- an attempt possibly sent but missing a handle;
- an active external execution;
- a completed execution awaiting collection.

## 10. Reconciliation

On restart, Loopward invokes `reconcile` when supported.

```rust
pub enum ReconcileOutcome {
    NotStarted,
    Running,
    Suspended,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}
```

`Unknown` is not treated as failure. If repeating the external effect could be unsafe and idempotency cannot be proven, the Attempt becomes `Ambiguous` and the Loop suspends for policy or human resolution.

## 11. Cancellation and Suspension

Cancellation is cooperative unless the runner guarantees stronger semantics.

Loopward records:

1. cancellation or suspension intent;
2. runner acknowledgement;
3. observed terminal state;
4. any residual resources or side effects.

A timeout while cancelling does not prove that execution stopped.

## 12. Failure Model

Runner errors are separated into:

```text
ProtocolError
UnsupportedCapability
AuthenticationError
AuthorizationError
ResourceError
Unavailable
Timeout
ExecutionFailed
Cancelled
AmbiguousExternalState
InvalidResult
```

Runner-internal retries remain internal unless they affect the public outcome, budget, evidence, or trace references.

## 13. Generic Command Runner

Phase One ships a generic command runner as the reference runner.

It receives:

- a materialized Boot Package directory;
- a Work Unit request file;
- a resource output directory;
- an expected result path;
- environment variables for identifiers and trace propagation.

Representative environment variables:

```text
LOOPWARD_LOOP_ID
LOOPWARD_EPISODE_ID
LOOPWARD_SESSION_ID
LOOPWARD_SESSION_GENERATION
LOOPWARD_WORK_UNIT_ID
LOOPWARD_ATTEMPT_ID
LOOPWARD_REQUEST_PATH
LOOPWARD_RESULT_PATH
LOOPWARD_RESOURCE_DIR
TRACEPARENT
TRACESTATE
```

No runner-specific vendor behavior is built into `loopward-core`.

## 14. Protocol Versioning

Every request, result, and event includes a protocol version.

Rules:

- incompatible major versions are rejected before execution;
- unknown optional fields are preserved or ignored according to schema rules;
- required-field removal requires a major version;
- a wrapper may translate versions but must record the source and target versions.

## 15. Acceptance Criteria

The protocol is acceptable when:

- a command, agent, remote service, or wrapper can implement the same public runner contract;
- one Work Unit may contain complex internal execution without leaking its internal topology;
- every external effect has a durable intent record;
- an interrupted process can reconcile without blindly repeating work;
- runner output cannot directly modify authoritative Loop state;
- cancellation, suspension, and ambiguous execution have distinct semantics;
- resources and results are integrity-bound and versioned.

