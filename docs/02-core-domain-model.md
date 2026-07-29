# Core Domain Model

**Status:** Draft for review  
**Version:** 0.1

## 1. Principal Entities

```text
LoopExecution
Episode
RuntimeSession
WorkUnit
WorkUnitAttempt
Checkpoint
Observation
Decision
AcceptanceCriterion
VerificationPlan
ResourceReference
Continuation
```

## 2. LoopExecution

The durable identity of one long-running task.

```rust
pub struct LoopExecution {
    pub id: LoopId,
    pub kind: LoopKind,
    pub status: LoopStatus,
    pub phase: PhaseRef,
    pub aggregate_revision: u64,
    pub checkpoint_version: u64,
    pub active_episode: Option<EpisodeId>,
    pub active_work_unit: Option<WorkUnitId>,
    pub budget: ResourceBudget,
}
```

Invariants:

- at most one active Episode in Phase One;
- at most one active Work Unit in Phase One;
- terminal Loops cannot advance;
- Session closure does not terminate the Loop;
- continuation is determined from durable state.

## 3. Episode

An Episode is one attempt to advance from a committed Checkpoint to the next durable semantic state.

```rust
pub struct Episode {
    pub id: EpisodeId,
    pub loop_id: LoopId,
    pub kind: EpisodeKind,
    pub status: EpisodeStatus,
    pub base_checkpoint_version: u64,
    pub terminal_checkpoint_version: Option<u64>,
    pub active_session: Option<SessionId>,
    pub trace_id: Option<TraceId>,
}
```

An Episode is also the OpenTelemetry trace boundary.

## 4. RuntimeSession

A bounded, replaceable execution environment.

```rust
pub struct RuntimeSession {
    pub id: SessionId,
    pub loop_id: LoopId,
    pub episode_id: EpisodeId,
    pub generation: u32,
    pub status: SessionStatus,
    pub runner: RunnerRef,
    pub context_budget: ContextBudget,
    pub boot_manifest_ref: ResourceRef,
    pub external_session_ref: Option<OpaqueRef>,
}
```

Closed and failed generations MUST NOT return to running.

## 5. WorkUnit

One externally observable unit of progress.

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

A Work Unit is not necessarily one model call, one agent, or one external workflow.

## 6. WorkUnitAttempt

Each execution attempt has its own durable identity and idempotency key.

```rust
pub struct WorkUnitAttempt {
    pub id: AttemptId,
    pub work_unit_id: WorkUnitId,
    pub episode_id: EpisodeId,
    pub ordinal: u32,
    pub status: AttemptStatus,
    pub idempotency_key: String,
    pub capabilities_snapshot: RunnerCapabilities,
    pub boot_package_ref: ResourceRef,
    pub external_execution_ref: Option<OpaqueRef>,
    pub side_effect_class: SideEffectClass,
    pub result_ref: Option<ResourceRef>,
}
```

## 7. Checkpoint

A versioned authoritative snapshot, not a conversation summary.

```rust
pub struct Checkpoint {
    pub identity: CheckpointIdentity,
    pub kernel: KernelState,
    pub goal: GoalState,
    pub pinned: PinnedState,
    pub work: WorkState,
    pub observations: ObservationSet,
    pub decisions: DecisionSet,
    pub domain: DomainState,
    pub resources: ResourceSet,
    pub verification: VerificationState,
    pub continuation: Continuation,
    pub telemetry: TelemetrySummary,
    pub integrity: IntegrityRecord,
}
```

A Checkpoint MAY represent pending verification, remediation required, suspension, or failure. It is a safe recovery point, not necessarily a success point.

## 8. Observation and Decision

```rust
pub enum VerificationStatus {
    Verified,
    Unverified,
    Disproven,
}
```

An Observation describes evidence-supported or hypothesized state. A Decision records a selected course of action. Runner-produced Decisions remain proposals until an explicit decision gate activates them.

## 9. ResourceReference

```rust
pub struct ResourceRef {
    pub uri: String,
    pub media_type: Option<String>,
    pub digest: Option<Digest>,
    pub version: Option<String>,
    pub size: Option<u64>,
    pub summary: Option<String>,
}
```

Resources used as evidence MUST be immutable, version-addressed, or content-addressed.

## 10. Acceptance and Verification

Runner completion, result production, result acceptance, Work Unit completion, and Loop completion are separate states.

```text
Runner completed
→ Result produced
→ Result validated
→ Result verified
→ Result accepted
→ Work Unit accepted
```

## 11. Domain State

Core stores domain-specific state as a schema-declared value:

```yaml
domain:
  schema: vendor.example/domain-state/v1
  state: {}
```

Core MUST NOT contain Git revisions, claims, sources, hypotheses, experiments, or other domain-specific entities.

## 12. Pure Transition Interface

```rust
pub fn decide(
    state: &AggregateState,
    command: &LifecycleCommand,
    policy: &EffectivePolicy,
) -> Result<TransitionDecision, DomainError>;

pub fn evolve(
    state: AggregateState,
    event: &LoopEvent,
) -> Result<AggregateState, DomainError>;
```

The domain layer performs no I/O.
