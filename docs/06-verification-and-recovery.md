# Verification and Recovery

## 1. Purpose

Loopward treats progress as verified state transition, not generated text.

A runner result is not accepted merely because the runner reports success. It must satisfy a predeclared output contract and verification plan. Recovery must preserve this distinction after crashes, retries, and Runtime Session rotation.

## 2. Result Lifecycle

```text
PRODUCED
  ↓
VALIDATING
  ↓
PENDING_VERIFICATION
  ↓
VERIFYING
  ├── ACCEPTED
  ├── REJECTED
  ├── STALE
  ├── SUSPENDED
  └── INCONCLUSIVE
```

Only `ACCEPTED` results can produce accepted Work Unit completion mutations.

## 3. Verification Plan

The verification plan exists before execution and is versioned.

```rust
pub struct VerificationPlan {
    pub id: VerificationPlanId,
    pub version: u64,
    pub target: VerificationTarget,
    pub gates: Vec<VerificationGateSpec>,
    pub aggregation: AggregationPolicy,
    pub remediation_policy: RemediationPolicy,
}
```

The plan binds verification to:

- the Work Unit;
- the expected output schema;
- applicable acceptance criteria;
- resource or workspace revisions;
- domain-specific validation rules.

A runner cannot weaken its own verification plan through its result.

## 4. Gate Types

```text
Schema
Integrity
Deterministic
Policy
Agentic
Human
```

Recommended order:

1. schema validation;
2. digest and provenance integrity;
3. deterministic checks;
4. policy checks;
5. agentic semantic review;
6. human approval where required.

Cheap and conclusive gates run before expensive semantic gates.

## 5. Verification Outcome

```rust
pub enum GateOutcome {
    Passed,
    Failed,
    Inconclusive,
    Suspended,
    Stale,
}

pub struct VerificationFinding {
    pub gate_id: GateId,
    pub outcome: GateOutcome,
    pub code: String,
    pub statement: String,
    pub evidence: Vec<ResourceRef>,
    pub target_digest: Digest,
}
```

Findings are durable and traceable to evidence.

## 6. Target Binding and Staleness

Verification must bind to the exact target it evaluated:

- resource digests;
- workspace or dataset revision;
- Work Unit result digest;
- verification-plan version;
- applicable constraint digest.

If the target changes after verification, the finding becomes stale. A stale pass cannot satisfy an acceptance criterion.

## 7. Aggregation

Phase One uses explicit required gates.

```text
Accepted
= every required gate passed
+ no blocking finding remains unresolved
+ target bindings are current
```

Failures are not averaged away by confidence scores. Optional advisory gates may produce non-blocking findings, but they must be labelled as advisory.

## 8. Agentic Verification

Agentic verification runs in a distinct, bounded context assembled for review. It should receive:

- applicable acceptance criteria;
- the result and relevant outputs;
- exact target revisions;
- relevant constraints;
- necessary evidence;
- the verification rubric.

It should not automatically inherit the implementation trajectory or runner conversation. This reduces confirmation bias and avoids carrying unrelated context.

Agentic verification is a proposal-generating gate unless the domain policy explicitly grants it authority.

## 9. Human Verification

Human gates suspend the Loop with a structured request:

```rust
pub struct InputRequest {
    pub code: String,
    pub prompt: String,
    pub expected_schema: SchemaRef,
    pub evidence: Vec<ResourceRef>,
}
```

The response is recorded as evidence. Approval must identify the principal, decision time, target digest, and scope.

## 10. Remediation

A rejected or inconclusive result does not silently restart the same Work Unit.

Remediation creates:

- a new Work Unit or a new explicitly related attempt;
- a new Episode when context locality or phase changes;
- lineage to the failed verification;
- bounded retry and resource budgets;
- updated evidence and constraints.

```text
verification finding
    ↓
remediation proposal
    ↓
runtime policy decision
    ↓
new Work Unit
    ↓
new Attempt
    ↓
re-verification
```

Repeated remediation without information gain triggers suspension or terminal failure according to policy.

## 11. Completion Gate

Loop completion is a separate decision from Work Unit acceptance.

The completion gate verifies:

- every required success criterion is satisfied;
- satisfaction is supported by current evidence;
- no required Work Unit remains active;
- no blocking issue remains;
- required final verification has passed;
- resource and token budget policies permit completion;
- authoritative state matches its referenced resources.

The Loop Controller alone commits the terminal transition.

## 12. Recovery Model

Recovery starts from the latest committed Checkpoint and journal position, not from trace data or runner conversation.

```text
Acquire lease
    ↓
Load latest committed Checkpoint
    ↓
Replay durable events after Checkpoint
    ↓
Reconcile active Attempt
    ↓
Validate resource integrity
    ↓
Resume, collect, retry, suspend, or fail
```

Recovery never assumes an external operation failed merely because Loopward did not record its result.

## 13. Recovery Decisions

| Observed state | Required action |
|---|---|
| Intent recorded, runner not called | Start using the same idempotency key |
| Start outcome uncertain | Reconcile; suspend if ambiguity remains |
| Runner reports running | Reattach and continue event collection |
| Runner reports completed | Collect and validate result |
| Result stored, verification incomplete | Resume verification |
| Verification accepted, Checkpoint missing | Commit mutation using expected revision |
| Checkpoint committed, process crashed | Continue from next Episode |
| Target digest changed | Mark verification stale and re-evaluate |

## 14. Checkpoint Safety

A Checkpoint may represent:

- progress after accepted work;
- suspension;
- failed attempt;
- ambiguous external state;
- recovery finding;
- terminal completion or failure.

Checkpointing is not synonymous with success.

## 15. No-Progress Detection

Phase One records deterministic signals:

- repeated identical finding codes;
- repeated result digests;
- unchanged target revision;
- repeated Work Unit objective;
- attempts without newly accepted evidence;
- budget spent since last accepted state transition.

Policy can suspend after configured thresholds. Loopward does not require a semantic “stuck” classifier in Phase One.

## 16. Acceptance Criteria

Verification and recovery are acceptable when:

- runner-reported success cannot bypass verification;
- every accepted state transition is supported by current evidence;
- verification is invalidated when its target changes;
- crashes at any durable boundary do not lose accepted work;
- uncertain external effects are reconciled rather than blindly repeated;
- remediation is explicit, bounded, and traceable;
- Loop completion has its own gate;
- recovery requires neither trace storage nor prior conversation history.

