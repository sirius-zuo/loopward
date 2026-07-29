# Context Lifecycle

## 1. Purpose

Context lifecycle management is a central design pillar of Loopward.

Loopward exists partly because a durable Loop must not require one continuously growing model conversation. The framework therefore separates:

- durable execution state;
- replaceable Runtime Sessions;
- retrievable resources;
- task-specific context assembled for the current Work Unit.

The objective is not to minimize tokens at any cost. The objective is to keep the context cost of an execution step proportional to the information needed by that step, without losing constraints, decisions, evidence, or recoverability.

## 2. Governing Principles

1. A Loop is durable; a Runtime Session is replaceable.
2. Execution history is retrievable state, not default prompt content.
3. Context is assembled for the current Work Unit.
4. Pinned constraints are preserved losslessly.
5. Large resources remain outside the prompt unless materialization is required.
6. Compaction produces an untrusted proposal until validated.
7. Every materialized context item has provenance and a selection reason.
8. Context reconstruction must be observable without recording sensitive payloads.

## 3. Context Capability Levels

Runners differ in how much context control they expose. Loopward records this explicitly.

```rust
pub enum ContextControlLevel {
    Opaque,
    Observable,
    ManagedBoot,
    Managed,
}

pub struct ContextCapabilities {
    pub control_level: ContextControlLevel,
    pub reports_usage: bool,
    pub reports_compaction: bool,
    pub supports_resource_fetch: bool,
    pub supports_structured_handoff: bool,
    pub supports_resume: bool,
}
```

The levels mean:

- `Opaque`: Loopward can start and collect a run but cannot inspect or control its context behavior.
- `Observable`: the runner reports context usage or compaction events.
- `ManagedBoot`: Loopward controls the initial Boot Package, while later context behavior is runner-managed.
- `Managed`: Loopward can also supply resources incrementally and request a structured drain or compaction.

Capability discovery is descriptive, not aspirational. Loopward must not assume a runner supports a capability it did not declare.

## 4. Context Item Model

Every item considered for a Boot Package is represented by metadata before payload materialization.

```rust
pub struct ContextItem {
    pub id: ContextItemId,
    pub kind: ContextItemKind,
    pub source: ResourceRef,
    pub classification: ContextClass,
    pub priority: ContextPriority,
    pub fidelity: Fidelity,
    pub sensitivity: Sensitivity,
    pub validity: Validity,
    pub estimated_tokens: Option<u64>,
    pub estimate_method: Option<EstimateMethod>,
    pub estimate_confidence: Option<f32>,
    pub digest: Digest,
    pub selection_reason: String,
}
```

### 4.1 Context classes

```text
Pinned
CurrentState
CurrentWork
Decision
Evidence
Reference
RecentOutcome
```

`Pinned` includes system policy, active user constraints, acceptance criteria, safety requirements, and applicable invariants.

### 4.2 Priority

```text
Required
Important
Supplemental
OnDemand
```

- `Required` items must be present or hydration fails.
- `Important` items are selected before supplemental material.
- `Supplemental` items use remaining budget.
- `OnDemand` items are referenced and fetched only if the runner supports scoped retrieval.

### 4.3 Fidelity

```text
Verbatim
Structured
Excerpt
Summary
ReferenceOnly
```

Pinned constraints must use `Verbatim` or a lossless structured representation. A lossy summary cannot replace them.

## 5. Context Budget

The budget must reserve space for execution rather than filling the model window with boot context.

```rust
pub struct ContextBudget {
    pub model_window_tokens: Option<u64>,
    pub soft_input_limit_tokens: u64,
    pub hard_input_limit_tokens: u64,
    pub reserved_output_tokens: u64,
    pub reserved_tool_tokens: u64,
    pub safety_margin_tokens: u64,
}
```

Token estimates must record their method and confidence. Unknown token usage remains unknown; Loopward must not turn an unavailable vendor metric into a fabricated precise value.

## 6. Deterministic Hydration Pipeline

Phase One uses a deterministic pipeline:

```text
Checkpoint + Work Unit + Runner Capabilities + Budget
    ↓
Validate pinned state and resource digests
    ↓
Select required context
    ↓
Select applicable decisions and verified observations
    ↓
Select current Work Unit inputs and recent outcomes
    ↓
Apply fidelity transformations
    ↓
Fit important and supplemental items to budget
    ↓
Emit Boot Package Manifest
    ↓
Materialize runner-specific payload
```

Selection rules must be versioned. The same inputs and rule version should produce the same Boot Package Manifest.

Phase One does not use a large model to decide what context should be loaded. Model-assisted relevance selection may be investigated later, but it cannot become an unrecorded hidden decision.

## 7. Boot Package

A Boot Package has two layers:

1. a stable manifest owned by Loopward;
2. a runner-specific materialization.

```rust
pub struct BootPackageManifest {
    pub schema_version: String,
    pub loop_id: LoopId,
    pub episode_id: EpisodeId,
    pub session_generation: u32,
    pub work_unit_id: WorkUnitId,
    pub checkpoint_ref: CheckpointRef,
    pub context_rule_version: String,
    pub budget: ContextBudget,
    pub items: Vec<ContextItem>,
    pub pinned_digest: Digest,
    pub manifest_digest: Digest,
}
```

The manifest provides:

- provenance;
- deterministic replay inputs;
- token estimates;
- inclusion and exclusion reasons;
- integrity binding to pinned state;
- a comparison point for later rework analysis.

The materialized payload may be Markdown, JSON, files, command-line arguments, or another runner-specific format. It is not authoritative state.

## 8. Resource References and On-Demand Retrieval

Large files, reports, logs, source corpora, test outputs, and external documents should normally be represented by `ResourceRef`.

```rust
pub struct ResourceGrant {
    pub resource: ResourceRef,
    pub permissions: Vec<ResourcePermission>,
    pub scope: WorkUnitId,
    pub expires_at: Option<Timestamp>,
}
```

On-demand retrieval must be:

- scoped to the active Work Unit;
- auditable;
- subject to access control;
- digest-checked when immutable content is expected;
- recorded as a resource-access event.

Loopward does not require every runner to support on-demand retrieval. A runner without that capability receives a fully materialized Boot Package within budget.

## 9. Compaction and Structured Handoff

Compaction is not ordinary conversation summarization. It converts an execution trajectory into a proposal for durable state.

```rust
pub struct HandoffProposal {
    pub completed_work: Vec<StateAssertionProposal>,
    pub active_work: Vec<StateAssertionProposal>,
    pub unresolved: Vec<OpenIssueProposal>,
    pub observations: Vec<ObservationProposal>,
    pub decision_proposals: Vec<DecisionProposal>,
    pub relevant_resources: Vec<ResourceRef>,
    pub recommended_next_action: Option<ContinuationProposal>,
    pub pinned_digest_seen: Digest,
}
```

The proposal is validated before checkpoint commit:

1. schema validation;
2. pinned digest comparison;
3. evidence reference validation;
4. workspace or resource-version validation when applicable;
5. distinction between verified and unverified claims;
6. policy validation;
7. conflict detection with active decisions.

Decision proposals do not become active decisions automatically.

## 10. Context Entropy Signals

Phase One does not define a single opaque “context entropy score.” It records deterministic signals:

- superseded decision count;
- stale resource reference count;
- contradictory active assertion count;
- repeated tool-output bytes;
- repeated resource reads;
- compaction count;
- estimated irrelevant carry tokens;
- locality change between consecutive Work Units;
- phase transition;
- pinned-state mismatch.

Rotation policy can use these individual signals. A composite score may be introduced only when its formula, inputs, and interpretation are explicit.

## 11. Context Efficiency Metrics

Loopward records metrics only when their inputs are available:

```text
Context Amplification Factor
= processed context tokens / estimated relevant tokens

Context Relevance Ratio
= estimated relevant tokens / processed context tokens

Task Context Mismatch
= processed context tokens / expected task context tokens

Session Carry Ratio
= repeatedly carried historical tokens / total input tokens

Rework After Rotation
= repeated previously completed work / prior completed work
```

These are evaluation metrics, not claims of exact semantic truth. Their estimation method must be attached to the measurement.

## 12. Failure Semantics

Hydration fails without starting a runner when:

- required pinned state is missing;
- a required resource cannot be resolved;
- an immutable resource digest does not match;
- the Boot Package exceeds the hard budget;
- its runner-specific materialization is invalid.

If a runner performs undocumented compaction or loses required constraints, Loopward records a capability or conformance finding. It must not silently claim managed context continuity.

## 13. Acceptance Criteria

The Context Lifecycle design is acceptable when:

- every Runtime Session can be reconstructed without prior conversation history;
- all required constraints survive reconstruction losslessly;
- every Boot Package item has provenance and a selection reason;
- raw artifacts can remain outside model context;
- compaction cannot directly mutate authoritative state;
- a failed hydration does not start a Work Unit Attempt;
- context metrics distinguish known, estimated, and unavailable values;
- a late-stage Work Unit can run from a bounded Boot Package independent of total Loop history.

