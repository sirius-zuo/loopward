# Risks, Open Questions, and Project Acceptance

## 1. Primary Risks

### 1.1 Checkpoint information loss

Structured state may omit implicit knowledge that existed only in runner reasoning.

Mitigations:

- lossless pinned state;
- structured handoff contract;
- evidence references;
- unresolved-issue capture;
- rework measurement after rotation;
- conformance fixtures with hidden-but-required constraints.

### 1.2 Constraint decay

Lossy summaries may weaken precise requirements.

Mitigations:

- pinned constraints bypass lossy compaction;
- digest comparison at hydration and drain;
- explicit applicability and scope;
- completion gate revalidation.

### 1.3 State and resource drift

A Checkpoint may describe a resource version that no longer exists or has changed.

Mitigations:

- digest-bound immutable references;
- version binding;
- stale verification detection;
- resource-integrity checks during recovery.

### 1.4 Rotation too early

Frequent replacement may cause unnecessary resource loading and loss of useful local working memory.

Mitigations:

- measure rework;
- permit controlled reuse experiments;
- keep Work Units cohesive;
- compare policies using the same fixture.

### 1.5 Rotation too late

Historical carry cost and contradictory context may accumulate before a fixed window threshold is reached.

Mitigations:

- phase and Work Unit boundaries;
- soft context budgets;
- explicit entropy signals;
- default fresh Session per Work Unit in Phase One.

### 1.6 Runner opacity

Some runners may report neither token use nor internal trace behavior.

Mitigations:

- capability levels;
- boundary-only tracing;
- conservative observable estimates;
- no fabricated precision;
- runner conformance findings.

### 1.7 Ambiguous external effects

A crash after runner start may leave unknown side effects.

Mitigations:

- durable intent before effect;
- idempotency key;
- reconciliation API;
- opaque external execution reference;
- suspension rather than unsafe repetition.

### 1.8 Observability becoming a hidden dependency

Operational tooling could accidentally become required for correctness.

Mitigations:

- Checkpoint and journal remain authoritative;
- tracing is best-effort;
- in-memory test instrumentation;
- no bundled backend or UI;
- tracing can be disabled.

### 1.9 Core abstraction becoming domain-heavy

Coding, research, and experiment concepts could leak into the kernel.

Mitigations:

- opaque, versioned domain state;
- domain-defined schemas and verification policies;
- resource references instead of domain-specific fields;
- a second non-coding reference implementation after Phase One.

### 1.10 Rebuilding an orchestration framework

Runner extensibility could drift into DAG, routing, and scheduling.

Mitigations:

- no orchestration concept in Core;
- Work Unit is the only outbound execution boundary;
- integrations use case-specific wrappers;
- Aether excluded from Phase One.

### 1.11 Recursive wrapper composition

An external system could invoke Loopward, which invokes the same external system again.

Mitigations belong primarily in wrappers and deployment policy:

- execution lineage;
- maximum nesting depth;
- resource-budget inheritance;
- cancellation propagation;
- cycle detection where external identities are available.

## 2. Open Questions

### 2.1 Minimum sufficient Checkpoint

Which fields are universally required, and which should remain domain-defined?

Phase One approach:

- keep kernel fields minimal;
- measure rework and missing-state failures;
- add fields only with evidence from fixtures.

### 2.2 Context relevance estimation

How accurately can relevant context be estimated without using the same expensive model being optimized?

Phase One approach:

- deterministic references, phase, Work Unit inputs, decision scope, and recent verified outcomes;
- record estimate method and uncertainty;
- defer model-assisted selection.

### 2.3 Session reuse policy

When does reuse within a cohesive Work Unit outperform a fresh Session?

Phase One approach:

- default fresh per Work Unit;
- allow explicit comparison profiles;
- measure rework, tokens, latency, and verification quality.

### 2.4 Domain extension contract

How should domains contribute state schemas, verification gates, completion policy, and rotation signals without creating an unstable plugin platform?

Phase One approach:

- versioned Rust traits and schema references;
- static composition;
- no marketplace or runtime discovery.

### 2.5 Checkpoint migration

How are long-lived Loops upgraded across schema and runtime versions?

Phase One approach:

- record every schema version;
- reject unsupported migration;
- define forward-only migration hooks before persistent compatibility is promised.

### 2.6 Evidence retention

How long should large resources remain available after Checkpoint compaction?

Phase One approach:

- configurable retention outside Core;
- immutable references in accepted state;
- fail integrity validation if required evidence is missing.

### 2.7 Human authority

Which transitions require human approval, and how is authority represented across domains?

Phase One approach:

- typed human verification gate;
- principal and target binding;
- domain policy decides applicability.

### 2.8 Cost accounting

How should cached tokens, tool costs, external services, and human work contribute to a common resource budget?

Phase One approach:

- typed optional usage dimensions;
- no forced conversion into one scalar;
- retain provider-reported raw values.

### 2.9 Trace sampling across Episodes

How can an external telemetry system retain an entire failed lineage when traces are independently sampled?

Phase One approach:

- emit stable Loop and Episode correlation attributes;
- link traces;
- retain errors and recovery signals when supported;
- keep recovery independent of traces.

### 2.10 Semantic completion

How can a domain verify that no materially relevant work remains?

Phase One approach:

- explicit success criteria;
- required verification gates;
- no generic semantic saturation algorithm.

## 3. Deferred Research

The following are deliberate research topics, not Phase One commitments:

- adaptive rotation learned from historical runs;
- small-model context relevance scoring;
- automatic detection of implicit knowledge before drain;
- cross-runner context portability;
- semantic evidence graphs;
- distributed Loop ownership;
- multi-Loop resource governance;
- privacy-preserving telemetry aggregation.

## 4. Framework-Level Acceptance Criteria

The Loopward design is accepted when it establishes:

1. an independent, domain-agnostic Loop Engineering framework;
2. a durable Loop identity separate from Runtime Sessions;
3. Context Lifecycle management as a central runtime responsibility;
4. structured, versioned, evidence-backed Checkpoints;
5. bounded Work Units and explicit Attempts;
6. replaceable Runtime Sessions with deterministic rotation and recovery;
7. verification before authoritative progress;
8. public inbound and outbound execution boundaries;
9. wrapper-based integration without mutual framework dependencies;
10. accurate tracing boundaries and standard context propagation;
11. no dependency on an orchestration framework, telemetry backend, or Web Preview;
12. a feasible Rust Phase One MVP with measurable hypotheses.

## 5. Decision Record

The following decisions are frozen for Phase One:

- project name: Loopward;
- implementation language: Rust;
- Loopward Core contains no orchestration concept;
- Aether is excluded;
- external composition is implemented by case-specific wrappers against public APIs;
- the framework is domain-agnostic and statically extensible;
- SQLite is the reference durable store;
- the command runner is the reference execution adapter;
- every Work Unit uses a fresh Runtime Session by default;
- every Episode ends at a durable Checkpoint boundary;
- one Episode is one trace;
- telemetry storage and visualization remain external;
- wiki generation is outside the current design phase.

