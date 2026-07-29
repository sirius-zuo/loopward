# Project Charter

**Status:** Draft for review  
**Version:** 0.1

## 1. Mission

Loopward provides lifecycle infrastructure for long-running intelligent tasks that must remain coherent, verifiable, recoverable, and context-efficient across multiple replaceable sessions and execution environments.

Its foundational rule is:

> The Loop is durable; the Session is replaceable; state lives outside the Session; context is reconstructed for the current work.

## 2. Motivation

In a long-running agent loop, conversation history, tool output, file contents, failed attempts, and subtask results accumulate continuously. The information required by the next action often remains local and small. Later model calls therefore repeatedly process historical context that contributes little to the current decision.

Loopward treats this as a runtime resource problem:

```text
Context Amplification Factor
= actual context processed / context relevant to the decision

Context Relevance Ratio
= relevant context / total Session context

Task Context Mismatch
= actual input context / expected task context

Session Carry Cost
= repeatedly processed historical context with no material contribution
```

Subagents isolate local work but do not shrink a coordinator Session that is already large. Loopward addresses continuity and context lifecycle across Session replacement.

## 3. Goals

Loopward MUST:

- preserve one logical Loop across multiple Runtime Sessions;
- make Session rotation safe, observable, and recoverable;
- persist authoritative Checkpoints independently of conversations;
- preserve policies, explicit constraints, and success criteria without lossy summarization;
- assemble bounded, task-specific Boot Packages;
- distinguish pruning, compaction, checkpointing, rotation, and rehydration;
- execute Work Units through stable public runner contracts;
- separate observations, decisions, proposals, and verification status;
- support deterministic, agentic, and human verification gates;
- recover from process, runner, Session, and telemetry failures;
- expose language-neutral inbound and outbound execution boundaries;
- allow domain-specific state and policies without importing domain concepts into Core;
- measure context cost, rotation, rework, and verified progress.

## 4. Non-Goals

Loopward Phase One MUST NOT implement:

- a DAG engine or workflow DSL;
- multi-agent orchestration;
- fan-out, fan-in, capability routing, or distributed scheduling;
- an agent registry or plugin marketplace;
- Aether integration or Aether-specific types;
- a permanent coordinator Session;
- vector retrieval or graph retrieval;
- automatic semantic context selection;
- exact control of provider-internal prompts;
- concurrent active Work Units;
- a trace storage/query backend;
- Grafana, Tempo, Jaeger, or another observability product;
- a Web preview or observability UI;
- a complete coding, research, or experiment product.

## 5. Framework Boundaries

Loopward owns:

```text
Loop lifecycle
Episode lifecycle
Runtime Session lifecycle
Work Unit lifecycle
Checkpoint versions
Pinned constraints
Context construction
Verification gates
Continuation decisions
Recovery policy
Resource budgets
Durable lifecycle events
Terminal outcomes
```

A runner owns:

```text
internal model and tool calls
internal workers or workflows
provider-specific Session behavior
temporary execution state
runner-specific telemetry
```

A domain extension owns:

```text
domain Checkpoint schema
domain Work Unit kinds
domain resources
domain verification rules
domain completion semantics
domain progress measures
```

## 6. Composition Principle

Loopward does not model orchestration. External systems MAY invoke Loopward, and Loopward Work Units MAY invoke external systems, through case-specific wrappers built against public APIs.

Examples outside Phase One:

```text
Loopward Work Unit → Aether wrapper → Aether execution
Aether worker call → Loopward wrapper → Loopward Loop
```

Each system retains ownership of its own authoritative state. External execution references remain opaque.

## 7. Technology Baseline

Phase One uses:

```text
Language: Rust
Async runtime: Tokio
Serialization: Serde
Durable store: SQLite
CLI: clap
Internal instrumentation: tracing
Telemetry export: isolated OpenTelemetry adapter
Wire contracts: versioned JSON Schema
```

The Rust implementation MUST NOT make the wire protocols Rust-specific.

## 8. Foundational Principles

1. The Loop is durable; Sessions are replaceable.
2. History is retrievable, not a default prompt.
3. Conversation is not authoritative state.
4. Context is constructed for the current action.
5. Required constraints are preserved losslessly.
6. Checkpoints record what is true now, not merely what happened.
7. Runner outputs are proposals until validated.
8. Verification determines acceptance.
9. External side effects require durable intent and reconciliation.
10. Token and context capacity are managed runtime resources.
