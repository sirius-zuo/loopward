# Phase One MVP and Evaluation

## 1. Objective

Phase One must prove the central Loopward claim:

> A durable Loop can continue across replaceable Runtime Sessions without carrying its full history, while preserving constraints, verified progress, recoverability, and trace correlation.

The MVP is a runtime experiment and reference implementation, not a general agent platform.

## 2. Hypotheses

### H1: Context carry cost can be bounded

A late-stage Work Unit can run from a bounded Boot Package whose size depends on the current work, not the total Loop history.

### H2: Structured Checkpoints preserve continuity

A fresh Runtime Session can continue without material rework or loss of active constraints.

### H3: Work Unit is a viable execution boundary

Different external executors can implement the same runner protocol without exposing internal topology.

### H4: Verified state transitions survive failure

Crashes and retries do not lose accepted work or repeat ambiguous external effects blindly.

### H5: Episode-level tracing is sufficient

Operators can follow a Loop across Episode traces and runner trace references without one long trace or an embedded telemetry backend.

## 3. Phase One Scope

Phase One includes:

- Rust workspace and public domain types;
- deterministic Loop Controller;
- SQLite durable store;
- Checkpoint and event journal;
- Context Manager and Boot Package manifest;
- Runtime Session and Episode lifecycle;
- Work Unit and Attempt lifecycle;
- generic command runner;
- deterministic verification gates;
- structured handoff validation;
- rotation and recovery;
- OpenTelemetry-compatible tracing;
- CLI for local execution;
- conformance testkit and failure injection.

## 4. Excluded Scope

Phase One excludes:

- Aether;
- orchestration APIs or concepts;
- DAG or workflow engine;
- distributed scheduler;
- agent registry or capability routing;
- domain-specific agent implementation;
- automatic semantic context planner;
- vector database or graph retrieval;
- built-in telemetry backend;
- observability dashboard or Web Preview;
- generic plugin marketplace;
- production multi-tenant control plane.

## 5. Reference Architecture

```text
loopward-api
    Public protocols and versioned data contracts

loopward-core
    Domain types and state-transition rules

loopward-runtime
    Controller, hydration, rotation, verification, recovery

loopward-store-sqlite
    Checkpoints, events, leases, Attempts, resource metadata

loopward-observability
    Framework-owned instrumentation boundary

loopward-observability-otel
    Optional OpenTelemetry implementation

loopward-runner-command
    Generic external process runner

loopward-cli
    Local operator interface

loopward-testkit
    Fixtures, fake runners, failure injection, trace recorder
```

## 6. Reference Scenario

The primary reference scenario is a small Rust maintenance task with five Episodes:

```text
Inspect
→ Implement
→ Verify
→ Remediate
→ Final Verify
```

The fixture must include:

- explicit constraints;
- a deterministic test failure;
- one rejected result;
- a remediation Work Unit;
- at least one forced process interruption;
- a Runtime Session rotation at every Work Unit boundary;
- a final completion gate.

The command runner can invoke a scripted fake runner for deterministic conformance and a configurable real agent command for comparative evaluation.

## 7. Context Stress Scenario

A synthetic prior history larger than 100,000 tokens is represented by durable events and resources.

The final verification Work Unit should receive:

- pinned constraints;
- current accepted state;
- current target revision;
- acceptance criteria;
- relevant evidence;
- references to archived history.

The Phase One fixture target is a Boot Package no larger than 30,000 estimated tokens. This threshold is an evaluation fixture, not a universal framework default.

## 8. Failure Injection Matrix

The testkit injects failures at:

1. before runner start;
2. after external start but before handle persistence;
3. during runner execution;
4. after result materialization;
5. during verification;
6. before Checkpoint transaction;
7. after Checkpoint commit but before process acknowledgement;
8. during Session drain.

Each case must have an expected reconciliation and recovery outcome.

## 9. Evaluation Layers

### 9.1 Deterministic conformance

Uses fake runners and known resources to verify:

- state machines;
- idempotency;
- optimistic concurrency;
- lease fencing;
- Checkpoint atomicity;
- context selection;
- redaction;
- trace links;
- failure recovery.

### 9.2 Real-runner evaluation

Uses the command runner to execute configured agent commands without adding vendor concepts to Core.

It compares:

- fresh Session per Work Unit;
- runner-managed continuous Session where available;
- token usage when reported;
- Boot Package size;
- repeated reads and repeated work;
- verification outcomes;
- time to accepted completion.

Unavailable runner metrics remain unavailable.

## 10. Success Metrics

```text
Constraint retention
= required constraints present and unchanged after every rotation

Recovery correctness
= expected state after every injected failure

Accepted rework rate
= repeated accepted work / prior accepted work

Context carry estimate
= historical material repeated in subsequent Boot Packages

Useful work per reported token
= accepted state transitions / reported processed tokens

Trace continuity
= Episodes correctly linked through Checkpoints
```

Metrics based on estimates must record their estimation method.

## 11. CLI Surface

Illustrative commands:

```text
loopward init
loopward create --spec <path>
loopward advance <loop-id>
loopward run <loop-id> --max-episodes <n>
loopward status <loop-id>
loopward events <loop-id>
loopward suspend <loop-id>
loopward resume <loop-id>
loopward cancel <loop-id>
loopward inspect-checkpoint <loop-id>
loopward verify-store
```

The CLI is a client of public runtime APIs, not a privileged alternative state machine.

## 12. Delivery Milestones

### M0: Contracts

- schema drafts;
- domain types;
- state-transition tests;
- error taxonomy.

### M1: Durable Core

- SQLite store;
- event journal;
- Checkpoint commit;
- lease and optimistic concurrency.

### M2: Execution Boundary

- Work Unit runner protocol;
- generic command runner;
- resource broker;
- structured result validation.

### M3: Context Lifecycle

- deterministic Context Manager;
- Boot Package manifest;
- pinned-state validation;
- token estimates and budget enforcement.

### M4: Verification and Rotation

- verification gates;
- Session Rotation;
- remediation;
- completion gate.

### M5: Recovery and Tracing

- reconciliation;
- failure injection;
- Episode trace links;
- in-memory instrumentation tests.

### M6: Comparative Evaluation

- reference Rust task;
- context stress scenario;
- real-runner profiles;
- evaluation report.

## 13. Phase One Acceptance Gates

Phase One is complete only when:

- all state-machine conformance tests pass;
- all eight failure-injection scenarios recover as specified;
- no required constraint changes across rotation;
- accepted results are always evidence-bound and verified;
- the context stress scenario meets its fixture budget;
- every Episode is trace-linked to its predecessor;
- the runtime operates with tracing disabled;
- the runtime operates without a telemetry backend;
- no Aether or orchestration concept appears in Core;
- at least two different command-runner profiles complete the same Work Unit protocol fixture;
- the evaluation report clearly distinguishes known, estimated, and unavailable token data.

