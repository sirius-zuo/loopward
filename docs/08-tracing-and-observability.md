# Tracing and Observability

## 1. Purpose

Loopward must emit enough structured tracing information to explain its own lifecycle decisions and correlate them with runner-owned traces.

Loopward does not attempt to change or fully observe a runner's internal behavior. A runner may expose its own trace, logs, usage data, or nothing at all. Loopward owns the trace of the public execution boundary and coordinates trace context when the runner supports it.

## 2. Boundary

Loopward owns instrumentation for:

- Loop and Episode lifecycle;
- Runtime Session lifecycle;
- Work Unit Attempts;
- context hydration and drain;
- resource access through Loopward boundaries;
- verification;
- Checkpoint commit;
- rotation decisions;
- suspension, cancellation, and recovery.

Loopward does not own:

- internal model calls;
- internal tool calls;
- internal subagents;
- runner-specific prompts or compaction;
- external workflow topology;
- telemetry storage, querying, visualization, or alerting.

## 3. Backend Independence

Loopward emits telemetry through standard interfaces. The telemetry pipeline and backend are deployment concerns independent of Loopward.

Phase One:

- supports OpenTelemetry-compatible tracing;
- propagates W3C Trace Context;
- may export through a configured OTLP exporter;
- does not ship or configure a telemetry backend;
- does not depend on an observability UI;
- validates instrumentation with an in-memory recorder or test exporter.

If an OTLP exporter is enabled, its endpoint is supplied through deployment configuration such as:

```text
OTEL_EXPORTER_OTLP_ENDPOINT
```

The design does not prescribe a local address or a specific backend product.

## 4. Trace Segmentation

One long-running Loop must not be represented as one indefinitely open trace.

```text
Loop
├── Episode 1 = Trace 1
├── Episode 2 = Trace 2, linked to Trace 1
├── Episode 3 = Trace 3, linked to Trace 2
└── ...
```

One Episode is one top-level trace. The committed Checkpoint terminates the Episode and provides the continuation reference for the next trace.

This design:

- avoids extremely long traces;
- keeps trace size aligned with a bounded unit of progress;
- survives telemetry loss;
- permits retention and sampling per Episode;
- matches Session Rotation and recovery boundaries.

## 5. Span Model

Recommended stable span names:

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

Span names are low-cardinality constants. Identifiers belong in attributes.

## 6. Required Attributes

Lifecycle spans include applicable attributes:

```text
loopward.loop.id
loopward.episode.id
loopward.episode.ordinal
loopward.session.id
loopward.session.generation
loopward.work_unit.id
loopward.attempt.id
loopward.runner.name
loopward.runner.protocol_version
loopward.checkpoint.version
loopward.phase
loopward.operation
loopward.outcome
```

Conditional attributes include:

```text
loopward.rotation.reason
loopward.suspension.code
loopward.failure.code
loopward.recovery.outcome
loopward.context.rule_version
loopward.context.estimated_tokens
loopward.context.actual_tokens
loopward.context.estimate_method
loopward.verification.gate_kind
loopward.verification.outcome
```

High-cardinality identifiers are appropriate for traces but must not automatically become metric labels.

## 7. Trace Links

Span links represent causal relations that are not parent-child containment:

```text
continuation_of
retry_of
recovery_of
remediation_of
external_execution
```

The root span of a new Episode links to the prior Episode trace and records the Checkpoint reference used for hydration.

A runner may return one or more opaque trace references. Loopward links or records them without claiming ownership of the external spans.

## 8. Propagation to Runners

`WorkUnitRequest` carries:

```rust
pub struct TraceContext {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
    pub baggage: Vec<BaggageItem>,
}
```

Only allow-listed baggage keys may cross the runner boundary. Sensitive Loop state, prompt content, credentials, and unrestricted identifiers must not be placed in baggage.

Runner trace coordination has three observable levels:

- `BoundaryOnly`: Loopward traces the call but the runner does not propagate context.
- `Correlated`: the runner returns an external trace reference.
- `Continuous`: the runner accepts propagated context and emits compatible child or linked spans.

The recorded level must reflect actual capability.

## 9. Events

Significant events may be attached to spans and also recorded durably when required for recovery:

```text
rotation.requested
runner.compaction.reported
handoff.received
verification.finding
checkpoint.committed
execution.suspended
execution.cancel.requested
recovery.ambiguity_detected
```

Trace events are diagnostic copies. The durable event journal remains authoritative.

## 10. Outcome and Error Semantics

Business outcomes and telemetry status are distinct.

Examples:

- a verification failure may be a valid business outcome and not a telemetry transport error;
- a suspended Loop may have a successful span with `loopward.outcome=suspended`;
- a runner protocol failure sets an error status and records a typed failure code;
- cancellation records whether it was requested, acknowledged, and observed.

Raw error messages may be sensitive. Public codes and sanitized descriptions are preferred.

## 11. Metrics

Loopward may derive low-cardinality metrics from lifecycle events:

```text
loopward.episodes
loopward.work_unit.attempts
loopward.session.rotations
loopward.checkpoint.commits
loopward.verification.outcomes
loopward.recovery.outcomes
loopward.context.input_tokens
loopward.context.carry_tokens_estimated
loopward.duration
```

Metric labels may include:

- outcome;
- runner name;
- Work Unit kind;
- phase;
- rotation reason;
- verification gate kind.

Loop, Episode, Session, Work Unit, Attempt, and resource identifiers must not be default metric labels.

## 12. Logs

Structured logs may complement traces, but logs are not the primary lifecycle protocol.

Logs should include trace and span identifiers when available. They must not contain:

- complete prompts;
- complete Boot Packages;
- credentials;
- unrestricted resource contents;
- raw user data by default.

## 13. Sampling and Reliability

Tracing is best-effort. A telemetry outage must not stop Loop progress unless a deployment policy explicitly requires audit delivery.

The authoritative recovery path is:

```text
Checkpoint + event journal + resource integrity
```

not:

```text
trace reconstruction
```

Sampling decisions should preserve errors, ambiguity, suspension, recovery, and rotation anomalies when the external telemetry system supports such policies.

## 14. Testability

Phase One instrumentation tests use:

- an in-memory span exporter or recorder;
- deterministic clocks and identifiers;
- assertions on span names, required attributes, links, and redaction;
- failure injection around runner, verification, and Checkpoint operations.

No external telemetry service is required for conformance tests.

## 15. Acceptance Criteria

Tracing is acceptable when:

- each Episode produces a bounded trace;
- consecutive Episodes are linked across Checkpoints;
- every runner call has a Loopward-owned boundary span;
- runner trace capability is accurately reported;
- lifecycle identifiers support correlation without becoming default metric labels;
- traces contain no full prompt or resource payload by default;
- telemetry loss cannot corrupt or block durable recovery;
- Loopward operates without any telemetry backend or observability UI.

