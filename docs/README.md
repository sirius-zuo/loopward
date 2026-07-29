# Loopward Design Documents

**Project status:** Formal design draft for review  
**Design version:** 0.1  
**Last updated:** 2026-07-29

Loopward is an independent loop engineering framework for durable, context-efficient agent work that spans multiple execution sessions. Context lifecycle management is a primary runtime responsibility, not an optional optimization.

## Reading Order

1. [Project Charter](00-project-charter.md)
2. [System Architecture](01-system-architecture.md)
3. [Core Domain Model](02-core-domain-model.md)
4. [Durable State and Transactions](03-durable-state-and-transactions.md)
5. [Context Lifecycle](04-context-lifecycle.md)
6. [Work Unit and Runner Protocol](05-work-unit-and-runner-protocol.md)
7. [Verification and Recovery](06-verification-and-recovery.md)
8. [Session Rotation](07-session-rotation.md)
9. [Tracing and Observability](08-tracing-and-observability.md)
10. [Public API](09-public-api.md)
11. [MVP and Evaluation](10-mvp-and-evaluation.md)
12. [Risks and Open Questions](11-risks-and-open-questions.md)

## Normative Language

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative. “Phase One” refers to the first implementation milestone described in the MVP document.

## Frozen Decisions

- Loopward is an independent Rust project.
- A Loop is durable; a Runtime Session is bounded and replaceable.
- Authoritative state lives outside runner conversations.
- Context is reconstructed for the current Work Unit.
- Checkpoints record recoverable truth, including incomplete states.
- A Work Unit is an externally observable unit of progress, not necessarily one agent call.
- Runner results are untrusted until validated and verified.
- Loopward Core has no orchestration, DAG, workflow topology, agent registry, or Aether concept.
- External composition uses case-specific wrappers against public APIs.
- Aether is excluded from Phase One.
- OpenTelemetry storage and visualization backends are external and optional.
- Phase One contains no Web preview or observability UI.

## Document Status

| Document | Status |
|---|---|
| Project Charter | Draft for review |
| System Architecture | Draft for review |
| Core Domain Model | Draft for review |
| Durable State and Transactions | Draft for review |
| Context Lifecycle | Draft for review |
| Work Unit and Runner Protocol | Draft for review |
| Verification and Recovery | Draft for review |
| Session Rotation | Draft for review |
| Tracing and Observability | Draft for review |
| Public API | Draft for review |
| MVP and Evaluation | Draft for review |
| Risks and Open Questions | Living design record |
