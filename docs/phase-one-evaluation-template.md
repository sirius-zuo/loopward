# Phase One Evaluation Report Template

Companion to `docs/10-mvp-and-evaluation.md` §9-§10: the report shape a Phase
One evaluation run (deterministic conformance, real-runner comparison, or
the context stress scenario) fills in. One instance of this template covers
one run of one scenario against one runner profile.

Fill in every field. Where a value is genuinely unavailable (a runner does
not report token usage, a metric could not be computed), write
`unavailable` explicitly rather than leaving the cell blank or guessing —
`docs/10` §13's acceptance gate requires the report "clearly distinguish
known, estimated, and unavailable token data", and a blank cell cannot be
told apart from an oversight.

## 1. Run identification

| Field | Value |
| --- | --- |
| Scenario | e.g. `tests/fixtures/scenario/retry-loop.json` (reference scenario) or the context stress scenario |
| Runner profile | e.g. `loopward-testkit` scripted `FakeRunner` (deterministic conformance), or the real command-runner profile under comparison |
| `loop_id` | |
| Started at / finished at | |
| Loopward commit / build | |
| Tracing configuration | `disabled` \| `NoopTelemetrySink` \| `RecordingSink` (in-process) \| OTLP endpoint `<url>` — `docs/10` §13 requires at least one run with tracing disabled and one without a telemetry backend configured |

## 2. Per-Episode table

One row per durable Episode (`episode.started` event), in order. Derive
these from the durable event journal (`LoopService::events`) and, where the
runner reports it, `WorkUnitResult.usage`.

| Episode | Boot Package estimated tokens (method) | Reported input/output tokens | Accepted state transitions | Repeated resource reads | Rotation reason | Verification outcome | Recovery events | Trace reference |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | | | | | | n/a (Execution) | | |
| 2 | | | | | | n/a (Execution) | | |
| 3 | | | | | | | | |
| ... | | | | | | | | |

Column definitions:

- **Boot Package estimated tokens (method)** — the sum of
  `BootPackageManifest.items[].estimated_tokens` for this Episode's
  `context.hydrated` manifest, plus the `EstimateMethod` recorded against
  those items (`loopward_api::context::EstimateMethod`). If any item's
  estimate is missing, say so rather than silently summing only the ones
  that have one.
- **Reported input/output tokens** — `WorkUnitResult.usage.known` /
  `.estimated` for this Episode's Attempt, when the runner reports them; the
  metric names it used (`docs/09-public-api.md`'s `UsageReport` has no fixed
  schema for these). Write `unavailable` for a Verification Episode (no
  Attempt runs) or a runner that does not report usage
  (`RunnerCapabilities.reports_token_usage == false`).
- **Accepted state transitions** — how many durable events this Episode
  produced that represent forward, accepted progress (`attempt.completed`,
  `verification.completed` with `Accepted`, `checkpoint.committed`) — the
  numerator of `docs/10` §10's "useful work per reported token".
- **Repeated resource reads** — how many resources this Episode's Boot
  Package carried that a *prior* Episode's Boot Package also carried in
  full (not by reference) — the numerator of `docs/10` §10's "context carry
  estimate". Zero for Episode 1 by construction.
- **Rotation reason** — the `rotation.requested` event's reason this Episode
  closed with (`WorkUnitBoundary`, `VerificationBoundary`,
  `SoftContextBudget`, `HardContextBudget`, `RunnerFailure`,
  `ContextIntegrityFailure`, `OperatorRequest`), or `n/a` if the Episode
  suspended before reaching one.
- **Verification outcome** — `Accepted` / `Rejected` / `Inconclusive` /
  `Suspended` / `Stale` for a Verification Episode; `n/a (Execution)` for an
  Execution Episode.
- **Recovery events** — any `loopward_runtime::RecoveryOutcome` this Episode
  was resolved through (most Episodes have none — a clean, uninterrupted
  Episode never calls `RecoveryManager`), and the crash point it corresponds
  to (`loopward_testkit::CrashPoint`, when the interruption was injected
  rather than incidental).
- **Trace reference** — the Episode's root `loopward.episode` span's
  `trace_id` (`RecordedSpan.context.trace_id` when tracing is captured
  in-process via `loopward_testkit::TraceRecorder`, or the OTLP trace id when
  exported), and its `continuation_of` link's target (the prior Episode's
  `trace_id`), so trace continuity (`docs/10` §13: "every Episode is
  trace-linked to its predecessor") is independently checkable from this
  report alone.

## 3. Success metrics (`docs/10` §10)

| Metric | Formula | Value | Notes |
| --- | --- | --- | --- |
| Constraint retention | required constraints present and unchanged after every rotation | pass / fail | compare each Episode's Boot Package `pinned_digest`; they must be identical for a Loop whose pinned constraint set never changed |
| Recovery correctness | expected state after every injected failure | pass / fail | one row per crash point actually exercised this run, cross-referenced against `loopward_testkit::CrashPoint::expected_recovery` |
| Accepted rework rate | repeated accepted work / prior accepted work | | |
| Context carry estimate | historical material repeated in subsequent Boot Packages | | sum of "repeated resource reads" above, over total resources referenced |
| Useful work per reported token | accepted state transitions / reported processed tokens | | `unavailable` if reported tokens are `unavailable` for every Episode — do not divide by an estimated figure and call the result "reported" |
| Trace continuity | Episodes correctly linked through Checkpoints | | fraction of Episodes after the first whose trace reference table entry above resolves to the immediately preceding Episode's `trace_id` |

## 4. Failure injection coverage (deterministic conformance runs only)

| Crash point (`docs/10` §8) | Injected this run? | Recovery outcome observed | Matches `CrashPoint::expected_recovery`? |
| --- | --- | --- | --- |
| before runner start | | | |
| after external start but before handle persistence | | | |
| during runner execution | | | |
| after result materialization | | | |
| during verification | | | |
| before Checkpoint transaction | | | |
| after Checkpoint commit but before process acknowledgement | | | |
| during Session drain | | | |

A full conformance pass exercises all eight
(`crates/loopward-testkit/tests/failure_injector.rs` does this against a
real `SqliteLoopStore` on every `cargo test -p loopward-testkit`); a
real-runner comparative evaluation run may exercise fewer, and should say so
rather than leaving rows blank.

## 5. Comparative runner notes (real-runner evaluation only, `docs/10` §9.2)

| Dimension | This runner profile | Notes |
| --- | --- | --- |
| Fresh Session per Work Unit, or runner-managed continuous Session | | |
| Token usage reporting | reported \| estimated \| unavailable | |
| Boot Package size vs. the fixture's 30,000 estimated-token target (`docs/10` §7) | | only meaningful for the context stress scenario |
| Repeated reads / repeated work observed | | |
| Time to accepted completion | | |

## 6. Summary

A short prose paragraph: what passed, what did not, and — critically — which
cells above are `unavailable` and why, so a reader cannot mistake an
`unavailable` runner metric for a zero or a pass.
