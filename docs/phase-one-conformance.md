# Phase One Conformance Report

**Status: DONE_WITH_CONCERNS.** All mechanically-checkable gates in Task 14's
brief pass. Two genuine gaps were found while running the gate and are
recorded here rather than papered over: `cargo doc --workspace --no-deps`
exits `0` but is not warning-free, and `docs/10-mvp-and-evaluation.md` §13's
"at least two different command-runner profiles complete the same Work Unit
protocol fixture" criterion is not demonstrated by any test in this
workspace. Neither is fixed here — both are outside this task's file list
(`docs/README.md`, `docs/phase-one-conformance.md`, `docs/schemas/*` only),
and the second would require new test infrastructure, not a documentation
change.

**Task 15 addendum (post-Task-14):** the second gap — the runner-profile-parity
criterion — has since been closed by a narrowly-scoped new test,
`crates/loopward-cli/tests/runner_profile_parity.rs`, added specifically to
close this acceptance gate (not part of the original 14-task file list). See
§8 below for the updated account. The `cargo doc` warnings gap remains
unresolved; it is unrelated to this addendum and is not addressed here.

## 1. Build identification

| Field | Value |
| --- | --- |
| Commit SHA | `ce8d6e1ac2c22b66428c8f73ea5a788502b551c8` |
| Working tree | clean at time of this run (`git status --short` empty) |
| `rustc --version` | `rustc 1.97.1 (8bab26f4f 2026-07-14)` |
| `cargo --version` | `cargo 1.97.1 (c980f4866 2026-06-30)` |
| Toolchain pin | `rust-toolchain.toml`: channel `1.97.1`, components `clippy`, `rustfmt` |
| `Cargo.lock` SHA-256 | `fc51f62ce02145b3f99c15b88a9a600951e849bb0a3982a92607f398ad583a4b` |

## 2. Step 1 — Architecture boundary check

Command:

```bash
rg -n "Aether|OrchestrationProvider|DAG|workflow topology|agent registry" crates
```

Result: 2 matches, both in module-level doc comments that explicitly assert
absence of these concepts (not production references to them):

- `crates/loopward-core/src/model.rs:6-7` — "Nothing here performs I/O,
  reads a clock, or knows about orchestration, DAGs, workflow topology, or
  an agent registry."
- `crates/loopward-core/src/lib.rs:17` — "No orchestration, DAG, workflow
  topology, agent registry, or Aether concept."

No `OrchestrationProvider` type, trait, or symbol exists anywhere in
`crates/`. **Pass** — this is the brief's explicitly allowed case (a
narrowed, reviewed assertion of absence), not leakage.

## 3. Step 2 — Forbidden observability coupling check

Command:

```bash
rg -n "Grafana|Tempo|Jaeger|localhost|Web Preview" crates .github Cargo.toml
```

Result: zero matches (command exited with rg's "no matches" status).
**Pass.**

## 4. Step 3 — Rust quality gate

| Command | Exit | Result |
| --- | --- | --- |
| `cargo fmt --all --check` | 0 | clean, no diffs |
| `cargo test --workspace` | 0 | 204 passed, 0 failed, 1 ignored (a `Doc-tests loopward_store_sqlite` doc test marked `ignore`, pre-existing) |
| `cargo clippy --workspace --all-targets -- -D warnings` | 0 | clean, no warnings |
| `cargo doc --workspace --no-deps` | 0 | **builds successfully but emits 18 rustdoc warnings** (see below) |

### 4.1 `cargo test --workspace` breakdown

All 204 passing tests (0 failures), by test binary:

| Crate / test binary | Passed |
| --- | --- |
| `loopward-api` unittests | 3 |
| `loopward-api` `contract_roundtrip` | 7 |
| `loopward-api` `workspace_smoke` | 1 |
| `loopward-cli` unittests | 0 |
| `loopward-cli` `cli_contract` | 8 |
| `loopward-cli` `e2e_phase_one` | 1 |
| `loopward-core` unittests | 0 |
| `loopward-core` `lifecycle_contract` | 17 |
| `loopward-observability` unittests | 8 |
| `loopward-observability` `semantic_conventions` | 7 |
| `loopward-observability-otel` unittests | 3 |
| `loopward-observability-otel` `episode_links` | 3 |
| `loopward-runner-command` unittests | 3 |
| `loopward-runner-command` `runner_contract` | 16 |
| `loopward-runtime` unittests | 17 |
| `loopward-runtime` `advance_episode` | 14 |
| `loopward-runtime` `context_hydration` | 16 |
| `loopward-runtime` `port_contract` | 10 |
| `loopward-runtime` `recovery_matrix` | 19 |
| `loopward-runtime` `verification_contract` | 25 |
| `loopward-store-sqlite` unittests | 3 |
| `loopward-store-sqlite` `transaction_contract` | 20 |
| `loopward-testkit` unittests | 0 |
| `loopward-testkit` `failure_injector` | 2 |
| Doc-tests (all crates combined) | 1 passed, 1 ignored |
| **Total** | **204 passed, 0 failed, 1 ignored** |

### 4.2 `cargo doc` warning detail — genuine gap

The brief's Step 3 expects documentation to build "without warnings."
`cargo doc --workspace --no-deps` exits `0` (a hard success), but produces
18 distinct `rustdoc` diagnostics across three crates:

- `loopward-runtime` (lib doc): 15 warnings — mostly
  `rustdoc::broken_intra_doc_links` (links to `LoopStore::append`,
  `EpisodeStatus`, `Self::run_execution_episode`,
  `Self::reconcile_without_active_episode`, `finalize`, `abort` that are out
  of scope from the linking doc comment) and
  `rustdoc::private_intra_doc_links` (public doc comments linking to private
  items: `HEURISTIC_CONFIDENCE`, `DefaultContextCandidates`, `PRECEDENCE`,
  `validate_create_request`).
- `loopward-store-sqlite` (lib doc): 2 warnings — broken links to
  `SqliteLoopStore::load` and `SqliteLoopStore::commit_checkpoint` (neither
  exists as named; `SqliteLoopStore` implements the `LoopStore` trait rather
  than defining these as inherent methods).
- `loopward-observability-otel`: 1 warning — broken link to `SpanLink` out
  of scope in `exporter.rs`.

None of Tasks 1-13's reports or the progress ledger record ever having run
`cargo doc --workspace --no-deps` before this task — the ledger's Gate C
(after Task 10) and every task report's "Verification" section list `fmt`,
`test`, and `clippy` only. So this is not a regression introduced by this
task; it is a pre-existing condition that had never actually been checked
against the brief's literal "documentation builds without warnings" wording
until now. Fixing it would mean editing doc comments in
`loopward-runtime`, `loopward-store-sqlite`, and
`loopward-observability-otel` source files, which is outside this task's
file list (`docs/README.md`, `docs/phase-one-conformance.md`,
`docs/schemas/*` only). Recorded here as an honest, unresolved finding
rather than silently treated as passing.

## 5. Step 4 — Phase One behavioral gate

| Command | Exit | Result |
| --- | --- | --- |
| `cargo test -p loopward-cli --test e2e_phase_one` | 0 | 1 passed, 0 failed — `phase_one_reference_scenario_completes_after_one_rejection_and_remediation` |
| `cargo test -p loopward-runtime --test recovery_matrix` | 0 | 19 passed, 0 failed |
| `cargo test -p loopward-runtime --test context_hydration` | 0 | 16 passed, 0 failed |
| `cargo test -p loopward-observability-otel --test episode_links` | 0 | 3 passed, 0 failed |

### 5.1 Context stress result

`context_hydration.rs`'s `the_hundred_thousand_token_history_yields_a_bounded_verification_manifest`
is the context-stress-scenario test named in `docs/10-mvp-and-evaluation.md`
§7, backed by `tests/fixtures/scenario/large-history.json` (72 candidate
resources; independently summed via the fixture JSON, their declared
`estimated_tokens` total **102,700**, satisfying the test's own
`>= 100_000` assertion and the §7 "synthetic prior history larger than
100,000 tokens" requirement).

The test asserts, against the real `hydrate()` context-selection path:

- `available_boot_tokens(&fixture.budget) == 30_000`;
- the resulting Boot Package's `selected_tokens()` is `<= 30_000` **and**
  exactly `29_792` (a pinned, not just bounded, assertion);
- `selected <= available`.

This is the §7/§13 "context stress scenario meets its fixture budget" gate,
and it is distinct from Task 13's reference-scenario fixture, whose own
(much tighter) internal budget assertion in `e2e_phase_one.rs` is a
separate, unrelated `< 5,000`-estimated-token check for the five-Episode
reference scenario — not the same number, not the same fixture, and not
conflated here.

**Result: pass.** Boot Package for the context stress scenario = 29,792
estimated tokens, within the 30,000-token fixture budget.

### 5.2 Recovery matrix result

`recovery_matrix.rs` contains 19 tests total; 8 of them are named
`failure_point_1_before_runner_start` through
`failure_point_8_during_session_drain`, one per crash point in
`docs/10-mvp-and-evaluation.md` §8's ordered list. All 8 pass, plus the
other 11 recovery-related tests in the same file (dangling-attempt
reconciliation, in-episode reconcile outcomes, verification-episode
recovery, etc.).

`loopward-testkit`'s `failure_injector.rs` independently re-proves all
eight named crash points against a **real** `SqliteLoopStore` (not an
in-memory fake) via `all_eight_crash_points_recover_as_documented` and
`every_crash_point_documents_its_expected_recovery` — both pass.

**Result: pass.** All eight documented failure-injection cases recover as
specified, verified against both the in-memory test doubles
(`recovery_matrix.rs`) and a real SQLite-backed store
(`failure_injector.rs`).

### 5.3 Constraint retention (byte-equivalence across rotation)

`crates/loopward-cli/tests/e2e_phase_one.rs` asserts `pinned_digest`
equality across all five hydrated Boot Package manifests produced across
the reference scenario's five Episodes (`manifests[0].pinned_digest ==
manifest.pinned_digest` for every subsequent manifest) — the required
constraint set is unchanged, byte-for-byte, across every Session rotation
in the scenario. **Result: pass.**

### 5.4 Trace linkage

`episode_links.rs`'s `two_episodes_produce_linked_traces_with_propagated_runner_context`
proves two Episodes produce two distinct top-level traces with a
`continuation_of` link from the second to the first.
`e2e_phase_one.rs` independently asserts all five Episodes of the reference
scenario produce five top-level traces with `continuation_of` links between
every consecutive pair. **Result: pass** — every Episode is trace-linked to
its predecessor.

### 5.5 Tracing-disabled / no-telemetry-backend result

- `crates/loopward-cli/src/commands.rs` (the CLI's composition root) falls
  back to `NoopTelemetrySink` whenever no OTLP endpoint is configured via
  environment (`OtelTelemetrySink::from_env()? == None => Arc::new(NoopTelemetrySink)`),
  and this is the path exercised by `e2e_phase_one.rs` (its module doc:
  "proving the runtime operates fully [without] `NoopTelemetrySink`/an OTLP
  sink").
- `episode_links.rs` (`loopward-observability-otel`'s own test suite) uses
  no OTLP exporter, no `localhost` endpoint, and no external telemetry
  service — verified independently by Step 2's `rg` scan above, which
  covers `crates/` including this file, and returned zero matches for
  `localhost` anywhere.
- Most of the workspace's 204 tests (everything outside
  `loopward-observability-otel`) never depend on
  `loopward-observability-otel` at all — telemetry is a pluggable sink
  behind `loopward_observability::TelemetrySink`, not a hard runtime
  dependency.

**Result: pass.** The runtime demonstrably operates with tracing disabled
(`NoopTelemetrySink`) and without any external telemetry backend
configured.

## 6. Token measurements: known, estimated, unavailable

Per `docs/10-mvp-and-evaluation.md` §9.2 ("Unavailable runner metrics remain
unavailable") and the evaluation template's instruction to write
`unavailable` explicitly rather than leave a value blank or guessed:

| Measurement | Value | Classification |
| --- | --- | --- |
| Context stress scenario Boot Package size | 29,792 tokens | **Estimated** — sum of `BootPackageManifest.items[].estimated_tokens`, computed by `loopward-runtime`'s deterministic heuristic estimator (`EstimateMethod::Heuristic`), not a real tokenizer count against real model input |
| Context stress scenario declared prior history | 102,700 tokens | **Estimated** — same heuristic method, summed from `tests/fixtures/scenario/large-history.json`'s 72 fixture candidates |
| Reference scenario (five-Episode) final Boot Package size | < 5,000 tokens (per Task 13's own assertion in `e2e_phase_one.rs`; not re-derived here as an exact figure) | **Estimated** — same heuristic method; this is a separate, tighter, scenario-specific bound, not the §7 context-stress 30,000-token budget |
| Real-runner (`CommandRunner`/vendor-agent) reported token usage | — | **Unavailable** — no test or fixture in this workspace runs `loopward-runner-command::CommandRunner` against a real vendor agent command and records `WorkUnitResult.usage`; `runner_contract.rs` exercises `CommandRunner`'s protocol contract only (isolated attempt directories, environment variables, result-file handling, capture bounds, cancellation), against scripted test-fixture commands, not a real agent, and does not assert on token usage at all |
| Comparative real-vs-fake runner token/time/rework metrics (`docs/10` §9.2's comparison table) | — | **Unavailable** — no real-runner comparative evaluation was executed as part of this task; this workspace's evidence is exclusively deterministic conformance (fake/scripted runners), never a configured real agent command |

No number above is invented or rounded up to fill a cell; every entry is
either a value actually produced by a passing test in this run, or marked
`unavailable` with a stated reason.

## 7. Cross-reference: `docs/10-mvp-and-evaluation.md` §13 acceptance gates

| Gate | Status | Evidence |
| --- | --- | --- |
| All state-machine conformance tests pass | **Pass** | `lifecycle_contract.rs` (17), `verification_contract.rs` (25), `advance_episode.rs` (14), `port_contract.rs` (10), `transaction_contract.rs` (20) all pass; 204/204 workspace tests pass overall |
| All eight failure-injection scenarios recover as specified | **Pass** | §5.2 above — 8 `failure_point_*` tests in `recovery_matrix.rs` plus independent real-store proof in `loopward-testkit`'s `failure_injector.rs` |
| No required constraint changes across rotation | **Pass** | §5.3 above — `pinned_digest` identical across all 5 Episodes in `e2e_phase_one.rs` |
| Accepted results are always evidence-bound and verified | **Pass** | `verification_contract.rs` (25 tests) exercises the deterministic verification gate requiring evidence before `Accepted`; `e2e_phase_one.rs`'s final Episode is `Accepted` only once the marker evidence exists |
| The context stress scenario meets its fixture budget | **Pass** | §5.1 above — 29,792 <= 30,000 estimated tokens |
| Every Episode is trace-linked to its predecessor | **Pass** | §5.4 above |
| The runtime operates with tracing disabled | **Pass** | §5.5 above — `NoopTelemetrySink` path exercised by `e2e_phase_one.rs` |
| The runtime operates without a telemetry backend | **Pass** | §5.5 above — no OTLP endpoint, no external service, in any passing test |
| No Aether or orchestration concept appears in Core | **Pass** | §2 above |
| At least two different command-runner profiles complete the same Work Unit protocol fixture | **Pass** | See §8 below (Task 15 addendum) — `crates/loopward-cli/tests/runner_profile_parity.rs` |
| The evaluation report clearly distinguishes known, estimated, and unavailable token data | **Pass (template); no filled instance exists yet** | `docs/phase-one-evaluation-template.md` (built in Task 13) defines this structure and requires `unavailable` to be written explicitly rather than left blank; §6 of this document is itself one filled instance, scoped to the deterministic-conformance data produced by this run only. No real-runner evaluation instance of the template has been filled in (none can be, absent a configured real agent command) |

## 8. Command-runner-profile parity — closed by Task 15

`docs/10-mvp-and-evaluation.md` §13 requires "at least two different
command-runner profiles complete the same Work Unit protocol fixture."
Phase One has exactly two runner implementations of
`loopward_api::runner::WorkUnitRunner`:

1. `loopward_runner_command::CommandRunner` — the one real execution
   adapter (Task 7), which shells out to an arbitrary external command.
2. `loopward_testkit::FakeRunner` — the scripted in-memory test double
   (Task 13), used to drive the five-Episode reference scenario in
   `e2e_phase_one.rs`.

### 8.1 Task 14's original finding (honest unmet criterion)

At the time Task 14 ran this gate, I checked whether any existing test runs
the *same* fixture or scenario through both and asserts equivalent outcomes:

- `crates/loopward-runner-command/tests/runner_contract.rs` exercises
  `CommandRunner` exclusively, against ad hoc scripted shell commands
  built inline in the test file (isolated attempt directories, documented
  environment variables, result-file handling, log capture bounds,
  cancellation) — it never touches
  `tests/fixtures/scenario/retry-loop.json` (the reference scenario
  fixture) or any fixture `FakeRunner` also runs.
- `crates/loopward-cli/tests/e2e_phase_one.rs` exercises `FakeRunner`
  exclusively against `retry-loop.json`.
- `crates/loopward-cli/src/commands.rs` wires `CommandRunner` as the real
  runner behind the CLI's `--runner-command` composition path, but
  `cli_contract.rs` does not use `CommandRunner` in its lifecycle test (it
  relies on the CLI's `NullRunner`/default path), and in any case does not
  run the same fixture that `e2e_phase_one.rs` runs through `FakeRunner`.

**Conclusion at the time: this criterion was genuinely unmet.** No test in
the workspace demonstrated two different runner profiles completing the
same Work Unit protocol fixture with equivalent outcomes. Fixing it meant
writing new integration-test infrastructure, which was new scope beyond
Task 14's file list (`docs/README.md`, `docs/phase-one-conformance.md`, and
`docs/schemas/*` only). It was recorded as an honest gap for a follow-up
task.

### 8.2 Task 15 addendum: the gap is now closed

A new, narrowly-scoped test closes this gap without re-running the full
five-Episode reference scenario (Task 13's scripted multi-Episode narrative
of rejection and remediation) through a real external process — that would
be disproportionate scope for what §13 actually asks for. Instead, one
simple Work Unit protocol fixture (one `WorkUnitRequest` shape and one
expected successful `WorkUnitResult` — `outcome: Produced`, carrying one
deterministic output resource) is driven through both runner profiles:

- `crates/loopward-cli/tests/runner_profile_parity.rs` (new file; this
  crate already carries `loopward-runner-command` as a normal dependency
  and `loopward-testkit` as a dev-dependency, so no `Cargo.toml` change was
  needed).
- `tests/fixtures/runner/parity.sh` (new file) is a small, portable POSIX
  shell command, invoked through `CommandRunner` exactly the way
  `runner_contract.rs`'s existing fixture scripts are (isolated attempt
  directory, `LOOPWARD_RESULT_PATH`, etc.) — it writes a fixed,
  hand-authored `result.json` declaring `outcome: "produced"` and one
  output resource with a fixed `uri`/`digest`/`size`/`summary`.
- The same output resource, byte-for-byte identical, is what
  `loopward_testkit::FakeRunner` is scripted (via
  `AttemptScript::succeeding_with`) to return for the same fixture request
  shape.
- The test's single assertion path (`two_different_runner_profiles_complete_the_same_work_unit_protocol_fixture`)
  drives the fixture through both `CommandRunner` and `FakeRunner` and
  asserts both collected results share the same protocol version, the same
  runner-asserted `WorkUnitOutcome`, and the same declared output resource
  content.

Note on terminology: `WorkUnitOutcome` (`crates/loopward-api/src/work_unit.rs`)
has exactly three variants — `Produced`, `Failed`, `Partial` — and
deliberately excludes acceptance-pipeline states such as `Accepted`
(`docs/06-verification-and-recovery.md` §2's own distinction: a runner
result is a proposal, never Loopward's own later acceptance determination).
"Equivalent successful outcome" for this gate therefore means both profiles
report `WorkUnitOutcome::Produced` for the same fixture with equivalent
output content — the full accept/verify pipeline is intentionally out of
scope for a runner-profile-parity test, which operates below that gate.

**Result: pass.** `cargo test -p loopward-cli --test runner_profile_parity`
— 1 passed, 0 failed. Verified together with the rest of the workspace:
`cargo test --workspace` — 205 passed, 0 failed, 1 ignored (the same
pre-existing ignored `Doc-tests loopward_store_sqlite` doc test noted in
§4.1); `cargo fmt --all --check` and
`cargo clippy --workspace --all-targets -- -D warnings` both clean.

## 9. Schema parity

`docs/schemas/*.yaml` were not modified. `cargo test --workspace`'s
contract-parity suites (`loopward-api`'s `contract_roundtrip.rs`, 7 tests;
`loopward-core`'s `lifecycle_contract.rs`, 17 tests; and several other
`*_contract.rs` files across `loopward-runtime`, `loopward-runner-command`,
and `loopward-store-sqlite`) all pass, and no discrepancy between the Rust
types and the YAML schemas was found during this run. No schema edits were
necessary or made.

## 10. Commands executed (verbatim, in order)

```bash
rg -n "Aether|OrchestrationProvider|DAG|workflow topology|agent registry" crates
rg -n "Grafana|Tempo|Jaeger|localhost|Web Preview" crates .github Cargo.toml
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
cargo test -p loopward-cli --test e2e_phase_one
cargo test -p loopward-runtime --test recovery_matrix
cargo test -p loopward-runtime --test context_hydration
cargo test -p loopward-observability-otel --test episode_links
```

## 11. Summary

Phase One's deterministic-conformance evidence is complete and passing:
204/204 tests green, zero Clippy warnings, `fmt` clean, all eight recovery
scenarios proven against both fakes and a real SQLite store, constraint
retention proven byte-exact across rotation, trace linkage proven across
Episodes, the context-stress fixture within its 30,000-token budget, and
the runtime proven to run correctly with tracing disabled and without a
telemetry backend. No Aether/orchestration concept appears anywhere in
production code.

Two items kept this report at DONE_WITH_CONCERNS rather than a clean pass:
`cargo doc --workspace --no-deps` succeeds but is not warning-free (18
rustdoc diagnostics, pre-existing, never previously checked), and the
runner-profile-parity acceptance criterion (§13's two-runner-profile
requirement) had no supporting test anywhere in this workspace. Both are
recorded as genuine findings rather than silently resolved or worked
around, per this task's mandate to report evidence honestly.

**Task 15 addendum:** the runner-profile-parity item has since been closed
— see §8.2. Only the `cargo doc` rustdoc-warnings item remains open; it is
unrelated to this addendum and was out of scope for it.
