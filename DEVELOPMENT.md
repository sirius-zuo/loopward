# Developing Loopward

This document covers building, testing, and contributing to the Loopward
workspace itself. For what Loopward is and how to use the CLI, see
[`README.md`](README.md).

## Toolchain

The workspace pins an exact toolchain in
[`rust-toolchain.toml`](rust-toolchain.toml): channel `1.97.1` with the
`clippy` and `rustfmt` components. If you use `rustup`, running any `cargo`
command in this directory fetches and uses that toolchain automatically —
no manual `rustup override` needed.

## Building

```bash
cargo build --workspace
```

## Testing

```bash
cargo test --workspace
```

This runs unit tests, integration tests, and doc tests across every crate,
including `crates/loopward-cli/tests/e2e_phase_one.rs` (the Phase One
end-to-end conformance scenario) and `runner_profile_parity.rs`.

To run a single crate's tests, or a single test by name substring:

```bash
cargo test -p loopward-runtime
cargo test -p loopward-cli e2e_phase_one
```

`loopward-testkit` provides the deterministic fakes integration tests build
on: `FixedClock`, `SequentialIdGenerator`, `FakeRunner`, `AttemptScript`,
`FailureInjector` (for crash-point injection), and `TraceRecorder`. Prefer
these over hand-rolled mocks when writing new runtime tests.

## Linting and formatting

CI (`.github/workflows/ci.yml`) runs three gates on every push and pull
request against `main`; run all three locally before pushing:

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The workspace denies `clippy::all` and `clippy::pedantic`
(`[workspace.lints.clippy]` in [`Cargo.toml`](Cargo.toml)) and forbids
`unsafe_code` outright. Expect Clippy to be strict; fix the lint rather than
suppressing it unless there's a specific, documented reason not to.

## Database migrations

`loopward-store-sqlite` embeds its migrations from
`crates/loopward-store-sqlite/migrations/` at compile time via
`sqlx::migrate!`. Add new migrations as new numbered `.sql` files in that
directory rather than editing an existing one — Phase One's durable schema
is versioned, and existing migrations are treated as immutable history.

## Workspace and dependency rules

The crate graph is a strict layering, enforced by review rather than a
build-time check (see
[`docs/01-system-architecture.md`](docs/01-system-architecture.md) §3–4):

- `loopward-api` is the stable public contract. It has no dependency on any
  other crate in this workspace.
- `loopward-core` is pure domain logic and depends only on `loopward-api`.
  It MUST NOT depend on Tokio, SQLite, OpenTelemetry, or any vendor SDK.
- `loopward-runtime` depends on `loopward-api`, `loopward-core`, and
  `loopward-observability` only. It MUST NOT depend on a concrete `LoopStore`,
  runner, or telemetry exporter implementation — those are injected through
  the ports it defines.
- `loopward-store-sqlite`, `loopward-observability-otel`, and
  `loopward-runner-command` are adapters: they depend on the contracts and
  runtime crates above, never the other way around.
- `loopward-cli` is the composition root — the one place concrete adapters
  are wired into a real `LoopController`.
- `loopward-testkit` depends on `loopward-api`, `loopward-core`, and
  `loopward-runtime` to provide fakes and conformance suites for the other
  crates' tests.

When adding a dependency between workspace crates, check that it points
"up" this list, not down.

## Documentation conventions

- [`docs/`](docs) holds the numbered design documents (project charter,
  architecture, domain model, and so on), read in order starting from
  [`docs/README.md`](docs/README.md). These use normative language (MUST,
  MUST NOT, SHOULD, MAY) and are the source of truth for Phase One's
  intended behavior.
- [`wiki/`](wiki) is the internal architecture wiki, one page per subsystem,
  each ending in a **Source Anchors** section tying it back to specification
  and implementation sources. See [`wiki/README.md`](wiki/README.md) for the
  page template and maintenance convention. It documents Loopward's own
  architecture for contributors, not how to consume Loopward as a
  dependency.
- [`docs/schemas/`](docs/schemas) holds structural drafts of the wire
  contracts (Checkpoint, Work Unit, Work Unit Result, Boot Package Manifest,
  Event) — design artifacts, not yet generated types.

Rust doc comments (`///`, `//!`) throughout the crates are written densely
and are expected to be read as documentation in their own right — many
explain *why*, including known gaps and deliberate scope cuts, not just
what the code does. `cargo doc --workspace --no-deps` builds but is not
currently warning-free (see
[`docs/phase-one-conformance.md`](docs/phase-one-conformance.md)).

## Commit conventions

Recent history follows Conventional Commits with a scope, e.g.
`fix(recovery): ...`, `perf(store): ...`, `feat(observability): ...`,
`test(store): ...`. Match this style for new commits: `type(scope):
imperative summary`.

## Before opening a pull request

1. `cargo fmt --all --check`
2. `cargo test --workspace`
3. `cargo clippy --workspace --all-targets -- -D warnings`
4. If your change affects Phase One's conformance surface, update
   [`docs/phase-one-conformance.md`](docs/phase-one-conformance.md) rather
   than leaving it silently stale.
