# Loopward

Loopward is a Rust framework that gives long-running agent work a durable
lifecycle. Its foundational rule:

> The Loop is durable; the Session is replaceable; state lives outside the
> Session; context is reconstructed for the current work.

A **Loop** persists across many bounded, disposable **Runtime Sessions**.
Authoritative state — Checkpoints, constraints, verification results, the
event journal — lives in SQLite, not in a conversation. Each unit of work
(a **Work Unit**) gets a freshly assembled, bounded **Boot Package** rather
than an ever-growing transcript, and every runner result is untrusted until
it is validated and verified.

Loopward does not orchestrate agents, define workflows, or provide a DAG
engine. It owns Loop, Episode, Session, and Work Unit lifecycle; a **runner**
(any external process, agent, or script) does the actual work behind a
stable public contract.

## Status

This is **Phase One**: a design draft with a working reference
implementation, not a finished product. Some fields accepted by the public
API — `goal`, `success_criteria`, `pinned_constraints`, `domain_schema` —
are validated but not yet stored durably (see
`RuntimeLoopService::create`'s doc comment in
`crates/loopward-runtime/src/controller/service.rs`), and `create()` does
not yet attach a Work Unit to a new Loop. See
[`docs/phase-one-conformance.md`](docs/phase-one-conformance.md) for the
current, honestly-reported conformance status and known gaps.

## Workspace layout

```text
loopward/
├── crates/
│   ├── loopward-api/               stable public wire contracts
│   ├── loopward-core/              pure domain model and transitions
│   ├── loopward-runtime/           the Loop Controller and lifecycle engine
│   ├── loopward-store-sqlite/      durable SQLite storage
│   ├── loopward-observability/     backend-neutral telemetry contract
│   ├── loopward-observability-otel/ OpenTelemetry adapter
│   ├── loopward-runner-command/    local process (command) runner
│   ├── loopward-cli/               the `loopward` operator CLI
│   └── loopward-testkit/           deterministic fakes and conformance suites
├── docs/     design documents (read in numeric order, start at docs/README.md)
├── wiki/     internal architecture wiki for contributors to Loopward itself
└── tests/    fixtures shared across integration tests
```

For crate responsibilities and dependency rules in detail, see
[`docs/01-system-architecture.md`](docs/01-system-architecture.md).

## Requirements

- Rust `1.97.1` (pinned in [`rust-toolchain.toml`](rust-toolchain.toml); if
  you use `rustup`, it is fetched automatically)
- SQLite is bundled via `sqlx`'s `sqlite` feature — no separate install needed

## Building

```bash
cargo build --workspace
```

## Using the CLI

The `loopward` binary (`crates/loopward-cli`) is the operator interface. It
resolves its database, resource root, run root, and lease duration from CLI
flags or matching environment variables, all rooted at `.loopward/` by
default:

```bash
cargo run -p loopward-cli -- init
cargo run -p loopward-cli -- create --spec '{
  "kind": "example",
  "goal": "describe the goal",
  "domain_schema": "example-v1"
}'
cargo run -p loopward-cli -- status <loop-id>
cargo run -p loopward-cli -- events <loop-id>
cargo run -p loopward-cli -- advance <loop-id>
```

Every subcommand accepts `--json` to emit a versioned JSON envelope instead
of the human-readable summary. Run `cargo run -p loopward-cli -- --help` for
the full subcommand list (`init`, `create`, `advance`, `run`, `status`,
`events`, `suspend`, `resume`, `cancel`, `inspect-checkpoint`,
`verify-store`).

Advancing a Loop past creation requires a **runner profile** — the
executable invoked for each Work Unit Attempt (`--runner-profile` /
`LOOPWARD_RUNNER_PROFILE`). Without one, the CLI substitutes a no-op runner
that cannot make progress; see `crates/loopward-cli/src/commands.rs`'s
`NullRunner` doc comment. The runner protocol itself is described in
[`docs/05-work-unit-and-runner-protocol.md`](docs/05-work-unit-and-runner-protocol.md).

Tracing is off by default and enabled only by setting the standard
`OTEL_EXPORTER_OTLP_ENDPOINT` environment variable — there is no CLI flag
for it, and the CLI never starts a telemetry backend of its own.

## Documentation

- [`docs/README.md`](docs/README.md) — design documents, read in numeric
  order starting with the [Project Charter](docs/00-project-charter.md) and
  [System Architecture](docs/01-system-architecture.md)
- [`wiki/README.md`](wiki/README.md) — internal architecture wiki, for
  developers working on Loopward itself
- [`docs/schemas/`](docs/schemas) — structural drafts of the wire contracts
  (Checkpoint, Work Unit, Work Unit Result, Boot Package Manifest, Event)

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for building, testing, and
contributing to this workspace.

## License

Apache-2.0, per [`Cargo.toml`](Cargo.toml).
