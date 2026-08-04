#![forbid(unsafe_code)]

//! `loopward`: the Phase One operator CLI.
//!
//! This binary is the composition root: it is the one place in the
//! workspace that assembles a real [`loopward_runtime::LoopController`] from
//! concrete adapters (`loopward-store-sqlite`, `loopward-runner-command`,
//! `loopward-observability-otel`) and drives it as a `LoopService` client —
//! see `commands` for the subcommands themselves and `config` for how their
//! shared flags resolve.
//!
//! # Tracing
//!
//! Tracing defaults to disabled and this binary never starts a telemetry
//! backend of its own (Global Constraint: "MUST operate with tracing
//! disabled and without a telemetry backend"; `docs/08-tracing-and-observability.md`
//! §2, §3). There is nothing for this module to initialize beyond what
//! `commands::build_service` already does: enabling `OpenTelemetry` export
//! is entirely a matter of setting the standard `OTEL_EXPORTER_OTLP_ENDPOINT`
//! environment variable, which
//! [`loopward_observability_otel::OtelTelemetrySink::from_env`] reads
//! directly when `build_service` constructs the `TelemetrySink` for this
//! invocation — no `--enable-tracing` flag, and no separate subscriber to
//! install here. (Nothing in this workspace emits through the `tracing`
//! crate's own macros; `TelemetrySink` is Loopward's own, deliberately
//! narrower, emission contract, so there is no `tracing_subscriber` fmt
//! layer for this binary to wire up either.)

mod commands;
mod config;

use std::process::ExitCode;

use clap::Parser;

/// The `loopward` operator CLI: local durable Loop operations over a
/// composition root assembled from this process's configuration.
#[derive(Debug, Parser)]
#[command(name = "loopward", version, about = "Operate Loopward Loops locally.")]
struct Cli {
    #[command(subcommand)]
    command: commands::Command,
    #[command(flatten)]
    config: config::ConfigArgs,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let config = cli.config.resolve();
    match commands::run(config, cli.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
