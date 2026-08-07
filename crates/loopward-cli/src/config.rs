//! Configuration resolution for the `loopward` operator CLI.
//!
//! Every value the composition root needs to assemble a real
//! [`loopward_runtime::ports::RuntimePorts`] and [`loopward_runtime::LoopController`]
//! is resolved here, in one place, before any subcommand runs: the `SQLite`
//! database path, the resource root, the command runner's working root, the
//! lease duration, and the runner profile (the executable
//! [`loopward_runner_command::CommandRunner`] invokes for each Work Unit
//! Attempt).
//!
//! Every value has a CLI flag and a matching environment variable fallback
//! (`clap`'s `env` attribute), with a local-development-friendly default
//! rooted at `.loopward/` in the current directory — except the runner
//! profile, which has no honest default (see [`ConfigArgs::runner_profile`]).
//!
//! Tracing is resolved separately, in `main.rs`, directly from the standard
//! `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable
//! ([`loopward_observability_otel::exporter::OTEL_EXPORTER_OTLP_ENDPOINT_ENV`]):
//! tracing defaults to disabled, and enabling OTLP export never requires a
//! CLI flag of its own, matching the Global Constraint that enabling OTLP
//! "MUST use standard environment variables and MUST NOT start another
//! service."

use std::path::PathBuf;

use clap::Args;

/// The default `SQLite` database file, relative to the current directory.
pub const DEFAULT_DATABASE_PATH: &str = ".loopward/loopward.sqlite3";
/// The default resource root, relative to the current directory.
pub const DEFAULT_RESOURCE_ROOT: &str = ".loopward/resources";
/// The default command-runner working root, relative to the current
/// directory.
pub const DEFAULT_RUN_ROOT: &str = ".loopward/runs";
/// The default lease duration, in seconds.
pub const DEFAULT_LEASE_SECONDS: u64 = 60;

/// Global configuration flags, present on every subcommand.
#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// The `SQLite` database file backing durable Loop state.
    #[arg(
        long = "database",
        global = true,
        env = "LOOPWARD_DATABASE_PATH",
        default_value = DEFAULT_DATABASE_PATH
    )]
    database_path: PathBuf,

    /// The root directory large resource content (Boot Packages, runner
    /// results, verification evidence) is stored under, content-addressed.
    #[arg(
        long = "resource-root",
        global = true,
        env = "LOOPWARD_RESOURCE_ROOT",
        default_value = DEFAULT_RESOURCE_ROOT
    )]
    resource_root: PathBuf,

    /// The root directory the command runner isolates each Work Unit
    /// Attempt's working directory under.
    #[arg(
        long = "run-root",
        global = true,
        env = "LOOPWARD_RUN_ROOT",
        default_value = DEFAULT_RUN_ROOT
    )]
    run_root: PathBuf,

    /// How long each acquired execution lease is valid for, in seconds.
    #[arg(
        long = "lease-seconds",
        global = true,
        env = "LOOPWARD_LEASE_SECONDS",
        default_value_t = DEFAULT_LEASE_SECONDS
    )]
    lease_seconds: u64,

    /// The runner profile: the executable invoked as the Work Unit runner
    /// for each Attempt.
    ///
    /// Deliberately has no default. Phase One's `LoopController` requires
    /// *some* `WorkUnitRunner` to construct at all (see
    /// `commands::NullRunner`'s doc comment for what stands in when this is
    /// unset), but there is no honest default executable to run on a
    /// caller's behalf — unlike the database, resource root, run root, and
    /// lease duration, which all have safe, local, side-effect-free
    /// defaults.
    #[arg(
        long = "runner-profile",
        global = true,
        env = "LOOPWARD_RUNNER_PROFILE"
    )]
    runner_profile: Option<PathBuf>,
}

/// Fully resolved configuration for one CLI invocation.
#[derive(Debug, Clone)]
pub struct Config {
    /// The `SQLite` database file backing durable Loop state.
    pub database_path: PathBuf,
    /// The root directory resource content is stored under.
    pub resource_root: PathBuf,
    /// The root directory the command runner isolates Attempts under.
    pub run_root: PathBuf,
    /// The execution lease duration.
    pub lease_ttl: time::Duration,
    /// The runner profile, when configured.
    pub runner_profile: Option<PathBuf>,
}

impl ConfigArgs {
    /// Resolves these arguments (already merged with environment fallbacks
    /// by `clap`) into a [`Config`].
    #[must_use]
    pub fn resolve(self) -> Config {
        let lease_seconds = i64::try_from(self.lease_seconds).unwrap_or(i64::MAX);
        Config {
            database_path: self.database_path,
            resource_root: self.resource_root,
            run_root: self.run_root,
            lease_ttl: time::Duration::seconds(lease_seconds),
            runner_profile: self.runner_profile,
        }
    }
}
