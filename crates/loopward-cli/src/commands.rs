//! The `loopward` subcommands: thin `LoopService` clients over the
//! composition root this crate assembles.
//!
//! Every mutating subcommand builds a versioned [`CreateLoopRequest`] or
//! `*Command` carrying fresh [`CommandMetadata`] (a `UUIDv7` request id and
//! idempotency key, generated per invocation — see [`new_command_metadata`]'s
//! doc comment for what that does and does not buy a caller) and, for
//! everything but `create`, an `expected_revision` read from a fresh
//! [`LoopService::status`] call immediately beforehand (see
//! [`expected_revision`]'s doc comment). Human-readable output is a concise
//! one-line summary; `--json` emits the underlying wire DTO wrapped in a
//! small [`JsonEnvelope`] that carries the protocol version alongside it.
//!
//! This module also closes two gaps no prior task left a real
//! implementation for, documented where they are defined:
//!
//! * [`SystemClock`] and [`UuidIdGenerator`]: the first concrete
//!   [`Clock`]/[`IdGenerator`] ports in this workspace outside test fakes.
//! * [`DefaultVerificationPlanResolver`]: the first concrete
//!   [`VerificationPlanResolver`], a fixed, permissive default plan so the
//!   CLI can actually drive a `LoopController` through verification.
//!
//! and one stand-in, [`NullRunner`], for when no runner profile is
//! configured.

use std::sync::Arc;

use async_trait::async_trait;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;

use loopward_api::error::LoopwardError;
use loopward_api::event::{EventCursor, EventDurabilityFilter, EventPage, EventQuery};
use loopward_api::ids::{
    AttemptId, CheckpointId, EpisodeId, EventId, IdParseError, LoopId, OperationId, SessionId,
    Timestamp, WorkUnitId,
};
use loopward_api::loop_service::{
    AdvanceLoopCommand, CancelLoopCommand, CommandMetadata, Constraint, CreateLoopRequest, Goal,
    LoopKind, LoopPolicies, LoopService, LoopStatusView, OperationHandle, ResourceCost,
    ResumeLoopCommand, RunCondition, RunLimits, RunUntilCommand, SuccessCriterion,
    SuspendLoopCommand,
};
use loopward_api::resource::{SchemaRef, VerificationPlanRef};
use loopward_api::runner::{
    CancellationReason, ReconcileOutcome, RunHandle, RunStatus, RunnerCapabilities, RunnerError,
    RunnerErrorCode, RunnerEventPage, SuspensionReason, WorkUnitRunner,
};
use loopward_api::trace::TraceContext;
use loopward_api::work_unit::{WorkUnitRequest, WorkUnitResult};
use loopward_observability::{NoopTelemetrySink, TelemetrySink};
use loopward_observability_otel::OtelTelemetrySink;
use loopward_runner_command::{CommandRunner, CommandRunnerConfig};
use loopward_runtime::ports::{LoopStore, ResourceBroker, StoreError};
use loopward_runtime::verification::{
    AggregationPolicy, GateConfig, GateEnforcement, GateId, RemediationPolicy, SchemaGateConfig,
    VerificationGateKind, VerificationGateSpec, VerificationPlanId,
};
use loopward_runtime::{
    Clock, IdGenerator, LoopController, RuntimeLoopService, RuntimePorts, VerificationPlanResolver,
    VerificationPlanTemplate,
};
use loopward_store_sqlite::{FileResourceBroker, SqliteLoopStore, SqliteStoreConfig};

use crate::config::Config;

// ---------------------------------------------------------------------------
// CLI surface
// ---------------------------------------------------------------------------

/// The `loopward` subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initializes local durable storage: creates (or opens) the database,
    /// applying every pending migration, and creates the resource and run
    /// root directories.
    Init {
        /// Emit the resolved paths as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Creates a new Loop from a JSON spec.
    Create {
        /// A JSON document describing the new Loop; see `CreateSpec`.
        #[arg(long)]
        spec: String,
        /// Emit the created Loop handle as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Advances a Loop by at most one durable Episode transition.
    Advance {
        /// The Loop to advance.
        loop_id: String,
        /// Emit the resulting operation handle as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Repeatedly advances a Loop until it reaches a terminal outcome (or
    /// suspends, if requested) or a supplied limit is reached.
    ///
    /// At least one of `--max-episodes`, `--deadline`, `--max-attempts`, or
    /// `--max-tokens` is required; an unbounded `run` is rejected by
    /// `LoopController` itself (`InvalidRequest`), not pre-checked here.
    Run {
        /// The Loop to run.
        loop_id: String,
        /// Stop once at most this many Episodes have been advanced through.
        #[arg(long)]
        max_episodes: Option<u32>,
        /// Stop once this RFC 3339 wall-clock deadline has passed.
        #[arg(long)]
        deadline: Option<String>,
        /// Stop once this many Work Unit Attempts have been started.
        #[arg(long)]
        max_attempts: Option<u32>,
        /// Stop once this many estimated tokens have been processed.
        #[arg(long)]
        max_tokens: Option<u64>,
        /// When to stop: at a terminal outcome only, or at a terminal
        /// outcome or suspension, whichever comes first.
        #[arg(long, value_enum, default_value_t = RunConditionArg::UntilTerminal)]
        condition: RunConditionArg,
        /// Emit the resulting operation handle as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Prints a compact status view for a Loop.
    Status {
        /// The Loop to query.
        loop_id: String,
        /// Emit the status view as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Queries the durable event journal for a Loop.
    Events {
        /// The Loop to query.
        loop_id: String,
        /// Return only events after this opaque cursor.
        #[arg(long)]
        after: Option<String>,
        /// The maximum number of events to return.
        #[arg(long, default_value_t = 100)]
        limit: u32,
        /// Emit the event page as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Requests a Loop suspend at its next safe boundary.
    Suspend {
        /// The Loop to suspend.
        loop_id: String,
        /// A caller-supplied reason for the suspension.
        #[arg(long)]
        reason: Option<String>,
        /// Emit the resulting operation handle as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Requests a suspended Loop resume.
    Resume {
        /// The Loop to resume.
        loop_id: String,
        /// Emit the resulting operation handle as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Explicitly cancels a Loop.
    Cancel {
        /// The Loop to cancel.
        loop_id: String,
        /// A caller-supplied reason for the cancellation.
        #[arg(long)]
        reason: Option<String>,
        /// Emit the resulting operation handle as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Prints the Loop's most recently committed Checkpoint.
    ///
    /// An operator diagnostic outside the versioned `LoopService` contract
    /// (`LoopService` deliberately never returns a full Checkpoint,
    /// `docs/09-public-api.md` §6): this reads directly from the configured
    /// `LoopStore`.
    InspectCheckpoint {
        /// The Loop to inspect.
        loop_id: String,
        /// Emit the checkpoint view as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Verifies the configured `SQLite` database opens and is fully
    /// migrated, without creating it if missing.
    VerifyStore {
        /// Emit the result as a versioned JSON document.
        #[arg(long)]
        json: bool,
    },
}

/// The `run --condition` values, mirroring
/// [`loopward_api::loop_service::RunCondition`].
///
/// A local mirror rather than deriving `clap::ValueEnum` on the `loopward-api`
/// type directly: that type belongs to the wire contract crate, which has no
/// reason to depend on `clap`.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum RunConditionArg {
    /// Stop once the Loop reaches any terminal outcome.
    UntilTerminal,
    /// Stop once the Loop reaches a terminal outcome or suspends.
    UntilTerminalOrSuspended,
}

impl From<RunConditionArg> for RunCondition {
    fn from(value: RunConditionArg) -> Self {
        match value {
            RunConditionArg::UntilTerminal => Self::UntilTerminal,
            RunConditionArg::UntilTerminalOrSuspended => Self::UntilTerminalOrSuspended,
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Everything a `loopward` invocation can fail with.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// The `LoopService` rejected the request.
    #[error(transparent)]
    Service(#[from] LoopwardError),
    /// A `LoopStore` operation failed, outside the `LoopService` contract
    /// (used by `inspect-checkpoint`'s direct store read).
    #[error(transparent)]
    StoreOperation(#[from] StoreError),
    /// The configured database could not be opened or migrated.
    #[error(transparent)]
    Store(#[from] loopward_store_sqlite::SqliteStoreError),
    /// The OTLP exporter named by `OTEL_EXPORTER_OTLP_ENDPOINT` could not be
    /// built.
    #[error("failed to build the OTLP exporter: {0}")]
    Telemetry(#[from] opentelemetry_otlp::ExporterBuildError),
    /// A `--spec` argument was not valid JSON for `CreateSpec`.
    #[error("invalid --spec json: {0}")]
    InvalidSpec(#[source] serde_json::Error),
    /// A `--deadline` argument was not a valid RFC 3339 timestamp.
    #[error("invalid --deadline: {0}")]
    InvalidDeadline(#[source] time::error::Parse),
    /// A `<loop-id>` argument was not a valid identifier.
    #[error("invalid loop id `{value}`: {source}")]
    InvalidLoopId {
        /// The raw text that failed to parse.
        value: String,
        /// The underlying parse failure.
        #[source]
        source: IdParseError,
    },
    /// A filesystem operation (creating a configured directory) failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Runs one resolved subcommand.
///
/// # Errors
///
/// Returns whatever the subcommand's underlying operation failed with; see
/// [`CliError`].
pub async fn run(config: Config, command: Command) -> Result<(), CliError> {
    match command {
        Command::Init { json } => cmd_init(&config, json).await,
        Command::Create { spec, json } => cmd_create(&config, &spec, json).await,
        Command::Advance { loop_id, json } => {
            cmd_advance(&config, parse_loop_id(&loop_id)?, json).await
        }
        Command::Run {
            loop_id,
            max_episodes,
            deadline,
            max_attempts,
            max_tokens,
            condition,
            json,
        } => {
            cmd_run(
                &config,
                parse_loop_id(&loop_id)?,
                max_episodes,
                deadline,
                max_attempts,
                max_tokens,
                condition,
                json,
            )
            .await
        }
        Command::Status { loop_id, json } => {
            cmd_status(&config, parse_loop_id(&loop_id)?, json).await
        }
        Command::Events {
            loop_id,
            after,
            limit,
            json,
        } => cmd_events(&config, parse_loop_id(&loop_id)?, after, limit, json).await,
        Command::Suspend {
            loop_id,
            reason,
            json,
        } => cmd_suspend(&config, parse_loop_id(&loop_id)?, reason, json).await,
        Command::Resume { loop_id, json } => {
            cmd_resume(&config, parse_loop_id(&loop_id)?, json).await
        }
        Command::Cancel {
            loop_id,
            reason,
            json,
        } => cmd_cancel(&config, parse_loop_id(&loop_id)?, reason, json).await,
        Command::InspectCheckpoint { loop_id, json } => {
            cmd_inspect_checkpoint(&config, parse_loop_id(&loop_id)?, json).await
        }
        Command::VerifyStore { json } => cmd_verify_store(&config, json).await,
    }
}

fn parse_loop_id(text: &str) -> Result<LoopId, CliError> {
    text.parse::<LoopId>()
        .map_err(|source| CliError::InvalidLoopId {
            value: text.to_owned(),
            source,
        })
}

// ---------------------------------------------------------------------------
// `init`
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct InitReport {
    database_path: String,
    resource_root: String,
    run_root: String,
}

async fn cmd_init(config: &Config, json: bool) -> Result<(), CliError> {
    let store = open_store(config, true).await?;
    std::fs::create_dir_all(&config.run_root)?;
    let _broker = FileResourceBroker::new(store.pool().clone(), &config.resource_root)?;

    let report = InitReport {
        database_path: config.database_path.display().to_string(),
        resource_root: config.resource_root.display().to_string(),
        run_root: config.run_root.display().to_string(),
    };
    if json {
        print_json(&report);
    } else {
        println!(
            "initialized loopward store\n  database:      {}\n  resource root: {}\n  run root:      {}",
            report.database_path, report.resource_root, report.run_root
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `create`
// ---------------------------------------------------------------------------

/// The CLI-friendly `--spec` shape `create` deserializes.
///
/// Rather than requiring callers to hand-assemble the full
/// [`CreateLoopRequest`] (which also carries `metadata` and
/// `protocol_version` — fields the CLI itself supplies, never the caller)
/// this narrows to exactly the domain-relevant fields, flattening
/// `kind`/`goal`/`domain_schema` from their wire newtypes to bare strings.
/// `success_criteria`, `pinned_constraints`, and `policies` are accepted
/// verbatim as their `loopward-api` wire shapes (they already have
/// straightforward JSON forms), rather than inventing a second, narrower
/// shape for them too.
#[derive(Debug, Clone, Deserialize)]
struct CreateSpec {
    kind: String,
    goal: String,
    #[serde(default)]
    success_criteria: Vec<SuccessCriterion>,
    #[serde(default)]
    pinned_constraints: Vec<Constraint>,
    domain_schema: String,
    #[serde(default)]
    initial_domain_state: serde_json::Value,
    #[serde(default)]
    policies: LoopPolicies,
}

fn build_create_request(spec: CreateSpec) -> CreateLoopRequest {
    CreateLoopRequest {
        metadata: new_command_metadata(None),
        protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
        kind: LoopKind::new(spec.kind),
        goal: Goal {
            statement: spec.goal,
        },
        success_criteria: spec.success_criteria,
        pinned_constraints: spec.pinned_constraints,
        domain_schema: SchemaRef::new(spec.domain_schema),
        initial_domain_state: spec.initial_domain_state,
        policies: spec.policies,
    }
}

async fn cmd_create(config: &Config, spec_json: &str, json: bool) -> Result<(), CliError> {
    let spec: CreateSpec = serde_json::from_str(spec_json).map_err(CliError::InvalidSpec)?;
    let request = build_create_request(spec);
    let service = build_service(config).await?;
    let handle = service.create(request).await?;
    emit(json, &handle, |handle| {
        format!(
            "created loop {} (revision {})",
            handle.loop_id, handle.aggregate_revision
        )
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// `advance` / `run` / `suspend` / `resume` / `cancel`
// ---------------------------------------------------------------------------

async fn cmd_advance(config: &Config, loop_id: LoopId, json: bool) -> Result<(), CliError> {
    let service = build_service(config).await?;
    let expected_revision = expected_revision(&service, loop_id).await?;
    let handle = service
        .advance(AdvanceLoopCommand {
            metadata: new_command_metadata(Some(expected_revision)),
            loop_id,
        })
        .await?;
    emit(json, &handle, describe_operation);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_run(
    config: &Config,
    loop_id: LoopId,
    max_episodes: Option<u32>,
    deadline: Option<String>,
    max_attempts: Option<u32>,
    max_tokens: Option<u64>,
    condition: RunConditionArg,
    json: bool,
) -> Result<(), CliError> {
    let deadline = deadline
        .map(|value| Timestamp::parse(&value, &Rfc3339))
        .transpose()
        .map_err(CliError::InvalidDeadline)?;
    let max_resource_cost =
        (max_attempts.is_some() || max_tokens.is_some()).then_some(ResourceCost {
            work_unit_attempts: max_attempts,
            estimated_tokens: max_tokens,
        });
    let limits = RunLimits {
        max_episodes,
        deadline,
        max_resource_cost,
    };

    let service = build_service(config).await?;
    let expected_revision = expected_revision(&service, loop_id).await?;
    let handle = service
        .run_until(RunUntilCommand {
            metadata: new_command_metadata(Some(expected_revision)),
            loop_id,
            condition: condition.into(),
            limits,
        })
        .await?;
    emit(json, &handle, describe_operation);
    Ok(())
}

async fn cmd_suspend(
    config: &Config,
    loop_id: LoopId,
    reason: Option<String>,
    json: bool,
) -> Result<(), CliError> {
    let service = build_service(config).await?;
    let expected_revision = expected_revision(&service, loop_id).await?;
    let handle = service
        .suspend(SuspendLoopCommand {
            metadata: new_command_metadata(Some(expected_revision)),
            loop_id,
            reason,
        })
        .await?;
    emit(json, &handle, describe_operation);
    Ok(())
}

async fn cmd_resume(config: &Config, loop_id: LoopId, json: bool) -> Result<(), CliError> {
    let service = build_service(config).await?;
    let expected_revision = expected_revision(&service, loop_id).await?;
    let handle = service
        .resume(ResumeLoopCommand {
            metadata: new_command_metadata(Some(expected_revision)),
            loop_id,
        })
        .await?;
    emit(json, &handle, describe_operation);
    Ok(())
}

async fn cmd_cancel(
    config: &Config,
    loop_id: LoopId,
    reason: Option<String>,
    json: bool,
) -> Result<(), CliError> {
    let service = build_service(config).await?;
    let expected_revision = expected_revision(&service, loop_id).await?;
    let handle = service
        .cancel(CancelLoopCommand {
            metadata: new_command_metadata(Some(expected_revision)),
            loop_id,
            reason,
        })
        .await?;
    emit(json, &handle, describe_operation);
    Ok(())
}

fn describe_operation(handle: &OperationHandle) -> String {
    format!(
        "loop {}: operation {} accepted at revision {}",
        handle.loop_id, handle.operation_id, handle.accepted_at_revision
    )
}

// ---------------------------------------------------------------------------
// `status` / `events`
// ---------------------------------------------------------------------------

async fn cmd_status(config: &Config, loop_id: LoopId, json: bool) -> Result<(), CliError> {
    let service = build_service(config).await?;
    let status = service.status(loop_id).await?;
    emit(json, &status, describe_status);
    Ok(())
}

fn describe_status(status: &LoopStatusView) -> String {
    format!(
        "loop {}: {:?} (phase {}, revision {})",
        status.loop_id, status.status, status.phase, status.aggregate_revision
    )
}

async fn cmd_events(
    config: &Config,
    loop_id: LoopId,
    after: Option<String>,
    limit: u32,
    json: bool,
) -> Result<(), CliError> {
    let service = build_service(config).await?;
    let page = service
        .events(EventQuery {
            loop_id,
            after: after.map(EventCursor::new),
            limit,
            durability: EventDurabilityFilter::Durable,
        })
        .await?;
    emit(json, &page, describe_events);
    Ok(())
}

fn describe_events(page: &EventPage) -> String {
    let mut lines: Vec<String> = page
        .events
        .iter()
        .map(|event| {
            format!(
                "[{}] {} (revision {})",
                event.journal_position, event.kind, event.aggregate_revision
            )
        })
        .collect();
    if let Some(cursor) = &page.next_cursor {
        lines.push(format!("next cursor: {cursor}"));
    }
    if lines.is_empty() {
        "no events".to_owned()
    } else {
        lines.join("\n")
    }
}

// ---------------------------------------------------------------------------
// `inspect-checkpoint` / `verify-store`
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct CheckpointView {
    loop_id: LoopId,
    status: String,
    checkpoint_id: CheckpointId,
    version: u64,
    aggregate_revision: u64,
    parent: Option<CheckpointId>,
    episode_id: Option<EpisodeId>,
    integrity_digest: Option<String>,
}

async fn cmd_inspect_checkpoint(
    config: &Config,
    loop_id: LoopId,
    json: bool,
) -> Result<(), CliError> {
    let store = open_store(config, true).await?;
    let stored = store.load(loop_id).await?;
    let checkpoint = &stored.state.checkpoint;
    let view = CheckpointView {
        loop_id,
        status: format!("{:?}", stored.state.execution.status),
        checkpoint_id: checkpoint.id,
        version: checkpoint.version,
        aggregate_revision: checkpoint.aggregate_revision,
        parent: checkpoint.parent,
        episode_id: checkpoint.episode_id,
        integrity_digest: checkpoint.integrity_digest.clone(),
    };
    emit(json, &view, |view| {
        format!(
            "loop {} checkpoint {} (version {}, revision {}, status {})",
            view.loop_id, view.checkpoint_id, view.version, view.aggregate_revision, view.status
        )
    });
    Ok(())
}

#[derive(Debug, Serialize)]
struct VerifyStoreReport {
    database_path: String,
    ok: bool,
}

async fn cmd_verify_store(config: &Config, json: bool) -> Result<(), CliError> {
    let _store = open_store(config, false).await?;
    let report = VerifyStoreReport {
        database_path: config.database_path.display().to_string(),
        ok: true,
    };
    emit(json, &report, |report| {
        format!(
            "store at {} is open and fully migrated",
            report.database_path
        )
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Composition: assembling `RuntimePorts` and `RuntimeLoopService`
// ---------------------------------------------------------------------------

/// Opens the configured `SQLite` database, applying every pending
/// migration.
///
/// `create_if_missing` is `false` for `verify-store`, which must fail
/// rather than silently create an empty database when asked to check one
/// that was never initialized.
async fn open_store(config: &Config, create_if_missing: bool) -> Result<SqliteLoopStore, CliError> {
    if create_if_missing
        && let Some(parent) = config.database_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let store_config =
        SqliteStoreConfig::new(&config.database_path).with_create_if_missing(create_if_missing);
    Ok(SqliteLoopStore::open(store_config).await?)
}

/// Builds a full [`RuntimeLoopService`] over the configured store, resource
/// broker, runner, telemetry sink, and this CLI's default verification plan
/// resolver.
async fn build_service(config: &Config) -> Result<RuntimeLoopService, CliError> {
    let store = open_store(config, true).await?;
    std::fs::create_dir_all(&config.run_root)?;
    let pool = store.pool().clone();
    let broker: Arc<dyn ResourceBroker> =
        Arc::new(FileResourceBroker::new(pool, &config.resource_root)?);

    let runner: Arc<dyn WorkUnitRunner> = match &config.runner_profile {
        Some(program) => Arc::new(CommandRunner::new(
            CommandRunnerConfig::new(program.clone(), config.run_root.clone()),
            Arc::clone(&broker),
        )),
        None => Arc::new(NullRunner),
    };

    let telemetry: Arc<dyn TelemetrySink> = match OtelTelemetrySink::from_env()? {
        Some(sink) => Arc::new(sink),
        None => Arc::new(NoopTelemetrySink),
    };

    let store: Arc<dyn LoopStore> = Arc::new(store);
    let ports = RuntimePorts {
        store,
        resources: broker,
        clock: Arc::new(SystemClock),
        ids: Arc::new(UuidIdGenerator),
        telemetry,
    };

    let plans: Arc<dyn VerificationPlanResolver> = Arc::new(DefaultVerificationPlanResolver::new());
    let controller = LoopController::new(ports, runner, plans).with_lease_ttl(config.lease_ttl);
    Ok(RuntimeLoopService::new(controller))
}

/// Builds fresh [`CommandMetadata`] for one command.
///
/// Each CLI invocation is a separate process with no durable memory of a
/// prior one, so `idempotency_key` is a fresh `UUIDv7` every call rather
/// than something a caller can supply to retry a specific earlier attempt.
/// This is a deliberate, documented scope boundary: `LoopService`'s
/// idempotency guarantee is real (`RuntimeLoopService` does hold an
/// in-process ledger keyed by this value), but this CLI does not expose a
/// way to construct a retryable request across two separate invocations —
/// it is a one-shot operator tool, not a retry-safe client library. A
/// deployment wanting that would add a `--idempotency-key` flag here.
fn new_command_metadata(expected_revision: Option<u64>) -> CommandMetadata {
    CommandMetadata {
        request_id: uuid::Uuid::now_v7().to_string(),
        idempotency_key: uuid::Uuid::now_v7().to_string(),
        expected_revision,
        principal: None,
        trace_context: TraceContext::default(),
    }
}

/// Reads a Loop's current aggregate revision to use as `expected_revision`
/// on the mutating command about to follow.
///
/// This CLI has no prior known revision of its own (each invocation is a
/// fresh process): rather than passing `None` and skipping optimistic
/// concurrency entirely, it reads current status immediately beforehand and
/// uses that. This still leaves a narrow race between the read and the
/// mutating call — a concurrent writer could commit in between — but that
/// is exactly the race `expected_revision` fencing exists to catch: a
/// concurrent commit in that window makes this command fail with
/// `LoopwardErrorCode::Conflict` rather than silently overwriting it.
async fn expected_revision(service: &RuntimeLoopService, loop_id: LoopId) -> Result<u64, CliError> {
    Ok(service.status(loop_id).await?.aggregate_revision)
}

#[derive(Serialize)]
struct JsonEnvelope<'a, T> {
    protocol_version: &'a str,
    data: T,
}

fn print_json<T: Serialize>(data: &T) {
    let envelope = JsonEnvelope {
        protocol_version: loopward_api::PROTOCOL_VERSION,
        data,
    };
    let text = serde_json::to_string_pretty(&envelope).expect("json envelope always serializes");
    println!("{text}");
}

/// Emits `data` as `--json`'s versioned document, or `describe(&data)` as a
/// concise human-readable line.
fn emit<T: Serialize>(json: bool, data: &T, describe: impl FnOnce(&T) -> String) {
    if json {
        print_json(data);
    } else {
        println!("{}", describe(data));
    }
}

// ---------------------------------------------------------------------------
// Gap 1: a real `Clock` and `IdGenerator`
// ---------------------------------------------------------------------------

/// A real, wall-clock [`Clock`].
///
/// No prior task builds a concrete `Clock` outside test fakes: every
/// existing implementation (`loopward-runtime`'s own test doubles) is a
/// fixed or scripted timestamp for deterministic tests. This is Task 12's
/// small, boring real one, reading the system clock exactly the way
/// `loopward-store-sqlite` and `RuntimeInstanceId::new` already do
/// elsewhere in this workspace (`Timestamp::now_utc()`).
#[derive(Debug, Clone, Copy, Default)]
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now_utc()
    }
}

/// A real, `UUIDv7`-backed [`IdGenerator`].
///
/// Every `loopward-api::ids` newtype already mints itself this way through
/// its own `::new()` constructor (`Uuid::now_v7()`); this just calls
/// through to each one, consistent with that existing convention rather
/// than inventing a second ID scheme.
#[derive(Debug, Clone, Copy, Default)]
struct UuidIdGenerator;

impl IdGenerator for UuidIdGenerator {
    fn new_loop_id(&self) -> LoopId {
        LoopId::new()
    }

    fn new_episode_id(&self) -> EpisodeId {
        EpisodeId::new()
    }

    fn new_session_id(&self) -> SessionId {
        SessionId::new()
    }

    fn new_work_unit_id(&self) -> WorkUnitId {
        WorkUnitId::new()
    }

    fn new_attempt_id(&self) -> AttemptId {
        AttemptId::new()
    }

    fn new_operation_id(&self) -> OperationId {
        OperationId::new()
    }

    fn new_event_id(&self) -> EventId {
        EventId::new()
    }

    fn new_checkpoint_id(&self) -> CheckpointId {
        CheckpointId::new()
    }
}

// ---------------------------------------------------------------------------
// Gap 2: a default `VerificationPlanResolver`
// ---------------------------------------------------------------------------

/// The Phase One CLI's default [`VerificationPlanResolver`].
///
/// No prior task builds a real verification plan registry: a Work Unit
/// only ever carries a `{reference, version}` pair
/// ([`VerificationPlanRef`]), and resolving that to gates, an aggregation
/// policy, and a remediation policy is explicitly `LoopController`'s own
/// documented seam for a deployment to plug a real registry into. This
/// resolver is the CLI's plug: it maps *every* reference to one fixed
/// default plan — a single required `Schema` gate configured to accept any
/// structurally valid result (no minimum output count, no required
/// pointers, no pinned protocol version) — rather than building a
/// general-purpose, configuration- or schema-driven registry nothing in
/// Phase One's scope calls for yet.
///
/// This is deliberately permissive, not deliberately thorough: it exists so
/// the CLI's `advance`/`run` can drive a real `LoopController` all the way
/// through verification, not to make a meaningful acceptance decision. A
/// deployment wanting real per-Work-Unit-kind verification plans replaces
/// this with its own `VerificationPlanResolver`.
#[derive(Debug, Clone)]
struct DefaultVerificationPlanResolver {
    template: VerificationPlanTemplate,
}

impl DefaultVerificationPlanResolver {
    fn new() -> Self {
        Self {
            template: VerificationPlanTemplate {
                id: VerificationPlanId::new("loopward.cli/default-plan"),
                version: 1,
                gates: vec![VerificationGateSpec {
                    id: GateId::new("default-schema-gate"),
                    kind: VerificationGateKind::Schema,
                    enforcement: GateEnforcement::Required,
                    criterion: None,
                    config: GateConfig::Schema(SchemaGateConfig::default()),
                }],
                aggregation: AggregationPolicy::default(),
                remediation_policy: RemediationPolicy::default(),
            },
        }
    }
}

impl VerificationPlanResolver for DefaultVerificationPlanResolver {
    fn resolve(&self, _reference: &VerificationPlanRef) -> Option<VerificationPlanTemplate> {
        Some(self.template.clone())
    }
}

// ---------------------------------------------------------------------------
// A runner stand-in for when no profile is configured
// ---------------------------------------------------------------------------

/// A [`WorkUnitRunner`] stand-in used when no `--runner-profile` is
/// configured.
///
/// [`LoopController::new`] always requires a runner to construct at all,
/// but Phase One's `LoopService::create` never assigns a Work Unit to a
/// newly created Loop (see `RuntimeLoopService::create`'s own "Known
/// limitation" doc comment) — no `LifecycleCommand` in this codebase does.
/// There is therefore no code path through this CLI today that actually
/// invokes a runner method. This stub exists purely so every subcommand can
/// still build one uniform `LoopController` without silently defaulting to
/// an arbitrary real executable (like `true`) that would misleadingly look
/// configured when it is not. Every method fails honestly with
/// [`RunnerErrorCode::Unavailable`] rather than pretending to execute
/// anything.
#[derive(Debug, Clone, Copy)]
struct NullRunner;

fn runner_unavailable() -> RunnerError {
    RunnerError {
        code: RunnerErrorCode::Unavailable,
        message:
            "no runner profile is configured; pass --runner-profile or set LOOPWARD_RUNNER_PROFILE"
                .to_owned(),
        retryable: false,
        trace_ref: None,
    }
}

#[async_trait]
impl WorkUnitRunner for NullRunner {
    async fn capabilities(&self) -> Result<RunnerCapabilities, RunnerError> {
        Err(runner_unavailable())
    }

    async fn start(&self, _request: WorkUnitRequest) -> Result<RunHandle, RunnerError> {
        Err(runner_unavailable())
    }

    async fn reconcile(&self, _handle: &RunHandle) -> Result<ReconcileOutcome, RunnerError> {
        Err(runner_unavailable())
    }

    async fn status(&self, _handle: &RunHandle) -> Result<RunStatus, RunnerError> {
        Err(runner_unavailable())
    }

    async fn events(
        &self,
        _handle: &RunHandle,
        _cursor: Option<EventCursor>,
    ) -> Result<RunnerEventPage, RunnerError> {
        Err(runner_unavailable())
    }

    async fn suspend(
        &self,
        _handle: &RunHandle,
        _reason: SuspensionReason,
    ) -> Result<(), RunnerError> {
        Err(runner_unavailable())
    }

    async fn cancel(
        &self,
        _handle: &RunHandle,
        _reason: CancellationReason,
    ) -> Result<(), RunnerError> {
        Err(runner_unavailable())
    }

    async fn collect(&self, _handle: &RunHandle) -> Result<WorkUnitResult, RunnerError> {
        Err(runner_unavailable())
    }
}
