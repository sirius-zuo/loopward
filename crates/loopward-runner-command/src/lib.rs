#![forbid(unsafe_code)]

//! The generic command runner: Loopward's Phase One reference
//! [`WorkUnitRunner`], which executes an arbitrary command as one Work Unit
//! Attempt (`docs/05-work-unit-and-runner-protocol.md` §13).
//!
//! This crate is an *adapter*. It depends on `loopward-api`'s port
//! definitions and on the [`ResourceBroker`] port from `loopward-runtime`,
//! never the reverse, and it is the only place a specific execution
//! mechanism — spawning a local process — is implemented. No runner-specific
//! vendor behavior belongs in `loopward-core`.
//!
//! Two boundaries are load-bearing:
//!
//! * **The command receives no Loop database credentials.** It gets a
//!   materialized request file, a materialized Boot Package manifest, an
//!   input directory, a resource output directory, an expected result path,
//!   and the documented identifier and trace environment variables. This
//!   crate has no dependency on any store or database type.
//! * **A collected result is a proposal, never an accepted outcome.**
//!   `collect` performs structural validation only: it never commits a
//!   Checkpoint, activates a decision, satisfies an acceptance criterion, or
//!   declares a Loop complete.

pub mod process;
pub mod protocol;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loopward_api::context::{ContextCapabilities, ContextControlLevel};
use loopward_api::event::EventCursor;
use loopward_api::ids::AttemptId;
use loopward_api::resource::{OpaqueRef, ResourceRef};
use loopward_api::runner::{
    CancellationReason, ReconcileOutcome, RunHandle, RunState, RunStatus, RunnerCapabilities,
    RunnerError, RunnerErrorCode, RunnerEventPage, SuspensionReason, WorkUnitRunner,
};
use loopward_api::work_unit::{WorkUnitRequest, WorkUnitResult};
use loopward_runtime::{ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle};

pub use crate::process::TerminationObservation;
use crate::process::{CaptureReport, RunEntry, RunRegistry, SpawnRequest};
use crate::protocol::AttemptLayout;

/// The default maximum size of a command's `result.json`, in bytes (4 MiB).
///
/// A Work Unit result carries references, not payloads: large content
/// belongs behind a [`ResourceRef`]. Four mebibytes is generous for a
/// reference-shaped document while still bounding what a misbehaving command
/// can force the runner to parse.
pub const DEFAULT_MAX_RESULT_BYTES: u64 = 4 * 1024 * 1024;

/// The default maximum bytes retained per captured stream (1 MiB).
///
/// Applies independently to standard output and standard error. Both streams
/// are always drained to end-of-file so the command never blocks on a full
/// pipe; bytes past this limit are counted and discarded, and the resulting
/// resource records the truncation.
pub const DEFAULT_MAX_CAPTURE_BYTES: u64 = 1024 * 1024;

/// The default cancellation grace period (5 seconds).
///
/// Applied twice: once waiting for cooperative termination, then again
/// waiting for a force kill to take effect.
pub const DEFAULT_CANCELLATION_GRACE: Duration = Duration::from_secs(5);

/// The default capture drain grace period (2 seconds).
///
/// A pipe reaches end-of-file only when *every* holder of its write end
/// closes it, so a command that backgrounds a longer-lived grandchild leaves
/// the inherited descriptors open after the command itself is reaped.
/// Draining is therefore bounded: once the command exits, capture is given
/// this long to finish before it is abandoned and the run's outcome is
/// published with whatever was captured. Ordinary commands close their pipes
/// as they exit and finish this instantly.
pub const DEFAULT_CAPTURE_DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The default program used to deliver cooperative termination (`kill`).
///
/// Resolved through `PATH`. See
/// [`CommandRunnerConfig::cooperative_stop_program`].
pub const DEFAULT_COOPERATIVE_STOP_PROGRAM: &str = "kill";

/// How a [`CommandRunner`] invokes and bounds its command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRunnerConfig {
    /// The program to execute for every Attempt.
    pub program: PathBuf,
    /// Fixed arguments passed to the program before it runs.
    pub args: Vec<String>,
    /// The directory beneath which one directory per Attempt is created.
    pub run_root: PathBuf,
    /// The maximum accepted size of the command's result file, in bytes.
    pub max_result_bytes: u64,
    /// The maximum bytes retained per captured stream.
    pub max_capture_bytes: u64,
    /// How long cancellation waits for cooperative termination before
    /// escalating, and then for the escalation to take effect.
    pub cancellation_grace: Duration,
    /// How long capture may keep draining after the command itself has
    /// exited, before it is abandoned so the run's outcome can be published.
    pub capture_drain_grace: Duration,
    /// The program invoked to deliver cooperative termination, given
    /// `-TERM <pid>`.
    ///
    /// `tokio::process::Child` exposes only an unconditional kill, and
    /// signalling through `libc` would require `unsafe`, which this workspace
    /// forbids — so a `SIGTERM` is delivered by running this program. If it
    /// cannot be invoked (for example on a minimal image with no `kill(1)` on
    /// `PATH`), cancellation escalates to an unconditional kill and reports
    /// [`TerminationObservation::ForceKilled`] rather than claiming a
    /// cooperative exit that never happened.
    pub cooperative_stop_program: PathBuf,
    /// The request/result protocol versions this runner accepts. Only the
    /// namespace and major component participate in compatibility.
    pub accepted_protocol_versions: Vec<String>,
}

impl CommandRunnerConfig {
    /// Builds a configuration with Loopward's documented defaults.
    #[must_use]
    pub fn new(program: impl Into<PathBuf>, run_root: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            run_root: run_root.into(),
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
            cancellation_grace: DEFAULT_CANCELLATION_GRACE,
            capture_drain_grace: DEFAULT_CAPTURE_DRAIN_GRACE,
            cooperative_stop_program: PathBuf::from(DEFAULT_COOPERATIVE_STOP_PROGRAM),
            accepted_protocol_versions: vec![loopward_api::PROTOCOL_VERSION.to_owned()],
        }
    }

    /// Sets the fixed arguments passed to the program.
    #[must_use]
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the maximum accepted result-file size, in bytes.
    #[must_use]
    pub fn with_max_result_bytes(mut self, max_result_bytes: u64) -> Self {
        self.max_result_bytes = max_result_bytes;
        self
    }

    /// Sets the maximum bytes retained per captured stream.
    #[must_use]
    pub fn with_max_capture_bytes(mut self, max_capture_bytes: u64) -> Self {
        self.max_capture_bytes = max_capture_bytes;
        self
    }

    /// Sets the cancellation grace period.
    #[must_use]
    pub fn with_cancellation_grace(mut self, cancellation_grace: Duration) -> Self {
        self.cancellation_grace = cancellation_grace;
        self
    }

    /// Sets how long capture may keep draining after the command exits.
    #[must_use]
    pub fn with_capture_drain_grace(mut self, capture_drain_grace: Duration) -> Self {
        self.capture_drain_grace = capture_drain_grace;
        self
    }

    /// Sets the program used to deliver cooperative termination.
    #[must_use]
    pub fn with_cooperative_stop_program(mut self, program: impl Into<PathBuf>) -> Self {
        self.cooperative_stop_program = program.into();
        self
    }

    /// Sets the accepted request/result protocol versions.
    #[must_use]
    pub fn with_accepted_protocol_versions<I, S>(mut self, versions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.accepted_protocol_versions = versions.into_iter().map(Into::into).collect();
        self
    }
}

/// A [`WorkUnitRunner`] that executes one local command per Work Unit
/// Attempt.
#[derive(Clone)]
pub struct CommandRunner {
    config: Arc<CommandRunnerConfig>,
    broker: Arc<dyn ResourceBroker>,
    runs: Arc<RunRegistry>,
}

impl std::fmt::Debug for CommandRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandRunner")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CommandRunner {
    /// Builds a runner over `config`, finalizing captured logs through
    /// `broker`.
    #[must_use]
    pub fn new(config: CommandRunnerConfig, broker: Arc<dyn ResourceBroker>) -> Self {
        Self {
            config: Arc::new(config),
            broker,
            runs: Arc::new(RunRegistry::default()),
        }
    }

    /// This runner's configuration.
    #[must_use]
    pub fn config(&self) -> &CommandRunnerConfig {
        &self.config
    }

    /// The isolated directory this runner uses for `attempt_id`.
    #[must_use]
    pub fn attempt_dir(&self, attempt_id: AttemptId) -> PathBuf {
        AttemptLayout::new(&self.config.run_root, attempt_id)
            .root()
            .to_path_buf()
    }

    /// What was observed after this Attempt's cancellation, or `None` when
    /// cancellation was never requested or the Attempt is unknown.
    ///
    /// # Panics
    ///
    /// Panics if another thread panicked while holding this run's
    /// bookkeeping lock.
    #[must_use]
    pub fn termination_observation(&self, handle: &RunHandle) -> Option<TerminationObservation> {
        let entry = self.runs.get(handle.attempt_id)?;
        let bookkeeping = entry
            .bookkeeping
            .lock()
            .expect("run bookkeeping lock poisoned");
        bookkeeping.termination
    }

    fn entry(&self, handle: &RunHandle) -> Result<Arc<RunEntry>, RunnerError> {
        self.runs.get(handle.attempt_id).ok_or_else(|| {
            runner_error(
                RunnerErrorCode::AmbiguousExternalState,
                format!(
                    "this runner holds no execution for attempt `{}`; its state cannot be determined",
                    handle.attempt_id
                ),
                false,
            )
        })
    }

    /// Builds the documented environment for one request.
    ///
    /// Identifier and path variables are always exported; the W3C trace
    /// variables are exported only when the request actually carries them,
    /// so a command can distinguish "no trace context" from "empty trace
    /// context".
    fn environment(request: &WorkUnitRequest, layout: &AttemptLayout) -> Vec<(String, String)> {
        let mut environment = vec![
            (
                protocol::ENV_LOOP_ID.to_owned(),
                request.loop_id.to_string(),
            ),
            (
                protocol::ENV_EPISODE_ID.to_owned(),
                request.episode_id.to_string(),
            ),
            (
                protocol::ENV_SESSION_ID.to_owned(),
                request.session_id.to_string(),
            ),
            (
                protocol::ENV_SESSION_GENERATION.to_owned(),
                request.session_generation.to_string(),
            ),
            (
                protocol::ENV_WORK_UNIT_ID.to_owned(),
                request.work_unit.id.to_string(),
            ),
            (
                protocol::ENV_ATTEMPT_ID.to_owned(),
                request.attempt_id.to_string(),
            ),
            (
                protocol::ENV_REQUEST_PATH.to_owned(),
                layout.request_path().display().to_string(),
            ),
            (
                protocol::ENV_RESULT_PATH.to_owned(),
                layout.result_path().display().to_string(),
            ),
            (
                protocol::ENV_RESOURCE_DIR.to_owned(),
                layout.outputs_dir().display().to_string(),
            ),
        ];

        if let Some(traceparent) = &request.trace_context.traceparent {
            environment.push((protocol::ENV_TRACEPARENT.to_owned(), traceparent.clone()));
        }
        if let Some(tracestate) = &request.trace_context.tracestate {
            environment.push((protocol::ENV_TRACESTATE.to_owned(), tracestate.clone()));
        }

        environment
    }

    /// Streams one captured log into the [`ResourceBroker`] and finalizes it.
    ///
    /// The bytes are read back from the (already capped) log file in fixed
    /// chunks, so nothing here grows with what the command produced.
    async fn finalize_log(
        &self,
        path: &Path,
        stream: &str,
        report: CaptureReport,
    ) -> Result<ResourceRef, RunnerError> {
        let summary = if report.truncated() {
            format!(
                "{stream} (truncated: retained {} of {} bytes)",
                report.bytes_captured, report.bytes_produced
            )
        } else {
            format!("{stream} ({} bytes)", report.bytes_captured)
        };

        let handle = self
            .broker
            .begin(ResourceBeginHint {
                media_type: Some("text/plain; charset=utf-8".to_owned()),
                summary: Some(summary),
            })
            .await
            .map_err(|err| resource_error(stream, &err))?;

        if let Err(err) = self.stream_file_into(path, &handle).await {
            let _ = self.broker.abort(handle).await;
            return Err(err);
        }

        self.broker
            .finalize(handle)
            .await
            .map_err(|err| resource_error(stream, &err))
    }

    async fn stream_file_into(
        &self,
        path: &Path,
        handle: &ResourceHandle,
    ) -> Result<(), RunnerError> {
        use tokio::io::AsyncReadExt;

        // A missing log means the command produced nothing on this stream;
        // an empty resource is the truthful representation.
        let Ok(mut file) = tokio::fs::File::open(path).await else {
            return Ok(());
        };

        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).await.map_err(|err| {
                runner_error(
                    RunnerErrorCode::ResourceError,
                    format!("could not read `{}`: {err}", path.display()),
                    true,
                )
            })?;
            if read == 0 {
                break;
            }
            self.broker
                .write(handle, &buffer[..read])
                .await
                .map_err(|err| resource_error("captured log", &err))?;
        }
        Ok(())
    }
}

#[async_trait]
impl WorkUnitRunner for CommandRunner {
    async fn capabilities(&self) -> Result<RunnerCapabilities, RunnerError> {
        Ok(RunnerCapabilities {
            protocol_versions: self.config.accepted_protocol_versions.clone(),
            resumable: false,
            supports_reconcile: true,
            supports_events: false,
            supports_suspend: false,
            supports_cancel: true,
            reports_token_usage: false,
            reports_internal_trace_context: false,
            context: ContextCapabilities {
                control_level: ContextControlLevel::ManagedBoot,
                reports_usage: false,
                reports_compaction: false,
                supports_resource_fetch: false,
                supports_structured_handoff: false,
                supports_resume: false,
            },
        })
    }

    async fn start(&self, request: WorkUnitRequest) -> Result<RunHandle, RunnerError> {
        // Per `docs/05` §14 an incompatible major version is rejected
        // *before execution*: nothing is materialized and no process runs.
        protocol::ensure_compatible_version(
            "the request",
            &request.protocol_version,
            &self.config.accepted_protocol_versions,
        )?;

        let layout = AttemptLayout::new(&self.config.run_root, request.attempt_id);
        layout.materialize(&request).await?;

        let environment = Self::environment(&request, &layout);
        let spawned = process::spawn(SpawnRequest {
            program: &self.config.program,
            args: &self.config.args,
            layout,
            environment,
            max_capture_bytes: self.config.max_capture_bytes,
            capture_drain_grace: self.config.capture_drain_grace,
            cooperative_stop_program: self.config.cooperative_stop_program.clone(),
        })
        .map_err(|err| {
            runner_error(
                RunnerErrorCode::ExecutionFailed,
                format!("could not spawn `{}`: {err}", self.config.program.display()),
                true,
            )
        })?;

        self.runs.insert(request.attempt_id, spawned.entry);

        Ok(RunHandle {
            attempt_id: request.attempt_id,
            external_execution_ref: spawned
                .pid
                .map(|pid| OpaqueRef::new(format!("process:{pid}"))),
        })
    }

    async fn reconcile(&self, handle: &RunHandle) -> Result<ReconcileOutcome, RunnerError> {
        let Some(entry) = self.runs.get(handle.attempt_id) else {
            // A local process registry does not survive a restart. An
            // attempt directory that exists without a live entry means the
            // effect may have happened: report `Unknown` rather than
            // asserting failure (`docs/05` §10).
            let attempt_dir = self.attempt_dir(handle.attempt_id);
            return Ok(if attempt_dir.exists() {
                ReconcileOutcome::Unknown
            } else {
                ReconcileOutcome::NotStarted
            });
        };

        let outcome = entry.finished();
        let bookkeeping = entry
            .bookkeeping
            .lock()
            .expect("run bookkeeping lock poisoned");

        Ok(match (outcome, bookkeeping.cancellation.is_some()) {
            (None, _) => ReconcileOutcome::Running,
            (Some(_), true) => ReconcileOutcome::Cancelled,
            (Some(outcome), false) if outcome.success => ReconcileOutcome::Completed,
            (Some(_), false) => ReconcileOutcome::Failed,
        })
    }

    async fn status(&self, handle: &RunHandle) -> Result<RunStatus, RunnerError> {
        let entry = self.entry(handle)?;
        let outcome = entry.finished();
        let bookkeeping = entry
            .bookkeeping
            .lock()
            .expect("run bookkeeping lock poisoned");

        let (state, detail) = match (outcome, bookkeeping.cancellation) {
            (None, None) => (RunState::Running, None),
            // A timeout while cancelling does not prove that execution
            // stopped (`docs/05` §11).
            (None, Some(reason)) => (
                RunState::Ambiguous,
                Some(format!(
                    "cancellation ({reason:?}) requested but termination was not observed"
                )),
            ),
            (Some(_), Some(reason)) => (
                RunState::Cancelled,
                Some(format!(
                    "cancelled ({reason:?}): {}",
                    bookkeeping.termination.map_or(
                        "termination was not classified",
                        TerminationObservation::describe
                    )
                )),
            ),
            (Some(outcome), None) => {
                let detail = Some(outcome.exit_code.map_or_else(
                    || "the command was terminated by a signal".to_owned(),
                    |code| format!("the command exited with status {code}"),
                ));
                if bookkeeping.collected {
                    (RunState::Completed, detail)
                } else {
                    (RunState::AwaitingCollection, detail)
                }
            }
        };

        Ok(RunStatus {
            state,
            detail,
            observed_at: Some(time::OffsetDateTime::now_utc()),
        })
    }

    async fn events(
        &self,
        _handle: &RunHandle,
        _cursor: Option<EventCursor>,
    ) -> Result<RunnerEventPage, RunnerError> {
        Err(runner_error(
            RunnerErrorCode::UnsupportedCapability,
            "the command runner does not report ephemeral progress events",
            false,
        ))
    }

    async fn suspend(
        &self,
        _handle: &RunHandle,
        _reason: SuspensionReason,
    ) -> Result<(), RunnerError> {
        Err(runner_error(
            RunnerErrorCode::UnsupportedCapability,
            "the command runner cannot suspend a running process",
            false,
        ))
    }

    async fn cancel(
        &self,
        handle: &RunHandle,
        reason: CancellationReason,
    ) -> Result<(), RunnerError> {
        let entry = self.entry(handle)?;
        {
            let mut bookkeeping = entry
                .bookkeeping
                .lock()
                .expect("run bookkeeping lock poisoned");
            bookkeeping.cancellation = Some(reason);
        }

        let observation = entry.terminate(self.config.cancellation_grace).await;

        let mut bookkeeping = entry
            .bookkeeping
            .lock()
            .expect("run bookkeeping lock poisoned");
        bookkeeping.termination = Some(observation);
        Ok(())
    }

    async fn collect(&self, handle: &RunHandle) -> Result<WorkUnitResult, RunnerError> {
        let entry = self.entry(handle)?;
        let outcome = entry.wait_for_outcome().await.ok_or_else(|| {
            runner_error(
                RunnerErrorCode::AmbiguousExternalState,
                format!(
                    "the supervisor for attempt `{}` ended without publishing an outcome",
                    handle.attempt_id
                ),
                false,
            )
        })?;

        let cancelled = entry
            .bookkeeping
            .lock()
            .expect("run bookkeeping lock poisoned")
            .cancellation
            .is_some();
        if cancelled {
            return Err(runner_error(
                RunnerErrorCode::Cancelled,
                format!(
                    "attempt `{}` was cancelled; no result may be collected",
                    handle.attempt_id
                ),
                false,
            ));
        }

        let mut result = protocol::read_result(
            &entry.layout.result_path(),
            handle.attempt_id,
            self.config.max_result_bytes,
            &self.config.accepted_protocol_versions,
        )
        .await?;

        // The captured streams are runner-produced output and must enter the
        // Loop as finalized, digest-bound resources rather than as inline
        // bytes (`docs/05` §8). They stay part of a *proposed* result.
        for (path, stream, report) in [
            (
                entry.layout.stdout_log_path(),
                protocol::STDOUT_LOG_FILE_NAME,
                outcome.stdout,
            ),
            (
                entry.layout.stderr_log_path(),
                protocol::STDERR_LOG_FILE_NAME,
                outcome.stderr,
            ),
        ] {
            result
                .outputs
                .push(self.finalize_log(&path, stream, report).await?);
        }

        entry
            .bookkeeping
            .lock()
            .expect("run bookkeeping lock poisoned")
            .collected = true;

        Ok(result)
    }
}

/// Builds a sanitized [`RunnerError`].
pub(crate) fn runner_error(
    code: RunnerErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> RunnerError {
    RunnerError {
        code,
        message: message.into(),
        retryable,
        trace_ref: None,
    }
}

fn resource_error(stream: &str, err: &ResourceError) -> RunnerError {
    runner_error(
        RunnerErrorCode::ResourceError,
        format!("could not finalize the captured `{stream}` resource: {err}"),
        true,
    )
}
