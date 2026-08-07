//! Subprocess lifecycle for the generic command runner: spawning, bounded
//! output capture, status polling, and cooperative cancellation.
//!
//! Each started Attempt owns one supervisor task. The supervisor is the only
//! holder of the [`tokio::process::Child`], which keeps `collect` (which
//! waits for exit) and `cancel` (which must interrupt that wait) from ever
//! contending for the same lock. The supervisor publishes the run's outcome
//! through a [`watch`] channel and accepts termination requests through an
//! [`mpsc`] control channel.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use loopward_api::ids::AttemptId;
use loopward_api::runner::CancellationReason;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use crate::protocol::AttemptLayout;

/// The size of the transfer buffer used while draining a child's pipe.
///
/// Capture is streamed through this fixed buffer, so an unbounded writer can
/// never grow the runner's memory footprint: bytes beyond the configured
/// capture limit are counted and discarded rather than buffered.
const CAPTURE_CHUNK_BYTES: usize = 8 * 1024;

/// What was observed after a cancellation request, per
/// `docs/05-work-unit-and-runner-protocol.md` §11: a timeout while
/// cancelling does not prove that execution stopped, so "exited", "had to be
/// force-killed", and "still running" stay distinguishable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationObservation {
    /// The process exited on its own after cooperative termination, within
    /// the configured grace period.
    ExitedWithinGrace,
    /// The process did not exit within the grace period and was force-killed.
    ForceKilled,
    /// Neither cooperative termination nor a force kill was observed to end
    /// the process; its state is not proven.
    StillRunning,
}

impl TerminationObservation {
    /// A short, human-readable description suitable for a status detail.
    #[must_use]
    pub fn describe(self) -> &'static str {
        match self {
            Self::ExitedWithinGrace => "the process exited within the cancellation grace period",
            Self::ForceKilled => {
                "the process did not exit within the cancellation grace period and was force-killed"
            }
            Self::StillRunning => "termination was not observed: the process may still be running",
        }
    }
}

impl fmt::Display for TerminationObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.describe())
    }
}

/// What one captured stream produced versus what was retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CaptureReport {
    /// Bytes actually written to the log file.
    pub bytes_captured: u64,
    /// Bytes the process produced, including any discarded overflow.
    pub bytes_produced: u64,
}

impl CaptureReport {
    /// Whether the stream produced more bytes than were retained.
    #[must_use]
    pub fn truncated(self) -> bool {
        self.bytes_produced > self.bytes_captured
    }
}

/// A capture task's progress, published as it goes.
///
/// The counters are shared rather than returned from the task so the
/// supervisor can still report what was captured after abandoning a capture
/// task that is blocked on a pipe an orphaned grandchild is holding open.
type CaptureProgress = Arc<Mutex<CaptureReport>>;

fn read_progress(progress: &CaptureProgress) -> CaptureReport {
    *progress.lock().expect("capture progress lock poisoned")
}

/// The terminal facts about one finished run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RunOutcome {
    pub(crate) exit_code: Option<i32>,
    pub(crate) success: bool,
    pub(crate) stdout: CaptureReport,
    pub(crate) stderr: CaptureReport,
}

/// A termination request sent to a run's supervisor task.
#[derive(Debug, Clone, Copy)]
enum ControlMessage {
    /// Ask the process to stop cooperatively (`SIGTERM` on Unix).
    Terminate,
    /// Stop the process unconditionally (`SIGKILL`).
    Kill,
}

/// The mutable, non-terminal bookkeeping for one run.
#[derive(Debug, Default)]
pub(crate) struct RunBookkeeping {
    pub(crate) cancellation: Option<CancellationReason>,
    pub(crate) termination: Option<TerminationObservation>,
    pub(crate) collected: bool,
}

/// One started Attempt's in-memory registry entry.
#[derive(Debug)]
pub(crate) struct RunEntry {
    pub(crate) layout: AttemptLayout,
    control: mpsc::Sender<ControlMessage>,
    outcome: watch::Receiver<Option<RunOutcome>>,
    /// Set by the supervisor whenever it delivers an unconditional kill,
    /// including when it had to escalate because cooperative termination
    /// could not be delivered at all.
    escalated: Arc<AtomicBool>,
    pub(crate) bookkeeping: Mutex<RunBookkeeping>,
}

impl RunEntry {
    /// The run's terminal outcome, when it has already finished.
    pub(crate) fn finished(&self) -> Option<RunOutcome> {
        *self.outcome.borrow()
    }

    /// Classifies an observed exit, distinguishing a genuinely cooperative
    /// stop from one the supervisor had to force.
    ///
    /// Cooperative termination can fail to be delivered at all — `kill(1)`
    /// missing from `PATH`, or a non-Unix target where no cooperative signal
    /// exists here — in which case the supervisor escalates immediately. That
    /// escalation must not be reported as a process that "exited within the
    /// grace period": it never had the chance.
    fn observed_exit(&self) -> TerminationObservation {
        if self.escalated.load(Ordering::Acquire) {
            TerminationObservation::ForceKilled
        } else {
            TerminationObservation::ExitedWithinGrace
        }
    }

    /// Waits indefinitely for the run to finish.
    pub(crate) async fn wait_for_outcome(&self) -> Option<RunOutcome> {
        let mut receiver = self.outcome.clone();
        match receiver.wait_for(Option::is_some).await {
            Ok(value) => *value,
            Err(_) => None,
        }
    }

    /// Waits up to `grace` for the run to finish, reporting whether it did.
    async fn wait_for_exit(&self, grace: Duration) -> bool {
        let mut receiver = self.outcome.clone();
        matches!(
            tokio::time::timeout(grace, receiver.wait_for(Option::is_some)).await,
            Ok(Ok(_))
        )
    }

    /// Requests cooperative termination, escalating to a force kill if the
    /// grace period expires, and reports what was actually observed.
    ///
    /// A process that had already exited before the request is reported as
    /// [`TerminationObservation::ExitedWithinGrace`]: no escalation was
    /// needed and the terminal state is known.
    pub(crate) async fn terminate(&self, grace: Duration) -> TerminationObservation {
        if self.finished().is_some() {
            return self.observed_exit();
        }

        // A closed control channel means the supervisor already finished.
        if self.control.send(ControlMessage::Terminate).await.is_err() {
            return self.observed_exit();
        }

        if self.wait_for_exit(grace).await {
            return self.observed_exit();
        }

        if self.control.send(ControlMessage::Kill).await.is_err() {
            return TerminationObservation::ForceKilled;
        }

        if self.wait_for_exit(grace).await {
            TerminationObservation::ForceKilled
        } else {
            TerminationObservation::StillRunning
        }
    }
}

/// The in-memory registry of started Attempts, keyed by [`AttemptId`].
///
/// `WorkUnitRunner`'s methods take `&self`, so the registry is the runner's
/// shared interior state.
#[derive(Debug, Default)]
pub(crate) struct RunRegistry {
    entries: Mutex<HashMap<AttemptId, Arc<RunEntry>>>,
}

impl RunRegistry {
    pub(crate) fn insert(&self, attempt_id: AttemptId, entry: Arc<RunEntry>) {
        self.entries
            .lock()
            .expect("run registry lock poisoned")
            .insert(attempt_id, entry);
    }

    pub(crate) fn get(&self, attempt_id: AttemptId) -> Option<Arc<RunEntry>> {
        self.entries
            .lock()
            .expect("run registry lock poisoned")
            .get(&attempt_id)
            .map(Arc::clone)
    }
}

/// How a run should be spawned.
pub(crate) struct SpawnRequest<'a> {
    pub(crate) program: &'a Path,
    pub(crate) args: &'a [String],
    pub(crate) layout: AttemptLayout,
    pub(crate) environment: Vec<(String, String)>,
    pub(crate) max_capture_bytes: u64,
    pub(crate) capture_drain_grace: Duration,
    pub(crate) cooperative_stop_program: PathBuf,
}

/// A spawned run: its registry entry plus the operating-system process
/// identifier, used as the Attempt's external execution reference.
pub(crate) struct SpawnedRun {
    pub(crate) entry: Arc<RunEntry>,
    pub(crate) pid: Option<u32>,
}

/// Spawns the command and starts its supervisor and capture tasks.
///
/// The command runs with the attempt directory as its working directory, its
/// standard input closed, and both output pipes captured. It inherits the
/// host process's environment plus the documented Loopward variables; those
/// variables carry identifiers, file paths, and trace context only, never
/// Loop database credentials.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] when the process cannot be
/// spawned or its pipes cannot be taken.
pub(crate) fn spawn(request: SpawnRequest<'_>) -> std::io::Result<SpawnedRun> {
    let mut command = Command::new(request.program);
    command
        .args(request.args)
        .current_dir(request.layout.root())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    for (key, value) in request.environment {
        command.env(key, value);
    }

    let mut child = command.spawn()?;
    let pid = child.id();

    let stdout = child.stdout.take().ok_or_else(|| {
        std::io::Error::other("the spawned command exposed no standard-output pipe")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        std::io::Error::other("the spawned command exposed no standard-error pipe")
    })?;

    let stdout_progress: CaptureProgress = Arc::default();
    let stderr_progress: CaptureProgress = Arc::default();

    let stdout_task = tokio::spawn(capture_stream(
        stdout,
        request.layout.stdout_log_path(),
        request.max_capture_bytes,
        Arc::clone(&stdout_progress),
    ));
    let stderr_task = tokio::spawn(capture_stream(
        stderr,
        request.layout.stderr_log_path(),
        request.max_capture_bytes,
        Arc::clone(&stderr_progress),
    ));

    let (control_tx, control_rx) = mpsc::channel(4);
    let (outcome_tx, outcome_rx) = watch::channel(None);
    let escalated = Arc::new(AtomicBool::new(false));

    tokio::spawn(supervise(Supervision {
        child,
        pid,
        control: control_rx,
        captures: Captures {
            stdout: stdout_task,
            stderr: stderr_task,
            stdout_progress,
            stderr_progress,
            drain_grace: request.capture_drain_grace,
        },
        outcome: outcome_tx,
        escalated: Arc::clone(&escalated),
        cooperative_stop_program: request.cooperative_stop_program,
    }));

    Ok(SpawnedRun {
        entry: Arc::new(RunEntry {
            layout: request.layout,
            control: control_tx,
            outcome: outcome_rx,
            escalated,
            bookkeeping: Mutex::new(RunBookkeeping::default()),
        }),
        pid,
    })
}

/// The two capture tasks and how long to wait for them once the child is gone.
struct Captures {
    stdout: tokio::task::JoinHandle<()>,
    stderr: tokio::task::JoinHandle<()>,
    stdout_progress: CaptureProgress,
    stderr_progress: CaptureProgress,
    drain_grace: Duration,
}

impl Captures {
    /// Waits for both capture tasks to finish draining, abandoning them if
    /// they do not finish within the drain grace period.
    ///
    /// A pipe does not reach end-of-file until *every* holder of its write
    /// end closes it, and a command may leave a backgrounded grandchild
    /// holding the inherited descriptors long after the direct child is
    /// reaped. Waiting unconditionally would stall the run's outcome — and
    /// therefore `collect` and `cancel` — for that orphan's entire lifetime.
    /// Abandoning the tasks drops the read ends and publishes whatever was
    /// captured up to that point.
    async fn drain(self) -> (CaptureReport, CaptureReport) {
        let Self {
            stdout,
            stderr,
            stdout_progress,
            stderr_progress,
            drain_grace,
        } = self;

        let stdout_abort = stdout.abort_handle();
        let stderr_abort = stderr.abort_handle();

        let joined = tokio::time::timeout(drain_grace, async move {
            let _ = stdout.await;
            let _ = stderr.await;
        })
        .await;

        if joined.is_err() {
            stdout_abort.abort();
            stderr_abort.abort();
        }

        (
            read_progress(&stdout_progress),
            read_progress(&stderr_progress),
        )
    }
}

/// Everything one supervisor task owns.
struct Supervision {
    child: Child,
    pid: Option<u32>,
    control: mpsc::Receiver<ControlMessage>,
    captures: Captures,
    outcome: watch::Sender<Option<RunOutcome>>,
    escalated: Arc<AtomicBool>,
    cooperative_stop_program: PathBuf,
}

/// Owns the child, services termination requests, and publishes the outcome.
async fn supervise(supervision: Supervision) {
    let Supervision {
        mut child,
        pid,
        mut control,
        captures,
        outcome,
        escalated,
        cooperative_stop_program,
    } = supervision;

    let mut control_open = true;
    let status = loop {
        tokio::select! {
            // `Child::wait` is cancel-safe, so losing this branch of the
            // select does not lose the child's exit status.
            waited = child.wait() => break waited.ok(),
            message = control.recv(), if control_open => match message {
                Some(ControlMessage::Terminate) => {
                    // Record the escalation *before* killing, so a caller
                    // that observes the exit also observes why it happened.
                    if !request_cooperative_stop(&cooperative_stop_program, pid).await {
                        escalated.store(true, Ordering::Release);
                        let _ = child.start_kill();
                    }
                }
                Some(ControlMessage::Kill) => {
                    escalated.store(true, Ordering::Release);
                    let _ = child.start_kill();
                }
                None => control_open = false,
            },
        }
    };

    let (stdout, stderr) = captures.drain().await;

    let _ = outcome.send(Some(RunOutcome {
        exit_code: status.and_then(|status| status.code()),
        success: status.is_some_and(|status| status.success()),
        stdout,
        stderr,
    }));
}

/// Asks the process to stop cooperatively, returning whether the request was
/// delivered.
///
/// `tokio::process::Child` only exposes an unconditional kill (`SIGKILL`),
/// and sending `SIGTERM` through `libc` would require `unsafe`, which this
/// workspace forbids. On Unix the signal is therefore delivered by invoking
/// `program` (`kill(1)` by default). Returning `false` — because the program
/// is missing from `PATH`, cannot be executed, or reports failure — means no
/// cooperative signal reached the process, and the caller must both escalate
/// and record that it did.
#[cfg(unix)]
async fn request_cooperative_stop(program: &Path, pid: Option<u32>) -> bool {
    let Some(pid) = pid else { return false };
    Command::new(program)
        .arg("-TERM")
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
}

/// No cooperative signal mechanism exists here, so the caller always
/// escalates — and, because this reports `false`, always records that it did.
#[cfg(not(unix))]
async fn request_cooperative_stop(_program: &Path, _pid: Option<u32>) -> bool {
    false
}

/// Streams one pipe into a log file, retaining at most `limit` bytes.
///
/// Reading always continues to end-of-file so the child never blocks on a
/// full pipe, but bytes past `limit` are counted and dropped rather than
/// written, bounding both the log file and this task's memory use. Progress
/// is published into `progress` after every chunk so the counts survive this
/// task being abandoned mid-stream.
async fn capture_stream<R>(mut reader: R, path: PathBuf, limit: u64, progress: CaptureProgress)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let Ok(mut file) = tokio::fs::File::create(&path).await else {
        return;
    };

    let mut report = CaptureReport::default();
    let mut buffer = vec![0_u8; CAPTURE_CHUNK_BYTES];
    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        report.bytes_produced += read as u64;

        let remaining = limit.saturating_sub(report.bytes_captured);
        if remaining > 0 {
            let take = usize::try_from(remaining).unwrap_or(usize::MAX).min(read);
            if file.write_all(&buffer[..take]).await.is_err() {
                break;
            }
            // Flush per chunk: the log must be readable while the command is
            // still running, and this task may be abandoned before it ever
            // reaches the flush below.
            if file.flush().await.is_err() {
                break;
            }
            report.bytes_captured += take as u64;
        }

        *progress.lock().expect("capture progress lock poisoned") = report;
    }

    let _ = file.flush().await;
    *progress.lock().expect("capture progress lock poisoned") = report;
}
