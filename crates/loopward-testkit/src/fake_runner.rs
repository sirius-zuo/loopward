//! [`FakeRunner`]: a scripted, deterministic [`WorkUnitRunner`].
//!
//! Ported from `loopward-runtime/tests/advance_episode.rs`'s `FakeRunner`
//! and `recovery_matrix.rs`'s `ConfigurableRunner` — the two near-identical
//! fakes those test files independently defined for exactly this purpose —
//! generalized so a whole conformance scenario can script every Attempt's
//! outcome up front rather than swapping runner instances between calls.
//!
//! Every call is scripted by [`AttemptScript`], looked up first by the
//! specific [`AttemptId`] a script was pinned to (the precise tool
//! [`crate::failure::FailureInjector`]-seeded scenarios need, since those
//! Attempts are never actually started through this runner), then by the
//! Attempt's 1-based ordinal — the order [`WorkUnitRunner::start`] was
//! called in this runner's own lifetime, matching the brief's "accepts
//! scripted outcomes by Attempt ordinal". An Attempt with no script at all
//! gets the permissive default: starts, runs, and collects a trivially
//! `Produced` empty result.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::context::{ContextCapabilities, ContextControlLevel};
use loopward_api::event::EventCursor;
use loopward_api::ids::AttemptId;
use loopward_api::resource::OpaqueRef;
use loopward_api::runner::{
    CancellationReason, ReconcileOutcome, RunHandle, RunState, RunStatus, RunnerCapabilities,
    RunnerError, RunnerErrorCode, RunnerEventPage, SuspensionReason, WorkUnitRunner,
};
use loopward_api::work_unit::{UsageReport, WorkUnitOutcome, WorkUnitRequest, WorkUnitResult};

/// One Attempt's scripted behavior across every [`WorkUnitRunner`] method a
/// [`FakeRunner`] may be asked to perform for it.
#[derive(Clone, Default)]
pub struct AttemptScript {
    /// [`WorkUnitRunner::start`] fails with a generic execution error.
    pub fail_start: bool,
    /// [`WorkUnitRunner::collect`] fails with a generic execution error.
    pub fail_collect: bool,
    /// The external execution reference [`WorkUnitRunner::start`] returns.
    /// Synthesized from the Attempt id when unset.
    pub external_ref: Option<OpaqueRef>,
    /// What [`WorkUnitRunner::reconcile`] answers. `None` means "the runner
    /// does not know" ([`ReconcileOutcome::Unknown`]) — the conservative
    /// default a real runner would give when asked about an Attempt it has
    /// no special knowledge of.
    pub reconcile: Option<ReconcileOutcome>,
    /// The result [`WorkUnitRunner::collect`] returns. A trivially
    /// `Produced` empty result when unset.
    pub result: Option<WorkUnitResult>,
    /// A side effect run exactly once, the first time
    /// [`WorkUnitRunner::collect`] succeeds for this Attempt — e.g. writing
    /// the marker file a later Deterministic verification gate checks for,
    /// simulating a remediation Attempt actually landing its fix.
    pub on_collect: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl AttemptScript {
    /// A script whose `start` call fails outright.
    #[must_use]
    pub fn failing_start() -> Self {
        Self {
            fail_start: true,
            ..Self::default()
        }
    }

    /// A script whose `collect` call fails outright.
    #[must_use]
    pub fn failing_collect() -> Self {
        Self {
            fail_collect: true,
            ..Self::default()
        }
    }

    /// A script that succeeds and collects `result`.
    #[must_use]
    pub fn succeeding_with(result: WorkUnitResult) -> Self {
        Self {
            result: Some(result),
            ..Self::default()
        }
    }

    /// Runs `hook` exactly once, the first time this Attempt is collected.
    #[must_use]
    pub fn with_on_collect(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_collect = Some(Arc::new(hook));
        self
    }

    /// Answers `reconcile` with `outcome`.
    #[must_use]
    pub fn with_reconcile(mut self, outcome: ReconcileOutcome) -> Self {
        self.reconcile = Some(outcome);
        self
    }
}

/// A generic execution failure, matching the shape
/// `advance_episode.rs`/`recovery_matrix.rs` both used for a scripted
/// failure.
fn fake_runner_error() -> RunnerError {
    RunnerError {
        code: RunnerErrorCode::ExecutionFailed,
        message: "the fake runner was scripted to fail".to_owned(),
        retryable: false,
        trace_ref: None,
    }
}

/// The permissive default result: `Produced`, no outputs, no proposals.
fn default_result(attempt_id: AttemptId) -> WorkUnitResult {
    WorkUnitResult {
        protocol_version: loopward_api::PROTOCOL_VERSION.to_owned(),
        attempt_id,
        outcome: WorkUnitOutcome::Produced,
        outputs: Vec::new(),
        state_proposals: Vec::new(),
        observation_proposals: Vec::new(),
        decision_proposals: Vec::new(),
        verification_evidence: Vec::new(),
        unresolved: Vec::new(),
        recommended_continuation: None,
        usage: UsageReport::default(),
        runner_trace_refs: Vec::new(),
    }
}

/// A deterministic [`WorkUnitRunner`] driven entirely by [`AttemptScript`]s
/// set up before the scenario runs.
pub struct FakeRunner {
    capabilities: RunnerCapabilities,
    ordinal_scripts: Mutex<HashMap<u32, AttemptScript>>,
    attempt_scripts: Mutex<HashMap<AttemptId, AttemptScript>>,
    attempt_ordinals: Mutex<HashMap<AttemptId, u32>>,
    next_ordinal: AtomicU32,
    start_calls: AtomicUsize,
    collect_calls: AtomicUsize,
    reconcile_calls: AtomicUsize,
}

impl Default for FakeRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeRunner {
    /// A runner with capabilities matching a typical Phase One command
    /// runner (supports `reconcile`, managed-boot context control, no
    /// events/suspend/cancel/token-usage reporting).
    #[must_use]
    pub fn new() -> Self {
        Self {
            capabilities: RunnerCapabilities {
                protocol_versions: vec![loopward_api::PROTOCOL_VERSION.to_owned()],
                resumable: false,
                supports_reconcile: true,
                supports_events: false,
                supports_suspend: false,
                supports_cancel: false,
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
            },
            ordinal_scripts: Mutex::new(HashMap::new()),
            attempt_scripts: Mutex::new(HashMap::new()),
            attempt_ordinals: Mutex::new(HashMap::new()),
            next_ordinal: AtomicU32::new(0),
            start_calls: AtomicUsize::new(0),
            collect_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
        }
    }

    /// Overrides the capabilities this runner reports.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: RunnerCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Scripts the `n`th (1-based) Attempt this runner is asked to `start`.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn script_ordinal(&self, ordinal: u32, script: AttemptScript) {
        self.ordinal_scripts
            .lock()
            .expect("ordinal scripts lock poisoned")
            .insert(ordinal, script);
    }

    /// Scripts a specific, already-known [`AttemptId`] — the seam
    /// [`crate::failure::FailureInjector`]-seeded scenarios need, since a
    /// seeded Attempt is never actually started through this runner.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn script_attempt(&self, attempt_id: AttemptId, script: AttemptScript) {
        self.attempt_scripts
            .lock()
            .expect("attempt scripts lock poisoned")
            .insert(attempt_id, script);
    }

    /// How many times [`WorkUnitRunner::start`] has been called.
    #[must_use]
    pub fn start_calls(&self) -> usize {
        self.start_calls.load(Ordering::SeqCst)
    }

    /// How many times [`WorkUnitRunner::collect`] has been called.
    #[must_use]
    pub fn collect_calls(&self) -> usize {
        self.collect_calls.load(Ordering::SeqCst)
    }

    /// How many times [`WorkUnitRunner::reconcile`] has been called.
    #[must_use]
    pub fn reconcile_calls(&self) -> usize {
        self.reconcile_calls.load(Ordering::SeqCst)
    }

    /// Resolves the script that applies to `attempt_id`: an explicit
    /// per-Attempt script first, then whatever ordinal this runner started
    /// it under, then the permissive default.
    ///
    /// # Panics
    ///
    /// Panics if an internal lock is poisoned.
    fn resolve(&self, attempt_id: AttemptId) -> AttemptScript {
        if let Some(script) = self
            .attempt_scripts
            .lock()
            .expect("attempt scripts lock poisoned")
            .get(&attempt_id)
        {
            return script.clone();
        }
        let ordinal = self
            .attempt_ordinals
            .lock()
            .expect("attempt ordinals lock poisoned")
            .get(&attempt_id)
            .copied();
        if let Some(ordinal) = ordinal
            && let Some(script) = self
                .ordinal_scripts
                .lock()
                .expect("ordinal scripts lock poisoned")
                .get(&ordinal)
        {
            return script.clone();
        }
        AttemptScript::default()
    }
}

#[async_trait]
impl WorkUnitRunner for FakeRunner {
    async fn capabilities(&self) -> Result<RunnerCapabilities, RunnerError> {
        Ok(self.capabilities.clone())
    }

    async fn start(&self, request: WorkUnitRequest) -> Result<RunHandle, RunnerError> {
        self.start_calls.fetch_add(1, Ordering::SeqCst);
        let ordinal = self.next_ordinal.fetch_add(1, Ordering::SeqCst) + 1;
        self.attempt_ordinals
            .lock()
            .expect("attempt ordinals lock poisoned")
            .insert(request.attempt_id, ordinal);

        let script = self.resolve(request.attempt_id);
        if script.fail_start {
            return Err(fake_runner_error());
        }
        let external_ref = script
            .external_ref
            .clone()
            .unwrap_or_else(|| OpaqueRef::new(format!("fake-run-{ordinal}")));
        Ok(RunHandle {
            attempt_id: request.attempt_id,
            external_execution_ref: Some(external_ref),
        })
    }

    async fn reconcile(&self, handle: &RunHandle) -> Result<ReconcileOutcome, RunnerError> {
        self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
        let script = self.resolve(handle.attempt_id);
        Ok(script.reconcile.unwrap_or(ReconcileOutcome::Unknown))
    }

    async fn status(&self, _handle: &RunHandle) -> Result<RunStatus, RunnerError> {
        Ok(RunStatus {
            state: RunState::Completed,
            detail: None,
            observed_at: None,
        })
    }

    async fn events(
        &self,
        _handle: &RunHandle,
        _cursor: Option<EventCursor>,
    ) -> Result<RunnerEventPage, RunnerError> {
        Ok(RunnerEventPage {
            events: Vec::new(),
            next_cursor: None,
        })
    }

    async fn suspend(
        &self,
        _handle: &RunHandle,
        _reason: SuspensionReason,
    ) -> Result<(), RunnerError> {
        Ok(())
    }

    async fn cancel(
        &self,
        _handle: &RunHandle,
        _reason: CancellationReason,
    ) -> Result<(), RunnerError> {
        Ok(())
    }

    async fn collect(&self, handle: &RunHandle) -> Result<WorkUnitResult, RunnerError> {
        self.collect_calls.fetch_add(1, Ordering::SeqCst);
        let script = self.resolve(handle.attempt_id);
        if script.fail_collect {
            return Err(fake_runner_error());
        }
        if let Some(hook) = &script.on_collect {
            hook();
        }
        Ok(script
            .result
            .clone()
            .unwrap_or_else(|| default_result(handle.attempt_id)))
    }
}
