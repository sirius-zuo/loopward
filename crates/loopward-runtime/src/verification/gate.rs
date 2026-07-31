//! The [`VerificationGate`] contract and Phase One's concrete gates
//! (`docs/06-verification-and-recovery.md` §4, §8, §9).
//!
//! Every gate answers exactly one question about one bound target and
//! returns one [`VerificationFinding`]. A gate that decides its criterion
//! does not hold returns `Ok` with a [`GateOutcome::Failed`] finding; only a
//! gate that could not be *run* returns `Err`. That split is
//! `docs/01-system-architecture.md` §8's "separates operation errors from
//! criterion failures", and it is why a broken checker can never be mistaken
//! for a passing check.
//!
//! Phase One implements [`SchemaGate`], [`IntegrityGate`],
//! [`DeterministicGate`], and [`PolicyGate`] fully. [`AgenticGate`] and
//! [`HumanGate`] implement the same contract but suspend: Loopward has no
//! Phase One mechanism for assembling an independent review context or
//! collecting a principal's approval, and `docs/06` §8-§9 are explicit that
//! neither may assert authority on its own. Suspending with a structured
//! [`InputRequest`] is the honest verdict, and it keeps the aggregate outcome
//! at [`loopward_core::VerificationOutcome::Suspended`] rather than silently
//! accepting unreviewed work.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loopward_api::ids::Timestamp;
use loopward_api::resource::{OutputContract, ResourceRef, SchemaRef};
use loopward_api::work_unit::{WorkUnitOutcome, WorkUnitResult};
use serde::{Deserialize, Serialize};

use crate::ports::{ResourceBeginHint, ResourceBroker};

use super::{
    Digest, FindingLineage, GateId, GateOutcome, InputRequest, VerificationError,
    VerificationFinding, VerificationGateKind, VerificationGateSpec, VerificationTarget,
};

// ---------------------------------------------------------------------------
// Gate configuration
// ---------------------------------------------------------------------------

/// The gate-kind-specific half of a [`VerificationGateSpec`].
///
/// Configuration lives on the spec rather than on the gate implementation so
/// one registered implementation per kind serves every spec of that kind in
/// every plan — two deterministic gates in one plan run two different
/// commands through one [`DeterministicGate`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateConfig {
    /// Configuration for [`SchemaGate`].
    Schema(SchemaGateConfig),
    /// Configuration for [`IntegrityGate`].
    Integrity(IntegrityGateConfig),
    /// Configuration for [`DeterministicGate`].
    Deterministic(DeterministicGateConfig),
    /// Configuration for [`PolicyGate`].
    Policy(PolicyGateConfig),
    /// Configuration for [`AgenticGate`].
    Agentic(AgenticGateConfig),
    /// Configuration for [`HumanGate`].
    Human(HumanGateConfig),
}

impl GateConfig {
    /// The gate kind this configuration belongs to.
    #[must_use]
    pub fn kind(&self) -> VerificationGateKind {
        match self {
            Self::Schema(_) => VerificationGateKind::Schema,
            Self::Integrity(_) => VerificationGateKind::Integrity,
            Self::Deterministic(_) => VerificationGateKind::Deterministic,
            Self::Policy(_) => VerificationGateKind::Policy,
            Self::Agentic(_) => VerificationGateKind::Agentic,
            Self::Human(_) => VerificationGateKind::Human,
        }
    }
}

/// What [`SchemaGate`] checks: the result's declared contract and the shape
/// of the documents it produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaGateConfig {
    /// The output contract the Work Unit declared. Recorded on the finding
    /// so a schema rejection names the contract it was judged against.
    pub contract: OutputContract,
    /// The result protocol version the result must declare, when pinned.
    pub expected_protocol_version: Option<String>,
    /// JSON pointers (e.g. `/summary`) that must resolve to a non-null value
    /// in every declared output document.
    #[serde(default)]
    pub required_pointers: Vec<String>,
    /// The minimum number of declared outputs.
    pub minimum_outputs: usize,
}

impl Default for SchemaGateConfig {
    fn default() -> Self {
        Self {
            contract: OutputContract {
                schema: String::new(),
            },
            expected_protocol_version: None,
            required_pointers: Vec::new(),
            minimum_outputs: 0,
        }
    }
}

/// What [`IntegrityGate`] checks: that declared digests and provenance hold
/// against the content actually stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityGateConfig {
    /// Every declared output must carry a content digest. An undigested
    /// output cannot be bound to, so it cannot be verified.
    pub require_output_digests: bool,
    /// Resolve every declared output and confirm its bytes hash to the
    /// digest the runner declared.
    pub verify_content_digests: bool,
    /// Resource URIs that must appear among the result's verification
    /// evidence.
    #[serde(default)]
    pub required_evidence: Vec<String>,
}

impl Default for IntegrityGateConfig {
    fn default() -> Self {
        Self {
            require_output_digests: true,
            verify_content_digests: true,
            required_evidence: Vec::new(),
        }
    }
}

/// The check command [`DeterministicGate`] runs: a test suite, a linter, a
/// checksum, a schema validator — anything reproducible whose exit code and
/// output are the verdict.
///
/// This is verification's own narrow use of subprocess execution. It is not
/// a `WorkUnitRunner` and it does not launch a Work Unit Attempt: it runs one
/// bounded check against work that has already been produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicGateConfig {
    /// The program to execute.
    pub program: String,
    /// The program's arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// The working directory to execute in. Inherited when absent.
    pub working_directory: Option<PathBuf>,
    /// Additional environment variables for the check.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// The exit code that means the check passed.
    pub expected_exit_code: i32,
    /// Text that must appear in the captured output for the check to pass,
    /// even when the exit code is right.
    pub required_stdout_substring: Option<String>,
    /// Text whose presence in the captured output fails the check, even when
    /// the exit code is right.
    pub forbidden_stdout_substring: Option<String>,
    /// How long the check may run before it is killed and the finding
    /// becomes [`GateOutcome::Inconclusive`] — a check that never finished
    /// did not pass and did not fail.
    pub timeout: Option<Duration>,
    /// How many bytes of captured output are retained as evidence.
    pub max_captured_output_bytes: usize,
}

impl Default for DeterministicGateConfig {
    fn default() -> Self {
        Self {
            program: String::new(),
            args: Vec::new(),
            working_directory: None,
            env: BTreeMap::new(),
            expected_exit_code: 0,
            required_stdout_substring: None,
            forbidden_stdout_substring: None,
            timeout: Some(Duration::from_mins(5)),
            max_captured_output_bytes: 64 * 1024,
        }
    }
}

/// What [`PolicyGate`] checks: deterministic domain and deployment rules that
/// need no external process.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyGateConfig {
    /// The runner outcomes policy admits. Empty means unconstrained.
    #[serde(default)]
    pub allowed_outcomes: Vec<WorkUnitOutcome>,
    /// Whether an open issue the runner labelled `blocking` bars acceptance.
    pub reject_blocking_open_issues: bool,
    /// Whether every proposed decision must carry a rationale.
    pub require_decision_rationale: bool,
    /// Ceilings on known usage metrics, by metric name
    /// (`docs/06` §11: resource and token budget policies must permit
    /// acceptance).
    #[serde(default)]
    pub usage_ceilings: BTreeMap<String, f64>,
}

impl Default for PolicyGateConfig {
    fn default() -> Self {
        Self {
            allowed_outcomes: vec![WorkUnitOutcome::Produced],
            reject_blocking_open_issues: true,
            require_decision_rationale: true,
            usage_ceilings: BTreeMap::new(),
        }
    }
}

/// What an agentic semantic review would be assembled from (`docs/06` §8).
///
/// Recorded on the suspending finding so the review context a later phase
/// assembles is the one the plan declared, not whatever the implementation
/// trajectory happened to contain.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgenticGateConfig {
    /// The rubric the reviewer must apply.
    pub rubric: Option<SchemaRef>,
    /// The reviewer profile the plan names.
    pub reviewer: Option<String>,
    /// The acceptance criteria the reviewer must judge against.
    #[serde(default)]
    pub criteria: Vec<String>,
}

/// The structured approval request [`HumanGate`] suspends with
/// (`docs/06` §9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanGateConfig {
    /// A stable, machine-readable request code.
    pub code: String,
    /// The prompt shown to the approving principal.
    pub prompt: String,
    /// The schema the approval response must satisfy. `docs/06` §9: the
    /// approval must identify the principal, decision time, target digest,
    /// and scope.
    pub expected_schema: SchemaRef,
}

impl Default for HumanGateConfig {
    fn default() -> Self {
        Self {
            code: "verification.human.approval".to_owned(),
            prompt: "Approve this verified result?".to_owned(),
            expected_schema: SchemaRef::new("loopward.verification-approval/v0.1"),
        }
    }
}

// ---------------------------------------------------------------------------
// The gate contract
// ---------------------------------------------------------------------------

/// Everything one gate evaluation is computed from.
///
/// Note what is *not* here: no way for the result to alter the spec, the
/// target, or the enforcement level. A runner cannot weaken its own
/// verification plan through its result (`docs/06` §3).
pub struct GateRequest {
    /// The gate spec being evaluated.
    pub spec: VerificationGateSpec,
    /// The exact target under judgement.
    pub target: VerificationTarget,
    /// The precomputed [`VerificationTarget::binding_digest`], so every
    /// finding in a run binds to one consistently computed value.
    pub target_digest: Digest,
    /// The collected runner result. A proposal, never an accepted outcome.
    pub result: Arc<WorkUnitResult>,
    /// The broker used to resolve declared content and to finalize captured
    /// evidence.
    pub resources: Arc<dyn ResourceBroker>,
    /// The lineage stamped onto every finding this evaluation produces.
    pub lineage: FindingLineage,
    /// The observation time stamped onto every finding.
    pub observed_at: Timestamp,
}

impl GateRequest {
    /// A finding for this gate, already bound to the target, the criterion
    /// the spec supports, and the run's lineage.
    ///
    /// Gates build their verdicts through this rather than assembling a
    /// [`VerificationFinding`] by hand, so no gate can forget its target
    /// binding or misreport its own enforcement level.
    #[must_use]
    pub fn finding(
        &self,
        outcome: GateOutcome,
        code: impl Into<String>,
        statement: impl Into<String>,
    ) -> VerificationFinding {
        VerificationFinding {
            gate_id: self.spec.id.clone(),
            kind: self.spec.kind,
            enforcement: self.spec.enforcement,
            outcome,
            code: code.into(),
            statement: statement.into(),
            evidence: Vec::new(),
            target_digest: self.target_digest.clone(),
            criterion: self.spec.criterion.clone(),
            lineage: self.lineage.clone(),
            input_request: None,
            observed_at: self.observed_at,
        }
    }

    /// The gate's identity, for error reporting.
    fn gate_id(&self) -> GateId {
        self.spec.id.clone()
    }
}

/// One question about one bound target.
#[async_trait]
pub trait VerificationGate: Send + Sync {
    /// The gate type this implementation evaluates.
    fn kind(&self) -> VerificationGateKind;

    /// Evaluates one gate spec against one target.
    ///
    /// # Errors
    ///
    /// Returns [`VerificationError`] only when the gate could not be run at
    /// all — unresolvable content, an unspawnable command, a configuration
    /// that does not match the gate's kind. A criterion that does not hold
    /// is an `Ok` [`GateOutcome::Failed`] finding, never an error.
    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError>;
}

/// Extracts the configuration variant a gate expects, or reports the
/// mismatch.
macro_rules! expect_config {
    ($request:expr, $variant:path, $kind:expr) => {
        match &$request.spec.config {
            $variant(config) => config.clone(),
            other => {
                return Err(VerificationError::ConfigMismatch {
                    gate: $request.gate_id(),
                    kind: $kind.label(),
                    config: other.kind().label(),
                });
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Schema gate
// ---------------------------------------------------------------------------

/// Checks that the result satisfies its declared output contract
/// (`docs/06` §4, step 1).
///
/// The cheapest and most conclusive gate: a result whose shape is wrong
/// cannot be right, and finding that out costs one JSON parse per output.
#[derive(Debug, Default, Clone, Copy)]
pub struct SchemaGate;

#[async_trait]
impl VerificationGate for SchemaGate {
    fn kind(&self) -> VerificationGateKind {
        VerificationGateKind::Schema
    }

    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError> {
        let config = expect_config!(request, GateConfig::Schema, VerificationGateKind::Schema);

        if let Some(expected) = &config.expected_protocol_version
            && &request.result.protocol_version != expected
        {
            return Ok(request.finding(
                GateOutcome::Failed,
                "verification.schema.protocol_mismatch",
                format!(
                    "result declares protocol version `{}`, but contract `{}` requires `{expected}`",
                    request.result.protocol_version, config.contract.schema
                ),
            ));
        }

        if request.result.outputs.len() < config.minimum_outputs {
            return Ok(request.finding(
                GateOutcome::Failed,
                "verification.schema.insufficient_outputs",
                format!(
                    "contract `{}` requires at least {} output(s); the result declares {}",
                    config.contract.schema,
                    config.minimum_outputs,
                    request.result.outputs.len()
                ),
            ));
        }

        for output in &request.result.outputs {
            let bytes = resolve(&request, output).await?;
            let Ok(document) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                let mut finding = request.finding(
                    GateOutcome::Failed,
                    "verification.schema.unparsable_output",
                    format!(
                        "output `{}` is not valid JSON, so it cannot satisfy contract `{}`",
                        output.uri, config.contract.schema
                    ),
                );
                finding.evidence.push(output.clone());
                return Ok(finding);
            };

            for pointer in &config.required_pointers {
                let present = document
                    .pointer(pointer)
                    .is_some_and(|value| !value.is_null());
                if !present {
                    let mut finding = request.finding(
                        GateOutcome::Failed,
                        "verification.schema.missing_field",
                        format!(
                            "output `{}` does not carry required field `{pointer}` of contract `{}`",
                            output.uri, config.contract.schema
                        ),
                    );
                    finding.evidence.push(output.clone());
                    return Ok(finding);
                }
            }
        }

        let mut finding = request.finding(
            GateOutcome::Passed,
            "verification.schema.satisfied",
            format!(
                "every declared output satisfies contract `{}`",
                config.contract.schema
            ),
        );
        finding.evidence.clone_from(&request.result.outputs);
        Ok(finding)
    }
}

// ---------------------------------------------------------------------------
// Integrity gate
// ---------------------------------------------------------------------------

/// Checks digest and provenance integrity (`docs/06` §4, step 2).
///
/// Evidence must be immutable and content-addressed
/// (`docs/02-core-domain-model.md` §9). This gate is what makes that true
/// rather than assumed: it resolves each declared output and confirms the
/// stored bytes still hash to the digest the runner declared.
#[derive(Debug, Default, Clone, Copy)]
pub struct IntegrityGate;

#[async_trait]
impl VerificationGate for IntegrityGate {
    fn kind(&self) -> VerificationGateKind {
        VerificationGateKind::Integrity
    }

    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError> {
        let config = expect_config!(
            request,
            GateConfig::Integrity,
            VerificationGateKind::Integrity
        );

        for output in &request.result.outputs {
            let Some(declared) = output.digest.as_deref() else {
                if config.require_output_digests {
                    let mut finding = request.finding(
                        GateOutcome::Failed,
                        "verification.integrity.undigested_output",
                        format!(
                            "output `{}` carries no content digest, so no finding can bind to it",
                            output.uri
                        ),
                    );
                    finding.evidence.push(output.clone());
                    return Ok(finding);
                }
                continue;
            };

            if config.verify_content_digests {
                let bytes = resolve(&request, output).await?;
                let actual = crate::context::content_digest(&bytes);
                if actual != declared {
                    let mut finding = request.finding(
                        GateOutcome::Failed,
                        "verification.integrity.digest_mismatch",
                        format!(
                            "output `{}` declares `{declared}` but its stored content hashes to `{actual}`",
                            output.uri
                        ),
                    );
                    finding.evidence.push(output.clone());
                    return Ok(finding);
                }
            }
        }

        for required in &config.required_evidence {
            let present = request
                .result
                .verification_evidence
                .iter()
                .any(|evidence| &evidence.uri == required);
            if !present {
                return Ok(request.finding(
                    GateOutcome::Failed,
                    "verification.integrity.missing_evidence",
                    format!(
                        "the plan requires evidence `{required}`, which the result does not offer"
                    ),
                ));
            }
        }

        // The target binding is only meaningful if the digests it names are
        // the digests the result actually declared.
        for (uri, bound) in &request.target.resource_digests {
            let declared = request
                .result
                .outputs
                .iter()
                .chain(request.result.verification_evidence.iter())
                .find(|reference| &reference.uri == uri)
                .and_then(|reference| reference.digest.as_deref());
            if let Some(declared) = declared
                && declared != bound.as_str()
            {
                return Ok(request.finding(
                    GateOutcome::Failed,
                    "verification.integrity.target_binding_mismatch",
                    format!(
                        "the target binds `{uri}` to `{bound}`, but the result declares `{declared}`"
                    ),
                ));
            }
        }

        let mut finding = request.finding(
            GateOutcome::Passed,
            "verification.integrity.satisfied",
            "every declared output resolves to content matching its declared digest",
        );
        finding.evidence.clone_from(&request.result.outputs);
        Ok(finding)
    }
}

// ---------------------------------------------------------------------------
// Deterministic gate
// ---------------------------------------------------------------------------

/// Runs a configured check command and turns its exit code and output into a
/// verdict (`docs/06` §4, step 3).
#[derive(Debug, Default, Clone, Copy)]
pub struct DeterministicGate;

#[async_trait]
impl VerificationGate for DeterministicGate {
    fn kind(&self) -> VerificationGateKind {
        VerificationGateKind::Deterministic
    }

    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError> {
        let config = expect_config!(
            request,
            GateConfig::Deterministic,
            VerificationGateKind::Deterministic
        );

        let mut command = tokio::process::Command::new(&config.program);
        command
            .args(&config.args)
            .envs(&config.env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(directory) = &config.working_directory {
            command.current_dir(directory);
        }

        let invocation = format!("{} {}", config.program, config.args.join(" "));
        let spawned = command.output();
        let output = match config.timeout {
            Some(limit) => match tokio::time::timeout(limit, spawned).await {
                Ok(output) => output,
                Err(_elapsed) => {
                    // A check that never finished neither passed nor failed.
                    return Ok(request.finding(
                        GateOutcome::Inconclusive,
                        "verification.deterministic.timed_out",
                        format!(
                            "check command `{invocation}` did not finish within {limit:?}, so it reached no verdict"
                        ),
                    ));
                }
            },
            None => spawned.await,
        }
        .map_err(|source| VerificationError::CommandNotExecutable {
            gate: request.gate_id(),
            program: config.program.clone(),
            source,
        })?;

        let mut captured = String::from_utf8_lossy(&output.stdout).into_owned();
        if !output.stderr.is_empty() {
            captured.push_str(&String::from_utf8_lossy(&output.stderr));
        }
        let retained: String = captured
            .char_indices()
            .take_while(|(index, _)| *index < config.max_captured_output_bytes)
            .map(|(_, character)| character)
            .collect();

        let evidence = finalize_evidence(&request, &config.program, retained.as_bytes()).await?;
        let exit_code = output.status.code();

        let mut finding = if exit_code != Some(config.expected_exit_code) {
            request.finding(
                GateOutcome::Failed,
                "verification.deterministic.unexpected_exit",
                format!(
                    "check command `{invocation}` exited with {} but the plan requires {}",
                    exit_code.map_or_else(|| "a signal".to_owned(), |code| code.to_string()),
                    config.expected_exit_code
                ),
            )
        } else if config
            .required_stdout_substring
            .as_ref()
            .is_some_and(|needle| !retained.contains(needle.as_str()))
        {
            request.finding(
                GateOutcome::Failed,
                "verification.deterministic.missing_output",
                format!(
                    "check command `{invocation}` exited {} but its output does not contain `{}`",
                    config.expected_exit_code,
                    config.required_stdout_substring.as_deref().unwrap_or("")
                ),
            )
        } else if config
            .forbidden_stdout_substring
            .as_ref()
            .is_some_and(|needle| retained.contains(needle.as_str()))
        {
            request.finding(
                GateOutcome::Failed,
                "verification.deterministic.forbidden_output",
                format!(
                    "check command `{invocation}` output contains forbidden text `{}`",
                    config.forbidden_stdout_substring.as_deref().unwrap_or("")
                ),
            )
        } else {
            request.finding(
                GateOutcome::Passed,
                "verification.deterministic.satisfied",
                format!(
                    "check command `{invocation}` exited {} as the plan requires",
                    config.expected_exit_code
                ),
            )
        };

        finding.evidence.push(evidence);
        Ok(finding)
    }
}

// ---------------------------------------------------------------------------
// Policy gate
// ---------------------------------------------------------------------------

/// Applies deterministic domain and deployment policy (`docs/06` §4, step 4).
#[derive(Debug, Default, Clone, Copy)]
pub struct PolicyGate;

#[async_trait]
impl VerificationGate for PolicyGate {
    fn kind(&self) -> VerificationGateKind {
        VerificationGateKind::Policy
    }

    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError> {
        let config = expect_config!(request, GateConfig::Policy, VerificationGateKind::Policy);

        if !config.allowed_outcomes.is_empty()
            && !config.allowed_outcomes.contains(&request.result.outcome)
        {
            return Ok(request.finding(
                GateOutcome::Failed,
                "verification.policy.outcome_not_admitted",
                format!(
                    "policy does not admit runner outcome `{:?}` for acceptance",
                    request.result.outcome
                ),
            ));
        }

        if config.reject_blocking_open_issues
            && let Some(issue) = request.result.unresolved.iter().find(|issue| {
                issue
                    .severity
                    .as_deref()
                    .is_some_and(|severity| severity.eq_ignore_ascii_case("blocking"))
            })
        {
            let mut finding = request.finding(
                GateOutcome::Failed,
                "verification.policy.blocking_issue_open",
                format!("a blocking issue remains open: {}", issue.statement),
            );
            finding.evidence.clone_from(&issue.evidence);
            return Ok(finding);
        }

        if config.require_decision_rationale
            && let Some(decision) = request.result.decision_proposals.iter().find(|proposal| {
                proposal
                    .rationale
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
            })
        {
            return Ok(request.finding(
                GateOutcome::Failed,
                "verification.policy.decision_without_rationale",
                format!(
                    "decision proposal `{}` carries no rationale",
                    decision.statement
                ),
            ));
        }

        for (metric, ceiling) in &config.usage_ceilings {
            if let Some(observed) = request.result.usage.known.get(metric)
                && observed > ceiling
            {
                return Ok(request.finding(
                    GateOutcome::Failed,
                    "verification.policy.budget_exceeded",
                    format!(
                        "usage metric `{metric}` reached {observed}, over the ceiling {ceiling}"
                    ),
                ));
            }
        }

        Ok(request.finding(
            GateOutcome::Passed,
            "verification.policy.satisfied",
            "every configured policy rule admits this result",
        ))
    }
}

// ---------------------------------------------------------------------------
// Agentic and human gates
// ---------------------------------------------------------------------------

/// Semantic review in a distinct, bounded context (`docs/06` §8).
///
/// Phase One has no mechanism for assembling that independent review context,
/// so this gate always suspends. It deliberately does *not* fall back to
/// passing, and it deliberately does not reuse the implementation
/// trajectory: `docs/06` §8 requires a review context assembled for review,
/// and an agentic gate is proposal-generating unless domain policy grants it
/// authority. Suspending records what the review would need and leaves the
/// decision to the Loop Controller.
#[derive(Debug, Default, Clone, Copy)]
pub struct AgenticGate;

#[async_trait]
impl VerificationGate for AgenticGate {
    fn kind(&self) -> VerificationGateKind {
        VerificationGateKind::Agentic
    }

    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError> {
        let config = expect_config!(request, GateConfig::Agentic, VerificationGateKind::Agentic);

        let mut finding = request.finding(
            GateOutcome::Suspended,
            "verification.agentic.review_required",
            format!(
                "semantic review against rubric `{}` by reviewer `{}` has not been performed; \
                 an agentic gate is proposal-generating and cannot pass itself",
                config
                    .rubric
                    .as_ref()
                    .map_or_else(|| "<unset>".to_owned(), SchemaRef::to_string),
                config.reviewer.as_deref().unwrap_or("<unset>")
            ),
        );
        finding.evidence.clone_from(&request.result.outputs);
        Ok(finding)
    }
}

/// Human approval (`docs/06` §9).
///
/// Suspends the Loop with a structured [`InputRequest`]. Collecting and
/// recording the response — which must identify the principal, decision time,
/// target digest, and scope — happens outside this gate; the gate's job is to
/// make sure the Loop stops until it has.
#[derive(Debug, Default, Clone, Copy)]
pub struct HumanGate;

#[async_trait]
impl VerificationGate for HumanGate {
    fn kind(&self) -> VerificationGateKind {
        VerificationGateKind::Human
    }

    async fn evaluate(
        &self,
        request: GateRequest,
    ) -> Result<VerificationFinding, VerificationError> {
        let config = expect_config!(request, GateConfig::Human, VerificationGateKind::Human);

        let evidence: Vec<ResourceRef> = request
            .result
            .outputs
            .iter()
            .chain(request.result.verification_evidence.iter())
            .cloned()
            .collect();

        let mut finding = request.finding(
            GateOutcome::Suspended,
            "verification.human.approval_required",
            format!(
                "human approval `{}` is required against target `{}`",
                config.code, request.target_digest
            ),
        );
        finding.input_request = Some(InputRequest {
            code: config.code.clone(),
            prompt: config.prompt.clone(),
            expected_schema: config.expected_schema.clone(),
            evidence: evidence.clone(),
        });
        finding.evidence = evidence;
        Ok(finding)
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Resolves `reference` through the request's broker, mapping a broker
/// failure to an operation error rather than to a criterion failure.
async fn resolve(
    request: &GateRequest,
    reference: &ResourceRef,
) -> Result<Vec<u8>, VerificationError> {
    request
        .resources
        .resolve(reference)
        .await
        .map_err(|source| VerificationError::Resource {
            gate: request.gate_id(),
            source,
        })
}

/// Finalizes `bytes` as durable evidence, so a finding points at content that
/// can still be read after the process that produced it is gone.
async fn finalize_evidence(
    request: &GateRequest,
    program: &str,
    bytes: &[u8],
) -> Result<ResourceRef, VerificationError> {
    let hint = ResourceBeginHint {
        media_type: Some("text/plain; charset=utf-8".to_owned()),
        summary: Some(format!(
            "captured output of check command `{program}` for gate `{}`",
            request.spec.id
        )),
    };
    let map = |source| VerificationError::Resource {
        gate: request.gate_id(),
        source,
    };
    let handle = request.resources.begin(hint).await.map_err(map)?;
    request.resources.write(&handle, bytes).await.map_err(map)?;
    request.resources.finalize(handle).await.map_err(map)
}
