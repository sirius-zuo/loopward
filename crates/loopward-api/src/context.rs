//! Context lifecycle DTOs: Boot Package manifests, context items, budgets,
//! and the proposal types a runner returns for durable-state handoff.
//!
//! These types implement the deterministic hydration pipeline described in
//! `docs/04-context-lifecycle.md`. A Boot Package Manifest is the stable,
//! Loopward-owned half of a Boot Package; the runner-specific materialized
//! payload built from it is not part of this wire contract.

use serde::{Deserialize, Serialize};

use crate::ids::{EpisodeId, LoopId, Timestamp, WorkUnitId};
use crate::resource::{CheckpointRef, ResourceRef};

/// How much control Loopward has over a runner's context behavior.
///
/// Capability discovery is descriptive, not aspirational: Loopward must not
/// assume a runner supports a level it did not declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextControlLevel {
    /// Loopward can start and collect a run but cannot inspect or control
    /// its context behavior.
    Opaque,
    /// The runner reports context usage or compaction events.
    Observable,
    /// Loopward controls the initial Boot Package; later context behavior
    /// is runner-managed.
    ManagedBoot,
    /// Loopward can also supply resources incrementally and request a
    /// structured drain or compaction.
    Managed,
}

/// A runner's declared context-management capabilities.
///
/// Each flag is an independently declared, orthogonal capability rather than
/// a state machine: `struct_excessive_bools` is intentionally allowed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ContextCapabilities {
    /// The runner's context control level.
    pub control_level: ContextControlLevel,
    /// Whether the runner reports token/context usage.
    pub reports_usage: bool,
    /// Whether the runner reports internal compaction events.
    pub reports_compaction: bool,
    /// Whether the runner supports scoped, on-demand resource fetch.
    pub supports_resource_fetch: bool,
    /// Whether the runner supports a structured context handoff/drain.
    pub supports_structured_handoff: bool,
    /// Whether the runner supports resuming a suspended run.
    pub supports_resume: bool,
}

/// The purpose a context item serves in a Boot Package.
///
/// `Pinned` includes system policy, active user constraints, acceptance
/// criteria, safety requirements, and applicable invariants; it must survive
/// hydration and drain losslessly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextClass {
    /// System policy, active constraints, acceptance criteria, safety
    /// requirements, and applicable invariants. Preserved losslessly.
    Pinned,
    /// The current authoritative domain/kernel state relevant to this step.
    CurrentState,
    /// Inputs specific to the current Work Unit.
    CurrentWork,
    /// An applicable, currently-active decision.
    Decision,
    /// Supporting evidence for a decision or observation.
    Evidence,
    /// A pointer to material that may be fetched on demand.
    Reference,
    /// A recently produced outcome relevant to continuity.
    RecentOutcome,
}

/// How urgently a context item must be included in a Boot Package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPriority {
    /// Must be present, or hydration fails.
    Required,
    /// Selected before supplemental material.
    Important,
    /// Uses remaining budget after required and important items.
    Supplemental,
    /// Referenced and fetched only if the runner supports scoped retrieval.
    OnDemand,
}

/// How faithfully a context item represents its source.
///
/// Pinned constraints must use `Verbatim` or a lossless structured
/// representation; a lossy summary can never replace them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fidelity {
    /// The exact source content, unmodified.
    Verbatim,
    /// A lossless structured representation of the source.
    Structured,
    /// A bounded excerpt of the source.
    Excerpt,
    /// A lossy natural-language summary.
    Summary,
    /// Not materialized; only a reference is included.
    ReferenceOnly,
}

/// How sensitive a context item's content is, for redaction and access
/// control purposes.
///
/// Phase One documentation does not enumerate a closed set of levels; this
/// is a minimal, conservative interpretation pending refinement by
/// `loopward-core`/`loopward-runtime` policy work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    /// Safe to expose without restriction.
    Public,
    /// Internal-only; not a secret, but not for external exposure.
    Internal,
    /// Confidential; must be redacted from logs and traces by default.
    Confidential,
    /// Access-restricted; requires explicit authorization to materialize.
    Restricted,
}

/// Whether a context item still reflects current authoritative state.
///
/// As with [`Sensitivity`], Phase One documentation names this field but
/// does not enumerate its closed variant set; this is a minimal
/// interpretation pending refinement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Validity {
    /// Reflects current authoritative state.
    Current,
    /// Known to no longer reflect current state.
    Stale,
    /// Explicitly replaced by a newer item.
    Superseded,
    /// Validity could not be determined.
    Unknown,
}

/// How a context item's token estimate was produced.
///
/// Unknown token usage remains unknown; Loopward must not turn an
/// unavailable vendor metric into a fabricated precise value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimateMethod {
    /// Counted with a concrete tokenizer.
    Tokenizer,
    /// Reported directly by the runner or model vendor.
    VendorReported,
    /// Estimated with a heuristic (e.g. byte length / average ratio).
    Heuristic,
}

/// One candidate item considered for inclusion in a Boot Package, carrying
/// its provenance and selection reasoning before payload materialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    /// An opaque, manifest-scoped identifier for this item.
    pub id: String,
    /// The kind of underlying material this item represents (e.g.
    /// `constraint`, `resource`, `decision-summary`). Open and
    /// domain-extensible; Phase One does not close this set.
    pub kind: String,
    /// Where this item's content comes from.
    pub source: ResourceRef,
    /// Why this item is being considered.
    pub classification: ContextClass,
    /// How urgently this item must be included.
    pub priority: ContextPriority,
    /// How faithfully this item represents its source.
    pub fidelity: Fidelity,
    /// How sensitive this item's content is.
    pub sensitivity: Sensitivity,
    /// Whether this item still reflects current authoritative state.
    pub validity: Validity,
    /// An estimated token cost, when available.
    pub estimated_tokens: Option<u64>,
    /// How the token estimate was produced, when available.
    pub estimate_method: Option<EstimateMethod>,
    /// Confidence in the token estimate, in `[0.0, 1.0]`, when available.
    pub estimate_confidence: Option<f32>,
    /// A content digest binding this item to an exact, immutable value.
    pub digest: String,
    /// A short, human-readable reason this item was selected.
    pub selection_reason: String,
}

/// The requested (not yet resolved) context budget attached to a Work Unit.
///
/// This is intentionally smaller than [`ContextBudget`]: a caller can
/// request soft/hard input limits, but reserved output/tool/safety-margin
/// tokens are resolved later by the Context Manager against runner and
/// model defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextBudgetRequest {
    /// A requested soft input token limit.
    pub soft_input_limit_tokens: Option<u64>,
    /// A requested hard input token limit.
    pub hard_input_limit_tokens: Option<u64>,
}

/// The resolved token budget for one Boot Package hydration, reserving
/// space for execution rather than filling the model window with boot
/// context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudget {
    /// The model's total context window, when known.
    pub model_window_tokens: Option<u64>,
    /// The soft input limit: finish the current safe unit of work, then
    /// rotate.
    pub soft_input_limit_tokens: u64,
    /// The hard input limit: stop admitting additional model work.
    pub hard_input_limit_tokens: u64,
    /// Tokens reserved for the model's output.
    pub reserved_output_tokens: u64,
    /// Tokens reserved for tool call/response overhead.
    pub reserved_tool_tokens: u64,
    /// An additional safety margin reserved beyond the above.
    pub safety_margin_tokens: u64,
}

/// The stable, Loopward-owned manifest half of a Boot Package.
///
/// The runner-specific materialized payload built from this manifest is not
/// authoritative state and is not part of this contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BootPackageManifest {
    /// The manifest schema version, e.g. `loopward.boot-package-manifest/v0.1`.
    pub schema_version: String,
    /// The Loop this Boot Package was hydrated for.
    pub loop_id: LoopId,
    /// The Episode this Boot Package was hydrated for.
    pub episode_id: EpisodeId,
    /// The Runtime Session generation this Boot Package targets.
    pub session_generation: u32,
    /// The Work Unit this Boot Package was assembled for.
    pub work_unit_id: WorkUnitId,
    /// The Checkpoint this manifest was hydrated from.
    pub checkpoint_ref: CheckpointRef,
    /// The version of the deterministic selection rules used to build this
    /// manifest. The same inputs and rule version must produce the same
    /// manifest.
    pub context_rule_version: String,
    /// The token budget this manifest was fit to.
    pub budget: ContextBudget,
    /// Every item selected (or considered and excluded) for this Boot
    /// Package, with provenance and selection reasoning.
    pub items: Vec<ContextItem>,
    /// A digest binding this manifest to the exact pinned state it saw.
    /// Pinned constraints must survive hydration and drain losslessly, and
    /// this digest is the integrity check for that guarantee.
    pub pinned_digest: String,
    /// A digest of the manifest itself, for integrity and replay checks.
    pub manifest_digest: String,
}

/// A permission granted for on-demand resource retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePermission {
    /// The grant allows reading the resource's content.
    Read,
    /// The grant allows writing or replacing the resource's content.
    Write,
}

/// A scoped, time-boxed grant permitting a runner to fetch a resource
/// on demand during a specific Work Unit.
///
/// On-demand retrieval performed under a grant must be scoped, auditable,
/// subject to access control, digest-checked when immutable content is
/// expected, and recorded as a resource-access event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceGrant {
    /// The resource this grant applies to.
    pub resource: ResourceRef,
    /// What the holder is permitted to do with the resource.
    pub permissions: Vec<ResourcePermission>,
    /// The Work Unit this grant is scoped to.
    pub scope: WorkUnitId,
    /// When this grant expires, if bounded.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub expires_at: Option<Timestamp>,
}

/// Whether a claim is supported by evidence, merely hypothesized, or has
/// been disproven (`docs/02-core-domain-model.md` §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    /// Supported by current evidence.
    Verified,
    /// Asserted but not yet evidence-checked.
    Unverified,
    /// Checked and found false.
    Disproven,
}

/// A runner's proposal that some durable-state assertion is now true.
///
/// This is a proposal only: it cannot mutate authoritative Loop state until
/// Loopward validates and verifies it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateAssertionProposal {
    /// The proposed assertion, in natural language.
    pub statement: String,
    /// Evidence offered in support of the assertion.
    #[serde(default)]
    pub evidence: Vec<ResourceRef>,
    /// The proposing runner's confidence in the assertion, when reported.
    pub confidence: Option<f32>,
}

/// A runner's proposal that an issue remains open and unresolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenIssueProposal {
    /// A description of the open issue.
    pub statement: String,
    /// An open-ended severity label (e.g. `blocking`, `advisory`).
    pub severity: Option<String>,
    /// Evidence relevant to the open issue.
    #[serde(default)]
    pub evidence: Vec<ResourceRef>,
}

/// A runner's proposal that some state is true, supported or hypothesized.
///
/// Decision proposals do not become active decisions automatically; an
/// observation proposal does not become a verified observation
/// automatically either. Both require an explicit acceptance gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationProposal {
    /// The observed or hypothesized state, in natural language.
    pub statement: String,
    /// The proposing runner's own assessment of support for this claim.
    /// Loopward's verification pipeline may reach a different conclusion.
    pub verification_status: VerificationStatus,
    /// Evidence relevant to the observation.
    #[serde(default)]
    pub evidence: Vec<ResourceRef>,
}

/// A runner's proposal that a course of action be selected.
///
/// Runner-produced decisions remain proposals until an explicit decision
/// gate activates them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionProposal {
    /// The proposed course of action, in natural language.
    pub statement: String,
    /// The rationale behind the proposal, when supplied.
    pub rationale: Option<String>,
    /// Alternatives considered and rejected, when supplied.
    #[serde(default)]
    pub alternatives_considered: Vec<String>,
}

/// A runner's proposed next action after producing a result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationProposal {
    /// An open, domain-extensible continuation kind (e.g. `verify`,
    /// `continue`, `rotate`).
    #[serde(rename = "type")]
    pub kind: String,
    /// Additional free-form detail about the proposed continuation.
    pub detail: Option<String>,
}
