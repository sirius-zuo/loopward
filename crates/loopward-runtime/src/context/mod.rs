//! Deterministic context hydration: turning a Checkpoint, a Work Unit, a
//! runner's declared capabilities, and a token budget into a bounded,
//! auditable Boot Package Manifest (`docs/04-context-lifecycle.md`).
//!
//! This is the Context Manager's core: a first-class component, not a helper
//! inside the Session Manager (`docs/01-system-architecture.md` §5). Phase
//! One does not use a model to decide what context to load — selection is a
//! fixed, versioned algorithm, so the same inputs under the same
//! `context_rule_version` always reproduce the same manifest, byte for byte.
//!
//! Three guarantees shape everything here:
//!
//! * **Pinned constraints survive losslessly.** A required pinned item is
//!   resolved, digest-checked, and admitted before any optional material is
//!   even priced, and it may never enter the package as a lossy summary.
//! * **A failed hydration starts nothing.** Every failure in
//!   `docs/04-context-lifecycle.md` §12 — missing pinned state, an
//!   unresolvable required resource, a digest mismatch, required content
//!   larger than the budget — is raised before selection, so no Work Unit
//!   Attempt can begin against a broken context.
//! * **Context is assembled for the current Work Unit.** The package is
//!   built from the current Checkpoint's projected candidates, never by
//!   replaying Loop history, so a late-stage Work Unit's Boot Package is
//!   bounded by the work in front of it rather than by everything behind it.
//!
//! # Candidate generation
//!
//! `loopward_core::Checkpoint` is Core's *identity and versioning* view of a
//! Checkpoint; it deliberately carries no pinned constraints, decisions, or
//! observations, because Core does not interpret Checkpoint content. The
//! hydrator therefore takes an already-projected [`ContextCandidate`] list
//! on the [`HydrationRequest`] — the Checkpoint Manager's projection of the
//! stored Checkpoint document (`docs/schemas/checkpoint-v0.1.yaml`: `pinned`,
//! `decisions.active`, `observations.verified`, `work`, `resources`) — and
//! performs exactly two derivations of its own:
//!
//! * every [`WorkUnit`] input is guaranteed to be required context — an
//!   uncovered input is projected into a required current-work candidate,
//!   and an input the projection covered at a lower priority has that
//!   candidate promoted to [`ContextPriority::Required`], so a declared
//!   input can never be dropped for budget reasons; and
//! * every [`WorkUnit`] constraint must be covered by a required pinned
//!   candidate, or hydration fails.
//!
//! # Manifest items and exclusion reasoning
//!
//! [`BootPackageManifest::items`] carries the *selected* items — what the
//! runner-specific materialization is built from. Exclusion reasoning for
//! every candidate that was considered and not selected is returned
//! alongside it on [`HydrationReport::considered`], which keeps the wire
//! manifest a description of the actual Boot Package while still recording
//! an inclusion or exclusion reason, and an estimation method, for every
//! candidate the algorithm looked at.

mod budget;
mod hydrator;
mod manifest;

use std::sync::Arc;

use loopward_api::context::{
    BootPackageManifest, ContextBudget, ContextClass, ContextPriority, EstimateMethod, Fidelity,
    Sensitivity, Validity, VerificationStatus,
};
use loopward_api::ids::EpisodeId;
use loopward_api::resource::ResourceRef;
use loopward_api::runner::RunnerCapabilities;
use loopward_api::work_unit::WorkUnit;
use loopward_core::Checkpoint;

use crate::ports::{ResourceBroker, ResourceError};

pub use budget::{REFERENCE_ONLY_TOKENS, available_boot_tokens, heuristic_token_estimate};
pub use hydrator::SelectionBucket;
pub use manifest::{BOOT_PACKAGE_MANIFEST_SCHEMA_VERSION, checkpoint_ref_for};

/// One item considered for a Boot Package, before payload materialization.
///
/// This is the hydrator's input form of [`loopward_api::context::ContextItem`]:
/// it carries the extra selection metadata the algorithm needs but that never
/// ships on the wire (a `verification_status`, and estimates that may still
/// be absent), and it omits the fields the hydrator computes itself (the
/// content digest, and the final fidelity and token estimate).
#[derive(Debug, Clone, PartialEq)]
pub struct ContextCandidate {
    /// A stable, manifest-scoped identifier. Candidates sharing a selection
    /// bucket *and* a [`ContextPriority`] are ordered lexicographically on
    /// this field, so it must be unique within one request.
    pub id: String,
    /// The kind of underlying material (e.g. `constraint`, `decision`).
    pub kind: String,
    /// Where this candidate's content comes from. When
    /// [`ResourceRef::digest`] is set, the content is treated as immutable
    /// and the resolved bytes must hash to it.
    pub source: ResourceRef,
    /// Why this candidate is being considered.
    pub classification: ContextClass,
    /// How urgently this candidate must be included.
    pub priority: ContextPriority,
    /// The fidelity this candidate would enter the package at. A required
    /// pinned candidate must be [`Fidelity::Verbatim`] or
    /// [`Fidelity::Structured`].
    pub fidelity: Fidelity,
    /// How sensitive this candidate's content is. Carried through untouched.
    pub sensitivity: Sensitivity,
    /// Whether this candidate still reflects current authoritative state.
    /// Optional material that is stale or superseded is excluded.
    pub validity: Validity,
    /// Whether the observation this candidate carries has been verified.
    /// Only a verified observation reaches
    /// [`SelectionBucket::VerifiedObservation`].
    pub verification_status: Option<VerificationStatus>,
    /// A caller-supplied token estimate. When absent, the hydrator resolves
    /// the content and falls back to [`heuristic_token_estimate`].
    pub estimated_tokens: Option<u64>,
    /// How `estimated_tokens` was produced.
    pub estimate_method: Option<EstimateMethod>,
    /// Confidence in `estimated_tokens`, in `[0.0, 1.0]`.
    pub estimate_confidence: Option<f32>,
    /// A short, human-readable reason this candidate is being considered.
    pub selection_reason: String,
}

impl ContextCandidate {
    /// A candidate with conservative defaults: verbatim fidelity, internal
    /// sensitivity, current validity, and no token estimate (which makes the
    /// hydrator resolve and measure the content itself).
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        kind: impl Into<String>,
        source: ResourceRef,
        classification: ContextClass,
        priority: ContextPriority,
    ) -> Self {
        Self {
            id: id.into(),
            kind: kind.into(),
            source,
            classification,
            priority,
            fidelity: Fidelity::Verbatim,
            sensitivity: Sensitivity::Internal,
            validity: Validity::Current,
            verification_status: None,
            estimated_tokens: None,
            estimate_method: None,
            estimate_confidence: None,
            selection_reason: String::new(),
        }
    }
}

/// Everything one Boot Package hydration is computed from.
///
/// The manifest's `loop_id` and `checkpoint_ref` are derived from
/// `checkpoint` rather than supplied separately, so a manifest can never
/// claim provenance from a Checkpoint it was not built from.
#[derive(Debug, Clone)]
pub struct HydrationRequest {
    /// The Checkpoint this Boot Package is hydrated from.
    pub checkpoint: Checkpoint,
    /// The Episode this Boot Package is being hydrated for.
    pub episode_id: EpisodeId,
    /// The Runtime Session generation this Boot Package targets.
    pub session_generation: u32,
    /// The Work Unit context is being assembled for.
    pub work_unit: WorkUnit,
    /// The declared capabilities of the runner that will receive the Boot
    /// Package. Only truthfully declared capabilities are used: on-demand
    /// references are only left unmaterialized for a runner that declared
    /// scoped resource fetch.
    pub capabilities: RunnerCapabilities,
    /// The resolved token budget this package must fit.
    pub budget: ContextBudget,
    /// The version of the selection rules being applied. Recorded in the
    /// manifest and covered by its digest, so a rule change is visible as a
    /// digest change.
    pub context_rule_version: String,
    /// The Checkpoint Manager's projection of candidate context items.
    pub candidates: Vec<ContextCandidate>,
}

/// Why a considered candidate did not enter the Boot Package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExclusionReason {
    /// The remaining budget could not accommodate the candidate.
    BudgetExhausted,
    /// The candidate was explicitly replaced by newer material.
    Superseded,
    /// The candidate no longer reflects current authoritative state.
    Stale,
    /// The candidate's content could not be resolved. Optional material is
    /// dropped rather than failing hydration.
    Unresolvable,
    /// The candidate's resolved bytes did not hash to its declared immutable
    /// digest.
    DigestMismatch,
}

impl ExclusionReason {
    /// The stable `snake_case` label used in diagnostics and telemetry.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::BudgetExhausted => "budget_exhausted",
            Self::Superseded => "superseded",
            Self::Stale => "stale",
            Self::Unresolvable => "unresolvable",
            Self::DigestMismatch => "digest_mismatch",
        }
    }
}

/// What the algorithm decided about one considered candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CandidateDisposition {
    /// The candidate is in the manifest.
    Included,
    /// The candidate was considered and left out, for this reason.
    Excluded(ExclusionReason),
}

/// The record of one candidate the algorithm considered.
///
/// Produced for *every* candidate, selected or not, so a Boot Package can be
/// audited for what it left behind as well as what it carried.
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateOutcome {
    /// The candidate's identifier.
    pub id: String,
    /// The selection bucket the candidate was placed in.
    pub bucket: SelectionBucket,
    /// The token cost that was weighed against the budget.
    pub estimated_tokens: Option<u64>,
    /// How that cost was estimated.
    pub estimate_method: Option<EstimateMethod>,
    /// Whether the candidate made it in.
    pub disposition: CandidateDisposition,
}

/// The result of one hydration: the Boot Package Manifest, plus the
/// selection record for every candidate considered.
#[derive(Debug, Clone, PartialEq)]
pub struct HydrationReport {
    /// The Boot Package Manifest, carrying only the selected items.
    pub manifest: BootPackageManifest,
    /// Every candidate considered, in selection order.
    pub considered: Vec<CandidateOutcome>,
}

impl HydrationReport {
    /// Discards the selection record and keeps the manifest.
    #[must_use]
    pub fn into_manifest(self) -> BootPackageManifest {
        self.manifest
    }

    /// The total estimated token cost of the selected Boot Package.
    #[must_use]
    pub fn selected_tokens(&self) -> u64 {
        self.manifest
            .items
            .iter()
            .filter_map(|item| item.estimated_tokens)
            .sum()
    }

    /// The record for the candidate with this identifier, if it was
    /// considered.
    #[must_use]
    pub fn outcome(&self, id: &str) -> Option<&CandidateOutcome> {
        self.considered.iter().find(|outcome| outcome.id == id)
    }

    /// What was decided about this candidate.
    #[must_use]
    pub fn disposition(&self, id: &str) -> Option<&CandidateDisposition> {
        self.outcome(id).map(|outcome| &outcome.disposition)
    }

    /// Why this candidate was left out, if it was.
    #[must_use]
    pub fn exclusion_reason(&self, id: &str) -> Option<ExclusionReason> {
        match self.disposition(id) {
            Some(CandidateDisposition::Excluded(reason)) => Some(*reason),
            _ => None,
        }
    }

    /// The selection bucket this candidate was placed in.
    #[must_use]
    pub fn bucket(&self, id: &str) -> Option<SelectionBucket> {
        self.outcome(id).map(|outcome| outcome.bucket)
    }
}

/// A hydration failure. Every variant is raised before any context is
/// selected or materialized, so a failed hydration never starts a Work Unit
/// Attempt (`docs/04-context-lifecycle.md` §12).
#[derive(Debug, thiserror::Error)]
pub enum HydrationError {
    /// The budget's reservations exceed its hard input limit, leaving no
    /// well-defined space for boot context.
    #[error("invalid context budget: reservations exceed the hard input limit")]
    InvalidBudget,
    /// A constraint the Work Unit declares has no required pinned candidate,
    /// so the pinned state this package would carry is incomplete.
    #[error("required pinned state is missing for constraint {reference}")]
    MissingPinnedConstraint {
        /// The unrepresented constraint reference.
        reference: String,
    },
    /// A required candidate's content could not be resolved.
    #[error("required context item {id} could not be resolved: {uri}")]
    MissingRequiredResource {
        /// The unresolvable candidate's identifier.
        id: String,
        /// The URI that failed to resolve.
        uri: String,
    },
    /// A required candidate declared an immutable digest that its resolved
    /// bytes do not hash to.
    #[error("digest mismatch for required context item {id}: expected {expected}, got {computed}")]
    DigestMismatch {
        /// The diverging candidate's identifier.
        id: String,
        /// The digest the candidate declared.
        expected: String,
        /// The digest its resolved bytes actually hash to.
        computed: String,
    },
    /// A required pinned candidate would enter the package lossily. A lossy
    /// summary can never replace a pinned constraint.
    #[error("required pinned context item {id} cannot enter the boot package as {fidelity}")]
    LossyPinnedFidelity {
        /// The offending candidate's identifier.
        id: String,
        /// The lossy fidelity it declared.
        fidelity: &'static str,
    },
    /// Required content alone does not fit the boot budget, so no valid Boot
    /// Package exists for this Work Unit and budget.
    #[error(
        "required context needs {required_tokens} tokens but only {available_tokens} are available"
    )]
    RequiredContentExceedsBudget {
        /// The total estimated cost of required content.
        required_tokens: u64,
        /// The tokens the Boot Package may occupy.
        available_tokens: u64,
    },
    /// Two candidates share an identifier, which would make tie-breaking —
    /// and therefore the manifest — non-deterministic.
    #[error("duplicate context candidate id: {id}")]
    DuplicateCandidateId {
        /// The duplicated identifier.
        id: String,
    },
    /// The resource backend failed while resolving required content.
    #[error("resource backend failed for required context item {id}")]
    Resource {
        /// The candidate being resolved.
        id: String,
        /// The underlying broker failure.
        #[source]
        source: ResourceError,
    },
    /// The manifest could not be serialized, leaving its integrity binding
    /// undefined.
    #[error("boot package manifest could not be serialized")]
    Serialization {
        /// The underlying serialization failure.
        #[source]
        source: serde_json::Error,
    },
}

/// Builds bounded, auditable Boot Packages from durable state.
///
/// The hydrator holds only a [`ResourceBroker`]: it reads content through
/// the same port as everything else in this crate and never touches a file,
/// a socket, or a clock itself.
#[derive(Clone)]
pub struct ContextHydrator {
    broker: Arc<dyn ResourceBroker>,
}

impl ContextHydrator {
    /// Builds a hydrator that resolves context content through `broker`.
    #[must_use]
    pub fn new(broker: Arc<dyn ResourceBroker>) -> Self {
        Self { broker }
    }

    /// Hydrates a Boot Package Manifest, together with the selection record
    /// for every candidate considered.
    ///
    /// # Errors
    ///
    /// Returns a [`HydrationError`] when the budget is invalid, required
    /// pinned state is missing or divergent, a required resource cannot be
    /// resolved, or required content does not fit the budget. In every case
    /// the failure happens before any context is selected.
    pub async fn hydrate(
        &self,
        request: HydrationRequest,
    ) -> Result<HydrationReport, HydrationError> {
        hydrator::hydrate(self.broker.as_ref(), request).await
    }

    /// Hydrates a Boot Package Manifest, discarding the selection record.
    ///
    /// # Errors
    ///
    /// As [`Self::hydrate`].
    pub async fn hydrate_manifest(
        &self,
        request: HydrationRequest,
    ) -> Result<BootPackageManifest, HydrationError> {
        self.hydrate(request)
            .await
            .map(HydrationReport::into_manifest)
    }
}

impl std::fmt::Debug for ContextHydrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextHydrator").finish_non_exhaustive()
    }
}
