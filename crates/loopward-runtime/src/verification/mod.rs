//! Verification: the machinery that makes "a runner result is not accepted
//! merely because the runner reports success"
//! (`docs/06-verification-and-recovery.md` §1) an executable rule rather than
//! an aspiration.
//!
//! A [`VerificationPlan`] exists before execution and is versioned. It names
//! the exact [`VerificationTarget`] being judged, an ordered set of
//! [`VerificationGateSpec`]s, an [`AggregationPolicy`], and a
//! [`RemediationPolicy`]. [`VerificationEngine::evaluate`] runs the plan's
//! gates in the order `docs/06` §4 recommends — cheap and conclusive before
//! expensive and semantic — and folds their [`VerificationFinding`]s into one
//! [`loopward_core::VerificationOutcome`]. Only
//! [`loopward_core::VerificationOutcome::Accepted`] may produce an accepted
//! Work Unit completion mutation.
//!
//! Three rules shape every type here:
//!
//! * **A runner cannot weaken its own verification plan.** The plan is an
//!   input to verification, never something the result carries: nothing in
//!   [`GateRequest`] lets a [`loopward_api::work_unit::WorkUnitResult`]
//!   remove, relax, or downgrade a gate. A plan with no required gate is
//!   rejected outright ([`VerificationError::NoRequiredGate`]) rather than
//!   vacuously accepting everything.
//! * **Findings bind to an exact target.** Every finding records the
//!   [`VerificationTarget::binding_digest`] it was evaluated against — the
//!   composite of the result digest, the resource digests, the workspace
//!   revision, the plan version, and the applicable constraint digest
//!   (`docs/06` §6). When that composite changes, prior findings become
//!   [`GateOutcome::Stale`], and a stale pass cannot satisfy an acceptance
//!   criterion.
//! * **Failures are not averaged away.** Aggregation is driven by required
//!   gates only. An advisory gate may record a failing finding — it stays
//!   durable and visible — but it can never reject a run, and it can never
//!   hide a required gate's pass (`docs/06` §7).
//!
//! Loop completion is a separate decision from Work Unit acceptance
//! (`docs/06` §11) and lives in [`assess_completion`], a synchronous, pure
//! function over already-computed findings.

mod engine;
mod gate;

use std::collections::BTreeMap;
use std::fmt;

use loopward_api::ids::{AttemptId, LoopId, Timestamp, WorkUnitId};
use loopward_api::resource::{ResourceRef, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::context::content_digest;
use crate::ports::ResourceError;

pub use engine::{
    CompletionAssessment, CompletionObstacle, EvidenceGap, VerificationEngine, VerificationRequest,
    VerificationRun, assess_completion,
};
pub use gate::{
    AgenticGate, AgenticGateConfig, DeterministicGate, DeterministicGateConfig, GateConfig,
    GateRequest, HumanGate, HumanGateConfig, IntegrityGate, IntegrityGateConfig, PolicyGate,
    PolicyGateConfig, SchemaGate, SchemaGateConfig, VerificationGate,
};

// ---------------------------------------------------------------------------
// Small opaque identifier and digest newtypes
// ---------------------------------------------------------------------------

macro_rules! define_string_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wraps a raw identifier value.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the underlying identifier text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

define_string_id!(
    VerificationPlanId,
    "The identity of one versioned Verification Plan, e.g. `verification-plan-001`."
);
define_string_id!(
    GateId,
    "The plan-scoped identity of one gate, e.g. `unit-tests`. Findings are traceable back to the gate that produced them."
);
define_string_id!(
    CriterionId,
    "The identity of one success or acceptance criterion, e.g. `criterion-001`, as carried by `docs/schemas/checkpoint-v0.1.yaml` `goal.success_criteria`."
);
define_string_id!(
    Digest,
    "A content or binding digest in `sha256:<hex>` form. Binds a finding to the exact bytes and revisions it was evaluated against."
);

// ---------------------------------------------------------------------------
// Gate kinds and enforcement
// ---------------------------------------------------------------------------

/// The gate types enumerated by `docs/06-verification-and-recovery.md` §4.
///
/// The declaration order below *is* the recommended evaluation order, and
/// [`EVALUATION_ORDER`](VerificationGateKind::EVALUATION_ORDER) makes it
/// explicit: cheap and conclusive gates run before expensive semantic ones,
/// so a schema violation is never paid for with an agentic review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationGateKind {
    /// The result satisfies its declared output contract.
    Schema,
    /// Declared digests and provenance hold against the actual content.
    Integrity,
    /// A configured, reproducible check command agrees.
    Deterministic,
    /// Domain and deployment policy admit the result.
    Policy,
    /// Semantic review in a distinct, bounded context (`docs/06` §8).
    Agentic,
    /// Human approval (`docs/06` §9).
    Human,
}

impl VerificationGateKind {
    /// The recommended evaluation order from `docs/06` §4.
    pub const EVALUATION_ORDER: &'static [Self] = &[
        Self::Schema,
        Self::Integrity,
        Self::Deterministic,
        Self::Policy,
        Self::Agentic,
        Self::Human,
    ];

    /// This kind's position in [`Self::EVALUATION_ORDER`]. Lower runs first.
    #[must_use]
    pub fn evaluation_rank(self) -> usize {
        Self::EVALUATION_ORDER
            .iter()
            .position(|kind| *kind == self)
            .unwrap_or(usize::MAX)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Integrity => "integrity",
            Self::Deterministic => "deterministic",
            Self::Policy => "policy",
            Self::Agentic => "agentic",
            Self::Human => "human",
        }
    }
}

/// Whether a gate's verdict binds the aggregate outcome.
///
/// `docs/06` §7: "Optional advisory gates may produce non-blocking findings,
/// but they must be labelled as advisory."
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateEnforcement {
    /// The gate must pass for the run to be accepted.
    Required,
    /// The gate's verdict is recorded but never blocks acceptance.
    Advisory,
}

impl GateEnforcement {
    /// Whether a non-passing finding from this gate blocks acceptance.
    #[must_use]
    pub fn is_required(self) -> bool {
        matches!(self, Self::Required)
    }
}

/// One gate's verdict (`docs/06` §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateOutcome {
    /// The gate's criterion holds against the bound target.
    Passed,
    /// The gate's criterion does not hold.
    Failed,
    /// The gate could not reach a conclusion.
    Inconclusive,
    /// The gate needs external input (human approval, policy decision)
    /// before it can conclude.
    Suspended,
    /// The target moved after this finding was recorded, so it no longer
    /// binds.
    Stale,
}

impl GateOutcome {
    /// Whether this verdict permits acceptance when the gate is required.
    #[must_use]
    pub fn is_passing(self) -> bool {
        matches!(self, Self::Passed)
    }

    /// The stable `snake_case` label used in diagnostics and errors.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Inconclusive => "inconclusive",
            Self::Suspended => "suspended",
            Self::Stale => "stale",
        }
    }
}

// ---------------------------------------------------------------------------
// Target binding
// ---------------------------------------------------------------------------

/// The exact thing a verification run judges (`docs/06` §6).
///
/// Verification binds to *all* of it — resource digests, workspace or dataset
/// revision, Work Unit result digest, verification-plan version, applicable
/// constraint digest — and [`Self::binding_digest`] collapses that binding
/// into one comparable value. A finding is current exactly while its
/// recorded digest still equals the current target's binding digest; when it
/// does not, the finding is stale by construction rather than by anyone
/// remembering to invalidate it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationTarget {
    /// The Work Unit whose result is being verified.
    pub work_unit_id: WorkUnitId,
    /// The digest of the collected Work Unit result document.
    pub result_digest: Digest,
    /// Every resource the verification binds to, keyed by URI. A
    /// [`BTreeMap`] rather than a map with arbitrary iteration order so the
    /// binding digest is reproducible.
    #[serde(default)]
    pub resource_digests: BTreeMap<String, Digest>,
    /// The workspace or dataset revision the result was produced against.
    pub workspace_revision: Option<String>,
    /// The verification-plan version this target was bound under.
    pub plan_version: u64,
    /// The digest of the applicable constraint set.
    pub constraint_digest: Option<Digest>,
}

impl VerificationTarget {
    /// The composite digest of every binding above.
    ///
    /// Each component is length-framed before hashing, so two different
    /// bindings can never hash alike by concatenating to the same byte
    /// string.
    #[must_use]
    pub fn binding_digest(&self) -> Digest {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"loopward.verification-target/v0.1");
        frame(&mut bytes, self.work_unit_id.to_string().as_bytes());
        frame(&mut bytes, self.result_digest.as_str().as_bytes());
        bytes.extend_from_slice(
            &u64::try_from(self.resource_digests.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for (uri, digest) in &self.resource_digests {
            frame(&mut bytes, uri.as_bytes());
            frame(&mut bytes, digest.as_str().as_bytes());
        }
        frame(
            &mut bytes,
            self.workspace_revision.as_deref().unwrap_or("").as_bytes(),
        );
        bytes.extend_from_slice(&self.plan_version.to_be_bytes());
        frame(
            &mut bytes,
            self.constraint_digest
                .as_ref()
                .map_or("", Digest::as_str)
                .as_bytes(),
        );
        Digest::new(content_digest(&bytes))
    }
}

/// Appends `value` to `buffer`, prefixed by its big-endian length.
fn frame(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    buffer.extend_from_slice(value);
}

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

/// How a run's findings fold into one outcome (`docs/06` §7).
///
/// Deliberately small. Phase One uses explicit required gates, and failures
/// are never averaged away by confidence scores, so there is no weighting or
/// scoring knob here to turn — only how eagerly the engine stops, and whether
/// an unreachable conclusion is treated as a rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregationPolicy {
    /// Stop evaluating later, more expensive gates once a required gate has
    /// failed. The run is already rejected at that point, so paying for an
    /// agentic review buys nothing.
    pub stop_on_required_failure: bool,
    /// Treat a required gate's [`GateOutcome::Inconclusive`] as a rejection
    /// rather than leaving the run inconclusive. Domains where "we could not
    /// tell" must be handled as "no" set this.
    pub treat_inconclusive_as_rejection: bool,
}

impl Default for AggregationPolicy {
    fn default() -> Self {
        Self {
            stop_on_required_failure: true,
            treat_inconclusive_as_rejection: false,
        }
    }
}

/// The bounds a rejected or inconclusive result is remediated under
/// (`docs/06` §10).
///
/// This type is *carried* by the plan and *recorded* on every finding's
/// lineage; nothing in this module acts on it. Remediation itself —
/// proposing a new Work Unit, opening a new Episode, deciding when repeated
/// findings without information gain must suspend — is the Loop Controller's
/// decision, and it reads this policy alongside the findings produced here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemediationPolicy {
    /// How many remediation rounds this verification may drive before the
    /// Loop must suspend or fail. `None` means the Loop's own attempt budget
    /// is the only bound.
    pub max_remediation_rounds: Option<u32>,
    /// Whether remediation must open a new Episode rather than reusing the
    /// failed one. `docs/06` §10: a rejected result does not silently
    /// restart the same Work Unit.
    pub require_new_episode: bool,
    /// How many consecutive rounds may repeat an identical finding code
    /// before the Loop suspends for lack of information gain (`docs/06`
    /// §15).
    pub suspend_after_repeated_finding_code: Option<u32>,
}

impl Default for RemediationPolicy {
    fn default() -> Self {
        Self {
            max_remediation_rounds: None,
            require_new_episode: true,
            suspend_after_repeated_finding_code: Some(2),
        }
    }
}

/// One gate as the plan declares it: identity, kind, enforcement, the
/// criterion it supports, and the configuration the gate implementation
/// evaluates against.
///
/// The configuration lives on the *spec*, not on the gate implementation, so
/// one registered implementation per kind serves every spec of that kind in
/// every plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerificationGateSpec {
    /// This gate's plan-scoped identity.
    pub id: GateId,
    /// Which gate type evaluates this spec.
    pub kind: VerificationGateKind,
    /// Whether this gate binds the aggregate outcome.
    pub enforcement: GateEnforcement,
    /// The success criterion this gate's verdict supports, when it supports
    /// one. Findings carry it forward so [`assess_completion`] can tell which
    /// criteria are backed by current evidence.
    pub criterion: Option<CriterionId>,
    /// The gate-kind-specific configuration.
    pub config: GateConfig,
}

/// The versioned plan that exists before execution (`docs/06` §3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerificationPlan {
    /// This plan's identity.
    pub id: VerificationPlanId,
    /// This plan's version. Part of the target binding, so a plan revision
    /// invalidates findings made under the previous version.
    pub version: u64,
    /// The exact target this plan judges.
    pub target: VerificationTarget,
    /// The gates to evaluate. Declaration order does not decide evaluation
    /// order: the engine sorts by [`VerificationGateKind::evaluation_rank`].
    pub gates: Vec<VerificationGateSpec>,
    /// How findings fold into one outcome.
    pub aggregation: AggregationPolicy,
    /// The bounds remediation runs under, for the Loop Controller.
    pub remediation_policy: RemediationPolicy,
}

impl VerificationPlan {
    /// Whether this plan declares at least one required gate.
    ///
    /// A plan without one would accept every result vacuously, which is
    /// exactly the "runner-reported success bypasses verification" failure
    /// `docs/06` §16 forbids.
    #[must_use]
    pub fn has_required_gate(&self) -> bool {
        self.gates.iter().any(|gate| gate.enforcement.is_required())
    }
}

// ---------------------------------------------------------------------------
// Findings
// ---------------------------------------------------------------------------

/// Where a finding came from, so a rejection is traceable and remediable.
///
/// Beyond the `docs/06` §5 sketch, but required by §10: remediation must
/// establish "lineage to the failed verification", and it cannot do that from
/// a gate identifier alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindingLineage {
    /// The Loop this finding belongs to.
    pub loop_id: LoopId,
    /// The Work Unit whose result was judged.
    pub work_unit_id: WorkUnitId,
    /// The Attempt that produced the judged result.
    pub attempt_id: AttemptId,
    /// The plan this finding was produced under.
    pub plan_id: VerificationPlanId,
    /// The plan version this finding was produced under.
    pub plan_version: u64,
    /// The committed Checkpoint version the Loop stood at when this finding
    /// was recorded. [`assess_completion`] uses it to tell current evidence
    /// from evidence the Loop has since moved past.
    pub checkpoint_version: u64,
}

/// A structured request that suspends the Loop pending external input
/// (`docs/06` §9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRequest {
    /// A stable, machine-readable request code.
    pub code: String,
    /// The prompt shown to the responding principal.
    pub prompt: String,
    /// The schema the response must satisfy.
    pub expected_schema: SchemaRef,
    /// The evidence the responder needs in order to decide.
    #[serde(default)]
    pub evidence: Vec<ResourceRef>,
}

/// One durable, evidence-traceable gate verdict (`docs/06` §5).
///
/// The `docs/06` §5 sketch carries `gate_id`, `outcome`, `code`, `statement`,
/// `evidence`, and `target_digest`. The remaining fields are what the rest of
/// §6-§11 needs from a *durable* finding: which kind of gate produced it and
/// whether it was required (so aggregation never has to consult the plan
/// again), which criterion it supports (so the completion gate can tell
/// satisfied criteria from unsupported ones), its lineage (so remediation can
/// be related to the failure it repairs), any input request that suspended
/// it, and when it was observed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerificationFinding {
    /// The gate that produced this verdict.
    pub gate_id: GateId,
    /// The kind of gate that produced it.
    pub kind: VerificationGateKind,
    /// Whether that gate was required or advisory.
    pub enforcement: GateEnforcement,
    /// The verdict.
    pub outcome: GateOutcome,
    /// A stable, machine-readable finding code (e.g.
    /// `verification.schema.missing_field`). Repeated identical codes are one
    /// of the no-progress signals in `docs/06` §15.
    pub code: String,
    /// A human-readable statement of what was checked and what was found.
    pub statement: String,
    /// The evidence supporting this verdict.
    #[serde(default)]
    pub evidence: Vec<ResourceRef>,
    /// The [`VerificationTarget::binding_digest`] this verdict was evaluated
    /// against.
    pub target_digest: Digest,
    /// The success criterion this verdict supports, when it supports one.
    pub criterion: Option<CriterionId>,
    /// Where this finding came from.
    pub lineage: FindingLineage,
    /// The structured request that suspended this gate, when it suspended.
    pub input_request: Option<InputRequest>,
    /// When this verdict was recorded.
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: Timestamp,
}

impl VerificationFinding {
    /// Whether this finding still binds to `target`.
    ///
    /// A finding is current exactly while the target it was evaluated against
    /// is still the target in force. This is the whole of `docs/06` §6: no
    /// separate invalidation pass, no "someone must remember to mark it".
    #[must_use]
    pub fn is_current_for(&self, target: &VerificationTarget) -> bool {
        self.outcome != GateOutcome::Stale && self.target_digest == target.binding_digest()
    }

    /// Whether this finding, taken alone, is a required verdict that did not
    /// pass.
    ///
    /// Only required gates block, and only when they did not pass. An
    /// advisory failure is durable and visible but never blocking
    /// (`docs/06` §7).
    ///
    /// This is a property of one finding in isolation: it says nothing about
    /// whether a *later* finding has since superseded this one. A caller
    /// looking at a durable, accumulated list — such as
    /// [`VerificationRun::findings`], which carries every prior run's
    /// findings forward — must decide for itself whether a superseded
    /// finding still matters. The engine's own aggregation step never sees
    /// one: it consults only the findings one run just produced. In
    /// contrast, [`assess_completion`] does look across the whole
    /// accumulated record, and treats a finding a current one has superseded
    /// as inert rather than as still blocking.
    #[must_use]
    pub fn is_blocking(&self) -> bool {
        self.enforcement.is_required() && !self.outcome.is_passing()
    }

    /// This finding with its verdict replaced by [`GateOutcome::Stale`],
    /// preserving its code, statement, evidence, and original target binding
    /// so the superseded verdict stays auditable.
    #[must_use]
    pub fn superseded(mut self) -> Self {
        self.outcome = GateOutcome::Stale;
        self
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// An operation error raised while running a verification plan.
///
/// Deliberately distinct from a criterion failure
/// (`docs/01-system-architecture.md` §8: the Verification Engine "separates
/// operation errors from criterion failures"). A gate that decides its
/// criterion does not hold returns `Ok` with a
/// [`GateOutcome::Failed`] finding; a gate that could not be *run* at all
/// returns one of these.
#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    /// The plan declares no required gate, so it could only ever accept
    /// vacuously.
    #[error("verification plan {plan} v{version} declares no required gate")]
    NoRequiredGate {
        /// The offending plan.
        plan: VerificationPlanId,
        /// The offending plan's version.
        version: u64,
    },
    /// No gate implementation is registered for a kind the plan uses.
    #[error("no gate implementation registered for kind `{kind}` (gate {gate})")]
    UnsupportedGateKind {
        /// The gate whose kind could not be resolved.
        gate: GateId,
        /// The unregistered kind.
        kind: &'static str,
    },
    /// A gate spec's configuration does not match its declared kind.
    #[error("gate {gate} declares kind `{kind}` but carries `{config}` configuration")]
    ConfigMismatch {
        /// The misconfigured gate.
        gate: GateId,
        /// The kind the spec declared.
        kind: &'static str,
        /// The kind the configuration belongs to.
        config: &'static str,
    },
    /// A gate could not resolve content it needed to reach a verdict.
    #[error("gate {gate} could not resolve verification content")]
    Resource {
        /// The gate that needed the content.
        gate: GateId,
        /// The underlying broker failure.
        #[source]
        source: ResourceError,
    },
    /// A deterministic gate's check command could not be executed at all.
    /// Distinct from the command running and failing, which is a
    /// [`GateOutcome::Failed`] finding.
    #[error("gate {gate} could not execute check command `{program}`")]
    CommandNotExecutable {
        /// The gate whose command could not be spawned.
        gate: GateId,
        /// The program that could not be spawned.
        program: String,
        /// The underlying spawn failure.
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::{Digest, VerificationGateKind, VerificationTarget};
    use loopward_api::ids::WorkUnitId;
    use std::collections::BTreeMap;

    fn target() -> VerificationTarget {
        VerificationTarget {
            work_unit_id: WorkUnitId::from_uuid(uuid::Uuid::from_u128(1)),
            result_digest: Digest::new("sha256:aa"),
            resource_digests: BTreeMap::from([("a".to_owned(), Digest::new("sha256:bb"))]),
            workspace_revision: Some("rev-1".to_owned()),
            plan_version: 1,
            constraint_digest: Some(Digest::new("sha256:cc")),
        }
    }

    #[test]
    fn binding_digest_is_stable_for_an_unchanged_target() {
        assert_eq!(target().binding_digest(), target().binding_digest());
    }

    #[test]
    fn every_binding_component_changes_the_digest() {
        let base = target().binding_digest();

        let mut result_changed = target();
        result_changed.result_digest = Digest::new("sha256:ab");
        assert_ne!(result_changed.binding_digest(), base);

        let mut resource_changed = target();
        resource_changed
            .resource_digests
            .insert("a".to_owned(), Digest::new("sha256:bc"));
        assert_ne!(resource_changed.binding_digest(), base);

        let mut workspace_changed = target();
        workspace_changed.workspace_revision = Some("rev-2".to_owned());
        assert_ne!(workspace_changed.binding_digest(), base);

        let mut plan_changed = target();
        plan_changed.plan_version = 2;
        assert_ne!(plan_changed.binding_digest(), base);

        let mut constraint_changed = target();
        constraint_changed.constraint_digest = Some(Digest::new("sha256:cd"));
        assert_ne!(constraint_changed.binding_digest(), base);
    }

    #[test]
    fn framing_prevents_component_boundary_collisions() {
        let mut left = target();
        left.workspace_revision = Some("ab".to_owned());
        left.result_digest = Digest::new("c");

        let mut right = target();
        right.workspace_revision = Some("b".to_owned());
        right.result_digest = Digest::new("ac");

        assert_ne!(left.binding_digest(), right.binding_digest());
    }

    #[test]
    fn evaluation_rank_orders_cheap_gates_first() {
        let ranks: Vec<_> = VerificationGateKind::EVALUATION_ORDER
            .iter()
            .map(|kind| kind.evaluation_rank())
            .collect();
        assert_eq!(ranks, vec![0, 1, 2, 3, 4, 5]);
        assert!(
            VerificationGateKind::Schema.evaluation_rank()
                < VerificationGateKind::Deterministic.evaluation_rank()
        );
        assert!(
            VerificationGateKind::Deterministic.evaluation_rank()
                < VerificationGateKind::Agentic.evaluation_rank()
        );
    }
}
