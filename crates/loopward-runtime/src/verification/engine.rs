//! Ordered gate evaluation, aggregation, and the Loop completion gate
//! (`docs/06-verification-and-recovery.md` §4, §6, §7, §11).
//!
//! [`VerificationEngine::evaluate`] is the only place a
//! [`loopward_core::VerificationOutcome`] is produced. It runs a plan's gates
//! in the recommended order, carries prior findings forward — marking any
//! whose target has moved as [`GateOutcome::Stale`] — and folds the required
//! gates' verdicts into one outcome.
//!
//! [`assess_completion`] is deliberately a different shape: synchronous,
//! pure, and computed only from a committed [`Checkpoint`] and the findings
//! already recorded against it. Loop completion is a separate decision from
//! Work Unit acceptance (`docs/06` §11), it runs no gates of its own, and it
//! performs no I/O — which is what lets the Loop Controller evaluate it
//! inside the same transaction that would commit the terminal Checkpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use loopward_api::ids::{CheckpointId, LoopId, Timestamp};
use loopward_api::work_unit::WorkUnitResult;
use loopward_core::{Checkpoint, VerificationOutcome};

use crate::ports::ResourceBroker;

use super::gate::{
    AgenticGate, DeterministicGate, GateRequest, HumanGate, IntegrityGate, PolicyGate, SchemaGate,
    VerificationGate,
};
use super::{
    CriterionId, Digest, FindingLineage, GateId, GateOutcome, VerificationError,
    VerificationFinding, VerificationGateKind, VerificationPlan, VerificationPlanId,
};

// ---------------------------------------------------------------------------
// Requests and runs
// ---------------------------------------------------------------------------

/// Everything one verification run is computed from.
///
/// The plan comes from the Work Unit, not from the result: nothing a runner
/// returns can add, remove, relax, or reorder a gate (`docs/06` §3).
pub struct VerificationRequest {
    /// The versioned plan to run.
    pub plan: VerificationPlan,
    /// The collected runner result under judgement.
    pub result: Arc<WorkUnitResult>,
    /// The lineage stamped onto every finding this run produces.
    pub lineage: FindingLineage,
    /// Findings already recorded for this Work Unit. Any that no longer bind
    /// to the plan's target are carried forward as
    /// [`GateOutcome::Stale`] rather than silently dropped, so a superseded
    /// pass stays auditable and cannot be mistaken for current evidence.
    pub prior_findings: Vec<VerificationFinding>,
    /// The broker gates resolve content and finalize evidence through.
    pub resources: Arc<dyn ResourceBroker>,
    /// The observation time stamped onto every finding.
    pub observed_at: Timestamp,
}

/// The durable record of one verification run.
#[derive(Debug, Clone, PartialEq)]
pub struct VerificationRun {
    /// The aggregate outcome. Only
    /// [`VerificationOutcome::Accepted`] may produce an accepted Work Unit
    /// completion mutation.
    pub outcome: VerificationOutcome,
    /// Every finding, prior and fresh, in evaluation order. Advisory
    /// findings are included even when they failed: they are durable, and
    /// they never affect `outcome`.
    pub findings: Vec<VerificationFinding>,
    /// The target binding every fresh finding was evaluated against.
    pub target_digest: Digest,
    /// The plan this run executed.
    pub plan_id: VerificationPlanId,
    /// The plan version this run executed.
    pub plan_version: u64,
}

// ---------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------

/// Runs verification plans against collected results.
///
/// One gate implementation is registered per [`VerificationGateKind`];
/// per-spec configuration lives on the plan, so a single registry serves
/// every plan the runtime executes.
#[derive(Clone)]
pub struct VerificationEngine {
    gates: BTreeMap<VerificationGateKind, Arc<dyn VerificationGate>>,
}

impl std::fmt::Debug for VerificationEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerificationEngine")
            .field(
                "gates",
                &self
                    .gates
                    .keys()
                    .map(|kind| kind.label())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for VerificationEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl VerificationEngine {
    /// An engine with Phase One's gate implementations registered.
    #[must_use]
    pub fn new() -> Self {
        Self::empty()
            .with_gate(Arc::new(SchemaGate))
            .with_gate(Arc::new(IntegrityGate))
            .with_gate(Arc::new(DeterministicGate))
            .with_gate(Arc::new(PolicyGate))
            .with_gate(Arc::new(AgenticGate))
            .with_gate(Arc::new(HumanGate))
    }

    /// An engine with no gate implementations registered.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            gates: BTreeMap::new(),
        }
    }

    /// Registers `gate` for the kind it declares, replacing any previous
    /// implementation of that kind.
    #[must_use]
    pub fn with_gate(mut self, gate: Arc<dyn VerificationGate>) -> Self {
        self.gates.insert(gate.kind(), gate);
        self
    }

    /// Runs `request`'s plan and aggregates its findings.
    ///
    /// Gates run in [`VerificationGateKind::EVALUATION_ORDER`] regardless of
    /// the order the plan declares them in, so a cheap conclusive check is
    /// never paid for after an expensive semantic one.
    ///
    /// # Errors
    ///
    /// Returns [`VerificationError::NoRequiredGate`] when the plan could only
    /// accept vacuously, [`VerificationError::UnsupportedGateKind`] when the
    /// plan names a kind no implementation is registered for,
    /// [`VerificationError::ConfigMismatch`] when a spec's configuration does
    /// not match its declared kind, and whatever operation error a gate
    /// raises when it could not be run at all. A gate deciding its criterion
    /// does not hold is never an error.
    pub async fn evaluate(
        &self,
        request: VerificationRequest,
    ) -> Result<VerificationRun, VerificationError> {
        let VerificationRequest {
            plan,
            result,
            lineage,
            prior_findings,
            resources,
            observed_at,
        } = request;

        if !plan.has_required_gate() {
            return Err(VerificationError::NoRequiredGate {
                plan: plan.id,
                version: plan.version,
            });
        }

        let target_digest = plan.target.binding_digest();

        // Prior findings first: a finding whose target has moved is stale,
        // and the whole point of `docs/06` §6 is that this happens by
        // construction rather than by anyone remembering to invalidate it.
        let mut findings: Vec<VerificationFinding> = prior_findings
            .into_iter()
            .map(|finding| {
                if finding.is_current_for(&plan.target) {
                    finding
                } else {
                    finding.superseded()
                }
            })
            .collect();

        let mut ordered: Vec<_> = plan.gates.iter().enumerate().collect();
        ordered.sort_by_key(|(index, spec)| (spec.kind.evaluation_rank(), *index));

        let required_specs = plan
            .gates
            .iter()
            .filter(|spec| spec.enforcement.is_required())
            .count();
        let mut fresh = Vec::with_capacity(ordered.len());

        for (_, spec) in ordered {
            if spec.config.kind() != spec.kind {
                return Err(VerificationError::ConfigMismatch {
                    gate: spec.id.clone(),
                    kind: spec.kind.label(),
                    config: spec.config.kind().label(),
                });
            }
            let gate = self.gates.get(&spec.kind).ok_or_else(|| {
                VerificationError::UnsupportedGateKind {
                    gate: spec.id.clone(),
                    kind: spec.kind.label(),
                }
            })?;

            let finding = gate
                .evaluate(GateRequest {
                    spec: spec.clone(),
                    target: plan.target.clone(),
                    target_digest: target_digest.clone(),
                    result: Arc::clone(&result),
                    resources: Arc::clone(&resources),
                    lineage: lineage.clone(),
                    observed_at,
                })
                .await?;

            let blocked = finding.is_blocking();
            fresh.push(finding);
            if blocked && plan.aggregation.stop_on_required_failure {
                break;
            }
        }

        let outcome = aggregate(
            &fresh,
            &target_digest,
            required_specs,
            plan.aggregation.treat_inconclusive_as_rejection,
        );
        findings.extend(fresh);

        Ok(VerificationRun {
            outcome,
            findings,
            target_digest,
            plan_id: plan.id,
            plan_version: plan.version,
        })
    }
}

/// Folds *this run's freshly evaluated* gates' verdicts into one outcome
/// (`docs/06` §7).
///
/// ```text
/// Accepted
/// = every required gate this run evaluated passed
/// + no blocking finding from this run remains unresolved
/// + target bindings are current
/// ```
///
/// Deliberately scoped to `fresh` — the findings this evaluation just
/// produced — never the full accumulated [`VerificationRun::findings`],
/// which also carries every prior run's findings forward, including any
/// just superseded to [`GateOutcome::Stale`] by this very evaluation. A
/// superseded finding was already accounted for when *its own* run's
/// outcome was computed; it is not re-litigated by a later run, and it can
/// never count as an unresolved blocker here even though
/// [`VerificationFinding::is_blocking`] would (correctly) call it required
/// and non-passing in isolation. (Loop completion, which does look across
/// every accumulated finding, is a separate decision — see
/// [`assess_completion`].)
///
/// Advisory findings are not consulted: an advisory failure is durable and
/// visible but can neither reject a run nor hide a required pass. Failures are
/// never averaged away — the precedence below is strictly worst-first, with a
/// stale binding beating everything (nothing was really verified), a failure
/// beating a suspension (a definite "no" outranks a pending question), and a
/// suspension beating an inconclusive result (a named next action outranks
/// "we could not tell").
fn aggregate(
    fresh: &[VerificationFinding],
    target_digest: &Digest,
    required_specs: usize,
    treat_inconclusive_as_rejection: bool,
) -> VerificationOutcome {
    let mut stale = false;
    let mut failed = false;
    let mut suspended = false;
    let mut inconclusive = false;
    let mut required_evaluated = 0usize;

    for finding in fresh
        .iter()
        .filter(|finding| finding.enforcement.is_required())
    {
        required_evaluated += 1;
        if &finding.target_digest != target_digest {
            stale = true;
            continue;
        }
        match finding.outcome {
            GateOutcome::Passed => {}
            GateOutcome::Stale => stale = true,
            GateOutcome::Failed => failed = true,
            GateOutcome::Suspended => suspended = true,
            GateOutcome::Inconclusive => {
                if treat_inconclusive_as_rejection {
                    failed = true;
                } else {
                    inconclusive = true;
                }
            }
        }
    }

    if stale {
        VerificationOutcome::Stale
    } else if failed {
        VerificationOutcome::Rejected
    } else if suspended {
        VerificationOutcome::Suspended
    } else if inconclusive || required_evaluated < required_specs {
        // A required gate that never ran did not pass, so the run cannot be
        // accepted on the strength of the ones that did.
        VerificationOutcome::Inconclusive
    } else {
        VerificationOutcome::Accepted
    }
}

// ---------------------------------------------------------------------------
// Completion gate
// ---------------------------------------------------------------------------

/// Why a required success criterion is not currently satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EvidenceGap {
    /// A required gate for the criterion failed.
    Failed,
    /// A required gate for the criterion could not reach a conclusion.
    Inconclusive,
    /// A required gate for the criterion is awaiting external input.
    Suspended,
    /// The criterion's evidence no longer binds to its target.
    Stale,
    /// The criterion's evidence was recorded against an earlier Checkpoint,
    /// so it does not describe the state the Loop would terminate in.
    NotCurrent,
}

/// One reason the Loop may not terminate as complete (`docs/06` §11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionObstacle {
    /// Not one required finding currently supports completion. Completion is
    /// never granted by default: an absence of evidence is an obstacle, not a
    /// pass.
    NoRequiredEvidence,
    /// A required success criterion is not supported by current evidence.
    CriterionUnsupported {
        /// The unsupported criterion.
        criterion: CriterionId,
        /// Why its evidence does not support it.
        gap: EvidenceGap,
    },
    /// A required finding that supports no particular criterion is still
    /// unresolved.
    BlockingFinding {
        /// The gate that produced it.
        gate_id: GateId,
        /// Its finding code.
        code: String,
        /// Why it does not support completion.
        gap: EvidenceGap,
    },
    /// A finding belongs to a different Loop and cannot support this Loop's
    /// completion.
    ForeignFinding {
        /// The gate that produced it.
        gate_id: GateId,
        /// The Loop it actually belongs to.
        loop_id: LoopId,
    },
}

/// Whether a Loop may terminate as complete, and if not, why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionAssessment {
    /// Whether the Loop Controller may commit the terminal transition.
    pub permitted: bool,
    /// The Checkpoint this assessment was computed against.
    pub checkpoint_id: CheckpointId,
    /// That Checkpoint's version. Evidence recorded against any other
    /// version is not current.
    pub checkpoint_version: u64,
    /// The success criteria backed by current, passing, required evidence,
    /// in ascending criterion order.
    pub satisfied: Vec<CriterionId>,
    /// Every reason completion is not permitted. Empty exactly when
    /// `permitted` is `true`.
    pub obstacles: Vec<CompletionObstacle>,
}

/// Decides whether a whole Loop may terminate as complete (`docs/06` §11).
///
/// Synchronous and pure: it runs no gates, resolves no resources, and reads
/// no clock. It judges only what has already been established — the committed
/// `checkpoint` and the `findings` recorded against this Loop — which is what
/// makes it safe to evaluate inside the transaction that would commit the
/// terminal Checkpoint.
///
/// Completion requires, per `docs/06` §11:
///
/// * every required success criterion satisfied — a criterion is satisfied
///   only by a required finding that passed;
/// * that satisfaction supported by *current* evidence — the finding must
///   still bind to its target (not [`GateOutcome::Stale`]) and must have been
///   recorded at this Checkpoint version, not at one the Loop has since moved
///   past;
/// * no blocking finding left unresolved, including required findings that
///   support no particular criterion, such as a final integrity check;
/// * evidence that actually belongs to this Loop.
///
/// `findings` is expected to be exactly the durable, ever-growing record
/// [`VerificationRun::findings`] accumulates: every prior run's findings,
/// carried forward and marked [`GateOutcome::Stale`] once superseded, plus
/// every fresh one. That accumulation must never make completion *less*
/// stable over time — a criterion a *current* finding already satisfies stays
/// satisfied no matter how many stale or out-of-date findings for the same
/// criterion the Loop produced on the way there. Concretely: a finding that
/// is not current (its Checkpoint version predates `checkpoint.version`, or
/// its target has moved and it was marked [`GateOutcome::Stale`]) is inert
/// whenever a current finding already speaks for that criterion — it can
/// neither satisfy it nor block it. It becomes an obstacle only when it is
/// the *sole* evidence the criterion has ever had. A current finding that
/// failed, in contrast, always blocks its criterion outright: live evidence
/// is never overridden by older evidence, and a live failure is never
/// averaged away by some other gate's pass.
///
/// Advisory findings are never obstacles (`docs/06` §7). Absence of evidence
/// is: a Loop with no required findings at all is not complete, it is
/// unverified.
///
/// # Scope
///
/// This function can only judge criteria for which `findings` supplies at
/// least one finding. It has no independent notion of *which* criteria a
/// Checkpoint's goal declares as required: `loopward_core`'s [`Checkpoint`]
/// is deliberately Core's identity-and-versioning view only (id, `loop_id`,
/// version, aggregate revision, parent, episode) and structurally cannot
/// carry a success-criteria list — that full content lives in the Checkpoint
/// Manager's stored schema (`docs/schemas/checkpoint-v0.1.yaml`:
/// `goal.success_criteria`), which has no Rust type in this crate yet. A
/// required criterion with **zero** evidence ever produced for it is
/// therefore invisible here and will never appear among `obstacles`.
/// Detecting that case is a later caller's job, once it holds the
/// authoritative required-criteria set to compare `findings` against (the
/// Loop Controller, a later task) — `permitted: true` from this function
/// alone is not proof that every criterion the Checkpoint requires has been
/// addressed, only that nothing it *was given evidence about* is unresolved.
#[must_use]
pub fn assess_completion(
    checkpoint: &Checkpoint,
    findings: &[VerificationFinding],
) -> CompletionAssessment {
    let mut obstacles = Vec::new();
    let mut passing: BTreeSet<CriterionId> = BTreeSet::new();
    let mut current_gaps: BTreeMap<CriterionId, EvidenceGap> = BTreeMap::new();
    let mut historical_gaps: BTreeMap<CriterionId, EvidenceGap> = BTreeMap::new();
    let mut supported = false;

    for finding in findings
        .iter()
        .filter(|finding| finding.enforcement.is_required())
    {
        if finding.lineage.loop_id != checkpoint.loop_id {
            obstacles.push(CompletionObstacle::ForeignFinding {
                gate_id: finding.gate_id.clone(),
                loop_id: finding.lineage.loop_id,
            });
            continue;
        }

        // "Current" means both at once: recorded at the Checkpoint version
        // in force, and not superseded by a moved target. Anything else is
        // historical — evidence from a point the Loop has since moved past
        // — and historical evidence must never permanently block a
        // criterion that a current finding already satisfies.
        let is_current = finding.lineage.checkpoint_version == checkpoint.version
            && finding.outcome != GateOutcome::Stale;

        if is_current {
            if finding.outcome == GateOutcome::Passed {
                supported = true;
                if let Some(criterion) = finding.criterion.clone() {
                    passing.insert(criterion);
                }
                continue;
            }
            let gap = current_gap(finding.outcome);
            match finding.criterion.clone() {
                Some(criterion) => {
                    current_gaps.entry(criterion).or_insert(gap);
                }
                None => obstacles.push(CompletionObstacle::BlockingFinding {
                    gate_id: finding.gate_id.clone(),
                    code: finding.code.clone(),
                    gap,
                }),
            }
            continue;
        }

        // Historical: has no bearing on completion unless it turns out to be
        // the only evidence its criterion ever had.
        if let Some(criterion) = finding.criterion.clone() {
            let gap = if finding.lineage.checkpoint_version == checkpoint.version {
                // Version matches, so `is_current` was false only because
                // the target moved out from under this finding.
                EvidenceGap::Stale
            } else {
                EvidenceGap::NotCurrent
            };
            historical_gaps.entry(criterion).or_insert(gap);
        }
    }

    // A criterion with a *current* unresolved gap is unsatisfied, even if
    // another gate passed for it: live evidence is never averaged away by a
    // pass (`docs/06` §7).
    for (criterion, gap) in &current_gaps {
        passing.remove(criterion);
        obstacles.push(CompletionObstacle::CriterionUnsupported {
            criterion: criterion.clone(),
            gap: *gap,
        });
    }

    // Historical evidence is inert once a current finding — passing or
    // failing — already speaks for the same criterion; it becomes an
    // obstacle only when it is the sole evidence on record.
    for (criterion, gap) in historical_gaps {
        if passing.contains(&criterion) || current_gaps.contains_key(&criterion) {
            continue;
        }
        obstacles.push(CompletionObstacle::CriterionUnsupported { criterion, gap });
    }

    if !supported {
        obstacles.push(CompletionObstacle::NoRequiredEvidence);
    }

    CompletionAssessment {
        permitted: obstacles.is_empty(),
        checkpoint_id: checkpoint.id,
        checkpoint_version: checkpoint.version,
        satisfied: passing.into_iter().collect(),
        obstacles,
    }
}

/// Maps a current, non-passing required finding's outcome to the gap it
/// leaves in its criterion's evidence.
///
/// Never called with [`GateOutcome::Passed`] (the caller handles that case
/// before reaching here) or [`GateOutcome::Stale`] (excluded from "current"
/// by definition), so those arms are unreachable.
fn current_gap(outcome: GateOutcome) -> EvidenceGap {
    match outcome {
        GateOutcome::Failed => EvidenceGap::Failed,
        GateOutcome::Inconclusive => EvidenceGap::Inconclusive,
        GateOutcome::Suspended => EvidenceGap::Suspended,
        GateOutcome::Passed | GateOutcome::Stale => {
            unreachable!("current_gap is only called for a current, non-passing finding")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{VerificationOutcome, aggregate};
    use crate::verification::{
        Digest, FindingLineage, GateEnforcement, GateId, GateOutcome, VerificationFinding,
        VerificationGateKind, VerificationPlanId,
    };
    use loopward_api::ids::{AttemptId, LoopId, WorkUnitId};

    fn finding(
        outcome: GateOutcome,
        enforcement: GateEnforcement,
        digest: &Digest,
    ) -> VerificationFinding {
        VerificationFinding {
            gate_id: GateId::new("gate"),
            kind: VerificationGateKind::Deterministic,
            enforcement,
            outcome,
            code: "test".to_owned(),
            statement: String::new(),
            evidence: Vec::new(),
            target_digest: digest.clone(),
            criterion: None,
            lineage: FindingLineage {
                loop_id: LoopId::from_uuid(uuid::Uuid::from_u128(1)),
                work_unit_id: WorkUnitId::from_uuid(uuid::Uuid::from_u128(2)),
                attempt_id: AttemptId::from_uuid(uuid::Uuid::from_u128(3)),
                plan_id: VerificationPlanId::new("plan"),
                plan_version: 1,
                checkpoint_version: 1,
            },
            input_request: None,
            observed_at: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn a_finding_bound_to_another_target_is_never_accepted() {
        let current = Digest::new("sha256:current");
        let other = Digest::new("sha256:other");
        let findings = vec![finding(
            GateOutcome::Passed,
            GateEnforcement::Required,
            &other,
        )];
        assert_eq!(
            aggregate(&findings, &current, 1, false),
            VerificationOutcome::Stale
        );
    }

    #[test]
    fn a_failure_outranks_a_suspension() {
        let digest = Digest::new("sha256:current");
        let findings = vec![
            finding(GateOutcome::Suspended, GateEnforcement::Required, &digest),
            finding(GateOutcome::Failed, GateEnforcement::Required, &digest),
        ];
        assert_eq!(
            aggregate(&findings, &digest, 2, false),
            VerificationOutcome::Rejected
        );
    }

    #[test]
    fn an_unevaluated_required_gate_cannot_be_accepted() {
        let digest = Digest::new("sha256:current");
        let findings = vec![finding(
            GateOutcome::Passed,
            GateEnforcement::Required,
            &digest,
        )];
        assert_eq!(
            aggregate(&findings, &digest, 2, false),
            VerificationOutcome::Inconclusive
        );
        assert_eq!(
            aggregate(&findings, &digest, 1, false),
            VerificationOutcome::Accepted
        );
    }

    #[test]
    fn an_inconclusive_required_gate_can_be_policy_rejected() {
        let digest = Digest::new("sha256:current");
        let findings = vec![finding(
            GateOutcome::Inconclusive,
            GateEnforcement::Required,
            &digest,
        )];
        assert_eq!(
            aggregate(&findings, &digest, 1, false),
            VerificationOutcome::Inconclusive
        );
        assert_eq!(
            aggregate(&findings, &digest, 1, true),
            VerificationOutcome::Rejected
        );
    }

    #[test]
    fn advisory_findings_do_not_reach_aggregation() {
        let digest = Digest::new("sha256:current");
        let findings = vec![
            finding(GateOutcome::Passed, GateEnforcement::Required, &digest),
            finding(GateOutcome::Failed, GateEnforcement::Advisory, &digest),
        ];
        assert_eq!(
            aggregate(&findings, &digest, 1, false),
            VerificationOutcome::Accepted
        );
    }
}
