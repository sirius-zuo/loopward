//! [`RotationPolicy::evaluate`]: the deterministic function from observed
//! rotation signals to one [`RotationDecision`].
//!
//! `docs/07-session-rotation.md` §7 enumerates every [`RotationReason`] and
//! §8 states Phase One's deterministic policy in prose (fresh Session per
//! Work Unit and for verification by default; mandatory rotation at phase
//! boundaries and after context integrity failure; mandatory closure after
//! runner failure; hard budget stops admission and drains; soft budget
//! recommends rotation at the next safe boundary) but does not name an
//! explicit precedence among reasons that could apply simultaneously. This
//! module picks one, in [`PRECEDENCE`], ordered by how urgently each reason
//! must be acted on: an unobservable or failed external effect first (the
//! generation cannot safely continue at all), then a proven integrity
//! violation, then a hard resource ceiling, then the structural boundaries
//! Phase One rotates at deterministically, then advisory recommendations
//! last. The rotation record still carries every contributing signal
//! (`docs/07` §9), so choosing one reason as primary never discards the
//! others.

use loopward_core::RotationReason;

/// Every signal [`crate::controller::advance`] can observe for one Episode
/// that might warrant rotating the current Runtime Session generation.
///
/// Each field is an independently observed, orthogonal signal rather than a
/// state machine, matching the same pattern
/// [`loopward_api::runner::RunnerCapabilities`] and
/// [`loopward_api::context::ContextCapabilities`] already use for the same
/// reason: `struct_excessive_bools` is intentionally allowed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct RotationContext {
    /// The current Work Unit's Attempt reached a terminal outcome.
    pub work_unit_boundary: bool,
    /// The Loop is about to verify a collected result and needs an
    /// independent context to do it in.
    pub verification_boundary: bool,
    /// A rejected or inconclusive result is about to be remediated.
    pub remediation_boundary: bool,
    /// The Loop crossed a domain-defined phase boundary.
    pub phase_boundary: bool,
    /// The Loop's local objective moved elsewhere.
    pub goal_locality_shift: bool,
    /// The soft context budget was reached or exceeded.
    pub soft_context_budget: bool,
    /// The hard context budget was reached or exceeded.
    pub hard_context_budget: bool,
    /// Pinned state or another context integrity check failed validation.
    pub context_integrity_failure: bool,
    /// Context entropy signals suggest a degraded working context.
    pub context_entropy_signal: bool,
    /// The runner reported internal compaction.
    pub runner_compaction: bool,
    /// The runner failed and its generation cannot be drained.
    pub runner_failure: bool,
    /// Rotation is part of crash or ambiguity recovery.
    pub recovery: bool,
    /// A deployment policy requires rotation.
    pub policy: bool,
    /// An operator explicitly requested rotation.
    pub operator_request: bool,
}

/// One [`RotationReason`] that applied, and whether Phase One treats it as
/// mandatory or merely advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotationSignal {
    /// The reason this signal names.
    pub reason: RotationReason,
    /// Whether this reason, alone, would make rotation mandatory rather than
    /// a recommendation for the next safe boundary.
    pub mandatory: bool,
}

/// The outcome of evaluating a [`RotationContext`]: whether to rotate, how
/// urgently, and every reason that applied.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RotationDecision {
    /// Whether any signal calls for rotation at all.
    pub rotate: bool,
    /// Whether the highest-precedence signal is mandatory. A mandatory
    /// decision must be acted on; a non-mandatory one is a recommendation
    /// for the next safe boundary (`docs/07` §10).
    pub mandatory: bool,
    /// The highest-precedence reason, when any signal applied. This is the
    /// reason the rotation record's `primary_reason` should carry
    /// (`docs/07` §9).
    pub primary_reason: Option<RotationReason>,
    /// Every signal that applied, highest precedence first. The rotation
    /// record stores all of them, not just the primary one.
    pub signals: Vec<RotationSignal>,
}

/// One precedence table entry: a predicate over [`RotationContext`], the
/// [`RotationReason`] it names, and whether Phase One treats it as
/// mandatory.
type PrecedenceEntry = (fn(&RotationContext) -> bool, RotationReason, bool);

/// The precedence order [`RotationPolicy::evaluate`] resolves ties with,
/// highest first: whether each signal fired, the [`RotationReason`] it
/// names, and whether Phase One treats it as mandatory.
const PRECEDENCE: &[PrecedenceEntry] = &[
    // The generation cannot be trusted to continue at all.
    (
        |context| context.runner_failure,
        RotationReason::RunnerFailure,
        true,
    ),
    (
        |context| context.context_integrity_failure,
        RotationReason::ContextIntegrityFailure,
        true,
    ),
    // A hard resource ceiling: admission must stop now.
    (
        |context| context.hard_context_budget,
        RotationReason::HardContextBudget,
        true,
    ),
    // Structural boundaries Phase One rotates at deterministically.
    (
        |context| context.phase_boundary,
        RotationReason::PhaseBoundary,
        true,
    ),
    (
        |context| context.remediation_boundary,
        RotationReason::RemediationBoundary,
        true,
    ),
    (
        |context| context.verification_boundary,
        RotationReason::VerificationBoundary,
        true,
    ),
    (
        |context| context.work_unit_boundary,
        RotationReason::WorkUnitBoundary,
        true,
    ),
    (|context| context.recovery, RotationReason::Recovery, true),
    (|context| context.policy, RotationReason::Policy, true),
    (
        |context| context.operator_request,
        RotationReason::OperatorRequest,
        true,
    ),
    // Advisory: recommended, not forced.
    (
        |context| context.goal_locality_shift,
        RotationReason::GoalLocalityShift,
        false,
    ),
    (
        |context| context.context_entropy_signal,
        RotationReason::ContextEntropySignal,
        false,
    ),
    (
        |context| context.runner_compaction,
        RotationReason::RunnerCompaction,
        false,
    ),
    (
        |context| context.soft_context_budget,
        RotationReason::SoftContextBudget,
        false,
    ),
];

/// Deterministically evaluates rotation signals into one [`RotationDecision`].
pub struct RotationPolicy;

impl RotationPolicy {
    /// Evaluates every signal in `context` against [`PRECEDENCE`].
    ///
    /// Every signal that applies is recorded in
    /// [`RotationDecision::signals`], in precedence order; the first one is
    /// [`RotationDecision::primary_reason`]. `rotate` is `true` whenever at
    /// least one signal applies, and `mandatory` reflects the primary
    /// signal alone: a mandatory reason and an advisory one can both be
    /// present (e.g. a soft budget signal alongside the deterministic
    /// work-unit boundary), and the mandatory one always wins the primary
    /// slot because [`PRECEDENCE`] lists every mandatory reason first.
    #[must_use]
    pub fn evaluate(context: &RotationContext) -> RotationDecision {
        let signals: Vec<RotationSignal> = PRECEDENCE
            .iter()
            .filter(|(predicate, _, _)| predicate(context))
            .map(|(_, reason, mandatory)| RotationSignal {
                reason: *reason,
                mandatory: *mandatory,
            })
            .collect();

        let primary = signals.first().copied();
        RotationDecision {
            rotate: !signals.is_empty(),
            mandatory: primary.is_some_and(|signal| signal.mandatory),
            primary_reason: primary.map(|signal| signal.reason),
            signals,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RotationContext, RotationPolicy};
    use loopward_core::RotationReason;

    #[test]
    fn no_signal_means_no_rotation() {
        let decision = RotationPolicy::evaluate(&RotationContext::default());
        assert!(!decision.rotate);
        assert!(!decision.mandatory);
        assert_eq!(decision.primary_reason, None);
        assert!(decision.signals.is_empty());
    }

    #[test]
    fn work_unit_boundary_is_mandatory_by_default() {
        let context = RotationContext {
            work_unit_boundary: true,
            ..RotationContext::default()
        };
        let decision = RotationPolicy::evaluate(&context);
        assert!(decision.rotate);
        assert!(decision.mandatory);
        assert_eq!(
            decision.primary_reason,
            Some(RotationReason::WorkUnitBoundary)
        );
    }

    #[test]
    fn soft_budget_alone_recommends_without_forcing() {
        let context = RotationContext {
            soft_context_budget: true,
            ..RotationContext::default()
        };
        let decision = RotationPolicy::evaluate(&context);
        assert!(decision.rotate);
        assert!(!decision.mandatory);
        assert_eq!(
            decision.primary_reason,
            Some(RotationReason::SoftContextBudget)
        );
    }

    #[test]
    fn hard_budget_stops_admission_and_is_mandatory() {
        let context = RotationContext {
            hard_context_budget: true,
            soft_context_budget: true,
            ..RotationContext::default()
        };
        let decision = RotationPolicy::evaluate(&context);
        assert!(decision.rotate);
        assert!(decision.mandatory);
        assert_eq!(
            decision.primary_reason,
            Some(RotationReason::HardContextBudget)
        );
        // Both signals are still recorded, not just the primary one.
        assert_eq!(decision.signals.len(), 2);
    }

    #[test]
    fn runner_failure_outranks_every_other_signal() {
        let context = RotationContext {
            runner_failure: true,
            context_integrity_failure: true,
            hard_context_budget: true,
            phase_boundary: true,
            ..RotationContext::default()
        };
        let decision = RotationPolicy::evaluate(&context);
        assert_eq!(decision.primary_reason, Some(RotationReason::RunnerFailure));
        assert!(decision.mandatory);
        assert_eq!(decision.signals.len(), 4);
    }

    #[test]
    fn verification_boundary_is_mandatory_and_distinct_from_work_unit_boundary() {
        let context = RotationContext {
            verification_boundary: true,
            ..RotationContext::default()
        };
        let decision = RotationPolicy::evaluate(&context);
        assert_eq!(
            decision.primary_reason,
            Some(RotationReason::VerificationBoundary)
        );
        assert!(decision.mandatory);
    }

    #[test]
    fn context_integrity_failure_outranks_hard_budget() {
        let context = RotationContext {
            context_integrity_failure: true,
            hard_context_budget: true,
            ..RotationContext::default()
        };
        let decision = RotationPolicy::evaluate(&context);
        assert_eq!(
            decision.primary_reason,
            Some(RotationReason::ContextIntegrityFailure)
        );
    }
}
