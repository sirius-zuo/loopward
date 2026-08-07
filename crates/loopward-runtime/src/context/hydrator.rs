//! The deterministic selection algorithm
//! (`docs/04-context-lifecycle.md` §6).
//!
//! The pipeline is fixed and contains no wall-clock reads, no random input,
//! no hash-map iteration, and no unstable sort, so the same inputs under the
//! same `context_rule_version` always produce the same manifest:
//!
//! ```text
//! validate budget
//!     -> project Work Unit inputs into candidates
//!     -> check every Work Unit constraint has a required pinned candidate
//!     -> rank candidates by (bucket, declared priority, id)
//!     -> resolve and digest-check every required candidate  [fails here]
//!     -> prove required content fits the boot budget        [fails here]
//!     -> price and fit optional candidates in bucket order
//!     -> emit the manifest with its pinned and manifest digests
//! ```
//!
//! Every failure mode in `docs/04-context-lifecycle.md` §12 is raised before
//! anything is selected, let alone materialized: a failed hydration never
//! reaches the point where a Work Unit Attempt could be started.

use std::collections::BTreeSet;

use futures_util::future::try_join_all;

use loopward_api::context::{
    BootPackageManifest, ContextClass, ContextItem, ContextPriority, EstimateMethod, Fidelity,
    Validity, VerificationStatus,
};

use crate::ports::{ResourceBroker, ResourceError};

use super::budget::{
    BudgetLedger, HEURISTIC_CONFIDENCE, REFERENCE_ONLY_CONFIDENCE, REFERENCE_ONLY_TOKENS,
    available_boot_tokens, heuristic_token_estimate,
};
use super::manifest::{
    BOOT_PACKAGE_MANIFEST_SCHEMA_VERSION, checkpoint_ref_for, content_digest, pinned_digest, seal,
};
use super::{
    CandidateDisposition, CandidateOutcome, ContextCandidate, ExclusionReason, HydrationError,
    HydrationReport, HydrationRequest,
};

/// The ordered selection buckets from `docs/04-context-lifecycle.md` §6.
///
/// The declaration order *is* the selection order: budget is offered to
/// buckets from the top down, within a bucket to the higher declared
/// [`ContextPriority`] first, and within one priority in lexicographic
/// `ContextItem::id` order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SelectionBucket {
    /// Required pinned state: policy, active user constraints, acceptance
    /// criteria, safety requirements, invariants. Never dropped, never
    /// summarized.
    RequiredPinned,
    /// Everything else the current Work Unit requires in order to run.
    RequiredCurrentWork,
    /// Active, non-superseded decisions applicable to this Work Unit.
    ApplicableActiveDecision,
    /// Evidence carrying a verified observation.
    VerifiedObservation,
    /// Important inputs and current state for this Work Unit.
    ImportantCurrentInput,
    /// Recently accepted outcomes, kept for continuity.
    RecentAcceptedOutcome,
    /// Anything that may use whatever budget is left.
    Supplemental,
    /// Material referenced rather than materialized.
    OnDemandReference,
}

impl SelectionBucket {
    /// Every bucket, in selection order.
    pub const ORDER: &'static [Self] = &[
        Self::RequiredPinned,
        Self::RequiredCurrentWork,
        Self::ApplicableActiveDecision,
        Self::VerifiedObservation,
        Self::ImportantCurrentInput,
        Self::RecentAcceptedOutcome,
        Self::Supplemental,
        Self::OnDemandReference,
    ];

    /// The stable `snake_case` label used in diagnostics and telemetry.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::RequiredPinned => "required_pinned",
            Self::RequiredCurrentWork => "required_current_work",
            Self::ApplicableActiveDecision => "applicable_active_decision",
            Self::VerifiedObservation => "verified_observation",
            Self::ImportantCurrentInput => "important_current_input",
            Self::RecentAcceptedOutcome => "recent_accepted_outcome",
            Self::Supplemental => "supplemental",
            Self::OnDemandReference => "on_demand_reference",
        }
    }

    /// Whether a candidate in this bucket must be present or hydration
    /// fails.
    #[must_use]
    pub fn is_required(self) -> bool {
        matches!(self, Self::RequiredPinned | Self::RequiredCurrentWork)
    }

    /// Places a candidate in exactly one bucket.
    ///
    /// The mapping is total by construction. Two deliberate consequences:
    /// a `Required` candidate always lands in one of the two required
    /// buckets whatever its class, so required material can never be
    /// out-ranked by optional material; and `Important` material whose class
    /// does not match a dedicated bucket (an unverified observation, a plain
    /// reference) falls to [`Self::Supplemental`] rather than being demoted
    /// to a reference — it still competes for remaining budget.
    ///
    /// That last case is why a bucket alone is not the whole selection key:
    /// demoted `Important` material shares [`Self::Supplemental`] with
    /// genuinely supplemental material, so [`priority_rank`] orders within
    /// the bucket and keeps "important is selected before supplemental
    /// material" (`docs/04-context-lifecycle.md` §4.2) true there too.
    fn classify(candidate: &ContextCandidate) -> Self {
        match (candidate.priority, candidate.classification) {
            (ContextPriority::Required, ContextClass::Pinned) => Self::RequiredPinned,
            (ContextPriority::Required, _) => Self::RequiredCurrentWork,
            (ContextPriority::OnDemand, _) => Self::OnDemandReference,
            (ContextPriority::Important, ContextClass::Decision)
                if candidate.validity == Validity::Current =>
            {
                Self::ApplicableActiveDecision
            }
            (ContextPriority::Important, ContextClass::Evidence)
                if candidate.verification_status == Some(VerificationStatus::Verified) =>
            {
                Self::VerifiedObservation
            }
            (
                ContextPriority::Important,
                ContextClass::Pinned | ContextClass::CurrentState | ContextClass::CurrentWork,
            ) => Self::ImportantCurrentInput,
            (ContextPriority::Important, ContextClass::RecentOutcome)
                if candidate.validity == Validity::Current =>
            {
                Self::RecentAcceptedOutcome
            }
            (ContextPriority::Supplemental | ContextPriority::Important, _) => Self::Supplemental,
        }
    }
}

/// The declared-priority order within a selection bucket.
///
/// `Required` outranks `Important`, which outranks `Supplemental`, which
/// outranks `OnDemand` (`docs/04-context-lifecycle.md` §4.2). This is the
/// second component of the selection key, so a bucket that can hold more than
/// one declared priority — [`SelectionBucket::Supplemental`], which also
/// receives demoted `Important` material — still offers budget to the higher
/// priority first.
fn priority_rank(priority: ContextPriority) -> u8 {
    match priority {
        ContextPriority::Required => 0,
        ContextPriority::Important => 1,
        ContextPriority::Supplemental => 2,
        ContextPriority::OnDemand => 3,
    }
}

/// The total, deterministic selection key: bucket, then declared priority,
/// then lexicographic identifier.
///
/// Identifiers are unique within one request
/// ([`HydrationError::DuplicateCandidateId`]), so this is a total order and
/// no two candidates ever tie.
fn selection_key(entry: &Ranked) -> (SelectionBucket, u8, &str) {
    (
        entry.bucket,
        priority_rank(entry.candidate.priority),
        entry.candidate.id.as_str(),
    )
}

/// A candidate with its bucket assigned, ready to be ordered.
struct Ranked {
    candidate: ContextCandidate,
    bucket: SelectionBucket,
}

/// A candidate's resolved cost and integrity binding.
struct Priced {
    fidelity: Fidelity,
    tokens: u64,
    estimate_method: EstimateMethod,
    estimate_confidence: Option<f32>,
    digest: String,
}

/// Runs the deterministic hydration pipeline.
///
/// # Errors
///
/// See [`HydrationError`]; every variant is raised before any candidate is
/// selected or materialized.
pub(crate) async fn hydrate(
    broker: &dyn ResourceBroker,
    request: HydrationRequest,
) -> Result<HydrationReport, HydrationError> {
    let available = available_boot_tokens(&request.budget)?;
    let candidates = project_candidates(&request)?;
    let ranked = rank(candidates)?;
    check_work_unit_constraints(&request, &ranked)?;

    // Phase 1: required content. Resolved and digest-checked before anything
    // optional is even looked at, so a missing or divergent pinned resource
    // fails here.
    let mut required: Vec<Ranked> = Vec::with_capacity(ranked.len());
    let mut optional: Vec<Ranked> = Vec::new();
    for entry in ranked {
        if entry.bucket.is_required() {
            required.push(entry);
        } else {
            optional.push(entry);
        }
    }

    // Each required candidate's resolve is an independent broker read with
    // no ordering dependency between them — `priced` is re-sorted by
    // `selection_key` below regardless of resolution order — so resolving
    // them concurrently rather than one `.await` at a time turns N
    // sequential round trips into one wait.
    let priced_required: Vec<(Priced, Vec<u8>)> =
        try_join_all(required.iter().map(|entry| price_required(broker, entry))).await?;

    let mut priced: Vec<(Ranked, Result<Priced, ExclusionReason>)> =
        Vec::with_capacity(required.len() + optional.len());
    let mut pinned_bytes: Vec<(String, Vec<u8>)> = Vec::new();
    let mut required_tokens: u64 = 0;
    for (entry, (cost, bytes)) in required.into_iter().zip(priced_required) {
        required_tokens = required_tokens.saturating_add(cost.tokens);
        if entry.bucket == SelectionBucket::RequiredPinned {
            pinned_bytes.push((entry.candidate.id.clone(), bytes));
        }
        priced.push((entry, Ok(cost)));
    }

    if required_tokens > available {
        return Err(HydrationError::RequiredContentExceedsBudget {
            required_tokens,
            available_tokens: available,
        });
    }

    // Phase 2: fit optional content, bucket by bucket, into what is left.
    let mut ledger = BudgetLedger::new(available);
    ledger.charge(required_tokens);
    let supports_fetch = request.capabilities.context.supports_resource_fetch;

    for entry in optional {
        let outcome = match stale_exclusion(&entry.candidate) {
            Some(reason) => Err(reason),
            None => match price_optional(broker, &entry, supports_fetch).await {
                Ok(cost) if ledger.try_charge(cost.tokens) => Ok(cost),
                Ok(_) => Err(ExclusionReason::BudgetExhausted),
                Err(reason) => Err(reason),
            },
        };
        priced.push((entry, outcome));
    }

    priced.sort_by(|left, right| selection_key(&left.0).cmp(&selection_key(&right.0)));

    assemble(request, priced, &pinned_bytes)
}

/// Guarantees every declared Work Unit input is required context.
///
/// This is the minimal candidate-generation step the hydrator performs on its
/// own: a Work Unit's declared inputs are, by definition, needed by the
/// current Work Unit, so they must be hydrated as required current work. Two
/// ways a Checkpoint projection can fail to deliver that, both handled here:
///
/// * **The projection omitted the input.** A required current-work candidate
///   is synthesized for it.
/// * **The projection covered the input at a lower priority.** The covering
///   candidate is *promoted* to [`ContextPriority::Required`]. Skipping
///   synthesis merely because some candidate shares the URI would leave a
///   declared input droppable by [`ExclusionReason::BudgetExhausted`], which
///   defeats this guard exactly as thoroughly as omitting it.
///
/// When several candidates cover one input, the promotion applies to the
/// lexicographically smallest identifier among them, so the outcome does not
/// depend on the order candidates arrived in.
fn project_candidates(request: &HydrationRequest) -> Result<Vec<ContextCandidate>, HydrationError> {
    let mut candidates = request.candidates.clone();

    let mut synthesized = Vec::new();
    for (index, input) in request.work_unit.inputs.iter().enumerate() {
        let mut covering: Vec<&mut ContextCandidate> = candidates
            .iter_mut()
            .filter(|candidate| candidate.source.uri == input.uri)
            .collect();

        if covering.is_empty() {
            let candidate = ContextCandidate::new(
                format!("work-unit-input-{index:04}"),
                "work-unit-input",
                input.clone(),
                ContextClass::CurrentWork,
                ContextPriority::Required,
            );
            synthesized.push(ContextCandidate {
                selection_reason: "Declared Work Unit input with no projected context candidate"
                    .to_owned(),
                ..candidate
            });
            continue;
        }
        if covering
            .iter()
            .any(|candidate| candidate.priority == ContextPriority::Required)
        {
            continue;
        }

        covering.sort_by(|left, right| left.id.cmp(&right.id));
        let promoted = covering.swap_remove(0);
        promoted.priority = ContextPriority::Required;
        promoted.selection_reason = format!(
            "{} (promoted to required: declared Work Unit input)",
            promoted.selection_reason
        );
    }
    candidates.extend(synthesized);

    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for candidate in &candidates {
        if !seen.insert(candidate.id.as_str()) {
            return Err(HydrationError::DuplicateCandidateId {
                id: candidate.id.clone(),
            });
        }
    }
    Ok(candidates)
}

/// Assigns buckets, orders by [`selection_key`], and rejects a required pinned
/// candidate that would enter the Boot Package lossily.
fn rank(candidates: Vec<ContextCandidate>) -> Result<Vec<Ranked>, HydrationError> {
    let mut ranked: Vec<Ranked> = candidates
        .into_iter()
        .map(|candidate| Ranked {
            bucket: SelectionBucket::classify(&candidate),
            candidate,
        })
        .collect();
    ranked.sort_by(|left, right| selection_key(left).cmp(&selection_key(right)));

    for entry in &ranked {
        if entry.bucket == SelectionBucket::RequiredPinned
            && !matches!(
                entry.candidate.fidelity,
                Fidelity::Verbatim | Fidelity::Structured
            )
        {
            return Err(HydrationError::LossyPinnedFidelity {
                id: entry.candidate.id.clone(),
                fidelity: fidelity_label(entry.candidate.fidelity),
            });
        }
    }
    Ok(ranked)
}

/// Every constraint the Work Unit declares must be represented by a required
/// pinned candidate, or the pinned state this Boot Package would carry is
/// incomplete.
fn check_work_unit_constraints(
    request: &HydrationRequest,
    ranked: &[Ranked],
) -> Result<(), HydrationError> {
    let pinned: BTreeSet<&str> = ranked
        .iter()
        .filter(|entry| entry.bucket == SelectionBucket::RequiredPinned)
        .map(|entry| entry.candidate.source.uri.as_str())
        .collect();
    for constraint in &request.work_unit.constraints {
        if !pinned.contains(constraint.reference.as_str()) {
            return Err(HydrationError::MissingPinnedConstraint {
                reference: constraint.reference.clone(),
            });
        }
    }
    Ok(())
}

/// Resolves, digest-checks, and prices one required candidate.
async fn price_required(
    broker: &dyn ResourceBroker,
    entry: &Ranked,
) -> Result<(Priced, Vec<u8>), HydrationError> {
    let candidate = &entry.candidate;
    let bytes = broker
        .resolve(&candidate.source)
        .await
        .map_err(|source| match source {
            ResourceError::UnresolvableReference(_) => HydrationError::MissingRequiredResource {
                id: candidate.id.clone(),
                uri: candidate.source.uri.clone(),
            },
            other => HydrationError::Resource {
                id: candidate.id.clone(),
                source: other,
            },
        })?;

    let computed = content_digest(&bytes);
    if let Some(expected) = candidate.source.digest.as_deref()
        && expected != computed
    {
        return Err(HydrationError::DigestMismatch {
            id: candidate.id.clone(),
            expected: expected.to_owned(),
            computed,
        });
    }

    let priced = if let Some(tokens) = candidate.estimated_tokens {
        Priced {
            fidelity: candidate.fidelity,
            tokens,
            estimate_method: candidate
                .estimate_method
                .unwrap_or(EstimateMethod::Heuristic),
            estimate_confidence: candidate.estimate_confidence,
            digest: computed,
        }
    } else {
        Priced {
            fidelity: candidate.fidelity,
            tokens: heuristic_token_estimate(bytes.len()),
            estimate_method: EstimateMethod::Heuristic,
            estimate_confidence: Some(HEURISTIC_CONFIDENCE),
            digest: computed,
        }
    };
    Ok((priced, bytes))
}

/// Prices one optional candidate, resolving it only when its declared
/// metadata is insufficient.
///
/// A candidate that declares both a content digest and a token estimate is
/// trusted without a fetch; anything else is resolved so its digest and cost
/// describe real bytes. Unlike a required candidate, a failure here excludes
/// the candidate instead of failing hydration.
async fn price_optional(
    broker: &dyn ResourceBroker,
    entry: &Ranked,
    supports_resource_fetch: bool,
) -> Result<Priced, ExclusionReason> {
    let candidate = &entry.candidate;
    // A runner that cannot fetch on demand receives a fully materialized
    // Boot Package, so an on-demand candidate costs its full estimate there.
    let reference_only =
        entry.bucket == SelectionBucket::OnDemandReference && supports_resource_fetch;
    let needs_bytes = candidate.source.digest.is_none()
        || (!reference_only && candidate.estimated_tokens.is_none());

    let bytes = if needs_bytes {
        Some(
            broker
                .resolve(&candidate.source)
                .await
                .map_err(|_| ExclusionReason::Unresolvable)?,
        )
    } else {
        None
    };

    let digest = if let Some(bytes) = bytes.as_deref() {
        let computed = content_digest(bytes);
        if let Some(expected) = candidate.source.digest.as_deref()
            && expected != computed
        {
            return Err(ExclusionReason::DigestMismatch);
        }
        computed
    } else if let Some(declared) = candidate.source.digest.clone() {
        declared
    } else {
        return Err(ExclusionReason::Unresolvable);
    };

    if reference_only {
        return Ok(Priced {
            fidelity: Fidelity::ReferenceOnly,
            tokens: REFERENCE_ONLY_TOKENS,
            estimate_method: EstimateMethod::Heuristic,
            estimate_confidence: Some(REFERENCE_ONLY_CONFIDENCE),
            digest,
        });
    }
    if let Some(tokens) = candidate.estimated_tokens {
        return Ok(Priced {
            fidelity: candidate.fidelity,
            tokens,
            estimate_method: candidate
                .estimate_method
                .unwrap_or(EstimateMethod::Heuristic),
            estimate_confidence: candidate.estimate_confidence,
            digest,
        });
    }
    match bytes.as_deref() {
        Some(bytes) => Ok(Priced {
            fidelity: candidate.fidelity,
            tokens: heuristic_token_estimate(bytes.len()),
            estimate_method: EstimateMethod::Heuristic,
            estimate_confidence: Some(HEURISTIC_CONFIDENCE),
            digest,
        }),
        None => Err(ExclusionReason::Unresolvable),
    }
}

/// Optional content that no longer reflects authoritative state is excluded
/// before it is priced: a superseded decision is not an *applicable* active
/// decision, and stale material is not current state.
fn stale_exclusion(candidate: &ContextCandidate) -> Option<ExclusionReason> {
    match candidate.validity {
        Validity::Superseded => Some(ExclusionReason::Superseded),
        Validity::Stale => Some(ExclusionReason::Stale),
        Validity::Current | Validity::Unknown => None,
    }
}

/// Builds the manifest and the per-candidate report from the priced,
/// already-ordered candidate list.
fn assemble(
    request: HydrationRequest,
    priced: Vec<(Ranked, Result<Priced, ExclusionReason>)>,
    pinned_bytes: &[(String, Vec<u8>)],
) -> Result<HydrationReport, HydrationError> {
    let mut items = Vec::new();
    let mut considered = Vec::with_capacity(priced.len());

    for (entry, outcome) in priced {
        let Ranked { candidate, bucket } = entry;
        match outcome {
            Ok(cost) => {
                considered.push(CandidateOutcome {
                    id: candidate.id.clone(),
                    bucket,
                    estimated_tokens: Some(cost.tokens),
                    estimate_method: Some(cost.estimate_method),
                    disposition: CandidateDisposition::Included,
                });
                items.push(into_item(candidate, cost));
            }
            Err(reason) => considered.push(CandidateOutcome {
                id: candidate.id,
                bucket,
                estimated_tokens: candidate.estimated_tokens,
                estimate_method: candidate
                    .estimate_method
                    .or(Some(EstimateMethod::Heuristic)),
                disposition: CandidateDisposition::Excluded(reason),
            }),
        }
    }

    let pinned: Vec<(&str, &[u8])> = pinned_bytes
        .iter()
        .map(|(id, bytes)| (id.as_str(), bytes.as_slice()))
        .collect();

    let manifest = seal(BootPackageManifest {
        schema_version: BOOT_PACKAGE_MANIFEST_SCHEMA_VERSION.to_owned(),
        loop_id: request.checkpoint.loop_id,
        episode_id: request.episode_id,
        session_generation: request.session_generation,
        work_unit_id: request.work_unit.id,
        checkpoint_ref: checkpoint_ref_for(&request.checkpoint),
        context_rule_version: request.context_rule_version,
        budget: request.budget,
        items,
        pinned_digest: pinned_digest(&pinned),
        manifest_digest: String::new(),
    })?;

    Ok(HydrationReport {
        manifest,
        considered,
    })
}

/// Projects a priced candidate into the wire-level [`ContextItem`] that
/// ships in the manifest.
fn into_item(candidate: ContextCandidate, priced: Priced) -> ContextItem {
    ContextItem {
        id: candidate.id,
        kind: candidate.kind,
        source: candidate.source,
        classification: candidate.classification,
        priority: candidate.priority,
        fidelity: priced.fidelity,
        sensitivity: candidate.sensitivity,
        validity: candidate.validity,
        estimated_tokens: Some(priced.tokens),
        estimate_method: Some(priced.estimate_method),
        estimate_confidence: priced.estimate_confidence,
        digest: priced.digest,
        selection_reason: candidate.selection_reason,
    }
}

/// The stable `snake_case` label for a fidelity, for diagnostics.
fn fidelity_label(fidelity: Fidelity) -> &'static str {
    match fidelity {
        Fidelity::Verbatim => "verbatim",
        Fidelity::Structured => "structured",
        Fidelity::Excerpt => "excerpt",
        Fidelity::Summary => "summary",
        Fidelity::ReferenceOnly => "reference_only",
    }
}
