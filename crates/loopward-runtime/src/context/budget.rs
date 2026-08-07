//! Boot Package budget arithmetic (`docs/04-context-lifecycle.md` §5).
//!
//! The budget reserves space for *execution* rather than filling the model
//! window with boot context: whatever is left after the output, tool, and
//! safety-margin reservations have been taken out of the hard input limit is
//! all the Boot Package may occupy.

use loopward_api::context::ContextBudget;

use super::HydrationError;

/// The token cost charged for an item materialized as a bare reference
/// rather than as payload.
///
/// A reference-only item contributes its `ResourceRef` (URI, media type,
/// digest, summary) to the Boot Package, not its content. The figure is a
/// deliberately conservative constant rather than a per-item measurement:
/// `docs/04-context-lifecycle.md` §5 requires that an unknown token cost stay
/// unknown, so this is recorded as an [`EstimateMethod::Heuristic`] estimate
/// with explicit low confidence.
///
/// [`EstimateMethod::Heuristic`]: loopward_api::context::EstimateMethod::Heuristic
pub const REFERENCE_ONLY_TOKENS: u64 = 16;

/// Confidence recorded alongside a [`REFERENCE_ONLY_TOKENS`] estimate.
pub(crate) const REFERENCE_ONLY_CONFIDENCE: f32 = 0.5;

/// The bytes-per-token ratio used when nothing better is available.
const HEURISTIC_BYTES_PER_TOKEN: u64 = 4;

/// Confidence recorded alongside a [`heuristic_token_estimate`].
pub(crate) const HEURISTIC_CONFIDENCE: f32 = 0.4;

/// The number of tokens a Boot Package may occupy for `budget`.
///
/// # Errors
///
/// Returns [`HydrationError::InvalidBudget`] when the reservations exceed
/// the hard input limit, which would otherwise underflow.
pub fn available_boot_tokens(budget: &ContextBudget) -> Result<u64, HydrationError> {
    budget
        .hard_input_limit_tokens
        .checked_sub(budget.reserved_output_tokens)
        .and_then(|v| v.checked_sub(budget.reserved_tool_tokens))
        .and_then(|v| v.checked_sub(budget.safety_margin_tokens))
        .ok_or(HydrationError::InvalidBudget)
}

/// A conservative token estimate for `byte_len` bytes of content.
///
/// This is the fallback used only when no caller-supplied estimate exists;
/// it rounds up, and its result is always recorded as
/// [`EstimateMethod::Heuristic`] with [`HEURISTIC_CONFIDENCE`] so a
/// downstream consumer can tell it apart from a tokenizer count.
///
/// [`EstimateMethod::Heuristic`]: loopward_api::context::EstimateMethod::Heuristic
#[must_use]
pub fn heuristic_token_estimate(byte_len: usize) -> u64 {
    u64::try_from(byte_len)
        .unwrap_or(u64::MAX)
        .div_ceil(HEURISTIC_BYTES_PER_TOKEN)
}

/// The running remainder of a Boot Package's token budget.
///
/// Required content is admitted unconditionally (the caller has already
/// proven it fits); everything else must ask.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BudgetLedger {
    remaining: u64,
}

impl BudgetLedger {
    /// Opens a ledger over `available` tokens.
    pub(crate) fn new(available: u64) -> Self {
        Self {
            remaining: available,
        }
    }

    /// Charges `cost` unconditionally, saturating at zero.
    pub(crate) fn charge(&mut self, cost: u64) {
        self.remaining = self.remaining.saturating_sub(cost);
    }

    /// Charges `cost` only if it fits, reporting whether it did.
    pub(crate) fn try_charge(&mut self, cost: u64) -> bool {
        if cost > self.remaining {
            return false;
        }
        self.remaining -= cost;
        true
    }
}
