//! The effective policy the reducer consults when deciding a transition.
//!
//! [`EffectivePolicy`] is the *already resolved* policy for one Loop: Core
//! does not merge deployment defaults, Loop policies, and Work Unit policies
//! — the runtime resolves those and hands the result to [`crate::decide`].
//! Keeping it a value keeps the reducer pure and makes policy differences
//! testable without a runtime.
//!
//! Note what is deliberately *not* configurable: whether a runner result
//! must be verified before acceptance. That is an invariant of the
//! architecture (`docs/06-verification-and-recovery.md` §16, "runner-reported
//! success cannot bypass verification"), not a policy knob.

use serde::{Deserialize, Serialize};

/// Whether a Runtime Session generation may execute more than one Work Unit
/// Attempt.
///
/// Phase One is deterministic: a fresh Runtime Session for each Work Unit by
/// default. Reuse is an opt-in experiment, not the default architecture
/// (`docs/07-session-rotation.md` §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionReusePolicy {
    /// Rotate to a fresh generation before each Work Unit Attempt.
    #[default]
    FreshPerWorkUnit,
    /// Allow a generation to serve more than one Attempt.
    AllowReuse,
}

/// The resolved policy governing one Loop's transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectivePolicy {
    /// The maximum number of Attempts permitted for one Work Unit, when
    /// bounded. Remediation must be explicit and bounded
    /// (`docs/06-verification-and-recovery.md` §10).
    pub max_attempts_per_work_unit: Option<u32>,
    /// Whether a Runtime Session generation may serve more than one Attempt.
    pub session_reuse: SessionReusePolicy,
    /// Whether a runner failure mandates closing the generation as failed
    /// instead of draining it (`docs/07-session-rotation.md` §8).
    pub close_session_on_runner_failure: bool,
}

impl Default for EffectivePolicy {
    /// The Phase One deterministic rotation policy.
    fn default() -> Self {
        Self {
            max_attempts_per_work_unit: None,
            session_reuse: SessionReusePolicy::FreshPerWorkUnit,
            close_session_on_runner_failure: true,
        }
    }
}
