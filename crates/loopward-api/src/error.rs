//! Stable, typed error codes for the inbound `LoopService` protocol, per
//! `docs/09-public-api.md` §10.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::trace::TraceRef;

/// A stable, wire-level error code for `LoopService` failures.
///
/// Codes are stable across versions: callers may branch on them. Details
/// belong in the sanitized `message`, never in the code itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopwardErrorCode {
    /// The request was structurally or semantically invalid.
    InvalidRequest,
    /// The request used an unsupported protocol version.
    UnsupportedVersion,
    /// The referenced Loop or entity does not exist.
    NotFound,
    /// The request's expected revision did not match current state.
    Conflict,
    /// The Loop's execution lease is held by another driver.
    LeaseUnavailable,
    /// The requested transition is not valid from the current state.
    InvalidStateTransition,
    /// A context, resource, or cost budget was exceeded.
    BudgetExceeded,
    /// Boot Package hydration failed before any runner was started.
    HydrationFailed,
    /// The runner reported a protocol-level failure.
    RunnerFailed,
    /// The produced result failed verification.
    VerificationFailed,
    /// Manual or automated recovery must run before this Loop can advance.
    RecoveryRequired,
    /// An external side effect's state cannot be determined safely.
    AmbiguousExternalState,
    /// The Loop is currently suspended.
    Suspended,
    /// The Loop is cancelled and cannot advance.
    Cancelled,
    /// An internal error occurred that does not fit another code.
    Internal,
}

/// A typed, sanitized `LoopService` error.
///
/// Includes a stable code, a sanitized message safe to surface to callers,
/// retry guidance, and a trace correlation reference where available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct LoopwardError {
    /// The stable error code.
    pub code: LoopwardErrorCode,
    /// A sanitized, non-sensitive human-readable message.
    pub message: String,
    /// Whether retrying the request may succeed.
    pub retryable: bool,
    /// A trace correlation reference, when available.
    pub trace_ref: Option<TraceRef>,
}

impl fmt::Display for LoopwardErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::InvalidRequest => "invalid_request",
            Self::UnsupportedVersion => "unsupported_version",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::LeaseUnavailable => "lease_unavailable",
            Self::InvalidStateTransition => "invalid_state_transition",
            Self::BudgetExceeded => "budget_exceeded",
            Self::HydrationFailed => "hydration_failed",
            Self::RunnerFailed => "runner_failed",
            Self::VerificationFailed => "verification_failed",
            Self::RecoveryRequired => "recovery_required",
            Self::AmbiguousExternalState => "ambiguous_external_state",
            Self::Suspended => "suspended",
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
        };
        f.write_str(label)
    }
}
