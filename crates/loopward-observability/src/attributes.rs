//! Stable span attribute keys (`docs/08-tracing-and-observability.md` §6).
//!
//! Span names are low-cardinality constants (see [`crate::spans::span_name`]);
//! identifiers and other varying facts belong in attributes, keyed by the
//! constants here. High-cardinality identifiers (Loop, Episode, Session,
//! Work Unit, Attempt, resource references) are appropriate as span
//! attributes but must never automatically become default metric labels
//! (§6, §11) — that boundary is enforced by keeping metrics derivation a
//! separate, deliberate concern from span attribution, not by anything in
//! this module.

/// Attributes lifecycle spans include when applicable (§6, "Required
/// Attributes" — required in the sense of "record it when the fact exists
/// for this span", not "every span carries every key").
pub mod attribute {
    /// The owning Loop's identifier.
    pub const LOOP_ID: &str = "loopward.loop.id";
    /// The Episode's identifier.
    pub const EPISODE_ID: &str = "loopward.episode.id";
    /// The Episode's ordinal position within the Loop.
    pub const EPISODE_ORDINAL: &str = "loopward.episode.ordinal";
    /// The Runtime Session's identifier.
    pub const SESSION_ID: &str = "loopward.session.id";
    /// The Runtime Session's generation.
    pub const SESSION_GENERATION: &str = "loopward.session.generation";
    /// The Work Unit's identifier.
    pub const WORK_UNIT_ID: &str = "loopward.work_unit.id";
    /// The Work Unit Attempt's identifier.
    pub const ATTEMPT_ID: &str = "loopward.attempt.id";
    /// The runner's declared name.
    pub const RUNNER_NAME: &str = "loopward.runner.name";
    /// The runner protocol version in effect for this call.
    pub const RUNNER_PROTOCOL_VERSION: &str = "loopward.runner.protocol_version";
    /// The Checkpoint version this span observed or produced.
    pub const CHECKPOINT_VERSION: &str = "loopward.checkpoint.version";
    /// The lifecycle phase this span represents.
    pub const PHASE: &str = "loopward.phase";
    /// The specific operation this span represents.
    pub const OPERATION: &str = "loopward.operation";
    /// The business outcome this span concluded with — distinct from
    /// telemetry transport status (`docs/08` §10: a suspended Loop is a
    /// *successful* span with `loopward.outcome=suspended`).
    pub const OUTCOME: &str = "loopward.outcome";

    /// Why a rotation was requested, when this span represents one.
    pub const ROTATION_REASON: &str = "loopward.rotation.reason";
    /// The stable suspension code, when this span represents a suspension.
    pub const SUSPENSION_CODE: &str = "loopward.suspension.code";
    /// The stable, sanitized failure code, when this span represents a
    /// failure (`docs/08` §10: "Public codes and sanitized descriptions are
    /// preferred" over raw, potentially sensitive error messages).
    pub const FAILURE_CODE: &str = "loopward.failure.code";
    /// The recovery disposition, when this span represents a recovery
    /// operation.
    pub const RECOVERY_OUTCOME: &str = "loopward.recovery.outcome";
    /// The context selection rule version in effect for hydration.
    pub const CONTEXT_RULE_VERSION: &str = "loopward.context.rule_version";
    /// The estimated token count selected during hydration.
    pub const CONTEXT_ESTIMATED_TOKENS: &str = "loopward.context.estimated_tokens";
    /// The actual observed token count, when a runner reports it.
    pub const CONTEXT_ACTUAL_TOKENS: &str = "loopward.context.actual_tokens";
    /// The method used to estimate context token counts.
    pub const CONTEXT_ESTIMATE_METHOD: &str = "loopward.context.estimate_method";
    /// The verification gate kind, when this span represents one gate.
    pub const VERIFICATION_GATE_KIND: &str = "loopward.verification.gate_kind";
    /// The verification outcome, when this span represents a verification
    /// judgement.
    pub const VERIFICATION_OUTCOME: &str = "loopward.verification.outcome";
}

/// Attribute keys that must never appear on a recorded span, however they
/// are spelled by a caller — full prompts, Boot Package payloads, and raw
/// resource content are never trace data (`docs/08` §2, §12, §15).
///
/// This is a documentation and testing aid, not an enforcement mechanism:
/// nothing in this crate can stop a caller from passing an oversized or
/// sensitive string as an attribute *value*. What this crate does guarantee
/// is that none of its own constants, span/event names, or built-in
/// instrumentation ever name one of these keys.
pub const FORBIDDEN_ATTRIBUTE_SUBSTRINGS: &[&str] = &[
    "prompt",
    "boot_package.payload",
    "boot_package_payload",
    "resource.content",
    "resource_content",
];
