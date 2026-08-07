//! Deterministic Runtime Session rotation policy
//! (`docs/07-session-rotation.md` §7, §8, §10).
//!
//! Rotation is a first-class lifecycle event, not an emergency response to an
//! almost-full context window. [`RotationPolicy::evaluate`] turns the signals
//! [`crate::controller`] observes for one Episode into a single
//! [`RotationDecision`]: whether to rotate at all, whether that rotation is
//! mandatory, and — when more than one signal applies at once — which
//! [`loopward_core::RotationReason`] is primary.

pub mod policy;

pub use policy::{RotationContext, RotationDecision, RotationPolicy, RotationSignal};
