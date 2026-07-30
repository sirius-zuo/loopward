//! Boot Package Manifest assembly and integrity
//! (`docs/04-context-lifecycle.md` §7).
//!
//! Two digests bind a manifest to what it actually describes:
//!
//! * `pinned_digest` is `SHA-256` over the stored pinned bytes, and is the
//!   hydration-side half of the pinned-constraint integrity check whose
//!   drain-side half is `HandoffProposal::pinned_digest_seen`. Pinned
//!   constraints must survive hydration and drain losslessly, so this digest
//!   is computed from the bytes actually resolved for the pinned items —
//!   never from caller-declared metadata.
//! * `manifest_digest` is `SHA-256` over the stored serialized manifest
//!   bytes, computed with the `manifest_digest` field itself held at the
//!   empty string so the digest is well defined and reproducible.
//!
//! Both are deterministic: the serialization is `serde_json` over structs
//! with fixed field order and no map iteration anywhere, so the same inputs
//! and the same `context_rule_version` always produce the same bytes.

use loopward_api::context::BootPackageManifest;
use loopward_api::resource::CheckpointRef;
use loopward_core::Checkpoint;
use sha2::{Digest, Sha256};

use super::HydrationError;

/// The manifest schema version this hydrator emits, matching
/// `docs/schemas/boot-package-manifest-v0.1.yaml`.
pub const BOOT_PACKAGE_MANIFEST_SCHEMA_VERSION: &str = "loopward.boot-package-manifest/v0.1";

/// The stable reference form of the Checkpoint a Boot Package was hydrated
/// from, e.g. `checkpoint://<loop id>/<version>`.
///
/// Derived rather than carried on [`super::HydrationRequest`] so a manifest
/// can never claim to have been built from a Checkpoint other than the one
/// it was actually given.
#[must_use]
pub fn checkpoint_ref_for(checkpoint: &Checkpoint) -> CheckpointRef {
    CheckpointRef::new(format!(
        "checkpoint://{}/{}",
        checkpoint.loop_id, checkpoint.version
    ))
}

/// Renders `bytes` as lowercase hexadecimal.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// The `sha256:<hex>` digest of `bytes`.
pub(crate) fn content_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex_lower(&Sha256::digest(bytes)))
}

/// `SHA-256` over the stored pinned bytes, in manifest order.
///
/// Each pinned item contributes its identifier and its resolved content,
/// both length-framed, so two different pinned sets can never hash alike by
/// concatenating to the same byte string.
pub(crate) fn pinned_digest(pinned: &[(&str, &[u8])]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"loopward.pinned/v0.1");
    for (id, bytes) in pinned {
        hasher.update(u64::try_from(id.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(id.as_bytes());
        hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(bytes);
    }
    format!("sha256:{}", hex_lower(&hasher.finalize()))
}

/// Seals `manifest` by computing its `manifest_digest` over its own
/// serialized bytes.
///
/// The incoming manifest must carry an empty `manifest_digest`; that is the
/// exact form hashed, so any holder of the manifest can recompute and check
/// the digest by blanking the field again.
///
/// # Errors
///
/// Returns [`HydrationError::Serialization`] if the manifest cannot be
/// serialized, which would leave its integrity binding undefined.
pub(crate) fn seal(
    mut manifest: BootPackageManifest,
) -> Result<BootPackageManifest, HydrationError> {
    manifest.manifest_digest = String::new();
    let bytes =
        serde_json::to_vec(&manifest).map_err(|source| HydrationError::Serialization { source })?;
    manifest.manifest_digest = content_digest(&bytes);
    Ok(manifest)
}
