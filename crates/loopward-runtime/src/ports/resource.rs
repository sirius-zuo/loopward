//! The [`ResourceBroker`] port: the boundary through which large resource
//! content (Boot Packages, runner results, verification evidence, ...) moves
//! in and out of a Loop, so the domain core and the durable journal only
//! ever carry a [`ResourceRef`] rather than the bytes themselves
//! (`docs/02-core-domain-model.md` §9).
//!
//! A resource moves through three states: begun (writable, not yet
//! durable), finalized (immutable, eligible to enter a Checkpoint —
//! `docs/03-durable-state-and-transactions.md` §10: "unfinalized resources
//! cannot enter a Checkpoint"), or aborted (never became durable). Once
//! finalized, content is addressed only by the returned [`ResourceRef`].

use async_trait::async_trait;
use loopward_api::resource::ResourceRef;

/// An in-progress, not-yet-durable resource write.
///
/// Opaque to callers: a broker may back it with a temp file path, an object
/// storage upload ID, or an in-memory buffer key. It is only ever passed
/// back to the broker that issued it, and it is consumed by [`finalize`] or
/// [`abort`] (`ResourceBroker::finalize`, `ResourceBroker::abort`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceHandle(String);

impl ResourceHandle {
    /// Wraps a raw, broker-assigned handle value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the underlying opaque handle value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Metadata supplied when beginning a new resource write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceBeginHint {
    /// The IANA media type of the content about to be written, when known.
    pub media_type: Option<String>,
    /// A short human-readable summary, carried through to the finalized
    /// [`ResourceRef`].
    pub summary: Option<String>,
}

/// An error returned by a [`ResourceBroker`] operation.
#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    /// The handle does not correspond to an open, in-progress resource
    /// write: already finalized, already aborted, or never issued by this
    /// broker.
    #[error("unknown or already-closed resource handle: {0}")]
    UnknownHandle(String),
    /// The referenced resource could not be resolved back to content.
    #[error("unresolvable resource reference: {0}")]
    UnresolvableReference(String),
    /// The underlying storage backend failed in a way this port does not
    /// otherwise model.
    #[error("resource backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// The boundary through which large resource content enters and leaves a
/// Loop.
///
/// `loopward-runtime` depends only on this trait: it never opens a file or
/// makes a network call itself. Concrete storage (filesystem, object
/// storage, ...) lives in a separate adapter crate, matching the constraint
/// that this crate depends on ports only.
#[async_trait]
pub trait ResourceBroker: Send + Sync {
    /// Begins a new, not-yet-durable resource write.
    async fn begin(&self, hint: ResourceBeginHint) -> Result<ResourceHandle, ResourceError>;

    /// Appends bytes to an in-progress resource write.
    async fn write(&self, handle: &ResourceHandle, bytes: &[u8]) -> Result<(), ResourceError>;

    /// Finalizes an in-progress write into an immutable [`ResourceRef`]
    /// eligible to enter a Checkpoint.
    async fn finalize(&self, handle: ResourceHandle) -> Result<ResourceRef, ResourceError>;

    /// Aborts an in-progress write; the resource never becomes durable.
    async fn abort(&self, handle: ResourceHandle) -> Result<(), ResourceError>;

    /// Resolves a previously finalized [`ResourceRef`] back to its content.
    async fn resolve(&self, reference: &ResourceRef) -> Result<Vec<u8>, ResourceError>;
}
