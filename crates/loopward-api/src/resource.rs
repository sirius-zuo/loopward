//! Resource references and other small, opaque wire-level reference types.
//!
//! Large files, reports, logs, source corpora, test outputs, and external
//! documents are represented by [`ResourceRef`] rather than being inlined.
//! Resources used as evidence must be immutable, version-addressed, or
//! content-addressed (see `docs/02-core-domain-model.md` §9).

use std::fmt;

use serde::{Deserialize, Serialize};

/// A reference to a resource stored outside the wire payload.
///
/// This is the one place large content is kept out of prompts and out of
/// durable lifecycle records: everything else points at a `ResourceRef`
/// instead of embedding a payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRef {
    /// The resource's location, e.g. `resource://output/017`.
    pub uri: String,
    /// The IANA media type of the resource content, when known.
    pub media_type: Option<String>,
    /// A content digest (e.g. `sha256:...`) used to bind evidence and
    /// outputs to an exact, immutable byte sequence.
    pub digest: Option<String>,
    /// An opaque version marker for mutable or versioned resources.
    pub version: Option<String>,
    /// The resource size in bytes, when known.
    pub size: Option<u64>,
    /// A short human-readable summary, never a substitute for the resource
    /// itself.
    pub summary: Option<String>,
}

/// An opaque reference to an external execution or session assigned by a
/// runner (e.g. a vendor job ID or process handle).
///
/// Loopward never interprets the contents of an `OpaqueRef`; it only stores
/// and replays it back to the runner that issued it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueRef(pub String);

impl OpaqueRef {
    /// Wraps a raw external reference string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for OpaqueRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A stable reference to the immutable Checkpoint a Boot Package or rotation
/// record was built from, e.g. `checkpoint://loop-001/1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointRef(pub String);

impl CheckpointRef {
    /// Wraps a raw checkpoint reference string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for CheckpointRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// An open, domain- or workflow-defined phase name (e.g. `implementation`).
///
/// Phases are not a closed Loopward Core enumeration: Core does not classify
/// domain-specific workflow phases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PhaseRef(pub String);

impl PhaseRef {
    /// Wraps a raw phase name.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for PhaseRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A reference to a versioned schema (e.g. a domain state schema or an
/// expected verification result schema), e.g.
/// `loopward.domain.example/v0.1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaRef(pub String);

impl SchemaRef {
    /// Wraps a raw schema reference string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for SchemaRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A reference to a pinned constraint, e.g. `constraint://constraint-001`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintRef {
    /// The constraint's opaque reference URI.
    #[serde(rename = "ref")]
    pub reference: String,
}

/// A reference to a versioned Verification Plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationPlanRef {
    /// The verification plan's opaque reference URI.
    #[serde(rename = "ref")]
    pub reference: String,
    /// The verification plan version this Work Unit was bound to.
    pub version: u32,
}

/// A reference to the runner that should (or did) execute a Work Unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerRef {
    /// The runner's registered name (e.g. `command`).
    pub name: String,
    /// An optional runner-specific profile or configuration variant.
    pub profile: Option<String>,
}

/// The declared shape of a Work Unit's expected output, identified by a
/// versioned schema name (e.g. `loopward.work-unit-result/v0.1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputContract {
    /// The versioned schema identifier the produced result must satisfy.
    pub schema: String,
}
