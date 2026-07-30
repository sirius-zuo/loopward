//! The backend failure mode this adapter reports through
//! [`StoreError::Backend`] and [`ResourceError::Backend`].
//!
//! The port errors model the *contract* failures every `LoopStore` shares
//! (revision conflicts, stale fencing tokens, a missing Loop). Everything
//! below is a failure the port deliberately does not model: `SQLite` itself
//! failing, durable JSON that will not encode or decode, and the storage
//! invariants of `docs/03-durable-state-and-transactions.md` §10 that have
//! no dedicated port variant — an unfinalized resource entering a
//! Checkpoint, a second active Episode, a reopened Session, a
//! non-monotonic version.
//!
//! Callers that need to distinguish them downcast the boxed source:
//!
//! ```ignore
//! if let StoreError::Backend(source) = &error
//!     && let Some(store_error) = source.downcast_ref::<SqliteStoreError>()
//! { /* ... */ }
//! ```

use loopward_api::ids::{CheckpointId, EpisodeId, LoopId, SessionId};
use loopward_runtime::ports::{ResourceError, StoreError};

/// A failure of the `SQLite`/filesystem backend, or of a storage invariant
/// the port error types do not model.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteStoreError {
    /// The underlying `SQLite` engine or driver failed.
    #[error("sqlite operation failed: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A schema migration could not be applied.
    #[error("schema migration failed: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),

    /// A durable value could not be encoded to JSON.
    #[error("could not encode {what} as durable json: {source}")]
    Encode {
        /// What was being encoded (e.g. `"projection"`).
        what: &'static str,
        /// The underlying `serde_json` failure.
        #[source]
        source: serde_json::Error,
    },

    /// A durable value could not be decoded from JSON.
    #[error("could not decode stored {what}: {source}")]
    Decode {
        /// What was being decoded (e.g. `"projection"`).
        what: &'static str,
        /// The underlying `serde_json` failure.
        #[source]
        source: serde_json::Error,
    },

    /// A value did not fit `SQLite`'s signed 64-bit integer columns.
    #[error("{what} value {value} does not fit a sqlite integer column")]
    IntegerRange {
        /// What was being converted (e.g. `"aggregate revision"`).
        what: &'static str,
        /// The offending value, rendered for diagnostics.
        value: String,
    },

    /// A stored timestamp could not be formatted or parsed as RFC 3339.
    #[error("stored timestamp {value} is not a valid rfc 3339 instant")]
    Timestamp {
        /// The offending textual value.
        value: String,
    },

    /// A resource referenced by a Checkpoint is unknown to this store, so
    /// its finalization can neither be proven nor its edge recorded (§10).
    #[error("resource {uri} is unknown to this store and cannot enter a checkpoint")]
    UnknownResource {
        /// The referenced resource URI.
        uri: String,
    },

    /// A resource referenced by a Checkpoint has not been finalized
    /// (§10: "unfinalized resources cannot enter a Checkpoint").
    #[error("resource {uri} is not finalized and cannot enter a checkpoint")]
    ResourceNotFinalized {
        /// The referenced resource URI.
        uri: String,
    },

    /// A commit would have left the Loop with two non-terminal Episodes
    /// (§10: "one Loop has at most one active Episode").
    #[error("loop {loop_id} already has an active episode {episode_id}")]
    ActiveEpisodeExists {
        /// The Loop the commit targeted.
        loop_id: LoopId,
        /// The Episode that is still active.
        episode_id: EpisodeId,
    },

    /// A commit would have moved an already-terminal Runtime Session
    /// generation back to a live state (§10: "closed Sessions cannot
    /// reopen").
    #[error("runtime session {session_id} is already {stored} and cannot reopen as {requested}")]
    ClosedSessionReopened {
        /// The Session generation the commit targeted.
        session_id: SessionId,
        /// The terminal status already recorded.
        stored: &'static str,
        /// The live status the commit tried to record.
        requested: &'static str,
    },

    /// A Checkpoint commit did not advance the Checkpoint version (§10:
    /// "checkpoint versions increase monotonically").
    #[error(
        "checkpoint version must increase past {expected_checkpoint_version} for {loop_id}, got {given}"
    )]
    NonMonotonicCheckpointVersion {
        /// The Loop the commit targeted.
        loop_id: LoopId,
        /// The Checkpoint version the caller expected to be current.
        expected_checkpoint_version: u64,
        /// The version of the Checkpoint being committed.
        given: u64,
    },

    /// A Checkpoint commit did not journal the `checkpoint.committed` event
    /// that records it.
    ///
    /// `docs/03-durable-state-and-transactions.md` §5 lists the
    /// `checkpoint.committed` journal event among the commit transaction's
    /// mandated contents, and the port documents
    /// [`CheckpointCommitRequest::events`] as recording "at minimum
    /// `checkpoint.committed`". Accepting a commit without it would leave a
    /// durable Checkpoint whose commit appears nowhere in the journal, while
    /// the projection's revision advanced anyway.
    ///
    /// [`CheckpointCommitRequest::events`]: loopward_runtime::ports::CheckpointCommitRequest::events
    #[error(
        "checkpoint commit for {loop_id} must journal a checkpoint.committed event naming checkpoint {checkpoint_id}"
    )]
    MissingCheckpointCommittedEvent {
        /// The Loop the commit targeted.
        loop_id: LoopId,
        /// The Checkpoint the journal event had to name.
        checkpoint_id: CheckpointId,
    },

    /// A write did not advance the aggregate revision far enough to cover
    /// the durable transitions it recorded (§10: "aggregate revisions
    /// increase monotonically").
    #[error(
        "aggregate revision must reach at least {required} for {loop_id} after {events} event(s), got {given}"
    )]
    NonMonotonicAggregateRevision {
        /// The Loop the write targeted.
        loop_id: LoopId,
        /// The lowest revision this write could legally end at.
        required: u64,
        /// The number of events the write recorded.
        events: usize,
        /// The revision the caller's projection carried.
        given: u64,
    },

    /// The request's own parts disagree, so persisting it would produce a
    /// durable state that contradicts itself.
    #[error("inconsistent checkpoint commit for {loop_id}: {detail}")]
    InconsistentRequest {
        /// The Loop the commit targeted.
        loop_id: LoopId,
        /// What disagreed.
        detail: String,
    },

    /// A filesystem operation against the resource root failed.
    #[error("resource io failed for {path}: {source}")]
    ResourceIo {
        /// The path the operation targeted.
        path: String,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
}

impl SqliteStoreError {
    /// Wraps a `SQLite` integer conversion failure.
    pub(crate) fn integer_range(what: &'static str, value: &impl ToString) -> Self {
        Self::IntegerRange {
            what,
            value: value.to_string(),
        }
    }

    /// Wraps a filesystem failure with the path that produced it.
    pub(crate) fn resource_io(path: &std::path::Path, source: std::io::Error) -> Self {
        Self::ResourceIo {
            path: path.display().to_string(),
            source,
        }
    }
}

impl From<sqlx::Error> for SqliteStoreError {
    fn from(source: sqlx::Error) -> Self {
        Self::Sqlx(source)
    }
}

impl From<SqliteStoreError> for StoreError {
    fn from(source: SqliteStoreError) -> Self {
        Self::Backend(Box::new(source))
    }
}

impl From<SqliteStoreError> for ResourceError {
    fn from(source: SqliteStoreError) -> Self {
        Self::Backend(Box::new(source))
    }
}
