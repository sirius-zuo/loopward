//! Durable identifiers and small primitive wire types shared across the
//! Loopward public contract.
//!
//! Every identifier in this module is backed by a `UUIDv7` so identifiers are
//! both globally unique and roughly time-ordered. They are opaque on the
//! wire: they serialize as their canonical string form and deserialize back
//! into the same typed newtype, so a `LoopId` can never be silently accepted
//! where an `EpisodeId` is expected even though both are UUIDs underneath.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A point in time, represented as an RFC 3339 timestamp on the wire.
///
/// Re-exported from the `time` crate rather than re-invented so that every
/// crate in the workspace shares one durable, non-ambiguous timestamp type.
pub type Timestamp = time::OffsetDateTime;

/// Error returned when parsing an identifier from its string form fails.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid {type_name}: {source}")]
pub struct IdParseError {
    /// The Rust type name of the identifier that failed to parse (e.g.
    /// `"LoopId"`), included so callers can build actionable diagnostics.
    pub type_name: &'static str,
    /// The underlying UUID parse failure.
    #[source]
    pub source: uuid::Error,
}

macro_rules! define_uuid_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a new, time-ordered identifier (`UUIDv7`).
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wraps an existing [`Uuid`] as this identifier type.
            #[must_use]
            pub fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            /// Returns the underlying [`Uuid`].
            #[must_use]
            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(s).map(Self).map_err(|source| IdParseError {
                    type_name: stringify!($name),
                    source,
                })
            }
        }
    };
}

define_uuid_id!(
    LoopId,
    "The durable identity of one long-running Loop execution."
);
define_uuid_id!(
    EpisodeId,
    "The identity of one Episode: one attempt to advance from a committed Checkpoint to the next durable semantic state."
);
define_uuid_id!(
    SessionId,
    "The identity of one Runtime Session: a bounded, replaceable execution environment."
);
define_uuid_id!(
    WorkUnitId,
    "The identity of one Work Unit: one externally observable unit of progress."
);
define_uuid_id!(
    AttemptId,
    "The identity of one Work Unit Attempt. A retry never overwrites a prior attempt; it creates a new one."
);
define_uuid_id!(
    OperationId,
    "The identity of one asynchronous `LoopService` operation (e.g. an `advance` or `run_until` invocation)."
);
define_uuid_id!(
    EventId,
    "The identity of one durable lifecycle event in the Loop's append-only event journal."
);
define_uuid_id!(
    CheckpointId,
    "The identity of one immutable, versioned Checkpoint."
);

#[cfg(test)]
mod tests {
    use super::{IdParseError, LoopId};
    use std::str::FromStr;

    #[test]
    fn display_and_from_str_round_trip() {
        let id = LoopId::new();
        let text = id.to_string();
        let parsed = LoopId::from_str(&text).expect("valid uuid text parses");
        assert_eq!(parsed, id);
    }

    #[test]
    fn from_str_rejects_malformed_text() {
        let err: IdParseError = LoopId::from_str("not-a-uuid").unwrap_err();
        assert_eq!(err.type_name, "LoopId");
    }

    #[test]
    fn serde_round_trip_is_transparent_string() {
        let id = LoopId::new();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        let decoded: LoopId = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, id);
    }
}
