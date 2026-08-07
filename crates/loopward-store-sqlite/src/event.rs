//! The append-only event journal: this adapter's durable encoding of
//! [`LoopEvent`], plus the read and write helpers every transaction uses
//! (`docs/03-durable-state-and-transactions.md` §6).
//!
//! # Why a local encoding
//!
//! [`loopward_core::LoopEvent`] is a pure domain type with no `serde`
//! derives: Core has no clock, no journal positions, and no opinion about
//! how a store spells its rows. Storage encoding is this adapter's
//! responsibility, so [`StoredPayload`] below mirrors `LoopEvent` variant
//! for variant and the two `From` impls are exhaustive matches — adding a
//! domain event without teaching the journal about it is a compile error,
//! not a silent data-loss bug.
//!
//! # Event identity
//!
//! The `loop_events` table has no `event_id` column, so
//! [`RecordedEvent::event_id`] is *derived*, deterministically, from the
//! journal coordinates that do exist: `SHA-256("loopward.event/v1", loop_id,
//! sequence)`, folded into a UUID. Determinism is the point. A randomly
//! minted identity would have to be persisted somewhere to survive a
//! reload, and an identity that changed between the [`RecordedEvent`]
//! returned by a write and the one returned by a later read would be worse
//! than useless. See the crate-level note in `lib.rs`.

use loopward_api::ids::{AttemptId, EpisodeId, EventId, LoopId, SessionId, Timestamp, WorkUnitId};
use loopward_api::resource::{OpaqueRef, ResourceRef};
use loopward_api::runner::SuspensionReason;
use loopward_core::{
    Checkpoint, EpisodeKind, LoopEvent, RotationDisposition, RotationReason, SessionCloseOutcome,
    VerificationOutcome,
};
use loopward_runtime::ports::{EVENT_SCHEMA_VERSION, RecordedEvent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};
use uuid::Uuid;

use crate::error::SqliteStoreError;
use crate::store::{format_timestamp, parse_timestamp, sql_i64, sql_u64};

/// How a Runtime Session generation ended, as stored.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredSessionCloseOutcome {
    Closed,
    Failed,
}

/// What a rotation does to the generation it rotates out, as stored.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredRotationDisposition {
    Drain,
    AbandonGeneration,
}

/// The durable JSON form of one [`LoopEvent`].
///
/// Internally tagged by `event`, whose value is exactly
/// [`LoopEvent::kind`]'s journal event type, so `payload_json` is
/// self-describing even read outside this crate.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event")]
enum StoredPayload {
    #[serde(rename = "episode.started")]
    EpisodeStarted {
        episode_id: EpisodeId,
        kind: EpisodeKind,
        base_checkpoint_version: u64,
    },
    #[serde(rename = "context.hydrated")]
    ContextHydrated {
        episode_id: EpisodeId,
        manifest: ResourceRef,
    },
    #[serde(rename = "session.started")]
    SessionStarted {
        session_id: SessionId,
        episode_id: EpisodeId,
        generation: u32,
        boot_manifest_ref: ResourceRef,
    },
    #[serde(rename = "attempt.prepared")]
    AttemptPrepared {
        attempt_id: AttemptId,
        work_unit_id: WorkUnitId,
        episode_id: EpisodeId,
        ordinal: u32,
        idempotency_key: String,
    },
    #[serde(rename = "attempt.starting")]
    AttemptStarting { attempt_id: AttemptId },
    #[serde(rename = "attempt.started")]
    AttemptStarted {
        attempt_id: AttemptId,
        external_ref: OpaqueRef,
    },
    #[serde(rename = "attempt.collecting")]
    AttemptCollecting { attempt_id: AttemptId },
    #[serde(rename = "attempt.completed")]
    AttemptCompleted {
        attempt_id: AttemptId,
        work_unit_id: WorkUnitId,
        result: ResourceRef,
    },
    #[serde(rename = "verification.completed")]
    VerificationCompleted {
        work_unit_id: WorkUnitId,
        attempt_id: AttemptId,
        outcome: VerificationOutcome,
    },
    #[serde(rename = "rotation.requested")]
    RotationRequested {
        session_id: SessionId,
        from_generation: u32,
        reason: RotationReason,
        disposition: StoredRotationDisposition,
    },
    #[serde(rename = "session.closed")]
    SessionClosed {
        session_id: SessionId,
        outcome: StoredSessionCloseOutcome,
    },
    #[serde(rename = "episode.committing")]
    EpisodeCommitting { episode_id: EpisodeId },
    #[serde(rename = "checkpoint.committed")]
    CheckpointCommitted { checkpoint: Checkpoint },
    #[serde(rename = "loop.suspended")]
    LoopSuspended { reason: SuspensionReason },
    #[serde(rename = "loop.completed")]
    LoopCompleted,
    #[serde(rename = "loop.failed")]
    LoopFailed { code: String },
}

impl From<&LoopEvent> for StoredPayload {
    #[expect(
        clippy::too_many_lines,
        reason = "an exhaustive, one-arm-per-variant translation table: splitting it would \
                  hide the fact that every domain event has a durable encoding"
    )]
    fn from(event: &LoopEvent) -> Self {
        match event {
            LoopEvent::EpisodeStarted {
                episode_id,
                kind,
                base_checkpoint_version,
            } => Self::EpisodeStarted {
                episode_id: *episode_id,
                kind: *kind,
                base_checkpoint_version: *base_checkpoint_version,
            },
            LoopEvent::ContextHydrated {
                episode_id,
                manifest,
            } => Self::ContextHydrated {
                episode_id: *episode_id,
                manifest: manifest.clone(),
            },
            LoopEvent::SessionStarted {
                session_id,
                episode_id,
                generation,
                boot_manifest_ref,
            } => Self::SessionStarted {
                session_id: *session_id,
                episode_id: *episode_id,
                generation: *generation,
                boot_manifest_ref: boot_manifest_ref.clone(),
            },
            LoopEvent::AttemptPrepared {
                attempt_id,
                work_unit_id,
                episode_id,
                ordinal,
                idempotency_key,
            } => Self::AttemptPrepared {
                attempt_id: *attempt_id,
                work_unit_id: *work_unit_id,
                episode_id: *episode_id,
                ordinal: *ordinal,
                idempotency_key: idempotency_key.clone(),
            },
            LoopEvent::AttemptStarting { attempt_id } => Self::AttemptStarting {
                attempt_id: *attempt_id,
            },
            LoopEvent::AttemptStarted {
                attempt_id,
                external_ref,
            } => Self::AttemptStarted {
                attempt_id: *attempt_id,
                external_ref: external_ref.clone(),
            },
            LoopEvent::AttemptCollecting { attempt_id } => Self::AttemptCollecting {
                attempt_id: *attempt_id,
            },
            LoopEvent::AttemptCompleted {
                attempt_id,
                work_unit_id,
                result,
            } => Self::AttemptCompleted {
                attempt_id: *attempt_id,
                work_unit_id: *work_unit_id,
                result: result.clone(),
            },
            LoopEvent::VerificationCompleted {
                work_unit_id,
                attempt_id,
                outcome,
            } => Self::VerificationCompleted {
                work_unit_id: *work_unit_id,
                attempt_id: *attempt_id,
                outcome: *outcome,
            },
            LoopEvent::RotationRequested {
                session_id,
                from_generation,
                reason,
                disposition,
            } => Self::RotationRequested {
                session_id: *session_id,
                from_generation: *from_generation,
                reason: *reason,
                disposition: match disposition {
                    RotationDisposition::Drain => StoredRotationDisposition::Drain,
                    RotationDisposition::AbandonGeneration => {
                        StoredRotationDisposition::AbandonGeneration
                    }
                },
            },
            LoopEvent::SessionClosed {
                session_id,
                outcome,
            } => Self::SessionClosed {
                session_id: *session_id,
                outcome: match outcome {
                    SessionCloseOutcome::Closed => StoredSessionCloseOutcome::Closed,
                    SessionCloseOutcome::Failed => StoredSessionCloseOutcome::Failed,
                },
            },
            LoopEvent::EpisodeCommitting { episode_id } => Self::EpisodeCommitting {
                episode_id: *episode_id,
            },
            LoopEvent::CheckpointCommitted { checkpoint } => Self::CheckpointCommitted {
                checkpoint: checkpoint.clone(),
            },
            LoopEvent::LoopSuspended { reason } => Self::LoopSuspended { reason: *reason },
            LoopEvent::LoopCompleted => Self::LoopCompleted,
            LoopEvent::LoopFailed { code } => Self::LoopFailed { code: code.clone() },
        }
    }
}

impl From<StoredPayload> for LoopEvent {
    #[expect(
        clippy::too_many_lines,
        reason = "an exhaustive, one-arm-per-variant translation table: splitting it would \
                  hide the fact that every stored payload decodes back to a domain event"
    )]
    fn from(payload: StoredPayload) -> Self {
        match payload {
            StoredPayload::EpisodeStarted {
                episode_id,
                kind,
                base_checkpoint_version,
            } => Self::EpisodeStarted {
                episode_id,
                kind,
                base_checkpoint_version,
            },
            StoredPayload::ContextHydrated {
                episode_id,
                manifest,
            } => Self::ContextHydrated {
                episode_id,
                manifest,
            },
            StoredPayload::SessionStarted {
                session_id,
                episode_id,
                generation,
                boot_manifest_ref,
            } => Self::SessionStarted {
                session_id,
                episode_id,
                generation,
                boot_manifest_ref,
            },
            StoredPayload::AttemptPrepared {
                attempt_id,
                work_unit_id,
                episode_id,
                ordinal,
                idempotency_key,
            } => Self::AttemptPrepared {
                attempt_id,
                work_unit_id,
                episode_id,
                ordinal,
                idempotency_key,
            },
            StoredPayload::AttemptStarting { attempt_id } => Self::AttemptStarting { attempt_id },
            StoredPayload::AttemptStarted {
                attempt_id,
                external_ref,
            } => Self::AttemptStarted {
                attempt_id,
                external_ref,
            },
            StoredPayload::AttemptCollecting { attempt_id } => {
                Self::AttemptCollecting { attempt_id }
            }
            StoredPayload::AttemptCompleted {
                attempt_id,
                work_unit_id,
                result,
            } => Self::AttemptCompleted {
                attempt_id,
                work_unit_id,
                result,
            },
            StoredPayload::VerificationCompleted {
                work_unit_id,
                attempt_id,
                outcome,
            } => Self::VerificationCompleted {
                work_unit_id,
                attempt_id,
                outcome,
            },
            StoredPayload::RotationRequested {
                session_id,
                from_generation,
                reason,
                disposition,
            } => Self::RotationRequested {
                session_id,
                from_generation,
                reason,
                disposition: match disposition {
                    StoredRotationDisposition::Drain => RotationDisposition::Drain,
                    StoredRotationDisposition::AbandonGeneration => {
                        RotationDisposition::AbandonGeneration
                    }
                },
            },
            StoredPayload::SessionClosed {
                session_id,
                outcome,
            } => Self::SessionClosed {
                session_id,
                outcome: match outcome {
                    StoredSessionCloseOutcome::Closed => SessionCloseOutcome::Closed,
                    StoredSessionCloseOutcome::Failed => SessionCloseOutcome::Failed,
                },
            },
            StoredPayload::EpisodeCommitting { episode_id } => {
                Self::EpisodeCommitting { episode_id }
            }
            StoredPayload::CheckpointCommitted { checkpoint } => {
                Self::CheckpointCommitted { checkpoint }
            }
            StoredPayload::LoopSuspended { reason } => Self::LoopSuspended { reason },
            StoredPayload::LoopCompleted => Self::LoopCompleted,
            StoredPayload::LoopFailed { code } => Self::LoopFailed { code },
        }
    }
}

/// Derives the durable identity of the event at `(loop_id, sequence)`.
///
/// Deterministic by construction: the same journal coordinates always name
/// the same event, whether the caller just wrote it or is reading it back
/// after a restart.
fn derive_event_id(loop_id: LoopId, sequence: u64) -> EventId {
    let mut hasher = Sha256::new();
    hasher.update(b"loopward.event-id/v1");
    hasher.update(loop_id.as_uuid().as_bytes());
    hasher.update(sequence.to_be_bytes());
    let digest = hasher.finalize();

    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // Stamp the UUID version (8, "custom") and the RFC 4122 variant so the
    // derived identity is a well-formed UUID rather than 16 loose bytes.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    EventId::from_uuid(Uuid::from_bytes(bytes))
}

/// The highest journal sequence already recorded for `loop_id`, or `0` if
/// the journal is empty.
pub(crate) async fn max_sequence(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<u64, SqliteStoreError> {
    let row = sqlx::query("SELECT COALESCE(MAX(sequence), 0) FROM loop_events WHERE loop_id = ?1")
        .bind(loop_id.to_string())
        .fetch_one(connection)
        .await?;
    sql_u64("event sequence", row.try_get::<i64, _>(0)?)
}

/// Appends one event to the journal at `sequence`, recording the aggregate
/// revision it advanced the Loop to.
pub(crate) async fn insert(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
    sequence: u64,
    aggregate_revision: u64,
    event: &LoopEvent,
    recorded_at: Timestamp,
) -> Result<(), SqliteStoreError> {
    let payload = serde_json::to_string(&StoredPayload::from(event)).map_err(|source| {
        SqliteStoreError::Encode {
            what: "event payload",
            source,
        }
    })?;

    sqlx::query(
        "INSERT INTO loop_events \
         (loop_id, sequence, aggregate_revision, schema_version, event_type, payload_json, occurred_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(loop_id.to_string())
    .bind(sql_i64("event sequence", sequence)?)
    .bind(sql_i64("aggregate revision", aggregate_revision)?)
    .bind(EVENT_SCHEMA_VERSION)
    .bind(event.kind())
    .bind(payload)
    .bind(format_timestamp(recorded_at)?)
    .execute(connection)
    .await?;

    Ok(())
}

/// Reads a Loop's whole journal in sequence order.
///
/// Phase One journals are bounded by one Loop's lifecycle, so this returns
/// the full slice rather than a page; the port explicitly allows either
/// ("an in-memory store can return everything it has; a real backend may
/// page").
pub(crate) async fn read_all(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<Vec<RecordedEvent>, SqliteStoreError> {
    let rows = sqlx::query(
        "SELECT sequence, aggregate_revision, schema_version, payload_json, occurred_at \
         FROM loop_events WHERE loop_id = ?1 ORDER BY sequence",
    )
    .bind(loop_id.to_string())
    .fetch_all(connection)
    .await?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let sequence = sql_u64("event sequence", row.try_get::<i64, _>(0)?)?;
        let payload: StoredPayload =
            serde_json::from_str(row.try_get::<&str, _>(3)?).map_err(|source| {
                SqliteStoreError::Decode {
                    what: "event payload",
                    source,
                }
            })?;
        events.push(RecordedEvent {
            event_id: derive_event_id(loop_id, sequence),
            sequence,
            aggregate_revision: sql_u64("aggregate revision", row.try_get::<i64, _>(1)?)?,
            schema_version: row.try_get::<String, _>(2)?,
            recorded_at: parse_timestamp(row.try_get::<&str, _>(4)?)?,
            event: payload.into(),
        });
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use loopward_api::ids::CheckpointId;

    use super::{LoopEvent, LoopId, StoredPayload, derive_event_id};

    fn round_trip(event: &LoopEvent) -> LoopEvent {
        let json = serde_json::to_string(&StoredPayload::from(event)).expect("payload encodes");
        let decoded: StoredPayload = serde_json::from_str(&json).expect("payload decodes");
        decoded.into()
    }

    #[test]
    fn payloads_round_trip_through_json() {
        let loop_id = LoopId::new();
        let events = vec![
            LoopEvent::LoopCompleted,
            LoopEvent::LoopFailed {
                code: "budget_exhausted".to_owned(),
            },
            LoopEvent::CheckpointCommitted {
                checkpoint: loopward_core::Checkpoint {
                    id: CheckpointId::new(),
                    loop_id,
                    version: 3,
                    aggregate_revision: 9,
                    parent: Some(CheckpointId::new()),
                    episode_id: None,
                    integrity_digest: Some("sha256:abc".to_owned()),
                },
            },
        ];
        for event in &events {
            assert_eq!(&round_trip(event), event);
        }
    }

    #[test]
    fn the_stored_tag_is_the_journal_event_type() {
        let event = LoopEvent::LoopCompleted;
        let json = serde_json::to_string(&StoredPayload::from(&event)).expect("payload encodes");
        assert!(json.contains(event.kind()), "{json} names {}", event.kind());
    }

    #[test]
    fn derived_event_ids_are_deterministic_and_position_specific() {
        let loop_id = LoopId::new();
        assert_eq!(derive_event_id(loop_id, 1), derive_event_id(loop_id, 1));
        assert_ne!(derive_event_id(loop_id, 1), derive_event_id(loop_id, 2));
        assert_ne!(
            derive_event_id(loop_id, 1),
            derive_event_id(LoopId::new(), 1)
        );
    }
}
