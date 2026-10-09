//! Lease acquisition, renewal, release, and the fencing token every write is
//! validated against (`docs/03-durable-state-and-transactions.md` §7).
//!
//! At most one driver may mutate a Loop. Reacquisition — by a different
//! runtime instance after the lease expired, or by the same instance after
//! losing and regaining it — strictly increments the fencing token, and
//! every [`crate::SqliteLoopStore`] write compares the caller's token to
//! the stored one inside the write transaction. A driver that resumes after
//! a pause therefore cannot commit, even though it is still running.
//!
//! `loop_leases.loop_id` is a foreign key onto `loops(loop_id)`: a Loop
//! becomes leasable once its genesis Checkpoint exists. An uncreated Loop
//! consequently has no lease row and a current fencing token of `0`, which
//! is exactly the token a genesis
//! [`commit_checkpoint`](loopward_runtime::ports::LoopStore::commit_checkpoint)
//! must present.

use loopward_api::ids::{LoopId, Timestamp};
use loopward_runtime::ports::{AcquireLease, LoopLease, RuntimeInstanceId, StoreError};
use sqlx::{Row, SqliteConnection};

use crate::error::SqliteStoreError;
use crate::store::{format_timestamp, parse_timestamp, sql_i64, sql_u64};

/// One `loop_leases` row.
struct StoredLease {
    holder_id: RuntimeInstanceId,
    fencing_token: u64,
    expires_at: Timestamp,
}

/// The fencing token currently valid for `loop_id`.
///
/// `0` when the Loop has never been leased — including when the Loop does
/// not exist yet — so a genesis commit is fenced by `0` and every later
/// write is fenced by a real, strictly increasing lease token.
pub(crate) async fn current_fencing_token(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<u64, SqliteStoreError> {
    let row = sqlx::query("SELECT fencing_token FROM loop_leases WHERE loop_id = ?1")
        .bind(loop_id.to_string())
        .fetch_optional(connection)
        .await?;
    match row {
        Some(row) => sql_u64("fencing token", row.try_get::<i64, _>(0)?),
        None => Ok(0),
    }
}

/// Reads the whole lease row, if one exists.
async fn read_lease(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<Option<StoredLease>, SqliteStoreError> {
    let Some(row) = sqlx::query(
        "SELECT holder_id, fencing_token, expires_at FROM loop_leases WHERE loop_id = ?1",
    )
    .bind(loop_id.to_string())
    .fetch_optional(connection)
    .await?
    else {
        return Ok(None);
    };

    let holder_text = row.try_get::<&str, _>(0)?;
    let holder_id = holder_text.parse::<RuntimeInstanceId>().map_err(|_| {
        SqliteStoreError::InconsistentRequest {
            loop_id,
            detail: format!("stored lease holder {holder_text} is not a runtime instance id"),
        }
    })?;

    Ok(Some(StoredLease {
        holder_id,
        fencing_token: sql_u64("fencing token", row.try_get::<i64, _>(1)?)?,
        expires_at: parse_timestamp(row.try_get::<&str, _>(2)?)?,
    }))
}

/// Acquires the exclusive driving lease, incrementing the fencing token.
///
/// Runs entirely inside the caller's `BEGIN IMMEDIATE` transaction: the
/// conflict check, the token increment, and the write are one atomic step,
/// so two runtime instances racing for the same Loop cannot both come away
/// believing they hold it.
pub(crate) async fn acquire_in_transaction(
    connection: &mut SqliteConnection,
    request: AcquireLease,
    now: Timestamp,
) -> Result<LoopLease, StoreError> {
    let AcquireLease {
        loop_id,
        holder_id,
        ttl,
    } = request;

    if !loop_exists(connection, loop_id).await? {
        return Err(StoreError::LoopNotFound(loop_id));
    }

    let next_token = match read_lease(connection, loop_id).await? {
        Some(existing) => {
            // A different instance may only take over an expired lease; the
            // same instance may always reacquire (§7).
            if existing.holder_id != holder_id && existing.expires_at > now {
                return Err(StoreError::LeaseConflict(loop_id));
            }
            existing.fencing_token + 1
        }
        None => 1,
    };

    let lease = LoopLease {
        loop_id,
        holder_id,
        fencing_token: next_token,
        acquired_at: now,
        expires_at: now + ttl,
    };

    sqlx::query(
        "INSERT INTO loop_leases (loop_id, holder_id, fencing_token, acquired_at, expires_at) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(loop_id) DO UPDATE SET \
             holder_id = excluded.holder_id, \
             fencing_token = excluded.fencing_token, \
             acquired_at = excluded.acquired_at, \
             expires_at = excluded.expires_at",
    )
    .bind(loop_id.to_string())
    .bind(holder_id.to_string())
    .bind(sql_i64("fencing token", lease.fencing_token).map_err(StoreError::from)?)
    .bind(format_timestamp(lease.acquired_at).map_err(StoreError::from)?)
    .bind(format_timestamp(lease.expires_at).map_err(StoreError::from)?)
    .execute(connection)
    .await
    .map_err(SqliteStoreError::from)?;

    Ok(lease)
}

/// Renews an already-held lease, extending its expiry without changing its
/// fencing token.
///
/// The fencing token and holder are part of the `UPDATE`'s own `WHERE`
/// clause, so the check that this caller still holds the lease and the
/// write that extends it are one statement: there is no window in which a
/// concurrent acquisition could slip between them.
pub(crate) async fn renew_in_transaction(
    connection: &mut SqliteConnection,
    lease: &LoopLease,
    now: Timestamp,
) -> Result<LoopLease, StoreError> {
    let Some(held) = read_lease(connection, lease.loop_id).await? else {
        return Err(StoreError::LeaseNotHeld(lease.loop_id));
    };
    if held.fencing_token != lease.fencing_token {
        return Err(StoreError::StaleFencingToken {
            loop_id: lease.loop_id,
            given: lease.fencing_token,
            current: held.fencing_token,
        });
    }
    if held.holder_id != lease.holder_id {
        return Err(StoreError::LeaseConflict(lease.loop_id));
    }

    // Preserve the TTL this lease was acquired with rather than inventing a
    // new one: renewal extends the same lease, it does not redefine it.
    let ttl = lease.expires_at - lease.acquired_at;
    let renewed = LoopLease {
        acquired_at: now,
        expires_at: now + ttl,
        ..*lease
    };

    let updated = sqlx::query(
        "UPDATE loop_leases SET acquired_at = ?1, expires_at = ?2 \
         WHERE loop_id = ?3 AND holder_id = ?4 AND fencing_token = ?5",
    )
    .bind(format_timestamp(renewed.acquired_at).map_err(StoreError::from)?)
    .bind(format_timestamp(renewed.expires_at).map_err(StoreError::from)?)
    .bind(renewed.loop_id.to_string())
    .bind(renewed.holder_id.to_string())
    .bind(sql_i64("fencing token", renewed.fencing_token).map_err(StoreError::from)?)
    .execute(connection)
    .await
    .map_err(SqliteStoreError::from)?;

    if updated.rows_affected() == 0 {
        return Err(StoreError::LeaseNotHeld(lease.loop_id));
    }
    Ok(renewed)
}

/// Hands a held lease back early by expiring it as of `now`.
///
/// The row stays, and with it the fencing token: deleting it would make
/// [`current_fencing_token`] fall back to `0` and let the next acquisition
/// hand out token `1` again. Expiring it instead means the next
/// acquisition, by any holder, takes the ordinary "expired lease" path in
/// [`acquire_in_transaction`] and still increments the token.
///
/// Only the current lease is expired — this holder, this fencing token, not
/// yet expired — so releasing a superseded or already-released lease is a
/// no-op rather than an error, and can never cut short a later
/// acquisition's lease. Runs inside the caller's `BEGIN IMMEDIATE`
/// transaction, so the check and the write cannot be split by a concurrent
/// acquisition.
pub(crate) async fn release_in_transaction(
    connection: &mut SqliteConnection,
    lease: &LoopLease,
    now: Timestamp,
) -> Result<(), StoreError> {
    let Some(held) = read_lease(connection, lease.loop_id).await? else {
        return Ok(());
    };
    if held.holder_id != lease.holder_id
        || held.fencing_token != lease.fencing_token
        || held.expires_at <= now
    {
        return Ok(());
    }

    sqlx::query(
        "UPDATE loop_leases SET expires_at = ?1 \
         WHERE loop_id = ?2 AND holder_id = ?3 AND fencing_token = ?4",
    )
    .bind(format_timestamp(now).map_err(StoreError::from)?)
    .bind(lease.loop_id.to_string())
    .bind(lease.holder_id.to_string())
    .bind(sql_i64("fencing token", lease.fencing_token).map_err(StoreError::from)?)
    .execute(connection)
    .await
    .map_err(SqliteStoreError::from)?;
    Ok(())
}

/// Whether a durable Loop row exists.
async fn loop_exists(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
) -> Result<bool, SqliteStoreError> {
    let row = sqlx::query("SELECT 1 FROM loops WHERE loop_id = ?1")
        .bind(loop_id.to_string())
        .fetch_optional(connection)
        .await?;
    Ok(row.is_some())
}
