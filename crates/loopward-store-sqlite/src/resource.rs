//! [`FileResourceBroker`]: content-addressed resource storage on the local
//! filesystem, with metadata in the same `SQLite` database as the Loop
//! (`docs/03-durable-state-and-transactions.md` §3, §10).
//!
//! Large bytes never enter `SQLite`. A resource moves through exactly three
//! states:
//!
//! ```text
//! begin    -> <root>/pending/<id>, resources row with finalized = 0
//! finalize -> SHA-256, atomic rename to <root>/sha256/<digest>,
//!             resources row with finalized = 1
//! abort    -> its own pending file and row removed, nothing else touched
//! ```
//!
//! Finalization is atomic where it matters: the bytes become visible under
//! their content address by a single [`std::fs::rename`], which is atomic
//! within a filesystem, and only then does the finalized metadata row
//! appear. A crash at any point leaves either a pending resource (which
//! `docs/03` §10 forbids from entering a Checkpoint) or a complete,
//! content-addressed one — never a half-published one.
//!
//! # Blocking I/O
//!
//! The port is `async`, but these are small, local filesystem calls against
//! a configured resource root, and the brief specifies [`std::fs::rename`]
//! for the publish step precisely because its atomicity is the guarantee
//! being relied on. Phase One therefore performs them inline rather than
//! wrapping every call in a blocking pool.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loopward_api::ids::Timestamp;
use loopward_api::resource::ResourceRef;
use loopward_runtime::ports::{ResourceBeginHint, ResourceBroker, ResourceError, ResourceHandle};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::error::SqliteStoreError;
use crate::store::{format_timestamp, hex_lower, sql_i64, sql_u64};

/// The URI scheme prefix every in-progress write is handed out under. The
/// handle *is* this URI, so an unfinalized resource that leaks into a
/// Checkpoint is recognisable — and rejected — by the store.
const PENDING_PREFIX: &str = "resource://pending/";

/// The URI scheme prefix every finalized, content-addressed resource is
/// published under.
const CONTENT_PREFIX: &str = "resource://sha256/";

/// The directory in-progress writes are buffered in.
const PENDING_DIRECTORY: &str = "pending";

/// The directory finalized content is published into, named for the digest
/// algorithm that addresses it.
const CONTENT_DIRECTORY: &str = "sha256";

/// A [`ResourceBroker`] storing content under a configured resource root
/// and metadata in a `SQLite` database.
///
/// Cheap to clone: the pool and the in-progress hints are shared.
#[derive(Debug, Clone)]
pub struct FileResourceBroker {
    pool: SqlitePool,
    root: PathBuf,
    /// Hints supplied at [`ResourceBroker::begin`] and carried through to
    /// the finalized [`ResourceRef`]. In-progress writes are not durable by
    /// construction — a crash abandons them — so this deliberately lives in
    /// memory rather than in a column.
    hints: Arc<Mutex<HashMap<String, ResourceBeginHint>>>,
}

impl FileResourceBroker {
    /// Opens a broker over `root`, creating the pending and content
    /// directories if they do not exist.
    ///
    /// `pool` should be the same pool the [`crate::SqliteLoopStore`] uses,
    /// so a Checkpoint commit can see — in its own transaction — whether a
    /// referenced resource has been finalized.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteStoreError::ResourceIo`] if the resource root cannot
    /// be created.
    pub fn new(pool: SqlitePool, root: impl Into<PathBuf>) -> Result<Self, SqliteStoreError> {
        let root = root.into();
        for directory in [root.join(PENDING_DIRECTORY), root.join(CONTENT_DIRECTORY)] {
            std::fs::create_dir_all(&directory)
                .map_err(|source| SqliteStoreError::resource_io(&directory, source))?;
        }
        Ok(Self {
            pool,
            root,
            hints: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// The configured resource root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The buffer file backing the in-progress write `pending_id`.
    fn pending_path(&self, pending_id: &str) -> PathBuf {
        self.root.join(PENDING_DIRECTORY).join(pending_id)
    }

    /// The published location of the content addressed by `digest`.
    fn content_path(&self, digest: &str) -> PathBuf {
        self.root.join(CONTENT_DIRECTORY).join(digest)
    }

    /// Resolves a handle to its pending identifier, rejecting anything that
    /// is not an in-progress write this broker issued.
    async fn open_pending(&self, handle: &ResourceHandle) -> Result<String, ResourceError> {
        let uri = handle.as_str();
        let Some(pending_id) = uri.strip_prefix(PENDING_PREFIX) else {
            return Err(ResourceError::UnknownHandle(uri.to_owned()));
        };

        let row = sqlx::query("SELECT finalized FROM resources WHERE resource_uri = ?1")
            .bind(uri)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqliteStoreError::from)?;
        match row {
            Some(row) if row.try_get::<i64, _>(0).map_err(SqliteStoreError::from)? == 0 => {
                Ok(pending_id.to_owned())
            }
            _ => Err(ResourceError::UnknownHandle(uri.to_owned())),
        }
    }

    /// Streams a pending buffer through `SHA-256` without holding all of it
    /// in memory.
    fn digest_of(path: &Path) -> Result<(String, u64), SqliteStoreError> {
        let mut file =
            File::open(path).map_err(|source| SqliteStoreError::resource_io(path, source))?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        let mut size: u64 = 0;
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|source| SqliteStoreError::resource_io(path, source))?;
            if read == 0 {
                break;
            }
            size += read as u64;
            hasher.update(&buffer[..read]);
        }
        Ok((hex_lower(&hasher.finalize()), size))
    }
}

#[async_trait]
impl ResourceBroker for FileResourceBroker {
    async fn begin(&self, hint: ResourceBeginHint) -> Result<ResourceHandle, ResourceError> {
        let pending_id = Uuid::now_v7().to_string();
        let uri = format!("{PENDING_PREFIX}{pending_id}");
        let path = self.pending_path(&pending_id);

        File::create(&path).map_err(|source| SqliteStoreError::resource_io(&path, source))?;

        sqlx::query(
            "INSERT INTO resources \
             (resource_uri, digest, media_type, size, storage_path, finalized, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
        )
        .bind(&uri)
        .bind("")
        .bind(hint.media_type.clone())
        .bind(0_i64)
        .bind(path.display().to_string())
        .bind(format_timestamp(Timestamp::now_utc()).map_err(ResourceError::from)?)
        .execute(&self.pool)
        .await
        .map_err(SqliteStoreError::from)?;

        self.hints
            .lock()
            .expect("resource hint lock poisoned")
            .insert(uri.clone(), hint);

        Ok(ResourceHandle::new(uri))
    }

    async fn write(&self, handle: &ResourceHandle, bytes: &[u8]) -> Result<(), ResourceError> {
        let pending_id = self.open_pending(handle).await?;
        let path = self.pending_path(&pending_id);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|source| SqliteStoreError::resource_io(&path, source))?;
        file.write_all(bytes)
            .map_err(|source| SqliteStoreError::resource_io(&path, source))?;
        Ok(())
    }

    async fn finalize(&self, handle: ResourceHandle) -> Result<ResourceRef, ResourceError> {
        let pending_id = self.open_pending(&handle).await?;
        let pending_path = self.pending_path(&pending_id);

        // Hash the bytes that are actually on disk, then publish them under
        // their own content address with one atomic rename. Identical
        // content renames onto an identical file, which is why replacing an
        // existing target is safe.
        let (digest, size) = Self::digest_of(&pending_path).map_err(ResourceError::from)?;
        let content_path = self.content_path(&digest);
        std::fs::rename(&pending_path, &content_path)
            .map_err(|source| SqliteStoreError::resource_io(&content_path, source))
            .map_err(ResourceError::from)?;

        let uri = format!("{CONTENT_PREFIX}{digest}");
        let hint = self
            .hints
            .lock()
            .expect("resource hint lock poisoned")
            .remove(handle.as_str())
            .unwrap_or_default();

        // Metadata last, and only once the bytes are durable at their
        // content address: the pending row disappears and the finalized row
        // appears in one transaction, so a resource is never observably
        // both.
        let mut transaction = self.pool.begin().await.map_err(SqliteStoreError::from)?;
        sqlx::query("DELETE FROM resources WHERE resource_uri = ?1")
            .bind(handle.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(SqliteStoreError::from)?;
        sqlx::query(
            "INSERT INTO resources \
             (resource_uri, digest, media_type, size, storage_path, finalized, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6) \
             ON CONFLICT(resource_uri) DO NOTHING",
        )
        .bind(&uri)
        .bind(format!("sha256:{digest}"))
        .bind(hint.media_type.clone())
        .bind(sql_i64("resource size", size).map_err(ResourceError::from)?)
        .bind(content_path.display().to_string())
        .bind(format_timestamp(Timestamp::now_utc()).map_err(ResourceError::from)?)
        .execute(&mut *transaction)
        .await
        .map_err(SqliteStoreError::from)?;
        transaction.commit().await.map_err(SqliteStoreError::from)?;

        Ok(ResourceRef {
            uri,
            media_type: hint.media_type,
            digest: Some(format!("sha256:{digest}")),
            version: None,
            size: Some(size),
            summary: hint.summary,
        })
    }

    async fn abort(&self, handle: ResourceHandle) -> Result<(), ResourceError> {
        let pending_id = self.open_pending(&handle).await?;
        let path = self.pending_path(&pending_id);

        // Only this write's own pending buffer, never finalized content.
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(SqliteStoreError::resource_io(&path, source).into()),
        }

        sqlx::query("DELETE FROM resources WHERE resource_uri = ?1 AND finalized = 0")
            .bind(handle.as_str())
            .execute(&self.pool)
            .await
            .map_err(SqliteStoreError::from)?;

        self.hints
            .lock()
            .expect("resource hint lock poisoned")
            .remove(handle.as_str());

        Ok(())
    }

    async fn resolve(&self, reference: &ResourceRef) -> Result<Vec<u8>, ResourceError> {
        let row = sqlx::query(
            "SELECT storage_path, size FROM resources WHERE resource_uri = ?1 AND finalized = 1",
        )
        .bind(&reference.uri)
        .fetch_optional(&self.pool)
        .await
        .map_err(SqliteStoreError::from)?;

        let Some(row) = row else {
            return Err(ResourceError::UnresolvableReference(reference.uri.clone()));
        };
        let path = PathBuf::from(
            row.try_get::<String, _>(0)
                .map_err(SqliteStoreError::from)?,
        );
        let size = sql_u64(
            "resource size",
            row.try_get::<i64, _>(1).map_err(SqliteStoreError::from)?,
        )
        .map_err(ResourceError::from)?;

        let bytes = std::fs::read(&path)
            .map_err(|source| SqliteStoreError::resource_io(&path, source))
            .map_err(ResourceError::from)?;
        if bytes.len() as u64 != size {
            return Err(ResourceError::UnresolvableReference(reference.uri.clone()));
        }
        Ok(bytes)
    }
}
