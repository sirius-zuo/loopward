# Code Review Fixes (`/code-review pr #2`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 10 findings from the `/code-review pr #2` run (see conversation history for the full JSON report) without changing any documented, intentional behavior.

**Architecture:** Each finding is an independent, surgical fix scoped to the file(s) it was found in. No cross-cutting refactor. Findings 1–6 are correctness bugs; 7 is a maintainability gap; 8–9 are efficiency; 10 is dead data. Two findings (2 and 6) required reading `loopward-core`'s reducer/invariants to ground a correct fix rather than a superficial one — that research is folded into each task's rationale below so the implementer doesn't have to redo it.

**Tech Stack:** Rust, `sqlx` (SQLite), `tokio`, the existing `loopward-core` / `loopward-runtime` / `loopward-store-sqlite` / `loopward-api` / `loopward-observability` workspace crates.

## Global Constraints

- Workspace `rust-version = "1.97"`, `edition = "2024"` (`Cargo.toml`) — don't use anything newer.
- `[workspace.lints.clippy] all = "deny"`, `pedantic = "deny"` (`Cargo.toml`) — every new function needs a doc comment on `pub`/`pub(crate)` items, matching the file's existing style (only comment the *why*, never restate the signature).
- Never bypass hooks or `--no-verify`. Every task ends with `cargo test -p <crate>` (or a full `cargo test --workspace` for cross-crate changes) actually passing before the commit.
- Follow this codebase's established idiom for "no `LifecycleCommand` exists for this transition": a direct, lease-fenced write validated by `validate_invariants` before it lands (see `crates/loopward-runtime/src/recovery/mod.rs` module doc and `LoopController::resume`/`cancel`). Don't invent a new pattern where an existing one already covers the case.
- Findings are fixed in the report's severity order (correctness first). Tasks are independent — they touch disjoint files except where noted — so they can be done in any order, but doing them in order keeps the diff reviewable against the original report.

---

### Task 1: Fix the crash window in `FileResourceBroker::finalize`

**Finding:** `resource.rs:216` — bytes are renamed to their content-addressed path before the DB transaction that flips `finalized` commits. A crash in between leaves a `resources` row that still says `pending` pointing at a file that no longer exists, and a retried `finalize()` fails trying to re-hash a pending buffer that was already moved away.

**Root cause (confirmed by reading the full file):** `begin()` inserts the pending row with `digest = ""`. `finalize()` computes the real digest only in memory, renames the file, and only *then* would it have anywhere durable to put the digest — but the metadata swap it does next (delete pending row, insert finalized row) is the very transaction that can be lost to a crash. There is currently no durable place holding the digest between "bytes renamed" and "metadata swapped", so a retried `finalize()` on that pending URI has no way to know what digest the vanished bytes were published under.

**Fix:** persist the computed digest onto the still-pending row *before* renaming (a single, cheap `UPDATE`, durable the instant it's written — SQLite autocommits a bare statement). On a retried `finalize()` where the pending file is already gone, recover the digest from that column instead of re-hashing, and derive size from the published file at `content_path(digest)` instead of the vanished pending file.

**Files:**
- Modify: `crates/loopward-store-sqlite/src/resource.rs:206-263` (the `finalize` method)
- Test: `crates/loopward-store-sqlite/src/resource.rs` (new `#[cfg(test)] mod tests` — there isn't one yet in this file; the crash window needs access to the private `pending_path`/`content_path`/pool, which only an in-crate unit test has)

**Interfaces:**
- Consumes: `FileResourceBroker` fields (`pool`, `hints`) and private helpers `pending_path`, `content_path`, `digest_of`, `open_pending` — unchanged signatures, all already `impl FileResourceBroker`-private.
- Produces: no public signature change. `finalize`'s behavior extends to also succeed when called a second time after the crash window described above.

- [ ] **Step 1: Add the crate-local test harness and the failing test**

Add to the bottom of `crates/loopward-store-sqlite/src/resource.rs`:

```rust
#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;
    use loopward_runtime::ports::ResourceBeginHint;

    async fn broker() -> (FileResourceBroker, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let pool = SqlitePoolOptions::new()
            .connect(&format!(
                "sqlite://{}/loopward.db?mode=rwc",
                directory.path().display()
            ))
            .await
            .expect("pool opens");
        crate::run_migrations(&pool).await.expect("migrations apply");
        let broker = FileResourceBroker::new(pool, directory.path().join("resources"))
            .expect("broker opens");
        (broker, directory)
    }

    #[tokio::test]
    async fn finalize_recovers_after_a_crash_between_rename_and_metadata_commit() {
        let (broker, _directory) = broker().await;
        let handle = broker
            .begin(ResourceBeginHint {
                media_type: Some("text/plain".to_owned()),
                summary: None,
            })
            .await
            .expect("begin succeeds");
        broker
            .write(&handle, b"hello crash window")
            .await
            .expect("write succeeds");

        // Simulate exactly the crash window `finalize` must recover from:
        // the bytes are already published under their content address (the
        // atomic rename succeeded) but the metadata swap that would flip
        // the row to `finalized = 1` never ran, so the row still names the
        // pending URI.
        let pending_id = handle
            .as_str()
            .strip_prefix(PENDING_PREFIX)
            .expect("pending uri");
        let pending_path = broker.pending_path(pending_id);
        let (digest, _size) =
            FileResourceBroker::digest_of(&pending_path).expect("digest computes");
        sqlx::query("UPDATE resources SET digest = ?1 WHERE resource_uri = ?2")
            .bind(format!("sha256:{digest}"))
            .bind(handle.as_str())
            .execute(&broker.pool)
            .await
            .expect("digest recorded");
        let content_path = broker.content_path(&digest);
        std::fs::rename(&pending_path, &content_path).expect("simulated publish");

        let resolved = broker
            .finalize(handle)
            .await
            .expect("finalize recovers from the crash window instead of erroring");
        assert_eq!(
            resolved.digest.as_deref(),
            Some(format!("sha256:{digest}").as_str())
        );

        let bytes = broker
            .resolve(&resolved)
            .await
            .expect("published content resolves");
        assert_eq!(bytes, b"hello crash window");
    }
}
```

Add `loopward-runtime` as a dev-dependency if it isn't already available to this crate's tests — check `crates/loopward-store-sqlite/Cargo.toml`'s `[dev-dependencies]` first; `ResourceBeginHint` is already used from `loopward_runtime::ports` in `crates/loopward-store-sqlite/tests/transaction_contract.rs`, so the dependency already exists at the `[dependencies]` level and is fine to use from an in-crate `#[cfg(test)]` module too.

- [ ] **Step 2: Run it and confirm it fails on the current code**

Run: `cargo test -p loopward-store-sqlite --lib finalize_recovers_after_a_crash_between_rename_and_metadata_commit`
Expected: FAIL — `finalize` calls `Self::digest_of(&pending_path)` on a file that no longer exists (`File::open` returns `NotFound`), surfaced as a `SqliteStoreError::ResourceIo` / `ResourceError`.

- [ ] **Step 3: Implement the fix**

Replace the body of `finalize` in `crates/loopward-store-sqlite/src/resource.rs:206-263`:

```rust
    async fn finalize(&self, handle: ResourceHandle) -> Result<ResourceRef, ResourceError> {
        let pending_id = self.open_pending(&handle).await?;
        let pending_path = self.pending_path(&pending_id);

        // Hash the bytes that are actually on disk, then publish them under
        // their own content address with one atomic rename. Identical
        // content renames onto an identical file, which is why replacing an
        // existing target is safe.
        let (digest, size) = if pending_path.exists() {
            let (digest, size) = Self::digest_of(&pending_path).map_err(ResourceError::from)?;

            // Record the digest before publishing the bytes: if the process
            // crashes between the rename below and the metadata swap at the
            // end of this method, this is what lets a retried `finalize` on
            // the same handle recover — the bytes are gone from
            // `pending_path` by then, so their digest can only be recovered
            // from here.
            sqlx::query("UPDATE resources SET digest = ?1 WHERE resource_uri = ?2")
                .bind(format!("sha256:{digest}"))
                .bind(handle.as_str())
                .execute(&self.pool)
                .await
                .map_err(SqliteStoreError::from)?;

            let content_path = self.content_path(&digest);
            std::fs::rename(&pending_path, &content_path)
                .map_err(|source| SqliteStoreError::resource_io(&content_path, source))
                .map_err(ResourceError::from)?;
            (digest, size)
        } else {
            self.recover_published_digest(handle.as_str(), &pending_path)
                .await?
        };

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
        .bind(self.content_path(&digest).display().to_string())
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

    /// Recovers `(digest, size)` for a pending resource whose bytes were
    /// already published (its pending file is gone) but whose metadata swap
    /// never committed — the crash window between the atomic rename and the
    /// transaction at the end of [`Self::finalize`].
    async fn recover_published_digest(
        &self,
        uri: &str,
        pending_path: &Path,
    ) -> Result<(String, u64), ResourceError> {
        let row = sqlx::query("SELECT digest FROM resources WHERE resource_uri = ?1")
            .bind(uri)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqliteStoreError::from)?;
        let Some(row) = row else {
            return Err(SqliteStoreError::resource_io(
                pending_path,
                std::io::Error::new(std::io::ErrorKind::NotFound, "pending buffer missing"),
            )
            .into());
        };
        let recorded: String = row.try_get(0).map_err(SqliteStoreError::from)?;
        let digest = recorded
            .strip_prefix("sha256:")
            .ok_or_else(|| {
                SqliteStoreError::resource_io(
                    pending_path,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "pending buffer vanished before its digest was recorded",
                    ),
                )
            })?
            .to_owned();
        let content_path = self.content_path(&digest);
        let size = std::fs::metadata(&content_path)
            .map_err(|source| SqliteStoreError::resource_io(&content_path, source))?
            .len();
        Ok((digest, size))
    }
```

- [ ] **Step 4: Run the new test and the full resource/transaction suite**

Run: `cargo test -p loopward-store-sqlite`
Expected: PASS, including `finalize_recovers_after_a_crash_between_rename_and_metadata_commit` and the pre-existing `finalized_resources_are_content_addressed_and_abort_removes_only_its_own_pending_file` in `tests/transaction_contract.rs`.

- [ ] **Step 5: Commit**

```bash
git add crates/loopward-store-sqlite/src/resource.rs
git commit -m "fix(resource): recover finalize from the rename/commit crash window"
```

---

### Task 2: Stop confirmed-Failed/Cancelled attempts from being recorded as durably Ambiguous

**Finding:** `recovery/reconcile.rs:477` — `reconcile_execution_attempt` treats a runner's *confirmed* `Failed`/`Cancelled` report the same as a genuinely `Unknown` one, forcing the reducer into `Ambiguous` (which permanently blocks the Loop until an operator intervenes), while the sibling `reconcile_dangling_attempt` in `recovery/mod.rs` trusts the same confirmed report and retires the Attempt for an automatic retry.

**Root cause (confirmed by reading `loopward-core`'s reducer):** `apply_rotation_requested` (`crates/loopward-core/src/reducer.rs:1096-1137`) maps a *pre-existing* `AttemptStatus::Starting | Running` to `Ambiguous` — but an attempt status that is **already** `Failed` or `Cancelled` falls through its `other => other` arm and is left untouched. `reconcile_execution_attempt` currently calls `apply(RequestRotation{reason: RunnerFailure})` against the *unmodified* state (`attempt.status` still `Running`/`Prepared` from before the crash), which is exactly why it lands on `Ambiguous`. `require_reconciled_attempt` (`crates/loopward-core/src/reducer.rs:231-249`) only blocks a *next* Episode on an attempt that is non-terminal or `Ambiguous` — `Failed`/`Cancelled` is not blocking. So if the confirmed terminal status is written onto `state.attempt` *before* calling `RequestRotation`, the reducer's own `other => other` branch preserves it, and the Loop ends up in exactly the state `reconcile_dangling_attempt` already produces for the "episode already terminated" case: suspended once (via `apply_loop_suspended`, which also parks the Work Unit at `Suspended`), then unstuck to `Pending` by `RecoveryManager::unstick_work_unit` on the next `recover()` call — the same two-call recovery pattern this module already uses elsewhere.

**Fix:** in the `Ok(ReconcileOutcome::Failed | ReconcileOutcome::Cancelled | ReconcileOutcome::Unknown) | Err(_)` arm, split confirmed `Failed`/`Cancelled` from `Unknown`/`Err(_)`. For the confirmed cases, mutate a local copy of `state.attempt` to the confirmed terminal status (mirroring `retire_dangling_attempt`'s pattern in the same crate) before calling `RequestRotation`.

**Files:**
- Modify: `crates/loopward-runtime/src/recovery/reconcile.rs:467-506`
- Test: `crates/loopward-runtime/tests/recovery_matrix.rs`

**Interfaces:**
- Consumes: `AttemptStatus` (already imported in `reconcile.rs` via `use loopward_core::{... AttemptStatus ...}`), `WorkUnitAttempt` fields `.status`, `.result_ref` (already used elsewhere in this file the same way, e.g. `retire_dangling_attempt` in `mod.rs`).
- Produces: no signature change to `reconcile_execution_attempt`. `EpisodeRecoveryOutcome::Suspended { reason: SuspensionReason::Recovery }` is still returned for this arm — only the durable `attempt.status` differs.

- [ ] **Step 1: Read the existing recovery-matrix test file to match its fixture style**

Run: `grep -n "async fn\|ReconcileOutcome::Failed\|ReconcileOutcome::Cancelled" crates/loopward-runtime/tests/recovery_matrix.rs` and open the file to find how a fake runner's `reconcile()` is stubbed to return `ReconcileOutcome::Failed`/`Cancelled` — reuse that exact fixture/runner-fake pattern for the new test below rather than inventing a new one.

- [ ] **Step 2: Write the failing test**

Add to `crates/loopward-runtime/tests/recovery_matrix.rs` (adapt the runner-fake construction to whatever helper the file already uses for a fixed `ReconcileOutcome`):

```rust
#[tokio::test]
async fn a_confirmed_failed_attempt_is_recorded_failed_not_ambiguous() {
    let harness = /* existing harness setup, e.g. Harness::open().await */;
    let loop_id = /* drive the harness to an Executing Episode with a Running Attempt,
                      exactly like the existing test(s) that reach reconcile_execution_attempt */;

    /* configure the fake runner's `reconcile` to return `Ok(ReconcileOutcome::Failed)` */

    let outcome = harness
        .recovery
        .recover(loop_id)
        .await
        .expect("recovery completes");
    assert!(matches!(
        outcome,
        loopward_runtime::recovery::RecoveryOutcome::LoopSuspended {
            reason: loopward_api::runner::SuspensionReason::Recovery
        }
    ));

    let stored = harness.store.load(loop_id).await.expect("loop loads");
    let attempt = stored.state.attempt.expect("attempt on record");
    assert_eq!(attempt.status, loopward_core::AttemptStatus::Failed);
    assert!(attempt.result_ref.is_none());

    // And it self-heals on the next recovery pass, exactly like a dangling
    // Attempt reconciled after the Episode had already terminated.
    let unstuck = harness
        .recovery
        .recover(loop_id)
        .await
        .expect("second recovery completes");
    assert_eq!(
        unstuck,
        loopward_runtime::recovery::RecoveryOutcome::WorkUnitReadyForRetry
    );
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p loopward-runtime --test recovery_matrix a_confirmed_failed_attempt_is_recorded_failed_not_ambiguous -- --nocapture`
Expected: FAIL — `attempt.status` is `Ambiguous`, not `Failed`.

- [ ] **Step 4: Implement the fix**

In `crates/loopward-runtime/src/recovery/reconcile.rs`, replace the final match arm (currently lines 478-505):

```rust
            Ok(ReconcileOutcome::Failed | ReconcileOutcome::Cancelled) => {
                // Confirmed terminal: the external effect is over. Record
                // the actual status *before* requesting rotation, so the
                // reducer's `apply_rotation_requested` (whose `other =>
                // other` arm preserves an already-terminal status) does not
                // fall back to its conservative `Ambiguous` mapping for a
                // pre-crash `Running`/`Prepared` attempt — that mapping
                // exists for effects that genuinely cannot be observed,
                // which this is not. `Failed`/`Cancelled` is not blocking
                // for `require_reconciled_attempt`, so this self-heals on
                // the next `recover()` the same way a dangling Attempt
                // reconciled after Episode termination already does.
                let confirmed_status = if matches!(reconciled, Ok(ReconcileOutcome::Cancelled)) {
                    loopward_core::AttemptStatus::Cancelled
                } else {
                    loopward_core::AttemptStatus::Failed
                };
                let mut state = state;
                if let Some(recorded) = state.attempt.as_mut()
                    && recorded.id == attempt.id
                {
                    recorded.status = confirmed_status;
                    recorded.result_ref = None;
                }
                let state = self
                    .controller
                    .apply(
                        loop_id,
                        lease,
                        &state,
                        LifecycleCommand::RequestRotation {
                            reason: RotationReason::RunnerFailure,
                        },
                    )
                    .await?;
                self.controller
                    .ensure_episode_terminal_and_commit(
                        loop_id,
                        lease,
                        state,
                        episode_id,
                        SuspensionReason::Recovery,
                    )
                    .await?;
                Ok(EpisodeRecoveryOutcome::Suspended {
                    reason: SuspensionReason::Recovery,
                })
            }
            // `Unknown`, or the reconcile call itself failing: neither
            // confirms the external effect is over, so neither can be
            // safely distinguished from an unobservable effect — this is
            // the case the reducer's `Ambiguous` mapping exists for.
            Ok(ReconcileOutcome::Unknown) | Err(_) => {
                let state = self
                    .controller
                    .apply(
                        loop_id,
                        lease,
                        &state,
                        LifecycleCommand::RequestRotation {
                            reason: RotationReason::RunnerFailure,
                        },
                    )
                    .await?;
                self.controller
                    .ensure_episode_terminal_and_commit(
                        loop_id,
                        lease,
                        state,
                        episode_id,
                        SuspensionReason::Recovery,
                    )
                    .await?;
                Ok(EpisodeRecoveryOutcome::Suspended {
                    reason: SuspensionReason::Recovery,
                })
            }
```

Update the doc comment directly above this match block (currently describing the old "Known simplification" — see the lines right before `Ok(ReconcileOutcome::Failed | ReconcileOutcome::Cancelled | ReconcileOutcome::Unknown,) | Err(_) => {`) to drop the "Known simplification: a confirmed Failed/Cancelled report is recorded as durably Ambiguous" sentence, since that is no longer what the code does.

- [ ] **Step 5: Run the test and the full recovery suite**

Run: `cargo test -p loopward-runtime --test recovery_matrix`
Expected: PASS, including every pre-existing case in that file (check specifically for any test that previously asserted `AttemptStatus::Ambiguous` after a confirmed `Failed`/`Cancelled` reconcile outcome — update its expectation to `Failed`/`Cancelled` if one exists, since that was asserting the bug).

- [ ] **Step 6: Commit**

```bash
git add crates/loopward-runtime/src/recovery/reconcile.rs crates/loopward-runtime/tests/recovery_matrix.rs
git commit -m "fix(recovery): record confirmed runner failure/cancellation, not Ambiguous"
```

---

### Task 3: Cover every referenced resource in the recovery pre-flight integrity check

**Finding:** `recovery/reconcile.rs:130` — `check_resource_integrity` only resolves `episode.hydrated_manifest`, `attempt.boot_package_ref`, and `attempt.result_ref`. It omits `session.boot_manifest_ref` and every `work_unit.inputs[].uri`, both of which `checkpoint.rs`'s `referenced_resources` (the function that runs the *identical* check at commit time) covers.

**Fix:** add the same two categories to the `refs` vector `check_resource_integrity` builds, mirroring `referenced_resources` in `crates/loopward-store-sqlite/src/checkpoint.rs:551-574`.

**Files:**
- Modify: `crates/loopward-runtime/src/recovery/reconcile.rs:130-165`
- Test: `crates/loopward-runtime/tests/recovery_matrix.rs`

**Interfaces:**
- Consumes: `AggregateState.session: Option<RuntimeSession>` (`.boot_manifest_ref: ResourceRef`), `AggregateState.work_unit: Option<WorkUnit>` (`.inputs: Vec<ResourceRef>`) — both already used the same way in `checkpoint.rs`.
- Produces: no signature change to `check_resource_integrity`.

- [ ] **Step 1: Write the failing test**

Add to `crates/loopward-runtime/tests/recovery_matrix.rs`, following whatever fixture pattern the file already uses to reach `EpisodeStatus::Executing` with a live Session (look for an existing test that reaches `recover_executing`/`reconcile_active_episode` for the pattern):

```rust
#[tokio::test]
async fn recovery_suspends_when_the_session_boot_manifest_is_unresolvable() {
    let harness = /* existing harness setup */;
    let loop_id = /* drive to an Executing Episode with a live Session */;

    // Delete (or otherwise make unresolvable) the resource backing
    // `session.boot_manifest_ref` specifically — not the Episode's
    // `hydrated_manifest`, not the Attempt's `boot_package_ref` — to prove
    // this exact category is checked.
    /* e.g. remove the file backing it from the resource root, or use a
       broker fake that reports it UnresolvableReference */

    let outcome = harness
        .recovery
        .recover(loop_id)
        .await
        .expect("recovery completes");
    assert!(matches!(
        outcome,
        loopward_runtime::recovery::RecoveryOutcome::LoopSuspended {
            reason: loopward_api::runner::SuspensionReason::Recovery
        }
    ));
}

#[tokio::test]
async fn recovery_suspends_when_a_work_unit_input_is_unresolvable() {
    let harness = /* existing harness setup */;
    let loop_id = /* drive to an Executing Episode whose Work Unit declares an input */;

    /* make the resource backing `work_unit.inputs[0].uri` unresolvable */

    let outcome = harness
        .recovery
        .recover(loop_id)
        .await
        .expect("recovery completes");
    assert!(matches!(
        outcome,
        loopward_runtime::recovery::RecoveryOutcome::LoopSuspended {
            reason: loopward_api::runner::SuspensionReason::Recovery
        }
    ));
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p loopward-runtime --test recovery_matrix recovery_suspends_when_the_session_boot_manifest_is_unresolvable recovery_suspends_when_a_work_unit_input_is_unresolvable`
Expected: FAIL — recovery proceeds past `check_resource_integrity` undetected (it returns `Ok(None)`), then fails deeper (or succeeds incorrectly) instead of cleanly suspending.

- [ ] **Step 3: Implement the fix**

In `crates/loopward-runtime/src/recovery/reconcile.rs`, in `check_resource_integrity` (lines 136-147), add the two missing categories right after the existing `attempt` block:

```rust
        let mut refs: Vec<ResourceRef> = Vec::new();
        if let Some(episode) = &state.episode
            && let Some(manifest_ref) = &episode.hydrated_manifest
        {
            refs.push(manifest_ref.clone());
        }
        if let Some(session) = &state.session {
            refs.push(session.boot_manifest_ref.clone());
        }
        if let Some(work_unit) = &state.work_unit {
            refs.extend(work_unit.inputs.iter().cloned());
        }
        if let Some(attempt) = &state.attempt {
            refs.push(attempt.boot_package_ref.clone());
            if let Some(result_ref) = &attempt.result_ref {
                refs.push(result_ref.clone());
            }
        }
```

Update the method's doc comment (currently: "Resolves every resource an in-flight Episode currently references (the hydrated Boot Package manifest, the Attempt's Boot Package, and its collected result, when present)...") to also name the Session's boot manifest and the Work Unit's inputs, so the comment matches `checkpoint.rs`'s `referenced_resources` doc.

- [ ] **Step 4: Run the tests and the full recovery suite**

Run: `cargo test -p loopward-runtime --test recovery_matrix`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/loopward-runtime/src/recovery/reconcile.rs crates/loopward-runtime/tests/recovery_matrix.rs
git commit -m "fix(recovery): check every resource category the commit-time check covers"
```

---

### Task 4: Actually populate `resource_grants` for on-demand-fetchable manifest items

**Finding:** `controller/advance.rs:970` (and `recovery/reconcile.rs:578`) — `WorkUnitRequest.resource_grants` is hardcoded to `Vec::new()`, even though `context/hydrator.rs:452-453` prices an `OnDemandReference` candidate as cheap `ReferenceOnly` whenever the runner declares `supports_resource_fetch: true`, implying the runner will fetch the real content later using a grant it's never actually given.

**Fix:** add a helper that turns every `ReferenceOnly`-fidelity item in a hydrated `BootPackageManifest` into a `ResourceGrant`, and call it at both `WorkUnitRequest` construction sites.

**Files:**
- Modify: `crates/loopward-runtime/src/context/mod.rs` (new function, plus imports)
- Modify: `crates/loopward-runtime/src/controller/advance.rs:960-975`
- Modify: `crates/loopward-runtime/src/recovery/reconcile.rs:568-583`
- Test: `crates/loopward-runtime/tests/context_hydration.rs`

**Interfaces:**
- Produces: `pub fn resource_grants_for_manifest(work_unit_id: loopward_api::ids::WorkUnitId, manifest: &BootPackageManifest) -> Vec<ResourceGrant>` in `crates/loopward-runtime/src/context/mod.rs`, re-exported so `crate::context::resource_grants_for_manifest` resolves from both `controller::advance` and `recovery::reconcile`.
- Consumes at call sites: `report.manifest` (`advance.rs`, already in scope as `report: HydrationReport`) and `boot_package: BootPackageManifest` (`reconcile.rs`'s `start_and_finish`, already deserialized in scope), plus `work_unit.id: WorkUnitId` (already in scope at both sites).

- [ ] **Step 1: Write the failing test**

Add to `crates/loopward-runtime/tests/context_hydration.rs` (reusing the file's existing `RecordingBroker`, `candidate(...)`, `work_unit(...)`, `budget(...)`, `request(...)` helpers):

```rust
#[tokio::test]
async fn on_demand_reference_only_items_produce_a_matching_resource_grant() {
    let broker = Arc::new(RecordingBroker::new());
    let work_unit = work_unit(&[], &["resource://history/on-demand-001"]);
    let mut on_demand = candidate(
        "od-001",
        "resource://history/on-demand-001",
        ContextClass::Evidence,
        ContextPriority::OnDemand,
        None,
    );
    on_demand.source.digest = Some(digest_of(&RecordingBroker::synthetic(
        "resource://history/on-demand-001",
    )));
    let capabilities = /* whatever the file's `request(...)` helper uses to set
                           `capabilities.context.supports_resource_fetch = true`;
                           check `RunnerCapabilities`/`ContextCapabilities`
                           construction in this file's existing tests */;

    let report = hydrate(
        Arc::clone(&broker),
        request(work_unit.clone(), budget(48_000, 8_000, 6_000, 4_000), vec![on_demand], true),
    )
    .await
    .expect("hydration succeeds");

    let grants = loopward_runtime::context::resource_grants_for_manifest(
        work_unit.id,
        &report.manifest,
    );
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].resource.uri, "resource://history/on-demand-001");
    assert_eq!(grants[0].scope, work_unit.id);
}
```

Adjust the test to however this file's `request(...)`/`work_unit(...)` helpers actually thread `supports_resource_fetch` — read the existing `on_demand`/`ReferenceOnly` test(s) already in this file (search `ReferenceOnly` and `REFERENCE_ONLY_TOKENS`) to match fixture construction exactly rather than guessing field names.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p loopward-runtime --test context_hydration on_demand_reference_only_items_produce_a_matching_resource_grant`
Expected: FAIL to compile — `loopward_runtime::context::resource_grants_for_manifest` doesn't exist yet.

- [ ] **Step 3: Implement the helper**

In `crates/loopward-runtime/src/context/mod.rs`, extend the import block (currently lines 61-69):

```rust
use loopward_api::context::{
    BootPackageManifest, ContextBudget, ContextClass, ContextPriority, EstimateMethod, Fidelity,
    ResourceGrant, ResourcePermission, Sensitivity, Validity, VerificationStatus,
};
use loopward_api::ids::{EpisodeId, WorkUnitId};
```

Add, near `HydrationReport`'s `impl` block (after `selected_tokens`, say):

```rust
/// The [`ResourceGrant`]s a runner needs to actually fetch every
/// [`Fidelity::ReferenceOnly`] item in `manifest` — the on-demand items the
/// hydrator priced as cheap references on the promise that the runner can
/// fetch them later (`docs/04-context-lifecycle.md` §6, the
/// `supports_resource_fetch` capability). Without a matching grant, a
/// runner honestly reporting that capability would be handed a manifest
/// promising fetchable content it has no permission to actually fetch.
#[must_use]
pub fn resource_grants_for_manifest(
    work_unit_id: WorkUnitId,
    manifest: &BootPackageManifest,
) -> Vec<ResourceGrant> {
    manifest
        .items
        .iter()
        .filter(|item| matches!(item.fidelity, Fidelity::ReferenceOnly))
        .map(|item| ResourceGrant {
            resource: item.source.clone(),
            permissions: vec![ResourcePermission::Read],
            scope: work_unit_id,
            expires_at: None,
        })
        .collect()
}
```

- [ ] **Step 4: Wire it in at both call sites**

In `crates/loopward-runtime/src/controller/advance.rs`, add `resource_grants_for_manifest` to the existing `use crate::context::ContextCandidate;` import line (near the top of the file), then change line 970:

```rust
            resource_grants: crate::context::resource_grants_for_manifest(
                work_unit.id,
                &report.manifest,
            ),
```

In `crates/loopward-runtime/src/recovery/reconcile.rs`, add `use crate::context::resource_grants_for_manifest;` near the existing `use crate::HydrationRequest;` import, then change line 578:

```rust
            resource_grants: resource_grants_for_manifest(work_unit.id, &boot_package),
```

- [ ] **Step 5: Run the test and the full hydration/advance suites**

Run: `cargo test -p loopward-runtime`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/loopward-runtime/src/context/mod.rs crates/loopward-runtime/src/controller/advance.rs crates/loopward-runtime/src/recovery/reconcile.rs crates/loopward-runtime/tests/context_hydration.rs
git commit -m "fix(context): grant on-demand resource_fetch access for reference-only items"
```

---

### Task 5: Validate invariants on every `append`, not just on Checkpoint commit

**Finding:** `store.rs:179` — `append_in_transaction` folds events through `evolve` and writes the projection, but (unlike `checkpoint::commit_in_transaction`'s callers — `LoopController::resume`/`cancel`/`RecoveryManager::commit_raw_projection`) never calls `loopward_core::validate_invariants` on the resulting state before persisting it.

**Root cause (confirmed by reading `append_in_transaction` in full):** after the `for domain_event in events { state = evolve(state, domain_event)?; ... }` loop, the very next call is `write_projection_row(connection, loop_id, &state).await?` — no invariant check in between. Every other direct-state-mutation path in this codebase (`resume`, `cancel`, `RecoveryManager::commit_raw_projection`) calls `validate_invariants(&state)?` right before writing, specifically as defense-in-depth against a `decide`/`evolve` bug producing a state that violates a domain invariant `loopward_core::invariants::validate_invariants` checks (e.g. `loop.at_most_one_active_episode`, `loop.at_most_one_active_work_unit`, `loop.terminal_has_no_live_children`). `append` is the one durable-write path missing this.

**Fix:** call `validate_invariants(&state)` right after the fold loop, before `write_projection_row`.

**Files:**
- Modify: `crates/loopward-store-sqlite/src/store.rs:1-25` (imports), `:218-236` (the fold loop)
- Test: `crates/loopward-store-sqlite/tests/transaction_contract.rs`

**Interfaces:**
- Consumes: `loopward_core::validate_invariants` (not currently imported in `store.rs`; `checkpoint.rs` already imports and uses it — same crate, same function).
- Produces: `append_in_transaction` now returns `Err(StoreError::from(SqliteStoreError::from(DomainError)))` (via the same conversion path `commit_checkpoint`'s callers already rely on) when the folded state violates an invariant, instead of silently persisting it.

- [ ] **Step 1: Confirm the error conversion path already exists**

Run: `grep -n "impl From<loopward_core::DomainError>\|impl From<DomainError>" crates/loopward-store-sqlite/src/error.rs`
If `SqliteStoreError` has no `From<DomainError>` conversion yet, this step's fix needs one added — check first, since `EventDidNotApply` (used for the `evolve` error two lines above the loop's end) already wraps a `DomainError` (`SqliteStoreError::EventDidNotApply { loop_id, source }` where `source` is `evolve`'s error type), so the type is very likely already in scope in this file; confirm its exact variant name before writing Step 3's code.

- [ ] **Step 2: Write the failing test**

Add to `crates/loopward-store-sqlite/tests/transaction_contract.rs`, following the file's existing pattern of committing genesis, then calling `store.append(...)` directly with hand-built events (see e.g. `append_folds_events_into_the_persisted_projection_not_just_the_revision` in this file for the exact fixture style):

```rust
#[tokio::test]
async fn append_rejects_a_folded_state_that_violates_an_invariant() {
    let harness = Harness::open().await;
    let loop_id = LoopId::new();
    commit_genesis(&harness, loop_id).await;

    // Any event (or event sequence) that `evolve` will apply successfully
    // but that leaves `execution.active_episode` disagreeing with the
    // actual `episode` field trips `loop.at_most_one_active_episode` in
    // `validate_invariants` without `evolve` itself objecting — mirror
    // whichever existing fixture in this file already constructs such a
    // state for `commit_checkpoint`'s own invariant tests (see
    // `one_loop_has_at_most_one_active_episode` below) and drive the same
    // shape through `append` instead of `commit_checkpoint`.
    let result = harness
        .store
        .append(
            loop_id,
            /* expected_revision matching genesis */,
            /* fencing_token from the harness's held lease */,
            vec![/* the offending event(s) */],
        )
        .await;

    assert!(
        matches!(result, Err(StoreError::Backend(_))),
        "append must reject a state that fails validate_invariants, not persist it"
    );
}
```

Adapt the exact event fixture to reuse whatever construction the existing `one_loop_has_at_most_one_active_episode` test (`transaction_contract.rs:1188`) already uses to provoke an invariant violation via `commit_checkpoint`, since that's the proven way this test file already produces an invalid `AggregateState`.

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p loopward-store-sqlite --test transaction_contract append_rejects_a_folded_state_that_violates_an_invariant`
Expected: FAIL — `append` currently succeeds and silently persists the bad projection.

- [ ] **Step 4: Implement the fix**

In `crates/loopward-store-sqlite/src/store.rs`, add to the imports (near the existing `use loopward_core::{AggregateState, LoopEvent, evolve};`):

```rust
use loopward_core::{AggregateState, LoopEvent, evolve, validate_invariants};
```

In `append_in_transaction`, immediately after the fold loop (currently ending at line 235 with the closing `}` of `for domain_event in events { ... }`) and before `write_projection_row(connection, loop_id, &state).await?;`:

```rust
        // Defense-in-depth: `write_projection_row` below is the one durable
        // write `append` performs, and — unlike `commit_checkpoint`, whose
        // callers (`LoopController::resume`/`cancel`,
        // `RecoveryManager::commit_raw_projection`) already validate before
        // writing — nothing upstream of here re-checks that folding these
        // events left `state` internally consistent. A `decide`/`evolve`
        // bug that produces an invariant-violating state must not be
        // silently persisted just because no Checkpoint happened to commit
        // while it was visible.
        validate_invariants(&state)
            .map_err(|source| SqliteStoreError::EventDidNotApply { loop_id, source })?;

        write_projection_row(connection, loop_id, &state).await?;
```

(Reuse `SqliteStoreError::EventDidNotApply` since it already wraps a `loopward_core::DomainError` for this exact file — check Step 1's finding; if `EventDidNotApply`'s `source` field type doesn't match `validate_invariants`'s error type exactly, use whatever existing variant does, rather than adding a new one.)

- [ ] **Step 5: Run the test and the full store suite**

Run: `cargo test -p loopward-store-sqlite`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/loopward-store-sqlite/src/store.rs crates/loopward-store-sqlite/tests/transaction_contract.rs
git commit -m "fix(store): validate invariants on append, not only on checkpoint commit"
```

---

### Task 6: Give `EpisodeRef.ordinal` a real value

**Finding:** `controller/service.rs:544` — `ordinal` is hardcoded to `0` for every active Episode because `AggregateState` carries no running per-Loop episode counter, which is worse than omitting the field (callers have no signal it's a placeholder).

**Root cause (confirmed by reading `loopward-core`):** `LoopExecution` (`crates/loopward-core/src/model.rs:128-149`) has no counter field, and `Episode` itself (`:268-292`) has no ordinal either — `AggregateState.episode: Option<Episode>` only ever holds the *current* Episode, never history. The SQLite adapter already computes and stores a real per-Loop ordinal (`episodes.ordinal`, `checkpoint.rs:319-336`), but that's adapter-internal and invisible to the port-level `AggregateState` the `RuntimeLoopService` reads.

**Fix:** add a durable, monotonically-incrementing `episode_count: u32` to `LoopExecution`, incremented in the reducer's `apply_episode_started`, and read it in `project_status`.

**Files:**
- Modify: `crates/loopward-core/src/model.rs:128-149` (`LoopExecution` struct)
- Modify: `crates/loopward-core/src/reducer.rs:919-940` (`apply_episode_started`)
- Modify: `crates/loopward-runtime/src/controller/service.rs:171-187` (genesis `LoopExecution` literal), `:539-545` (`project_status`)
- Modify (add the new field to every other `LoopExecution { ... }` literal so the workspace compiles):
  - `crates/loopward-core/tests/lifecycle_contract.rs:221`
  - `crates/loopward-store-sqlite/tests/transaction_contract.rs:98`
  - `crates/loopward-runtime/tests/recovery_matrix.rs:690`
  - `crates/loopward-runtime/tests/port_contract.rs:454`
  - `crates/loopward-runtime/tests/advance_episode.rs:730` and `:902`
  - `crates/loopward-testkit/src/failure.rs:315`
  - `crates/loopward-observability-otel/tests/episode_links.rs:596`
- Test: `crates/loopward-core/tests/lifecycle_contract.rs` (new test), plus a `RuntimeLoopService`-level test

**Interfaces:**
- Produces: `LoopExecution.episode_count: u32` — a new public field. Every existing `LoopExecution { .. }` struct literal across the workspace must add `episode_count: 0,` (or the appropriate starting value in a test fixture that pre-seeds Episode history) or the workspace fails to compile; this is intentional — Rust's exhaustive struct-literal requirement is what catches every call site that needs updating.
- Consumes at `service.rs:544`: `state.execution.episode_count` in place of the hardcoded `0`.

- [ ] **Step 1: Write the failing test in `loopward-core`**

Add to `crates/loopward-core/tests/lifecycle_contract.rs` (matching the file's existing `decide`/`evolve` driving style — read a nearby test first for the exact harness helpers):

```rust
#[test]
fn episode_count_increments_on_every_episode_started() {
    let mut state = /* the file's existing helper for a fresh AggregateState post-genesis,
                        matching whatever `execution` fixture nearby tests build at line 221 */;
    assert_eq!(state.execution.episode_count, 0);

    let episode_id = EpisodeId::new();
    let events = decide(&state, /* whatever the file's existing tests pass for */
        LifecycleCommand::StartEpisode { episode_id })
        .expect("starting the first episode is allowed");
    for event in &events {
        state = evolve(state, event).expect("event applies");
    }
    assert_eq!(state.execution.episode_count, 1);
}
```

Adapt to this test file's actual existing helpers (a `fresh_state()`-style function, or whatever it's named) rather than inventing new scaffolding — grep the file for how its other `StartEpisode` tests are structured first.

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `cargo test -p loopward-core --test lifecycle_contract episode_count_increments_on_every_episode_started`
Expected: FAIL to compile — `episode_count` doesn't exist on `LoopExecution` yet.

- [ ] **Step 3: Add the field**

In `crates/loopward-core/src/model.rs`, add to `LoopExecution` (after `active_work_unit`, before `budget`):

```rust
    /// The active Work Unit, of which there is at most one in Phase One.
    pub active_work_unit: Option<WorkUnitId>,
    /// The number of Episodes this Loop has ever started. Increments in
    /// [`crate::evolve`] on every `episode.started` event
    /// (`docs/07-session-rotation.md` §3), giving
    /// [`crate::AggregateState::active_episode`] a durable ordinal instead
    /// of only an identity — the public API's `EpisodeRef::ordinal` is
    /// projected from this.
    pub episode_count: u32,
    /// The Loop's resource budget.
    pub budget: ResourceBudget,
```

- [ ] **Step 4: Increment it in the reducer**

In `crates/loopward-core/src/reducer.rs`, change `apply_episode_started` (lines 919-940):

```rust
fn apply_episode_started(
    state: &mut AggregateState,
    episode_id: EpisodeId,
    kind: EpisodeKind,
    base_checkpoint_version: u64,
) {
    state.execution.episode_count += 1;
    state.episode = Some(Episode {
        id: episode_id,
        loop_id: state.execution.id,
        kind,
        status: EpisodeStatus::Preparing,
        base_checkpoint_version,
        terminal_checkpoint_version: None,
        active_session: None,
        hydrated_manifest: None,
        trace_id: None,
    });
    // The previous generation's Session and Attempt records stay in the
    // projection: Session generations increase strictly across the whole
    // Loop, and Attempt ordinals accumulate per Work Unit, so both must
    // survive an Episode boundary. Both are terminal by construction here.
}
```

- [ ] **Step 5: Fix every other `LoopExecution { .. }` construction site**

Add `episode_count: 0,` (or, for a fixture that specifically pre-seeds a Loop already several Episodes in, the matching count) to each literal listed under **Files** above. For each file, run e.g.:

```bash
grep -n "LoopExecution {" -A 15 crates/loopward-core/tests/lifecycle_contract.rs
```

and add the field in the same style as the surrounding fields (trailing comma, matching indentation). Do this for all nine remaining sites.

- [ ] **Step 6: Wire it into `RuntimeLoopService`**

In `crates/loopward-runtime/src/controller/service.rs`, the genesis `LoopExecution` literal (currently lines 171-187) needs `episode_count: 0,` added (a brand-new Loop has started zero Episodes). In `project_status` (currently lines 539-545):

```rust
    let active_episode = state.active_episode().map(|episode| EpisodeRef {
        episode_id: episode.id,
        ordinal: state.execution.episode_count,
    });
```

Remove the now-stale comment above it ("`AggregateState` tracks only the *current* Episode... a dedicated counter is future work, so this is always reported as `0` for now").

`EpisodeRef.ordinal` in `loopward-api` is `u32` (confirmed in `crates/loopward-api/src/loop_service.rs:321-326`), matching the new field's type exactly — no cast needed.

- [ ] **Step 7: Add a service-level regression test**

Add to whichever test file already exercises `RuntimeLoopService::status` end-to-end across two Episodes (check `crates/loopward-runtime/tests/advance_episode.rs` first, since it already builds multi-episode fixtures at lines 730/902):

```rust
#[tokio::test]
async fn status_reports_a_real_episode_ordinal_after_two_episodes() {
    /* drive the harness through one full Execution + Verification Episode
       pair (the file's existing multi-episode fixtures already do this),
       then assert */
    let status = service.status(loop_id).await.expect("status loads");
    let episode = status.active_episode.expect("an episode is active");
    assert!(episode.ordinal >= 2, "ordinal must reflect episode history, not always 0");
}
```

- [ ] **Step 8: Build and run the full workspace**

Run: `cargo build --workspace && cargo test --workspace`
Expected: everything compiles (confirms every `LoopExecution` literal was updated) and all tests, including the two new ones, pass.

- [ ] **Step 9: Commit**

```bash
git add crates/loopward-core/src/model.rs crates/loopward-core/src/reducer.rs crates/loopward-core/tests/lifecycle_contract.rs crates/loopward-runtime/src/controller/service.rs crates/loopward-runtime/tests/advance_episode.rs crates/loopward-runtime/tests/recovery_matrix.rs crates/loopward-runtime/tests/port_contract.rs crates/loopward-store-sqlite/tests/transaction_contract.rs crates/loopward-testkit/src/failure.rs crates/loopward-observability-otel/tests/episode_links.rs
git commit -m "fix(core): track a durable episode_count so EpisodeRef.ordinal is real"
```

---

### Task 7: Pin the schema version constants to the shapes they describe

**Finding:** `lib.rs:65` — `CHECKPOINT_SCHEMA_VERSION` and `WORK_UNIT_SCHEMA_VERSION` describe struct shapes owned by `loopward-core` but live in `loopward-store-sqlite` with nothing tying the version string to the actual field layout, so a future field change to `Checkpoint`/`WorkUnit` compiles and round-trips fine without anyone being forced to bump the version.

**Fix:** add a "schema fingerprint" test next to each constant: a fixed, hardcoded list of the struct's field names, checked against the type's actual serialized field set. A field addition/removal/rename breaks the test, forcing a deliberate decision (bump the version and update the fingerprint, or explain why not).

**Files:**
- Modify: `crates/loopward-store-sqlite/src/lib.rs` (doc comments on the two constants, pointing at the new test)
- Test: `crates/loopward-store-sqlite/tests/schema_version_pinning.rs` (new file)

**Interfaces:**
- Consumes: `loopward_core::Checkpoint`, `loopward_core::WorkUnit` (via `loopward_api::work_unit::WorkUnit` — confirm the exact type `WORK_UNIT_SCHEMA_VERSION` is meant to describe by checking `write_work_unit` in `checkpoint.rs:427-455`, which serializes `work_unit: &WorkUnit` from `loopward_api::work_unit`), and `serde_json::to_value`.
- Produces: no runtime code change — this is a compile-time-adjacent guard implemented as a test.

- [ ] **Step 1: Write the fingerprint test (this *is* the "failing test" step — the point is that it currently has nothing to fail against, so first prove it passes today, establishing the baseline)**

Create `crates/loopward-store-sqlite/tests/schema_version_pinning.rs`:

```rust
//! Ties `CHECKPOINT_SCHEMA_VERSION` and `WORK_UNIT_SCHEMA_VERSION`
//! (`crates/loopward-store-sqlite/src/lib.rs`) to the actual field layout of
//! the `loopward-core`/`loopward-api` structs they describe. Neither struct
//! lives in this crate, so nothing else forces a version bump when a field
//! is added, removed, or renamed — this test does that instead: it fails
//! the moment the serialized field set drifts from the list below, forcing
//! a deliberate choice (bump the version and update this list, or justify
//! why the version doesn't need to move).

use std::collections::BTreeSet;

/// The exact field set `loopward.checkpoint/v0.1` describes, taken from
/// `loopward_core::Checkpoint`'s definition at the time this constant was
/// last bumped.
const CHECKPOINT_V0_1_FIELDS: &[&str] = &[
    "id",
    "loop_id",
    "version",
    "aggregate_revision",
    "parent",
    "episode_id",
    "integrity_digest",
];

/// The exact field set `loopward.work-unit/v0.1` describes, taken from
/// `loopward_api::work_unit::WorkUnit`'s definition at the time this
/// constant was last bumped.
const WORK_UNIT_V0_1_FIELDS: &[&str] = &[
    /* fill in from `cargo doc -p loopward-api --open` or by reading
       `crates/loopward-api/src/work_unit.rs`'s `WorkUnit` struct directly —
       list every field name exactly as declared */
];

fn field_names_of<T: serde::Serialize>(sample: &T) -> BTreeSet<String> {
    let value = serde_json::to_value(sample).expect("sample serializes");
    let serde_json::Value::Object(map) = value else {
        panic!("expected a JSON object");
    };
    map.keys().cloned().collect()
}

#[test]
fn checkpoint_schema_version_matches_the_checkpoint_shape_it_names() {
    assert_eq!(
        loopward_store_sqlite::CHECKPOINT_SCHEMA_VERSION,
        "loopward.checkpoint/v0.1",
        "bump this test's expected version alongside the constant"
    );
    let sample = /* build one representative loopward_core::Checkpoint,
                    e.g. reuse a fixture helper already used in
                    transaction_contract.rs for a genesis checkpoint */;
    let actual: BTreeSet<String> = field_names_of(&sample);
    let expected: BTreeSet<String> = CHECKPOINT_V0_1_FIELDS
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    assert_eq!(
        actual, expected,
        "Checkpoint's field set no longer matches CHECKPOINT_SCHEMA_VERSION \
         (loopward.checkpoint/v0.1) — bump CHECKPOINT_SCHEMA_VERSION in \
         loopward-store-sqlite/src/lib.rs and update CHECKPOINT_V0_1_FIELDS \
         in this test to match"
    );
}

#[test]
fn work_unit_schema_version_matches_the_work_unit_shape_it_names() {
    assert_eq!(
        loopward_store_sqlite::WORK_UNIT_SCHEMA_VERSION,
        "loopward.work-unit/v0.1",
        "bump this test's expected version alongside the constant"
    );
    let sample = /* build one representative loopward_api::work_unit::WorkUnit,
                    reusing an existing fixture helper if one is available in
                    this crate's test suite, or `context_hydration.rs`'s
                    `work_unit(...)` helper pattern if this crate can reach it */;
    let actual: BTreeSet<String> = field_names_of(&sample);
    let expected: BTreeSet<String> = WORK_UNIT_V0_1_FIELDS
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    assert_eq!(
        actual, expected,
        "WorkUnit's field set no longer matches WORK_UNIT_SCHEMA_VERSION \
         (loopward.work-unit/v0.1) — bump WORK_UNIT_SCHEMA_VERSION in \
         loopward-store-sqlite/src/lib.rs and update WORK_UNIT_V0_1_FIELDS \
         in this test to match"
    );
}
```

Fill in `WORK_UNIT_V0_1_FIELDS` and both `sample` builders by reading `crates/loopward-api/src/work_unit.rs`'s `WorkUnit` struct and reusing whatever fixture-construction helper `crates/loopward-store-sqlite/tests/transaction_contract.rs` already has for a `Checkpoint` (it builds one for genesis) — don't hand-roll a second one.

- [ ] **Step 2: Run it and confirm it passes today (establishing the baseline this task is meant to create)**

Run: `cargo test -p loopward-store-sqlite --test schema_version_pinning`
Expected: PASS. (There is no "fix the implementation" step for this task — the deliverable *is* the test. If it fails at this point, the field lists in Step 1 were transcribed incorrectly; fix the lists, not `lib.rs`.)

- [ ] **Step 3: Point the constants' doc comments at the new test**

In `crates/loopward-store-sqlite/src/lib.rs`, extend the doc comments on both constants:

```rust
/// The Checkpoint schema version this adapter records against every stored
/// Checkpoint, matching `docs/schemas/checkpoint-v0.1.yaml`.
///
/// Pinned to `loopward_core::Checkpoint`'s exact field set by
/// `tests/schema_version_pinning.rs`'s
/// `checkpoint_schema_version_matches_the_checkpoint_shape_it_names` — bump
/// both together.
pub const CHECKPOINT_SCHEMA_VERSION: &str = "loopward.checkpoint/v0.1";

/// The Work Unit schema version this adapter records against every stored
/// Work Unit, matching `docs/schemas/work-unit-v0.1.yaml`.
///
/// Pinned to `loopward_api::work_unit::WorkUnit`'s exact field set by
/// `tests/schema_version_pinning.rs`'s
/// `work_unit_schema_version_matches_the_work_unit_shape_it_names` — bump
/// both together.
pub const WORK_UNIT_SCHEMA_VERSION: &str = "loopward.work-unit/v0.1";
```

- [ ] **Step 4: Run the full crate's tests**

Run: `cargo test -p loopward-store-sqlite`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/loopward-store-sqlite/src/lib.rs crates/loopward-store-sqlite/tests/schema_version_pinning.rs
git commit -m "test(store): pin schema version constants to the struct shapes they name"
```

---

### Task 8: Batch `write_resource_edges`' per-resource round trips

**Finding:** `checkpoint.rs:513` — `write_resource_edges` issues one `SELECT` and one `INSERT` per referenced resource URI, sequentially, inside a `for` loop — up to ~10 round trips per Checkpoint commit, on the path every Episode boundary runs through.

**Fix:** one batched `SELECT ... WHERE resource_uri IN (...)` for every distinct URI, then one multi-row `INSERT ... VALUES (...), (...), ...`, using `sqlx::QueryBuilder` (not previously used in this crate, but a standard, already-available part of `sqlx`, which is already a workspace dependency).

**Files:**
- Modify: `crates/loopward-store-sqlite/src/checkpoint.rs:510-547`
- Test: `crates/loopward-store-sqlite/tests/transaction_contract.rs` (the existing resource-edge tests should already cover correctness; add one asserting query count if the harness can observe it, otherwise rely on the existing `unfinalized_resources_cannot_enter_a_checkpoint` test continuing to pass as the regression guard)

**Interfaces:**
- No signature change to `write_resource_edges`. Internal implementation only.

- [ ] **Step 1: Confirm the existing tests already cover the behavior being preserved**

Run: `cargo test -p loopward-store-sqlite --test transaction_contract unfinalized_resources_cannot_enter_a_checkpoint finalized_resources_are_content_addressed`
Expected: PASS on the current code — this establishes the regression baseline before touching the implementation.

- [ ] **Step 2: Implement the batched version**

Replace `write_resource_edges` in `crates/loopward-store-sqlite/src/checkpoint.rs:513-547`:

```rust
/// Records this Checkpoint's resource edges, rejecting any resource that is
/// not durably finalized (§10: "unfinalized resources cannot enter a
/// Checkpoint").
///
/// Batched into one `SELECT ... IN (...)` and one multi-row `INSERT`
/// instead of one round trip per edge: a Checkpoint can reference up to
/// ~5 resources (hydrated manifest, boot manifest, work-unit inputs, boot
/// package, result), and this runs on every Episode boundary.
async fn write_resource_edges(
    connection: &mut SqliteConnection,
    loop_id: LoopId,
    state: &AggregateState,
) -> Result<(), SqliteStoreError> {
    let checkpoint_version = sql_i64("checkpoint version", state.execution.checkpoint_version)?;
    let edges = referenced_resources(state);
    if edges.is_empty() {
        return Ok(());
    }

    let distinct_uris: Vec<&str> = {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        edges
            .iter()
            .filter(|(uri, _)| seen.insert(uri.as_str()))
            .map(|(uri, _)| uri.as_str())
            .collect()
    };

    let mut select = sqlx::QueryBuilder::new("SELECT resource_uri, finalized FROM resources WHERE resource_uri IN (");
    let mut separated = select.separated(", ");
    for uri in &distinct_uris {
        separated.push_bind(*uri);
    }
    separated.push_unseparated(")");
    let rows = select.build().fetch_all(&mut *connection).await?;

    let finalized: std::collections::HashMap<String, i64> = rows
        .iter()
        .map(|row| -> Result<(String, i64), SqliteStoreError> {
            Ok((row.try_get::<String, _>(0)?, row.try_get::<i64, _>(1)?))
        })
        .collect::<Result<_, _>>()?;

    for (uri, _relation) in &edges {
        match finalized.get(uri) {
            None => {
                return Err(SqliteStoreError::UnknownResource { uri: uri.clone() });
            }
            Some(value) if *value != 1 => {
                return Err(SqliteStoreError::ResourceNotFinalized { uri: uri.clone() });
            }
            Some(_) => {}
        }
    }

    let mut insert = sqlx::QueryBuilder::new(
        "INSERT INTO resource_edges (loop_id, checkpoint_version, resource_uri, relation) ",
    );
    insert.push_values(edges.iter(), |mut row, (uri, relation)| {
        row.push_bind(loop_id.to_string())
            .push_bind(checkpoint_version)
            .push_bind(uri.clone())
            .push_bind(*relation);
    });
    insert.push(" ON CONFLICT(loop_id, checkpoint_version, resource_uri, relation) DO NOTHING");
    insert.build().execute(&mut *connection).await?;

    Ok(())
}
```

Add `use std::collections::BTreeSet;` to `checkpoint.rs`'s imports if not already present (check — `BTreeSet` isn't currently imported in this file; `hydrator.rs` imports it for a similar dedup pattern, but that's a different crate/file).

- [ ] **Step 3: Run the full store test suite**

Run: `cargo test -p loopward-store-sqlite`
Expected: PASS, including `unfinalized_resources_cannot_enter_a_checkpoint` and `finalized_resources_are_content_addressed_and_abort_removes_only_its_own_pending_file` — both exercise `write_resource_edges` through `commit_checkpoint` and must still reject an unfinalized/unknown resource with the same error variants as before.

- [ ] **Step 4: Commit**

```bash
git add crates/loopward-store-sqlite/src/checkpoint.rs
git commit -m "perf(store): batch write_resource_edges into two round trips"
```

---

### Task 9: Resolve required context candidates concurrently

**Finding:** `context/hydrator.rs:221` — `hydrate()` resolves required candidates via sequential `.await` calls in a `for` loop instead of concurrently, even though each `broker.resolve()` call is an independent I/O operation and hydration runs at least twice per Work Unit (execution + verification episodes) plus once more at drain time.

**Scope decision:** only the **required**-candidate loop is parallelized. The **optional**-candidate loop (`price_optional`) shares a single `BudgetLedger` that charges tokens in strict bucket-then-priority-then-id order — that ordering is exactly what makes selection deterministic (`docs/04-context-lifecycle.md` §6), so it cannot be safely made concurrent without either breaking determinism or a materially larger refactor (pre-fetch bytes concurrently, then charge the ledger sequentially over already-resolved bytes). Required-candidate resolution has no such ordering dependency: `priced` is re-sorted by `selection_key` at the very end regardless of resolution order, so parallelizing it is a pure latency win with no behavior change.

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/loopward-runtime/Cargo.toml` (`[dependencies]`)
- Modify: `crates/loopward-runtime/src/context/hydrator.rs:203-260`
- Test: `crates/loopward-runtime/tests/context_hydration.rs`

**Interfaces:**
- Consumes: `futures_util::future::try_join_all` (new dependency).
- Produces: no signature change to `hydrate`.

- [ ] **Step 1: Add the `futures-util` dependency**

In `Cargo.toml`'s `[workspace.dependencies]` (alphabetical order — between `clap` and `jsonschema`):

```toml
futures-util = "0.3.31"
```

In `crates/loopward-runtime/Cargo.toml`'s `[dependencies]` (alongside the existing `async-trait = { workspace = true }`):

```toml
futures-util = { workspace = true }
```

- [ ] **Step 2: Write the failing/characterization test**

Add to `crates/loopward-runtime/tests/context_hydration.rs`. This test proves concurrency doesn't corrupt selection order — it deliberately registers required candidates whose IDs sort in the *opposite* order from the order they'd complete in if resolved one-at-a-time by declaration order, using the existing `RecordingBroker`:

```rust
#[tokio::test]
async fn required_candidates_resolve_concurrently_and_still_sort_deterministically() {
    let broker = Arc::new(RecordingBroker::new());
    let candidates = vec![
        candidate(
            "b-required",
            "resource://required/b",
            ContextClass::CurrentWork,
            ContextPriority::Required,
            None,
        ),
        candidate(
            "a-required",
            "resource://required/a",
            ContextClass::CurrentWork,
            ContextPriority::Required,
            None,
        ),
    ];
    let report = hydrate(
        Arc::clone(&broker),
        request(
            work_unit(&["resource://required/a", "resource://required/b"], &[]),
            budget(48_000, 8_000, 6_000, 4_000),
            candidates,
            true,
        ),
    )
    .await
    .expect("hydration succeeds");

    let ids: Vec<&str> = report
        .manifest
        .items
        .iter()
        .map(|item| item.id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec!["a-required", "b-required"],
        "manifest order must follow selection_key regardless of resolution order"
    );
}
```

Adapt candidate construction to whatever this file's `work_unit(...)` helper expects for declared inputs (it may already synthesize required candidates for declared inputs — check the file's `project_candidates`-adjacent existing tests for how `work_unit(&["uri1", "uri2"], ...)` behaves, since `project_candidates` in `hydrator.rs` promotes/synthesizes required candidates for declared inputs and this test should not accidentally end up asserting against synthesized IDs instead of the two explicit ones).

- [ ] **Step 3: Run it to verify it currently passes (this task doesn't change output, only latency — so this step confirms the *baseline* behavior before the refactor, not a bug)**

Run: `cargo test -p loopward-runtime --test context_hydration required_candidates_resolve_concurrently_and_still_sort_deterministically`
Expected: PASS already, on the pre-change sequential implementation. This is the regression guard for Step 5.

- [ ] **Step 4: Implement the concurrent resolution**

In `crates/loopward-runtime/src/context/hydrator.rs`, add the import:

```rust
use futures_util::future::try_join_all;
```

Replace the required/optional split and required-pricing loop (currently lines 219-239):

```rust
    let mut required: Vec<Ranked> = Vec::with_capacity(ranked.len());
    let mut optional: Vec<Ranked> = Vec::new();
    for entry in ranked {
        if entry.bucket.is_required() {
            required.push(entry);
        } else {
            optional.push(entry);
        }
    }

    // Each required candidate's resolve is an independent broker read with
    // no ordering dependency between them — `priced` is re-sorted by
    // `selection_key` below regardless of resolution order — so resolving
    // them concurrently rather than one `.await` at a time turns N
    // sequential round trips into one wait.
    let priced_required: Vec<(Priced, Vec<u8>)> = try_join_all(
        required.iter().map(|entry| price_required(broker, entry)),
    )
    .await?;

    let mut priced: Vec<(Ranked, Result<Priced, ExclusionReason>)> =
        Vec::with_capacity(required.len() + optional.len());
    let mut pinned_bytes: Vec<(String, Vec<u8>)> = Vec::new();
    let mut required_tokens: u64 = 0;
    for (entry, (cost, bytes)) in required.into_iter().zip(priced_required) {
        required_tokens = required_tokens.saturating_add(cost.tokens);
        if entry.bucket == SelectionBucket::RequiredPinned {
            pinned_bytes.push((entry.candidate.id.clone(), bytes));
        }
        priced.push((entry, Ok(cost)));
    }

    if required_tokens > available {
        return Err(HydrationError::RequiredContentExceedsBudget {
            required_tokens,
            available_tokens: available,
        });
    }
```

The rest of `hydrate` (the optional-candidate loop, the final sort, and `assemble`) is unchanged.

- [ ] **Step 5: Run the new test and the full hydration suite**

Run: `cargo test -p loopward-runtime --test context_hydration`
Expected: PASS. Pay particular attention to any pre-existing test asserting `broker.resolved_uris()` equals an exact sequence when *multiple* required candidates are involved — if one exists and fails because concurrent polling resolved them in a different order than declared, that test was relying on an implementation detail this task deliberately changes; loosen it to assert set membership (`resolved_uris().contains(...)`) rather than exact sequence, or split it to assert order only across the (still-sequential) optional-candidate resolutions.

Also check: if two or more required candidates are simultaneously unresolvable in an existing test, the *specific* `HydrationError` returned may no longer be deterministically "the first one in selection order" (`try_join_all` returns whichever future errors first, not necessarily index 0). If such a test exists and starts failing, relax its assertion to check the error variant/kind rather than which specific candidate id it names — do not weaken it to accept success.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/loopward-runtime/Cargo.toml crates/loopward-runtime/src/context/hydrator.rs crates/loopward-runtime/tests/context_hydration.rs
git commit -m "perf(context): resolve required hydration candidates concurrently"
```

---

### Task 10: Record every rotation signal, not just the primary reason

**Finding:** `rotation/policy.rs:91` — `RotationPolicy::evaluate` computes the full ordered `signals: Vec<RotationSignal>`, but nothing outside `policy.rs` (and its own tests) ever reads `RotationDecision.signals` — only `primary_reason` reaches the durable `RotationRequested` event and any telemetry.

**Scope decision:** thread `signals` into the `loopward.episode` span as a new conditional observability attribute, not into the durable `LoopEvent::RotationRequested` journal event. Reaching the journal would require changing `LifecycleCommand::RequestRotation` and `LoopEvent::RotationRequested`'s shape in `loopward-core` and every caller across both `loopward-runtime` and `loopward-core`'s own reducer tests — a much larger, riskier change for what the report itself calls a "dead-data" (not correctness) finding. Telemetry is explicitly documented in this codebase as best-effort and never load-bearing for durable recovery (`LoopController::open_episode_span`'s doc comment, `docs/08-tracing-and-observability.md` §13/§15), which is exactly the right place for "contributing signals, for whoever's looking" to live without touching the domain model.

**Files:**
- Modify: `crates/loopward-observability/src/attributes.rs` (new constant)
- Modify: `docs/08-tracing-and-observability.md` §6 (extend the documented conditional-attribute vocabulary)
- Modify: `crates/loopward-observability/tests/semantic_conventions.rs` (extend the contract test)
- Modify: `crates/loopward-runtime/src/controller/advance.rs` (both `RotationPolicy::evaluate` call sites)
- Test: covered by the `semantic_conventions.rs` update plus a new assertion in the existing rotation/advance tests

**Interfaces:**
- Produces: `attribute::ROTATION_SIGNALS: &str = "loopward.rotation.signals"`.
- Consumes: `RotationDecision.signals: Vec<RotationSignal>` (`crates/loopward-runtime/src/rotation/policy.rs`, already `pub`), each with `.reason: RotationReason` (has `.label()`) and `.mandatory: bool`.

- [ ] **Step 1: Write the failing contract test**

In `crates/loopward-observability/tests/semantic_conventions.rs`, extend `conditional_attribute_keys_match_the_documented_set` (currently lines 71-100):

```rust
#[test]
fn conditional_attribute_keys_match_the_documented_set() {
    let conditional = [
        attribute::ROTATION_REASON,
        attribute::ROTATION_SIGNALS,
        attribute::SUSPENSION_CODE,
        attribute::FAILURE_CODE,
        attribute::RECOVERY_OUTCOME,
        attribute::CONTEXT_RULE_VERSION,
        attribute::CONTEXT_ESTIMATED_TOKENS,
        attribute::CONTEXT_ACTUAL_TOKENS,
        attribute::CONTEXT_ESTIMATE_METHOD,
        attribute::VERIFICATION_GATE_KIND,
        attribute::VERIFICATION_OUTCOME,
    ];
    assert_eq!(
        conditional,
        [
            "loopward.rotation.reason",
            "loopward.rotation.signals",
            "loopward.suspension.code",
            "loopward.failure.code",
            "loopward.recovery.outcome",
            "loopward.context.rule_version",
            "loopward.context.estimated_tokens",
            "loopward.context.actual_tokens",
            "loopward.context.estimate_method",
            "loopward.verification.gate_kind",
            "loopward.verification.outcome",
        ]
    );
```

(Keep the rest of the function — whatever assertions follow the array literal in the current file — unchanged.)

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `cargo test -p loopward-observability --test semantic_conventions conditional_attribute_keys_match_the_documented_set`
Expected: FAIL to compile — `attribute::ROTATION_SIGNALS` doesn't exist yet.

- [ ] **Step 3: Add the attribute constant**

In `crates/loopward-observability/src/attributes.rs`, add right after `ROTATION_REASON` (line 46):

```rust
    pub const ROTATION_REASON: &str = "loopward.rotation.reason";
    /// Every [`RotationSignal`] that applied to a rotation decision, highest
    /// precedence first — not just the primary reason that was recorded on
    /// the durable `RotationRequested` event.
    pub const ROTATION_SIGNALS: &str = "loopward.rotation.signals";
```

(Match the file's existing doc-comment style for adjacent constants exactly — check the line above for its format before finalizing wording.)

- [ ] **Step 4: Document the new attribute**

In `docs/08-tracing-and-observability.md`, add `loopward.rotation.signals` to the conditional-attributes list (§6, currently lines 116-129), directly after `loopward.rotation.reason`:

```text
Conditional attributes include:

loopward.rotation.reason
loopward.rotation.signals
loopward.suspension.code
...
```

- [ ] **Step 5: Run the contract test again**

Run: `cargo test -p loopward-observability --test semantic_conventions`
Expected: PASS.

- [ ] **Step 6: Record the signals at both `RotationPolicy::evaluate` call sites**

In `crates/loopward-runtime/src/controller/advance.rs`, `run_execution_episode`'s admission check (around line 896-901), after `admission_decision` is computed, before the `if admission_decision.primary_reason == ...` branch:

```rust
        let admission_decision = RotationPolicy::evaluate(&RotationContext {
            soft_context_budget,
            hard_context_budget,
            ..RotationContext::default()
        });
        if !admission_decision.signals.is_empty() {
            episode_span.set_end_attribute(
                attribute::ROTATION_SIGNALS,
                rotation_signals_label(&admission_decision.signals),
            );
        }
```

And in `finish_execution_attempt` (around line 1163-1170), after `rotation_decision` is computed:

```rust
        let rotation_decision = RotationPolicy::evaluate(&RotationContext {
            work_unit_boundary: true,
            soft_context_budget,
            ..RotationContext::default()
        });
        // Note: `finish_execution_attempt` has no `episode_span` in scope —
        // it's called from both `run_execution_episode` (which does) and
        // `RecoveryManager::finish_collected` (which doesn't). Only record
        // this when a span is actually active; a `None` here is exactly
        // the same accepted, best-effort telemetry gap `recover_draining`
        // already documents for its own commit's `loopward.rotation.reason`.
```

Since `finish_execution_attempt` has no `episode_span` parameter, do **not** add span recording there — only at the `run_execution_episode` admission-check call site, which already has `episode_span` in scope. Add a helper near the top of `advance.rs` (or in `rotation/policy.rs`, re-exported):

```rust
/// A stable, comma-joined rendering of every signal a [`RotationDecision`]
/// carried, e.g. `"work_unit_boundary(mandatory),soft_context_budget"` —
/// for `attribute::ROTATION_SIGNALS`. Best-effort observability only, never
/// parsed back into a `RotationReason` by anything durable.
fn rotation_signals_label(signals: &[loopward_core::RotationReason]) -> String {
    signals
        .iter()
        .map(loopward_core::RotationReason::label)
        .collect::<Vec<_>>()
        .join(",")
}
```

Correct the type: `RotationDecision.signals` is `Vec<RotationSignal>` (from `crate::rotation::RotationSignal`, with `.reason` and `.mandatory` fields), not `Vec<RotationReason>` — write the helper against the real type:

```rust
fn rotation_signals_label(signals: &[crate::rotation::RotationSignal]) -> String {
    signals
        .iter()
        .map(|signal| {
            if signal.mandatory {
                format!("{}(mandatory)", signal.reason.label())
            } else {
                signal.reason.label().to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}
```

- [ ] **Step 7: Add a regression test asserting the attribute is set**

In whichever existing `advance_episode.rs` test already exercises the soft-budget-plus-work-unit-boundary double-signal case (search for `soft_context_budget` in `crates/loopward-runtime/tests/advance_episode.rs`; if none exists at the span-observation level, use the crate's `RecordingSink`/telemetry test double the same way `crates/loopward-observability/tests/semantic_conventions.rs` does), assert that the completed Episode's span carries `attribute::ROTATION_SIGNALS` containing both `work_unit_boundary` and `soft_context_budget` when both signals fire together — mirroring `rotation/policy.rs`'s own existing unit test `work_unit_boundary_outranks_a_concurrent_soft_budget_signal`, but observed through the span this time instead of the bare `RotationDecision`.

- [ ] **Step 8: Run the full observability and runtime suites**

Run: `cargo test -p loopward-observability -p loopward-runtime`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/loopward-observability/src/attributes.rs crates/loopward-observability/tests/semantic_conventions.rs docs/08-tracing-and-observability.md crates/loopward-runtime/src/controller/advance.rs crates/loopward-runtime/tests/advance_episode.rs
git commit -m "feat(observability): record every rotation signal, not just the primary reason"
```

---

## Final verification

After all ten tasks:

- [ ] Run `cargo build --workspace` — confirms every `LoopExecution` literal (Task 6) and every new import compiles.
- [ ] Run `cargo test --workspace` — every crate's suite passes.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings` (the workspace denies `clippy::all` and `clippy::pedantic` — confirm every new function has whatever `#[must_use]`/doc-comment conventions its neighbors use, matching what triggered no warnings before).
- [ ] Re-read the original 10 findings against the diff and confirm each one's `failure_scenario` no longer reproduces.
