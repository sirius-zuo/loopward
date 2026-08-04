//! End-to-end contract tests for the `loopward` binary, driven entirely
//! through the compiled process (via `assert_cmd`) rather than any internal
//! Rust API — exactly the surface a real operator uses.
//!
//! Every test gets its own [`TempDir`] for `--database`/`--resource-root`/
//! `--run-root`, so tests never collide with each other or with anything
//! outside the temp directory, and can run concurrently under `cargo test`'s
//! default parallelism.

use std::time::Duration;

use assert_cmd::Command;
use serde_json::Value;
use tempfile::TempDir;

/// A minimal, valid `create --spec` document: the only fields
/// `validate_create_request` requires are a non-empty `kind`,
/// `goal.statement`, and `domain_schema` (see `commands::CreateSpec`).
const CREATE_SPEC: &str =
    r#"{"kind":"demo","goal":"ship the thing","domain_schema":"demo.schema.v1"}"#;

/// The `--lease-seconds` value [`full_lifecycle_via_the_cli`] passes to
/// every lease-acquiring command, `create` included, now that
/// `RuntimeLoopService::create` honors the configured lease TTL rather than
/// a hardcoded 60 seconds. Short enough to keep the whole test's real sleep
/// time in the low single-digit seconds.
const TEST_LEASE_SECONDS: &str = "2";

/// How long [`full_lifecycle_via_the_cli`] sleeps between two commands that
/// must not hold the lease concurrently: just past [`TEST_LEASE_SECONDS`],
/// so the next command's lease acquisition is guaranteed to see the prior
/// one expired.
const TEST_LEASE_SLEEP: Duration = Duration::from_millis(2_500);

/// Builds a `loopward` invocation rooted at `temp`'s database, resource, and
/// run directories, so this test's process never touches the real
/// filesystem outside its own temp directory.
fn loopward(temp: &TempDir) -> Command {
    let mut cmd = Command::cargo_bin("loopward").expect("the `loopward` binary should build");
    cmd.arg("--database")
        .arg(temp.path().join("loopward.sqlite3"))
        .arg("--resource-root")
        .arg(temp.path().join("resources"))
        .arg("--run-root")
        .arg(temp.path().join("runs"));
    cmd
}

/// Parses `--json`'s versioned envelope (`{"protocol_version": ..., "data":
/// ...}`) from a command's stdout.
fn json_data(stdout: &[u8]) -> Value {
    let envelope: Value = serde_json::from_slice(stdout).expect("stdout should be valid json");
    envelope
        .get("data")
        .cloned()
        .expect("json envelope should carry a `data` field")
}

/// Creates a Loop and returns its `loop_id` as a string.
fn create_loop(temp: &TempDir) -> String {
    let assert = loopward(temp)
        .args(["create", "--spec", CREATE_SPEC, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    data["loop_id"]
        .as_str()
        .expect("created loop handle should carry a loop_id")
        .to_owned()
}

/// Like [`create_loop`], but requests `lease_seconds` for the genesis lease
/// `create` itself acquires, rather than relying on the CLI's default.
fn create_loop_with_lease_seconds(temp: &TempDir, lease_seconds: &str) -> String {
    let assert = loopward(temp)
        .args([
            "create",
            "--spec",
            CREATE_SPEC,
            "--json",
            "--lease-seconds",
            lease_seconds,
        ])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    data["loop_id"]
        .as_str()
        .expect("created loop handle should carry a loop_id")
        .to_owned()
}

#[test]
fn init_creates_the_local_store_and_directories() {
    let temp = TempDir::new().expect("tempdir");

    loopward(&temp).arg("init").assert().success();

    assert!(temp.path().join("loopward.sqlite3").exists());
    assert!(temp.path().join("resources").is_dir());
    assert!(temp.path().join("runs").is_dir());
}

#[test]
fn init_is_idempotent() {
    let temp = TempDir::new().expect("tempdir");

    loopward(&temp).arg("init").assert().success();
    // A second `init` against the same store re-opens it and re-applies
    // (already-applied) migrations rather than failing outright.
    loopward(&temp).arg("init").assert().success();
}

#[test]
fn verify_store_fails_cleanly_without_prior_init() {
    let temp = TempDir::new().expect("tempdir");

    // No `init` first: `verify-store` must not create the database (unlike
    // every other subcommand's `open_store(.., create_if_missing = true)`),
    // and must fail as a clean, non-zero exit rather than panicking.
    let assert = loopward(&temp).args(["verify-store"]).assert().failure();
    assert!(!temp.path().join("loopward.sqlite3").exists());
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(!stderr.is_empty(), "expected a non-empty error message");
}

#[test]
fn verify_store_succeeds_after_init() {
    let temp = TempDir::new().expect("tempdir");

    loopward(&temp).arg("init").assert().success();
    let assert = loopward(&temp)
        .args(["verify-store", "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["ok"], Value::Bool(true));
}

#[test]
fn create_rejects_invalid_spec_json() {
    let temp = TempDir::new().expect("tempdir");

    let assert = loopward(&temp)
        .args(["create", "--spec", "not json"])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(stderr.contains("--spec"), "stderr was: {stderr}");
}

#[test]
fn run_without_any_finite_limit_is_rejected_as_invalid_request() {
    let temp = TempDir::new().expect("tempdir");
    let loop_id = create_loop(&temp);

    // No `--max-episodes`, `--deadline`, `--max-attempts`, or
    // `--max-tokens`: `LoopController::run_until` itself rejects this as
    // `ControllerError::UnboundedRunUntil`, mapped to
    // `LoopwardErrorCode::InvalidRequest` — not something `commands.rs`
    // pre-checks.
    let assert = loopward(&temp).args(["run", &loop_id]).assert().failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("invalid_request"),
        "expected an invalid_request error, stderr was: {stderr}"
    );
}

/// `create` acquires a real, durable execution lease as part of committing
/// the genesis Checkpoint (`RuntimeLoopService::create`), held by *that
/// invocation's own* `LoopController`, for this CLI's configured
/// `--lease-seconds` (defaulted here, since this test doesn't pass one, to
/// `DEFAULT_LEASE_SECONDS`'s 60 real seconds).
/// Every subsequent `loopward` invocation is a fresh process with its own
/// randomly generated runtime instance identity
/// (`LoopController::new`'s `RuntimeInstanceId::new()`), and Loopward's
/// lease semantics let only *the same* instance reacquire an unexpired
/// lease (`loopward-store-sqlite/src/lease.rs`'s own doc comment, §7): a
/// different instance must wait for it to expire. So the very next
/// lease-acquiring command (`advance`, `run`, `suspend`, `resume`,
/// `cancel`) issued as a separate CLI process shortly after `create`
/// deterministically conflicts with `create`'s own still-live lease —
/// this is a real, working-as-shipped characteristic of the Phase One
/// runtime's single-writer model colliding with this CLI's one-process-
/// per-invocation design, not a bug introduced by this composition root.
/// This test asserts that collision is surfaced cleanly (a clean
/// `LeaseUnavailable`/non-zero exit, never a panic), fast, without waiting
/// out the 60-second window.
#[test]
fn advance_and_run_conflict_with_the_lease_create_still_holds() {
    let temp = TempDir::new().expect("tempdir");
    loopward(&temp).arg("init").assert().success();
    let loop_id = create_loop(&temp);

    let assert = loopward(&temp)
        .args(["advance", &loop_id])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("lease_unavailable"),
        "expected a lease_unavailable error, stderr was: {stderr}"
    );

    let assert = loopward(&temp)
        .args(["run", &loop_id, "--max-episodes", "2"])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("lease_unavailable"),
        "expected a lease_unavailable error, stderr was: {stderr}"
    );

    // Reads that never touch the lease (status, events) are unaffected.
    let assert = loopward(&temp)
        .args(["status", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["loop_id"], Value::String(loop_id.clone()));
    assert_eq!(data["status"], Value::String("active".to_owned()));

    let assert = loopward(&temp)
        .args(["events", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    let events = data["events"]
        .as_array()
        .expect("events should be an array");
    assert!(!events.is_empty(), "expected at least one durable event");
}

/// Exercises every subcommand named in the task brief, end to end, over one
/// Loop's lifecycle — including the state transitions `suspend`/`resume`/
/// `cancel` drive.
///
/// `create`'s genesis lease is configurable (`RuntimeLoopService::create`
/// honors `LoopController::lease_ttl`, the same TTL every other
/// lease-acquiring command uses), so this test passes a short
/// `--lease-seconds` — [`TEST_LEASE_SECONDS`] — to *every* command,
/// including `create`, and sleeps just past that short TTL between
/// commands rather than paying a fixed, unrelated real-time cost.
///
/// `advance` and `run --max-episodes` are still expected to fail once the
/// lease is available, not succeed: Phase One's `RuntimeLoopService::create`
/// never assigns a Work Unit to a freshly created Loop (its own documented
/// "Known limitation"), and no `LifecycleCommand` this CLI can issue does
/// either, so `LoopController::advance` always finds `work_unit: None` and
/// returns `ControllerError::NoWorkUnit`, mapped to a clean
/// `InvalidRequest` — this test asserts that failure is surfaced cleanly (a
/// non-zero exit with a readable message), not that the operation
/// completes.
#[test]
fn full_lifecycle_via_the_cli() {
    let temp = TempDir::new().expect("tempdir");

    loopward(&temp).arg("init").assert().success();

    let loop_id = create_loop_with_lease_seconds(&temp, TEST_LEASE_SECONDS);

    // Wait just past `create`'s short, configured lease so the next
    // command, a separate process with its own runtime instance identity,
    // can acquire it (see this test's doc comment).
    std::thread::sleep(TEST_LEASE_SLEEP);

    // advance: fails cleanly (no work unit assigned) rather than panicking,
    // now that the lease itself is available.
    let assert = loopward(&temp)
        .args(["advance", &loop_id, "--lease-seconds", TEST_LEASE_SECONDS])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("invalid_request"),
        "expected an invalid_request error, stderr was: {stderr}"
    );

    std::thread::sleep(TEST_LEASE_SLEEP);

    // run --max-episodes 2: same underlying NoWorkUnit failure, reached
    // through run_until's internal advance loop instead of a direct
    // advance call.
    let assert = loopward(&temp)
        .args([
            "run",
            &loop_id,
            "--max-episodes",
            "2",
            "--lease-seconds",
            TEST_LEASE_SECONDS,
        ])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("invalid_request"),
        "expected an invalid_request error, stderr was: {stderr}"
    );

    // status --json: the Loop is untouched by the failed advance/run above,
    // so it is still active.
    let assert = loopward(&temp)
        .args(["status", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["loop_id"], Value::String(loop_id.clone()));
    assert_eq!(data["status"], Value::String("active".to_owned()));

    // events --json: at least the genesis checkpoint-committed event from
    // `create` is durable.
    let assert = loopward(&temp)
        .args(["events", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    let events = data["events"]
        .as_array()
        .expect("events should be an array");
    assert!(!events.is_empty(), "expected at least one durable event");

    std::thread::sleep(TEST_LEASE_SLEEP);

    // suspend: active -> suspended.
    loopward(&temp)
        .args([
            "suspend",
            &loop_id,
            "--reason",
            "operator pause",
            "--lease-seconds",
            TEST_LEASE_SECONDS,
        ])
        .assert()
        .success();
    let assert = loopward(&temp)
        .args(["status", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["status"], Value::String("suspended".to_owned()));

    std::thread::sleep(TEST_LEASE_SLEEP);

    // resume: suspended -> active (no dangling attempt to recover).
    loopward(&temp)
        .args(["resume", &loop_id, "--lease-seconds", TEST_LEASE_SECONDS])
        .assert()
        .success();
    let assert = loopward(&temp)
        .args(["status", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["status"], Value::String("active".to_owned()));

    std::thread::sleep(TEST_LEASE_SLEEP);

    // cancel: active -> terminal.
    loopward(&temp)
        .args([
            "cancel",
            &loop_id,
            "--reason",
            "no longer needed",
            "--lease-seconds",
            TEST_LEASE_SECONDS,
        ])
        .assert()
        .success();
    let assert = loopward(&temp)
        .args(["status", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["status"], Value::String("terminal".to_owned()));

    // inspect-checkpoint: reads directly from the store, outside the
    // LoopService contract, and works regardless of terminal status (and
    // never touches the lease).
    let assert = loopward(&temp)
        .args(["inspect-checkpoint", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["loop_id"], Value::String(loop_id.clone()));

    // verify-store: the database this whole test used is open and fully
    // migrated.
    loopward(&temp).args(["verify-store"]).assert().success();
}
