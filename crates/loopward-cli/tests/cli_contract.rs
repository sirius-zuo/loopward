//! End-to-end contract tests for the `loopward` binary, driven entirely
//! through the compiled process (via `assert_cmd`) rather than any internal
//! Rust API — exactly the surface a real operator uses.
//!
//! Every test gets its own [`TempDir`] for `--database`/`--resource-root`/
//! `--run-root`, so tests never collide with each other or with anything
//! outside the temp directory, and can run concurrently under `cargo test`'s
//! default parallelism.

use assert_cmd::Command;
use serde_json::Value;
use tempfile::TempDir;

/// A minimal, valid `create --spec` document: the only fields
/// `validate_create_request` requires are a non-empty `kind`,
/// `goal.statement`, and `domain_schema` (see `commands::CreateSpec`).
const CREATE_SPEC: &str =
    r#"{"kind":"demo","goal":"ship the thing","domain_schema":"demo.schema.v1"}"#;

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
/// the genesis Checkpoint (`RuntimeLoopService::create`), and every
/// subsequent `loopward` invocation is a fresh process with its own randomly
/// generated runtime instance identity (`LoopController::new`'s
/// `RuntimeInstanceId::new()`). Each lease-acquiring operation hands its
/// lease back when it returns (`LoopStore::release_lease`), so the very next
/// `advance`/`run`, issued as a separate process within `create`'s default
/// 60-second `--lease-seconds`, reaches the controller rather than failing
/// `lease_unavailable` — and fails only for the reason Phase One's `create`
/// leaves it no Work Unit to run (`ControllerError::NoWorkUnit`, mapped to
/// `invalid_request`).
#[test]
fn advance_and_run_right_after_create_are_not_blocked_by_its_lease() {
    let temp = TempDir::new().expect("tempdir");
    loopward(&temp).arg("init").assert().success();
    let loop_id = create_loop(&temp);

    let assert = loopward(&temp)
        .args(["advance", &loop_id])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("invalid_request"),
        "expected an invalid_request (no work unit) error, stderr was: {stderr}"
    );

    // `advance` failing must not strand its lease either.
    let assert = loopward(&temp)
        .args(["run", &loop_id, "--max-episodes", "2"])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("invalid_request"),
        "expected an invalid_request (no work unit) error, stderr was: {stderr}"
    );
}

/// Regression test: chaining mutating commands as separate `loopward`
/// processes, back to back and well within the default 60-second
/// `--lease-seconds`, must not fail `lease_unavailable` on the second one.
///
/// Before `LoopStore::release_lease`, every operation left its lease held
/// until the TTL ran out, and the next invocation — a different runtime
/// instance — was refused until then.
#[test]
fn mutating_commands_chain_across_separate_invocations() {
    let temp = TempDir::new().expect("tempdir");
    loopward(&temp).arg("init").assert().success();
    let loop_id = create_loop(&temp);

    for (args, expected_status) in [
        (vec!["suspend", &loop_id, "--reason", "test"], "suspended"),
        (vec!["resume", &loop_id], "active"),
        (vec!["suspend", &loop_id, "--reason", "again"], "suspended"),
        (vec!["resume", &loop_id], "active"),
        (vec!["cancel", &loop_id, "--reason", "done"], "terminal"),
    ] {
        loopward(&temp).args(&args).assert().success();
        let assert = loopward(&temp)
            .args(["status", &loop_id, "--json"])
            .assert()
            .success();
        let data = json_data(&assert.get_output().stdout);
        assert_eq!(
            data["status"],
            Value::String(expected_status.to_owned()),
            "after `{}`",
            args.join(" ")
        );
    }
}

/// Exercises every subcommand named in the task brief, end to end, over one
/// Loop's lifecycle — including the state transitions `suspend`/`resume`/
/// `cancel` drive — with every command a separate process issued straight
/// after the last (see [`mutating_commands_chain_across_separate_invocations`]).
///
/// `advance` and `run --max-episodes` are expected to fail, not succeed:
/// Phase One's `RuntimeLoopService::create` never assigns a Work Unit to a
/// freshly created Loop (its own documented "Known limitation"), and no
/// `LifecycleCommand` this CLI can issue does either, so
/// `LoopController::advance` always finds `work_unit: None` and returns
/// `ControllerError::NoWorkUnit`, mapped to a clean `InvalidRequest` — this
/// test asserts that failure is surfaced cleanly (a non-zero exit with a
/// readable message), not that the operation completes.
#[test]
fn full_lifecycle_via_the_cli() {
    let temp = TempDir::new().expect("tempdir");

    loopward(&temp).arg("init").assert().success();

    let loop_id = create_loop(&temp);

    // advance: fails cleanly (no work unit assigned) rather than panicking.
    let assert = loopward(&temp)
        .args(["advance", &loop_id])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("invalid_request"),
        "expected an invalid_request error, stderr was: {stderr}"
    );

    // run --max-episodes 2: same underlying NoWorkUnit failure, reached
    // through run_until's internal advance loop instead of a direct
    // advance call.
    let assert = loopward(&temp)
        .args(["run", &loop_id, "--max-episodes", "2"])
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

    // suspend: active -> suspended.
    loopward(&temp)
        .args(["suspend", &loop_id, "--reason", "operator pause"])
        .assert()
        .success();
    let assert = loopward(&temp)
        .args(["status", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["status"], Value::String("suspended".to_owned()));

    // resume: suspended -> active (no dangling attempt to recover).
    loopward(&temp)
        .args(["resume", &loop_id])
        .assert()
        .success();
    let assert = loopward(&temp)
        .args(["status", &loop_id, "--json"])
        .assert()
        .success();
    let data = json_data(&assert.get_output().stdout);
    assert_eq!(data["status"], Value::String("active".to_owned()));

    // cancel: active -> terminal.
    loopward(&temp)
        .args(["cancel", &loop_id, "--reason", "no longer needed"])
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
