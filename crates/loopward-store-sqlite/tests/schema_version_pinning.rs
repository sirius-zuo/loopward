//! Ties `CHECKPOINT_SCHEMA_VERSION` and `WORK_UNIT_SCHEMA_VERSION`
//! (`crates/loopward-store-sqlite/src/lib.rs`) to the actual field layout of
//! the `loopward-core`/`loopward-api` structs they describe. Neither struct
//! lives in this crate, so nothing else forces a version bump when a field is
//! added, removed, or renamed — this test does that instead: it fails the
//! moment the serialized field set drifts from the list below, forcing a
//! deliberate choice (bump the version and update this list, or justify why
//! the version doesn't need to move).

use std::collections::BTreeSet;

use loopward_api::ids::{CheckpointId, LoopId, WorkUnitId};
use loopward_api::resource::{OutputContract, RunnerRef, VerificationPlanRef};
use loopward_api::work_unit::{RetryPolicy, WorkUnitKind, WorkUnitStatus};
use loopward_core::{Checkpoint, WorkUnit};
use loopward_store_sqlite::{CHECKPOINT_SCHEMA_VERSION, WORK_UNIT_SCHEMA_VERSION};

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
    "id",
    "kind",
    "objective",
    "status",
    "inputs",
    "constraints",
    "output_contract",
    "verification_plan",
    "runner",
    "budget",
    "retry_policy",
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
        CHECKPOINT_SCHEMA_VERSION,
        "loopward.checkpoint/v0.1",
        "bump this test's expected version alongside the constant"
    );
    let sample = Checkpoint {
        id: CheckpointId::new(),
        loop_id: LoopId::new(),
        version: 0,
        aggregate_revision: 0,
        parent: None,
        episode_id: None,
        integrity_digest: None,
    };
    let actual: BTreeSet<String> = field_names_of(&sample);
    let expected: BTreeSet<String> = CHECKPOINT_V0_1_FIELDS
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    assert_eq!(
        actual, expected,
        "Checkpoint's field set no longer matches \
         CHECKPOINT_SCHEMA_VERSION (loopward.checkpoint/v0.1) — bump \
         CHECKPOINT_SCHEMA_VERSION in loopward-store-sqlite/src/lib.rs and \
         update CHECKPOINT_V0_1_FIELDS in this test to match"
    );
}

#[test]
fn work_unit_schema_version_matches_the_work_unit_shape_it_names() {
    assert_eq!(
        WORK_UNIT_SCHEMA_VERSION,
        "loopward.work-unit/v0.1",
        "bump this test's expected version alongside the constant"
    );
    let sample = WorkUnit {
        id: WorkUnitId::new(),
        kind: WorkUnitKind::new("test"),
        objective: "test objective".to_owned(),
        status: WorkUnitStatus::Accepted,
        inputs: vec![],
        constraints: vec![],
        output_contract: OutputContract {
            schema: "loopward.work-unit-result/v0.1".to_owned(),
        },
        verification_plan: VerificationPlanRef {
            reference: "verification-plan://test".to_owned(),
            version: 0,
        },
        runner: RunnerRef {
            name: "command".to_owned(),
            profile: None,
        },
        budget: loopward_api::work_unit::ResourceBudget::default(),
        retry_policy: RetryPolicy::default(),
    };
    let actual: BTreeSet<String> = field_names_of(&sample);
    let expected: BTreeSet<String> = WORK_UNIT_V0_1_FIELDS
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    assert_eq!(
        actual, expected,
        "WorkUnit's field set no longer matches \
         WORK_UNIT_SCHEMA_VERSION (loopward.work-unit/v0.1) — bump \
         WORK_UNIT_SCHEMA_VERSION in loopward-store-sqlite/src/lib.rs and \
         update WORK_UNIT_V0_1_FIELDS in this test to match"
    );
}
