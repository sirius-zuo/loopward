PRAGMA foreign_keys = ON;

CREATE TABLE loops (
    loop_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    aggregate_revision INTEGER NOT NULL,
    checkpoint_version INTEGER NOT NULL,
    projection_json TEXT NOT NULL
);

CREATE TABLE loop_events (
    loop_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    aggregate_revision INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (loop_id, sequence)
);

CREATE TABLE loop_leases (
    loop_id TEXT PRIMARY KEY REFERENCES loops(loop_id) ON DELETE CASCADE,
    holder_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE command_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    PRIMARY KEY (scope, idempotency_key)
);

CREATE TABLE checkpoints (
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    checkpoint_version INTEGER NOT NULL,
    checkpoint_id TEXT NOT NULL UNIQUE,
    aggregate_revision INTEGER NOT NULL,
    parent_checkpoint_id TEXT,
    schema_version TEXT NOT NULL,
    checkpoint_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (loop_id, checkpoint_version)
);

CREATE TABLE episodes (
    episode_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    status TEXT NOT NULL,
    base_checkpoint_version INTEGER NOT NULL,
    terminal_checkpoint_version INTEGER,
    trace_id TEXT,
    UNIQUE (loop_id, ordinal)
);

CREATE TABLE runtime_sessions (
    session_id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES episodes(episode_id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    runner_ref TEXT NOT NULL,
    boot_manifest_uri TEXT NOT NULL,
    external_session_ref TEXT,
    UNIQUE (episode_id, generation)
);

CREATE TABLE work_units (
    work_unit_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    work_unit_json TEXT NOT NULL
);

CREATE TABLE work_unit_attempts (
    attempt_id TEXT PRIMARY KEY,
    work_unit_id TEXT NOT NULL REFERENCES work_units(work_unit_id) ON DELETE CASCADE,
    episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
    ordinal INTEGER NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    boot_package_uri TEXT NOT NULL,
    external_execution_ref TEXT,
    result_uri TEXT,
    UNIQUE (work_unit_id, ordinal),
    UNIQUE (work_unit_id, idempotency_key)
);

CREATE TABLE verification_runs (
    verification_run_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    work_unit_id TEXT REFERENCES work_units(work_unit_id),
    plan_version INTEGER NOT NULL,
    target_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    findings_json TEXT NOT NULL
);

CREATE TABLE suspensions (
    suspension_id TEXT PRIMARY KEY,
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    reason_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE resources (
    resource_uri TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    media_type TEXT,
    size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    finalized INTEGER NOT NULL CHECK (finalized IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE TABLE resource_edges (
    loop_id TEXT NOT NULL REFERENCES loops(loop_id) ON DELETE CASCADE,
    checkpoint_version INTEGER NOT NULL,
    resource_uri TEXT NOT NULL REFERENCES resources(resource_uri),
    relation TEXT NOT NULL,
    PRIMARY KEY (loop_id, checkpoint_version, resource_uri, relation),
    FOREIGN KEY (loop_id, checkpoint_version)
        REFERENCES checkpoints(loop_id, checkpoint_version)
);

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
