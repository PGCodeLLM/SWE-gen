-- swegen ledger schema
--
-- Replaces the append-only JSONL ledgers with Postgres tables. Design rules:
--
--   * Append, don't upsert. Every write is a plain INSERT; "latest wins" is
--     computed on read, mirroring load_latest_postchecks() which kept the
--     newest record per `instance` by (attempt, timestamp, line_index). A
--     BIGSERIAL `id` stands in for line_index; `written_at` for timestamp.
--     This preserves the original write-never-blocks semantics and avoids
--     upsert contention across the sharded workers.
--   * Hot query fields are typed columns; the full original record is kept in
--     `payload JSONB` so no data is lost vs. the JSONL files and reads can
--     reconstruct the exact dict the workers used to emit.
--   * `source_file` + `source_line` tag rows imported by the JSONL backfill
--     so it is idempotent; worker-written rows leave them NULL.
--
-- Applied on first connect by swegen.db.apply_schema().

-- ---------------------------------------------------------------------------
-- postcheck-status.jsonl  (ValidationWorker, +shared merged copy)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS postcheck_status (
    id              BIGSERIAL PRIMARY KEY,
    instance        TEXT        NOT NULL,
    attempt         INTEGER     NOT NULL DEFAULT 1,
    status          TEXT,
    stage           TEXT,
    event           TEXT        NOT NULL DEFAULT 'postcheck_status',
    schema_version  INTEGER     NOT NULL DEFAULT 1,
    worker_id       TEXT,
    checker_node    TEXT,
    source_node     TEXT,
    merged_from_backfill BOOLEAN NOT NULL DEFAULT FALSE,
    timestamp       TEXT,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_postcheck_latest
    ON postcheck_status (instance, attempt DESC, written_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_postcheck_status
    ON postcheck_status (status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_postcheck_backfill
    ON postcheck_status (source_file, source_line)
    WHERE source_file IS NOT NULL;

-- ---------------------------------------------------------------------------
-- reward-backfill-status.jsonl  (RewardBackfillWorker)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reward_backfill_status (
    id              BIGSERIAL PRIMARY KEY,
    instance        TEXT        NOT NULL,
    attempt         INTEGER     NOT NULL DEFAULT 1,
    status          TEXT,
    stage           TEXT,
    event           TEXT        NOT NULL DEFAULT 'reward_backfill_status',
    schema_version  INTEGER     NOT NULL DEFAULT 1,
    worker_id       TEXT,
    checker_node    TEXT,
    source_node     TEXT,
    timestamp       TEXT,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_reward_backfill_latest
    ON reward_backfill_status (instance, attempt DESC, written_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_reward_backfill_status
    ON reward_backfill_status (status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_reward_backfill_backfill
    ON reward_backfill_status (source_file, source_line)
    WHERE source_file IS NOT NULL;

-- ---------------------------------------------------------------------------
-- blacklist.jsonl  (ValidationWorker._record_blacklist)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS blacklist (
    id              BIGSERIAL PRIMARY KEY,
    instance        TEXT        NOT NULL,
    stage           TEXT,
    attempts        INTEGER,
    blacklisted_at  TEXT,
    error           TEXT,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_blacklist_instance
    ON blacklist (instance, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_blacklist_backfill
    ON blacklist (source_file, source_line)
    WHERE source_file IS NOT NULL;

-- ---------------------------------------------------------------------------
-- create.jsonl  (success ledger, swegen/create/create.py) — keyed by task_id
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS create_success (
    id              BIGSERIAL PRIMARY KEY,
    task_id         TEXT        NOT NULL,
    key             TEXT,
    repo            TEXT,
    pr              TEXT,
    harbor          TEXT,
    ts              TEXT,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_create_success_task
    ON create_success (task_id, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_create_success_backfill
    ON create_success (source_file, source_line)
    WHERE source_file IS NOT NULL;

-- ---------------------------------------------------------------------------
-- stage3-reward-guard.jsonl  (Stage3Guard.emit)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage3_reward_guard (
    id              BIGSERIAL PRIMARY KEY,
    instance        TEXT,
    event           TEXT        NOT NULL DEFAULT 'stage3_reward_guard',
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_stage3_guard_instance
    ON stage3_reward_guard (instance, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stage3_guard_backfill
    ON stage3_reward_guard (source_file, source_line)
    WHERE source_file IS NOT NULL;

-- ---------------------------------------------------------------------------
-- orchestrator-progress.jsonl  /  orchestrator-instance-status.jsonl
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orchestrator_progress (
    id              BIGSERIAL PRIMARY KEY,
    pr              TEXT,
    instance        TEXT,
    status          TEXT,
    event           TEXT        NOT NULL DEFAULT 'orchestrator_progress',
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_orch_progress_pr
    ON orchestrator_progress (pr, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_orch_progress_backfill
    ON orchestrator_progress (source_file, source_line)
    WHERE source_file IS NOT NULL;

CREATE TABLE IF NOT EXISTS orchestrator_instance_status (
    id              BIGSERIAL PRIMARY KEY,
    instance        TEXT        NOT NULL,
    status          TEXT,
    event           TEXT        NOT NULL DEFAULT 'orchestrator_instance_status',
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_orch_instance_latest
    ON orchestrator_instance_status (instance, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_orch_instance_backfill
    ON orchestrator_instance_status (source_file, source_line)
    WHERE source_file IS NOT NULL;

-- ---------------------------------------------------------------------------
-- all_images<suffix>.jsonl / pushed_images<suffix>.jsonl  (push_all_verified)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pushed_images (
    id              BIGSERIAL PRIMARY KEY,
    instance        TEXT        NOT NULL,
    registry        TEXT,
    suffix          TEXT        NOT NULL DEFAULT '',
    swr_url         TEXT,
    pushed          BOOLEAN     NOT NULL DEFAULT FALSE,
    event           TEXT        NOT NULL DEFAULT 'pushed_images',
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_file     TEXT,
    source_line     INTEGER,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_pushed_images_lookup
    ON pushed_images (registry, suffix, instance, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pushed_images_backfill
    ON pushed_images (source_file, source_line)
    WHERE source_file IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Distributed PGMQ pipeline state
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_tasks (
    task_id         TEXT        NOT NULL,
    task_version    INTEGER     NOT NULL,
    repo            TEXT        NOT NULL,
    pr              INTEGER     NOT NULL,
    trace_id        UUID        NOT NULL,
    state           TEXT        NOT NULL DEFAULT 'queued',
    current_stage   TEXT        NOT NULL DEFAULT 'generate',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    last_error      TEXT,
    last_reason     TEXT,
    CONSTRAINT pk_pipeline_tasks PRIMARY KEY (task_id, task_version),
    CONSTRAINT uq_pipeline_tasks_repo_pr_version UNIQUE (repo, pr, task_version),
    CONSTRAINT ck_pipeline_tasks_task_id_safe CHECK (
        task_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$'
    ),
    CONSTRAINT ck_pipeline_tasks_task_version_positive CHECK (task_version > 0),
    CONSTRAINT ck_pipeline_tasks_repo_safe CHECK (
        repo ~ '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'
        AND repo !~ '(^|/)\.{1,2}($|/)'
    ),
    CONSTRAINT ck_pipeline_tasks_pr_positive CHECK (pr > 0),
    CONSTRAINT ck_pipeline_tasks_state CHECK (
        state IN ('queued', 'running', 'rejected', 'failed', 'completed')
    ),
    CONSTRAINT ck_pipeline_tasks_current_stage CHECK (
        current_stage IN ('generate', 'validate', 'reward', 'push')
    )
);
CREATE INDEX IF NOT EXISTS idx_pipeline_tasks_state_stage
    ON pipeline_tasks (state, current_stage, updated_at);

CREATE TABLE IF NOT EXISTS pipeline_task_files (
    task_id         TEXT    NOT NULL,
    task_version    INTEGER NOT NULL,
    path            TEXT    NOT NULL,
    content         BYTEA   NOT NULL,
    mode            INTEGER NOT NULL,
    size_bytes      BIGINT  NOT NULL,
    sha256          TEXT    NOT NULL,
    CONSTRAINT pk_pipeline_task_files PRIMARY KEY (task_id, task_version, path),
    CONSTRAINT fk_pipeline_task_files_task FOREIGN KEY (task_id, task_version)
        REFERENCES pipeline_tasks (task_id, task_version) ON DELETE CASCADE,
    CONSTRAINT ck_pipeline_task_files_path_safe CHECK (
        path <> ''
        AND path NOT LIKE '/%'
        AND path NOT LIKE '%//%'
        AND path !~ '(^|/)\.{1,2}($|/)'
        AND strpos(path, chr(92)) = 0
    ),
    CONSTRAINT ck_pipeline_task_files_size_nonnegative CHECK (size_bytes >= 0),
    CONSTRAINT ck_pipeline_task_files_size_matches_content CHECK (
        size_bytes = octet_length(content)
    ),
    CONSTRAINT ck_pipeline_task_files_mode CHECK (mode BETWEEN 0 AND 511),
    CONSTRAINT ck_pipeline_task_files_sha256 CHECK (sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS pipeline_stage_results (
    task_id          TEXT        NOT NULL,
    task_version     INTEGER     NOT NULL,
    stage            TEXT        NOT NULL,
    attempt          INTEGER     NOT NULL,
    status           TEXT        NOT NULL,
    pgmq_msg_id      BIGINT      NOT NULL,
    pgmq_read_count  INTEGER     NOT NULL,
    worker_id        TEXT        NOT NULL,
    node_name        TEXT        NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    finished_at      TIMESTAMPTZ NOT NULL,
    result           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error            TEXT,
    CONSTRAINT pk_pipeline_stage_results
        PRIMARY KEY (task_id, task_version, stage, attempt),
    CONSTRAINT fk_pipeline_stage_results_task FOREIGN KEY (task_id, task_version)
        REFERENCES pipeline_tasks (task_id, task_version) ON DELETE CASCADE,
    CONSTRAINT ck_pipeline_stage_results_stage CHECK (
        stage IN ('generate', 'validate', 'reward', 'push')
    ),
    CONSTRAINT ck_pipeline_stage_results_attempt_positive CHECK (attempt > 0),
    CONSTRAINT ck_pipeline_stage_results_status CHECK (
        status IN ('succeeded', 'rejected', 'failed')
    ),
    CONSTRAINT ck_pipeline_stage_results_pgmq_msg_id_positive CHECK (pgmq_msg_id > 0),
    CONSTRAINT ck_pipeline_stage_results_pgmq_read_count_positive CHECK (pgmq_read_count > 0),
    CONSTRAINT ck_pipeline_stage_results_timestamps CHECK (finished_at >= started_at)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_stage_results_status
    ON pipeline_stage_results (status, finished_at DESC);
