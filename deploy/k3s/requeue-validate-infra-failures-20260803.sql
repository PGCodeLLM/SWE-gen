\set ON_ERROR_STOP on

-- Requeue Validate tasks lost to infrastructure, after the Go/Rust module
-- mirrors went live on 2026-08-03.
--
-- Two classes are requeued:
--   * Harbor nop/oracle timeouts (3600s) -- the environment never finished
--     building, so the task was never actually evaluated.
--   * Go and Rust dependency fetches, plus their network signatures. These hit
--     proxy.golang.org and index.crates.io directly because neither toolchain
--     reads the npm or apt mirrors; a measured fetch went from a 61s/100s
--     timeout to under 4s once the mirrors were configured.
--
-- Deliberately NOT requeued: tasks whose Validate stage was `rejected`. Those
-- ran to completion and reported a wrong reward (unexpected_nop_reward or
-- unexpected_oracle_reward), which is a genuine task-validity failure. Retrying
-- them would produce the same verdict and burn their remaining deliveries.
-- Ordinary build/exec errors are also left alone: a Dockerfile that cannot
-- build is not fixed by a registry mirror.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_validate_requeue_audit (
    task_id               TEXT        NOT NULL,
    task_version          INTEGER     NOT NULL,
    source_stage_attempt  INTEGER     NOT NULL,
    source_finished_at    TIMESTAMPTZ NOT NULL,
    failure_category      TEXT        NOT NULL,
    reason                TEXT        NOT NULL,
    target_queue          TEXT        NOT NULL,
    target_msg_id         BIGINT      NOT NULL,
    new_stage_attempt     INTEGER     NOT NULL,
    new_event_id          UUID        NOT NULL,
    requeued_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, task_version, source_stage_attempt),
    UNIQUE (target_queue, target_msg_id)
);

LOCK TABLE pgmq.q_swegen_validate IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE validate_infra_requeue_candidates ON COMMIT DROP AS
WITH latest_validate AS (
    SELECT DISTINCT ON (result.task_id, result.task_version)
        result.task_id,
        result.task_version,
        result.attempt AS source_stage_attempt,
        result.finished_at AS source_finished_at,
        result.status,
        result.error,
        task.trace_id
    FROM pipeline_stage_results AS result
    JOIN pipeline_tasks AS task
      ON task.task_id = result.task_id
     AND task.task_version = result.task_version
    WHERE result.stage = 'validate'
      AND result.finished_at > now() - interval '5 hour'
      AND task.state = 'failed'
      AND task.current_stage = 'validate'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
), classified AS (
    SELECT
        latest.*,
        CASE
            WHEN error LIKE '%timed out after%' THEN 'harbor_timeout'
            WHEN error ILIKE '%cargo fetch%' OR error ILIKE '%crates.io%'
                THEN 'cargo_fetch'
            WHEN error ILIKE '%go mod download%' OR error ILIKE '%proxy.golang.org%'
                THEN 'go_mod_download'
            ELSE 'network'
        END AS failure_category
    FROM latest_validate AS latest
    -- Only the most recent Validate attempt matters, and only when it failed
    -- outright. A `rejected` status means the rewards were wrong, which no
    -- amount of retrying changes.
    WHERE status = 'failed'
      AND (
          error LIKE '%timed out after%'
          OR error ILIKE '%cargo fetch%'
          OR error ILIKE '%crates.io%'
          OR error ILIKE '%go mod download%'
          OR error ILIKE '%proxy.golang.org%'
          OR error ILIKE '%dial tcp%'
          OR error ILIKE '%i/o timeout%'
          OR error ILIKE '%spurious network%'
          OR error ILIKE '%Temporary failure resolving%'
      )
      -- A reward mismatch is a task defect even when the text mentions a
      -- timeout somewhere in the captured build log.
      AND error NOT ILIKE '%unexpected_nop_reward%'
      AND error NOT ILIKE '%unexpected_oracle_reward%'
)
SELECT
    classified.*,
    classified.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM classified
WHERE NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_validate AS live
    WHERE live.message->>'task_id' = classified.task_id
      AND (live.message->>'task_version')::INTEGER = classified.task_version
)
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_validate_requeue_audit AS audit
    WHERE audit.task_id = classified.task_id
      AND audit.task_version = classified.task_version
      AND audit.source_stage_attempt = classified.source_stage_attempt
);

ALTER TABLE validate_infra_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE validate_infra_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM validate_infra_requeue_candidates AS candidate
CROSS JOIN LATERAL pgmq.send(
    'swegen_validate',
    jsonb_build_object(
        'schema_version', 1,
        'event_id', candidate.new_event_id,
        'task_id', candidate.task_id,
        'task_version', candidate.task_version,
        'stage', 'validate',
        'attempt', candidate.new_stage_attempt,
        'trace_id', candidate.trace_id,
        'enqueued_at', candidate.requeued_at
    ),
    0
) AS sent(target_msg_id);

UPDATE validate_infra_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM validate_infra_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM validate_infra_requeue_candidates
        WHERE target_msg_id IS NULL OR target_msg_id <= 0
    ) THEN
        RAISE EXCEPTION 'PGMQ did not confirm every Validate requeue send';
    END IF;
END
$guard$;

UPDATE pipeline_tasks AS task
SET state = 'queued',
    current_stage = 'validate',
    updated_at = candidate.requeued_at,
    finished_at = NULL,
    last_error = NULL,
    last_reason = NULL
FROM validate_infra_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_validate_requeue_audit (
    task_id, task_version, source_stage_attempt, source_finished_at,
    failure_category, reason, target_queue, target_msg_id,
    new_stage_attempt, new_event_id, requeued_at
)
SELECT
    task_id,
    task_version,
    source_stage_attempt,
    source_finished_at,
    failure_category,
    'Validate infrastructure failure before the Go/Rust mirrors went live 2026-08-03',
    'swegen_validate',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at
FROM validate_infra_requeue_candidates;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_validate_requeue_audit
WHERE reason =
    'Validate infrastructure failure before the Go/Rust mirrors went live 2026-08-03'
GROUP BY failure_category
ORDER BY failure_category;
