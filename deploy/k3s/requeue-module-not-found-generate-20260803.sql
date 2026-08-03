\set ON_ERROR_STOP on

-- Requeue Generate tasks lost to the 2026-08-03 02:04-05:40 crash loop on node
-- ecs-z00579134-20260707-bugfix-0003.  Every worker there died at import time
-- with "ModuleNotFoundError: No module named 'swegen'", so each delivery burned
-- in ~0.3s without the task ever being attempted.  The image itself is intact
-- (it imports swegen fine), so these tasks were never actually evaluated and
-- are safe to retry.
--
-- Run with Generate held at zero so no worker can claim a task between the
-- live-queue dedupe and pgmq.send().

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_generate_requeue_audit (
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

LOCK TABLE pgmq.q_swegen_generate IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE module_not_found_requeue_candidates ON COMMIT DROP AS
WITH latest_generate AS (
    SELECT DISTINCT ON (result.task_id, result.task_version)
        result.task_id,
        result.task_version,
        result.attempt AS source_stage_attempt,
        result.finished_at AS source_finished_at,
        result.error,
        task.trace_id
    FROM pipeline_stage_results AS result
    JOIN pipeline_tasks AS task
      ON task.task_id = result.task_id
     AND task.task_version = result.task_version
    WHERE result.stage = 'generate'
      AND result.status = 'failed'
      AND task.state = 'failed'
      AND task.current_stage = 'generate'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
), classified AS (
    SELECT
        latest.*,
        'module_not_found'::TEXT AS failure_category
    FROM latest_generate AS latest
    -- Only the interpreter-level import crash.  A task that failed for its own
    -- reasons (unmerged PR, oversized file, bad SHA) must not be resurrected
    -- here: it would fail again identically and re-burn its deliveries.
    WHERE error LIKE '%No module named ''swegen''%'
)
SELECT
    classified.*,
    classified.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM classified
WHERE NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_generate AS live
    WHERE live.message->>'task_id' = classified.task_id
      AND (live.message->>'task_version')::INTEGER = classified.task_version
)
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_generate_requeue_audit AS audit
    WHERE audit.task_id = classified.task_id
      AND audit.task_version = classified.task_version
      AND audit.source_stage_attempt = classified.source_stage_attempt
);

ALTER TABLE module_not_found_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE module_not_found_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM module_not_found_requeue_candidates AS candidate
CROSS JOIN LATERAL pgmq.send(
    'swegen_generate',
    jsonb_build_object(
        'schema_version', 1,
        'event_id', candidate.new_event_id,
        'task_id', candidate.task_id,
        'task_version', candidate.task_version,
        'stage', 'generate',
        'attempt', candidate.new_stage_attempt,
        'trace_id', candidate.trace_id,
        'enqueued_at', candidate.requeued_at
    ),
    0
) AS sent(target_msg_id);

UPDATE module_not_found_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM module_not_found_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM module_not_found_requeue_candidates
        WHERE target_msg_id IS NULL OR target_msg_id <= 0
    ) THEN
        RAISE EXCEPTION 'PGMQ did not confirm every Generate requeue send';
    END IF;
END
$guard$;

UPDATE pipeline_tasks AS task
SET state = 'queued',
    current_stage = 'generate',
    updated_at = candidate.requeued_at,
    finished_at = NULL,
    last_error = NULL,
    last_reason = NULL
FROM module_not_found_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_generate_requeue_audit (
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
    'ModuleNotFoundError crash loop on bugfix-0003, 2026-08-03 02:04-05:40',
    'swegen_generate',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at
FROM module_not_found_requeue_candidates;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id,
       min(requeued_at) AS first_requeued_at,
       max(requeued_at) AS last_requeued_at
FROM pipeline_generate_requeue_audit
WHERE reason =
    'ModuleNotFoundError crash loop on bugfix-0003, 2026-08-03 02:04-05:40'
GROUP BY failure_category
ORDER BY failure_category;
