\set ON_ERROR_STOP on

-- Requeue Generate tasks that failed with APIConnectionError (LLM endpoint unreachable).
--
-- The generate stage calls an LLM endpoint to produce task instructions. When
-- that endpoint is unreachable, the OpenAI client library raises APIConnectionError
-- and the task is marked failed — even though the failure is transient network
-- infrastructure, not a defect in the task inputs. These are safe to retry once
-- the endpoint is reachable.
--
-- Observed in last 24h: 95 APIConnectionError failures (typically connection
-- refused, DNS lookup failed, or network unreachable).
--
-- Deliberately NOT requeued:
--   * Tasks whose error is NOT APIConnectionError (includes RuntimeError from
--     the generate script itself failing, GitHub API timeouts, subprocess errors)
--   * Tasks already pushed to SWR (checked against both push stage results and
--     pushed_images ledger, since the sets overlap but aren't identical)
--   * Tasks whose latest attempt succeeded or are already queued
--
-- Idempotent via audit table PK + live-queue check.

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

CREATE TEMP TABLE generate_api_connection_requeue_candidates ON COMMIT DROP AS
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
      -- Narrow to a recent window where we've confirmed these are transient
      AND result.finished_at > now() - interval '24 hours'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
)
SELECT
    latest.*,
    'api_connection_error' AS failure_category,
    latest.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM latest_generate AS latest
WHERE error LIKE '%APIConnectionError%'
-- Never double-enqueue something already sitting in the live generate queue.
AND NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_generate AS live
    WHERE live.message->>'task_id' = latest.task_id
      AND (live.message->>'task_version')::INTEGER = latest.task_version
)
-- Idempotency against previous runs of this script (or other generate requeue scripts).
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_generate_requeue_audit AS audit
    WHERE audit.task_id = latest.task_id
      AND audit.task_version = latest.task_version
      AND audit.source_stage_attempt = latest.source_stage_attempt
)
-- SAFETY: skip anything already pushed to SWR (authoritative push stage).
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_stage_results AS pushed
    WHERE pushed.task_id = latest.task_id
      AND pushed.stage = 'push'
      AND pushed.status = 'succeeded'
)
-- SAFETY: and per the pushed_images ledger.
AND NOT EXISTS (
    SELECT 1
    FROM pushed_images AS ledger
    WHERE ledger.instance = latest.task_id
      AND ledger.pushed IS TRUE
);

ALTER TABLE generate_api_connection_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE generate_api_connection_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM generate_api_connection_requeue_candidates AS candidate
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

UPDATE generate_api_connection_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM generate_api_connection_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM generate_api_connection_requeue_candidates
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
FROM generate_api_connection_requeue_candidates AS candidate
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
    'Generate LLM endpoint APIConnectionError (network unreachable, transient infra)',
    'swegen_generate',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at
FROM generate_api_connection_requeue_candidates;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_generate_requeue_audit
WHERE reason =
    'Generate LLM endpoint APIConnectionError (network unreachable, transient infra)'
GROUP BY failure_category
ORDER BY requeued DESC, failure_category;
