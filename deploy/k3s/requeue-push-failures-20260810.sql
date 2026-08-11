\set ON_ERROR_STOP on

-- Requeue Push tasks that failed in the last 24 hours.
--
-- The push stage builds and uploads task images to SWR. Failures are typically
-- transient: network issues reaching SWR, buildkit cache contention, registry
-- timeouts, or ephemeral disk pressure. The error messages are usually terse
-- ("image push failed for <task_id>", "image build failed for <task_id>") with
-- no specific classification, so this script requeues ALL recent push failures
-- that have not already been retried or succeeded — matching the pattern of the
-- previous manual requeue on 2026-08-04.
--
-- Deliberately NOT requeued:
--   * Tasks whose latest push attempt succeeded
--   * Tasks already in the live push queue
--   * Tasks already requeued (idempotency via audit table PK)
--
-- No SWR-already-pushed exclusion needed: if a task succeeded at push, it won't
-- match the "status='failed'" filter. The audit table PK ensures idempotency.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_push_requeue_audit (
    batch_id              UUID        NOT NULL DEFAULT gen_random_uuid(),
    task_id               TEXT        NOT NULL,
    task_version          INTEGER     NOT NULL,
    source_stage_attempt  INTEGER     NOT NULL,
    source_finished_at    TIMESTAMPTZ NOT NULL,
    source_error          TEXT,
    target_queue          TEXT        NOT NULL,
    target_msg_id         BIGINT      NOT NULL,
    new_stage_attempt     INTEGER     NOT NULL,
    new_event_id          UUID        NOT NULL,
    requeued_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason                TEXT        NOT NULL,
    PRIMARY KEY (task_id, task_version, source_stage_attempt),
    UNIQUE (target_queue, target_msg_id)
);

LOCK TABLE pgmq.q_swegen_push IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE push_requeue_candidates ON COMMIT DROP AS
WITH latest_push AS (
    SELECT DISTINCT ON (result.task_id, result.task_version)
        result.task_id,
        result.task_version,
        result.attempt AS source_stage_attempt,
        result.finished_at AS source_finished_at,
        result.error AS source_error,
        task.trace_id
    FROM pipeline_stage_results AS result
    JOIN pipeline_tasks AS task
      ON task.task_id = result.task_id
     AND task.task_version = result.task_version
    WHERE result.stage = 'push'
      AND result.status = 'failed'
      AND task.state = 'failed'
      AND task.current_stage = 'push'
      -- Narrow to last 24h where we know these are transient
      AND result.finished_at > now() - interval '24 hours'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
)
SELECT
    latest.*,
    latest.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM latest_push AS latest
-- Never double-enqueue something already sitting in the live push queue.
WHERE NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_push AS live
    WHERE live.message->>'task_id' = latest.task_id
      AND (live.message->>'task_version')::INTEGER = latest.task_version
)
-- Idempotency against previous runs.
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_push_requeue_audit AS audit
    WHERE audit.task_id = latest.task_id
      AND audit.task_version = latest.task_version
      AND audit.source_stage_attempt = latest.source_stage_attempt
)
-- Sanity: if somehow a later attempt succeeded, don't requeue the earlier failure.
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_stage_results AS later
    WHERE later.task_id = latest.task_id
      AND later.task_version = latest.task_version
      AND later.stage = 'push'
      AND later.status = 'succeeded'
);

ALTER TABLE push_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE push_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM push_requeue_candidates AS candidate
CROSS JOIN LATERAL pgmq.send(
    'swegen_push',
    jsonb_build_object(
        'schema_version', 1,
        'event_id', candidate.new_event_id,
        'task_id', candidate.task_id,
        'task_version', candidate.task_version,
        'stage', 'push',
        'attempt', candidate.new_stage_attempt,
        'trace_id', candidate.trace_id,
        'enqueued_at', candidate.requeued_at
    ),
    0
) AS sent(target_msg_id);

UPDATE push_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM push_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM push_requeue_candidates
        WHERE target_msg_id IS NULL OR target_msg_id <= 0
    ) THEN
        RAISE EXCEPTION 'PGMQ did not confirm every Push requeue send';
    END IF;
END
$guard$;

UPDATE pipeline_tasks AS task
SET state = 'queued',
    current_stage = 'push',
    updated_at = candidate.requeued_at,
    finished_at = NULL,
    last_error = NULL,
    last_reason = NULL
FROM push_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_push_requeue_audit (
    batch_id, task_id, task_version, source_stage_attempt, source_finished_at,
    source_error, target_queue, target_msg_id,
    new_stage_attempt, new_event_id, requeued_at, reason
)
SELECT
    gen_random_uuid(),
    task_id,
    task_version,
    source_stage_attempt,
    source_finished_at,
    source_error,
    'swegen_push',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at,
    'Retry latest Push failures from the last 24 hours (transient build/push/registry issues)'
FROM push_requeue_candidates;

COMMIT;

SELECT count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_push_requeue_audit
WHERE reason =
    'Retry latest Push failures from the last 24 hours (transient build/push/registry issues)'
  AND requeued_at > now() - interval '5 minutes';
