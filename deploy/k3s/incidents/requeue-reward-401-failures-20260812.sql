-- Requeue reward tasks that failed with HTTP 401 "Invalid token" errors
-- between 15:00-17:00 on 2026-08-12 when the old gpt-5.6-sol endpoint
-- token expired. New endpoint configured at arcyleung-ubuntu.tailb940e6.ts.net.

BEGIN;

LOCK TABLE pgmq.q_swegen_reward IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE reward_401_requeue_candidates ON COMMIT DROP AS
WITH latest_reward AS (
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
    WHERE result.stage = 'reward'
      AND result.status = 'failed'
      AND task.state = 'failed'
      AND task.current_stage = 'reward'
      AND result.finished_at > now() - interval '3 hours'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
)
SELECT
    latest.*,
    '401_invalid_token' AS failure_category,
    latest.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM latest_reward AS latest
WHERE error ILIKE '%HTTP 401%无效的令牌%'
AND NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_reward AS live
    WHERE live.message->>'task_id' = latest.task_id
      AND (live.message->>'task_version')::INTEGER = latest.task_version
)
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_reward_requeue_audit AS audit
    WHERE audit.task_id = latest.task_id
      AND audit.task_version = latest.task_version
      AND audit.source_stage_attempt = latest.source_stage_attempt
);

UPDATE pipeline_tasks AS task
SET state = 'queued',
    current_stage = 'reward',
    updated_at = candidate.requeued_at,
    finished_at = NULL,
    last_error = NULL,
    last_reason = NULL
FROM reward_401_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

WITH enqueued AS (
    SELECT
        candidate.*,
        pgmq.send(
            'swegen_reward',
            jsonb_build_object(
                'task_id', candidate.task_id,
                'task_version', candidate.task_version,
                'stage_attempt', candidate.new_stage_attempt,
                'event_id', candidate.new_event_id::TEXT
            )
        ) AS msg_id
    FROM reward_401_requeue_candidates AS candidate
)
INSERT INTO pipeline_reward_requeue_audit (
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
    'Retry reward tasks that failed with HTTP 401 invalid token (old endpoint expired, now using arcyleung-ubuntu)',
    'swegen_reward',
    msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at
FROM enqueued;

COMMIT;

SELECT 
    COUNT(*) AS requeued,
    MIN(target_msg_id) AS first_msg_id,
    MAX(target_msg_id) AS last_msg_id
FROM pipeline_reward_requeue_audit
WHERE reason ILIKE '%401 invalid token%'
  AND requeued_at > now() - interval '5 minutes';
