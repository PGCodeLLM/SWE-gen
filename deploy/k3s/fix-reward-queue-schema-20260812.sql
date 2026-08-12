-- Fix reward queue schema mismatch that broke workers at 17:00 on 2026-08-12
-- 
-- ROOT CAUSE: The requeue script (requeue-reward-401-failures-20260812.sql) sent
-- messages with incomplete schema (missing event_id, stage, trace_id, enqueued_at)
-- and used wrong field names (stage_attempt instead of attempt). Workers rejected
-- all messages with validation errors.
--
-- FIX: Purged the entire reward queue and rebuilt from pipeline_tasks with correct
-- schema matching the working generate/validate queues.

BEGIN;

TRUNCATE pgmq.q_swegen_reward;

WITH reward_rebuild AS (
    SELECT 
        t.task_id,
        t.task_version,
        t.trace_id,
        COALESCE(
            (SELECT MAX(r.attempt) + 1 
             FROM pipeline_stage_results r 
             WHERE r.task_id = t.task_id 
               AND r.task_version = t.task_version 
               AND r.stage = 'reward'),
            1
        ) AS attempt,
        to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS enqueued_at_iso
    FROM pipeline_tasks t
    WHERE t.state = 'queued' 
      AND t.current_stage = 'reward'
),
enqueued AS (
    SELECT 
        *,
        pgmq.send(
            'swegen_reward',
            jsonb_build_object(
                'stage', 'reward',
                'attempt', attempt,
                'task_id', task_id,
                'task_version', task_version,
                'event_id', gen_random_uuid()::TEXT,
                'trace_id', trace_id,
                'enqueued_at', enqueued_at_iso,
                'schema_version', 1
            )
        ) AS msg_id
    FROM reward_rebuild
)
SELECT 
    COUNT(*) AS rebuilt,
    MIN(msg_id) AS first_msg_id,
    MAX(msg_id) AS last_msg_id
FROM enqueued;

COMMIT;
