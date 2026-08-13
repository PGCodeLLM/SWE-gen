\set ON_ERROR_STOP on

-- Requeue Validate tasks that timed out waiting for a build slot on 2026-08-13.
--
-- Scaling the fleet to 480 generate + 220 validate + 96 repair put 796
-- docker-consuming workers against 288 node-local build slots (72 x 4), a 2.8x
-- oversubscription. A worker that cannot get a slot still burns Harbor's
-- 3600-second ceiling while it waits, so the task dies with
-- "TimeoutError: Harbor nop timed out after 3600 seconds" without its
-- environment ever being built. The validate timeout rate tracks fleet size
-- exactly: 0.2% at 12:00 on a small fleet, 43% at 15:00 once generate reached
-- 480, and 79% by 18:00 after repair was added.
--
-- These tasks were never evaluated, so the timeout says nothing about task
-- validity -- it is a scheduling failure. generate has since been scaled to
-- 220, taking oversubscription to 1.9x.
--
-- Deliberately NOT requeued:
--   * `rejected` Validate tasks. Those ran to completion and reported a wrong
--     reward, a genuine task-validity failure that a retry only reproduces.
--   * Build failures, missing reward files, and git transport errors. Those
--     have their own causes and are handled separately; only the slot-starvation
--     timeout signature is in scope here.
--
-- Idempotent via the audit table PK plus a live-queue check. Note that
-- pgmq.send is wrapped to route validate messages with attempt > 1 to
-- swegen_validate_repaired, so most of this batch lands in that queue.

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

CREATE TEMP TABLE validate_timeout_requeue_candidates ON COMMIT DROP AS
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
      AND result.finished_at > now() - interval '8 hours'
      AND task.state = 'failed'
      AND task.current_stage = 'validate'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
), classified AS (
    SELECT
        latest.*,
        'build_slot_starvation_timeout' AS failure_category
    FROM latest_validate AS latest
    WHERE status = 'failed'
      AND error ILIKE '%timed out after%'
      -- A reward mismatch is a task defect even when a timeout appears in the
      -- captured log.
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
-- The routing wrapper sends attempt > 1 here, so this queue must be checked too
-- or a task already awaiting a retry would be enqueued twice.
AND NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_validate_repaired AS live
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

ALTER TABLE validate_timeout_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE validate_timeout_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM validate_timeout_requeue_candidates AS candidate
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

UPDATE validate_timeout_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM validate_timeout_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM validate_timeout_requeue_candidates
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
FROM validate_timeout_requeue_candidates AS candidate
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
    'Validate timed out waiting for a build slot during the 2026-08-13 fleet overscale',
    'swegen_validate',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at
FROM validate_timeout_requeue_candidates;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_validate_requeue_audit
WHERE reason =
    'Validate timed out waiting for a build slot during the 2026-08-13 fleet overscale'
GROUP BY failure_category
ORDER BY failure_category;
