\set ON_ERROR_STOP on

-- Requeue Validate tasks lost to the node-0005 Docker daemon outage on
-- 2026-08-13.
--
-- At 2026-08-13 12:24:06 CST the Docker daemon on ecs-...-0005 restarted as
-- part of a 29.6.1 -> 29.6.2 upgrade. Every Validate worker with an in-flight
-- `docker compose build` lost its socket mid-build and Harbor reported
-- "Cannot connect to the Docker daemon at unix:///var/run/docker.sock".
-- ~6.1k tasks failed inside a ~15 minute window, against a baseline of ~50 per
-- 15 minutes. The captured build logs all carry the same 04:23:52Z stamp.
--
-- These tasks were never actually evaluated -- the environment never finished
-- building -- so the failure says nothing about task validity. Every candidate
-- is at attempt 1, so requeueing does not exhaust anyone's delivery budget.
--
-- Deliberately NOT requeued:
--   * `rejected` Validate tasks. Those ran to completion and reported a wrong
--     reward, which is a genuine task-validity failure; retrying reproduces the
--     same verdict and burns a delivery.
--   * Ordinary build failures (git clone TLS resets, buildkit solve errors).
--     Those are unrelated to the outage and are handled by their own requeues.

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

CREATE TEMP TABLE validate_daemon_requeue_candidates ON COMMIT DROP AS
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
      AND result.finished_at > now() - interval '6 hour'
      AND task.state = 'failed'
      AND task.current_stage = 'validate'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
), classified AS (
    SELECT
        latest.*,
        'docker_daemon_outage' AS failure_category
    FROM latest_validate AS latest
    -- Only the most recent Validate attempt, and only an outright failure.
    WHERE status = 'failed'
      AND error ILIKE '%Cannot connect to the Docker daemon%'
      -- A reward mismatch is a task defect even if the daemon also blipped.
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

ALTER TABLE validate_daemon_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE validate_daemon_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM validate_daemon_requeue_candidates AS candidate
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

UPDATE validate_daemon_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM validate_daemon_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM validate_daemon_requeue_candidates
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
FROM validate_daemon_requeue_candidates AS candidate
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
    'Validate lost to the node-0005 Docker daemon restart on 2026-08-13',
    'swegen_validate',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at
FROM validate_daemon_requeue_candidates;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_validate_requeue_audit
WHERE reason =
    'Validate lost to the node-0005 Docker daemon restart on 2026-08-13'
GROUP BY failure_category
ORDER BY failure_category;
