\set ON_ERROR_STOP on

-- Requeue Generate tasks lost when the glm-5.2-pretrain-256k-v1 backend was
-- withdrawn from the gateway on 2026-08-14 at ~20:50.
--
-- Generate had been running at ~95% success on that model with 96 workers all
-- evening. At 20:50 it went to ZERO successes and 1,616 failures in ten
-- minutes. The router is explicit about why:
--
--   400 - litellm.BadRequestError: You passed in model=GLM-52_pre-train_256K.
--   There are no healthy deployments for this model.
--   Received Model Group=GLM-52_pre-train_256K
--
-- The model id still resolves at the router, so the request is accepted and
-- then fails at dispatch -- there is no upstream left to serve it. That is an
-- infrastructure withdrawal, not a task defect: the same tasks were succeeding
-- against the same code minutes earlier.
--
-- All Generate workers (static and every dynamic pool) are scaled to 0 while
-- this lands. The requeued work must NOT be handed back to a model group with
-- no healthy deployment -- it would fail instantly and burn each task's
-- delivery budget. Scale Generate back up only after repointing it at a model
-- verified to answer AND to return schema-valid JSON.
--
-- Deliberately NOT requeued:
--   * github_pr_gone (404) -- the upstream PR is gone; no retry fixes that.
--   * Anything already pushed to SWR, per both the push stage and the
--     pushed_images ledger, matching the sibling generate requeue scripts.
--
-- Idempotent via the audit table PK plus a live-queue check.

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

CREATE TEMP TABLE generate_dead_model_20260814b ON COMMIT DROP AS
WITH latest_generate AS (
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
    WHERE result.stage = 'generate'
      AND result.finished_at > now() - interval '3 hours'
      AND task.state = 'failed'
      AND task.current_stage = 'generate'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
), classified AS (
    SELECT
        latest.*,
        CASE
            WHEN error ILIKE '%no healthy deployments%'
                THEN 'model_group_withdrawn'
            WHEN error ILIKE '%502%' OR error ILIKE '%Bad Gateway%'
                THEN 'gateway_502'
            ELSE 'gateway_connection'
        END AS failure_category
    FROM latest_generate AS latest
    WHERE status = 'failed'
      AND (
          error ILIKE '%no healthy deployments%'
          OR error ILIKE '%502%'
          OR error ILIKE '%Bad Gateway%'
          OR error ILIKE '%APIConnection%'
          OR error ILIKE '%APITimeout%'
      )
      -- A vanished upstream PR is a task defect, not an endpoint problem.
      AND error NOT ILIKE '%404%'
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
)
-- SAFETY: never resurrect something already pushed to SWR.
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_stage_results AS pushed
    WHERE pushed.task_id = classified.task_id
      AND pushed.stage = 'push'
      AND pushed.status = 'succeeded'
)
AND NOT EXISTS (
    SELECT 1
    FROM pushed_images AS ledger
    WHERE ledger.instance = classified.task_id
      AND ledger.pushed IS TRUE
);

ALTER TABLE generate_dead_model_20260814b ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE generate_dead_model_20260814b_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM generate_dead_model_20260814b AS candidate
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

UPDATE generate_dead_model_20260814b AS candidate
SET target_msg_id = sent.target_msg_id
FROM generate_dead_model_20260814b_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1 FROM generate_dead_model_20260814b
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
FROM generate_dead_model_20260814b AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_generate_requeue_audit (
    task_id, task_version, source_stage_attempt, source_finished_at,
    failure_category, reason, target_queue, target_msg_id,
    new_stage_attempt, new_event_id, requeued_at
)
SELECT
    task_id, task_version, source_stage_attempt, source_finished_at,
    failure_category,
    'Generate lost when the glm-5.2-pretrain-256k-v1 model group lost every healthy deployment on 2026-08-14',
    'swegen_generate',
    target_msg_id, new_stage_attempt, new_event_id, requeued_at
FROM generate_dead_model_20260814b;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_generate_requeue_audit
WHERE reason =
    'Generate lost when the glm-5.2-pretrain-256k-v1 model group lost every healthy deployment on 2026-08-14'
GROUP BY failure_category
ORDER BY requeued DESC;
