\set ON_ERROR_STOP on

-- Generate failures lost to the 2026-08-18 inference-gateway outage.
--
-- Between 01:54 and 04:45 (+08) the gateway at 7.244.3.251:8088 returned 502 /
-- 504 / "ZaiException - Connection error" for every request. The worker harness
-- surfaced that as `RuntimeError: command exited with status 1` out of the Task
-- Generation stage, 8-11 seconds per task -- the agent never ran. 2,695 tasks
-- failed that way, all on attempt 1. The endpoint prober latched
-- `breaker_open = TRUE` at `consecutive_fail = 15` and the controller drove
-- every dynamic Generate pool to zero, where they stayed after the gateway
-- itself recovered.
--
-- The operator asked for the whole failed-Generate backlog back, not just the
-- outage window, so this requeues every `state = 'failed'` Generate task whose
-- latest attempt is below 5 -- 17,115 rows spanning 2026-08-05 to today.
--
-- Deliberately left alone: the 10,487 tasks whose latest attempt is 5 or higher.
-- Those have exhausted the delivery budget in `retry_or_dead_letter`; feeding
-- them back would burn a fleet-hour to re-derive the same terminal failure and
-- dead-letter them anyway. They need a diagnosis, not a retry.
--
-- Generate is held at zero for the duration: the breaker is still latched when
-- this runs, and is only cleared (probe-gated) afterwards.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_generate_requeue_gateway_outage_20260818_audit (
    task_id               TEXT        NOT NULL,
    task_version          INTEGER     NOT NULL,
    source_stage_attempt  INTEGER     NOT NULL,
    source_stage_status   TEXT        NOT NULL,
    source_finished_at    TIMESTAMPTZ NOT NULL,
    target_queue          TEXT        NOT NULL,
    target_msg_id         BIGINT      NOT NULL,
    new_stage_attempt     INTEGER     NOT NULL,
    new_event_id          UUID        NOT NULL,
    reason                TEXT        NOT NULL,
    requeued_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, task_version, source_stage_attempt),
    UNIQUE (target_queue, target_msg_id)
);

-- Generate is held at zero while this transaction runs. This lock prevents a
-- producer from inserting a duplicate between the live-message check and send.
LOCK TABLE pgmq.q_swegen_generate IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE gateway_outage_requeue_candidates ON COMMIT DROP AS
WITH latest_generate AS (
    SELECT
        task.task_id,
        task.task_version,
        task.trace_id,
        latest.attempt AS source_stage_attempt,
        latest.status AS source_stage_status,
        latest.finished_at AS source_finished_at
    FROM pipeline_tasks AS task
    JOIN LATERAL (
        SELECT result.attempt, result.status, result.finished_at
        FROM pipeline_stage_results AS result
        WHERE result.task_id = task.task_id
          AND result.task_version = task.task_version
          AND result.stage = 'generate'
        ORDER BY result.attempt DESC, result.finished_at DESC
        LIMIT 1
    ) AS latest ON true
    WHERE task.state = 'failed'
      AND task.current_stage = 'generate'
)
SELECT
    latest_generate.*,
    latest_generate.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM latest_generate
-- Retry budget is spent at attempt 5; those rows are excluded on purpose.
WHERE latest_generate.source_stage_attempt < 5
AND NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_generate AS live
    WHERE live.message->>'task_id' = latest_generate.task_id
      AND (live.message->>'task_version')::INTEGER = latest_generate.task_version
)
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_generate_requeue_gateway_outage_20260818_audit AS audit
    WHERE audit.task_id = latest_generate.task_id
      AND audit.task_version = latest_generate.task_version
      AND audit.source_stage_attempt = latest_generate.source_stage_attempt
);

ALTER TABLE gateway_outage_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE gateway_outage_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM gateway_outage_requeue_candidates AS candidate
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

UPDATE gateway_outage_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM gateway_outage_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gateway_outage_requeue_candidates
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
FROM gateway_outage_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_generate_requeue_gateway_outage_20260818_audit (
    task_id, task_version, source_stage_attempt, source_stage_status,
    source_finished_at, target_queue, target_msg_id, new_stage_attempt,
    new_event_id, reason, requeued_at
)
SELECT
    task_id,
    task_version,
    source_stage_attempt,
    source_stage_status,
    source_finished_at,
    'swegen_generate',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    'gateway outage 20260818; operator requeue of all failed Generate below attempt 5',
    requeued_at
FROM gateway_outage_requeue_candidates;

COMMIT;

SELECT count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id,
       min(requeued_at) AS first_requeued_at,
       max(requeued_at) AS last_requeued_at
FROM pipeline_generate_requeue_gateway_outage_20260818_audit
WHERE reason = 'gateway outage 20260818; operator requeue of all failed Generate below attempt 5';
