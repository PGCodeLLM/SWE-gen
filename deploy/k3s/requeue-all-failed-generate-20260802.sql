\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_generate_requeue_all_failed_audit (
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

CREATE TEMP TABLE failed_generate_requeue_candidates ON COMMIT DROP AS
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
WHERE NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_generate AS live
    WHERE live.message->>'task_id' = latest_generate.task_id
      AND (live.message->>'task_version')::INTEGER = latest_generate.task_version
)
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_generate_requeue_all_failed_audit AS audit
    WHERE audit.task_id = latest_generate.task_id
      AND audit.task_version = latest_generate.task_version
      AND audit.source_stage_attempt = latest_generate.source_stage_attempt
);

ALTER TABLE failed_generate_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE failed_generate_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM failed_generate_requeue_candidates AS candidate
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

UPDATE failed_generate_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM failed_generate_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM failed_generate_requeue_candidates
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
FROM failed_generate_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_generate_requeue_all_failed_audit (
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
    'operator requested GLM Generate restart at 32',
    requeued_at
FROM failed_generate_requeue_candidates;

COMMIT;

SELECT count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id,
       min(requeued_at) AS first_requeued_at,
       max(requeued_at) AS last_requeued_at
FROM pipeline_generate_requeue_all_failed_audit
WHERE reason = 'operator requested GLM Generate restart at 32';
