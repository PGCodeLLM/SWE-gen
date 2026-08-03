-- Recover the high-confidence false Repair handoffs emitted by the old Repair
-- image on 2026-07-31.  The transaction deliberately leaves infrastructure
-- retries, mirror retries, real Repair handoffs, and active Validate leases in
-- swegen_validate_repaired.
--
-- Run with psql -X -v ON_ERROR_STOP=1 against swegen_distributed.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_queue_recovery_audit (
    recovery_id         UUID        NOT NULL,
    reason              TEXT        NOT NULL,
    source_queue        TEXT        NOT NULL,
    source_msg_id       BIGINT      NOT NULL,
    source_read_count   INTEGER     NOT NULL,
    source_enqueued_at  TIMESTAMPTZ NOT NULL,
    source_payload      JSONB       NOT NULL,
    task_id             TEXT        NOT NULL,
    task_version        INTEGER     NOT NULL,
    old_stage           TEXT        NOT NULL,
    old_attempt         INTEGER     NOT NULL,
    old_event_id        UUID        NOT NULL,
    target_queue        TEXT        NOT NULL,
    target_msg_id       BIGINT      NOT NULL,
    new_stage           TEXT        NOT NULL,
    new_attempt         INTEGER     NOT NULL,
    new_event_id        UUID        NOT NULL,
    new_enqueued_at     TIMESTAMPTZ NOT NULL,
    recovered_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_queue, source_msg_id),
    UNIQUE (target_queue, target_msg_id)
);

-- Prevent a Validate worker from claiming one of the selected visible rows
-- between the evidence check and pgmq.archive().
LOCK TABLE pgmq.q_swegen_validate_repaired IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE repair_bulk_recovery_candidates ON COMMIT DROP AS
WITH matched AS (
    SELECT
        q.msg_id AS source_msg_id,
        q.read_ct AS source_read_count,
        q.enqueued_at AS source_enqueued_at,
        q.message AS source_payload,
        task.task_id,
        task.task_version,
        task.trace_id,
        repair.attempt AS matched_repair_attempt,
        repair.started_at AS repair_started_at,
        repair.finished_at AS repair_finished_at,
        EXTRACT(EPOCH FROM repair.finished_at - repair.started_at) AS runtime_seconds,
        COALESCE((
            SELECT max(previous.attempt)
            FROM pipeline_stage_results AS previous
            WHERE previous.task_id = task.task_id
              AND previous.task_version = task.task_version
              AND previous.stage = 'repair'
        ), 0) AS latest_repair_attempt
    FROM pgmq.q_swegen_validate_repaired AS q
    JOIN pipeline_tasks AS task
      ON task.task_id = q.message->>'task_id'
     AND task.task_version = (q.message->>'task_version')::INTEGER
    JOIN pipeline_stage_results AS repair
      ON repair.task_id = task.task_id
     AND repair.task_version = task.task_version
     AND repair.stage = 'repair'
     AND abs(EXTRACT(EPOCH FROM (q.enqueued_at - repair.finished_at))) < 5
    LEFT JOIN pipeline_queue_recovery_audit AS prior_recovery
      ON prior_recovery.source_queue = 'swegen_validate_repaired'
     AND prior_recovery.source_msg_id = q.msg_id
    WHERE q.vt <= now()
      AND task.state = 'queued'
      AND task.current_stage = 'validate'
      AND repair.status = 'succeeded'
      AND EXTRACT(EPOCH FROM repair.finished_at - repair.started_at) < 30
      AND COALESCE((repair.result->>'agent_reported_success')::BOOLEAN, false) = false
      AND COALESCE((repair.result->>'agent_nop_passed')::BOOLEAN, false) = false
      AND COALESCE((repair.result->>'agent_oracle_passed')::BOOLEAN, false) = false
      AND NOT (repair.result ? 'agent_changed_files')
      AND prior_recovery.source_msg_id IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM pipeline_stage_activity AS activity
          WHERE activity.task_id = task.task_id
            AND activity.task_version = task.task_version
            AND activity.stage = 'validate'
            AND activity.pgmq_msg_id = q.msg_id
      )
      AND NOT EXISTS (
          SELECT 1
          FROM pgmq.q_swegen_repair AS queued_repair
          WHERE queued_repair.message->>'task_id' = task.task_id
            AND (queued_repair.message->>'task_version')::INTEGER = task.task_version
      )
      AND EXISTS (
          SELECT 1
          FROM pipeline_stage_results AS latest_validate
          WHERE latest_validate.task_id = task.task_id
            AND latest_validate.task_version = task.task_version
            AND latest_validate.stage = 'validate'
            AND latest_validate.status IN ('failed', 'rejected')
            AND latest_validate.attempt = (
                SELECT max(validate_result.attempt)
                FROM pipeline_stage_results AS validate_result
                WHERE validate_result.task_id = task.task_id
                  AND validate_result.task_version = task.task_version
                  AND validate_result.stage = 'validate'
            )
      )
)
SELECT
    gen_random_uuid() AS recovery_id,
    'old Repair image emitted a false success after a sub-30-second all-false run'
        AS reason,
    matched.*,
    gen_random_uuid() AS new_event_id,
    matched.latest_repair_attempt + 1 AS new_repair_attempt,
    clock_timestamp() AS new_enqueued_at
FROM matched;

DO $guard$
DECLARE
    candidate_count INTEGER;
    distinct_task_count INTEGER;
    invalid_attempt_count INTEGER;
BEGIN
    SELECT count(*), count(DISTINCT (task_id, task_version)),
           count(*) FILTER (WHERE new_repair_attempt NOT BETWEEN 1 AND 3)
      INTO candidate_count, distinct_task_count, invalid_attempt_count
      FROM repair_bulk_recovery_candidates;
    IF candidate_count <> 2446 OR distinct_task_count <> candidate_count THEN
        RAISE EXCEPTION
            'recovery evidence drifted: expected 2446 unique candidates, found % rows / % tasks',
            candidate_count, distinct_task_count;
    END IF;
    IF invalid_attempt_count <> 0 THEN
        RAISE EXCEPTION 'recovery would exceed the configured Repair attempt limit';
    END IF;
END
$guard$;

ALTER TABLE repair_bulk_recovery_candidates
    ADD COLUMN target_msg_id BIGINT,
    ADD COLUMN archived BOOLEAN NOT NULL DEFAULT false;

CREATE TEMP TABLE repair_bulk_recovery_sends ON COMMIT DROP AS
SELECT candidate.source_msg_id, sent.target_msg_id
FROM repair_bulk_recovery_candidates AS candidate
CROSS JOIN LATERAL pgmq.send(
    'swegen_repair',
    jsonb_build_object(
        'schema_version', 1,
        'event_id', candidate.new_event_id,
        'task_id', candidate.task_id,
        'task_version', candidate.task_version,
        'stage', 'repair',
        'attempt', candidate.new_repair_attempt,
        'trace_id', candidate.trace_id,
        'enqueued_at', candidate.new_enqueued_at
    ),
    0
) AS sent(target_msg_id);

UPDATE repair_bulk_recovery_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM repair_bulk_recovery_sends AS sent
WHERE sent.source_msg_id = candidate.source_msg_id;

UPDATE pipeline_tasks AS task
SET state = 'queued',
    current_stage = 'repair',
    updated_at = candidate.new_enqueued_at,
    finished_at = NULL,
    last_error = NULL,
    last_reason = NULL
FROM repair_bulk_recovery_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

UPDATE repair_bulk_recovery_candidates AS candidate
SET archived = pgmq.archive('swegen_validate_repaired', candidate.source_msg_id);

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM repair_bulk_recovery_candidates
        WHERE target_msg_id IS NULL OR target_msg_id <= 0 OR NOT archived
    ) THEN
        RAISE EXCEPTION 'PGMQ did not confirm every Repair send and priority archive';
    END IF;
END
$guard$;

INSERT INTO pipeline_queue_recovery_audit (
    recovery_id, reason,
    source_queue, source_msg_id, source_read_count, source_enqueued_at, source_payload,
    task_id, task_version, old_stage, old_attempt, old_event_id,
    target_queue, target_msg_id, new_stage, new_attempt, new_event_id, new_enqueued_at
)
SELECT
    recovery_id, reason,
    'swegen_validate_repaired', source_msg_id, source_read_count,
    source_enqueued_at, source_payload,
    task_id, task_version, source_payload->>'stage',
    (source_payload->>'attempt')::INTEGER, (source_payload->>'event_id')::UUID,
    'swegen_repair', target_msg_id, 'repair', new_repair_attempt,
    new_event_id, new_enqueued_at
FROM repair_bulk_recovery_candidates;

COMMIT;

SELECT count(*) AS recovered,
       min(source_msg_id) AS first_source_msg_id,
       max(source_msg_id) AS last_source_msg_id,
       min(target_msg_id) AS first_target_msg_id,
       max(target_msg_id) AS last_target_msg_id,
       min(recovered_at) AS recovered_at
FROM pipeline_queue_recovery_audit
WHERE reason =
    'old Repair image emitted a false success after a sub-30-second all-false run';
