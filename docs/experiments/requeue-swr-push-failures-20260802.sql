-- Transactional recovery of failed Push tasks whose exact remote BuildKit
-- output is already present in SWR.
--
-- Recovery ID: 20260802_swr_push_requeue_v1
--
-- This is deliberately limited to tasks with a successful remote build row.
-- Historic Push failures without a surviving local image or successful remote
-- image need a rebuild and are not safe no-op Push retries.

BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '10min';
SELECT pg_advisory_xact_lock(
    hashtext('swegen-manual-requeue'),
    hashtext('20260802-swr-push-v1')
);

CREATE TABLE IF NOT EXISTS pipeline_manual_requeue_audit (
    recovery_id        TEXT        NOT NULL,
    task_id            TEXT        NOT NULL,
    task_version       INTEGER     NOT NULL,
    source_stage       TEXT        NOT NULL,
    source_attempt     INTEGER     NOT NULL,
    source_status      TEXT        NOT NULL,
    source_finished_at TIMESTAMPTZ NOT NULL,
    source_worker_id   TEXT        NOT NULL,
    source_error_md5   TEXT        NOT NULL,
    classification     TEXT        NOT NULL,
    target_stage       TEXT        NOT NULL,
    target_queue       TEXT        NOT NULL,
    target_attempt     INTEGER     NOT NULL,
    event_id           UUID        NOT NULL,
    pgmq_msg_id        BIGINT      NOT NULL,
    enqueued_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (recovery_id, task_id, task_version),
    UNIQUE (target_queue, pgmq_msg_id)
);

CREATE TEMP TABLE recovery_candidates ON COMMIT DROP AS
WITH latest_push AS (
    SELECT DISTINCT ON (result.task_id, result.task_version)
        result.*
    FROM pipeline_stage_results AS result
    WHERE result.stage = 'push'
    ORDER BY result.task_id, result.task_version, result.attempt DESC
), eligible AS (
    SELECT
        latest.task_id,
        latest.task_version,
        task.trace_id,
        latest.attempt AS source_attempt,
        latest.status AS source_status,
        latest.finished_at AS source_finished_at,
        latest.worker_id AS source_worker_id,
        md5(COALESCE(latest.error, '')) AS source_error_md5,
        'push_remote_image_present'::TEXT AS classification,
        'push'::TEXT AS target_stage,
        'swegen_push'::TEXT AS target_queue
    FROM latest_push AS latest
    JOIN pipeline_tasks AS task
      USING (task_id, task_version)
    WHERE latest.status = 'failed'
      AND task.state = 'failed'
      AND task.current_stage = 'push'
      AND EXISTS (
          SELECT 1
          FROM pipeline_remote_builds AS build
          WHERE build.environment_name = latest.task_id
            AND build.route = 'remote'
            AND build.status = 'success'
            AND build.image_ref IS NOT NULL
            AND build.image_ref LIKE (
                'swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/' ||
                'swesandbox/public/swe-gen/feature-implementation/generated:%'
            )
      )
)
SELECT
    eligible.*,
    COALESCE((
        SELECT MAX(previous.attempt)
        FROM pipeline_stage_results AS previous
        WHERE previous.task_id = eligible.task_id
          AND previous.task_version = eligible.task_version
          AND previous.stage = 'push'
    ), 0) + 1 AS target_attempt,
    gen_random_uuid() AS event_id,
    clock_timestamp() AS enqueued_at
FROM eligible
WHERE NOT EXISTS (
        SELECT 1
        FROM pipeline_stage_activity AS activity
        WHERE activity.task_id = eligible.task_id
          AND activity.task_version = eligible.task_version
          AND activity.stage = 'push'
          AND activity.heartbeat_at > now() - INTERVAL '5 minutes'
    )
  AND NOT EXISTS (
        SELECT 1
        FROM pgmq.q_swegen_push AS queued
        WHERE queued.message ->> 'task_id' = eligible.task_id
          AND (queued.message ->> 'task_version')::INTEGER = eligible.task_version
    )
  AND NOT EXISTS (
        SELECT 1
        FROM pipeline_stage_results AS later
        WHERE later.task_id = eligible.task_id
          AND later.task_version = eligible.task_version
          AND later.stage = 'push'
          AND later.status = 'succeeded'
          AND later.finished_at > eligible.source_finished_at
    )
  AND NOT EXISTS (
        SELECT 1
        FROM pipeline_manual_requeue_audit AS prior_audit
        WHERE prior_audit.recovery_id = '20260802_swr_push_requeue_v1'
          AND prior_audit.task_id = eligible.task_id
          AND prior_audit.task_version = eligible.task_version
    );

SELECT COUNT(*) AS selected_for_requeue FROM recovery_candidates;

CREATE TEMP TABLE recovery_task_updates ON COMMIT DROP AS
WITH updated AS (
    UPDATE pipeline_tasks AS task
    SET state = 'queued',
        current_stage = 'push',
        updated_at = candidate.enqueued_at,
        finished_at = NULL,
        last_error = NULL,
        last_reason = 'manual_requeue:' || candidate.classification
    FROM recovery_candidates AS candidate
    WHERE task.task_id = candidate.task_id
      AND task.task_version = candidate.task_version
      AND task.state = 'failed'
      AND task.current_stage = 'push'
    RETURNING task.task_id, task.task_version
)
SELECT * FROM updated;

DO $assert_updates$
BEGIN
    IF (SELECT COUNT(*) FROM recovery_task_updates)
       <> (SELECT COUNT(*) FROM recovery_candidates) THEN
        RAISE EXCEPTION 'candidate/task update count mismatch: % versus %',
            (SELECT COUNT(*) FROM recovery_candidates),
            (SELECT COUNT(*) FROM recovery_task_updates);
    END IF;
END
$assert_updates$;

CREATE TEMP TABLE recovery_sent ON COMMIT DROP AS
SELECT
    candidate.*,
    sent.msg_id AS pgmq_msg_id
FROM recovery_candidates AS candidate
CROSS JOIN LATERAL pgmq.send(
    candidate.target_queue,
    jsonb_build_object(
        'schema_version', 1,
        'event_id', candidate.event_id,
        'task_id', candidate.task_id,
        'task_version', candidate.task_version,
        'stage', candidate.target_stage,
        'attempt', candidate.target_attempt,
        'trace_id', candidate.trace_id,
        'enqueued_at', candidate.enqueued_at
    ),
    NULL,
    candidate.enqueued_at
) AS sent(msg_id);

DO $assert_sends$
BEGIN
    IF (SELECT COUNT(*) FROM recovery_sent)
       <> (SELECT COUNT(*) FROM recovery_candidates) THEN
        RAISE EXCEPTION 'candidate/PGMQ send count mismatch: % versus %',
            (SELECT COUNT(*) FROM recovery_candidates),
            (SELECT COUNT(*) FROM recovery_sent);
    END IF;
END
$assert_sends$;

INSERT INTO pipeline_manual_requeue_audit (
    recovery_id,
    task_id,
    task_version,
    source_stage,
    source_attempt,
    source_status,
    source_finished_at,
    source_worker_id,
    source_error_md5,
    classification,
    target_stage,
    target_queue,
    target_attempt,
    event_id,
    pgmq_msg_id,
    enqueued_at
)
SELECT
    '20260802_swr_push_requeue_v1',
    sent.task_id,
    sent.task_version,
    'push',
    sent.source_attempt,
    sent.source_status,
    sent.source_finished_at,
    sent.source_worker_id,
    sent.source_error_md5,
    sent.classification,
    sent.target_stage,
    sent.target_queue,
    sent.target_attempt,
    sent.event_id,
    sent.pgmq_msg_id,
    sent.enqueued_at
FROM recovery_sent AS sent;

COMMIT;
