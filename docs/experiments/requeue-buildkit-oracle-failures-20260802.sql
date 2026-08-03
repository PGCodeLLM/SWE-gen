-- Transactional recovery of latest authoritative production Validate failures.
--
-- Recovery ID: 20260802_buildkit_oracle_requeue_v1
-- Infrastructure/BuildKit/Docker failures go to the live normal Validate FIFO.
-- Oracle reward, Oracle artifact, and patch application failures go to Repair.
-- Unexpected NOP rewards, experiment/canary results, ambiguous errors, active
-- claims, existing queue payloads, advanced tasks, and later successes are excluded.

BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '10min';
SELECT pg_advisory_xact_lock(hashtext('swegen-manual-requeue'), hashtext('20260802-v1'));

CREATE TABLE IF NOT EXISTS pipeline_manual_requeue_audit (
    recovery_id       TEXT        NOT NULL,
    task_id           TEXT        NOT NULL,
    task_version      INTEGER     NOT NULL,
    source_stage      TEXT        NOT NULL,
    source_attempt    INTEGER     NOT NULL,
    source_status     TEXT        NOT NULL,
    source_finished_at TIMESTAMPTZ NOT NULL,
    source_worker_id  TEXT        NOT NULL,
    source_error_md5  TEXT        NOT NULL,
    classification   TEXT        NOT NULL,
    target_stage      TEXT        NOT NULL,
    target_queue      TEXT        NOT NULL,
    target_attempt    INTEGER     NOT NULL,
    event_id          UUID        NOT NULL,
    pgmq_msg_id       BIGINT      NOT NULL,
    enqueued_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (recovery_id, task_id, task_version),
    UNIQUE (target_queue, pgmq_msg_id)
);

CREATE TEMP TABLE recovery_candidates ON COMMIT DROP AS
WITH latest_validate AS (
    SELECT DISTINCT ON (result.task_id, result.task_version)
        result.*
    FROM pipeline_stage_results AS result
    WHERE result.stage = 'validate'
    ORDER BY result.task_id, result.task_version, result.attempt DESC
), classified AS (
    SELECT
        latest.task_id,
        latest.task_version,
        task.trace_id,
        latest.attempt AS source_attempt,
        latest.status AS source_status,
        latest.finished_at AS source_finished_at,
        latest.worker_id AS source_worker_id,
        md5(COALESCE(latest.error, '')) AS source_error_md5,
        CASE
            WHEN latest.worker_id ~* '(canary|manual|experiment|audit)'
                THEN 'exclude_experiment'
            WHEN latest.status = 'succeeded'
              OR task.state NOT IN ('failed', 'rejected')
              OR task.current_stage <> 'validate'
                THEN 'exclude_state'
            WHEN latest.result ->> 'reason' = 'unexpected_nop_reward'
                THEN 'leave_unexpected_nop'
            WHEN latest.result ->> 'reason' = 'unexpected_oracle_reward'
                THEN 'repair_oracle_reward'
            WHEN COALESCE(latest.error, '') ~* (
                '(fix[.]patch|bug[.]patch|patch -p[0-9]|patch application|'
                'malformed patch|reversed [(]or previously applied[)] patch|'
                'saving rejects to file|hunk .* failed|does not apply|'
                'can.t find file to patch|invalid patch|missing .*patch|'
                'no such file[^\n]*patch)'
            )
                THEN 'repair_patch_artifact'
            WHEN latest.status = 'failed'
             AND COALESCE(latest.error, '') ~* (
                '(timed out|timeout|context deadline exceeded|deadline exceeded|'
                'remote buildkit|build request|build submission|build timed out after 600|'
                'docker compose command failed|failed to solve|did not complete successfully|'
                'cannot connect to the docker daemon|docker daemon|no space left on device|'
                'input/output error|connection (timed out|reset|refused)|recv failure|'
                'tls handshake timeout|proxyconnect|unexpected eof|early eof|rpc failed|'
                'temporary failure|could not resolve|name or service not known|'
                'network is unreachable|failed to do request|dial tcp|i/o timeout|'
                '504 gateway|gateway timeout|service unavailable|too many requests|'
                'failed to fetch anonymous token|failed to authorize)'
             )
                THEN 'validate_infra'
            WHEN latest.status = 'failed'
             AND COALESCE(latest.error, '') ~* 'Harbor oracle'
                THEN 'repair_oracle_artifact'
            ELSE 'exclude_ambiguous'
        END AS classification
    FROM latest_validate AS latest
    JOIN pipeline_tasks AS task
      USING (task_id, task_version)
), eligible AS (
    SELECT
        classified.*,
        CASE
            WHEN classification = 'validate_infra' THEN 'validate'
            ELSE 'repair'
        END AS target_stage,
        CASE
            WHEN classification = 'validate_infra' THEN 'swegen_validate'
            ELSE 'swegen_repair'
        END AS target_queue
    FROM classified
    WHERE classification IN (
        'validate_infra',
        'repair_oracle_reward',
        'repair_patch_artifact',
        'repair_oracle_artifact'
    )
)
SELECT
    eligible.*,
    COALESCE((
        SELECT MAX(previous.attempt)
        FROM pipeline_stage_results AS previous
        WHERE previous.task_id = eligible.task_id
          AND previous.task_version = eligible.task_version
          AND previous.stage = eligible.target_stage
    ), 0) + 1 AS target_attempt,
    gen_random_uuid() AS event_id,
    clock_timestamp() AS enqueued_at
FROM eligible
WHERE NOT EXISTS (
        SELECT 1
        FROM pipeline_stage_activity AS activity
        WHERE activity.task_id = eligible.task_id
          AND activity.task_version = eligible.task_version
          AND activity.heartbeat_at > now() - INTERVAL '5 minutes'
    )
  AND NOT EXISTS (
        SELECT 1
        FROM (
            SELECT message FROM pgmq.q_swegen_validate
            UNION ALL
            SELECT message FROM pgmq.q_swegen_validate_repaired
            UNION ALL
            SELECT message FROM pgmq.q_swegen_repair
        ) AS queued
        WHERE queued.message ->> 'task_id' = eligible.task_id
          AND (queued.message ->> 'task_version')::INTEGER = eligible.task_version
    )
  AND NOT EXISTS (
        SELECT 1
        FROM pipeline_stage_results AS later
        WHERE later.task_id = eligible.task_id
          AND later.task_version = eligible.task_version
          AND later.finished_at > eligible.source_finished_at
          AND later.status = 'succeeded'
          AND later.stage IN ('validate', 'repair', 'reward', 'push')
    )
  AND NOT EXISTS (
        SELECT 1
        FROM pipeline_manual_requeue_audit AS prior_audit
        WHERE prior_audit.recovery_id = '20260802_buildkit_oracle_requeue_v1'
          AND prior_audit.task_id = eligible.task_id
          AND prior_audit.task_version = eligible.task_version
    );

-- Lock every selected authoritative task and make the stage transition before
-- publishing payloads. The transaction keeps both changes invisible until commit.
CREATE TEMP TABLE recovery_task_updates ON COMMIT DROP AS
WITH updated AS (
    UPDATE pipeline_tasks AS task
    SET state = 'queued',
        current_stage = candidate.target_stage,
        updated_at = candidate.enqueued_at,
        finished_at = NULL,
        last_error = NULL,
        last_reason = 'manual_requeue:' || candidate.classification
    FROM recovery_candidates AS candidate
    WHERE task.task_id = candidate.task_id
      AND task.task_version = candidate.task_version
      AND task.state IN ('failed', 'rejected')
      AND task.current_stage = 'validate'
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
    '20260802_buildkit_oracle_requeue_v1',
    sent.task_id,
    sent.task_version,
    'validate',
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
