\set ON_ERROR_STOP on

-- Requeue Validate tasks lost to BuildKit/Docker and network infrastructure in
-- the 24 hours to 2026-08-14.
--
-- Three infrastructure families, none of which says anything about task
-- validity:
--
--   * harbor_timeout (~2.1k) -- "Harbor nop timed out after 3600 seconds". The
--     dominant cause was node 0002, whose Docker storage corrupted at 18:02 on
--     2026-08-13: `docker system df` returned HTTP 500 ("rw layer snapshot not
--     found"), 549 dead containers accumulated, and builds could not obtain a
--     usable snapshot layer, so they hung until Harbor's ceiling fired. That
--     node produced ONE success in 1,543 non-rejected attempts while healthy
--     peers ran at 9-23%. It has since been drained, cleaned and returned to
--     service.
--   * git_net (~1.2k) -- RPC failed / GnuTLS recv error / early EOF /
--     fetch-pack disconnect while cloning github.com. Transient transport.
--   * buildkit_rst (~23) -- RST_STREAM from the BuildKit farm mid-build.
--
-- Deliberately NOT requeued:
--   * `rejected` tasks. Those ran to completion and reported a wrong reward,
--     a genuine defect that a retry only reproduces.
--   * Dockerfile build failures, missing reward files and patch-apply failures.
--     Those are real per-task defects; a healthy node builds them just the same.
--
-- Idempotent via the audit table PK plus a live-queue check on BOTH validate
-- queues -- pgmq.send is wrapped to route attempt > 1 to
-- swegen_validate_repaired, and most of this batch is attempt >= 2, so checking
-- only the main queue would double-enqueue anything already awaiting a retry.

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

CREATE TEMP TABLE validate_infra_2026_08_14 ON COMMIT DROP AS
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
      AND result.finished_at > now() - interval '24 hours'
      AND task.state = 'failed'
      AND task.current_stage = 'validate'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
), classified AS (
    SELECT
        latest.*,
        CASE
            WHEN error ILIKE '%timed out after%' THEN 'harbor_timeout'
            WHEN error ILIKE '%rst_stream%' OR error ILIKE '%buildkit%'
                THEN 'buildkit_rst_stream'
            ELSE 'git_transport'
        END AS failure_category
    FROM latest_validate AS latest
    WHERE status = 'failed'
      AND (
          error ILIKE '%timed out after%'
          OR error ILIKE '%rst_stream%'
          OR error ILIKE '%buildkit%'
          OR error ILIKE '%Recv failure%'
          OR error ILIKE '%TLS%'
          OR error ILIKE '%early EOF%'
          OR error ILIKE '%RPC failed%'
          OR error ILIKE '%fetch-pack%'
      )
      -- A reward mismatch is a task defect even when the captured log also
      -- mentions a timeout or a transport hiccup.
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

ALTER TABLE validate_infra_2026_08_14 ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE validate_infra_2026_08_14_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM validate_infra_2026_08_14 AS candidate
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

UPDATE validate_infra_2026_08_14 AS candidate
SET target_msg_id = sent.target_msg_id
FROM validate_infra_2026_08_14_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1 FROM validate_infra_2026_08_14
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
FROM validate_infra_2026_08_14 AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_validate_requeue_audit (
    task_id, task_version, source_stage_attempt, source_finished_at,
    failure_category, reason, target_queue, target_msg_id,
    new_stage_attempt, new_event_id, requeued_at
)
SELECT
    task_id, task_version, source_stage_attempt, source_finished_at,
    failure_category,
    'Validate lost to BuildKit/Docker corruption and git transport in the 24h to 2026-08-14',
    'swegen_validate',
    target_msg_id, new_stage_attempt, new_event_id, requeued_at
FROM validate_infra_2026_08_14;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_validate_requeue_audit
WHERE reason =
    'Validate lost to BuildKit/Docker corruption and git transport in the 24h to 2026-08-14'
GROUP BY failure_category
ORDER BY requeued DESC;
