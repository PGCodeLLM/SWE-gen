\set ON_ERROR_STOP on

-- Validate work lost to /data disk saturation on 2026-08-19.
--
-- Generate was scaled to 2,000 pods across two model pools. Generate and
-- Validate share /dev/vdb1 (it carries /data/docker and /data/containerd), and
-- at that pod count the volume ran at 91-153% utilisation with 45-63ms write
-- latency. Validate's whole job is `docker compose` builds against that disk,
-- so its builds started failing fast instead of running: from 08:00 the stage
-- went from ~40% success to ~11%, infrastructure failures went 29% -> 81%, and
-- mean duration halved (1096s -> 546s). The tell that this is contention and
-- not task quality is that the *semantic* rejection rate fell at the same time
-- (30% -> 4%) -- tasks were dying before they could be judged at all.
--
-- Validate has since been scaled to zero so the API-bound Generate stage has
-- the disk to itself. This requeues the Validate tasks that failed inside the
-- saturation window with a disk-contention signature, so they are re-processed
-- once Validate runs again.
--
-- Scope: stage='validate', status='failed', finished_at >= 08:00 (+08), and a
-- Docker-compose or Harbor-timeout error. 820 rows, mean duration 224s -- the
-- fast-fail shape of a build that could not get disk, against the 721s mean of
-- the pre-saturation failures.
--
-- Deliberately left alone: the 1,501 Docker-compose/timeout failures from
-- 21:00-08:00, before the saturation. Those ran 721s on average and a prior
-- investigation found most Docker-compose failures at this stage are repos
-- whose own Dockerfile cannot build (cargo/go/yarn errors in the repo itself).
-- Requeueing those would burn a build slot each to reproduce the same error.
-- They need a diagnosis -- or a blocklist -- not a retry.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_validate_requeue_disk_saturation_20260819_audit (
    task_id               TEXT        NOT NULL,
    task_version          INTEGER     NOT NULL,
    source_stage_attempt  INTEGER     NOT NULL,
    source_stage_status   TEXT        NOT NULL,
    source_finished_at    TIMESTAMPTZ NOT NULL,
    source_error          TEXT,
    target_queue          TEXT        NOT NULL,
    target_msg_id         BIGINT      NOT NULL,
    new_stage_attempt     INTEGER     NOT NULL,
    new_event_id          UUID        NOT NULL,
    reason                TEXT        NOT NULL,
    requeued_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, task_version, source_stage_attempt),
    UNIQUE (target_queue, target_msg_id)
);

-- Validate is held at zero while this transaction runs. This lock prevents a
-- producer from inserting a duplicate between the live-message check and send.
LOCK TABLE pgmq.q_swegen_validate IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE validate_saturation_requeue_candidates ON COMMIT DROP AS
WITH latest_validate AS (
    SELECT
        task.task_id,
        task.task_version,
        task.trace_id,
        latest.attempt AS source_stage_attempt,
        latest.status AS source_stage_status,
        latest.finished_at AS source_finished_at,
        latest.error AS source_error
    FROM pipeline_tasks AS task
    JOIN LATERAL (
        SELECT result.attempt, result.status, result.finished_at, result.error
        FROM pipeline_stage_results AS result
        WHERE result.task_id = task.task_id
          AND result.task_version = task.task_version
          AND result.stage = 'validate'
        ORDER BY result.attempt DESC, result.finished_at DESC
        LIMIT 1
    ) AS latest ON true
    WHERE task.state = 'failed'
      AND task.current_stage = 'validate'
)
SELECT
    latest_validate.*,
    latest_validate.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM latest_validate
-- Only the saturation window, and only the disk-contention error signature.
WHERE latest_validate.source_finished_at >= TIMESTAMPTZ '2026-08-19 08:00:00+08'
  AND (
        latest_validate.source_error LIKE '%Docker compose command failed%'
     OR latest_validate.source_error LIKE '%timed out after%'
  )
AND NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_validate AS live
    WHERE live.message->>'task_id' = latest_validate.task_id
      AND (live.message->>'task_version')::INTEGER = latest_validate.task_version
)
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_validate_requeue_disk_saturation_20260819_audit AS audit
    WHERE audit.task_id = latest_validate.task_id
      AND audit.task_version = latest_validate.task_version
      AND audit.source_stage_attempt = latest_validate.source_stage_attempt
);

ALTER TABLE validate_saturation_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE validate_saturation_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM validate_saturation_requeue_candidates AS candidate
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

UPDATE validate_saturation_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM validate_saturation_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM validate_saturation_requeue_candidates
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
FROM validate_saturation_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_validate_requeue_disk_saturation_20260819_audit (
    task_id, task_version, source_stage_attempt, source_stage_status,
    source_finished_at, source_error, target_queue, target_msg_id,
    new_stage_attempt, new_event_id, reason, requeued_at
)
SELECT
    task_id,
    task_version,
    source_stage_attempt,
    source_stage_status,
    source_finished_at,
    source_error,
    'swegen_validate',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    'validate lost to /data disk saturation 20260819 while generate ran at 2000 pods',
    requeued_at
FROM validate_saturation_requeue_candidates;

COMMIT;

SELECT count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id,
       min(source_finished_at) AS earliest_source_failure,
       max(requeued_at) AS last_requeued_at
FROM pipeline_validate_requeue_disk_saturation_20260819_audit
WHERE reason = 'validate lost to /data disk saturation 20260819 while generate ran at 2000 pods';
