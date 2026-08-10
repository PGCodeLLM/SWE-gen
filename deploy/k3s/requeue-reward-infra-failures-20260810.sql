\set ON_ERROR_STOP on

-- Requeue Reward tasks lost to reward-hacking-checker INFRASTRUCTURE errors.
--
-- The reward stage calls an external reward-hacking checker LLM
-- (SWEGEN_REWARD_ENDPOINT, default model gpt-5.6-sol). When that endpoint is
-- unreachable, unauthenticated, proxied badly, or 5xx-ing, the checker never
-- returns a verdict and `reward_action` raises
--     "reward-hacking checker infrastructure error (model=...): <transport error>"
-- The task is then marked failed even though its test bundle was never actually
-- evaluated. Those tasks are recoverable: retrying once the endpoint is healthy
-- produces a real verdict.
--
-- Observed signatures in this class (all transport/auth, never a verdict):
--   HTTP 502 / 503 / 500  -- nginx in front of the checker, or the backend
--   HTTP 401 / 403        -- checker rejected the API key
--   ProxyError            -- corporate proxy refused/failed the tunnel
--   ReadError             -- connection cut mid-response
--   ConnectError          -- could not establish a connection
--   timeout / timed out   -- no response within the client deadline
--
-- Deliberately NOT requeued:
--   * status = 'rejected' -- the checker RAN and returned is_hacking=true. That
--     is a genuine reward-hacking verdict, not an infra fault. Retrying would
--     reproduce the same verdict and burn deliveries.
--   * Any reward failure whose error is NOT the infrastructure-error marker
--     (e.g. "reward test bundle is empty or unreadable" is a task defect, and
--     a missing SWEGEN_REWARD_API_KEY is a config fault to fix, not retry).
--   * Tasks ALREADY PUSHED TO SWR. A pushed task has shipped downstream;
--     re-running its reward gate could contradict an artifact already published
--     and would waste the delivery. Push evidence is taken from BOTH the
--     authoritative push stage results AND the pushed_images ledger
--     (pushed IS TRUE), because the two sets overlap but are not identical.
--
-- Unbounded in time on purpose ("ever"): this class of failure has no other
-- recovery path -- unlike generate/validate/push, the reward stage has no
-- requeue tooling, so these have been accumulating terminally.
--
-- Idempotent: the audit table's primary key plus the live-queue check make a
-- second run a no-op for anything already requeued.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_reward_requeue_audit (
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

LOCK TABLE pgmq.q_swegen_reward IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE reward_infra_requeue_candidates ON COMMIT DROP AS
WITH latest_reward AS (
    -- Only the most recent reward attempt per task decides eligibility.
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
    WHERE result.stage = 'reward'
      AND task.state = 'failed'
      AND task.current_stage = 'reward'
    ORDER BY result.task_id, result.task_version,
             result.attempt DESC, result.finished_at DESC
), classified AS (
    SELECT
        latest.*,
        CASE
            WHEN error ILIKE '%HTTP 502%' THEN 'http_502'
            WHEN error ILIKE '%HTTP 503%' THEN 'http_503'
            WHEN error ILIKE '%HTTP 500%' THEN 'http_500'
            WHEN error ILIKE '%HTTP 401%' THEN 'http_401_unauthorized'
            WHEN error ILIKE '%HTTP 403%' THEN 'http_403_forbidden'
            WHEN error ILIKE '%ProxyError%' THEN 'proxy_error'
            WHEN error ILIKE '%ReadError%' THEN 'read_error'
            WHEN error ILIKE '%ConnectError%' THEN 'connect_error'
            WHEN error ILIKE '%timeout%' OR error ILIKE '%timed out%'
                THEN 'timeout'
            ELSE 'checker_infrastructure'
        END AS failure_category
    FROM latest_reward AS latest
    WHERE status = 'failed'
      -- The marker that the checker itself could not be reached. A 'rejected'
      -- status never reaches here (filtered above), so a real reward-hacking
      -- verdict is never requeued.
      AND error ILIKE '%reward-hacking checker infrastructure error%'
)
SELECT
    classified.*,
    classified.source_stage_attempt + 1 AS new_stage_attempt,
    gen_random_uuid() AS new_event_id,
    clock_timestamp() AS requeued_at
FROM classified
-- Never double-enqueue something already sitting in the live reward queue.
WHERE NOT EXISTS (
    SELECT 1
    FROM pgmq.q_swegen_reward AS live
    WHERE live.message->>'task_id' = classified.task_id
      AND (live.message->>'task_version')::INTEGER = classified.task_version
)
-- Idempotency against previous runs of this script.
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_reward_requeue_audit AS audit
    WHERE audit.task_id = classified.task_id
      AND audit.task_version = classified.task_version
      AND audit.source_stage_attempt = classified.source_stage_attempt
)
-- SAFETY: skip anything already pushed to SWR (authoritative push stage).
AND NOT EXISTS (
    SELECT 1
    FROM pipeline_stage_results AS pushed
    WHERE pushed.task_id = classified.task_id
      AND pushed.stage = 'push'
      AND pushed.status = 'succeeded'
)
-- SAFETY: and per the pushed_images ledger, which covers pushes recorded
-- outside this pipeline's push stage. Only pushed IS TRUE counts; the table
-- also holds not-yet-pushed rows.
AND NOT EXISTS (
    SELECT 1
    FROM pushed_images AS ledger
    WHERE ledger.instance = classified.task_id
      AND ledger.pushed IS TRUE
);

ALTER TABLE reward_infra_requeue_candidates
    ADD COLUMN target_msg_id BIGINT;

CREATE TEMP TABLE reward_infra_requeue_sends ON COMMIT DROP AS
SELECT candidate.task_id, candidate.task_version, sent.target_msg_id
FROM reward_infra_requeue_candidates AS candidate
CROSS JOIN LATERAL pgmq.send(
    'swegen_reward',
    jsonb_build_object(
        'schema_version', 1,
        'event_id', candidate.new_event_id,
        'task_id', candidate.task_id,
        'task_version', candidate.task_version,
        'stage', 'reward',
        'attempt', candidate.new_stage_attempt,
        'trace_id', candidate.trace_id,
        'enqueued_at', candidate.requeued_at
    ),
    0
) AS sent(target_msg_id);

UPDATE reward_infra_requeue_candidates AS candidate
SET target_msg_id = sent.target_msg_id
FROM reward_infra_requeue_sends AS sent
WHERE sent.task_id = candidate.task_id
  AND sent.task_version = candidate.task_version;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM reward_infra_requeue_candidates
        WHERE target_msg_id IS NULL OR target_msg_id <= 0
    ) THEN
        RAISE EXCEPTION 'PGMQ did not confirm every Reward requeue send';
    END IF;
END
$guard$;

UPDATE pipeline_tasks AS task
SET state = 'queued',
    current_stage = 'reward',
    updated_at = candidate.requeued_at,
    finished_at = NULL,
    last_error = NULL,
    last_reason = NULL
FROM reward_infra_requeue_candidates AS candidate
WHERE task.task_id = candidate.task_id
  AND task.task_version = candidate.task_version;

INSERT INTO pipeline_reward_requeue_audit (
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
    'Reward-hacking checker infrastructure failure; checker never returned a verdict',
    'swegen_reward',
    target_msg_id,
    new_stage_attempt,
    new_event_id,
    requeued_at
FROM reward_infra_requeue_candidates;

COMMIT;

SELECT failure_category,
       count(*) AS requeued,
       min(target_msg_id) AS first_msg_id,
       max(target_msg_id) AS last_msg_id
FROM pipeline_reward_requeue_audit
WHERE reason =
    'Reward-hacking checker infrastructure failure; checker never returned a verdict'
GROUP BY failure_category
ORDER BY requeued DESC, failure_category;
