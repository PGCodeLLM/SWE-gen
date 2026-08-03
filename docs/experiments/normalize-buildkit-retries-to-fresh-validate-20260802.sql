-- Correct the audited infrastructure retries that the cluster's compatibility
-- pgmq.send(text,jsonb,integer) wrapper redirected to Validate Repaired solely
-- because their Validate attempt is greater than one. Move visible messages
-- only; in-flight claims remain valid and continue on their original queue.

BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '10min';
SELECT pg_advisory_xact_lock(hashtext('swegen-manual-requeue'), hashtext('20260802-v1'));

CREATE TEMP TABLE recovery_visible_repaired ON COMMIT DROP AS
SELECT
    audit.task_id,
    audit.task_version,
    audit.event_id,
    repaired.msg_id AS old_msg_id,
    repaired.message,
    repaired.enqueued_at
FROM pipeline_manual_requeue_audit AS audit
JOIN pgmq.q_swegen_validate_repaired AS repaired
  ON repaired.msg_id = audit.pgmq_msg_id
 AND repaired.message ->> 'event_id' = audit.event_id::TEXT
WHERE audit.recovery_id = '20260802_buildkit_oracle_requeue_v1'
  AND audit.classification = 'validate_infra'
  AND audit.target_queue = 'swegen_validate'
  AND repaired.vt <= clock_timestamp()
FOR UPDATE OF repaired SKIP LOCKED;

CREATE TEMP TABLE recovery_fresh_sent ON COMMIT DROP AS
SELECT
    visible.*,
    sent.msg_id AS new_msg_id
FROM recovery_visible_repaired AS visible
CROSS JOIN LATERAL pgmq.send(
    'swegen_validate',
    visible.message,
    NULL,
    clock_timestamp()
) AS sent(msg_id);

DO $archive_originals$
DECLARE
    moved record;
    archived boolean;
BEGIN
    FOR moved IN SELECT * FROM recovery_fresh_sent ORDER BY old_msg_id
    LOOP
        SELECT pgmq.archive('swegen_validate_repaired', moved.old_msg_id)
        INTO archived;
        IF archived IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'could not archive repaired Validate message %', moved.old_msg_id;
        END IF;
    END LOOP;
END
$archive_originals$;

UPDATE pipeline_manual_requeue_audit AS audit
SET pgmq_msg_id = moved.new_msg_id
FROM recovery_fresh_sent AS moved
WHERE audit.recovery_id = '20260802_buildkit_oracle_requeue_v1'
  AND audit.task_id = moved.task_id
  AND audit.task_version = moved.task_version;

-- Preserve the actual queue identity for claims that were already in flight
-- and therefore intentionally were not moved.
UPDATE pipeline_manual_requeue_audit AS audit
SET target_queue = 'swegen_validate_repaired'
WHERE audit.recovery_id = '20260802_buildkit_oracle_requeue_v1'
  AND audit.classification = 'validate_infra'
  AND audit.target_queue = 'swegen_validate'
  AND (
      EXISTS (
          SELECT 1
          FROM pgmq.q_swegen_validate_repaired AS live
          WHERE live.msg_id = audit.pgmq_msg_id
            AND live.message ->> 'event_id' = audit.event_id::TEXT
      )
      OR EXISTS (
          SELECT 1
          FROM pgmq.a_swegen_validate_repaired AS archived
          WHERE archived.msg_id = audit.pgmq_msg_id
            AND archived.message ->> 'event_id' = audit.event_id::TEXT
      )
  );

COMMIT;
