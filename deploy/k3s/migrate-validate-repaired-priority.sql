\set ON_ERROR_STOP on

BEGIN;

SELECT pgmq.create('swegen_validate_repaired');

-- Compatibility routing for Repair workers running an older image. Initial
-- Generate -> Validate handoffs always use attempt 1; every later Validate
-- attempt is emitted after Repair and belongs in the priority FIFO.
CREATE OR REPLACE FUNCTION pgmq.send(queue_name text, msg jsonb, delay integer)
RETURNS SETOF bigint
LANGUAGE sql
AS $function$
    SELECT *
    FROM pgmq.send(
        CASE
            WHEN queue_name = 'swegen_validate'
             AND msg->>'stage' = 'validate'
             AND COALESCE(msg->>'attempt', '') ~ '^[0-9]+$'
             AND (msg->>'attempt')::integer > 1
                THEN 'swegen_validate_repaired'
            ELSE queue_name
        END,
        msg,
        NULL,
        clock_timestamp() + make_interval(secs => delay)
    );
$function$;

-- Move currently visible Repair -> Validate handoffs without stealing an
-- in-flight claim from an old validator. Re-running the migration is safe.
DO $migration$
DECLARE
    candidate record;
    new_msg_id bigint;
    archived boolean;
    moved integer := 0;
BEGIN
    FOR candidate IN
        SELECT msg_id, message
        FROM pgmq.q_swegen_validate
        WHERE vt <= clock_timestamp()
          AND message->>'stage' = 'validate'
          AND COALESCE(message->>'attempt', '') ~ '^[0-9]+$'
          AND (message->>'attempt')::integer > 1
        ORDER BY msg_id
        FOR UPDATE SKIP LOCKED
    LOOP
        SELECT send
        INTO new_msg_id
        FROM pgmq.send('swegen_validate_repaired', candidate.message, 0) AS send;

        IF new_msg_id IS NULL THEN
            RAISE EXCEPTION 'Could not enqueue repaired Validate message %', candidate.msg_id;
        END IF;

        SELECT pgmq.archive('swegen_validate', candidate.msg_id)
        INTO archived;
        IF archived IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'Could not archive original Validate message %', candidate.msg_id;
        END IF;
        moved := moved + 1;
    END LOOP;

    RAISE NOTICE 'Moved % repaired Validate messages into swegen_validate_repaired', moved;
END
$migration$;

COMMIT;
