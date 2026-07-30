-- Run while connected to swegen_distributed after provisioning either the
-- supported PGMQ extension or the complete reviewed SQL-only PGMQ API.
DO $swegen_pgmq$
DECLARE
    installed_version text;
    installed_major integer;
    installed_minor integer;
    sql_only_api_complete boolean;
    metrics_has_visible_length boolean;
BEGIN
    SELECT extversion
    INTO installed_version
    FROM pg_catalog.pg_extension
    WHERE extname = 'pgmq';

    IF installed_version IS NOT NULL THEN
        IF split_part(installed_version, '.', 1) !~ '^[0-9]+$'
           OR split_part(installed_version, '.', 2) !~ '^[0-9]+$' THEN
            RAISE EXCEPTION 'Unrecognized PGMQ extension version: %', installed_version;
        END IF;

        installed_major := split_part(installed_version, '.', 1)::integer;
        installed_minor := split_part(installed_version, '.', 2)::integer;

        IF installed_major <> 1 OR installed_minor < 5 THEN
            RAISE EXCEPTION
                'Unsupported PGMQ version %; this pipeline requires >= 1.5.0 and < 2.0.0',
                installed_version;
        END IF;
    ELSE
        SELECT
            to_regprocedure('pgmq.create(text)') IS NOT NULL
            AND to_regprocedure('pgmq.send(text,jsonb,integer)') IS NOT NULL
            AND to_regprocedure('pgmq.read(text,integer,integer,jsonb)') IS NOT NULL
            AND to_regprocedure('pgmq.read_with_poll(text,integer,integer,integer,integer,jsonb)') IS NOT NULL
            AND to_regprocedure('pgmq.set_vt(text,bigint,integer)') IS NOT NULL
            AND to_regprocedure('pgmq.archive(text,bigint)') IS NOT NULL
            AND to_regprocedure('pgmq.metrics(text)') IS NOT NULL
        INTO sql_only_api_complete;

        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_type AS type_definition
            JOIN pg_catalog.pg_namespace AS type_namespace
              ON type_namespace.oid = type_definition.typnamespace
            JOIN pg_catalog.pg_attribute AS type_attribute
              ON type_attribute.attrelid = type_definition.typrelid
            WHERE type_namespace.nspname = 'pgmq'
              AND type_definition.typname = 'metrics_result'
              AND type_attribute.attname = 'queue_visible_length'
              AND type_attribute.attnum > 0
              AND NOT type_attribute.attisdropped
        )
        INTO metrics_has_visible_length;

        IF NOT sql_only_api_complete OR NOT metrics_has_visible_length THEN
            RAISE EXCEPTION
                'PGMQ is unavailable in database %; provision a compatible extension or the complete SQL-only API',
                current_database();
        END IF;
    END IF;
END
$swegen_pgmq$;

SELECT pgmq.create('swegen_generate');
SELECT pgmq.create('swegen_validate');
SELECT pgmq.create('swegen_repair');
SELECT pgmq.create('swegen_reward');
SELECT pgmq.create('swegen_push');
SELECT pgmq.create('swegen_dead');
