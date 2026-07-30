-- Idempotent live migration for the validation repair relay.
-- Run while connected to the swegen_distributed database.
BEGIN;

ALTER TABLE public.pipeline_tasks
    DROP CONSTRAINT IF EXISTS ck_pipeline_tasks_current_stage;
ALTER TABLE public.pipeline_tasks
    ADD CONSTRAINT ck_pipeline_tasks_current_stage CHECK (
        current_stage IN ('generate', 'validate', 'repair', 'reward', 'push')
    );

ALTER TABLE public.pipeline_stage_results
    DROP CONSTRAINT IF EXISTS ck_pipeline_stage_results_stage;
ALTER TABLE public.pipeline_stage_results
    ADD CONSTRAINT ck_pipeline_stage_results_stage CHECK (
        stage IN ('generate', 'validate', 'repair', 'reward', 'push')
    );

ALTER TABLE public.pipeline_stage_activity
    DROP CONSTRAINT IF EXISTS ck_pipeline_stage_activity_stage;
ALTER TABLE public.pipeline_stage_activity
    ADD CONSTRAINT ck_pipeline_stage_activity_stage CHECK (
        stage IN ('generate', 'validate', 'repair', 'reward', 'push')
    );

CREATE INDEX IF NOT EXISTS idx_pipeline_tasks_repair_candidates
    ON public.pipeline_tasks (updated_at, task_id, task_version)
    WHERE state IN ('failed', 'rejected')
      AND current_stage IN ('validate', 'repair');

SELECT pgmq.create('swegen_repair');

COMMIT;
