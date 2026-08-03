-- Idempotent live migration for Reward rejection repair and revalidation.
-- Run while connected to the swegen_distributed database.
BEGIN;

ALTER TABLE public.pipeline_tasks
    DROP CONSTRAINT IF EXISTS ck_pipeline_tasks_current_stage;
ALTER TABLE public.pipeline_tasks
    ADD CONSTRAINT ck_pipeline_tasks_current_stage CHECK (
        current_stage IN (
            'generate', 'validate', 'repair', 'reward', 'reward_repair', 'push'
        )
    );

ALTER TABLE public.pipeline_stage_results
    DROP CONSTRAINT IF EXISTS ck_pipeline_stage_results_stage;
ALTER TABLE public.pipeline_stage_results
    ADD CONSTRAINT ck_pipeline_stage_results_stage CHECK (
        stage IN ('generate', 'validate', 'repair', 'reward', 'reward_repair', 'push')
    );

ALTER TABLE public.pipeline_stage_activity
    DROP CONSTRAINT IF EXISTS ck_pipeline_stage_activity_stage;
ALTER TABLE public.pipeline_stage_activity
    ADD CONSTRAINT ck_pipeline_stage_activity_stage CHECK (
        stage IN ('generate', 'validate', 'repair', 'reward', 'reward_repair', 'push')
    );

CREATE INDEX IF NOT EXISTS idx_pipeline_tasks_reward_repair_candidates
    ON public.pipeline_tasks (updated_at, task_id, task_version)
    WHERE (state = 'rejected' AND current_stage = 'reward')
       OR (state = 'failed' AND current_stage = 'reward_repair');

SELECT pgmq.create('swegen_reward_repair');

COMMIT;
