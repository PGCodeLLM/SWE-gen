BEGIN;

CREATE TABLE IF NOT EXISTS public.buildkit_intermediates (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repo                TEXT NOT NULL,
    dependency_key      TEXT NOT NULL,
    build_key           TEXT NOT NULL,
    commit_sha          TEXT,
    lockfile_path       TEXT,
    lockfile_sha256     TEXT,
    status              TEXT NOT NULL,
    claim_token         UUID,
    claim_owner         TEXT,
    image_ref           TEXT,
    image_digest        TEXT,
    source_task_id      TEXT,
    source_task_version INTEGER,
    build_seconds       DOUBLE PRECISION,
    cold_build_seconds  DOUBLE PRECISION NOT NULL,
    dockerfile_sha256   TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    error               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    ready_at            TIMESTAMPTZ,
    last_used_at        TIMESTAMPTZ,
    CONSTRAINT uq_buildkit_intermediates_identity
        UNIQUE (repo, dependency_key, build_key),
    CONSTRAINT ck_buildkit_intermediates_repo CHECK (
        repo = lower(repo) AND repo ~ '^[^/[:space:]]+/[^/[:space:]]+$'
    ),
    CONSTRAINT ck_buildkit_intermediates_dependency_key CHECK (
        dependency_key ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_buildkit_intermediates_build_key CHECK (
        build_key ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_buildkit_intermediates_lockfile_digest CHECK (
        lockfile_sha256 IS NULL OR lockfile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_buildkit_intermediates_dockerfile_digest CHECK (
        dockerfile_sha256 IS NULL OR dockerfile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_buildkit_intermediates_image_digest CHECK (
        image_digest IS NULL OR image_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_buildkit_intermediates_status CHECK (
        status IN ('building', 'ready', 'failed', 'retired')
    ),
    CONSTRAINT ck_buildkit_intermediates_cold_threshold CHECK (
        cold_build_seconds > 600
    ),
    CONSTRAINT ck_buildkit_intermediates_build_seconds CHECK (
        build_seconds IS NULL OR build_seconds > 0
    ),
    CONSTRAINT ck_buildkit_intermediates_source_version CHECK (
        source_task_version IS NULL OR source_task_version > 0
    ),
    CONSTRAINT ck_buildkit_intermediates_claim CHECK (
        (status = 'building' AND claim_token IS NOT NULL AND claim_owner IS NOT NULL)
        OR status <> 'building'
    ),
    CONSTRAINT ck_buildkit_intermediates_ready CHECK (
        (status = 'ready' AND image_ref IS NOT NULL AND image_digest IS NOT NULL
         AND ready_at IS NOT NULL)
        OR status <> 'ready'
    )
);

CREATE INDEX IF NOT EXISTS idx_buildkit_intermediates_repo_ready
    ON public.buildkit_intermediates (repo, dependency_key, updated_at DESC)
    WHERE status = 'ready';
CREATE INDEX IF NOT EXISTS idx_buildkit_intermediates_building
    ON public.buildkit_intermediates (updated_at)
    WHERE status = 'building';

-- Historical dependency bases produced by the controlled 2026-08-02 farm
-- timeout experiments.  All six manifests were verified in SWR before this
-- migration.  Superseded variants remain visible because they are useful
-- audit/reuse inputs; authoritative_validation identifies the variants that
-- themselves completed NOP=0 and Oracle=1 on the source task.
INSERT INTO public.buildkit_intermediates (
    repo, dependency_key, build_key, commit_sha, lockfile_path,
    lockfile_sha256, status, image_ref, image_digest, source_task_id,
    source_task_version, build_seconds, cold_build_seconds,
    dockerfile_sha256, metadata, ready_at, last_used_at
)
VALUES
(
    'runbox/runbox7',
    'ae4449b8e848fd82e86577486226160baff97bd9040f0ffc0d25fdac2c5aa78d',
    'd261a89424dce8b6b476ad06fb17d666168a18a7f97cb1013950fd94a71c45f3',
    'ca6df8e5620e38a24054ca20504eb024c146489b', 'package-lock.json',
    'ae4449b8e848fd82e86577486226160baff97bd9040f0ffc0d25fdac2c5aa78d',
    'ready',
    'swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-runbox7-ca6df8e5-ae4449b8e848',
    'sha256:939ac290b161d0084a74e615ff7a6ade6ae4dcfa3d189fe703409c5cce636668',
    'runbox__runbox7-833', 1, NULL, 637.0, NULL,
    '{"authoritative_validation":false,"kind":"dependency-fetch","seeded_from":"docs/verifier_yield_improvement.md","superseded_by":"dep-runbox7-ca6df8e5-ae4449b8e848-ngcc"}'::jsonb,
    now(), now()
),
(
    'runbox/runbox7',
    'ae4449b8e848fd82e86577486226160baff97bd9040f0ffc0d25fdac2c5aa78d',
    '082e3e9b32b995aff0faf9d37d8cc1303a1284629d6c7b7e63a7684ce9a10c8d',
    'ca6df8e5620e38a24054ca20504eb024c146489b', 'package-lock.json',
    'ae4449b8e848fd82e86577486226160baff97bd9040f0ffc0d25fdac2c5aa78d',
    'ready',
    'swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-runbox7-ca6df8e5-ae4449b8e848-ngcc',
    'sha256:b57bd6c68c82e3009b20d3289e19136f58cf442c71140f27766037d82ef140f5',
    'runbox__runbox7-833', 1, 13.2, 637.0,
    '082e3e9b32b995aff0faf9d37d8cc1303a1284629d6c7b7e63a7684ce9a10c8d',
    '{"authoritative_validation":false,"kind":"dependency-precompile","seeded_from":"docs/verifier_yield_improvement.md","superseded_by":"dep-runbox7-ca6df8e5-ae4449b8e848-ngcc-w1"}'::jsonb,
    now(), now()
),
(
    'runbox/runbox7',
    'ae4449b8e848fd82e86577486226160baff97bd9040f0ffc0d25fdac2c5aa78d',
    'b0cbc7dbb19c30c67640636108520669542bf3ca34227b6fa71758081ea090ec',
    'ca6df8e5620e38a24054ca20504eb024c146489b', 'package-lock.json',
    'ae4449b8e848fd82e86577486226160baff97bd9040f0ffc0d25fdac2c5aa78d',
    'ready',
    'swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-runbox7-ca6df8e5-ae4449b8e848-ngcc-w1',
    'sha256:d1b1c1f057e75cada23335cb693d87c326b982a66ccee3c6fb1b58acf10254de',
    'runbox__runbox7-833', 1, 0.9, 637.0,
    'b0cbc7dbb19c30c67640636108520669542bf3ca34227b6fa71758081ea090ec',
    '{"authoritative_validation":true,"kind":"dependency-precompile","nop_reward":0,"oracle_reward":1,"seeded_from":"docs/verifier_yield_improvement.md"}'::jsonb,
    now(), now()
),
(
    'revault/revault-gui',
    '2fc887c39ebedcd529909c03c17d0a6643e9958ed038573fc0972a0cf870d2a3',
    '19fefb868f7433db3f21fd4779e79c99d9d5c2443d389518360a84843d4dbc99',
    'b3ff45888fdc874993c120dccec0ddb47491d4ca', 'Cargo.lock',
    '2fc887c39ebedcd529909c03c17d0a6643e9958ed038573fc0972a0cf870d2a3',
    'ready',
    'swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-revault-gui-b3ff4588-2fc887c39ebe',
    'sha256:6ad85b1436221344428abd8ded5ec31cc719c32672dad8587c78860e49b0a8e6',
    'revault__revault-gui-218', 1, NULL, 600.001,
    '19fefb868f7433db3f21fd4779e79c99d9d5c2443d389518360a84843d4dbc99',
    '{"authoritative_validation":false,"kind":"dependency-fetch","seeded_from":"docs/verifier_yield_improvement.md"}'::jsonb,
    now(), now()
),
(
    'revault/revault-gui',
    '2fc887c39ebedcd529909c03c17d0a6643e9958ed038573fc0972a0cf870d2a3',
    '67536c87cdef4aab9194e07b944fcfa263e4e31ebca4c6413589b4d50572fb48',
    'b3ff45888fdc874993c120dccec0ddb47491d4ca', 'Cargo.lock',
    '2fc887c39ebedcd529909c03c17d0a6643e9958ed038573fc0972a0cf870d2a3',
    'ready',
    'swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-revault-gui-b3ff4588-2fc887c39ebe-testbuild-j1',
    'sha256:97413163a12e66f449c7fd7589d4db59563958b165f032a0b0f3e87880e83231',
    'revault__revault-gui-218', 1, 723.6, 600.001,
    '67536c87cdef4aab9194e07b944fcfa263e4e31ebca4c6413589b4d50572fb48',
    '{"authoritative_validation":false,"kind":"dependency-precompile","nop_reward":1,"oracle_reward":1,"seeded_from":"docs/verifier_yield_improvement.md","warning":"source task had invalid NOP semantics"}'::jsonb,
    now(), now()
),
(
    'jix/varisat',
    '6879d03a5a4bae9764a925e129f13eae6b0b47434f2fe89d6bd49b76fe9aff62',
    'd24ce4c7048e66c6a1ea7a0703b83ed1026428f0155840a0c3b3769fbfa6fe0d',
    'b92d6e87b775ba7d9df20e0f8dfbb8c116dd8d86', 'Cargo.lock',
    '6879d03a5a4bae9764a925e129f13eae6b0b47434f2fe89d6bd49b76fe9aff62',
    'ready',
    'swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox/public/swe-gen/feature-implementation/generated:dep-varisat-b92d6e87-6879d03a5a4b-testbuild-j1',
    'sha256:e970cd239ab67dad304b157ec37aa9084d01a95ce868b7677de89154324f3604',
    'jix__varisat-55', 1, 3077.0, 2077.9,
    'd24ce4c7048e66c6a1ea7a0703b83ed1026428f0155840a0c3b3769fbfa6fe0d',
    '{"authoritative_validation":true,"kind":"dependency-precompile","nop_reward":0,"oracle_reward":1,"seeded_from":"docs/verifier_yield_improvement.md"}'::jsonb,
    now(), now()
)
ON CONFLICT (repo, dependency_key, build_key) DO UPDATE SET
    commit_sha = EXCLUDED.commit_sha,
    lockfile_path = EXCLUDED.lockfile_path,
    lockfile_sha256 = EXCLUDED.lockfile_sha256,
    status = EXCLUDED.status,
    claim_token = NULL,
    claim_owner = NULL,
    image_ref = EXCLUDED.image_ref,
    image_digest = EXCLUDED.image_digest,
    source_task_id = EXCLUDED.source_task_id,
    source_task_version = EXCLUDED.source_task_version,
    build_seconds = EXCLUDED.build_seconds,
    cold_build_seconds = EXCLUDED.cold_build_seconds,
    dockerfile_sha256 = EXCLUDED.dockerfile_sha256,
    metadata = public.buildkit_intermediates.metadata || EXCLUDED.metadata,
    error = NULL,
    ready_at = COALESCE(public.buildkit_intermediates.ready_at, now()),
    updated_at = now()
WHERE public.buildkit_intermediates.status <> 'building';

COMMIT;
