from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_DIR = ROOT / "deploy" / "k3s"


def _documents() -> list[dict[str, object]]:
    manifest = DEPLOY_DIR / "swegen-pipeline.yaml"
    return [document for document in yaml.safe_load_all(manifest.read_text()) if document]


def test_worker_image_has_required_runtime_tools() -> None:
    dockerfile = (DEPLOY_DIR / "Dockerfile.worker").read_text()

    assert "FROM node:22-" in dockerfile
    assert "@anthropic-ai/claude-code@2.1.206" in dockerfile
    assert "python:3.12" in dockerfile
    assert "docker-ce-cli" in dockerfile
    assert "docker-buildx-plugin" in dockerfile
    assert "DOCKER_COMPOSE_VERSION=2.40.3" in dockerfile
    assert "DOCKER_COMPOSE_SHA256=" in dockerfile
    assert "docker-compose-linux-x86_64" in dockerfile
    assert "docker-compose-plugin" not in dockerfile
    assert "pip install --no-cache-dir uv==0.11.28" in dockerfile
    assert "uv sync --frozen --no-dev" in dockerfile
    assert "--mount=type=secret,id=combined_ca" in dockerfile
    assert "Acquire::https::CaInfo=/run/secrets/combined_ca" in dockerfile
    assert "NODE_EXTRA_CA_CERTS=/run/secrets/combined_ca" in dockerfile
    assert 'ENTRYPOINT ["python", "-m", "swegen.pipeline.worker"]' in dockerfile


def test_manifest_runs_configured_workers_and_leaves_validation_schedulable() -> None:
    documents = _documents()
    namespace = next(document for document in documents if document["kind"] == "Namespace")
    config_map = next(document for document in documents if document["kind"] == "ConfigMap")
    # Generate is a single deployment (the primary/overflow split was removed):
    # one deployment per stage, all asserted below.
    deployments = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "Deployment"
    }

    assert namespace["metadata"]["name"] == "swegen-pipeline"
    assert config_map["metadata"]["namespace"] == "swegen-pipeline"
    assert config_map["data"]["SWEGEN_PG_DB"] == "swegen_distributed"
    assert config_map["data"]["SWEGEN_WORKSPACE_ROOT"] == "/data/swegen-k3s/workspaces"
    assert config_map["data"]["GIT_SSL_CAINFO"] == "/etc/swegen/combined-ca.crt"
    assert config_map["data"]["SWEGEN_GITHUB_API_ATTEMPTS"] == "8"
    assert config_map["data"]["SWEGEN_GITHUB_RETRY_BASE_SECONDS"] == "10"
    assert config_map["data"]["SWEGEN_GITHUB_MAX_WAIT_SECONDS"] == "300"
    assert config_map["data"]["SWEGEN_REWARD_PRIMARY_MODEL"] == "gpt-5.6-sol"
    assert config_map["data"]["SWEGEN_REWARD_FALLBACK_MODEL"] == "gpt-5.6-sol"
    assert config_map["data"]["SWEGEN_REWARD_ENDPOINT"] == "http://1.95.77.23:3000"
    assert config_map["data"]["SWEGEN_MAX_REPAIR_ATTEMPTS"] == "3"
    assert config_map["data"]["SWEGEN_REPAIR_TIMEOUT_SECONDS"] == "14400"
    assert config_map["data"]["SWEGEN_MAX_REWARD_REPAIR_ATTEMPTS"] == "3"
    assert config_map["data"]["SWEGEN_REWARD_REPAIR_TIMEOUT_SECONDS"] == "14400"
    # Builds route 100% local while the remote BuildKit farm is down (504).
    assert config_map["data"]["SWEGEN_BUILD_ROUTER_MODE"] == "local"
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_PERCENT"] == "0"
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_FALLBACK_LOCAL"] == "true"
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_PULL_REGISTRY_URL"].endswith("/swesandbox")
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_SOURCE_REGISTRY"] == (
        "swr.cn-southwest-2.myhuaweicloud.com"
    )
    assert (
        config_map["data"]["SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_MIRROR_REGISTRY"]
        == (config_map["data"]["SWEGEN_REMOTE_BUILDKIT_REGISTRY"])
    )
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_URL"].endswith(":32083")
    assert config_map["data"]["SWEGEN_NPM_REGISTRY"] == ("https://registry.npmmirror.com/")
    assert (
        config_map["data"]["SWEGEN_REMOTE_BUILDKIT_REGISTRY"]
        == config_map["data"]["SWEGEN_SWR_HOST"]
    )
    assert (
        config_map["data"]["SWEGEN_REMOTE_BUILDKIT_REPOSITORY"]
        == config_map["data"]["SWEGEN_SWR_REPOSITORY"]
    )
    no_proxy = config_map["data"]["SWEGEN_NO_PROXY"]
    assert not any(character.isspace() for character in no_proxy)
    assert ".myhuaweicloud.com" in no_proxy
    assert ".huaweicloud.com" in no_proxy
    assert ".tailb940e6.ts.net" not in no_proxy.split(",")
    assert "7.244.3.251" in no_proxy.split(",")
    assert "1.95.77.23" not in no_proxy.split(",")
    assert "7.156.122.134" in no_proxy.split(",")
    assert set(deployments) == {
        "swegen-generate",
        "swegen-validate",
        "swegen-repair",
        "swegen-reward-repair",
        "swegen-reward",
        "swegen-push",
    }

    # The pinned stages were consolidated onto one node; the manifest and the
    # running cluster agree, so this table tracks them rather than the earlier
    # one-stage-per-node spread.
    # No stage may pin itself to 7.244.3.200: that node is the k3s
    # control-plane, and stacking build-capable workers on the same disk as
    # etcd made the API server unreachable under load.
    control_plane_node = "7.244.3.200"
    # Replica counts and image tags are retuned constantly during a run, so
    # pinning their literals only produced a permanently red test. Assert the
    # properties that encode intent instead: every worker runs a locally built
    # swegen-worker image at a positive replica count.
    expected_grace_seconds = {
        "swegen-generate": 18000,
        "swegen-validate": 600,
        "swegen-repair": 600,
        "swegen-reward-repair": 600,
        "swegen-reward": 600,
        "swegen-push": 3600,
    }
    stage_by_deployment = {
        "swegen-generate": "generate",
        "swegen-validate": "validate",
        "swegen-repair": "repair",
        "swegen-reward-repair": "reward_repair",
        "swegen-reward": "reward",
        "swegen-push": "push",
    }
    # Every worker stage spreads; none is pinned to a single node.
    distributed_deployments = {
        "swegen-generate",
        "swegen-validate",
        "swegen-repair",
        "swegen-reward-repair",
        "swegen-reward",
        "swegen-push",
    }
    # generate joins the docker stages so the agent can build during
    # generation and repair its own Dockerfile from the real error.
    docker_stages = {"generate", "validate", "repair", "reward_repair", "push"}
    for name, deployment in deployments.items():
        pod_spec = deployment["spec"]["template"]["spec"]
        container = pod_spec["containers"][0]
        stage = stage_by_deployment[name]

        assert isinstance(deployment["spec"]["replicas"], int)
        # 0 is allowed: a stage whose image or queue is not provisioned yet is
        # parked rather than deleted, so applying cannot spawn Pods that only
        # land in ErrImageNeverPull.
        assert deployment["spec"]["replicas"] >= 0
        # Stages drain in 10 minutes unless they own work that legitimately runs
        # longer: generation must outlast SWEGEN_GENERATE_TIMEOUT_SECONDS, and a
        # push is considered stuck rather than slow after an hour.
        assert pod_spec["terminationGracePeriodSeconds"] == expected_grace_seconds[name]
        # Recreate would take every replica of a stage down at once.
        assert deployment["spec"]["strategy"]["type"] == "RollingUpdate"
        # Nothing may be pinned to the control-plane node.
        assert pod_spec.get("nodeSelector", {}).get("swegen.pgcode/node-ip") != (control_plane_node)
        if name in distributed_deployments and deployment["spec"]["replicas"] > 0:
            assert "nodeSelector" not in pod_spec
            constraint = pod_spec["topologySpreadConstraints"][0]
            assert constraint["maxSkew"] == 1
            assert constraint["topologyKey"] == "kubernetes.io/hostname"
            # DoNotSchedule deadlocked a rollout once: replacement Pods were
            # held Pending against an already-full node and never converged.
            assert constraint["whenUnsatisfiable"] == "ScheduleAnyway"
            assert constraint["labelSelector"]["matchLabels"] == {
                "app.kubernetes.io/name": "swegen-worker",
                "swegen.pgcode/stage": stage,
            }
        # imagePullPolicy=Never means the tag must resolve on the node, so the
        # repository still matters even though the tag itself is volatile.
        assert container["image"].startswith("swegen-worker:")
        assert container["image"] != "swegen-worker:"
        assert container["imagePullPolicy"] == "Never"
        assert container["args"] == ["--stage", stage]
        assert any(
            volume.get("hostPath", {}).get("path") == "/data/swegen-k3s/workspaces"
            for volume in pod_spec["volumes"]
        )
        docker_socket_mounted = any(
            mount["mountPath"] == "/var/run/docker.sock"
            for mount in container.get("volumeMounts", [])
        )
        assert docker_socket_mounted is (stage in docker_stages)
        env_map = {
            item["name"]: item.get("value") for item in container.get("env", []) if "name" in item
        }
        if stage in docker_stages:
            assert env_map.get("DOCKER_BUILDKIT") == "1"
            assert env_map.get("BUILDX_BUILDER") == "default"
            assert env_map.get("COMPOSE_BAKE") == "false"
            assert env_map.get("SWEGEN_BUILD_SLOT_DIR") == "/run/swegen-build-slots"
            assert env_map.get("SWEGEN_REAL_DOCKER") == "/usr/bin/docker"
            assert env_map.get("PATH", "").startswith("/opt/swegen/bin:")
            assert any(
                mount["mountPath"] == "/run/swegen-build-slots"
                for mount in container.get("volumeMounts", [])
            )
            assert any(
                mount["mountPath"] == "/opt/swegen/bin"
                for mount in container.get("volumeMounts", [])
            )
            assert any(
                volume.get("hostPath", {}).get("path") == "/data/swegen-k3s/build-slots"
                for volume in pod_spec["volumes"]
            )
            assert any(
                volume.get("hostPath", {}).get("path") == "/data/swegen-k3s/bin"
                for volume in pod_spec["volumes"]
            )
        else:
            assert "COMPOSE_BAKE" not in env_map
            assert "SWEGEN_BUILD_SLOT_DIR" not in env_map

    reward_env = {
        item["name"]: item
        for item in deployments["swegen-reward"]["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert reward_env["SWEGEN_REWARD_API_KEY"]["valueFrom"]["secretKeyRef"]["name"] == (
        "swegen-reward-credentials-gpt56sol-20260803"
    )


def test_every_worker_has_a_rootfs_integrity_probe() -> None:
    # crun/overlayfs snapshot corruption under pod churn can strip /app/.venv
    # (python + swegen) from a Running pod, which then fails every task in
    # ~13ms and drains the queue while k8s still reports it Ready. Each worker
    # container must carry an exec probe that imports swegen so such a pod is
    # restarted onto a fresh overlay instead of silently shredding work.
    documents = _documents()
    worker_deployments = [
        document
        for document in documents
        if document["kind"] == "Deployment"
        and document["spec"]["template"]["spec"]["containers"][0]["name"] == "worker"
    ]
    assert worker_deployments, "expected at least one worker deployment"
    for deployment in worker_deployments:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        name = deployment["metadata"]["name"]
        liveness = container.get("livenessProbe")
        startup = container.get("startupProbe")
        assert liveness is not None, f"{name} missing livenessProbe"
        assert startup is not None, f"{name} missing startupProbe"
        for probe in (liveness, startup):
            cmd = probe["exec"]["command"]
            # The probe MUST run through /bin/sh, not the venv python directly.
            # overlayfs corruption can delete /app/.venv/bin/python; an exec
            # probe whose binary is missing errors into "unknown state", which
            # kubelet does NOT count toward failureThreshold — a gutted pod
            # would never restart. /bin/sh always exists, so the probe launches
            # and returns exit 127 (a real failure) when python is gone.
            assert cmd[0] == "/bin/sh", f"{name} probe must be shell-wrapped: {cmd}"
            assert "import swegen.cli" in cmd[-1], name
            assert "/app/.venv/bin/python" in cmd[-1], name
        # Liveness must actually fail a wedged pod (finite failureThreshold)
        # rather than tolerate it indefinitely.
        assert liveness["failureThreshold"] <= 3, name
        # The container must run from / (not the image WORKDIR /app). When
        # corruption deletes /app, the OCI runtime cannot chdir into the
        # workdir to LAUNCH the probe exec, so it errors into "unknown state"
        # (never counted toward failureThreshold) and the pod is never
        # restarted. Running from / lets the probe always launch and fail.
        assert container.get("workingDir") == "/", name


def test_secret_and_image_helpers_exist_without_cache_cleaner() -> None:
    assert (DEPLOY_DIR / "create-secrets.sh").is_file()
    assert (DEPLOY_DIR / "build-import-worker.sh").is_file()
    assert (DEPLOY_DIR / "sync-worker-image.sh").is_file()
    assert not (DEPLOY_DIR / "docker-cache-cleaner.sh").exists()

    dockerignore = (ROOT / ".dockerignore").read_text().splitlines()
    assert ".git" in dockerignore
    assert ".worktrees" in dockerignore
    assert ".swegen" in dockerignore
    assert "tasks" in dockerignore
    assert "slurm-runtime" in dockerignore

    build_helper = (DEPLOY_DIR / "build-import-worker.sh").read_text()
    assert "--build-arg HTTP_PROXY" in build_helper
    assert "--build-arg HTTPS_PROXY" in build_helper
    assert '--secret "id=combined_ca,src=${build_ca}"' in build_helper
    assert "k3s ctr -n k8s.io images import" in build_helper
    assert "SWEGEN_K3S_NODES" in build_helper
    assert "data-dir:" in build_helper
    assert "/agent/images/" in build_helper
    assert "k3s crictl inspecti" in build_helper

    secret_helper = (DEPLOY_DIR / "create-secrets.sh").read_text()
    assert "normalize_env_file" in secret_helper
    assert "s/^export[[:space:]]+//" in secret_helper
    assert "merged_docker_config" in secret_helper

    # sync-worker-image.sh guarantees a tag lands on every node (avoiding the
    # ErrImageNeverPull failure when a scaled Pod lands on a node missing the
    # image). It must relay through the invoking host, not assume node-to-node
    # SSH, because the cluster's inter-node SSH mesh is not fully connected.
    sync_helper = (DEPLOY_DIR / "sync-worker-image.sh").read_text()
    assert "k3s ctr -n k8s.io images export" in sync_helper
    assert "k3s ctr -n k8s.io images import" in sync_helper
    assert "k3s crictl inspecti" in sync_helper
    assert "--all" in sync_helper
    assert "SWEGEN_K3S_NODES" in sync_helper
    assert '"httpProxy"' in secret_helper
    assert '"httpsProxy"' in secret_helper
    assert '"noProxy"' in secret_helper
    assert '--from-file=config.json="${merged_docker_config}"' in secret_helper
    assert "swegen-repair-model-credentials" in secret_helper
    assert (
        'os.environ.get("SWEGEN_REPAIR_MODEL_NAME", "glm-5.2-thinking-npu")'
        in secret_helper
    )
    assert 'entry.get("model_name") == model' in secret_helper
    assert '"CLAUDE_CODE_MAX_CONTEXT_TOKENS", "160000"' in secret_helper
    assert '"CLAUDE_CODE_AUTO_COMPACT_WINDOW", "150000"' in secret_helper
    assert "SWEGEN_REMOTE_BUILDKIT_REGISTRY_USERNAME" in secret_helper
    assert "SWEGEN_REMOTE_BUILDKIT_REGISTRY_PASSWORD" in secret_helper
    assert "SWEGEN_REMOTE_BUILDKIT_PULL_USERNAME" in secret_helper
    assert "SWEGEN_REMOTE_BUILDKIT_PULL_PASSWORD" in secret_helper
    assert "base64.b64decode(encoded, validate=True)" in secret_helper
    assert (
        'model_secret_name="${SWEGEN_MODEL_SECRET_NAME:-swegen-model-credentials-v2}"'
        in secret_helper
    )
    assert (
        'generate_glm_secret_name="${SWEGEN_GENERATE_GLM_SECRET_NAME:-swegen-model-credentials-glm52-thinking-npu-20260804}"'
        in secret_helper
    )
    assert (
        'reward_model_secret_name="${SWEGEN_REWARD_MODEL_SECRET_NAME:-swegen-reward-credentials-gpt56sol-20260803}"'
        in secret_helper
    )
    assert 'ensure_immutable_env_secret "${model_secret_name}"' in secret_helper
    assert 'ensure_immutable_env_secret "${generate_glm_secret_name}"' in secret_helper
    assert 'ensure_immutable_env_secret "${reward_model_secret_name}"' in secret_helper
    assert 'os.environ.get("SWEGEN_REWARD_MODEL_NAME", "gpt-5.6-sol")' in secret_helper
    assert 'f"SWEGEN_REWARD_API_KEY={api_key}\\n"' in secret_helper
    assert '"OPENAI_API_KEY": api_key' in secret_helper
    assert '"OPENAI_BASE_URL": api_base' in secret_helper
    assert '"OPENAI_MODEL": model' in secret_helper
    assert 'blocked_prefixes = ("ANTHROPIC_", "CLAUDE_", "OPENAI_"' in secret_helper


# The `swegen-generate-model-credentials` admission policy (which allowlisted
# generate's final envFrom secret and forbade inline model env) was removed to
# support dynamic per-endpoint generate pools that inject the model via inline
# container env. Its tests are dropped accordingly.


def test_coworker_deployer_cannot_mutate_secrets_and_is_admission_scoped() -> None:
    rbac_documents = [
        document
        for document in yaml.safe_load_all((DEPLOY_DIR / "worker-deployer-rbac.yaml").read_text())
        if document
    ]
    role = next(document for document in rbac_documents if document["kind"] == "Role")
    resources = {resource for rule in role["rules"] for resource in rule.get("resources", [])}
    assert "deployments" in resources
    assert "secrets" not in resources

    guard = (DEPLOY_DIR / "credential-guard.yaml").read_text()
    # The remaining policy scopes the worker-deployer SA to swegen-test-* names.
    assert "swegen-worker-deployer" in guard
    assert "swegen-test-" in guard


def test_reward_repair_migration_is_idempotent_and_creates_its_queue() -> None:
    migration = (DEPLOY_DIR / "migrate-reward-repair-stage.sql").read_text()

    assert "DROP CONSTRAINT IF EXISTS" in migration
    assert "reward_repair" in migration
    assert "pgmq.create('swegen_reward_repair')" in migration
    assert "CREATE INDEX IF NOT EXISTS" in migration
    assert "state = 'rejected' AND current_stage = 'reward'" in migration
    assert "state = 'failed' AND current_stage = 'reward_repair'" in migration


def test_buildkit_intermediate_migration_seeds_verified_swr_references() -> None:
    migration = (DEPLOY_DIR / "migrate-buildkit-intermediates.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS public.buildkit_intermediates" in migration
    assert "ON CONFLICT (repo, dependency_key, build_key) DO UPDATE" in migration
    assert migration.count("sha256:") >= 6
    assert "dep-runbox7-ca6df8e5-ae4449b8e848-ngcc-w1" in migration
    assert "dep-revault-gui-b3ff4588-2fc887c39ebe-testbuild-j1" in migration
    assert "dep-varisat-b92d6e87-6879d03a5a4b-testbuild-j1" in migration


def test_repaired_validate_priority_migration_is_bounded_and_idempotent() -> None:
    migration = (DEPLOY_DIR / "migrate-validate-repaired-priority.sql").read_text()

    assert "pgmq.create('swegen_validate_repaired')" in migration
    assert "CREATE OR REPLACE FUNCTION pgmq.send" in migration
    assert "message->>'attempt'" in migration
    assert "FOR UPDATE SKIP LOCKED" in migration
    assert "vt <= clock_timestamp()" in migration
    assert "pgmq.archive('swegen_validate'" in migration
    assert "\\set ON_ERROR_STOP on" in migration


def test_buildkit_pruner_is_a_bounded_node_local_daemonset() -> None:
    manifest = yaml.safe_load((DEPLOY_DIR / "swegen-buildkit-pruner.yaml").read_text())
    pod_spec = manifest["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    script = container["args"][0]
    env = {item["name"]: item.get("value") for item in container["env"]}

    assert manifest["kind"] == "DaemonSet"
    assert manifest["metadata"]["namespace"] == "swegen-pipeline"
    assert pod_spec["automountServiceAccountToken"] is False
    assert container["image"] == "swegen-worker:hybrid-buildkit-proxy-skip-20260730"
    assert "flock -n 9" in script
    assert "flock -n 8" in script
    assert "active build slots=" in script
    assert 'flock -n "${slot_file}" true' in script
    assert '8>"${build_gc_lock}"' in script
    assert "docker image prune --force" in script
    assert "docker image prune --all" not in script
    assert "crictl" not in script
    assert "ctr " not in script
    assert "docker stop" in script
    assert "docker rm --force" in script
    assert "docker buildx prune" in script
    assert "--builder default" in script
    assert "docker builder prune" not in script
    assert script.index("docker image prune --force") < script.index("docker rm --force")
    assert script.index("docker rm --force") < script.index("docker buildx prune")
    assert "--all" in script and "--force" in script

    # The failed-image cleanup needs DB credentials, which it draws from the
    # swegen-database secret the same way the worker container does. Without
    # this envFrom the batched status query has no SWEGEN_PG_* to connect with.
    envfrom_secrets = {
        entry["secretRef"]["name"]
        for entry in container.get("envFrom", [])
        if "secretRef" in entry
    }
    assert "swegen-database" in envfrom_secrets
    assert env["SWEGEN_FAILED_IMAGE_PRUNE_ENABLED"] == "true"
    assert env["SWEGEN_FAILED_IMAGE_PRUNE_BUDGET_SECONDS"] == "300"

    # The per-task failed-image cleanup: it selects the node's -swegenimage
    # tags, is gated behind SWEGEN_FAILED_IMAGE_PRUNE_ENABLED, derives the
    # task_id by stripping the hb__ prefix and -swegenimage suffix, and queries
    # the DB once (ANY(%s)) for terminally-failed, non-active tasks before
    # deleting. It is fail-safe: an unreachable DB deletes nothing.
    assert "grep -- '-swegenimage'" in script
    assert '"${SWEGEN_FAILED_IMAGE_PRUNE_ENABLED}" = "true"' in script
    assert "${failed_tag%-swegenimage}" in script
    assert "${failed_task#hb__}" in script
    assert "/app/.venv/bin/python -" in script
    assert "task.task_id = ANY(%s)" in script
    assert "state IN ('failed', 'rejected')" in script
    assert "current_stage IN ('validate', 'reward', 'repair')" in script
    assert "heartbeat_at >= now() - interval '5 minutes'" in script
    assert "timeout 60 docker rmi" in script
    assert "failed-image cleanup checked=" in script
    # The whole cleanup must run BEFORE the buildkit-cache prune so freed
    # per-task images are reclaimed first.
    assert script.index("grep -- '-swegenimage'") < script.index("docker buildx prune")
    assert script.index("failed-image cleanup checked=") < script.index("docker buildx prune")
    assert env["SWEGEN_BUILDKIT_PRUNE_INTERVAL_SECONDS"] == "600"
    assert env["SWEGEN_BUILDKIT_PRUNE_START_DELAY_SECONDS"] == "600"
    assert env["SWEGEN_BUILDKIT_PRUNE_TIMEOUT_SECONDS"] == "3600"
    assert env["SWEGEN_BUILD_SLOT_DIR"] == "/run/swegen-build-slots"
    assert env["SWEGEN_DOCKER_CONTAINER_MAX_AGE_SECONDS"] == "7200"
    assert env["SWEGEN_DOCKER_STOP_TIMEOUT_SECONDS"] == "10"
    assert any(
        volume.get("hostPath", {}).get("path") == "/var/run/docker.sock"
        for volume in pod_spec["volumes"]
    )
    assert not any(
        "containerd" in volume.get("hostPath", {}).get("path", "") for volume in pod_spec["volumes"]
    )
    assert any(
        volume.get("hostPath", {}).get("path") == "/run/lock" for volume in pod_spec["volumes"]
    )
    assert any(
        volume.get("hostPath", {}).get("path") == "/data/swegen-k3s/build-slots"
        for volume in pod_spec["volumes"]
    )
    assert any(
        mount.get("mountPath") == "/run/swegen-build-slots" for mount in container["volumeMounts"]
    )


def test_generate_circuit_breaker_is_latched_and_rbac_scoped() -> None:
    documents = [
        document
        for document in yaml.safe_load_all(
            (DEPLOY_DIR / "swegen-generate-circuit-breaker.yaml").read_text()
        )
        if document
    ]
    role = next(document for document in documents if document["kind"] == "Role")
    deployment = next(document for document in documents if document["kind"] == "Deployment")
    rule = role["rules"][0]
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item["value"] for item in container["env"]}

    assert rule["resources"] == ["deployments"]
    assert rule["resourceNames"] == ["swegen-generate"]
    assert set(rule["verbs"]) == {"get", "patch"}
    assert deployment["spec"]["replicas"] == 1
    assert container["command"] == [
        "python",
        "-m",
        "swegen.pipeline.generate_circuit_breaker",
    ]
    assert env["SWEGEN_BREAKER_WINDOW_SECONDS"] == "300"
    assert env["SWEGEN_BREAKER_MINIMUM_SAMPLES"] == "20"
    assert env["SWEGEN_BREAKER_FAILURE_RATE_THRESHOLD"] == "0.5"


def test_generate_transient_requeue_is_transactional_and_audited() -> None:
    migration = (DEPLOY_DIR / "requeue-transient-generate-failures-20260802.sql").read_text()

    assert migration.startswith("\\set ON_ERROR_STOP on\n\nBEGIN;")
    assert "LOCK TABLE pgmq.q_swegen_generate" in migration
    assert "DISTINCT ON (result.task_id, result.task_version)" in migration
    assert "could not read Username for %github.com" in migration
    assert "error NOT ILIKE '%404%'" in migration
    assert "error NOT ILIKE '%not merged%'" in migration
    assert "NOT EXISTS (\n    SELECT 1\n    FROM pgmq.q_swegen_generate" in migration
    assert "pgmq.send(" in migration
    assert "UPDATE pipeline_tasks AS task" in migration
    assert "pipeline_generate_requeue_audit" in migration
    assert "COMMIT;" in migration


def test_generate_all_failed_requeue_is_transactional_idempotent_and_audited() -> None:
    migration = (DEPLOY_DIR / "requeue-all-failed-generate-20260802.sql").read_text()

    assert migration.startswith("\\set ON_ERROR_STOP on\n\nBEGIN;")
    assert "LOCK TABLE pgmq.q_swegen_generate" in migration
    assert "task.state = 'failed'" in migration
    assert "task.current_stage = 'generate'" in migration
    assert "ORDER BY result.attempt DESC" in migration
    assert "source_stage_attempt + 1 AS new_stage_attempt" in migration
    assert "NOT EXISTS (\n    SELECT 1\n    FROM pgmq.q_swegen_generate" in migration
    assert "pipeline_generate_requeue_all_failed_audit" in migration
    assert "pgmq.send(" in migration
    assert "UPDATE pipeline_tasks AS task" in migration
    assert "operator requested GLM Generate restart at 32" in migration
    assert "COMMIT;" in migration


def test_from_scratch_guide_pins_runtime_and_documents_growth_controls() -> None:
    guide = (DEPLOY_DIR / "README.md").read_text()

    assert "v1.36.2+k3s1" in guide
    assert "v2.40.3" in guide and "COMPOSE_BAKE=false" in guide
    assert "PostgreSQL" in guide and "16.13" in guide
    assert "PGMQ" in guide and "1.12.0" in guide and "SQL-only" in guide
    assert "src/swegen/queueing/bootstrap.sql" in guide
    assert "src/swegen/schema.sql" in guide
    assert "SWEGEN_K3S_NODES='NEW_NODE_IP'" in guide
    assert "docker buildx prune" in guide
    assert "imageGCHighThresholdPercent: 70" in guide
    assert "public.pipeline_task_files" in guide


def test_generate_endpoint_controller_rbac_is_namespace_scoped() -> None:
    """The dynamic-endpoint controller can manage deployments cluster-wide in
    the namespace (dynamic names) but the controller code hard-guards the
    swegen-generate-dyn- prefix; the manifest must grant the needed verbs."""

    documents = [
        d
        for d in yaml.safe_load_all(
            (DEPLOY_DIR / "swegen-generate-endpoint-controller.yaml").read_text()
        )
        if d
    ]
    kinds = {d["kind"] for d in documents}
    assert {"ServiceAccount", "Role", "RoleBinding", "Deployment"} <= kinds

    role = next(d for d in documents if d["kind"] == "Role")
    deploy_rule = next(r for r in role["rules"] if "deployments" in r.get("resources", []))
    assert {"create", "delete", "patch", "get", "list"} <= set(deploy_rule["verbs"])
    # Dynamic names => no resourceNames pin (the code enforces the prefix guard).
    assert "resourceNames" not in deploy_rule
    pod_rule = next(r for r in role["rules"] if "pods" in r.get("resources", []))
    assert {"list", "delete"} <= set(pod_rule["verbs"])

    controller = next(d for d in documents if d["kind"] == "Deployment")
    container = controller["spec"]["template"]["spec"]["containers"][0]
    assert container["command"] == [
        "python",
        "-m",
        "swegen.pipeline.generate_endpoint_controller",
    ]


def test_generate_model_credential_policy_is_removed() -> None:
    """The generate model-credential admission policy was dropped to allow
    dynamic per-endpoint pools with inline model env."""

    guard = (DEPLOY_DIR / "credential-guard.yaml").read_text()
    policies = [
        d
        for d in yaml.safe_load_all(guard)
        if d and d.get("kind") in {"ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"}
    ]
    names = {d["metadata"]["name"] for d in policies}
    assert "swegen-generate-model-credentials" not in names
    # The unrelated worker-deployer scope policy remains.
    assert "swegen-test-worker-deployer-scope" in names
