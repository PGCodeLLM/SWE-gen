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
    assert config_map["data"]["SWEGEN_MAX_REPAIR_ATTEMPTS"] == "3"
    assert config_map["data"]["SWEGEN_REPAIR_TIMEOUT_SECONDS"] == "14400"
    assert config_map["data"]["SWEGEN_BUILD_ROUTER_MODE"] == "hybrid"
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_PERCENT"] == "75"
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_FALLBACK_LOCAL"] == "true"
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_PULL_REGISTRY_URL"].endswith(
        "/swesandbox"
    )
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_SOURCE_REGISTRY"] == (
        "swr.cn-southwest-2.myhuaweicloud.com"
    )
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_BASE_IMAGE_MIRROR_REGISTRY"] == (
        config_map["data"]["SWEGEN_REMOTE_BUILDKIT_REGISTRY"]
    )
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_URL"].endswith(":32083")
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_REGISTRY"] == config_map[
        "data"
    ]["SWEGEN_SWR_HOST"]
    assert config_map["data"]["SWEGEN_REMOTE_BUILDKIT_REPOSITORY"] == config_map[
        "data"
    ]["SWEGEN_SWR_REPOSITORY"]
    no_proxy = config_map["data"]["SWEGEN_NO_PROXY"]
    assert not any(character.isspace() for character in no_proxy)
    assert ".myhuaweicloud.com" in no_proxy
    assert ".huaweicloud.com" in no_proxy
    assert ".tailb940e6.ts.net" not in no_proxy.split(",")
    assert "7.244.3.251" in no_proxy.split(",")
    assert "7.156.122.134" in no_proxy.split(",")
    assert set(deployments) == {
        "swegen-generate",
        "swegen-validate",
        "swegen-repair",
        "swegen-reward",
        "swegen-push",
    }

    expected_nodes = {
        "swegen-generate": "7.244.2.110",
        "swegen-reward": "7.244.1.209",
        "swegen-push": "7.244.3.200",
    }
    expected_replicas = {
        "swegen-generate": 48,
        "swegen-validate": 96,
        "swegen-repair": 4,
        "swegen-reward": 16,
        "swegen-push": 1,
    }
    expected_images = {
        "swegen-generate": "swegen-worker:e2e",
        "swegen-validate": "swegen-worker:hybrid-buildkit-proxy-skip-20260730",
        "swegen-repair": "swegen-worker:hybrid-buildkit-proxy-skip-20260730",
        "swegen-reward": "swegen-worker:e2e",
        "swegen-push": "swegen-worker:hybrid-buildkit-proxy-skip-20260730",
    }
    docker_stages = {"validate", "repair", "push"}
    for name, deployment in deployments.items():
        pod_spec = deployment["spec"]["template"]["spec"]
        container = pod_spec["containers"][0]
        stage = name.removeprefix("swegen-")

        assert deployment["spec"]["replicas"] == expected_replicas[name]
        if name in {"swegen-validate", "swegen-repair"}:
            assert "nodeSelector" not in pod_spec
            if name == "swegen-validate":
                assert deployment["spec"]["strategy"] == {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxSurge": 0, "maxUnavailable": "20%"},
                }
                assert pod_spec["terminationGracePeriodSeconds"] == 300
                assert pod_spec["topologySpreadConstraints"] == [
                    {
                        "maxSkew": 1,
                        "topologyKey": "kubernetes.io/hostname",
                        "whenUnsatisfiable": "DoNotSchedule",
                        "labelSelector": {
                            "matchLabels": {
                                "app.kubernetes.io/name": "swegen-worker",
                                "swegen.pgcode/stage": "validate",
                            }
                        },
                    }
                ]
        else:
            assert pod_spec["nodeSelector"] == {
                "swegen.pgcode/node-ip": expected_nodes[name]
            }
        assert container["image"] == expected_images[name]
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
            item["name"]: item.get("value")
            for item in container.get("env", [])
            if "name" in item
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


def test_secret_and_image_helpers_exist_without_cache_cleaner() -> None:
    assert (DEPLOY_DIR / "create-secrets.sh").is_file()
    assert (DEPLOY_DIR / "build-import-worker.sh").is_file()
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
    assert '"httpProxy"' in secret_helper
    assert '"httpsProxy"' in secret_helper
    assert '"noProxy"' in secret_helper
    assert '--from-file=config.json="${merged_docker_config}"' in secret_helper
    assert "swegen-repair-model-credentials" in secret_helper
    assert 'os.environ.get("SWEGEN_REPAIR_MODEL_NAME", "glm-5.2-moedsa")' in secret_helper
    assert 'entry.get("model_name") == model' in secret_helper
    assert '"CLAUDE_CODE_MAX_CONTEXT_TOKENS", "160000"' in secret_helper
    assert '"CLAUDE_CODE_AUTO_COMPACT_WINDOW", "150000"' in secret_helper


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
