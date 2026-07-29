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
    assert "docker-compose-plugin" in dockerfile
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
    no_proxy = config_map["data"]["SWEGEN_NO_PROXY"]
    assert not any(character.isspace() for character in no_proxy)
    assert ".myhuaweicloud.com" in no_proxy
    assert ".huaweicloud.com" in no_proxy
    assert ".tailb940e6.ts.net" not in no_proxy.split(",")
    assert "7.244.3.251" in no_proxy.split(",")
    assert set(deployments) == {
        "swegen-generate",
        "swegen-validate",
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
        "swegen-validate": 168,
        "swegen-reward": 16,
        "swegen-push": 1,
    }
    expected_images = {
        "swegen-generate": "swegen-worker:e2e",
        "swegen-validate": "swegen-worker:e2e-observe-tail-activity-20260729",
        "swegen-reward": "swegen-worker:e2e",
        "swegen-push": "swegen-worker:e2e-ca-20260729",
    }
    docker_stages = {"validate", "push"}
    for name, deployment in deployments.items():
        pod_spec = deployment["spec"]["template"]["spec"]
        container = pod_spec["containers"][0]
        stage = name.removeprefix("swegen-")

        assert deployment["spec"]["replicas"] == expected_replicas[name]
        if name == "swegen-validate":
            assert "nodeSelector" not in pod_spec
            assert deployment["spec"]["strategy"] == {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxSurge": "50%", "maxUnavailable": 0},
            }
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


def test_from_scratch_guide_pins_runtime_and_documents_growth_controls() -> None:
    guide = (DEPLOY_DIR / "README.md").read_text()

    assert "v1.36.2+k3s1" in guide
    assert "PostgreSQL" in guide and "16.13" in guide
    assert "PGMQ" in guide and "1.12.0" in guide and "SQL-only" in guide
    assert "src/swegen/queueing/bootstrap.sql" in guide
    assert "src/swegen/schema.sql" in guide
    assert "SWEGEN_K3S_NODES='NEW_NODE_IP'" in guide
    assert "docker buildx prune" in guide
    assert "imageGCHighThresholdPercent: 70" in guide
    assert "public.pipeline_task_files" in guide
