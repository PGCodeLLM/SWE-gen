from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml


def _write_config(path: Path) -> None:
    path.write_text(
        "[autoqueue]\n"
        "max_queued = 1\n"
        "\n[pipeline]\n"
        'namespace = "swegen-pipeline-tester"\n'
        'secret_source_namespace = "swegen-pipeline"\n'
        'worker_image = "swegen-worker:tester-test"\n'
        'workspace_host_path = "/data/swegen-test/workspaces"\n'
        'repo_cache_host_path = "/data/swegen-test/cache"\n'
        'successful_tasks_host_path = "/data/swegen-test/successful"\n'
        'k3s_nodes = ["192.0.2.10"]\n'
        'k3s_ssh_user = "root"\n'
        'build_ca_path = "/etc/ssl/certs/ca-certificates.crt"\n'
        "build_worker_image_on_start = false\n"
        "autoqueue_workers = 1\n"
        "generate_workers = 2\n"
        "validate_workers = 3\n"
        "reward_workers = 4\n"
        "push_workers = 5\n"
        "rollout_timeout_seconds = 17\n"
        "\n[swr.minddistiller]\n"
        'host = "mind.example"\n'
        'repository = "team/generated"\n'
        'username = "configured-user"\n'
        'password = "configured-password"\n'
    )


def _write_fake_kubectl(path: Path) -> None:
    path.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        'printf "%s\\n" "$*" >>"${FAKE_KUBECTL_LOG}"\n'
        'if [[ "${1:-}" == "apply" ]]; then\n'
        '  manifest="${@: -1}"\n'
        '  if [[ " $* " != *" --dry-run=server "* ]]; then\n'
        '    cp -- "${manifest}" "${FAKE_APPLIED_MANIFEST}"\n'
        "  fi\n"
        "fi\n"
    )
    path.chmod(0o755)


def _run_switch(tmp_path: Path, action: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    root = Path(__file__).resolve().parents[1]
    script = root / "master_switch.sh"
    config = tmp_path / "swegen.toml"
    _write_config(config)
    fake_kubectl = tmp_path / "kubectl"
    _write_fake_kubectl(fake_kubectl)
    command_log = tmp_path / "kubectl.log"
    environment = dict(os.environ)
    environment.update(
        {
            "SWEGEN_CONFIG_SOURCE": str(config),
            "SWEGEN_KUBECTL": str(fake_kubectl),
            "FAKE_KUBECTL_LOG": str(command_log),
            "FAKE_APPLIED_MANIFEST": str(tmp_path / "applied.yaml"),
            "SWEGEN_GENERATE_REPLICAS": "99",
            "SWEGEN_ROLLOUT_TIMEOUT": "99h",
        }
    )
    result = subprocess.run(
        [str(script), action],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    commands = command_log.read_text().splitlines() if command_log.exists() else []
    return result, commands


def test_start_zero_applies_manifest_and_starts_autoqueue_last(tmp_path: Path) -> None:
    result, commands = _run_switch(tmp_path, "start")

    assert result.returncode == 0, result.stderr
    apply_commands = [command for command in commands if command.startswith("apply ")]
    assert apply_commands[0].startswith("apply --dry-run=server -f ")
    assert apply_commands[1].startswith("apply -f ")
    assert any(
        "patch secret/swegen-private-files --type=merge --patch-file" in command
        for command in commands
    )
    assert "configured-password" not in "\n".join(commands)
    expected_workers = {
        "deployment/swegen-generate": 2,
        "deployment/swegen-validate": 3,
        "deployment/swegen-reward": 4,
        "deployment/swegen-push": 5,
    }
    worker_scales = [
        next(
            index
            for index, command in enumerate(commands)
            if f"{name} --replicas={replicas}" in command
        )
        for name, replicas in expected_workers.items()
    ]
    autoqueue_scale = next(
        index
        for index, command in enumerate(commands)
        if "deployment/swegen-autoqueue --replicas=1" in command
    )
    assert max(worker_scales) < autoqueue_scale
    assert any("--timeout=17s" in command for command in commands)
    documents = list(yaml.safe_load_all((tmp_path / "applied.yaml").read_text()))
    replicas = {
        document["metadata"]["name"]: document["spec"]["replicas"]
        for document in documents
        if isinstance(document, dict) and document.get("kind") == "Deployment"
    }
    assert replicas == {
        "swegen-autoqueue": 0,
        "swegen-generate": 2,
        "swegen-validate": 3,
        "swegen-reward": 4,
        "swegen-push": 5,
    }
    assert all(
        document.get("metadata", {}).get("name") == "swegen-pipeline-tester"
        if document.get("kind") == "Namespace"
        else document.get("metadata", {}).get("namespace") == "swegen-pipeline-tester"
        for document in documents
        if isinstance(document, dict)
    )
    deployments = [
        document
        for document in documents
        if isinstance(document, dict) and document.get("kind") == "Deployment"
    ]
    assert {
        deployment["spec"]["template"]["spec"]["containers"][0]["image"]
        for deployment in deployments
    } == {"swegen-worker:tester-test"}
    host_paths = {
        volume["hostPath"]["path"]
        for deployment in deployments
        for volume in deployment["spec"]["template"]["spec"].get("volumes", [])
        if "hostPath" in volume
    }
    assert "/data/swegen-test/workspaces" in host_paths
    assert "/data/swegen-test/cache" in host_paths
    assert "/data/swegen-test/successful" in host_paths
    assert not any(path.startswith("/data/swegen-k3s/") for path in host_paths)
    assert "SWE-gen pipeline is running" in result.stdout


def test_stop_disables_autoqueue_before_workers(tmp_path: Path) -> None:
    result, commands = _run_switch(tmp_path, "stop")

    assert result.returncode == 0, result.stderr
    scale_commands = [command for command in commands if " scale " in f" {command} "]
    assert "deployment/swegen-autoqueue --replicas=0" in scale_commands[0]
    assert all("--replicas=0" in command for command in scale_commands)
