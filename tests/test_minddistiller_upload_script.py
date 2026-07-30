from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _write_fake_docker(path: Path, *, fail_build: bool = False) -> None:
    build_result = "exit 7" if fail_build else 'touch "${FAKE_DOCKER_STATE}/local"'
    path.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        'printf "%s\\n" "$*" >>"${FAKE_DOCKER_LOG}"\n'
        'case "${1:-} ${2:-}" in\n'
        '  "login "*) exit 0 ;;\n'
        '  "image inspect") [[ -f "${FAKE_DOCKER_STATE}/local" ]] ;;\n'
        f'  "build "*) {build_result} ;;\n'
        '  "manifest inspect")\n'
        '    if [[ -f "${FAKE_DOCKER_STATE}/remote" ]]; then exit 0; fi\n'
        '    echo "manifest unknown" >&2\n'
        "    exit 1\n"
        "    ;;\n"
        '  "tag "*) exit 0 ;;\n'
        '  "push "*)\n'
        '    if [[ -z "${FAKE_MANIFEST_ALWAYS_MISSING:-}" ]]; then\n'
        '      touch "${FAKE_DOCKER_STATE}/remote"\n'
        "    fi\n"
        "    ;;\n"
        '  "image rm") exit 0 ;;\n'
        "esac\n"
    )
    path.chmod(0o755)


def _run_script(
    tmp_path: Path,
    *,
    fail_build: bool = False,
    manifest_always_missing: bool = False,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    root = Path(__file__).resolve().parents[1]
    script = root / "swr_credentials" / "minddistiller_build_and_upload.sh"
    ids = tmp_path / "ids.txt"
    ids.write_text("owner__repo-1\nowner__repo-1\nINVALID/ID\nmissing__repo-2\n")
    task_root = tmp_path / "tasks"
    dockerfile = task_root / "owner__repo-1" / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM scratch\n")
    credentials = tmp_path / "minddistiller.csv"
    credentials.write_text("field,value\n用户名,test-user\n密码,test-password\n")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_fake_docker(fake_bin / "docker", fail_build=fail_build)
    state_dir = tmp_path / "docker-state"
    state_dir.mkdir()
    docker_log = tmp_path / "docker.log"
    report = tmp_path / "report.txt"
    environment = dict(os.environ)
    environment.update(
        {
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "FAKE_DOCKER_LOG": str(docker_log),
            "FAKE_DOCKER_STATE": str(state_dir),
            "SWR_CREDENTIALS_FILE": str(credentials),
            "LOG_DIR": str(tmp_path / "logs"),
            "REPORT_FILE": str(report),
            "JOBS": "1",
            "SWR_RETRIES": "1",
            "PROGRESS_EVERY": "1",
        }
    )
    if manifest_always_missing:
        environment["FAKE_MANIFEST_ALWAYS_MISSING"] = "1"
    result = subprocess.run(
        [str(script), str(ids), str(task_root)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    return result, docker_log, report


def test_selector_builds_retained_local_tag_and_pushes_expected_remote_tag(
    tmp_path: Path,
) -> None:
    result, docker_log, report = _run_script(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "Selected Harbor tasks: 1" in result.stdout
    assert "Missing tasks or Dockerfiles: 1" in result.stdout
    assert "Invalid IDs ignored: 1" in result.stdout
    assert "Duplicate IDs ignored: 1" in result.stdout
    commands = docker_log.read_text().splitlines()
    local = "ea_sz_owner__repo-1:latest"
    remote = (
        "swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com/"
        "aifm.coder.exp/swegen/generated:owner__repo-1"
    )
    assert any(command.startswith("build ") and f"--tag {local}" in command for command in commands)
    assert f"tag {local} {remote}" in commands
    assert f"push {remote}" in commands
    assert commands.count(f"manifest inspect {remote}") == 2
    assert "Failed:     0" in report.read_text()


def test_batch_script_returns_failure_when_an_individual_build_fails(tmp_path: Path) -> None:
    result, _docker_log, report = _run_script(tmp_path, fail_build=True)

    assert result.returncode == 1
    assert "1 image build/upload operation(s) failed" in result.stderr
    assert "Failed:     1" in report.read_text()


def test_batch_script_rejects_push_without_a_visible_remote_manifest(tmp_path: Path) -> None:
    result, _docker_log, report = _run_script(tmp_path, manifest_always_missing=True)

    assert result.returncode == 1
    assert "1 image build/upload operation(s) failed" in result.stderr
    assert "Push failures: 1" in report.read_text()
