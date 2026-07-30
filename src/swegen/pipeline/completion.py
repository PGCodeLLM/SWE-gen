"""Successful-task export and ``public.completed_tasks`` persistence."""

from __future__ import annotations

import csv
import os
import re
import shutil
import socket
from dataclasses import dataclass
from pathlib import Path

from swegen import db
from swegen.model_settings import load_completed_task_settings
from swegen.pipeline.models import PipelineTask

_TABLE_HEADER_RE = re.compile(r"^\s*\[([^]]+)]\s*(?:#.*)?$")
_DOCKER_IMAGE_RE = re.compile(r"^\s*docker_image\s*=")


@dataclass(frozen=True, slots=True)
class CompletedTaskExport:
    directory: Path
    dockerfile_path: Path
    config_path: Path
    test_path: Path


def _read_required_text(path: Path) -> str:
    if not path.is_file():
        raise RuntimeError(f"required Harbor task file is missing: {path}")
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise RuntimeError(f"could not read Harbor task file {path}: {error}") from error


def _toml_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def append_cwm_metadata(
    config_text: str,
    task: PipelineTask,
    *,
    repo_full_name: str,
    base_commit: str,
    issue_number: str | None,
) -> str:
    if re.search(r"(?m)^\s*\[cwm_task_metadata]\s*$", config_text):
        return config_text
    lines = [
        "[cwm_task_metadata]",
        f"repo_full_name = {_toml_quote(repo_full_name)}",
        f"pr_id = {_toml_quote(str(task.pr))}",
    ]
    if issue_number:
        lines.append(f"issue_number = {_toml_quote(issue_number)}")
    lines.extend(
        [
            'training_domain = "feature"',
            f"source_commit = {_toml_quote(base_commit)}",
        ]
    )
    separator = "\n" if config_text.endswith("\n") else "\n\n"
    return config_text + separator + "\n".join(lines) + "\n"


def set_environment_docker_image(config_text: str, image_ref: str) -> str:
    lines = config_text.splitlines(keepends=True)
    environment_start: int | None = None
    environment_end = len(lines)
    for index, line in enumerate(lines):
        match = _TABLE_HEADER_RE.match(line.rstrip("\r\n"))
        if match is None:
            continue
        if match.group(1).strip() == "environment":
            environment_start = index
            continue
        if environment_start is not None:
            environment_end = index
            break
    if environment_start is None:
        suffix = "" if config_text.endswith("\n") else "\n"
        return (
            config_text
            + suffix
            + "\n[environment]\n"
            + f"docker_image = {_toml_quote(image_ref)}\n"
        )

    replacement = f"docker_image = {_toml_quote(image_ref)}\n"
    for index in range(environment_start + 1, environment_end):
        if _DOCKER_IMAGE_RE.match(lines[index]):
            newline = "\r\n" if lines[index].endswith("\r\n") else "\n"
            lines[index] = replacement.rstrip("\n") + newline
            return "".join(lines)
    lines.insert(environment_end, replacement)
    return "".join(lines)


def _public_metadata(task: PipelineTask) -> tuple[str, str, str | None]:
    row = db.query_one(
        "SELECT source.repo AS repo_full_name, "
        "COALESCE(source.base_commit, '') AS base_commit, "
        "NULLIF(to_jsonb(source)->>'issue_number', '') AS issue_number "
        "FROM public.pr_tasks AS source "
        "WHERE LOWER(source.repo) = %s AND source.pull_number = %s "
        "ORDER BY source.source_table LIMIT 1",
        (task.repo.lower(), task.pr),
    )
    if row is None:
        raise RuntimeError(f"public.pr_tasks has no metadata for {task.repo} PR {task.pr}")
    repo_full_name = str(row.get("repo_full_name") or "").strip()
    base_commit = str(row.get("base_commit") or "").strip()
    if not repo_full_name or not base_commit:
        raise RuntimeError(f"public.pr_tasks metadata is incomplete for {task.repo} PR {task.pr}")
    return (
        repo_full_name,
        base_commit,
        (str(row["issue_number"]) if row.get("issue_number") is not None else None),
    )


def _config_path(task_dir: Path) -> Path:
    explicit = task_dir / "config.toml"
    return explicit if explicit.is_file() else task_dir / "task.toml"


def export_completed_task(
    task: PipelineTask,
    task_dir: Path,
    *,
    voyager_image_ref: str,
    minddistiller_image_ref: str,
) -> CompletedTaskExport:
    """Dump a task, persist all three variants, then delete exactly three files."""

    dockerfile_source = task_dir / "environment" / "Dockerfile"
    config_source = _config_path(task_dir)
    test_source = task_dir / "tests" / "test.sh"
    dockerfile_original = _read_required_text(dockerfile_source)
    config_original = _read_required_text(config_source)
    test_original = _read_required_text(test_source)

    output_root = load_completed_task_settings().output_dir.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / task.task_id
    # Never remove or replace a pre-existing directory.  Merge the authoritative
    # task files into it so retries are idempotent and no unrelated file is lost.
    shutil.copytree(task_dir, destination, dirs_exist_ok=True)

    dumped_dockerfile = destination / "environment" / "Dockerfile"
    dumped_config = destination / config_source.relative_to(task_dir)
    dumped_test = destination / "tests" / "test.sh"
    repo_full_name, base_commit, issue_number = _public_metadata(task)
    config_voyager = append_cwm_metadata(
        config_original,
        task,
        repo_full_name=repo_full_name,
        base_commit=base_commit,
        issue_number=issue_number,
    )
    config_minddistiller = set_environment_docker_image(
        config_original,
        minddistiller_image_ref,
    )

    with db.connection() as connection:
        with connection.transaction():
            connection.execute(
                "INSERT INTO public.completed_tasks ("
                "instance_id, harbor_directory_hostname, harbor_directory_filepath, "
                "dockerfile_original, test_sh_original, config_toml_original, "
                "dockerfile_minddistiller, test_sh_minddistiller, config_toml_minddistiller, "
                "dockerfile_voyager, test_sh_voyager, config_toml_voyager"
                ") VALUES ("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
                ") ON CONFLICT (instance_id) DO UPDATE SET "
                "harbor_directory_hostname = EXCLUDED.harbor_directory_hostname, "
                "harbor_directory_filepath = EXCLUDED.harbor_directory_filepath, "
                "dockerfile_original = EXCLUDED.dockerfile_original, "
                "test_sh_original = EXCLUDED.test_sh_original, "
                "config_toml_original = EXCLUDED.config_toml_original, "
                "dockerfile_minddistiller = EXCLUDED.dockerfile_minddistiller, "
                "test_sh_minddistiller = EXCLUDED.test_sh_minddistiller, "
                "config_toml_minddistiller = EXCLUDED.config_toml_minddistiller, "
                "dockerfile_voyager = EXCLUDED.dockerfile_voyager, "
                "test_sh_voyager = EXCLUDED.test_sh_voyager, "
                "config_toml_voyager = EXCLUDED.config_toml_voyager",
                (
                    task.task_id,
                    os.environ.get("NODE_NAME", "").strip() or socket.gethostname(),
                    str(destination),
                    dockerfile_original,
                    test_original,
                    config_original,
                    dockerfile_original,
                    test_original,
                    config_minddistiller,
                    f"FROM {voyager_image_ref}",
                    test_original,
                    config_voyager,
                ),
            )

    # The requested disk cleanup is deliberately explicit.  Do not replace this
    # with directory cleanup: every other dumped task artifact must remain.
    dumped_dockerfile.unlink()
    dumped_config.unlink()
    dumped_test.unlink()
    return CompletedTaskExport(destination, dumped_dockerfile, dumped_config, dumped_test)


def load_minddistiller_login(path: Path, *, expected_host: str) -> tuple[str, str]:
    """Load username/password from the supplied Chinese-labelled credential CSV."""

    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = {row[0].strip(): row[1] for row in csv.reader(handle) if len(row) >= 2}
    except OSError as error:
        raise RuntimeError(f"could not read MindDistiller SWR credentials: {error}") from error
    username = rows.get("用户名", "").strip()
    password = rows.get("密码", "")
    login_command = rows.get("镜像访问凭证", "")
    if not username or not password:
        raise RuntimeError("MindDistiller SWR credential CSV is missing 用户名 or 密码")
    if expected_host not in login_command:
        raise RuntimeError(
            "MindDistiller credential CSV login host does not match [swr.minddistiller].host"
        )
    return username, password


__all__ = [
    "CompletedTaskExport",
    "append_cwm_metadata",
    "export_completed_task",
    "load_minddistiller_login",
    "set_environment_docker_image",
]
