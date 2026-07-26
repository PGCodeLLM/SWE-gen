#!/usr/bin/env python3
"""Stage and launch the stable SWE-gen topology on two Slurm nodes.

The cluster does not provide a shared filesystem, so this launcher builds a
small per-node bundle, transfers it over ``srun`` stdin, bootstraps a node-local
virtual environment, and submits one exclusive batch allocation per node.
By default, each allocation starts six four-worker orchestrators (SG/HK/DE
twice), for 8+8+8 workers per node and 48 workers total. Setting
``--groups-per-route=1`` starts SG/HK/DE once per node, for 12 workers per node
and 24 workers total.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shlex
import shutil
import socket
import stat
import subprocess
import tarfile
import tempfile
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

from generate_orchestrator_shards import generate_shards

DEFAULT_NODES = (
    "ecs-z00579134-20260707-bugfix-0002",
    "ecs-z00579134-20260707-bugfix-0005",
)
NODE_IPS = {
    "ecs-z00579134-20260707-bugfix-0002": "7.244.3.78",
    "ecs-z00579134-20260707-bugfix-0005": "7.244.2.110",
    "ecs-z00579134-20260707-bugfix-0006": "7.244.1.209",
}
ROUTE_ENV_FILES = {"sg": ".env", "hk": ".env_hk", "de": ".env_de"}
ROUTES = tuple(ROUTE_ENV_FILES)
GROUP_SUFFIXES = ("a", "b")
DEFAULT_GROUPS_PER_ROUTE = 2
WORKERS_PER_SHARD = 4
GROUP_ORDER = tuple(
    (route, suffix) for suffix in GROUP_SUFFIXES[:DEFAULT_GROUPS_PER_ROUTE] for route in ROUTES
)
SHARDS_PER_NODE = len(GROUP_ORDER)
WORKERS_PER_NODE = WORKERS_PER_SHARD * SHARDS_PER_NODE
TOTAL_WORKERS = WORKERS_PER_NODE * 2
ENDPOINT = "https://arcyleung-ubuntu.tailb940e6.ts.net"
DEFAULT_REMOTE_ROOT = "/data/work/slurm-swegen/slurm-runtime"
SAFE_RUN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SECRET_RE = re.compile(
    r"(?i)(://)[^/@\s]+@|\bgithub_pat_[A-Za-z0-9_]{10,}|"
    r"\b(?:sk|ghp)_[A-Za-z0-9_-]{10,}|\bsk-[A-Za-z0-9_-]{10,}|"
    r"\bAuthorization[\"']?\s*[:=]\s*[\"']?Bearer\s+[A-Za-z0-9._~+/=-]+"
)
EXTRA_RUNTIME_FILES = (
    "src/slurm_two_node.py",
    "src/slurm_node_worker.sh",
    "src/slurm_collect.py",
)
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class NodeSpec:
    node: str
    node_ip: str
    index: int
    initial_delay_seconds: int
    expected_workers: int = WORKERS_PER_NODE


class SlurmLaunchError(RuntimeError):
    pass


def group_order(groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE) -> tuple[tuple[str, str], ...]:
    if groups_per_route not in range(1, len(GROUP_SUFFIXES) + 1):
        raise ValueError(f"groups per route must be between 1 and {len(GROUP_SUFFIXES)}")
    return tuple(
        (route, suffix) for suffix in GROUP_SUFFIXES[:groups_per_route] for route in ROUTES
    )


def workers_per_node(groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE) -> int:
    return len(group_order(groups_per_route)) * WORKERS_PER_SHARD


def total_workers(groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE) -> int:
    return workers_per_node(groups_per_route) * len(DEFAULT_NODES)


def manifest_name(groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE) -> str:
    return f"slurm-2n-{total_workers(groups_per_route)}w-manifest.json"


def redact(text: str) -> str:
    return SECRET_RE.sub(
        lambda match: f"{match.group(1)}<REDACTED>@" if match.group(1) else "<REDACTED>", text
    )


def private_swegen_config(workspace: Path) -> Path:
    """Return the required private token-pool config or fail closed."""
    path = workspace / "swegen.toml"
    if not path.is_file():
        raise FileNotFoundError(f"missing private SWE-gen config: {path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise PermissionError(
            f"private SWE-gen config must be mode 0600 (found {mode:04o}): {path}"
        )
    return path


def command_prefix() -> list[str]:
    if os.geteuid() == 0:
        return ["sudo", "-u", "alex", "-H"]
    return []


def validate_nodes(
    nodes: Iterable[str], groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE
) -> tuple[NodeSpec, NodeSpec]:
    values = tuple(nodes)
    if len(values) != 2:
        raise ValueError("exactly two Slurm nodes are required")
    if len(set(values)) != 2:
        raise ValueError("Slurm nodes must be unique")
    unknown = [node for node in values if node not in NODE_IPS]
    if unknown:
        raise ValueError(f"unknown Slurm node(s): {', '.join(unknown)}")
    expected_workers = workers_per_node(groups_per_route)
    return (
        NodeSpec(values[0], NODE_IPS[values[0]], 1, 0, expected_workers),
        NodeSpec(values[1], NODE_IPS[values[1]], 2, 30, expected_workers),
    )


def shard_names(
    nodes: Iterable[NodeSpec], groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE
) -> list[str]:
    names: list[str] = []
    for spec in nodes:
        for route, suffix in group_order(groups_per_route):
            names.append(f"r6-{route}-n{spec.index}-{suffix}")
    return names


def task_instance(repo: str, pull_number: object) -> str:
    return f"{repo.lower().replace('/', '__')}-{pull_number}"


def completed_instances(run_dir: Path) -> set[str]:
    """Return successful instances that still have a local task artifact."""
    ledger = run_dir / "create.jsonl"
    successful: set[str] = set()
    if ledger.is_file():
        with ledger.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                instance = record.get("task_id")
                if not isinstance(instance, str) or not instance:
                    harbor = record.get("harbor")
                    instance = Path(harbor).name if isinstance(harbor, str) else ""
                if instance:
                    successful.add(instance)

    artifact_roots = (run_dir / "tasks", run_dir / "tasks_voyager_postprocessed")
    return {
        instance
        for instance in successful
        if any((root / instance).is_dir() for root in artifact_roots)
    }


def write_remaining_source(source: Path, destination: Path, completed: set[str]) -> tuple[int, int]:
    total = 0
    remaining = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        source.open(encoding="utf-8", errors="replace") as src,
        destination.open("w", encoding="utf-8") as dst,
    ):
        for raw in src:
            if not raw.strip():
                continue
            total += 1
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at source line {total}: {exc}") from exc
            repo = record.get("repo")
            pull_number = record.get("pull_number")
            if not isinstance(repo, str) or pull_number is None:
                raise ValueError(f"source line {total} lacks repo/pull_number")
            if task_instance(repo, pull_number) in completed:
                continue
            dst.write(raw if raw.endswith("\n") else raw + "\n")
            remaining += 1
    return total, remaining


def prepare_shards(
    source: Path,
    run_dir: Path,
    shard_dir: Path,
    run_name: str,
    nodes: tuple[NodeSpec, NodeSpec],
    groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE,
) -> tuple[dict[str, Any], dict[str, int]]:
    done = completed_instances(run_dir)
    filtered = shard_dir / "remaining.jsonl"
    total, remaining = write_remaining_source(source, filtered, done)
    manifest = generate_shards(
        filtered,
        shard_dir,
        names=shard_names(nodes, groups_per_route),
        manifest_name=manifest_name(groups_per_route),
        run_name=run_name,
        workers=WORKERS_PER_SHARD,
    )
    return manifest, {"source_entries": total, "completed": len(done), "remaining": remaining}


def _tracked_files(workspace: Path) -> list[Path]:
    proc = subprocess.run(
        command_prefix() + ["git", "-C", str(workspace), "ls-files", "-z"],
        capture_output=True,
        check=True,
    )
    paths = {
        workspace / item.decode()
        for item in proc.stdout.split(b"\0")
        if item and (workspace / item.decode()).is_file()
    }
    for relative in EXTRA_RUNTIME_FILES:
        candidate = workspace / relative
        if candidate.is_file():
            paths.add(candidate)
    return sorted(paths)


def _add_file(tar: tarfile.TarFile, source: Path, arcname: str, mode: int | None = None) -> None:
    info = tar.gettarinfo(str(source), arcname=arcname)
    if mode is not None:
        info.mode = mode
    with source.open("rb") as fh:
        tar.addfile(info, fh)


def _add_bytes(tar: tarfile.TarFile, content: bytes, arcname: str, mode: int) -> None:
    info = tarfile.TarInfo(arcname)
    info.size = len(content)
    info.mode = mode
    info.mtime = int(datetime.now(UTC).timestamp())
    tar.addfile(info, io.BytesIO(content))


def _normalise_credential_profiles(
    credentials: Mapping[str, str] | Sequence[Mapping[str, str]] | None,
) -> list[dict[str, str]]:
    if credentials is None:
        return []
    raw_profiles: Sequence[Mapping[str, str]]
    if isinstance(credentials, Mapping):
        raw_profiles = [credentials]
    elif isinstance(credentials, Sequence) and not isinstance(credentials, (str, bytes)):
        if not credentials:
            raise ValueError("runtime credential profile list must not be empty")
        raw_profiles = credentials
    else:
        raise TypeError("runtime credentials must be a mapping, sequence of mappings, or None")

    profiles: list[dict[str, str]] = []
    for index, raw_profile in enumerate(raw_profiles):
        if not isinstance(raw_profile, Mapping):
            raise TypeError(f"runtime credential profile {index} is not a mapping")
        profile: dict[str, str] = {}
        for key, value in raw_profile.items():
            if not isinstance(key, str) or not ENV_NAME_RE.fullmatch(key):
                raise ValueError(f"invalid runtime credential environment name: {key!r}")
            if not isinstance(value, str) or not value or "\0" in value:
                raise ValueError(f"runtime credential profile {index} has invalid {key}")
            profile[key] = value
        profiles.append(profile)
    return profiles


def _render_credential_env(credentials: Mapping[str, str]) -> bytes:
    return "".join(
        f"export {key}={shlex.quote(value)}\n" for key, value in sorted(credentials.items())
    ).encode()


def load_runtime_credentials() -> tuple[dict[str, str], str]:
    """Load API credentials without writing them to the repository or argv."""

    def selected(values: dict[str, str]) -> dict[str, str]:
        result = {
            key: values.get(key, "")
            for key in (
                "OPENAI_API_KEY",
                "ANTHROPIC_API_KEY",
                "ANTHROPIC_AUTH_TOKEN",
            )
        }
        if not result["ANTHROPIC_API_KEY"]:
            result["ANTHROPIC_API_KEY"] = result["ANTHROPIC_AUTH_TOKEN"]
        if not result["ANTHROPIC_AUTH_TOKEN"]:
            result["ANTHROPIC_AUTH_TOKEN"] = result["ANTHROPIC_API_KEY"]
        return result

    current = selected(dict(os.environ))
    if all(current.values()):
        return current, "process-environment"

    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().decode(errors="replace")
            raw_environment = (entry / "environ").read_bytes().split(b"\0")
        except OSError:
            continue
        if "orchestrator.py" not in cmdline and "run_orchestrator.sh" not in cmdline:
            continue
        values: dict[str, str] = {}
        for item in raw_environment:
            if b"=" not in item:
                continue
            key, value = item.split(b"=", 1)
            name = key.decode(errors="ignore")
            if name in {
                "OPENAI_API_KEY",
                "ANTHROPIC_API_KEY",
                "ANTHROPIC_AUTH_TOKEN",
            }:
                values[name] = value.decode(errors="replace")
        credentials = selected(values)
        if all(credentials.values()):
            return credentials, f"local-worker-pid-{entry.name}"
    raise SlurmLaunchError(
        "API credentials are absent from the environment and no existing local "
        "SWE-gen worker exposes them"
    )


def _populate_bundle(
    bundle: Path,
    workspace: Path,
    node: NodeSpec,
    manifest: dict[str, Any],
    run_name: str,
    env_files: dict[str, Path],
    uv_bin: Path,
    proxy_ca: Path | None,
    credentials: Mapping[str, str] | Sequence[Mapping[str, str]] | None,
    groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE,
    expected_shards_per_node: int | None = None,
) -> None:
    swegen_config = private_swegen_config(workspace)
    credential_profiles = _normalise_credential_profiles(credentials)
    node_token = f"n{node.index}"
    selected_shards = [item for item in manifest["shards"] if f"-{node_token}-" in item["name"]]
    expected_shards = (
        expected_shards_per_node
        if expected_shards_per_node is not None
        else len(group_order(groups_per_route))
    )
    if expected_shards < 1:
        raise ValueError("expected shards per node must be positive")
    if len(selected_shards) != expected_shards:
        raise ValueError(f"expected {expected_shards} shards for {node.node}")

    with tarfile.open(bundle, "w:gz") as tar:
        for source in _tracked_files(workspace):
            _add_file(tar, source, str(source.relative_to(workspace)))
        manifest_path = Path(manifest["manifest_path"])
        _add_file(
            tar,
            manifest_path,
            str(manifest_path.relative_to(workspace)),
        )
        for shard in selected_shards:
            path = Path(shard["path"])
            _add_file(tar, path, str(path.relative_to(workspace)))
        for route, path in env_files.items():
            _add_file(tar, path, f".slurm-secrets/env/{ROUTE_ENV_FILES[route]}", 0o600)
        _add_file(tar, swegen_config, ".slurm-secrets/swegen.toml", 0o600)
        if credential_profiles:
            profile_names: list[str] = []
            for index, profile in enumerate(credential_profiles):
                profile_name = f"backend-{index:03d}.env"
                profile_names.append(profile_name)
                _add_bytes(
                    tar,
                    _render_credential_env(profile),
                    f".slurm-secrets/model-profiles/{profile_name}",
                    0o600,
                )
            # Keep the historical path as a fallback for old launchers and for
            # bootstrap/preflight code that needs one known-good profile.
            _add_bytes(
                tar,
                _render_credential_env(credential_profiles[0]),
                ".slurm-secrets/credentials.env",
                0o600,
            )
            _add_bytes(
                tar,
                ("\n".join(profile_names) + "\n").encode(),
                ".slurm-secrets/model-profiles/profiles.list",
                0o600,
            )
        _add_file(tar, uv_bin, "bootstrap/uv", 0o755)
        if proxy_ca is not None and proxy_ca.is_file():
            _add_file(tar, proxy_ca, ".slurm-secrets/ProxyCA260122.crt", 0o644)
            system_ca = Path("/etc/ssl/certs/ca-certificates.crt")
            if system_ca.is_file():
                combined_ca = system_ca.read_bytes() + b"\n" + proxy_ca.read_bytes()
                _add_bytes(tar, combined_ca, ".slurm-secrets/combined-ca.crt", 0o644)
        references = workspace / "runs" / run_name / "task_references.json"
        if references.is_file():
            _add_file(
                tar,
                references,
                f"runs/{run_name}/task_references.json",
                0o664,
            )


def build_bundle(
    workspace: Path,
    node: NodeSpec,
    manifest: dict[str, Any],
    run_name: str,
    env_files: dict[str, Path],
    uv_bin: Path,
    proxy_ca: Path | None,
    credentials: Mapping[str, str] | Sequence[Mapping[str, str]] | None,
    groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE,
    expected_shards_per_node: int | None = None,
) -> Path:
    """Build a private bundle on the workspace data disk and remove partials."""
    staging_dir = workspace / ".swegen-slurm-bundles"
    try:
        metadata = staging_dir.lstat()
    except FileNotFoundError:
        staging_dir.mkdir(mode=0o700)
        metadata = staging_dir.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SlurmLaunchError(f"bundle staging path is not a directory: {staging_dir}")
    staging_dir.chmod(0o700)
    if os.geteuid() == 0:
        owner = workspace.stat()
        os.chown(staging_dir, owner.st_uid, owner.st_gid)

    descriptor, raw_path = tempfile.mkstemp(
        prefix=f"swegen-{node.index}-",
        suffix=".tar.gz",
        dir=staging_dir,
    )
    os.close(descriptor)
    bundle = Path(raw_path)
    bundle.chmod(0o600)
    try:
        _populate_bundle(
            bundle,
            workspace,
            node,
            manifest,
            run_name,
            env_files,
            uv_bin,
            proxy_ca,
            credentials,
            groups_per_route,
            expected_shards_per_node,
        )
    except BaseException:
        bundle.unlink(missing_ok=True)
        raise
    return bundle


def _run(
    argv: list[str],
    *,
    stdin: BinaryIO | None = None,
    timeout: int = 1800,
) -> subprocess.CompletedProcess[bytes]:
    proc = subprocess.run(
        argv,
        stdin=stdin,
        capture_output=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        stdout = redact(proc.stdout.decode(errors="replace"))[-4000:]
        stderr = redact(proc.stderr.decode(errors="replace"))[-4000:]
        raise SlurmLaunchError(
            f"command failed ({proc.returncode}): {shlex.join(argv)}\n{stdout}\n{stderr}"
        )
    return proc


def _run_stage_command(
    argv: list[str],
    *,
    stdin_path: Path | None = None,
    timeout: int = 1800,
    max_ssh_attempts: int = 3,
) -> subprocess.CompletedProcess[bytes]:
    """Retry only transient SSH transport failures, reopening bundle stdin."""
    attempts = max_ssh_attempts if argv and argv[0] == "ssh" else 1
    for attempt in range(1, attempts + 1):
        try:
            if stdin_path is None:
                return _run(argv, timeout=timeout)
            with stdin_path.open("rb") as stream:
                return _run(argv, stdin=stream, timeout=timeout)
        except SlurmLaunchError as error:
            detail = str(error).lower()
            transient = any(
                marker in detail
                for marker in (
                    "command failed (255)",
                    "connection timed out",
                    "server not responding",
                    "connection reset by peer",
                    "broken pipe",
                )
            )
            if attempt >= attempts or not transient:
                raise
            time.sleep(min(2**attempt, 10))
    raise AssertionError("unreachable")


def remote_workspace(remote_root: str, run_name: str) -> str:
    return f"{remote_root.rstrip('/')}/{run_name}/workspace"


def stage_node(
    bundle: Path,
    node: NodeSpec,
    remote_path: str,
    run_name: str,
    shard_relative_dir: str,
    *,
    transport: str = "srun",
    skip_preflight: bool = False,
) -> str:
    if transport not in {"srun", "ssh"}:
        raise ValueError("stage transport must be srun or ssh")

    def remote_argv(command: str) -> list[str]:
        if transport == "ssh":
            if node.node == socket.gethostname():
                return command_prefix() + ["bash", "-lc", command]
            return [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=15",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=4",
                f"alex@{node.node_ip}",
                f"bash -lc {shlex.quote(command)}",
            ]
        return command_prefix() + [
            "srun",
            "--quiet",
            "--nodes=1",
            "--ntasks=1",
            f"--nodelist={node.node}",
            "bash",
            "-lc",
            command,
        ]

    extract = (
        "set -euo pipefail; umask 077; "
        f"mkdir -p {shlex.quote(remote_path)}; "
        f"find {shlex.quote(remote_path)} -maxdepth 1 -type f "
        "-name 'swegen.toml' -delete; "
        f"tar -xzf - -C {shlex.quote(remote_path)}; "
        f"profile_dir={shlex.quote(remote_path + '/.slurm-secrets/model-profiles')}; "
        'profile_list="$profile_dir/profiles.list"; '
        'if [[ -s "$profile_list" ]]; then '
        'while IFS= read -r -d "" profile_file; do '
        'grep -Fqx "$(basename "$profile_file")" "$profile_list" '
        '|| rm -f "$profile_file"; '
        'done < <(find "$profile_dir" -maxdepth 1 -type f '
        "-name 'backend-*.env' -print0); fi"
    )
    _run_stage_command(remote_argv(extract), stdin_path=bundle)

    preflight_command = (
        ": # preflight skipped by explicit operator request"
        if skip_preflight
        else (
            'bash src/slurm_node_worker.sh --preflight --workspace "$PWD" '
            f"--run-name {shlex.quote(run_name)} --node-index {node.index} "
            f"--shard-dir {shlex.quote(shard_relative_dir)}"
        )
    )
    bootstrap = f"""
set -euo pipefail
cd {shlex.quote(remote_path)}
mkdir -p .home .uv-cache runs/{shlex.quote(run_name)}
export HOME="$PWD/.home"
export UV_CACHE_DIR="$PWD/.uv-cache"
set -a
source .slurm-secrets/env/.env
set +a
# uv uses its own TLS stack and does not reliably reach pypi through the
# corporate TLS-interception proxy (curl-style proxy vars in .env aren't
# honored the same way), so a re-stage's `uv sync` times out fetching build
# deps like hatchling. The workspace venv + uv cache are already fully
# populated on every node from the initial launch, so resolve OFFLINE from the
# local cache (deterministic, no pypi). Point uv at the combined CA bundle too
# in case any object genuinely needs the network.
if [ -f .slurm-secrets/combined-ca.crt ]; then
  export SSL_CERT_FILE="$PWD/.slurm-secrets/combined-ca.crt"
  export UV_SYSTEM_CERTS=1
fi
./bootstrap/uv sync --frozen --no-dev --offline --python /usr/bin/python3.12 \
  || ./bootstrap/uv sync --frozen --no-dev --python /usr/bin/python3.12
{preflight_command}
"""
    proc = _run_stage_command(remote_argv(bootstrap), timeout=3600)
    return redact(proc.stdout.decode(errors="replace")).strip()


def submit_node(
    workspace: Path,
    node: NodeSpec,
    remote_path: str,
    run_name: str,
    shard_relative_dir: str,
    groups_per_route: int = DEFAULT_GROUPS_PER_ROUTE,
) -> str:
    group_order(groups_per_route)
    remote_output = f"{remote_path}/runs/{run_name}/slurm-%j.out"
    argv = command_prefix() + [
        "sbatch",
        "--parsable",
        f"--nodelist={node.node}",
        "--nodes=1",
        "--ntasks=1",
        "--exclusive",
        f"--job-name=swegen-{run_name}-n{node.index}",
        f"--output={remote_output}",
        "--export=NONE",
        str(workspace / "src" / "slurm_node_worker.sh"),
        "--workspace",
        remote_path,
        "--run-name",
        run_name,
        "--node-index",
        str(node.index),
        "--initial-delay",
        str(node.initial_delay_seconds),
        "--shard-dir",
        shard_relative_dir,
        "--groups-per-route",
        str(groups_per_route),
    ]
    proc = _run(argv, timeout=120)
    output = proc.stdout.decode(errors="replace").strip()
    job_id = output.split(";", 1)[0].strip()
    if not job_id.isdigit():
        raise SlurmLaunchError(f"could not parse sbatch job id from {output!r}")
    return job_id


def local_orchestrator_count(run_name: str) -> int:
    count = 0
    proc_root = Path("/proc")
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().decode(errors="replace")
        except OSError:
            continue
        if "src/orchestrator.py\x00" not in cmdline:
            continue
        if f"--run-name\x00{run_name}\x00" in cmdline:
            count += 1
    return count


def write_plan(path: Path, plan: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--nodes", nargs=2, default=list(DEFAULT_NODES))
    parser.add_argument("--action", choices=("plan", "stage", "submit"), default="plan")
    parser.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    parser.add_argument("--uv-bin", type=Path, default=Path(shutil.which("uv") or ""))
    parser.add_argument("--proxy-ca", type=Path, default=Path("/data/work/alex/ProxyCA260122.crt"))
    parser.add_argument("--allow-local-workers", action="store_true")
    parser.add_argument(
        "--groups-per-route",
        type=int,
        choices=range(1, len(GROUP_SUFFIXES) + 1),
        default=DEFAULT_GROUPS_PER_ROUTE,
        help="Four-worker orchestrator groups per proxy route and node (default: 2).",
    )
    parser.add_argument(
        "--reuse-remote-credentials",
        action="store_true",
        help="Retain the private credentials file from a prior successful stage.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    workspace = args.workspace.resolve()
    source = args.source.resolve()
    if not SAFE_RUN_RE.fullmatch(args.run_name):
        raise SystemExit("run name may contain only letters, numbers, dot, underscore, and dash")
    try:
        nodes = validate_nodes(args.nodes, args.groups_per_route)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not source.is_file():
        raise SystemExit(f"missing source JSONL: {source}")
    if not args.uv_bin.is_file():
        raise SystemExit(f"missing uv binary: {args.uv_bin}")
    if args.action != "plan":
        try:
            private_swegen_config(workspace)
        except (FileNotFoundError, PermissionError) as exc:
            raise SystemExit(str(exc)) from exc

    env_files = {route: workspace / filename for route, filename in ROUTE_ENV_FILES.items()}
    missing_env = [str(path) for path in env_files.values() if not path.is_file()]
    if missing_env:
        raise SystemExit(f"missing proxy environment file(s): {', '.join(missing_env)}")

    run_dir = workspace / "runs" / args.run_name
    shard_dir = workspace / "data_cache" / "orchestrator_shards" / f"slurm-{args.run_name}"
    manifest, input_counts = prepare_shards(
        source,
        run_dir,
        shard_dir,
        args.run_name,
        nodes,
        args.groups_per_route,
    )
    manifest_path = shard_dir / manifest_name(args.groups_per_route)
    manifest["manifest_path"] = str(manifest_path)
    shard_relative_dir = str(shard_dir.relative_to(workspace))

    plan: dict[str, Any] = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_name": args.run_name,
        "run_dir": str(run_dir),
        "endpoint": ENDPOINT,
        "expected_workers": total_workers(args.groups_per_route),
        "workers_per_node": workers_per_node(args.groups_per_route),
        "workers_per_shard": WORKERS_PER_SHARD,
        "groups_per_route": args.groups_per_route,
        "proxy_workers_per_node": dict.fromkeys(ROUTES, WORKERS_PER_SHARD * args.groups_per_route),
        "input": input_counts,
        "manifest": str(manifest_path),
        "nodes": [],
        "action": args.action,
    }
    for spec in nodes:
        remote_path = remote_workspace(args.remote_root, args.run_name)
        node_shards = [item for item in manifest["shards"] if f"-n{spec.index}-" in item["name"]]
        plan["nodes"].append(
            {
                **asdict(spec),
                "remote_workspace": remote_path,
                "remote_run_dir": f"{remote_path}/runs/{args.run_name}",
                "job_id": None,
                "shards": [item["name"] for item in node_shards],
            }
        )
    plan_path = run_dir / "slurm-plan.json"
    write_plan(plan_path, plan)
    print(
        f"Planned {len(manifest['shards'])} shards across two nodes: "
        f"{workers_per_node(args.groups_per_route)}+"
        f"{workers_per_node(args.groups_per_route)}="
        f"{total_workers(args.groups_per_route)} workers; "
        f"remaining input={input_counts['remaining']}",
        flush=True,
    )
    if args.action == "plan":
        print(f"Plan: {plan_path}", flush=True)
        return 0

    if args.action == "submit" and not args.allow_local_workers:
        local_count = local_orchestrator_count(args.run_name)
        if local_count:
            raise SystemExit(
                f"refusing to submit while {local_count} local orchestrator(s) for "
                f"{args.run_name} are active; stop them first to keep total concurrency at "
                f"{total_workers(args.groups_per_route)}"
            )

    credentials: dict[str, str] | None = None
    credential_source = "existing-remote-file"
    if not args.reuse_remote_credentials:
        credentials, credential_source = load_runtime_credentials()
    plan["credential_source"] = credential_source

    bundles: list[Path] = []
    try:
        for node_record, spec in zip(plan["nodes"], nodes, strict=True):
            bundle = build_bundle(
                workspace,
                spec,
                manifest,
                args.run_name,
                env_files,
                args.uv_bin.resolve(),
                args.proxy_ca.resolve() if args.proxy_ca.is_file() else None,
                credentials,
                args.groups_per_route,
            )
            bundles.append(bundle)
            output = stage_node(
                bundle,
                spec,
                node_record["remote_workspace"],
                args.run_name,
                shard_relative_dir,
            )
            node_record["stage"] = "ready"
            node_record["preflight"] = output.splitlines()[-12:]
            print(f"{spec.node} ({spec.node_ip}): staged and preflight passed", flush=True)

        if args.action == "submit":
            for node_record, spec in zip(plan["nodes"], nodes, strict=True):
                job_id = submit_node(
                    workspace,
                    spec,
                    node_record["remote_workspace"],
                    args.run_name,
                    shard_relative_dir,
                    args.groups_per_route,
                )
                node_record["job_id"] = job_id
                node_record["submitted_at"] = datetime.now(UTC).isoformat(timespec="seconds")
                print(f"{spec.node} ({spec.node_ip}): submitted job {job_id}", flush=True)
    finally:
        for bundle in bundles:
            bundle.unlink(missing_ok=True)

    plan["updated_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    write_plan(plan_path, plan)
    print(f"Plan: {plan_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
