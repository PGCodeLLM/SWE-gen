#!/usr/bin/env python3
"""Reconcile a YAML Stage-I policy with the four-node Slurm allocation.

The web UI owns the desired configuration. This controller is deliberately a
separate process: it polls that small YAML file, uses ``scontrol`` for
non-destructive pause/resume, and performs a collect/cancel/relaunch cycle only
when the requested per-route concurrency changes or an allocation disappears.

The global API circuit breaker counts current-revision Stage-I failures across
all collected node journals. On trip it suspends the jobs and atomically writes
``desired_state: paused``. The latch is cleared only by an explicit later UI
reset marker, with the configured cooldown suppressing an immediate re-trip
from the same failure window.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from slurm_collect import job_state as collected_job_state
from slurm_two_node import redact

DEFAULT_WORKSPACE = Path("/data/work/slurm-swegen")
DEFAULT_CONFIG_PATH = Path("/data/work/alex/SWE-gen/swegen-config.yaml")
DEFAULT_RUN_NAME = "20260716-sol-max-full-16w"
DEFAULT_REVISION = "r9"
DEFAULT_INPUT = DEFAULT_WORKSPACE / "data_cache/pr_tasks_export_ts_js_12122_removed.jsonl"
DEFAULT_MODELS = Path("/data/work/alex/SWE-gen/models.yaml")
DEFAULT_CANDIDATE_REMOTE_ROOT = Path("/data/work/slurm-swegen/slurm-candidate-runtime")
DEFAULT_OPUS_MODEL = "gpt-5.6-sol"
DEFAULT_SONNET_MODEL = "gpt-5.6-terra"
ROUTES = ("sg", "hk", "de")
RUNNING_STATES = {"RUNNING", "CONFIGURING", "COMPLETING"}
SUSPENDED_STATES = {"SUSPENDED"}
PENDING_STATES = {"PENDING"}
LIVE_STATES = RUNNING_STATES | SUSPENDED_STATES | PENDING_STATES
UNCERTAIN_STATES = {"UNKNOWN"}
API_FAILURE_REASONS = {"Transient network/API error"}
STAGE_WORKER_SERVICES = (
    "swegen-baseline-validation.service",
    "swegen-reward-backfill.service",
)


class ConfigError(ValueError):
    """The desired control document is absent or unsafe to apply."""


class ReconcileError(RuntimeError):
    """The desired topology could not be applied safely."""


def now_iso(now: datetime | None = None) -> str:
    value = now or datetime.now(UTC)
    return value.astimezone(UTC).isoformat(timespec="seconds")


def parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _integer(value: object, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{name} must be an integer")
    if value < minimum or value > maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return value


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{name} must be a mapping")
    return value


def _path(value: object, name: str, base: Path) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty path")
    path = Path(value).expanduser()
    return (path if path.is_absolute() else base / path).resolve()


@dataclass(frozen=True)
class RunConfig:
    name: str
    directory: Path
    input_jsonl: Path
    models_yaml: Path
    plan_path: Path
    revision: str


@dataclass(frozen=True)
class SlurmConfig:
    node_count: int
    routes: dict[str, int]

    @property
    def workers_per_node(self) -> int:
        return sum(self.routes.values())

    @property
    def total_workers(self) -> int:
        return self.node_count * self.workers_per_node


@dataclass(frozen=True)
class ModelsConfig:
    opus: str
    sonnet: str

    def as_dict(self) -> dict[str, str]:
        return {"opus": self.opus, "sonnet": self.sonnet}


@dataclass(frozen=True)
class ControllerConfig:
    poll_interval_seconds: int
    status_path: Path


@dataclass(frozen=True)
class CircuitBreakerConfig:
    enabled: bool
    window_seconds: int
    failure_threshold: int
    cooldown_seconds: int


@dataclass(frozen=True)
class ControlConfig:
    path: Path
    desired_state: str
    run: RunConfig
    models: ModelsConfig
    slurm: SlurmConfig
    controller: ControllerConfig
    circuit_breaker: CircuitBreakerConfig
    metadata: dict[str, Any]


def default_config_document(workspace: Path = DEFAULT_WORKSPACE) -> dict[str, Any]:
    run_dir = workspace / "runs" / DEFAULT_RUN_NAME
    return {
        "version": 1,
        "desired_state": "running",
        "run": {
            "name": DEFAULT_RUN_NAME,
            "dir": str(run_dir),
            "input_jsonl": str(DEFAULT_INPUT),
            "models_yaml": str(DEFAULT_MODELS),
            "plan_path": str(run_dir / f"slurm-stage1-{DEFAULT_REVISION}-4n-plan.json"),
            "revision": DEFAULT_REVISION,
        },
        "models": {
            "opus": DEFAULT_OPUS_MODEL,
            "sonnet": DEFAULT_SONNET_MODEL,
        },
        "slurm": {
            "node_count": 4,
            "routes": {"sg": 4, "hk": 4, "de": 4},
        },
        "controller": {
            "poll_interval_seconds": 15,
            "status_path": str(run_dir / ".slurm-control" / "status.json"),
        },
        "circuit_breaker": {
            "enabled": True,
            "window_seconds": 300,
            "failure_threshold": 10,
            "cooldown_seconds": 900,
        },
        "metadata": {
            "updated_at": now_iso(),
            "updated_by": "initial-default",
        },
    }


@contextmanager
def config_write_lock(path: Path):
    """Serialize dashboard and circuit-breaker read/modify/write cycles."""
    lock_path = path.with_name(path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        lock_path.chmod(0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _atomic_yaml_unlocked(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    owner = path if path.exists() else path.parent
    owner_stat = owner.stat()
    mode = 0o600
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(yaml.safe_dump(dict(value), sort_keys=False), encoding="utf-8")
    temporary.chmod(mode)
    if os.geteuid() == 0:
        os.chown(temporary, owner_stat.st_uid, owner_stat.st_gid)
    os.replace(temporary, path)


def atomic_yaml(path: Path, value: Mapping[str, Any]) -> None:
    with config_write_lock(path):
        _atomic_yaml_unlocked(path, value)


def ensure_default_config(path: Path, workspace: Path = DEFAULT_WORKSPACE) -> bool:
    """Create a safe 4+4+4 running policy only when no document exists."""
    with config_write_lock(path):
        if path.exists():
            return False
        _atomic_yaml_unlocked(path, default_config_document(workspace))
        return True


def load_config(path: Path) -> ControlConfig:
    if not path.is_file():
        raise ConfigError(f"missing SWE-gen control config: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ConfigError(f"could not read control config: {error}") from error
    root = _mapping(raw, "config")
    if root.get("version") != 1:
        raise ConfigError("config.version must be 1")
    desired_state = root.get("desired_state")
    if desired_state not in {"running", "paused"}:
        raise ConfigError("desired_state must be running or paused")

    base = path.parent.resolve()
    run = _mapping(root.get("run"), "run")
    name = run.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ConfigError("run.name must be non-empty")
    revision = run.get("revision")
    if not isinstance(revision, str) or not revision.startswith("r") or not revision[1:].isdigit():
        raise ConfigError("run.revision must look like r9")
    run_config = RunConfig(
        name=name.strip(),
        directory=_path(run.get("dir"), "run.dir", base),
        input_jsonl=_path(run.get("input_jsonl"), "run.input_jsonl", base),
        models_yaml=_path(run.get("models_yaml"), "run.models_yaml", base),
        plan_path=_path(run.get("plan_path"), "run.plan_path", base),
        revision=revision,
    )

    models = root.get("models")
    if models is None:
        # Keep existing control documents working while the WebUI migrates the
        # new role selectors into the public desired-state schema.
        model_values: Mapping[str, Any] = {
            "opus": DEFAULT_OPUS_MODEL,
            "sonnet": DEFAULT_SONNET_MODEL,
        }
    else:
        model_values = _mapping(models, "models")

    def model_name(role: str) -> str:
        value = model_values.get(role)
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(f"models.{role} must be a non-empty model_name")
        result = value.strip()
        if len(result) > 256 or any(ord(character) < 32 for character in result):
            raise ConfigError(f"models.{role} contains an invalid model_name")
        return result

    models_config = ModelsConfig(opus=model_name("opus"), sonnet=model_name("sonnet"))

    slurm = _mapping(root.get("slurm"), "slurm")
    node_count = _integer(slurm.get("node_count"), "slurm.node_count", 1, 4)
    if node_count != 4:
        raise ConfigError("the current launcher requires slurm.node_count to be 4")
    route_values = _mapping(slurm.get("routes"), "slurm.routes")
    routes = {
        route: _integer(route_values.get(route), f"slurm.routes.{route}", 0, 16) for route in ROUTES
    }
    if desired_state == "running" and not any(routes.values()):
        raise ConfigError("at least one Slurm route must have positive concurrency")
    slurm_config = SlurmConfig(node_count=node_count, routes=routes)

    controller = _mapping(root.get("controller"), "controller")
    controller_config = ControllerConfig(
        poll_interval_seconds=_integer(
            controller.get("poll_interval_seconds"),
            "controller.poll_interval_seconds",
            1,
            3600,
        ),
        status_path=_path(controller.get("status_path"), "controller.status_path", base),
    )

    breaker = _mapping(root.get("circuit_breaker"), "circuit_breaker")
    enabled = breaker.get("enabled")
    if not isinstance(enabled, bool):
        raise ConfigError("circuit_breaker.enabled must be true or false")
    breaker_config = CircuitBreakerConfig(
        enabled=enabled,
        window_seconds=_integer(
            breaker.get("window_seconds"), "circuit_breaker.window_seconds", 1, 86400
        ),
        failure_threshold=_integer(
            breaker.get("failure_threshold"), "circuit_breaker.failure_threshold", 1, 1000000
        ),
        cooldown_seconds=_integer(
            breaker.get("cooldown_seconds"),
            "circuit_breaker.cooldown_seconds",
            0,
            604800,
        ),
    )
    metadata = root.get("metadata")
    return ControlConfig(
        path=path.resolve(),
        desired_state=desired_state,
        run=run_config,
        models=models_config,
        slurm=slurm_config,
        controller=controller_config,
        circuit_breaker=breaker_config,
        metadata=dict(metadata) if isinstance(metadata, Mapping) else {},
    )


def update_desired_state(
    path: Path,
    desired_state: str,
    updated_by: str,
    reason: str,
    *,
    updated_at: datetime | None = None,
) -> None:
    if desired_state not in {"running", "paused"}:
        raise ValueError("invalid desired state")
    with config_write_lock(path):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        root = dict(_mapping(raw, "config"))
        root["desired_state"] = desired_state
        metadata = dict(root.get("metadata")) if isinstance(root.get("metadata"), Mapping) else {}
        metadata.update(
            {"updated_at": now_iso(updated_at), "updated_by": updated_by, "reason": reason}
        )
        root["metadata"] = metadata
        _atomic_yaml_unlocked(path, root)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    owner = path if path.exists() else path.parent
    owner_stat = owner.stat()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(dict(value), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.chmod(0o644)
    if os.geteuid() == 0:
        os.chown(temporary, owner_stat.st_uid, owner_stat.st_gid)
    os.replace(temporary, path)


def read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def load_plan(path: Path) -> dict[str, Any]:
    value = read_json(path)
    if not isinstance(value.get("nodes"), list):
        raise ReconcileError(f"invalid or missing Slurm plan: {path}")
    return value


def plan_routes(plan: Mapping[str, Any]) -> dict[str, int]:
    value = plan.get("proxy_workers_per_node")
    if not isinstance(value, Mapping):
        return {}
    try:
        return {route: int(value.get(route, -1)) for route in ROUTES}
    except (TypeError, ValueError):
        return {}


def plan_models(plan: Mapping[str, Any]) -> dict[str, str]:
    backend = plan.get("model_backend")
    if not isinstance(backend, Mapping):
        return {}

    def normalized_roles(value: object) -> dict[str, str] | None:
        if not isinstance(value, Mapping):
            return None
        opus = value.get("opus")
        sonnet = value.get("sonnet")
        if isinstance(opus, str) and opus and isinstance(sonnet, str) and sonnet:
            return {"opus": opus, "sonnet": sonnet}
        return None

    top_level_roles = normalized_roles(backend.get("roles"))
    profile_roles: dict[str, str] | None = None
    profiles = backend.get("profiles")
    if isinstance(profiles, list) and profiles:
        for profile in profiles:
            if not isinstance(profile, Mapping):
                return {}
            current = normalized_roles(profile.get("roles"))
            if current is None:
                return {}
            if profile_roles is None:
                profile_roles = current
            elif current != profile_roles:
                return {}
        if top_level_roles is not None and profile_roles != top_level_roles:
            return {}

    resolved_roles = top_level_roles or profile_roles
    if resolved_roles is not None:
        return resolved_roles

    # Plans created before role-aware configuration recorded only the two
    # default model names. Recognize that exact legacy pair so deploying the
    # controller itself does not needlessly replace healthy allocations.
    legacy = backend.get("models")
    if isinstance(legacy, list) and {item for item in legacy if isinstance(item, str)} == {
        DEFAULT_OPUS_MODEL,
        DEFAULT_SONNET_MODEL,
    }:
        return {"opus": DEFAULT_OPUS_MODEL, "sonnet": DEFAULT_SONNET_MODEL}
    return {}


def model_config_sha256(path: Path) -> str:
    """Return the private model catalog fingerprint used by staged workers."""
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReconcileError(f"could not fingerprint model config {path}: {error}") from error
    return digest.hexdigest()


def plan_model_config_sha256(plan: Mapping[str, Any]) -> str | None:
    backend = plan.get("model_backend")
    if not isinstance(backend, Mapping):
        return None
    value = backend.get("config_sha256")
    return value if isinstance(value, str) and len(value) == 64 else None


def _public_endpoint(value: object) -> str | None:
    """Normalize an endpoint for status output without userinfo or query data."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = urlsplit(value.strip())
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or hostname is None:
        return None
    host = f"[{hostname}]" if ":" in hostname else hostname
    authority = f"{host}:{port}" if port is not None else host
    return f"{parsed.scheme}://{authority}"


def model_config_endpoints(path: Path, model_names: set[str]) -> list[str]:
    """Return credential-free endpoint alternatives for the selected roles."""
    try:
        root = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ReconcileError(f"could not inspect model config {path}: {error}") from error
    if not isinstance(root, Mapping) or not isinstance(root.get("model_list"), list):
        raise ReconcileError(f"model config must contain model_list: {path}")
    endpoints: list[str] = []
    for entry in root["model_list"]:
        if not isinstance(entry, Mapping) or entry.get("model_name") not in model_names:
            continue
        params = entry.get("litellm_params")
        endpoint = _public_endpoint(params.get("api_base")) if isinstance(params, Mapping) else None
        if endpoint is not None and endpoint not in endpoints:
            endpoints.append(endpoint)
    return endpoints


def plan_model_endpoints(plan: Mapping[str, Any]) -> list[str]:
    """Return credential-free endpoint roots from new and legacy plans."""
    backend = plan.get("model_backend")
    if not isinstance(backend, Mapping):
        return []
    endpoints: list[str] = []
    advertised = backend.get("endpoints")
    if isinstance(advertised, list):
        for value in advertised:
            endpoint = _public_endpoint(value)
            if endpoint is not None and endpoint not in endpoints:
                endpoints.append(endpoint)
    profiles = backend.get("profiles")
    if isinstance(profiles, list):
        for profile in profiles:
            if not isinstance(profile, Mapping):
                continue
            endpoint = _public_endpoint(profile.get("api_base"))
            if endpoint is not None and endpoint not in endpoints:
                endpoints.append(endpoint)
    legacy = backend.get("api_base")
    endpoint = _public_endpoint(legacy)
    if not endpoints and endpoint is not None:
        endpoints.append(endpoint)
    return endpoints


@dataclass(frozen=True)
class JobObservation:
    node: str
    job_id: str | None
    state: str


class SlurmClient:
    """Small command boundary so reconciliation is deterministic in tests."""

    def __init__(self) -> None:
        self.prefix = ["sudo", "-u", "alex", "-H"] if os.geteuid() == 0 else []

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        timeout: int = 3600,
    ) -> subprocess.CompletedProcess[str]:
        command = self.prefix + list(argv)
        result = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            detail = redact((result.stdout + "\n" + result.stderr)[-4000:])
            raise ReconcileError(
                f"command failed ({result.returncode}): {' '.join(argv)}\n{detail}"
            )
        return result

    def run_privileged(
        self,
        argv: Sequence[str],
        *,
        timeout: int = 60,
    ) -> subprocess.CompletedProcess[str]:
        if os.geteuid() != 0:
            raise ReconcileError(
                "Slurm pause/resume requires the controller to run as root"
            )
        result = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            detail = redact((result.stdout + "\n" + result.stderr)[-4000:])
            raise ReconcileError(
                f"command failed ({result.returncode}): {' '.join(argv)}\n{detail}"
            )
        return result

    def job_state(self, job_id: str | None) -> str:
        return collected_job_state(job_id)

    def suspend(self, job_ids: Sequence[str]) -> None:
        for job_id in job_ids:
            self.run_privileged(["scontrol", "suspend", job_id], timeout=60)

    def resume(self, job_ids: Sequence[str]) -> None:
        for job_id in job_ids:
            self.run_privileged(["scontrol", "resume", job_id], timeout=60)

    def hold(self, job_ids: Sequence[str]) -> None:
        for job_id in job_ids:
            self.run_privileged(["scontrol", "hold", job_id], timeout=60)

    def release(self, job_ids: Sequence[str]) -> None:
        for job_id in job_ids:
            self.run_privileged(["scontrol", "release", job_id], timeout=60)

    def cancel(self, job_ids: Sequence[str]) -> None:
        for job_id in job_ids:
            self.run_privileged(["scancel", job_id], timeout=60)


class ServiceManager:
    """Stop and start Stage II/III systemd services so the circuit breaker
    does not leave validation and reward workers burning compute/API credits
    while Stage I is suspended."""

    def stop(self, services: Sequence[str]) -> list[str]:
        """Stop the named services.  Return the list actually stopped."""
        ...

    def start(self, services: Sequence[str]) -> list[str]:
        """Start the named services.  Return the list actually started."""
        ...

    def is_active(self, service: str) -> bool:
        """Return True if the service is currently running."""
        ...


class SystemdServiceManager(ServiceManager):
    """Concrete implementation that shells out to ``systemctl``."""

    def _run(self, verb: str, services: Sequence[str]) -> list[str]:
        if not services:
            return []
        argv = ["systemctl", verb, *services]
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=60,
        )
        # systemctl returns 0 on success; non-zero is fine for stop (already
        # stopped) and we don't want to block reconciliation.
        return services if result.returncode == 0 else []

    def stop(self, services: Sequence[str]) -> list[str]:
        return self._run("stop", services)

    def start(self, services: Sequence[str]) -> list[str]:
        return self._run("start", services)

    def is_active(self, service: str) -> bool:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", service],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0


def observe_jobs(plan: Mapping[str, Any], client: SlurmClient) -> list[JobObservation]:
    observations: list[JobObservation] = []
    for record in plan.get("nodes", []):
        if not isinstance(record, Mapping):
            continue
        job_id = str(record["job_id"]) if record.get("job_id") else None
        observations.append(
            JobObservation(
                node=str(record.get("node") or "unknown"),
                job_id=job_id,
                state=client.job_state(job_id).split("+", 1)[0].upper(),
            )
        )
    return observations


def api_failures_in_window(
    run_dir: Path,
    revision: str,
    *,
    window_seconds: int,
    now: datetime,
    plan: Mapping[str, Any] | None = None,
) -> tuple[int, list[dict[str, str]]]:
    cutoff = now.astimezone(UTC).timestamp() - window_seconds
    matches: list[dict[str, str]] = []
    paths: list[Path] = []
    if plan is not None:
        for node in plan.get("nodes", []):
            if not isinstance(node, Mapping):
                continue
            node_name = str(node.get("node") or "")
            index = node.get("index")
            shards = node.get("shards")
            if not isinstance(shards, list):
                shards = []
                for route, workers in plan_routes(plan).items():
                    if workers > 0:
                        shards.append(f"{revision}-{route}-n{index}-a")
                    if workers > 4:
                        shards.append(f"{revision}-{route}-n{index}-b")
            for shard in shards:
                if not isinstance(shard, str) or not shard.startswith(f"{revision}-"):
                    continue
                suffix = shard.removeprefix(f"{revision}-")
                filename = f"orchestrator-instance-status-{suffix}-{revision}.jsonl"
                node_path = run_dir / "slurm-nodes" / node_name / filename
                root_path = run_dir / filename
                if node_path.is_file():
                    paths.append(node_path)
                elif root_path.is_file():
                    paths.append(root_path)
    else:
        paths = [
            *run_dir.glob(f"orchestrator-instance-status-*-{revision}.jsonl"),
            *run_dir.glob(f"slurm-nodes/*/orchestrator-instance-status-*-{revision}.jsonl"),
        ]
    seen: set[tuple[object, ...]] = set()
    for path in sorted(set(paths)):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            if record.get("status") != "failure":
                continue
            reason = record.get("failure_reason")
            if reason not in API_FAILURE_REASONS:
                continue
            timestamp = parse_timestamp(record.get("timestamp"))
            if timestamp is None or timestamp.timestamp() < cutoff:
                continue
            identity = (
                record.get("timestamp"),
                record.get("instance"),
                record.get("slurm_group"),
                record.get("worker_id"),
                reason,
            )
            if identity in seen:
                continue
            seen.add(identity)
            matches.append(
                {
                    "timestamp": now_iso(timestamp),
                    "instance": str(record.get("instance") or ""),
                    "node": str(record.get("slurm_node") or path.parent.name),
                    "route": str(record.get("slurm_route") or ""),
                }
            )
    matches.sort(key=lambda item: item["timestamp"])
    return len(matches), matches[-20:]


def observed_cluster_state(jobs: Sequence[JobObservation], held_job_ids: Sequence[str] = ()) -> str:
    if not jobs or not any(job.job_id for job in jobs):
        return "stopped"
    held = set(held_job_ids)
    submitted = [job for job in jobs if job.job_id]
    if submitted and all(
        job.state in SUSPENDED_STATES or (job.state in PENDING_STATES and job.job_id in held)
        for job in submitted
    ):
        return "paused"
    states = {job.state for job in jobs if job.job_id}
    if states and states <= SUSPENDED_STATES:
        return "paused"
    if states and states <= RUNNING_STATES:
        return "running"
    if states and states <= PENDING_STATES:
        return "starting"
    if states & LIVE_STATES:
        return "mixed"
    if states & UNCERTAIN_STATES:
        return "unknown"
    return "stopped"


class Reconciler:
    def __init__(
        self,
        config_path: Path = DEFAULT_CONFIG_PATH,
        workspace: Path = DEFAULT_WORKSPACE,
        *,
        client: SlurmClient | None = None,
        now_fn: Callable[[], datetime] | None = None,
        collect_action: Callable[[Path], Mapping[str, Any]] | None = None,
        health_action: Callable[[Path, int], Mapping[str, Any]] | None = None,
        launch_action: Callable[[ControlConfig, str, Path | None], Path] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        service_manager: ServiceManager | None = None,
        skip_preflight: bool = False,
    ) -> None:
        self.config_path = config_path.resolve()
        self.workspace = workspace.resolve()
        self.client = client or SlurmClient()
        self.now_fn = now_fn or (lambda: datetime.now(UTC))
        self.collect_action = collect_action or self._collect
        self.health_action = health_action or self._health
        self.launch_action = launch_action or self._launch
        self.sleep_fn = sleep_fn
        self.service_manager = service_manager or SystemdServiceManager()
        # When set, stage runtime bundles without the (slow) network/Docker/quota
        # preflight builds and accept the resulting "skipped" preflight evidence.
        # Used to relaunch quickly when node preflight is contended or trusted.
        self.skip_preflight = skip_preflight

    def _collect(self, plan_path: Path) -> Mapping[str, Any]:
        health = self._run_collector(plan_path, include_tasks=True)
        errors = [
            item
            for item in health.get("nodes", [])
            if isinstance(item, Mapping)
            and item.get("job_id")
            and str(item.get("state") or "").upper() not in (PENDING_STATES | SUSPENDED_STATES)
            and (item.get("collection_error") or not item.get("collected"))
        ]
        if errors:
            summary = ", ".join(
                f"{item.get('node')}:{item.get('collection_error') or item.get('state')}"
                for item in errors
            )
            raise ReconcileError(f"could not safely collect all active nodes: {summary}")
        return health

    def _health(self, plan_path: Path, window_seconds: int) -> Mapping[str, Any]:
        return self._run_collector(
            plan_path,
            include_tasks=False,
            window_seconds=window_seconds,
        )

    def _run_collector(
        self,
        plan_path: Path,
        *,
        include_tasks: bool,
        window_seconds: int | None = None,
    ) -> Mapping[str, Any]:
        plan_path = plan_path.resolve()
        plan = load_plan(plan_path)
        run_dir = Path(str(plan.get("run_dir") or plan_path.parent)).resolve()
        argv = [
            str(self.workspace / ".venv" / "bin" / "python"),
            str(self.workspace / "src" / "slurm_collect.py"),
            "--plan",
            str(plan_path),
        ]
        if include_tasks:
            argv.append("--include-tasks")
        if window_seconds is not None:
            argv.extend(["--health-window-seconds", str(window_seconds)])
        argv.extend(["--transport", "ssh"])
        self.client.run_privileged(argv, timeout=3600)
        health = read_json(run_dir / "slurm-health.json")
        if not health:
            raise ReconcileError("Slurm collector did not produce slurm-health.json")
        return health

    def _launch(self, config: ControlConfig, action: str, plan_path: Path | None) -> Path:
        if action not in {"plan", "stage", "submit"}:
            raise ValueError(f"unsupported launch action: {action}")
        python = self.workspace / ".venv" / "bin" / "python"
        argv = [
            str(python),
            str(self.workspace / "src" / "slurm_four_node_restart.py"),
            "--workspace",
            str(self.workspace),
            "--run-name",
            config.run.name,
            "--source",
            str(config.run.input_jsonl),
            "--models",
            str(config.run.models_yaml),
            "--opus-model",
            config.models.opus,
            "--sonnet-model",
            config.models.sonnet,
            "--revision",
            config.run.revision,
            "--route-workers",
            ",".join(str(config.slurm.routes[route]) for route in ROUTES),
            "--stage-transport",
            "ssh",
            "--action",
            action,
        ]
        if plan_path is not None:
            argv.extend(["--plan-path", str(plan_path)])
        if action == "stage":
            argv.extend(["--remote-root", str(DEFAULT_CANDIDATE_REMOTE_ROOT)])
            if self.skip_preflight:
                argv.append("--skip-preflight")
        elif action == "submit":
            # Controller submits are reached only after _restart validates a
            # fully staged, preflighted candidate. Re-stage the identical
            # active-runtime bundle without repeating network/Docker probes.
            argv.append("--skip-preflight")
        self.client.run_privileged(argv, timeout=7200)
        return (plan_path or config.run.plan_path).resolve()

    def _status_previous(self, config: ControlConfig) -> dict[str, Any]:
        return read_json(config.controller.status_path)

    def _explicit_breaker_reset(
        self,
        config: ControlConfig,
        previous_breaker: Mapping[str, Any],
    ) -> bool:
        if config.desired_state != "running" or not previous_breaker.get("tripped"):
            return False
        tripped_at = parse_timestamp(previous_breaker.get("tripped_at"))
        reset_requested_at = parse_timestamp(
            config.metadata.get("circuit_breaker_reset_requested_at")
        )
        if reset_requested_at is None:
            return False
        return tripped_at is None or reset_requested_at >= tripped_at

    def _publish_breaker_reset_progress(
        self,
        config: ControlConfig,
        previous: Mapping[str, Any],
        now: datetime,
        suppressed_until: str,
    ) -> None:
        """Publish reset acceptance before a potentially long topology restart."""
        status = dict(previous)
        previous_controller = previous.get("controller")
        controller = (
            dict(previous_controller) if isinstance(previous_controller, Mapping) else {}
        )
        controller.update(
            {
                "state": "reconciling",
                "pid": os.getpid(),
                "config_path": str(self.config_path),
                "workspace": str(self.workspace),
                "poll_interval_seconds": config.controller.poll_interval_seconds,
                "last_action": "circuit_breaker_reset",
                "last_error": None,
                "reason": (
                    "explicit resume cleared the API-failure circuit breaker; "
                    "converging the requested topology"
                ),
            }
        )
        previous_breaker = previous.get("circuit_breaker")
        breaker = dict(previous_breaker) if isinstance(previous_breaker, Mapping) else {}
        breaker.update(
            {
                "enabled": config.circuit_breaker.enabled,
                "tripped": False,
                "tripped_at": None,
                "explicit_reset_required": False,
                "window_seconds": config.circuit_breaker.window_seconds,
                "failure_threshold": config.circuit_breaker.failure_threshold,
                "cooldown_seconds": config.circuit_breaker.cooldown_seconds,
                "suppressed_until": suppressed_until,
            }
        )
        desired = previous.get("desired")
        desired_state = dict(desired) if isinstance(desired, Mapping) else {}
        desired_state.update(
            {
                "state": config.desired_state,
                "node_count": config.slurm.node_count,
                "routes": config.slurm.routes,
                "workers_per_node": config.slurm.workers_per_node,
                "total_workers": config.slurm.total_workers,
                "models": config.models.as_dict(),
            }
        )
        status.update(
            {
                "version": 1,
                "timestamp": now_iso(now),
                "controller": controller,
                "desired": desired_state,
                "circuit_breaker": breaker,
            }
        )
        atomic_json(config.controller.status_path, status)

    def _wait_cancelled(self, jobs: Sequence[JobObservation], timeout_seconds: int = 180) -> None:
        deadline = time.monotonic() + timeout_seconds
        job_ids = [job.job_id for job in jobs if job.job_id]
        while job_ids:
            live = [job_id for job_id in job_ids if self.client.job_state(job_id) in LIVE_STATES]
            if not live:
                return
            if time.monotonic() >= deadline:
                raise ReconcileError(f"timed out waiting for cancelled jobs: {', '.join(live)}")
            self.sleep_fn(2)

    def _candidate_has_preflight(self, record: Mapping[str, Any]) -> bool:
        value = record.get("preflight")
        if isinstance(value, str):
            lines = [value.strip()]
        elif isinstance(value, list):
            lines = [item.strip() for item in value if isinstance(item, str)]
        else:
            return False
        evidence = [line for line in lines if line]
        if not evidence:
            return False
        # When preflight is intentionally skipped (contended/trusted nodes) the
        # candidate carries "skipped" evidence; accept it only in that mode.
        if self.skip_preflight:
            return True
        return not any("skipped" in line.casefold() for line in evidence)

    def _validate_staged_candidate(
        self,
        config: ControlConfig,
        candidate_plan: Mapping[str, Any],
        desired_model_fingerprint: str,
        desired_model_endpoints: set[str],
    ) -> None:
        if plan_routes(candidate_plan) != config.slurm.routes:
            raise ReconcileError("candidate launch plan does not match requested route concurrency")
        if int(candidate_plan.get("expected_workers", -1)) != config.slurm.total_workers:
            raise ReconcileError("candidate launch plan has an incorrect total worker count")
        if plan_models(candidate_plan) != config.models.as_dict():
            raise ReconcileError("candidate launch plan does not match requested role models")
        if plan_model_config_sha256(candidate_plan) != desired_model_fingerprint:
            raise ReconcileError("candidate launch plan does not match requested model endpoints")
        if set(plan_model_endpoints(candidate_plan)) != desired_model_endpoints:
            raise ReconcileError("candidate launch plan advertises the wrong model endpoints")
        if candidate_plan.get("action") != "stage":
            raise ReconcileError("candidate launch plan is not a staged-only plan")

        nodes = candidate_plan.get("nodes")
        if not isinstance(nodes, list) or len(nodes) != config.slurm.node_count:
            raise ReconcileError("candidate launch plan does not contain every staged node")
        node_names: set[str] = set()
        for record in nodes:
            if not isinstance(record, Mapping):
                raise ReconcileError("candidate launch plan has an invalid staged node record")
            node = record.get("node")
            if not isinstance(node, str) or not node or node in node_names:
                raise ReconcileError("candidate launch plan has duplicate or invalid staged nodes")
            node_names.add(node)
            if record.get("job_id") not in {None, ""}:
                raise ReconcileError("candidate launch plan already contains submitted jobs")
            if record.get("stage") != "ready":
                raise ReconcileError("candidate launch plan is not fully staged")
            try:
                node_workers = int(record.get("expected_workers", -1))
            except (TypeError, ValueError):
                node_workers = -1
            if node_workers != config.slurm.workers_per_node:
                raise ReconcileError("candidate launch plan has an incorrect per-node worker count")
            if not self._candidate_has_preflight(record):
                raise ReconcileError("candidate launch plan lacks successful preflight evidence")

    def _restart(
        self,
        config: ControlConfig,
        plan: Mapping[str, Any],
        jobs: Sequence[JobObservation],
    ) -> Path:
        uncertain_ids = [job.job_id for job in jobs if job.job_id and job.state in UNCERTAIN_STATES]
        if uncertain_ids:
            raise ReconcileError(
                "refusing to relaunch while existing job state is unknown: "
                + ", ".join(uncertain_ids)
            )
        control_dir = config.controller.status_path.parent
        control_dir.mkdir(parents=True, exist_ok=True)
        candidate = control_dir / f"candidate-{config.run.revision}-plan.json"
        desired_model_fingerprint = model_config_sha256(config.run.models_yaml)
        desired_model_endpoints = set(
            model_config_endpoints(
                config.run.models_yaml,
                set(config.models.as_dict().values()),
            )
        )

        candidate_plan: Mapping[str, Any] | None = None
        if candidate.is_file():
            try:
                existing_candidate = load_plan(candidate)
                self._validate_staged_candidate(
                    config,
                    existing_candidate,
                    desired_model_fingerprint,
                    desired_model_endpoints,
                )
            except (OSError, ReconcileError, TypeError, ValueError):
                pass
            else:
                candidate_plan = existing_candidate

        if candidate_plan is None:
            self.launch_action(config, "stage", candidate)
            candidate_plan = load_plan(candidate)
            self._validate_staged_candidate(
                config,
                candidate_plan,
                desired_model_fingerprint,
                desired_model_endpoints,
            )

        old_ids = [job.job_id for job in jobs if job.job_id and job.state in LIVE_STATES]
        running_ids = [job.job_id for job in jobs if job.job_id and job.state in RUNNING_STATES]
        if old_ids:
            if running_ids:
                self.collect_action(config.run.plan_path)
            self.client.cancel(old_ids)
            self._wait_cancelled(jobs)
            self.collect_action(config.run.plan_path)

        launched_path = self.launch_action(config, "submit", None)
        launched = load_plan(launched_path)
        launched_jobs = [
            str(record["job_id"])
            for record in launched.get("nodes", [])
            if isinstance(record, Mapping) and record.get("job_id")
        ]
        if len(launched_jobs) != config.slurm.node_count:
            if launched_jobs:
                self.client.cancel(launched_jobs)
            raise ReconcileError(
                f"relaunch submitted {len(launched_jobs)}/{config.slurm.node_count} jobs"
            )
        if plan_routes(launched) != config.slurm.routes:
            self.client.cancel(launched_jobs)
            raise ReconcileError("submitted plan does not match requested route concurrency")
        if plan_models(launched) != config.models.as_dict():
            self.client.cancel(launched_jobs)
            raise ReconcileError("submitted plan does not match requested role models")
        if plan_model_config_sha256(launched) != desired_model_fingerprint:
            self.client.cancel(launched_jobs)
            raise ReconcileError("submitted plan does not match requested model endpoints")
        if set(plan_model_endpoints(launched)) != desired_model_endpoints:
            self.client.cancel(launched_jobs)
            raise ReconcileError("submitted plan advertises the wrong model endpoints")
        candidate.unlink(missing_ok=True)
        return launched_path

    def _pause_jobs(
        self,
        jobs: Sequence[JobObservation],
        held_job_ids: set[str],
    ) -> bool:
        running_ids = [job.job_id for job in jobs if job.job_id and job.state in RUNNING_STATES]
        pending_ids = [
            job.job_id
            for job in jobs
            if job.job_id and job.state in PENDING_STATES and job.job_id not in held_job_ids
        ]
        if running_ids:
            self.client.suspend(running_ids)
        if pending_ids:
            self.client.hold(pending_ids)
            held_job_ids.update(pending_ids)
        return bool(running_ids or pending_ids)

    def _resume_jobs(
        self,
        jobs: Sequence[JobObservation],
        held_job_ids: set[str],
    ) -> bool:
        suspended_ids = [job.job_id for job in jobs if job.job_id and job.state in SUSPENDED_STATES]
        releasable_ids = [
            job.job_id
            for job in jobs
            if job.job_id and job.state in PENDING_STATES and job.job_id in held_job_ids
        ]
        if releasable_ids:
            self.client.release(releasable_ids)
            held_job_ids.difference_update(releasable_ids)
        if suspended_ids:
            self.client.resume(suspended_ids)
        return bool(releasable_ids or suspended_ids)

    def _stop_stage_workers(self) -> list[str]:
        """Stop Stage II/III systemd services.  Best-effort: failures are logged
        but do not block reconciliation."""
        try:
            stopped = self.service_manager.stop(STAGE_WORKER_SERVICES)
        except (OSError, subprocess.SubprocessError) as exc:
            stopped = []
            print(f"warning: could not stop stage workers: {redact(str(exc))}", flush=True)
        return stopped

    def _start_stage_workers(self) -> list[str]:
        """Start Stage II/III systemd services.  Best-effort: failures are
        logged but do not block reconciliation."""
        try:
            started = self.service_manager.start(STAGE_WORKER_SERVICES)
        except (OSError, subprocess.SubprocessError) as exc:
            started = []
            print(f"warning: could not start stage workers: {redact(str(exc))}", flush=True)
        return started

    def reconcile_once(self) -> dict[str, Any]:
        config = load_config(self.config_path)
        now = self.now_fn().astimezone(UTC)
        previous = self._status_previous(config)
        previous_breaker = previous.get("circuit_breaker")
        if not isinstance(previous_breaker, Mapping):
            previous_breaker = {}
        previous_controller = previous.get("controller")
        if not isinstance(previous_controller, Mapping):
            previous_controller = {}

        plan = load_plan(config.run.plan_path)
        jobs = observe_jobs(plan, self.client)
        current_job_ids = {job.job_id for job in jobs if job.job_id}
        held_job_ids = {
            str(job_id)
            for job_id in previous_controller.get("held_job_ids", [])
            if str(job_id) in current_job_ids
        }
        health: Mapping[str, Any] = {}
        collection_error: str | None = None
        try:
            health = self.health_action(
                config.run.plan_path,
                config.circuit_breaker.window_seconds,
            )
        except (OSError, ReconcileError, subprocess.SubprocessError, ValueError) as exc:
            collection_error = redact(str(exc))
        health_nodes = [item for item in health.get("nodes", []) if isinstance(item, Mapping)]
        node_collection_errors = [
            f"{item.get('node')}:{item.get('collection_error')}"
            for item in health_nodes
            if item.get("collection_error")
        ]
        if node_collection_errors:
            collection_error = "; ".join(node_collection_errors)

        terminal_failure_count, recent_failures = api_failures_in_window(
            config.run.directory,
            config.run.revision,
            window_seconds=config.circuit_breaker.window_seconds,
            now=now,
            plan=plan,
        )
        diagnostic_failure_count = sum(
            int(item.get("recent_api_failure_events", 0) or 0) for item in health_nodes
        )
        failure_count = max(terminal_failure_count, diagnostic_failure_count)
        tripped = bool(previous_breaker.get("tripped")) and config.circuit_breaker.enabled
        tripped_at = previous_breaker.get("tripped_at") if tripped else None
        suppressed_until = previous_breaker.get("suppressed_until")
        suppressed_dt = parse_timestamp(suppressed_until)
        explicit_reset = self._explicit_breaker_reset(config, previous_breaker)
        action = "none"
        reason = "topology already matches desired configuration"
        error: str | None = None
        stage_workers_action = "none"
        desired_model_fingerprint: str | None = None
        desired_model_endpoints: list[str] = []

        try:
            desired_model_fingerprint = model_config_sha256(config.run.models_yaml)
            desired_model_endpoints = model_config_endpoints(
                config.run.models_yaml,
                set(config.models.as_dict().values()),
            )
            if not config.circuit_breaker.enabled:
                tripped = False
                tripped_at = None
                suppressed_until = None
            elif explicit_reset:
                tripped = False
                tripped_at = None
                suppressed_dt = datetime.fromtimestamp(
                    now.timestamp() + config.circuit_breaker.cooldown_seconds, UTC
                )
                suppressed_until = now_iso(suppressed_dt)
                action = "circuit_breaker_reset"
                reason = "explicit resume cleared the latched API-failure circuit breaker"
                self._start_stage_workers()
                stage_workers_action = "started"
                self._publish_breaker_reset_progress(
                    config,
                    previous,
                    now,
                    suppressed_until,
                )

            breaker_suppressed = suppressed_dt is not None and now < suppressed_dt
            should_trip = (
                config.circuit_breaker.enabled
                and config.desired_state == "running"
                and not tripped
                and not explicit_reset
                and not breaker_suppressed
                and failure_count >= config.circuit_breaker.failure_threshold
            )
            if should_trip:
                self._stop_stage_workers()
                stage_workers_action = "stopped"
                self._pause_jobs(jobs, held_job_ids)
                tripped = True
                tripped_at = now_iso(now)
                update_desired_state(
                    self.config_path,
                    "paused",
                    "circuit-breaker",
                    (
                        f"{failure_count} global API failures in "
                        f"{config.circuit_breaker.window_seconds}s"
                    ),
                    updated_at=now,
                )
                action = "circuit_breaker_tripped"
                reason = (
                    f"paused Stage-I after {failure_count} global API failures "
                    f"in {config.circuit_breaker.window_seconds}s"
                )
                jobs = observe_jobs(plan, self.client)
            elif tripped:
                if self._pause_jobs(jobs, held_job_ids):
                    self._stop_stage_workers()
                    stage_workers_action = "stopped"
                    action = "enforce_breaker_pause"
                    jobs = observe_jobs(plan, self.client)
                reason = "API-failure circuit breaker is latched; explicit resume is required"
            elif config.desired_state == "paused":
                if self._pause_jobs(jobs, held_job_ids):
                    self._stop_stage_workers()
                    stage_workers_action = "stopped"
                    action = "paused"
                    reason = (
                        "suspended running jobs and held pending jobs to match desired_state=paused"
                    )
                    jobs = observe_jobs(plan, self.client)
                else:
                    reason = "all submitted jobs are suspended, held, or inactive"
            else:
                topology_matches = (
                    int(plan.get("expected_workers", -1)) == config.slurm.total_workers
                    and plan_routes(plan) == config.slurm.routes
                    and plan_models(plan) == config.models.as_dict()
                    and plan_model_config_sha256(plan) == desired_model_fingerprint
                    and len(jobs) == config.slurm.node_count
                    and all(job.job_id for job in jobs)
                )
                known_dead = any(
                    job.job_id is None
                    or (job.state not in LIVE_STATES and job.state not in UNCERTAIN_STATES)
                    for job in jobs
                )
                if not topology_matches or known_dead:
                    self._restart(config, plan, jobs)
                    held_job_ids.clear()
                    action = "restarted"
                    reason = "relaunched fixed plan to apply topology or replace inactive jobs"
                    plan = load_plan(config.run.plan_path)
                    jobs = observe_jobs(plan, self.client)
                else:
                    if self._resume_jobs(jobs, held_job_ids):
                        self._start_stage_workers()
                        stage_workers_action = "started"
                        action = "resumed"
                        reason = "resumed jobs to match desired_state=running"
                        jobs = observe_jobs(plan, self.client)
        except (OSError, ReconcileError, subprocess.SubprocessError, ValueError) as exc:
            error = redact(str(exc))
            action = "error"
            reason = "reconciliation failed; controller will retry without widening scope"

        applied_state = observed_cluster_state(jobs, held_job_ids)
        observed_active_workers = health.get("active_workers")
        if isinstance(observed_active_workers, int) and not isinstance(
            observed_active_workers, bool
        ):
            active_workers = max(observed_active_workers, 0)
            active_workers_source = "remote_processes"
        else:
            active_workers = sum(
                config.slurm.workers_per_node for job in jobs if job.state in RUNNING_STATES
            )
            active_workers_source = "allocation_estimate"
        if applied_state == "paused":
            active_workers = 0
        if action == "restarted":
            active_workers = 0
            active_workers_source = "awaiting_remote_health"
        status = {
            "version": 1,
            "timestamp": now_iso(now),
            "controller": {
                "state": "error" if error else "degraded" if collection_error else "running",
                "applied_state": applied_state,
                "pid": os.getpid(),
                "config_path": str(self.config_path),
                "workspace": str(self.workspace),
                "poll_interval_seconds": config.controller.poll_interval_seconds,
                "last_action": action,
                "last_error": error or collection_error,
                "collection_error": collection_error,
                "held_job_ids": sorted(held_job_ids),
                "reason": reason,
                "stage_workers_action": stage_workers_action,
            },
            "run": {
                "name": config.run.name,
                "active_plan": str(config.run.plan_path),
                "revision": plan.get("revision"),
                "models_yaml": str(config.run.models_yaml),
            },
            "models": {
                "desired": config.models.as_dict(),
                "observed": plan_models(plan),
            },
            "model_backend": {
                "desired_config_sha256": desired_model_fingerprint,
                "observed_config_sha256": plan_model_config_sha256(plan),
                "desired_endpoints": desired_model_endpoints,
                "observed_endpoints": plan_model_endpoints(plan),
            },
            "slurm": {
                "node_count": config.slurm.node_count,
                "routes": plan_routes(plan),
                "active_workers": active_workers,
                "active_workers_source": active_workers_source,
                "expected_workers": plan.get("expected_workers"),
            },
            "desired": {
                "state": "paused" if action == "circuit_breaker_tripped" else config.desired_state,
                "node_count": config.slurm.node_count,
                "routes": config.slurm.routes,
                "workers_per_node": config.slurm.workers_per_node,
                "total_workers": config.slurm.total_workers,
                "models": config.models.as_dict(),
                "model_config_sha256": desired_model_fingerprint,
                "model_endpoints": desired_model_endpoints,
            },
            "observed": {
                "state": applied_state,
                "plan_path": str(config.run.plan_path),
                "revision": plan.get("revision"),
                "routes": plan_routes(plan),
                "expected_workers": plan.get("expected_workers"),
                "models": plan_models(plan),
                "model_config_sha256": plan_model_config_sha256(plan),
                "model_endpoints": plan_model_endpoints(plan),
                "jobs": [
                    {"node": job.node, "job_id": job.job_id, "state": job.state} for job in jobs
                ],
            },
            "circuit_breaker": {
                "enabled": config.circuit_breaker.enabled,
                "tripped": tripped,
                "tripped_at": tripped_at,
                "explicit_reset_required": tripped,
                "window_seconds": config.circuit_breaker.window_seconds,
                "failure_threshold": config.circuit_breaker.failure_threshold,
                "failure_count": failure_count,
                "terminal_failure_count": terminal_failure_count,
                "diagnostic_failure_count": diagnostic_failure_count,
                "cooldown_seconds": config.circuit_breaker.cooldown_seconds,
                "suppressed_until": suppressed_until,
                "recent_failures": recent_failures,
            },
        }
        atomic_json(config.controller.status_path, status)
        return status


def acquire_lock(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise ReconcileError(f"another Slurm controller holds {path}") from None
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--once", action="store_true", help="Reconcile once instead of watching")
    parser.add_argument(
        "--initialize",
        action="store_true",
        help="Create the default config if absent before starting",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Stage runtime bundles without network/Docker/quota preflight builds "
        "(use when node preflight is contended or the node setup is trusted).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.initialize:
        created = ensure_default_config(args.config.resolve(), args.workspace.resolve())
        print(
            f"config={'created' if created else 'exists'} path={args.config.resolve()}", flush=True
        )
    config = load_config(args.config.resolve())
    lock_handle = acquire_lock(config.controller.status_path.parent / "controller.lock")
    reconciler = Reconciler(args.config, args.workspace, skip_preflight=args.skip_preflight)
    stop = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    while True:
        try:
            status = reconciler.reconcile_once()
            controller = status["controller"]
            print(
                f"{status['timestamp']} action={controller['last_action']} "
                f"desired={status['desired']['state']} observed={status['observed']['state']} "
                f"api_failures={status['circuit_breaker']['failure_count']}",
                flush=True,
            )
        except (ConfigError, ReconcileError, OSError) as exc:
            print(f"controller error: {redact(str(exc))}", file=sys.stderr, flush=True)
            if args.once:
                return 1
        if args.once or stop:
            break
        try:
            interval = load_config(args.config.resolve()).controller.poll_interval_seconds
        except ConfigError:
            interval = 30
        deadline = time.monotonic() + interval
        while not stop and time.monotonic() < deadline:
            time.sleep(min(1, max(deadline - time.monotonic(), 0)))
    lock_handle.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
