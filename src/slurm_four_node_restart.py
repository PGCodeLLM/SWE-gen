#!/usr/bin/env python3
"""Stage and launch a proxy-balanced four-node Stage-I topology.

This operational launcher reads the backend endpoint and credential from a
protected LiteLLM model config, but workers continue to call the compatible
OpenAI and Anthropic endpoints directly through their SG/HK/DE route proxies.
Secrets are transferred only in the private bundle and never written to the
public launch plan or command line.
"""
# DEPRECATED: superseded by the k3s pipeline (swegen/pipeline/), but STILL LIVE.
# As of 2026-08-13 two workers are running run `20260716-sol-max-full-16w` from
# separate checkouts (/data/nfs_shared/swegen, /data/work/slurm-swegen) for which
# this repo is the source of truth. Do not delete without confirming that run has
# finished.

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import stat
from datetime import UTC, datetime
from pathlib import Path
from shutil import which
from typing import Any
from urllib.parse import urlsplit

import yaml

from generate_orchestrator_shards import generate_shards
from slurm_two_node import (
    NodeSpec,
    _run,
    build_bundle,
    command_prefix,
    remote_workspace,
    stage_node,
    task_instance,
)

RUN_NAME = "20260716-sol-max-full-16w"
SOURCE = Path("data_cache/pr_tasks_export_ts_js_12122_removed.jsonl")
MODELS = Path("/data/work/alex/SWE-gen/models.yaml")
DEFAULT_OPUS_MODEL = "gpt-5.6-sol"
DEFAULT_SONNET_MODEL = "gpt-5.6-terra"
REMOTE_ROOT = "/data/work/slurm-swegen/slurm-runtime"
REVISION = "r9"
ROUTES = ("sg", "hk", "de")
SUFFIXES = ("a", "b", "c")
WORKERS_PER_GROUP = 4
DEFAULT_ROUTE_WORKERS = (4, 4, 4)
DEFAULT_WORKERS_PER_NODE = sum(DEFAULT_ROUTE_WORKERS)
NODES = (
    NodeSpec(
        "ecs-z00579134-20260707-bugfix-0002",
        "7.244.3.78",
        1,
        0,
        DEFAULT_WORKERS_PER_NODE,
    ),
    NodeSpec(
        "ecs-z00579134-20260707-bugfix-0005",
        "7.244.2.110",
        2,
        60,
        DEFAULT_WORKERS_PER_NODE,
    ),
    NodeSpec(
        "ecs-z00579134-20260707-bugfix-0003",
        "7.244.3.200",
        3,
        120,
        DEFAULT_WORKERS_PER_NODE,
    ),
    NodeSpec(
        "ecs-z00579134-20260707-bugfix-0006",
        "7.244.1.209",
        4,
        180,
        DEFAULT_WORKERS_PER_NODE,
    ),
)


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_route_workers(value: str) -> tuple[int, int, int]:
    parts = value.split(",")
    if len(parts) != len(ROUTES):
        raise argparse.ArgumentTypeError("route workers must be SG,HK,DE counts")
    try:
        workers = tuple(int(part) for part in parts)
    except ValueError as error:
        raise argparse.ArgumentTypeError("route worker counts must be integers") from error
    if any(worker not in range(0, 13) for worker in workers):
        raise argparse.ArgumentTypeError("each route worker count must be between 0 and 12")
    if not any(workers):
        raise argparse.ArgumentTypeError("at least one route must have a worker")
    return workers  # type: ignore[return-value]


def route_groups(route_workers: tuple[int, int, int]) -> list[tuple[str, str, int]]:
    """Return non-empty route groups in deterministic launch order.

    A route's first four workers use group ``a`` and any remaining workers use
    group ``b``. This preserves the established four-worker isolation while
    allowing the controller to request every per-route concurrency from 0 to 12.
    """
    groups: list[tuple[str, str, int]] = []
    for suffix_index, suffix in enumerate(SUFFIXES):
        for route, workers in zip(ROUTES, route_workers, strict=True):
            group_workers = min(WORKERS_PER_GROUP, max(workers - suffix_index * 4, 0))
            if group_workers:
                groups.append((route, suffix, group_workers))
    return groups


def nodes_for_workers(workers_per_node: int) -> tuple[NodeSpec, ...]:
    return tuple(
        NodeSpec(
            node.node,
            node.node_ip,
            node.index,
            node.initial_delay_seconds,
            workers_per_node,
        )
        for node in NODES
    )


def _profile_urls(api_base: str) -> tuple[str, str]:
    if api_base.endswith("/v1"):
        return api_base, api_base.removesuffix("/v1")
    return api_base + "/v1", api_base


def _validated_api_base(value: object, roles: list[str]) -> str:
    api_base = str(value or "").rstrip("/")
    parsed = urlsplit(api_base)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/v1"}
        or parsed.query
        or parsed.fragment
    ):
        joined = ", ".join(roles)
        raise ValueError(
            "configured Stage-I models must share one non-empty valid public api_base "
            f"within each profile; invalid public api_base for role(s): {joined}"
        )
    return api_base


def load_model_credential_pool(
    path: Path,
    opus_model: str = DEFAULT_OPUS_MODEL,
    sonnet_model: str = DEFAULT_SONNET_MODEL,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Load complete backend profiles for the selected model roles.

    Repeated ``model_name`` entries are intentional: each distinct
    ``(api_base, api_key)`` pair describes one backend profile. A profile is
    usable only when it supplies both selected roles. Exact duplicates are
    ignored, while conflicting duplicates and half-configured backends fail
    closed before any Slurm work is staged.
    """
    if not path.is_file():
        raise FileNotFoundError(f"missing model config: {path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise PermissionError(f"model config must be mode 0600 (found {mode:04o}): {path}")
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    entries = value.get("model_list") if isinstance(value, dict) else None
    if not isinstance(entries, list):
        raise ValueError("model config lacks model_list")

    role_names = {"opus": opus_model, "sonnet": sonnet_model}
    matched_roles: set[str] = set()
    grouped: dict[tuple[str, str], dict[str, str]] = {}
    group_order: list[tuple[str, str]] = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        roles = [role for role, name in role_names.items() if entry.get("model_name") == name]
        if not roles:
            continue
        params = entry.get("litellm_params")
        if not isinstance(params, dict):
            joined = ", ".join(roles)
            raise ValueError(f"model config has invalid litellm_params for role(s): {joined}")
        api_base = _validated_api_base(params.get("api_base"), roles)
        api_key = str(params.get("api_key") or "")
        if not api_key:
            joined = ", ".join(roles)
            raise ValueError(
                f"model config requires non-empty api_base and api_key for role(s): {joined}"
            )
        identity = (api_base, api_key)
        if identity not in grouped:
            grouped[identity] = {}
            group_order.append(identity)
        canonical_params = dict(params)
        canonical_params["api_base"] = api_base
        canonical_params["api_key"] = api_key
        signature = json.dumps(canonical_params, sort_keys=True, separators=(",", ":"))
        for role in roles:
            matched_roles.add(role)
            previous = grouped[identity].get(role)
            if previous is not None and previous != signature:
                raise ValueError(f"model config has conflicting entries for {role} at {api_base!r}")
            grouped[identity][role] = signature

    for role, name in role_names.items():
        if role not in matched_roles:
            raise ValueError(f"model config has no exact model_name match for {role}: {name!r}")

    incomplete = [
        (api_base, sorted(set(role_names) - set(grouped[identity])))
        for identity in group_order
        for api_base, _api_key in (identity,)
        if set(grouped[identity]) != set(role_names)
    ]
    if incomplete:
        details = "; ".join(
            f"{api_base!r} missing {','.join(missing)}" for api_base, missing in incomplete
        )
        raise ValueError(
            "configured Stage-I models must share one non-empty api_base/api_key "
            f"within each complete profile; incomplete profile(s): {details}"
        )

    credentials: list[dict[str, str]] = []
    public_profiles: list[dict[str, Any]] = []
    for index, (api_base, api_key) in enumerate(group_order):
        profile_name = f"backend-{index:03d}"
        openai_base, anthropic_base = _profile_urls(api_base)
        credentials.append(
            {
                "OPENAI_API_KEY": api_key,
                "ANTHROPIC_API_KEY": api_key,
                "ANTHROPIC_AUTH_TOKEN": api_key,
                "OPENAI_BASE_URL": openai_base,
                "ANTHROPIC_BASE_URL": anthropic_base,
                "OPENAI_MODEL": opus_model,
                "ANTHROPIC_MODEL": opus_model,
                "ANTHROPIC_DEFAULT_OPUS_MODEL": opus_model,
                "ANTHROPIC_DEFAULT_SONNET_MODEL": sonnet_model,
            }
        )
        public_profiles.append(
            {
                "name": profile_name,
                "api_base": api_base,
                "openai_base_url": openai_base,
                "anthropic_base_url": anthropic_base,
                "models": sorted(set(role_names.values())),
                "roles": dict(role_names),
            }
        )

    public = {
        "profiles": public_profiles,
        "endpoints": [profile["api_base"] for profile in public_profiles],
        "routing_strategy": "per-task-round-robin-with-shard-round-robin-fallback",
        "config_sha256": sha256_file(path),
    }
    return credentials, public


def load_model_credentials(
    path: Path,
    opus_model: str = DEFAULT_OPUS_MODEL,
    sonnet_model: str = DEFAULT_SONNET_MODEL,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Preserve the original one-profile API for callers that require it."""
    profiles, public = load_model_credential_pool(path, opus_model, sonnet_model)
    if len(profiles) != 1:
        raise ValueError(
            f"model config resolves to {len(profiles)} backend profiles; "
            "use load_model_credential_pool()"
        )
    profile_public = dict(public["profiles"][0])
    profile_public["config_sha256"] = public["config_sha256"]
    return profiles[0], profile_public


def assign_model_profiles(shards: list[str], profile_count: int) -> dict[str, str]:
    """Assign the shard fallback profiles in deterministic global order."""
    if profile_count < 1:
        raise ValueError("profile count must be positive")
    return {shard: f"backend-{index % profile_count:03d}" for index, shard in enumerate(shards)}


def successful_instances(run_dir: Path) -> set[str]:
    ledgers = [run_dir / "create.jsonl", *sorted((run_dir / "slurm-nodes").glob("*/create.jsonl"))]
    successful: set[str] = set()
    for ledger in ledgers:
        if not ledger.is_file():
            continue
        with ledger.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
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
    return successful


def write_remaining(source: Path, destination: Path, completed: set[str]) -> dict[str, int]:
    source_entries = 0
    successful_excluded = 0
    remaining = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        source.open(encoding="utf-8", errors="replace") as input_file,
        destination.open("w", encoding="utf-8") as output_file,
    ):
        for line in input_file:
            if not line.strip():
                continue
            source_entries += 1
            record = json.loads(line)
            repo = record.get("repo")
            pull_number = record.get("pull_number")
            if not isinstance(repo, str) or pull_number is None:
                raise ValueError(f"source line {source_entries} lacks repo/pull_number")
            if task_instance(repo, pull_number) in completed:
                successful_excluded += 1
                continue
            output_file.write(line if line.endswith("\n") else line + "\n")
            remaining += 1
    destination.chmod(0o600)
    return {
        "source_entries": source_entries,
        "successful_excluded": successful_excluded,
        "remaining": remaining,
    }


def shard_names(
    revision: str,
    nodes: tuple[NodeSpec, ...],
    route_workers: tuple[int, int, int],
) -> list[str]:
    return [
        f"{revision}-{route}-n{node.index}-{suffix}"
        for node in nodes
        for route, suffix, _workers in route_groups(route_workers)
    ]


def write_plan(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    owner = path if path.exists() else path.parent
    owner_stat = owner.stat()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    # 0o644, not 0o600: the plan is node/job metadata (not a secret) and the
    # alex-run Stage-3 reward worker reads it via --plan; a root-written 0o600
    # plan makes that worker crash-loop with PermissionError after every
    # controller relaunch. (slurm_collect.py already writes the same file 0o644.)
    temporary.chmod(0o644)
    if os.geteuid() == 0:
        os.chown(temporary, owner_stat.st_uid, owner_stat.st_gid)
    os.replace(temporary, path)


def prior_job_history(path: Path, replaced_at: str) -> list[dict[str, Any]]:
    """Preserve replaced allocations without making watchers follow a new plan.

    The collector and baseline validator intentionally watch the stable r9 plan
    path. Reconfiguration therefore overwrites that plan in place, while this
    append-only summary retains the old job IDs for audits and crash recovery.
    """
    if not path.is_file():
        return []
    try:
        previous = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(previous, dict):
        return []
    history = previous.get("job_history")
    result = list(history) if isinstance(history, list) else []
    nodes = previous.get("nodes")
    if not isinstance(nodes, list):
        return result
    jobs = [
        {
            "node": record.get("node"),
            "node_ip": record.get("node_ip"),
            "index": record.get("index"),
            "job_id": str(record["job_id"]) if record.get("job_id") else None,
            "submitted_at": record.get("submitted_at"),
        }
        for record in nodes
        if isinstance(record, dict)
    ]
    job_ids = tuple(item["job_id"] for item in jobs if item["job_id"])
    if not job_ids:
        return result
    for item in result:
        if not isinstance(item, dict):
            continue
        existing = tuple(
            str(node.get("job_id"))
            for node in item.get("nodes", [])
            if isinstance(node, dict) and node.get("job_id")
        )
        if existing == job_ids:
            return result
    result.append(
        {
            "replaced_at": replaced_at,
            "revision": previous.get("revision"),
            "topology": previous.get("topology"),
            "expected_workers": previous.get("expected_workers"),
            "proxy_workers_per_node": previous.get("proxy_workers_per_node"),
            "nodes": jobs,
        }
    )
    return result


def submit_node(
    workspace: Path,
    node: NodeSpec,
    remote_path: str,
    run_name: str,
    shard_relative_dir: str,
    revision: str,
    route_workers: tuple[int, int, int],
    *,
    skip_preflight: bool = False,
) -> str:
    output = f"{remote_path}/runs/{run_name}/slurm-stage1-{revision}-%j.out"
    argv = command_prefix() + [
        "sbatch",
        "--parsable",
        f"--nodelist={node.node}",
        "--nodes=1",
        "--ntasks=1",
        "--exclusive",
        f"--job-name=swegen-stage1-{revision}-n{node.index}",
        f"--output={output}",
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
        "--shard-revision",
        revision,
        "--route-workers",
        ",".join(str(value) for value in route_workers),
        "--workers-per-group",
        str(WORKERS_PER_GROUP),
        "--preserve-master-config",
    ]
    if skip_preflight:
        argv.append("--skip-preflight")
    result = _run(argv, timeout=120)
    job_id = result.stdout.decode(errors="replace").strip().split(";", 1)[0]
    if not job_id.isdigit():
        raise RuntimeError(f"could not parse job id from {job_id!r}")
    return job_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--run-name", default=RUN_NAME)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--models", type=Path, default=MODELS)
    parser.add_argument("--opus-model", default=DEFAULT_OPUS_MODEL)
    parser.add_argument("--sonnet-model", default=DEFAULT_SONNET_MODEL)
    parser.add_argument("--remote-root", default=REMOTE_ROOT)
    parser.add_argument("--revision", default=REVISION)
    parser.add_argument(
        "--plan-path",
        type=Path,
        help="Override the output plan path (used for non-disruptive candidate validation).",
    )
    parser.add_argument(
        "--route-workers",
        type=parse_route_workers,
        default=DEFAULT_ROUTE_WORKERS,
        metavar="SG,HK,DE",
        help="Workers per node for the SG, HK, and DE proxies (default: 4,4,4).",
    )
    parser.add_argument(
        "--stage-transport",
        choices=("srun", "ssh"),
        default="srun",
        help="Transport used to stage and preflight node-local workspaces.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Stage runtime bundles without running network, Docker, or quota preflights.",
    )
    parser.add_argument("--action", choices=("plan", "stage", "submit"), default="plan")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = args.workspace.resolve()
    source = args.source if args.source.is_absolute() else workspace / args.source
    source = source.resolve()
    route_workers = args.route_workers
    workers_per_node = sum(route_workers)
    expected_workers = workers_per_node * len(NODES)
    nodes = nodes_for_workers(workers_per_node)
    groups = route_groups(route_workers)
    all_shard_names = shard_names(args.revision, nodes, route_workers)
    shard_workers = [group_workers for _node in nodes for _route, _suffix, group_workers in groups]
    credential_profiles, model = load_model_credential_pool(
        args.models.resolve(),
        opus_model=args.opus_model,
        sonnet_model=args.sonnet_model,
    )
    profile_assignments = assign_model_profiles(all_shard_names, len(credential_profiles))
    run_dir = workspace / "runs" / args.run_name
    shard_dir = (
        workspace
        / "data_cache"
        / "orchestrator_shards"
        / (f"slurm-{args.run_name}-{args.revision}-4n-{expected_workers}w")
    )
    filtered = shard_dir / "remaining.jsonl"
    counts = write_remaining(source, filtered, successful_instances(run_dir))
    manifest_name = f"slurm-{args.revision}-4n-{expected_workers}w-manifest.json"
    manifest = generate_shards(
        filtered,
        shard_dir,
        names=all_shard_names,
        manifest_name=manifest_name,
        run_name=args.run_name,
        workers=shard_workers,
    )
    manifest_path = shard_dir / manifest_name
    manifest["manifest_path"] = str(manifest_path)
    owner = workspace.stat()
    for path in (shard_dir, *shard_dir.iterdir()):
        if path.is_dir():
            path.chmod(0o775)
        else:
            path.chmod(0o600)
        if os.geteuid() == 0:
            os.chown(path, owner.st_uid, owner.st_gid)
    for path in shard_dir.iterdir():
        if path.is_file():
            path.chmod(0o600)
    shard_relative_dir = str(shard_dir.relative_to(workspace))
    remote_path = remote_workspace(args.remote_root, args.run_name)
    created_at = now()
    default_plan_path = run_dir / f"slurm-stage1-{args.revision}-4n-plan.json"
    plan_path = args.plan_path or default_plan_path
    if not plan_path.is_absolute():
        plan_path = workspace / plan_path
    plan_path = plan_path.resolve()
    env_files = {
        route: workspace / filename
        for route, filename in {"sg": ".env", "hk": ".env_hk", "de": ".env_de"}.items()
    }
    missing_env_files = [str(path) for path in env_files.values() if not path.is_file()]
    if missing_env_files:
        raise FileNotFoundError("missing route environment files: " + ", ".join(missing_env_files))
    uv_bin = Path(shlex.split(os.environ.get("SWEGEN_UV_BIN", "uv"))[0])
    if not uv_bin.is_absolute():
        resolved = which(str(uv_bin))
        if not resolved:
            raise FileNotFoundError("uv executable not found")
        uv_bin = Path(resolved)
    if not uv_bin.is_file() or not os.access(uv_bin, os.X_OK):
        raise FileNotFoundError(f"uv executable is not accessible: {uv_bin}")
    uv_bin = uv_bin.resolve()
    proxy_ca = Path("/data/work/alex/ProxyCA260122.crt")
    plan: dict[str, Any] = {
        "schema_version": 1,
        "topology": "stage1-four-node-" + "-".join(str(value) for value in route_workers),
        "action": args.action,
        "stage_transport": args.stage_transport,
        "created_at": created_at,
        "updated_at": created_at,
        "run_name": args.run_name,
        "run_dir": str(run_dir),
        "runtime_workspace": remote_path,
        "expected_workers": expected_workers,
        "workers_per_node": workers_per_node,
        "workers_per_group": WORKERS_PER_GROUP,
        "proxy_workers_per_node": dict(zip(ROUTES, route_workers, strict=True)),
        "revision": args.revision,
        "model_backend": model,
        "model_routing": {
            "strategy": model["routing_strategy"],
            "profile_count": len(credential_profiles),
            "fallback_profile_by_shard": profile_assignments,
        },
        "input": counts,
        "manifest": str(manifest_path),
        "job_history": prior_job_history(plan_path, created_at),
        "nodes": [],
    }
    for node in nodes:
        plan["nodes"].append(
            {
                "node": node.node,
                "node_ip": node.node_ip,
                "index": node.index,
                "initial_delay_seconds": node.initial_delay_seconds,
                "expected_workers": workers_per_node,
                "remote_workspace": remote_path,
                "remote_run_dir": f"{remote_path}/runs/{args.run_name}",
                "shards": [name for name in all_shard_names if f"-n{node.index}-" in name],
                "fallback_model_profiles": {
                    name: profile_assignments[name]
                    for name in all_shard_names
                    if f"-n{node.index}-" in name
                },
                "job_id": None,
            }
        )
    write_plan(plan_path, plan)
    print(
        f"planned {len(manifest['shards'])} shards; excluded={counts['successful_excluded']} "
        f"remaining={counts['remaining']} workers={plan['expected_workers']}",
        flush=True,
    )
    if args.action == "plan":
        print(f"plan={plan_path}", flush=True)
        return 0

    for record, node in zip(plan["nodes"], nodes, strict=True):
        bundle = build_bundle(
            workspace,
            node,
            manifest,
            args.run_name,
            env_files,
            uv_bin,
            proxy_ca if proxy_ca.is_file() else None,
            credential_profiles,
            expected_shards_per_node=len(groups),
        )
        try:
            preflight = stage_node(
                bundle,
                node,
                remote_path,
                args.run_name,
                shard_relative_dir,
                transport=args.stage_transport,
                skip_preflight=args.skip_preflight,
            )
        finally:
            bundle.unlink(missing_ok=True)
        record["stage"] = "ready"
        record["preflight"] = (
            ["skipped by explicit operator request"]
            if args.skip_preflight
            else preflight.splitlines()[-12:]
        )
        record["staged_at"] = now()
        plan["updated_at"] = now()
        write_plan(plan_path, plan)
        stage_result = (
            "staged; preflight skipped" if args.skip_preflight else "staged and preflight passed"
        )
        print(f"{node.node}: {stage_result}", flush=True)

    if args.action == "submit":
        for record, node in zip(plan["nodes"], nodes, strict=True):
            job_id = submit_node(
                workspace,
                node,
                remote_path,
                args.run_name,
                shard_relative_dir,
                args.revision,
                route_workers,
                skip_preflight=args.skip_preflight,
            )
            record["job_id"] = job_id
            record["submitted_at"] = now()
            plan["updated_at"] = now()
            write_plan(plan_path, plan)
            print(f"{node.node}: submitted job {job_id}", flush=True)
    print(f"plan={plan_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
