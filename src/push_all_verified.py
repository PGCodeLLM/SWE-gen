#!/usr/bin/env python3
"""Push all verified SWE-gen Docker images to the SWR registries.

Reads the postcheck-status.jsonl ledger, finds accepted instances
(oracle=1, nop=0, passed reward-hack test), and pushes their Docker
images to one or more SWR registries.  Each image is BUILT ONCE and
pushed to every configured registry, then removed locally to save disk.

Two registries are configured by default:

  reg1 "trajectory"  swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com
                     /aifm.coder.exp/swegen/generated
  reg2 "platform"    swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com
                     /swesandbox/public/swe-gen/feature-implementation/generated

For every registry a pair of JSONL files is maintained in the output dir:

  all_images<suffix>.jsonl    — every accepted instance mapped to its SWR URL
  pushed_images<suffix>.jsonl — subset verified pushed to that registry

reg1 uses no suffix (all_images.jsonl / pushed_images.jsonl) for backwards
compatibility; reg2 uses "_platform".

Work runs across a thread pool (default 32 workers): each worker builds an
instance's image once, pushes it to whichever registries still need it, and
removes the local images.

Usage:
    PYTHONPATH=src .venv/bin/python3 src/push_all_verified.py \
        --run-dir runs/20260716-sol-max-full-16w \
        --proxy-env .env --workers 32

    # Dry-run (show what would be done)
    PYTHONPATH=src .venv/bin/python3 src/push_all_verified.py \
        --run-dir runs/20260716-sol-max-full-16w --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from swegen.ledger_repo import LedgerRepo

SWEGEN_IMAGE_SUFFIX = "-swegenimage"

# ── Registry definitions ────────────────────────────────────────────


@dataclass
class Registry:
    """A target SWR registry with its own JSONL bookkeeping."""

    name: str            # short label, e.g. "trajectory"
    registry: str        # host, e.g. swr-...myhuaweicloud.com
    repository: str      # repo path, e.g. aifm.coder.exp/swegen/generated
    file_suffix: str     # suffix for all_images<suffix>.jsonl (e.g. "" or "_platform")

    # populated at runtime
    all_images_path: Path = field(default=None, init=False)
    pushed_images_path: Path = field(default=None, init=False)
    already_recorded: set = field(default_factory=set, init=False)
    already_pushed: set = field(default_factory=set, init=False)

    def remote_tag(self, instance: str) -> str:
        return f"{self.registry}/{self.repository}:{instance}"

    def all_images_repo(self) -> LedgerRepo:
        return LedgerRepo(self.all_images_path)

    def pushed_images_repo(self) -> LedgerRepo:
        return LedgerRepo(self.pushed_images_path)


DEFAULT_REGISTRIES = [
    Registry(
        name="trajectory",
        registry="swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com",
        repository="aifm.coder.exp/swegen/generated",
        file_suffix="",
    ),
    Registry(
        name="platform",
        registry="swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com",
        repository="swesandbox/public/swe-gen/feature-implementation/generated",
        file_suffix="_platform",
    ),
]


# ── Image name helpers ──────────────────────────────────────────────


def _docker_image_name(name: str) -> str:
    """Mirror Harbor's Docker image-name sanitization.

    Docker image references forbid consecutive or leading/trailing separators
    (e.g. ``amol-__dukpy`` yields ``-__`` which is rejected), so after mapping
    illegal chars to ``-`` we collapse runs of separators and trim edges.
    """
    name = name.lower()
    if not name[0:1].isalnum():
        name = "0" + name
    name = re.sub(r"[^a-z0-9._-]", "-", name)
    # Docker accepts the ``__`` double-underscore Harbor uses as the org/repo
    # delimiter, but rejects a ``-`` adjacent to another separator (e.g.
    # ``amol-__dukpy`` -> ``-__``).  Fix only those illegal adjacencies without
    # disturbing the standard ``hb__<org>__<repo>`` shape.
    name = re.sub(r"-+_", "_", name)   # "-_" / "--_" -> "_"
    name = re.sub(r"_-+", "_", name)   # "_-" / "_--" -> "_"
    name = re.sub(r"-{2,}", "-", name)  # collapse runs of dashes
    name = re.sub(r"\.{2,}", ".", name)  # collapse runs of dots
    name = name.strip("._-") or "0"
    return name


def local_image_tag(instance: str) -> str:
    """Return the local Docker image tag Harbor builds for an instance."""
    image_name = _docker_image_name(f"hb__{instance}")
    if not image_name.endswith(SWEGEN_IMAGE_SUFFIX):
        image_name = f"{image_name}{SWEGEN_IMAGE_SUFFIX}"
    return f"{image_name}:latest"


# ── Ledger loading ──────────────────────────────────────────────────


def load_accepted_instances(ledger_path: Path) -> list[str]:
    """Return instance IDs with status='accepted' from the postcheck ledger."""
    repo = LedgerRepo(ledger_path)
    if repo.backend == "postgres":
        return repo.load_accepted()
    # jsonl fallback: original scan + latest-wins + accepted filter.
    latest: dict[str, dict] = {}
    if not ledger_path.is_file():
        return []
    with ledger_path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            inst = rec.get("instance")
            if not isinstance(inst, str) or not inst:
                continue
            ts = rec.get("timestamp", "")
            if inst not in latest or ts >= latest[inst].get("timestamp", ""):
                latest[inst] = rec
    return sorted(
        inst for inst, rec in latest.items()
        if rec.get("status") == "accepted"
    )


# ── JSONL helpers (thread-safe) ─────────────────────────────────────

_jsonl_lock = threading.Lock()


def load_jsonl_set(path: Path, key: str = "instance_id") -> set[str]:
    """Load a set of values from a JSONL file (jsonl) or the pushed_images
    table (postgres). The path stem selects the subset: all_images* -> every
    row for that suffix; pushed_images* -> only rows with pushed=true."""
    repo = LedgerRepo(path)
    if repo.backend == "postgres":
        from swegen import db

        stem = path.stem  # e.g. "all_images_platform" or "pushed_images"
        only_pushed = stem.startswith("pushed_images")
        prefix = "pushed_images" if only_pushed else "all_images"
        suffix = stem[len(prefix):]  # e.g. "_platform" or ""
        sql = "SELECT DISTINCT instance FROM pushed_images WHERE suffix = %s"
        params: tuple = (suffix,)
        if only_pushed:
            sql += " AND pushed = TRUE"
        try:
            rows = db.query_all(sql, params)
        except Exception:
            return set()
        return {r["instance"] for r in rows if r.get("instance")}
    # jsonl fallback
    if not path.is_file():
        return set()
    values: set[str] = set()
    with path.open(encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            v = rec.get(key)
            if isinstance(v, str):
                values.add(v)
    return values


def append_jsonl(path: Path, record: dict) -> None:
    """Append a record to a JSONL file (thread-safe)."""
    with _jsonl_lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
            f.flush()


def _append_image_record(repo: LedgerRepo, record: dict, *, reg: "Registry", pushed: bool) -> None:
    """Append an image ledger record via the repo, stamping the indexed
    columns (instance/registry/suffix/pushed) used for set-membership reads.
    The full record (with instance_id, swr_url, timestamps) is kept in payload."""
    record = {
        **record,
        "instance": record.get("instance_id"),
        "registry": reg.name,
        "suffix": reg.file_suffix,
        "pushed": pushed,
    }
    with _jsonl_lock:
        repo.append(record)


# ── Docker operations ───────────────────────────────────────────────


def image_exists_in_registry(remote_tag: str, timeout: int = 60) -> bool:
    """Check if an image already exists in the remote registry.

    Uses ``docker manifest inspect``.  Returns False on timeout or any
    error (conservative: caller will then attempt a push).
    """
    try:
        proc = subprocess.run(
            ["docker", "manifest", "inspect", remote_tag],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def image_exists_locally(local_tag: str) -> bool:
    """Check if a Docker image exists locally."""
    proc = subprocess.run(
        ["docker", "image", "inspect", local_tag],
        capture_output=True,
        timeout=30,
    )
    return proc.returncode == 0


# Base-image registries that are NOT reachable from the build host.  A task's
# Dockerfile whose FROM points here (the "postprocessed"/preloaded variant) cannot
# be built locally; we prefer the self-contained (FROM ubuntu:24.04, git clone)
# variant of the same instance from another search root.
UNREACHABLE_BASE_MARKERS = ("6sudmx",)


def _dockerfile_from_ok(dockerfile: Path) -> bool:
    """True if the Dockerfile's FROM base image is reachable from this host."""
    try:
        with dockerfile.open(encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.lstrip().upper().startswith("FROM "):
                    return not any(m in line for m in UNREACHABLE_BASE_MARKERS)
    except OSError:
        return False
    return True


def find_task_dir(instance: str, search_dirs: list[Path]) -> Path | None:
    """Find a *buildable* task directory for an instance across multiple roots.

    Prefers a self-contained Dockerfile (reachable FROM base).  Only if no root
    has one does it fall back to a preloaded/unreachable-base Dockerfile so the
    caller can report the accurate failure.
    """
    fallback: Path | None = None
    for root in search_dirs:
        candidate = root / instance
        dockerfile = candidate / "environment" / "Dockerfile"
        if dockerfile.is_file():
            if _dockerfile_from_ok(dockerfile):
                return candidate
            if fallback is None:
                fallback = candidate
    return fallback


def build_image_direct(
    instance: str,
    task_dir: Path,
    proxy_env: dict[str, str] | None = None,
    log=print,
) -> str | None:
    """Build the Docker image for an instance directly from its Dockerfile.

    Returns the local image tag on success, or None on failure.
    """
    env_dir = task_dir / "environment"
    dockerfile = env_dir / "Dockerfile"
    if not dockerfile.is_file():
        log(f"  [BUILD] No Dockerfile at {dockerfile}")
        return None

    local_tag = local_image_tag(instance)
    cmd: list[str] = ["docker", "build"]

    if proxy_env:
        for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
            value = proxy_env.get(key)
            if value:
                cmd += ["--build-arg", f"{key}={value}"]
        no_proxy = ".myhuaweicloud.com,.huaweicloud.com,100.*,10.*,.huawei.com,127.0.0.1"
        cmd += [
            "--build-arg", f"no_proxy={no_proxy}",
            "--build-arg", f"NO_PROXY={no_proxy}",
        ]

    cmd += ["-t", local_tag, str(env_dir)]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    except subprocess.TimeoutExpired:
        log(f"  [BUILD] docker build timed out for {instance}")
        return None
    if proc.returncode != 0:
        stderr = proc.stderr.strip()[-500:]
        log(f"  [BUILD] docker build failed for {instance}: {stderr}")
        return None

    if not image_exists_locally(local_tag):
        log(f"  [BUILD] Image {local_tag} not found after build")
        return None
    return local_tag


def _safe_rmi(tag: str, force: bool = True, timeout: int = 120) -> None:
    """Best-effort ``docker rmi`` that never raises."""
    cmd = ["docker", "rmi"] + (["-f"] if force else []) + [tag]
    try:
        subprocess.run(cmd, capture_output=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        pass


def push_to_registry(local_tag: str, remote_tag: str, log=print) -> bool:
    """Tag ``local_tag`` as ``remote_tag`` and push it.  Removes remote tag after."""
    tag_proc = subprocess.run(
        ["docker", "tag", local_tag, remote_tag],
        capture_output=True, text=True, timeout=60,
    )
    if tag_proc.returncode != 0:
        log(f"  [TAG] Failed: {tag_proc.stderr.strip()[-200:]}")
        return False

    log(f"  [PUSH] Pushing {remote_tag} ...")
    try:
        push_proc = subprocess.run(
            ["docker", "push", remote_tag],
            capture_output=True, text=True, timeout=1800,
        )
    except subprocess.TimeoutExpired:
        log(f"  [PUSH] Timed out: {remote_tag}")
        _safe_rmi(remote_tag)
        return False
    if push_proc.returncode != 0:
        log(f"  [PUSH] Failed: {push_proc.stderr.strip()[-200:]}")
        _safe_rmi(remote_tag)
        return False

    log(f"  [PUSH] Success: {remote_tag}")
    # Remove the remote tag locally (layers stay via the source tag until we drop it).
    _safe_rmi(remote_tag, force=False)
    return True


def remove_local_image(local_tag: str) -> None:
    """Remove a local Docker image to reclaim disk space.

    Best-effort: under heavy concurrent Docker load ``rmi`` can be slow, so we
    use a generous timeout and swallow any error — a leftover image is a disk
    concern, never a correctness one, and is swept up separately.
    """
    _safe_rmi(local_tag, force=True, timeout=300)


# ── Per-instance work ───────────────────────────────────────────────


@dataclass
class InstanceResult:
    instance: str
    pushed: list[str] = field(default_factory=list)   # registry names newly pushed
    skipped: list[str] = field(default_factory=list)  # registry names already present
    failed: list[str] = field(default_factory=list)   # registry names that failed
    build_failed: bool = False
    no_task_dir: bool = False


def process_instance(
    idx: int,
    total: int,
    instance: str,
    registries: list[Registry],
    search_dirs: list[Path],
    proxy_env: dict[str, str] | None,
    args,
) -> InstanceResult:
    """Build (if needed) and push one instance to every pending registry."""
    result = InstanceResult(instance=instance)
    prefix = f"[{idx}/{total}] {instance}"
    lines: list[str] = [f"\n{prefix}"]

    def log(msg: str) -> None:
        lines.append(msg)

    local_tag = local_image_tag(instance)

    # Record every registry's URL in its all_images file; decide which registries
    # still need a push.
    pending: list[Registry] = []
    for reg in registries:
        remote_tag = reg.remote_tag(instance)
        if instance not in reg.already_recorded:
            _append_image_record(
                reg.all_images_repo(),
                {
                    "instance_id": instance,
                    "swr_url": remote_tag,
                },
                reg=reg,
                pushed=False,
            )
            reg.already_recorded.add(instance)

        if instance in reg.already_pushed:
            log(f"  [{reg.name}] SKIP (already in pushed_images{reg.file_suffix}.jsonl)")
            result.skipped.append(reg.name)
            continue

        if not args.skip_registry_check and image_exists_in_registry(remote_tag):
            log(f"  [{reg.name}] REGISTRY already has it")
            _append_image_record(
                reg.pushed_images_repo(),
                {
                    "instance_id": instance,
                    "swr_url": remote_tag,
                    "verified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
                reg=reg,
                pushed=True,
            )
            reg.already_pushed.add(instance)
            result.skipped.append(reg.name)
            continue

        pending.append(reg)

    if not pending:
        _flush(lines)
        return result

    # Ensure a local image exists (build once, shared across registries).
    have_local = image_exists_locally(local_tag)
    built_here = False
    if not have_local:
        task_dir = find_task_dir(instance, search_dirs)
        if task_dir is None:
            log(f"  [SKIP] Task directory not found")
            result.no_task_dir = True
            for reg in pending:
                result.failed.append(reg.name)
            _flush(lines)
            return result
        if args.dry_run:
            log(f"  [DRY-RUN] Would build from {task_dir} and push to: "
                f"{', '.join(r.name for r in pending)}")
            _flush(lines)
            return result
        log(f"  [BUILD] Rebuilding from {task_dir} ...")
        local_tag = build_image_direct(instance, task_dir, proxy_env=proxy_env, log=log)
        if local_tag is None:
            log(f"  [FAIL] Could not build image")
            result.build_failed = True
            for reg in pending:
                result.failed.append(reg.name)
            _flush(lines)
            return result
        built_here = True
    else:
        log(f"  [CACHE] Local image exists")

    if args.dry_run:
        log(f"  [DRY-RUN] Would push to: {', '.join(r.name for r in pending)}")
        _flush(lines)
        return result

    # Push to each pending registry.
    for reg in pending:
        remote_tag = reg.remote_tag(instance)
        if push_to_registry(local_tag, remote_tag, log=log):
            _append_image_record(
                reg.pushed_images_repo(),
                {
                    "instance_id": instance,
                    "swr_url": remote_tag,
                    "pushed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
                reg=reg,
                pushed=True,
            )
            reg.already_pushed.add(instance)
            result.pushed.append(reg.name)
        else:
            result.failed.append(reg.name)

    # Clean up the local base image to reclaim disk (only if we built it or
    # cleanup is requested).  Always remove to bound disk usage across 177 imgs.
    remove_local_image(local_tag)
    if built_here:
        # also prune dangling build layers occasionally handled by caller
        pass

    _flush(lines)
    return result


_print_lock = threading.Lock()


def _flush(lines: list[str]) -> None:
    with _print_lock:
        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()


# ── Main ────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--instance", help="Restrict to one specific instance ID.")
    parser.add_argument("--instance-list", type=Path, default=None,
                        help="File with one instance ID per line. When given, this "
                             "is the authoritative target set (the local postcheck "
                             "ledger is NOT consulted for the accepted list).")
    parser.add_argument("--extra-task-dir", type=Path, action="append", default=[],
                        help="Additional root dir to search for <instance>/environment/"
                             "Dockerfile (repeatable). Searched after the default roots.")
    parser.add_argument("--max-instances", type=int, default=0,
                        help="Max instances to process (0 = all).")
    parser.add_argument("--workers", type=int, default=32,
                        help="Number of concurrent build+push workers (default: 32).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--proxy-env", type=Path, default=Path(".env"),
                        help="Proxy environment file for docker build args (default: .env).")
    parser.add_argument("--skip-registry-check", action="store_true",
                        help="Skip 'docker manifest inspect' existence check (faster).")
    parser.add_argument("--registries", default="trajectory,platform",
                        help="Comma-separated registry names to push to "
                             "(subset of: trajectory, platform).")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Directory for the JSONL files "
                             "(default: <run-dir>/.validation-worker)")
    args = parser.parse_args(argv)

    run_dir = args.run_dir.resolve()
    worker_dir = run_dir / ".validation-worker"
    ledger_path = worker_dir / "postcheck-status.jsonl"
    output_dir = (args.output_dir or worker_dir).resolve()

    # Select registries.
    selected = {r.strip() for r in args.registries.split(",") if r.strip()}
    registries = [r for r in DEFAULT_REGISTRIES if r.name in selected]
    if not registries:
        print(f"No valid registries selected from {args.registries!r}", file=sys.stderr)
        return 1

    # Wire up per-registry JSONL paths and existing state.
    for reg in registries:
        reg.all_images_path = output_dir / f"all_images{reg.file_suffix}.jsonl"
        reg.pushed_images_path = output_dir / f"pushed_images{reg.file_suffix}.jsonl"
        reg.already_recorded = load_jsonl_set(reg.all_images_path)
        reg.already_pushed = load_jsonl_set(reg.pushed_images_path)

    # Task search directories (in priority order).
    search_dirs = [
        worker_dir / "tasks",
        run_dir / "tasks",
        run_dir / "tasks_voyager_postprocessed",
    ]
    search_dirs += [p.resolve() for p in args.extra_task_dir]

    # Load proxy env for docker build.
    proxy_env: dict[str, str] | None = None
    proxy_env_path = args.proxy_env.resolve()
    if proxy_env_path.is_file():
        from dotenv import dotenv_values
        proxy_env = {k: v for k, v in dotenv_values(proxy_env_path).items() if v}
        print(f"Loaded proxy env from {proxy_env_path}", flush=True)

    if args.instance_list:
        if not args.instance_list.is_file():
            print(f"Instance list not found: {args.instance_list}", file=sys.stderr)
            return 1
        accepted = sorted({
            line.strip()
            for line in args.instance_list.read_text().splitlines()
            if line.strip()
        })
        print(f"Loaded {len(accepted)} instances from {args.instance_list}", flush=True)
    else:
        if not ledger_path.is_file():
            print(f"Ledger not found: {ledger_path}", file=sys.stderr)
            return 1
        accepted = load_accepted_instances(ledger_path)
        print(f"Found {len(accepted)} accepted instances in ledger", flush=True)

    if args.instance:
        if args.instance not in accepted:
            print(f"Instance {args.instance} is not in accepted list", file=sys.stderr)
            return 1
        accepted = [args.instance]

    if args.max_instances > 0:
        accepted = accepted[: args.max_instances]

    print(f"Registries: {', '.join(r.name for r in registries)}", flush=True)
    for reg in registries:
        print(f"  [{reg.name}] recorded={len(reg.already_recorded)} "
              f"pushed={len(reg.already_pushed)} -> {reg.registry}/{reg.repository}",
              flush=True)
    print(f"Processing {len(accepted)} instance(s) with {args.workers} workers", flush=True)

    total = len(accepted)
    results: list[InstanceResult] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                process_instance, i + 1, total, inst, registries,
                search_dirs, proxy_env, args,
            ): inst
            for i, inst in enumerate(accepted)
        }
        for fut in as_completed(futures):
            inst = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                print(f"  [ERROR] {inst}: {exc}", flush=True)
                results.append(InstanceResult(instance=inst, build_failed=True))

    # Summary.
    per_reg_pushed = {r.name: 0 for r in registries}
    per_reg_skipped = {r.name: 0 for r in registries}
    per_reg_failed = {r.name: 0 for r in registries}
    build_failures = 0
    missing_task = 0
    for res in results:
        if res.build_failed:
            build_failures += 1
        if res.no_task_dir:
            missing_task += 1
        for n in res.pushed:
            per_reg_pushed[n] += 1
        for n in res.skipped:
            per_reg_skipped[n] += 1
        for n in res.failed:
            per_reg_failed[n] += 1

    print("\n=== Done ===", flush=True)
    for reg in registries:
        print(f"  [{reg.name}] pushed={per_reg_pushed[reg.name]} "
              f"skipped={per_reg_skipped[reg.name]} failed={per_reg_failed[reg.name]}  "
              f"({reg.pushed_images_path})", flush=True)
    print(f"  build failures: {build_failures}  (missing task dir: {missing_task})",
          flush=True)

    total_failed = sum(per_reg_failed.values())
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
