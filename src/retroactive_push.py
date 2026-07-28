#!/usr/bin/env python3
"""Retroactively build and push verified SWE-gen Docker images to SWR.

Reads the postcheck-status.jsonl ledger, finds accepted instances, rebuilds
their Docker images via Harbor, and pushes them to the SWR registry.  Skips
instances whose image already exists in the registry.

Usage:
    # Push up to 5 verified instances
    python src/retroactive_push.py --run-dir runs/20260716-sol-max-full-16w

    # Push one specific instance
    python src/retroactive_push.py --run-dir runs/20260716-sol-max-full-16w \
        --instance 0xpolygonid__js-sdk-253

    # Dry-run (show what would be pushed)
    python src/retroactive_push.py --run-dir runs/20260716-sol-max-full-16w --dry-run
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_SWR_REGISTRY = "swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com"
DEFAULT_SWR_REPOSITORY = "aifm.coder.exp/swegen/generated"
SWEGEN_IMAGE_SUFFIX = "-swegenimage"


def _docker_image_name(name: str) -> str:
    """Mirror Harbor's Docker image-name sanitization."""
    name = name.lower()
    if not name[0:1].isalnum():
        name = "0" + name
    import re
    return re.sub(r"[^a-z0-9._-]", "-", name)


def local_image_tag(instance: str) -> str:
    """Return the local Docker image tag Harbor builds for an instance."""
    image_name = _docker_image_name(f"hb__{instance}")
    if not image_name.endswith(SWEGEN_IMAGE_SUFFIX):
        image_name = f"{image_name}{SWEGEN_IMAGE_SUFFIX}"
    return f"{image_name}:latest"


def swr_image_tag(instance: str, registry: str, repository: str) -> str:
    """Return the SWR registry image tag for an instance."""
    return f"{registry}/{repository}:{instance}"


def load_accepted_instances(ledger_path: Path) -> list[str]:
    """Return instance IDs with status='accepted' from the postcheck ledger."""
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
    return sorted(inst for inst, rec in latest.items() if rec.get("status") == "accepted")


def image_exists_in_registry(remote_tag: str) -> bool:
    """Check if an image already exists in the remote registry."""
    proc = subprocess.run(
        ["docker", "manifest", "inspect", remote_tag],
        capture_output=True,
        text=True,
        timeout=60,
    )
    return proc.returncode == 0


def build_image_direct(
    instance: str,
    task_dir: Path,
    proxy_env: dict[str, str] | None = None,
) -> str | None:
    """Build the Docker image for an instance directly from its Dockerfile.

    Returns the local image tag on success, or None on failure.
    Uses ``docker build`` with proxy build-args so RUN commands can reach
    the internet through the corporate proxy.
    """
    env_dir = task_dir / "environment"
    dockerfile = env_dir / "Dockerfile"
    if not dockerfile.is_file():
        print(f"  [BUILD] No Dockerfile at {dockerfile}", flush=True)
        return None

    local_tag = local_image_tag(instance)
    cmd: list[str] = ["docker", "build"]

    # Pass proxy settings as build-args so RUN curl/apt-get work inside
    # the build container.  The no-proxy list excludes Huawei internal
    # domains so they are reached directly.
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

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip()[-500:]
        print(f"  [BUILD] docker build failed for {instance}: {stderr}", flush=True)
        return None

    # Verify the image exists
    inspect = subprocess.run(
        ["docker", "image", "inspect", local_tag],
        capture_output=True,
        timeout=30,
    )
    if inspect.returncode != 0:
        print(f"  [BUILD] Image {local_tag} not found after build", flush=True)
        return None
    return local_tag


def push_image(local_tag: str, remote_tag: str) -> bool:
    """Tag and push a Docker image to the SWR registry."""
    # Tag
    tag_proc = subprocess.run(
        ["docker", "tag", local_tag, remote_tag],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if tag_proc.returncode != 0:
        print(f"  [TAG] Failed: {tag_proc.stderr.strip()[-200:]}", flush=True)
        return False

    # Push
    print(f"  [PUSH] Pushing {remote_tag} ...", flush=True)
    push_proc = subprocess.run(
        ["docker", "push", remote_tag],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if push_proc.returncode != 0:
        print(f"  [PUSH] Failed: {push_proc.stderr.strip()[-200:]}", flush=True)
        subprocess.run(["docker", "rmi", "-f", remote_tag], capture_output=True, timeout=30)
        return False

    print(f"  [PUSH] Success: {remote_tag}", flush=True)

    # Clean up remote tag locally
    subprocess.run(["docker", "rmi", remote_tag], capture_output=True, timeout=30)
    # Clean up local image
    subprocess.run(["docker", "rmi", local_tag], capture_output=True, timeout=30)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--instance", help="Restrict to one specific instance ID.")
    parser.add_argument("--max-instances", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--swr-registry", default=DEFAULT_SWR_REGISTRY)
    parser.add_argument("--swr-repository", default=DEFAULT_SWR_REPOSITORY)
    parser.add_argument("--skip-registry-check", action="store_true",
                        help="Skip checking if image already exists in registry (faster).")
    parser.add_argument("--proxy-env", type=Path, default=Path(".env"),
                        help="Proxy environment file for docker build args (default: .env).")
    args = parser.parse_args(argv)

    run_dir = args.run_dir.resolve()
    worker_dir = run_dir / ".validation-worker"
    ledger_path = worker_dir / "postcheck-status.jsonl"
    tasks_dir = worker_dir / "tasks"

    # Load proxy env for docker build
    proxy_env: dict[str, str] | None = None
    proxy_env_path = args.proxy_env.resolve()
    if proxy_env_path.is_file():
        from dotenv import dotenv_values
        proxy_env = {k: v for k, v in dotenv_values(proxy_env_path).items() if v}
        print(f"Loaded proxy env from {proxy_env_path}", flush=True)

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

    accepted = accepted[: args.max_instances]
    print(f"Processing up to {len(accepted)} instance(s)", flush=True)

    pushed = 0
    skipped = 0
    failed = 0

    for instance in accepted:
        remote_tag = swr_image_tag(instance, args.swr_registry, args.swr_repository)
        print(f"\n[{instance}]", flush=True)

        # Check if already in registry
        if not args.skip_registry_check:
            if image_exists_in_registry(remote_tag):
                print(f"  [SKIP] Already exists in registry: {remote_tag}", flush=True)
                skipped += 1
                continue

        # Check if image exists locally already
        local_tag = local_image_tag(instance)
        inspect = subprocess.run(
            ["docker", "image", "inspect", local_tag],
            capture_output=True,
            timeout=30,
        )
        if inspect.returncode == 0:
            print(f"  [CACHE] Local image exists: {local_tag}", flush=True)
        else:
            # Need to rebuild
            task_dir = tasks_dir / instance
            if not (task_dir / "tests" / "test.sh").is_file():
                # Try the main tasks dir
                task_dir = run_dir / "tasks" / instance
            if not (task_dir / "tests" / "test.sh").is_file():
                print(f"  [SKIP] Task directory not found for {instance}", flush=True)
                skipped += 1
                continue

            if args.dry_run:
                print(f"  [DRY-RUN] Would rebuild from {task_dir}", flush=True)
                skipped += 1
                continue

            print(f"  [BUILD] Rebuilding image from {task_dir} ...", flush=True)
            local_tag = build_image_direct(
                instance, task_dir, proxy_env=proxy_env
            )
            if local_tag is None:
                print(f"  [FAIL] Could not build image for {instance}", flush=True)
                failed += 1
                continue

        if args.dry_run:
            print(f"  [DRY-RUN] Would push {local_tag} -> {remote_tag}", flush=True)
            skipped += 1
            continue

        if push_image(local_tag, remote_tag):
            pushed += 1
        else:
            failed += 1

    print(f"\nDone: {pushed} pushed, {skipped} skipped, {failed} failed", flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())