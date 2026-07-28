#!/usr/bin/env python3
"""Export pushed_images*.jsonl by querying an SWR registry namespace.

For each configured registry this script determines which instance images
actually exist in the registry and (over)writes that registry's
``pushed_images<suffix>.jsonl`` with the verified subset.

Because the SWR v2 registry API (tags/list) is unreachable through the
corporate proxy, the reliable method is: take the candidate instance IDs
from ``all_images<suffix>.jsonl`` and verify each with ``docker pull -q``
(removing the pulled image immediately).  Verification runs concurrently.

Registries (by --registries name, comma separated):
  trajectory  swr-coder-data-trajectory-o84wch.swr-pro.myhuaweicloud.com
              /aifm.coder.exp/swegen/generated            -> pushed_images.jsonl
  platform    swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com
              /swesandbox/public/swe-gen/feature-implementation/generated
                                                          -> pushed_images_platform.jsonl

Usage:
    PYTHONPATH=src .venv/bin/python3 src/export_pushed_images.py \
        --output-dir runs/20260716-sol-max-full-16w/.validation-worker \
        --registries trajectory,platform --workers 32 --show-missing
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Registry:
    name: str
    registry: str
    repository: str
    file_suffix: str

    def remote_tag(self, instance: str) -> str:
        return f"{self.registry}/{self.repository}:{instance}"


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


def list_registry_tags(registry: str, repository: str) -> list[str]:
    """Best-effort tag listing via the Docker v2 API (often blocked by proxy)."""
    import shutil
    image_ref = f"docker://{registry}/{repository}"
    if shutil.which("skopeo"):
        try:
            proc = subprocess.run(
                ["skopeo", "list-tags", image_ref],
                capture_output=True, text=True, timeout=120,
            )
            if proc.returncode == 0:
                return json.loads(proc.stdout).get("Tags", [])
        except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
            pass
    return []


def verify_tag_exists(remote_tag: str) -> bool:
    """Verify a tag exists via ``docker pull -q``; remove the pulled image after."""
    try:
        proc = subprocess.run(
            ["docker", "pull", "-q", remote_tag],
            capture_output=True, text=True, timeout=180,
        )
        if proc.returncode == 0:
            subprocess.run(["docker", "rmi", remote_tag], capture_output=True, timeout=30)
            return True
        return False
    except (subprocess.TimeoutExpired, OSError):
        return False


def load_jsonl_set(path: Path, key: str = "instance_id") -> set[str]:
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


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, sort_keys=True) + "\n")


_print_lock = threading.Lock()


def export_registry(reg: Registry, output_dir: Path, workers: int, show_missing: bool) -> None:
    all_images_path = output_dir / f"all_images{reg.file_suffix}.jsonl"
    pushed_images_path = output_dir / f"pushed_images{reg.file_suffix}.jsonl"

    print(f"\n=== [{reg.name}] {reg.registry}/{reg.repository} ===", flush=True)

    # Try the API first; fall back to verifying all_images candidates.
    tags = list_registry_tags(reg.registry, reg.repository)
    if tags:
        candidates = sorted(set(tags))
        print(f"  Registry API returned {len(candidates)} tags", flush=True)
    else:
        candidates = sorted(load_jsonl_set(all_images_path))
        print(f"  Registry API unavailable; verifying {len(candidates)} "
              f"candidates from {all_images_path.name} via docker pull", flush=True)

    if not candidates:
        print(f"  No candidates for {reg.name}; skipping.", flush=True)
        write_jsonl(pushed_images_path, [])
        return

    verified: list[str] = []
    done = 0

    def _check(inst: str) -> tuple[str, bool]:
        return inst, verify_tag_exists(reg.remote_tag(inst))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_check, inst): inst for inst in candidates}
        for fut in as_completed(futures):
            inst, ok = fut.result()
            done += 1
            if ok:
                verified.append(inst)
            with _print_lock:
                mark = "✓" if ok else "✗"
                sys.stdout.write(f"  [{done}/{len(candidates)}] {mark} {inst}\n")
                sys.stdout.flush()

    verified.sort()
    records = [
        {
            "instance_id": inst,
            "swr_url": reg.remote_tag(inst),
            "verified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        for inst in verified
    ]
    write_jsonl(pushed_images_path, records)
    print(f"  Wrote {len(records)} verified entries to {pushed_images_path}", flush=True)

    if show_missing:
        all_ids = load_jsonl_set(all_images_path)
        missing = sorted(all_ids - set(verified))
        print(f"  Missing from registry ({len(missing)}):", flush=True)
        for inst in missing:
            print(f"    {inst}", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Directory containing all_images*.jsonl; "
                             "pushed_images*.jsonl are written here.")
    parser.add_argument("--registries", default="trajectory,platform",
                        help="Comma-separated registry names (subset of: "
                             "trajectory, platform).")
    parser.add_argument("--workers", type=int, default=32,
                        help="Concurrent docker-pull verifications (default: 32).")
    parser.add_argument("--show-missing", action="store_true",
                        help="Print instances in all_images but not verified in registry.")
    args = parser.parse_args(argv)

    output_dir = args.output_dir.resolve()
    selected = {r.strip() for r in args.registries.split(",") if r.strip()}
    registries = [r for r in DEFAULT_REGISTRIES if r.name in selected]
    if not registries:
        print(f"No valid registries selected from {args.registries!r}", file=sys.stderr)
        return 1

    for reg in registries:
        export_registry(reg, output_dir, args.workers, args.show_missing)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
