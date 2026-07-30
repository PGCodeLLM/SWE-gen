#!/usr/bin/env python3
"""Stop k3s worker containers whose Kubernetes Pod UID no longer exists."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from collections.abc import Sequence

DEFAULT_NAMESPACE = "swegen-pipeline"
DEFAULT_MIN_AGE_SECONDS = 300
DEFAULT_STOP_TIMEOUT_SECONDS = 10
WORKER_POD_PATTERN = re.compile(r"^swegen-(?:generate|validate|repair|reward|push)(?:-|$)")


def _run_json(command: Sequence[str]) -> dict[str, object]:
    completed = subprocess.run(
        list(command),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    value = json.loads(completed.stdout)
    if not isinstance(value, dict):
        raise RuntimeError(f"command returned non-object JSON: {' '.join(command)}")
    return value


def _live_pod_uids(namespace: str) -> set[str]:
    payload = _run_json(["k3s", "kubectl", "get", "pods", "-n", namespace, "-o", "json"])
    live: set[str] = set()
    for item in payload.get("items", []):
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata", {})
        if isinstance(metadata, dict) and isinstance(metadata.get("uid"), str):
            live.add(metadata["uid"])
    return live


def _orphaned_worker_containers(
    namespace: str,
    live_uids: set[str],
    *,
    min_age_seconds: int,
    now_ns: int,
) -> list[tuple[str, str, str]]:
    payload = _run_json(["k3s", "crictl", "ps", "-a", "-o", "json"])
    orphans: list[tuple[str, str, str]] = []
    for container in payload.get("containers", []):
        if not isinstance(container, dict):
            continue
        labels = container.get("labels", {})
        if not isinstance(labels, dict):
            continue
        pod_namespace = labels.get("io.kubernetes.pod.namespace")
        pod_name = labels.get("io.kubernetes.pod.name")
        pod_uid = labels.get("io.kubernetes.pod.uid")
        container_id = container.get("id")
        created_at = container.get("createdAt")
        if (
            pod_namespace != namespace
            or not isinstance(pod_name, str)
            or WORKER_POD_PATTERN.match(pod_name) is None
            or not isinstance(pod_uid, str)
            or pod_uid in live_uids
            or not isinstance(container_id, str)
            or not container_id
        ):
            continue
        try:
            age_seconds = (now_ns - int(created_at)) / 1_000_000_000
        except (TypeError, ValueError):
            continue
        if age_seconds < min_age_seconds:
            continue
        orphans.append((container_id, pod_name, str(container.get("state", "UNKNOWN"))))
    return orphans


def reconcile(
    *,
    namespace: str,
    min_age_seconds: int,
    stop_timeout_seconds: int,
    dry_run: bool,
) -> int:
    live_uids = _live_pod_uids(namespace)
    orphans = _orphaned_worker_containers(
        namespace,
        live_uids,
        min_age_seconds=min_age_seconds,
        now_ns=time.time_ns(),
    )
    for container_id, pod_name, state in orphans:
        print(
            f"orphan pod={pod_name} container={container_id[:12]} state={state}",
            flush=True,
        )
        if dry_run:
            continue
        if state == "CONTAINER_RUNNING":
            subprocess.run(
                [
                    "k3s",
                    "crictl",
                    "stop",
                    "--timeout",
                    str(stop_timeout_seconds),
                    container_id,
                ],
                check=True,
                timeout=stop_timeout_seconds + 15,
            )
        subprocess.run(
            ["k3s", "crictl", "rm", container_id],
            check=True,
            timeout=30,
        )
    print(f"reconciled={0 if dry_run else len(orphans)} candidates={len(orphans)}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--min-age-seconds", type=int, default=DEFAULT_MIN_AGE_SECONDS)
    parser.add_argument(
        "--stop-timeout-seconds",
        type=int,
        default=DEFAULT_STOP_TIMEOUT_SECONDS,
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.min_age_seconds < 60:
        parser.error("--min-age-seconds must be at least 60")
    if args.stop_timeout_seconds < 1:
        parser.error("--stop-timeout-seconds must be positive")
    try:
        return reconcile(
            namespace=args.namespace,
            min_age_seconds=args.min_age_seconds,
            stop_timeout_seconds=args.stop_timeout_seconds,
            dry_run=args.dry_run,
        )
    except (OSError, subprocess.SubprocessError, ValueError, RuntimeError) as error:
        print(f"reconcile failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
