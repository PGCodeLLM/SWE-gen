"""Copy task images that exist only in the platform SWR registry to trajectory.

Every accepted task image is meant to live in both SWR registries, but the two
have drifted: ``pushed_images`` currently lists thousands of instances with a
verified ``registry='platform'`` row and no ``registry='trajectory'`` row.  This
module runs as a small always-on pod that repeatedly closes that gap.

Each cycle it

1. asks Postgres for the platform-only backlog,
2. copies every backlogged image platform -> trajectory across a thread pool
   (pull, retag, push through the shared :mod:`push_all_verified` helpers), and
3. records a ``registry='trajectory'`` ledger row so the backlog shrinks and the
   same instance is not copied again next hour.

The two registries do NOT share a repository path -- platform publishes under
``swesandbox/public/swe-gen/feature-implementation/generated`` and trajectory
under ``aifm.coder.exp/swegen/generated`` -- so the source and destination tags
are built from separate coordinates (see :mod:`swegen.pipeline.actions`).

Local images are deleted immediately after every instance, in a ``finally``
block, because the cluster runs close to DiskPressure and a copy loop that
retains even a fraction of a 5,000-image backlog fills a node's disk long before
the cycle ends.

Usage::

    python -m swegen.tools.trajectory_sync            # hourly forever
    python -m swegen.tools.trajectory_sync --once --limit 5   # one small batch
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import threading
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from push_all_verified import (
    MAX_DOCKER_OUTPUT_BYTES,
    image_exists_in_registry,
    push_to_registry,
    remove_local_image,
)
from swegen import db
from swegen.create.claude_code_utils import redact_sensitive_text
from swegen.pipeline.actions import (
    _PROXY_ENVIRONMENT_NAMES,
    DEFAULT_SWR_HOST,
    DEFAULT_SWR_REGISTRY,
    DEFAULT_SWR_REPOSITORY,
    TRAJECTORY_SWR_HOST,
    TRAJECTORY_SWR_REGISTRY,
    TRAJECTORY_SWR_REPOSITORY,
    TRAJECTORY_SWR_SUFFIX,
)
from swegen.tools.subprocess_utils import run_bounded_command

LOGGER = logging.getLogger(__name__)

DEFAULT_SYNC_THREADS = 8
DEFAULT_SYNC_INTERVAL_SECONDS = 3600
DEFAULT_PULL_TIMEOUT_SECONDS = 1800.0

# Platform rows carry suffix '_platform'; trajectory rows carry ''.  The backlog
# is keyed on ``registry`` alone so a future suffix change cannot silently empty
# it.  ``NOT IN`` is safe here: ``instance`` is NOT NULL in the schema.
_BACKLOG_SQL = """
    WITH platform_images AS (
        SELECT DISTINCT instance
        FROM pushed_images
        WHERE registry = %s AND pushed
    ), trajectory_images AS (
        SELECT DISTINCT instance
        FROM pushed_images
        WHERE registry = %s AND pushed
    )
    SELECT instance
    FROM platform_images
    WHERE instance NOT IN (SELECT instance FROM trajectory_images)
    ORDER BY instance
"""
# pushed_images has no natural unique key (it is an append-only ledger), so
# idempotency is expressed as a guarded INSERT rather than ON CONFLICT: a
# re-run, a concurrent worker, or a restarted pod adds at most one verified
# trajectory row per instance.
_RECORD_TRAJECTORY_PUSH_SQL = """
    INSERT INTO pushed_images (
        instance, registry, suffix, swr_url, pushed, event, payload
    )
    SELECT %s, %s, %s, %s, TRUE, 'pushed_images', %s::jsonb
    WHERE NOT EXISTS (
        SELECT 1
        FROM pushed_images
        WHERE instance = %s AND registry = %s AND suffix = %s AND pushed
    )
"""


class SyncConnection(Protocol):
    """The slice of a psycopg connection this module needs."""

    def execute(self, query: str, params: tuple[object, ...] = ()) -> object: ...


class SyncStatus(StrEnum):
    """Terminal state of one instance's copy attempt."""

    SYNCED = "synced"
    ALREADY_PRESENT = "already_present"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RegistryCoordinates:
    """Source (platform) and destination (trajectory) SWR coordinates."""

    platform_host: str = DEFAULT_SWR_HOST
    platform_repository: str = DEFAULT_SWR_REPOSITORY
    platform_registry: str = DEFAULT_SWR_REGISTRY
    trajectory_host: str = TRAJECTORY_SWR_HOST
    trajectory_repository: str = TRAJECTORY_SWR_REPOSITORY
    trajectory_registry: str = TRAJECTORY_SWR_REGISTRY
    trajectory_suffix: str = TRAJECTORY_SWR_SUFFIX

    def __post_init__(self) -> None:
        for name in (
            "platform_host",
            "platform_repository",
            "platform_registry",
            "trajectory_host",
            "trajectory_repository",
            "trajectory_registry",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be blank")
        if (self.platform_host, self.platform_repository) == (
            self.trajectory_host,
            self.trajectory_repository,
        ):
            raise ValueError("platform and trajectory coordinates must differ")

    def platform_tag(self, instance: str) -> str:
        """Return the source image reference for one instance."""

        return f"{self.platform_host.strip('/')}/{self.platform_repository.strip('/')}:{instance}"

    def trajectory_tag(self, instance: str) -> str:
        """Return the destination image reference for one instance."""

        return (
            f"{self.trajectory_host.strip('/')}/{self.trajectory_repository.strip('/')}:{instance}"
        )


@dataclass(frozen=True, slots=True)
class SyncSettings:
    """Runtime knobs for the sync loop."""

    coordinates: RegistryCoordinates = RegistryCoordinates()
    threads: int = DEFAULT_SYNC_THREADS
    interval_seconds: int = DEFAULT_SYNC_INTERVAL_SECONDS
    pull_timeout_seconds: float = DEFAULT_PULL_TIMEOUT_SECONDS
    limit: int = 0  # 0 means "the whole backlog"

    def __post_init__(self) -> None:
        if self.threads <= 0:
            raise ValueError("threads must be positive")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.pull_timeout_seconds <= 0:
            raise ValueError("pull_timeout_seconds must be positive")
        if self.limit < 0:
            raise ValueError("limit must not be negative")

    @classmethod
    def from_environment(cls) -> SyncSettings:
        """Build settings from the pod's environment."""

        return cls(
            coordinates=RegistryCoordinates(
                platform_host=_environment_value("SWEGEN_SWR_HOST", DEFAULT_SWR_HOST),
                platform_repository=_environment_value(
                    "SWEGEN_SWR_REPOSITORY",
                    DEFAULT_SWR_REPOSITORY,
                ),
                platform_registry=_environment_value("SWEGEN_SWR_REGISTRY", DEFAULT_SWR_REGISTRY),
            ),
            threads=_environment_positive_integer(
                "SWEGEN_TRAJECTORY_SYNC_THREADS",
                DEFAULT_SYNC_THREADS,
            ),
            interval_seconds=_environment_positive_integer(
                "SWEGEN_TRAJECTORY_SYNC_INTERVAL_SECONDS",
                DEFAULT_SYNC_INTERVAL_SECONDS,
            ),
            pull_timeout_seconds=float(
                _environment_positive_integer(
                    "SWEGEN_TRAJECTORY_SYNC_PULL_TIMEOUT_SECONDS",
                    int(DEFAULT_PULL_TIMEOUT_SECONDS),
                )
            ),
            limit=_environment_non_negative_integer("SWEGEN_TRAJECTORY_SYNC_LIMIT", 0),
        )


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    """What happened to one instance."""

    instance: str
    status: SyncStatus
    detail: str = ""


@dataclass(slots=True)
class CycleSummary:
    """Per-cycle tally logged at the end of every scan."""

    attempted: int = 0
    synced: int = 0
    already_present: int = 0
    failed: int = 0
    cancelled: int = 0

    def record(self, outcome: SyncOutcome) -> None:
        self.attempted += 1
        if outcome.status is SyncStatus.SYNCED:
            self.synced += 1
        elif outcome.status is SyncStatus.ALREADY_PRESENT:
            self.already_present += 1
        else:
            self.failed += 1

    def as_text(self) -> str:
        return (
            f"attempted={self.attempted} synced={self.synced} "
            f"skipped_already_present={self.already_present} failed={self.failed} "
            f"cancelled={self.cancelled}"
        )


def _environment_value(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _environment_positive_integer(name: str, default: int) -> int:
    value = _environment_non_negative_integer(name, default)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _environment_non_negative_integer(name: str, default: int) -> int:
    raw_value = os.environ.get(name, "").strip()
    try:
        value = int(raw_value) if raw_value else default
    except ValueError as error:
        raise ValueError(f"{name} must be a non-negative integer") from error
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _safe_error_text(error: BaseException, max_chars: int = 500) -> str:
    """Return a bounded, credential-redacted one-line error summary."""

    detail = " ".join(redact_sensitive_text(str(error)).split())
    name = type(error).__name__
    return (f"{name}: {detail}" if detail else name)[:max_chars]


def proxy_environment() -> dict[str, str]:
    """Return the proxy variables SWR traffic needs, exactly as Push forwards them.

    ``NO_PROXY`` is the load-bearing one: it lists ``.myhuaweicloud.com`` so the
    pull and push reach SWR directly instead of through the corporate proxy.
    """

    return {name: value for name in _PROXY_ENVIRONMENT_NAMES if (value := os.environ.get(name, ""))}


def _docker_environment() -> dict[str, str]:
    return {**os.environ, **proxy_environment()}


def pull_image(remote_tag: str, *, timeout_seconds: float = DEFAULT_PULL_TIMEOUT_SECONDS) -> bool:
    """Pull one image, returning False (never raising) on any failure."""

    LOGGER.info("  [PULL] Pulling %s ...", remote_tag)
    try:
        result = run_bounded_command(
            ["docker", "pull", remote_tag],
            timeout_seconds=timeout_seconds,
            max_output_bytes=MAX_DOCKER_OUTPUT_BYTES,
            env=_docker_environment(),
            redactor=redact_sensitive_text,
        )
    except (OSError, RuntimeError, TimeoutError) as error:
        LOGGER.warning("  [PULL] Failed: %s", _safe_error_text(error))
        return False
    if result.returncode != 0:
        LOGGER.warning("  [PULL] Failed: %s", result.tail or "<empty>")
        return False
    return True


def load_backlog(
    connection: SyncConnection,
    coordinates: RegistryCoordinates,
    *,
    limit: int = 0,
) -> list[str]:
    """Return instances pushed to platform that trajectory is still missing."""

    cursor = connection.execute(
        _BACKLOG_SQL,
        (coordinates.platform_registry, coordinates.trajectory_registry),
    )
    fetchall = getattr(cursor, "fetchall", None)
    if not callable(fetchall):
        raise RuntimeError("backlog query returned an invalid cursor")
    instances: list[str] = []
    for row in fetchall():
        if not isinstance(row, Mapping):
            raise RuntimeError("backlog query returned an invalid row")
        instance = row.get("instance")
        if isinstance(instance, str) and instance:
            instances.append(instance)
    return instances[:limit] if limit > 0 else instances


def record_trajectory_push(
    connection: SyncConnection,
    instance: str,
    *,
    coordinates: RegistryCoordinates,
    now: datetime | None = None,
) -> bool:
    """Record a verified trajectory push; return True when a row was written."""

    swr_url = coordinates.trajectory_tag(instance)
    verified_at = (now or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = json.dumps(
        {
            "instance": instance,
            "instance_id": instance,
            "swr_url": swr_url,
            "registry": coordinates.trajectory_registry,
            "suffix": coordinates.trajectory_suffix,
            "pushed": True,
            "source": "trajectory_sync",
            "verified_at": verified_at,
        },
        sort_keys=True,
    )
    cursor = connection.execute(
        _RECORD_TRAJECTORY_PUSH_SQL,
        (
            instance,
            coordinates.trajectory_registry,
            coordinates.trajectory_suffix,
            swr_url,
            payload,
            instance,
            coordinates.trajectory_registry,
            coordinates.trajectory_suffix,
        ),
    )
    return getattr(cursor, "rowcount", 0) == 1


def sync_instance(
    instance: str,
    settings: SyncSettings,
) -> SyncOutcome:
    """Copy one image to trajectory, always removing every local tag afterwards.

    Never raises: a single bad instance must not take down the batch, so every
    failure mode is folded into a ``FAILED`` outcome with a redacted detail.
    """

    coordinates = settings.coordinates
    platform_tag = coordinates.platform_tag(instance)
    trajectory_tag = coordinates.trajectory_tag(instance)
    # Only the two remote aliases this job creates are cleaned up. The Harbor
    # local tag (hb__<instance>-swegenimage) is deliberately left alone: a
    # validate/push worker on the same node may be using it right now.
    cleanup_tags = (platform_tag, trajectory_tag)
    try:
        if image_exists_in_registry(trajectory_tag):
            # Nothing to copy, but the ledger disagreed with the registry.
            # Recording it keeps this instance out of every future backlog.
            _record_push_safely(instance, coordinates)
            return SyncOutcome(instance, SyncStatus.ALREADY_PRESENT)
        if not pull_image(platform_tag, timeout_seconds=settings.pull_timeout_seconds):
            return SyncOutcome(
                instance,
                SyncStatus.FAILED,
                f"pull from platform failed: {platform_tag}",
            )
        # push_to_registry performs the retag and drops the remote alias itself.
        if not push_to_registry(platform_tag, trajectory_tag, log=LOGGER.info):
            return SyncOutcome(
                instance,
                SyncStatus.FAILED,
                f"push to trajectory failed: {trajectory_tag}",
            )
        recorded = _record_push_safely(instance, coordinates)
        return SyncOutcome(
            instance,
            SyncStatus.SYNCED,
            "" if recorded else "pushed, but the ledger row was not written",
        )
    except Exception as error:  # defensive: one instance may never kill the run
        return SyncOutcome(instance, SyncStatus.FAILED, _safe_error_text(error))
    finally:
        for cleanup_tag in cleanup_tags:
            try:
                remove_local_image(cleanup_tag)
            except Exception as error:
                LOGGER.warning(
                    "failed to remove local image alias %s: %s",
                    cleanup_tag,
                    _safe_error_text(error),
                )


def _record_push_safely(instance: str, coordinates: RegistryCoordinates) -> bool:
    """Write the trajectory ledger row on this thread's own DB connection.

    A ledger failure must not undo a completed push (the image really is in the
    registry), so it is logged and the instance is simply retried next cycle.
    """

    try:
        # Each thread takes its own pooled connection (never shares one) and
        # commits explicitly, mirroring swegen.ledger_repo's ledger INSERT.
        with db.connection() as connection:
            with connection.transaction():
                return record_trajectory_push(connection, instance, coordinates=coordinates)
    except Exception as error:
        LOGGER.warning(
            "failed to record trajectory push for %s: %s",
            instance,
            _safe_error_text(error),
        )
        return False


def run_cycle(
    settings: SyncSettings,
    *,
    stop_event: threading.Event | None = None,
) -> CycleSummary:
    """Scan for the backlog and copy every instance across the thread pool."""

    stop_event = stop_event or threading.Event()
    summary = CycleSummary()
    with db.connection() as connection:
        backlog = load_backlog(connection, settings.coordinates, limit=settings.limit)
    LOGGER.info(
        "trajectory sync backlog: %d instance(s) in %s but not %s",
        len(backlog),
        settings.coordinates.platform_registry,
        settings.coordinates.trajectory_registry,
    )
    if not backlog or stop_event.is_set():
        return summary

    started_at = time.monotonic()
    executor = ThreadPoolExecutor(
        max_workers=settings.threads,
        thread_name_prefix="trajectory-sync",
    )
    try:
        futures = {
            executor.submit(sync_instance, instance, settings): instance for instance in backlog
        }
        for future in as_completed(futures):
            if stop_event.is_set():
                # Finish what is already running (so no local image is
                # stranded) but never start another instance.
                for pending in futures:
                    pending.cancel()
            instance = futures[future]
            try:
                outcome = future.result()
            except CancelledError:
                summary.cancelled += 1
                continue
            except Exception as error:  # pragma: no cover - sync_instance swallows
                outcome = SyncOutcome(instance, SyncStatus.FAILED, _safe_error_text(error))
            summary.record(outcome)
            if outcome.status is SyncStatus.FAILED:
                LOGGER.warning("trajectory sync failed for %s: %s", instance, outcome.detail)
            else:
                LOGGER.info("trajectory sync %s for %s", outcome.status.value, instance)
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    LOGGER.info(
        "trajectory sync cycle finished in %.1fs: %s",
        time.monotonic() - started_at,
        summary.as_text(),
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="run a single scan and exit")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="process at most this many instances per cycle (0 means the whole backlog)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the hourly trajectory sync loop until SIGTERM."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    arguments = _parser().parse_args(argv)
    settings = SyncSettings.from_environment()
    if arguments.limit is not None:
        settings = replace(settings, limit=arguments.limit)
    stop_event = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        # In-flight copies are allowed to finish so their local images are
        # always removed by sync_instance's finally block.
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    LOGGER.info(
        "trajectory sync starting: threads=%d interval=%ds source=%s target=%s proxy=%s",
        settings.threads,
        settings.interval_seconds,
        settings.coordinates.platform_tag("<instance>"),
        settings.coordinates.trajectory_tag("<instance>"),
        ",".join(sorted(proxy_environment())) or "<none>",
    )
    try:
        while not stop_event.is_set():
            try:
                run_cycle(settings, stop_event=stop_event)
            except Exception as error:
                LOGGER.error("trajectory sync cycle failed: %s", _safe_error_text(error))
            if arguments.once:
                break
            stop_event.wait(settings.interval_seconds)
    finally:
        db.close_pool()
    return 0


__all__ = [
    "CycleSummary",
    "RegistryCoordinates",
    "SyncOutcome",
    "SyncSettings",
    "SyncStatus",
    "load_backlog",
    "main",
    "proxy_environment",
    "pull_image",
    "record_trajectory_push",
    "run_cycle",
    "sync_instance",
]


if __name__ == "__main__":  # pragma: no cover - exercised by the container runtime
    raise SystemExit(main())
