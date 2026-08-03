"""Latched Kubernetes circuit breaker for the distributed Generate stage."""

from __future__ import annotations

import argparse
import json
import os
import signal
import ssl
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from swegen import db

_SERVICE_ACCOUNT_ROOT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
_SAMPLE_SQL = """
    WITH breaker AS (
        SELECT reset_at
        FROM pipeline_stage_circuit_breakers
        WHERE stage = %s
    ), boundary AS (
        SELECT greatest(
            clock_timestamp() - make_interval(secs => %s),
            COALESCE((SELECT reset_at FROM breaker), '-infinity'::timestamptz)
        ) AS since
    )
    SELECT
        count(*)::integer AS sample_count,
        count(*) FILTER (WHERE result.status = 'failed')::integer AS failure_count
    FROM pipeline_stage_results AS result
    CROSS JOIN boundary
    WHERE result.stage = %s
      AND result.finished_at >= boundary.since
"""
_ENSURE_BREAKER_SQL = """
    INSERT INTO pipeline_stage_circuit_breakers (
        stage, deployment_name, is_open, window_seconds, minimum_samples,
        failure_rate_threshold, updated_at
    ) VALUES (%s, %s, false, %s, %s, %s, clock_timestamp())
    ON CONFLICT (stage) DO UPDATE
    SET deployment_name = EXCLUDED.deployment_name,
        window_seconds = EXCLUDED.window_seconds,
        minimum_samples = EXCLUDED.minimum_samples,
        failure_rate_threshold = EXCLUDED.failure_rate_threshold,
        updated_at = clock_timestamp()
"""
_LOCK_BREAKER_SQL = """
    SELECT is_open, tripped_at, reset_at
    FROM pipeline_stage_circuit_breakers
    WHERE stage = %s
    FOR UPDATE
"""
_TRIP_BREAKER_SQL = """
    UPDATE pipeline_stage_circuit_breakers
    SET is_open = true,
        sample_count = %s,
        failure_count = %s,
        failure_rate = %s,
        trip_count = trip_count + 1,
        tripped_at = %s,
        reason = %s,
        updated_at = %s
    WHERE stage = %s AND NOT is_open
"""
_RESET_BREAKER_SQL = """
    UPDATE pipeline_stage_circuit_breakers
    SET is_open = false,
        sample_count = 0,
        failure_count = 0,
        failure_rate = NULL,
        reset_at = %s,
        reason = %s,
        updated_at = %s
    WHERE stage = %s
"""
_INSERT_EVENT_SQL = """
    INSERT INTO pipeline_circuit_breaker_events (
        stage, deployment_name, event, occurred_at, reason,
        window_seconds, minimum_samples, failure_rate_threshold,
        sample_count, failure_count, failure_rate
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


class BreakerConnection(Protocol):
    def execute(self, query: str, params: tuple[object, ...] = ()) -> object: ...

    def transaction(self) -> object: ...


class DeploymentScaler(Protocol):
    def scale_to_zero(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class BreakerPolicy:
    stage: str = "generate"
    deployment_name: str = "swegen-generate"
    namespace: str = "swegen-pipeline"
    window_seconds: int = 300
    minimum_samples: int = 20
    failure_rate_threshold: float = 0.5
    poll_seconds: float = 15.0

    def __post_init__(self) -> None:
        if not self.stage.strip():
            raise ValueError("stage must not be blank")
        if not self.deployment_name.strip():
            raise ValueError("deployment_name must not be blank")
        if not self.namespace.strip():
            raise ValueError("namespace must not be blank")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.minimum_samples <= 0:
            raise ValueError("minimum_samples must be positive")
        if not 0 < self.failure_rate_threshold < 1:
            raise ValueError("failure_rate_threshold must be between zero and one")
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")


@dataclass(frozen=True, slots=True)
class BreakerSample:
    sample_count: int
    failure_count: int

    def __post_init__(self) -> None:
        if self.sample_count < 0 or self.failure_count < 0:
            raise ValueError("sample counts must be non-negative")
        if self.failure_count > self.sample_count:
            raise ValueError("failure_count must not exceed sample_count")

    @property
    def failure_rate(self) -> float | None:
        if self.sample_count == 0:
            return None
        return self.failure_count / self.sample_count

    def trips(self, policy: BreakerPolicy) -> bool:
        rate = self.failure_rate
        return (
            self.sample_count >= policy.minimum_samples
            and rate is not None
            and rate > policy.failure_rate_threshold
        )


def _row_mapping(row: object | None) -> Mapping[str, object]:
    if not isinstance(row, Mapping):
        raise RuntimeError("circuit-breaker query returned an invalid row")
    return row


def _fetchone(cursor: object) -> object | None:
    fetchone = getattr(cursor, "fetchone", None)
    if not callable(fetchone):
        raise RuntimeError("circuit-breaker query returned an invalid cursor")
    return fetchone()


def ensure_breaker(connection: BreakerConnection, policy: BreakerPolicy) -> None:
    connection.execute(
        _ENSURE_BREAKER_SQL,
        (
            policy.stage,
            policy.deployment_name,
            policy.window_seconds,
            policy.minimum_samples,
            policy.failure_rate_threshold,
        ),
    )


def sample_results(connection: BreakerConnection, policy: BreakerPolicy) -> BreakerSample:
    row = _row_mapping(
        _fetchone(
            connection.execute(
                _SAMPLE_SQL,
                (policy.stage, policy.window_seconds, policy.stage),
            )
        )
    )
    return BreakerSample(
        sample_count=int(row["sample_count"]),
        failure_count=int(row["failure_count"]),
    )


def breaker_is_open(connection: BreakerConnection, policy: BreakerPolicy) -> bool:
    row = _row_mapping(_fetchone(connection.execute(_LOCK_BREAKER_SQL, (policy.stage,))))
    return bool(row["is_open"])


def trip_breaker(
    connection: BreakerConnection,
    policy: BreakerPolicy,
    sample: BreakerSample,
    *,
    now: datetime,
) -> bool:
    rate = sample.failure_rate
    if rate is None:
        raise ValueError("cannot trip from an empty sample")
    reason = (
        f"Generate terminal failure rate {rate:.1%} exceeded "
        f"{policy.failure_rate_threshold:.1%}: {sample.failure_count}/"
        f"{sample.sample_count} results in {policy.window_seconds}s"
    )
    cursor = connection.execute(
        _TRIP_BREAKER_SQL,
        (
            sample.sample_count,
            sample.failure_count,
            rate,
            now,
            reason,
            now,
            policy.stage,
        ),
    )
    if getattr(cursor, "rowcount", None) != 1:
        return False
    connection.execute(
        _INSERT_EVENT_SQL,
        (
            policy.stage,
            policy.deployment_name,
            "tripped",
            now,
            reason,
            policy.window_seconds,
            policy.minimum_samples,
            policy.failure_rate_threshold,
            sample.sample_count,
            sample.failure_count,
            rate,
        ),
    )
    return True


def reset_breaker(
    connection: BreakerConnection,
    policy: BreakerPolicy,
    *,
    reason: str,
    now: datetime,
) -> None:
    reason = reason.strip()
    if not reason:
        raise ValueError("reset reason must not be blank")
    connection.execute(_RESET_BREAKER_SQL, (now, reason, now, policy.stage))
    connection.execute(
        _INSERT_EVENT_SQL,
        (
            policy.stage,
            policy.deployment_name,
            "reset",
            now,
            reason,
            policy.window_seconds,
            policy.minimum_samples,
            policy.failure_rate_threshold,
            0,
            0,
            None,
        ),
    )


class KubernetesDeploymentScaler:
    """Minimal in-cluster Kubernetes client scoped to one Deployment."""

    def __init__(self, policy: BreakerPolicy) -> None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "").strip()
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443").strip()
        if not host:
            raise RuntimeError("KUBERNETES_SERVICE_HOST is not set")
        token_path = _SERVICE_ACCOUNT_ROOT / "token"
        ca_path = _SERVICE_ACCOUNT_ROOT / "ca.crt"
        self._token = token_path.read_text(encoding="utf-8").strip()
        context = ssl.create_default_context(cafile=str(ca_path))
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=context),
        )
        self._policy = policy
        self._api_root = f"https://{host}:{port}"
        self._url = (
            f"{self._api_root}/apis/apps/v1/namespaces/"
            f"{policy.namespace}/deployments/{policy.deployment_name}"
        )
        self._pod_collection_url = (
            f"{self._api_root}/api/v1/namespaces/{policy.namespace}/pods"
            f"?labelSelector=swegen.pgcode%2Fstage%3D{policy.stage}"
        )

    def _request(
        self,
        method: str,
        payload: dict[str, object] | None = None,
        *,
        url: str | None = None,
    ) -> dict[str, object]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url or self._url,
            data=body,
            method=method,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/json",
                "Content-Type": "application/merge-patch+json",
            },
        )
        try:
            with self._opener.open(request, timeout=10) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")[-1000:]
            raise RuntimeError(
                f"Kubernetes API {method} failed with HTTP {error.code}: {detail}"
            ) from error
        except urllib.error.URLError as error:
            raise RuntimeError(f"Kubernetes API {method} failed: {error.reason}") from error

    def scale_to_zero(self) -> bool:
        current = self._request("GET")
        spec = current.get("spec")
        replicas = spec.get("replicas") if isinstance(spec, Mapping) else None
        if replicas == 0:
            # Already scaled, but a previously-terminating Pod can still be
            # burning deliveries, so keep sweeping until they are really gone.
            self._delete_lingering_pods()
            return False
        self._request("PATCH", {"spec": {"replicas": 0}})
        self._delete_lingering_pods()
        return True

    def _delete_lingering_pods(self) -> int:
        """Force-delete stage Pods that outlive the scale-to-zero.

        Scaling the Deployment only sets a deletionTimestamp; the Pod then runs
        out its terminationGracePeriodSeconds, which Generate sets to 18000s so
        a real Claude Code session can finish. A crash-looping worker is not a
        real session: on 2026-08-03 two such Pods survived the trip and burned
        29,782 deliveries over 3.5 hours at ~0.3s each. Tripping the breaker
        means the stage is known-bad, so the grace period no longer protects
        anything worth keeping.
        """

        deleted = 0
        try:
            listing = self._request("GET", url=self._pod_collection_url)
        except RuntimeError:
            # Never let a listing failure stop the scale-to-zero above.
            return 0
        items = listing.get("items")
        if not isinstance(items, list):
            return 0
        for item in items:
            if not isinstance(item, Mapping):
                continue
            metadata = item.get("metadata")
            if not isinstance(metadata, Mapping):
                continue
            name = metadata.get("name")
            if not isinstance(name, str) or not name:
                continue
            url = f"{self._api_root}/api/v1/namespaces/{self._policy.namespace}/pods/{name}"
            try:
                self._request("DELETE", {"gracePeriodSeconds": 0}, url=url)
                deleted += 1
            except RuntimeError:
                # Already gone, or raced with the controller. Keep sweeping.
                continue
        return deleted


def reconcile_once(
    connection: BreakerConnection,
    policy: BreakerPolicy,
    scaler: DeploymentScaler,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[bool, BreakerSample | None, bool]:
    """Reconcile once, returning (open, sample, scaled)."""

    with connection.transaction():  # type: ignore[attr-defined]
        ensure_breaker(connection, policy)
        if breaker_is_open(connection, policy):
            open_breaker = True
            sample = None
        else:
            sample = sample_results(connection, policy)
            open_breaker = sample.trips(policy)
            if open_breaker:
                trip_breaker(connection, policy, sample, now=now())
    scaled = scaler.scale_to_zero() if open_breaker else False
    return open_breaker, sample, scaled


def policy_from_environment() -> BreakerPolicy:
    return BreakerPolicy(
        stage=os.environ.get("SWEGEN_BREAKER_STAGE", "generate"),
        deployment_name=os.environ.get("SWEGEN_BREAKER_DEPLOYMENT_NAME", "swegen-generate"),
        namespace=os.environ.get("SWEGEN_BREAKER_NAMESPACE", "swegen-pipeline"),
        window_seconds=int(os.environ.get("SWEGEN_BREAKER_WINDOW_SECONDS", "300")),
        minimum_samples=int(os.environ.get("SWEGEN_BREAKER_MINIMUM_SAMPLES", "20")),
        failure_rate_threshold=float(
            os.environ.get("SWEGEN_BREAKER_FAILURE_RATE_THRESHOLD", "0.5")
        ),
        poll_seconds=float(os.environ.get("SWEGEN_BREAKER_POLL_SECONDS", "15")),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="clear the durable latch; this does not scale Generate up",
    )
    parser.add_argument("--reason", default="operator reset")
    parser.add_argument("--once", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    policy = policy_from_environment()
    pool = db.get_pool()
    try:
        if args.reset:
            timestamp = datetime.now(UTC)
            with pool.connection() as connection:
                with connection.transaction():
                    ensure_breaker(connection, policy)
                    reset_breaker(connection, policy, reason=args.reason, now=timestamp)
            print(f"reset {policy.stage} circuit breaker at {timestamp.isoformat()}", flush=True)
            return

        scaler = KubernetesDeploymentScaler(policy)
        stop = False

        def request_stop(_signum: int, _frame: object) -> None:
            nonlocal stop
            stop = True

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        while not stop:
            try:
                with pool.connection() as connection:
                    is_open, sample, scaled = reconcile_once(connection, policy, scaler)
                if sample is not None:
                    rate = sample.failure_rate
                    rate_text = "n/a" if rate is None else f"{rate:.1%}"
                    print(
                        f"breaker open={is_open} sample={sample.sample_count} "
                        f"failures={sample.failure_count} rate={rate_text} scaled={scaled}",
                        flush=True,
                    )
                elif scaled:
                    print("breaker is latched; scaled Generate to zero", flush=True)
                else:
                    print("breaker is latched; Generate remains at zero", flush=True)
            except Exception as error:
                print(
                    f"circuit-breaker reconciliation failed: {type(error).__name__}: {error}",
                    flush=True,
                )
            if args.once:
                return
            time.sleep(policy.poll_seconds)
    finally:
        db.close_pool()


if __name__ == "__main__":
    main()
