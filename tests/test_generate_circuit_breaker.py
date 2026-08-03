from __future__ import annotations

from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime

from swegen.pipeline.generate_circuit_breaker import (
    BreakerPolicy,
    BreakerSample,
    reconcile_once,
)

NOW = datetime(2026, 8, 2, 18, 45, tzinfo=UTC)


class Cursor:
    def __init__(self, row: object | None = None, *, rowcount: int = 0) -> None:
        self.row = row
        self.rowcount = rowcount

    def fetchone(self) -> object | None:
        return self.row


class Connection:
    def __init__(self, results: Sequence[Cursor]) -> None:
        self.results = deque(results)
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.transactions = 0

    def execute(self, query: str, params: tuple[object, ...] = ()) -> Cursor:
        self.calls.append((" ".join(query.split()), params))
        return self.results.popleft()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.transactions += 1
        yield


class Scaler:
    def __init__(self) -> None:
        self.calls = 0

    def scale_to_zero(self) -> bool:
        self.calls += 1
        return True


def test_breaker_requires_minimum_sample_and_strictly_more_than_half_failures() -> None:
    policy = BreakerPolicy(minimum_samples=20, failure_rate_threshold=0.5)

    assert BreakerSample(19, 19).trips(policy) is False
    assert BreakerSample(20, 10).trips(policy) is False
    assert BreakerSample(20, 11).trips(policy) is True


def test_reconcile_trips_persists_event_and_scales_generate_to_zero() -> None:
    connection = Connection(
        [
            Cursor(),
            Cursor({"is_open": False, "tripped_at": None, "reset_at": None}),
            Cursor({"sample_count": 762, "failure_count": 724}),
            Cursor(rowcount=1),
            Cursor(),
        ]
    )
    scaler = Scaler()

    is_open, sample, scaled = reconcile_once(
        connection,
        BreakerPolicy(),
        scaler,
        now=lambda: NOW,
    )

    assert is_open is True
    assert sample == BreakerSample(762, 724)
    assert scaled is True
    assert scaler.calls == 1
    assert connection.transactions == 1
    assert any("UPDATE pipeline_stage_circuit_breakers" in query for query, _ in connection.calls)
    assert any(
        "INSERT INTO pipeline_circuit_breaker_events" in query for query, _ in connection.calls
    )


def test_latched_breaker_scales_zero_without_sampling_or_auto_reset() -> None:
    connection = Connection(
        [
            Cursor(),
            Cursor({"is_open": True, "tripped_at": NOW, "reset_at": None}),
        ]
    )
    scaler = Scaler()

    is_open, sample, scaled = reconcile_once(connection, BreakerPolicy(), scaler)

    assert is_open is True
    assert sample is None
    assert scaled is True
    assert scaler.calls == 1
    assert not any("pipeline_stage_results" in query for query, _ in connection.calls)


class FakeApiScaler:
    """KubernetesDeploymentScaler with its HTTP layer replaced."""

    def __init__(self, replicas: int, pod_names: Sequence[str]) -> None:
        from swegen.pipeline.generate_circuit_breaker import KubernetesDeploymentScaler

        self.calls: list[tuple[str, str | None, object]] = []
        self.replicas = replicas
        self.pod_names = list(pod_names)
        self.scaler = KubernetesDeploymentScaler.__new__(KubernetesDeploymentScaler)
        self.scaler._policy = BreakerPolicy()
        self.scaler._api_root = "https://api"
        self.scaler._url = "https://api/deployments/swegen-generate"
        self.scaler._pod_collection_url = "https://api/pods?labelSelector=stage"
        self.scaler._request = self._request  # type: ignore[method-assign]

    def _request(self, method, payload=None, *, url=None):
        self.calls.append((method, url, payload))
        if url is None:
            return {"spec": {"replicas": self.replicas}}
        if method == "GET":
            return {"items": [{"metadata": {"name": name}} for name in self.pod_names]}
        return {}

    @property
    def deleted(self) -> list[str]:
        return [
            url.rsplit("/", 1)[-1]
            for method, url, _ in self.calls
            if method == "DELETE" and url is not None
        ]


def test_scale_to_zero_force_deletes_lingering_stage_pods() -> None:
    # Scaling alone only sets a deletionTimestamp; Generate's 18000s grace then
    # lets a crash-looping Pod keep burning deliveries for hours.
    fake = FakeApiScaler(replicas=32, pod_names=["swegen-generate-a", "swegen-generate-b"])

    assert fake.scaler.scale_to_zero() is True

    assert ("PATCH", None, {"spec": {"replicas": 0}}) in fake.calls
    assert fake.deleted == ["swegen-generate-a", "swegen-generate-b"]
    assert all(
        payload == {"gracePeriodSeconds": 0}
        for method, _, payload in fake.calls
        if method == "DELETE"
    )


def test_scale_to_zero_keeps_sweeping_when_already_at_zero() -> None:
    # The 2026-08-03 incident: the Deployment was already at zero, yet two
    # surviving Pods failed 29,782 tasks over the next 3.5 hours.
    fake = FakeApiScaler(replicas=0, pod_names=["swegen-generate-survivor"])

    assert fake.scaler.scale_to_zero() is False

    assert fake.deleted == ["swegen-generate-survivor"]
    assert not any(method == "PATCH" for method, _, _ in fake.calls)


def test_scale_to_zero_survives_a_pod_listing_failure() -> None:
    fake = FakeApiScaler(replicas=8, pod_names=[])

    def explode(method, payload=None, *, url=None):
        fake.calls.append((method, url, payload))
        if url is not None:
            raise RuntimeError("Kubernetes API GET failed with HTTP 403")
        return {"spec": {"replicas": fake.replicas}}

    fake.scaler._request = explode  # type: ignore[method-assign]

    # The scale-down is what matters; a listing failure must not undo it.
    assert fake.scaler.scale_to_zero() is True
    assert ("PATCH", None, {"spec": {"replicas": 0}}) in fake.calls
