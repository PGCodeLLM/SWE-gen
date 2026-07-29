from __future__ import annotations

from subprocess import CompletedProcess

import pytest


def test_generate_total_is_apportioned_across_main_and_overflow() -> None:
    from swegen.dashboard.server import K3sScaler

    assert K3sScaler.plan("generate", 96, max_replicas=768) == [
        ("swegen-generate", 92),
        ("swegen-generate-overflow", 4),
    ]
    assert K3sScaler.plan("generate", 80, max_replicas=768) == [
        ("swegen-generate-overflow", 0),
        ("swegen-generate", 80),
    ]


@pytest.mark.parametrize(
    ("stage", "replicas"),
    [
        ("generate; delete pods", 1),
        ("validate", True),
        ("reward", 1.5),
        ("push", -1),
        ("push", 769),
    ],
)
def test_scaler_rejects_unknown_stages_and_invalid_bounds(
    stage: object,
    replicas: object,
) -> None:
    from swegen.dashboard.server import K3sScaler

    with pytest.raises(ValueError):
        K3sScaler.plan(stage, replicas, max_replicas=768)


def test_scaler_accepts_the_dynamic_cluster_cpu_ceiling() -> None:
    from swegen.dashboard.server import K3sScaler

    assert K3sScaler.plan("reward", 768, max_replicas=768) == [("swegen-reward", 768)]
    with pytest.raises(ValueError, match="between 0 and 768"):
        K3sScaler.plan("reward", 769, max_replicas=768)


def test_scaler_uses_allowlisted_kubectl_argument_arrays() -> None:
    from swegen.dashboard.server import K3sScaler

    commands: list[list[str]] = []

    def runner(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        commands.append(command)
        return CompletedProcess(command, 0, stdout="scaled", stderr="")

    applied = K3sScaler(runner=runner).scale(
        "generate",
        96,
        max_replicas=768,
    )

    assert applied == [
        {"deployment": "swegen-generate", "replicas": 92},
        {"deployment": "swegen-generate-overflow", "replicas": 4},
    ]
    assert commands == [
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "scale",
            "deployment/swegen-generate",
            "--replicas=92",
        ],
        [
            "kubectl",
            "--request-timeout=10s",
            "-n",
            "swegen-pipeline",
            "scale",
            "deployment/swegen-generate-overflow",
            "--replicas=4",
        ],
    ]


def test_cache_retains_last_good_capacity_without_dropping_below_configured() -> None:
    from swegen.dashboard.server import SnapshotCache

    class HealthyCollector:
        def collect(self) -> dict[str, object]:
            return {"ok": True}

    class FailedCollector:
        def collect(self) -> dict[str, object]:
            raise RuntimeError("k3s temporarily unavailable")

    cache = SnapshotCache()
    cache._snapshot = {
        "postgres": {},
        "k3s": {
            "scaling": {"max_replicas": 768, "stale": False},
            "stages": {"validate": {"desired": 800}},
        },
        "sources": {},
    }
    cache._collectors = {
        "postgres": HealthyCollector(),
        "k3s": FailedCollector(),
    }

    cache.refresh()

    scaling = cache.snapshot()["k3s"]["scaling"]
    assert scaling["max_replicas"] == 800
    assert scaling["stale"] is True
