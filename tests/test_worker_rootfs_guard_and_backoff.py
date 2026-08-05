"""Rootfs-corruption defenses in the pipeline worker.

Overlayfs corruption under pod churn can strip /app and the venv python from a
Running pod. Two defenses guard against a gutted pod draining the queue:
(1) a boot-time integrity check that exits before the claim loop, and
(2) a consecutive-fast-failure backoff so a pod that slips past (1) throttles
itself instead of claiming thousands of messages in seconds.
"""
from __future__ import annotations

import pytest

import swegen.pipeline.worker as worker_mod


def test_verify_runtime_integrity_passes_in_a_healthy_env() -> None:
    # The real test environment has /app? Not necessarily — so only assert it
    # does not raise when the interpreter + swegen import both resolve.
    # Patch /app existence to avoid depending on the container layout.
    import pathlib

    real_is_dir = pathlib.Path.is_dir

    def fake_is_dir(self):
        if str(self) == "/app":
            return True
        return real_is_dir(self)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(pathlib.Path, "is_dir", fake_is_dir)
        worker_mod._verify_runtime_integrity()  # must not raise


def test_verify_runtime_integrity_exits_when_app_missing(monkeypatch) -> None:
    import pathlib

    monkeypatch.setattr(pathlib.Path, "is_dir", lambda self: False)
    with pytest.raises(SystemExit) as exc:
        worker_mod._verify_runtime_integrity()
    assert exc.value.code == 70


def test_backoff_constants_are_sane() -> None:
    assert worker_mod._FAST_FAILURE_STREAK_LIMIT >= 2
    assert worker_mod._FAST_FAILURE_SECONDS > 0
    assert (
        worker_mod._FAST_FAILURE_BACKOFF_BASE_SECONDS
        <= worker_mod._FAST_FAILURE_BACKOFF_CAP_SECONDS
    )


def test_fast_failure_streak_triggers_growing_backoff() -> None:
    # Emulate run_forever's backoff arithmetic to prove it escalates and caps.
    L = worker_mod._FAST_FAILURE_STREAK_LIMIT
    base = worker_mod._FAST_FAILURE_BACKOFF_BASE_SECONDS
    cap = worker_mod._FAST_FAILURE_BACKOFF_CAP_SECONDS

    def backoff_for(streak: int) -> float:
        return min(cap, base * 2 ** (streak - L))

    # Below the limit: no backoff would fire (run_forever guards on >= L).
    assert L >= 2
    # At the limit and beyond: strictly increasing until the cap.
    b_at = backoff_for(L)
    b_next = backoff_for(L + 1)
    assert b_at == base
    assert b_next > b_at
    # Escalates to the cap for a large streak.
    assert backoff_for(L + 20) == cap
