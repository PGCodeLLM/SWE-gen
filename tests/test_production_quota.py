from __future__ import annotations

import pytest

from swegen.production_quota import ProductionQuota


def test_shared_quota_counts_only_successful_reservations(tmp_path):
    path = tmp_path / "production-quota.json"
    first = ProductionQuota(path, 2, stale_after_seconds=3600, poll_interval=0.01)
    second = ProductionQuota(path, 2, stale_after_seconds=3600, poll_interval=0.01)

    first_token = first.acquire()
    second_token = second.acquire()
    assert first_token is not None
    assert second_token is not None

    first.complete(first_token, success=True)
    second.complete(second_token, success=False)
    replacement = second.acquire()
    assert replacement is not None
    second.complete(replacement, success=True)

    snapshot = first.snapshot()
    assert snapshot.successes == 2
    assert snapshot.reservations == 0
    assert snapshot.reached
    assert first.acquire() is None


def test_existing_quota_file_rejects_a_different_limit(tmp_path):
    path = tmp_path / "production-quota.json"
    ProductionQuota(path, 2, stale_after_seconds=3600)

    with pytest.raises(ValueError, match=r"has limit 2"):
        ProductionQuota(path, 3, stale_after_seconds=3600)
