from __future__ import annotations

import pytest

from orchestrator import validate_worker_proxy_config, worker_child_env


def proxy_env() -> dict[str, str]:
    return {
        "SWEGEN_WORKER_PROXY_POOL": "socks5://proxy-a:1080,socks5://proxy-b:1080",
        "SWEGEN_CLAUDE_PROXY_POOL": "http://127.0.0.1:18087,http://127.0.0.1:18108",
        "SWEGEN_PROXY_WORKERS_PER_ENDPOINT": "16",
        "GIT_PROXY": "http://git-proxy:8080",
        "SWEGEN_GITHUB_PROXY": "http://git-proxy:8080",
        "no_proxy": "127.0.0.1,10.*",
    }


def test_worker_proxy_pool_splits_32_workers_evenly() -> None:
    env = proxy_env()

    first = worker_child_env(env, 15)
    second = worker_child_env(env, 16)
    last = worker_child_env(env, 31)

    assert first["HTTPS_PROXY"] == "http://127.0.0.1:18087"
    assert first["HTTP_PROXY"] == "http://git-proxy:8080"
    assert first["SWEGEN_CLAUDE_PROXY"] == "http://127.0.0.1:18087"
    assert second["HTTPS_PROXY"] == "http://127.0.0.1:18108"
    assert second["SWEGEN_ASSIGNED_SOCKS_PROXY"] == "socks5://proxy-b:1080"
    assert second["SWEGEN_CLAUDE_PROXY"] == "http://127.0.0.1:18108"
    assert last["SWEGEN_PROXY_ENDPOINT_INDEX"] == "1"
    assert second["GIT_PROXY"] == "http://git-proxy:8080"
    assert second["SWEGEN_GITHUB_PROXY"] == "http://git-proxy:8080"
    assert second["no_proxy"] == "127.0.0.1,10.*,proxy-a,proxy-b"
    assert second["NO_PROXY"] == second["no_proxy"]


def test_worker_proxy_pool_merges_upper_and_lower_bypass_entries() -> None:
    env = proxy_env()
    env["NO_PROXY"] = "localhost,PROXY-A"

    child = worker_child_env(env, 0)

    assert child["NO_PROXY"] == "127.0.0.1,10.*,localhost,PROXY-A,proxy-b"
    assert child["no_proxy"] == child["NO_PROXY"]


def test_worker_proxy_pool_enforces_endpoint_capacity() -> None:
    with pytest.raises(ValueError, match="exceed proxy capacity 32"):
        validate_worker_proxy_config(proxy_env(), 33)


def test_worker_proxy_pool_reports_exact_assignments() -> None:
    assert validate_worker_proxy_config(proxy_env(), 32) == [
        "workers 0-15: socks5://proxy-a:1080 (Claude bridge http://127.0.0.1:18087)",
        "workers 16-31: socks5://proxy-b:1080 (Claude bridge http://127.0.0.1:18108)",
    ]
