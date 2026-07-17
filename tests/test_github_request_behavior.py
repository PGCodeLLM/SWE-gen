from __future__ import annotations

from swegen.net import github_requests_kwargs


def test_github_proxy_is_explicitly_configured(monkeypatch) -> None:
    monkeypatch.setenv("SWEGEN_GITHUB_PROXY", "http://github-proxy:8080")

    assert github_requests_kwargs() == {
        "proxies": {
            "http": "http://github-proxy:8080",
            "https": "http://github-proxy:8080",
        }
    }
