from __future__ import annotations

import requests

from orchestrator import _is_github_rate_limited, _is_retryable_failure
from swegen.create.pr_fetcher import GitHubPRFetcher


def response(status: int, *, headers: dict[str, str] | None = None, payload=None):
    item = requests.Response()
    item.status_code = status
    item.url = "https://api.github.com/repos/example/repo/pulls/1"
    item.headers.update(headers or {})
    if payload is not None:
        import json

        item._content = json.dumps(payload).encode()
        item.headers["Content-Type"] = "application/json"
    else:
        item._content = b"{}"
    return item


def test_actual_requests_error_strings_are_retryable_and_classified() -> None:
    forbidden = (
        "requests.exceptions.HTTPError: 403 Client Error: Forbidden for url: "
        "https://api.github.com/repos/example/repo/pulls/1"
    )
    unavailable = (
        "requests.exceptions.HTTPError: 503 Server Error: Service Unavailable for "
        "url: https://api.github.com/repos/example/repo/pulls/1"
    )

    assert _is_github_rate_limited(forbidden)
    assert _is_github_rate_limited(unavailable)
    assert _is_retryable_failure(unavailable)


def test_api_get_retries_503_then_returns_json(monkeypatch) -> None:
    replies = [response(503), response(200, payload={"merged": True})]
    sleeps: list[float] = []
    monkeypatch.setenv("SWEGEN_GITHUB_API_ATTEMPTS", "2")
    monkeypatch.setenv("SWEGEN_GITHUB_RETRY_BASE_SECONDS", "0")
    monkeypatch.setattr(
        "swegen.create.pr_fetcher.requests.get", lambda *_args, **_kwargs: replies.pop(0)
    )
    monkeypatch.setattr("swegen.create.pr_fetcher.random.uniform", lambda *_args: 0.0)
    monkeypatch.setattr("swegen.create.pr_fetcher.time.sleep", sleeps.append)

    result = GitHubPRFetcher("example/repo", 1, "token")._api_get("/pulls/1")

    assert result == {"merged": True}
    assert sleeps == [0.0]


def test_api_get_waits_until_primary_rate_limit_reset(monkeypatch) -> None:
    replies = [
        response(
            403,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1010"},
        ),
        response(200, payload={"ok": True}),
    ]
    sleeps: list[float] = []
    monkeypatch.setenv("SWEGEN_GITHUB_API_ATTEMPTS", "2")
    monkeypatch.setenv("SWEGEN_GITHUB_MAX_WAIT_SECONDS", "3600")
    monkeypatch.setattr(
        "swegen.create.pr_fetcher.requests.get", lambda *_args, **_kwargs: replies.pop(0)
    )
    monkeypatch.setattr("swegen.create.pr_fetcher.time.time", lambda: 1000.0)
    monkeypatch.setattr("swegen.create.pr_fetcher.random.uniform", lambda *_args: 0.0)
    monkeypatch.setattr("swegen.create.pr_fetcher.time.sleep", sleeps.append)

    result = GitHubPRFetcher("example/repo", 1, "token")._api_get("/pulls/1")

    assert result == {"ok": True}
    assert sleeps == [11.0]


def test_api_get_includes_github_message_on_final_error(monkeypatch) -> None:
    monkeypatch.setenv("SWEGEN_GITHUB_API_ATTEMPTS", "1")
    monkeypatch.setattr(
        "swegen.create.pr_fetcher.requests.get",
        lambda *_args, **_kwargs: response(
            403, payload={"message": "API rate limit exceeded"}
        ),
    )

    try:
        GitHubPRFetcher("example/repo", 1, "token")._api_get("/pulls/1")
    except requests.HTTPError as error:
        assert "GitHub message: API rate limit exceeded" in str(error)
        assert error.response is not None
        assert error.response.status_code == 403
    else:
        raise AssertionError("expected HTTPError")
