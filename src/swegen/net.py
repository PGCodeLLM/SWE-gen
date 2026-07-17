from __future__ import annotations

import os


def ssl_verification_disabled() -> bool:
    """Return True when the user explicitly opted out of TLS verification."""
    value = os.environ.get("SWEGEN_SSL_NO_VERIFY", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def requests_ssl_kwargs() -> dict[str, bool]:
    """Keyword args for requests calls that need the opt-in no-verify mode.

    When verification is enabled, return no kwargs so requests can still honor
    REQUESTS_CA_BUNDLE/CURL_CA_BUNDLE from the environment.
    """
    if ssl_verification_disabled():
        return {"verify": False}
    return {}


def github_requests_kwargs() -> dict[str, dict[str, str]]:
    """Return an explicit proxy configuration for GitHub REST API requests.

    ``SWEGEN_GITHUB_PROXY`` takes precedence over process-wide proxy variables.
    This lets batch workers use an HTTP proxy for GitHub while Claude/OpenAI
    traffic continues to use its separately configured route.
    """
    proxy = os.environ.get("SWEGEN_GITHUB_PROXY", "").strip()
    if not proxy:
        return {}
    return {"proxies": {"http": proxy, "https": proxy}}
