import pytest

from orchestrator import failure_reason_from_output


def test_tls_clone_failure_is_not_labeled_as_nop_or_oracle() -> None:
    output = """
fatal: server certificate verification failed
Validation failed. Review the task files and logs.
"""

    assert failure_reason_from_output(output, 1) == "Git clone TLS certificate verification failed"


def test_docker_mirror_failure_is_not_labeled_as_nop_or_oracle() -> None:
    output = """
Unable to connect to archive.ubuntu.com:80
Unable to locate package git
Validation failed. Review the task files and logs.
"""

    assert (
        failure_reason_from_output(output, 1)
        == "Docker build could not reach Ubuntu package mirrors"
    )


@pytest.mark.parametrize(
    "output",
    [
        "openai.AuthenticationError: Incorrect API key provided",
        'API Error: 401 {"error":{"type":"authentication_failed"}}',
        "All credentials are exhausted or cooling down",
        'Error code: 429 - {"error":{"code":"insufficient_quota"}}',
        "Model usage exhausted for the current account",
        "Your credit balance is too low to access the API",
        "Combined LLM call failed: RemainQuota=0",
        "HTTP/1.1 401 Unauthorized",
        "Backend request failed with status_code: 401",
    ],
)
def test_model_api_failures_are_transient(output: str) -> None:
    assert failure_reason_from_output(output, 1) == "Transient network/API error"


def test_github_rate_limit_takes_priority_over_model_api_failure() -> None:
    output = """
API rate limit exceeded for GitHub token.
Upstream detail: authentication_failed; RemainQuota=0
"""

    assert (
        failure_reason_from_output(output, 1)
        == "GitHub rate limit or forbidden response"
    )
