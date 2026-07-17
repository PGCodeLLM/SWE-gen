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
