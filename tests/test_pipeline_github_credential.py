"""Local task builds must authenticate their in-Dockerfile GitHub clones.

Regression guard: when the remote BuildKit farm went down, validate/repair built
task images locally. Task Dockerfiles ``RUN git clone https://github.com/...``
inside the build container, which has no GitHub credential, so clones failed with
``could not read Username`` or hung until the build timeout, collapsing throughput.
``_inject_github_credential`` prepends a ``git config insteadOf`` fed by the
per-pod token pool so those clones authenticate; the token must be redacted from
persisted logs and never break the git-install ordering.
"""
from __future__ import annotations

from pathlib import Path
from uuid import UUID

from swegen.create.claude_code_utils import redact_sensitive_text
from swegen.pipeline import actions
from swegen.pipeline.models import PipelineTask

_DOCKERFILE = """FROM ubuntu:24.04
COPY swegen-proxy-ca.crt /tmp/swegen-proxy-ca.crt
RUN apt-get update && apt-get install -y git && update-ca-certificates
WORKDIR /app
RUN git clone https://github.com/fullstorydev/grpcui.git src && \\
    cd src && make
"""


def _task(trace: str = "12345678-1234-5678-1234-567812345678") -> PipelineTask:
    return PipelineTask(
        task_id="owner__repo-42",
        task_version=1,
        repo="owner/repo",
        pr=42,
        trace_id=UUID(trace),
    )


def _write_dockerfile(tmp_path: Path, text: str = _DOCKERFILE) -> Path:
    environment = tmp_path / "environment"
    environment.mkdir(parents=True)
    (environment / "Dockerfile").write_text(text)
    return environment / "Dockerfile"


def test_injects_git_credential_before_the_clone_and_after_git_install(tmp_path) -> None:
    dockerfile = _write_dockerfile(tmp_path)
    assert actions._inject_github_credential(tmp_path, "ghp_secret123") is True
    out = dockerfile.read_text()

    # git config lands after git is installed and immediately before the clone.
    assert out.index("apt-get install -y git") < out.index("git config")
    assert out.index("git config") < out.index("git clone https://github.com")
    assert 'insteadOf "https://github.com/"' in out
    assert "x-access-token:ghp_secret123@github.com" in out
    # exactly one occurrence of the token; no ENV/ARG that would persist it.
    assert out.count("ghp_secret123") == 1
    assert "ENV GITHUB_TOKEN" not in out
    assert "ARG GITHUB_TOKEN" not in out


def test_injection_is_idempotent(tmp_path) -> None:
    dockerfile = _write_dockerfile(tmp_path)
    assert actions._inject_github_credential(tmp_path, "ghp_secret123") is True
    once = dockerfile.read_text()
    # A second pass (nop then oracle rewrite the same Dockerfile) must not stack.
    assert actions._inject_github_credential(tmp_path, "ghp_secret123") is True
    assert dockerfile.read_text() == once
    assert once.count(actions._GITHUB_CREDENTIAL_MARKER) == 1


def test_noop_without_token_or_github_clone(tmp_path) -> None:
    dockerfile = _write_dockerfile(tmp_path)
    # No token -> no change.
    assert actions._inject_github_credential(tmp_path, None) is False
    assert actions._GITHUB_CREDENTIAL_MARKER not in dockerfile.read_text()

    # Dockerfile with no github clone -> no change.
    other = tmp_path / "other"
    other.mkdir()
    (other).joinpath("environment").mkdir()
    (other / "environment" / "Dockerfile").write_text("FROM scratch\n")
    assert actions._inject_github_credential(other, "ghp_secret123") is False


def test_injected_token_is_redacted_from_logs(tmp_path) -> None:
    dockerfile = _write_dockerfile(tmp_path)
    actions._inject_github_credential(tmp_path, "ghp_supersecretvalue")
    # The build echoes the RUN line into logs persisted to the DB; redaction
    # must scrub the tokenized URL so the secret never lands in stage results.
    redacted = redact_sensitive_text(dockerfile.read_text())
    assert "ghp_supersecretvalue" not in redacted
    assert "<REDACTED>@github.com" in redacted


def test_token_comes_from_pool_and_varies_by_task(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    pool = ["ghp_a", "ghp_b", "ghp_c", "ghp_d", "ghp_e"]
    monkeypatch.setattr("swegen.pipeline.actions.load_github_tokens", lambda: pool)

    # Deterministic per task (trace_id % len), and spread across the pool.
    picks = {
        actions._task_github_token(
            _task(f"{i:08d}-1234-5678-1234-567812345678")
        )
        for i in range(50)
    }
    assert picks <= set(pool)
    assert len(picks) > 1


def test_env_github_token_overrides_pool(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_env_override")
    monkeypatch.setattr(
        "swegen.pipeline.actions.load_github_tokens", lambda: ["ghp_pool"]
    )
    assert actions._task_github_token(_task()) == "ghp_env_override"
