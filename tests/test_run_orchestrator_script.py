from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_launcher_uses_current_cli_and_mirrors_proxy_environment(tmp_path: Path):
    repository = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    report = tmp_path / "launch-report.txt"
    system_ca = tmp_path / "system-ca.pem"
    proxy_ca = tmp_path / "proxy-ca.pem"
    combined_ca = tmp_path / "combined-ca.pem"
    system_ca.write_text("SYSTEM ROOTS\n")
    proxy_ca.write_text("HUAWEI PROXY ROOT\n")
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/usr/bin/env bash
set -eu
{
    printf 'cwd=%s\\n' "$PWD"
    printf 'arg=%s\\n' "$@"
    printf 'HTTP_PROXY=%s\\n' "${HTTP_PROXY-}"
    printf 'http_proxy=%s\\n' "${http_proxy-}"
    printf 'HTTPS_PROXY=%s\\n' "${HTTPS_PROXY-}"
    printf 'https_proxy=%s\\n' "${https_proxy-}"
    printf 'NO_PROXY=%s\\n' "${NO_PROXY-}"
    printf 'no_proxy=%s\\n' "${no_proxy-}"
    printf 'REQUESTS_CA_BUNDLE=%s\\n' "${REQUESTS_CA_BUNDLE-}"
    printf 'SSL_CERT_FILE=%s\\n' "${SSL_CERT_FILE-}"
    printf 'CURL_CA_BUNDLE=%s\\n' "${CURL_CA_BUNDLE-}"
    printf 'GIT_SSL_CAINFO=%s\\n' "${GIT_SSL_CAINFO-}"
    printf 'PIP_CERT=%s\\n' "${PIP_CERT-}"
    printf 'NODE_EXTRA_CA_CERTS=%s\\n' "${NODE_EXTRA_CA_CERTS-}"
} > "${SWEGEN_LAUNCH_REPORT}"
"""
    )
    fake_uv.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "SWEGEN_LAUNCH_REPORT": str(report),
            "WORKERS": "7",
            "CC_TIMEOUT": "1234",
            "TRANSIENT_ATTEMPTS": "5",
            "HTTP_PROXY": "http://proxy.example:8080",
            "https_proxy": "http://secure-proxy.example:8080",
            "NO_PROXY": "localhost,127.0.0.1",
            "SWEGEN_SYSTEM_CA_BUNDLE": str(system_ca),
            "SWEGEN_PROXY_CA_CERT": str(proxy_ca),
            "SWEGEN_CA_BUNDLE_OUTPUT": str(combined_ca),
        }
    )
    env.pop("http_proxy", None)
    env.pop("HTTPS_PROXY", None)
    env.pop("no_proxy", None)

    subprocess.run(
        ["bash", str(repository / "run_orchestrator.sh"), "--include-obs-missing"],
        cwd=tmp_path,
        env=env,
        check=True,
    )

    lines = report.read_text().splitlines()
    assert lines[:11] == [
        f"cwd={repository}",
        "arg=run",
        "arg=python",
        "arg=src/orchestrator.py",
        "arg=--workers",
        "arg=7",
        "arg=--cc-timeout",
        "arg=1234",
        "arg=--transient-attempts",
        "arg=5",
        "arg=--include-obs-missing",
    ]
    proxy_values = dict(line.split("=", 1) for line in lines[11:])
    assert proxy_values["HTTP_PROXY"] == proxy_values["http_proxy"]
    assert proxy_values["HTTPS_PROXY"] == proxy_values["https_proxy"]
    assert proxy_values["NO_PROXY"] == proxy_values["no_proxy"]
    for name in (
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
        "CURL_CA_BUNDLE",
        "GIT_SSL_CAINFO",
        "PIP_CERT",
    ):
        assert proxy_values[name] == str(combined_ca)
    assert proxy_values["NODE_EXTRA_CA_CERTS"] == str(combined_ca)
    assert combined_ca.read_text() == "SYSTEM ROOTS\n\nHUAWEI PROXY ROOT\n\n"
