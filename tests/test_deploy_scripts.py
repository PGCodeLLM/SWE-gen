from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


def test_create_secrets_clones_existing_secrets_into_tester_namespace(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    config = tmp_path / "swegen.toml"
    config.write_text(
        "[pipeline]\n"
        'namespace = "swegen-pipeline-tester"\n'
        'secret_source_namespace = "swegen-pipeline"\n'
    )
    apply_dir = tmp_path / "applied"
    apply_dir.mkdir()
    fake_kubectl = tmp_path / "kubectl"
    fake_kubectl.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'printf "%s\\n" "$*" >>"${FAKE_KUBECTL_LOG}"\n'
        'for ((index = 1; index <= $#; index++)); do\n'
        '  if [[ "${!index}" == "get" ]]; then\n'
        '    next=$((index + 1))\n'
        '    resource="${!next:-}"\n'
        '    if [[ "${resource}" == secret/* && " $* " == *" -o json "* ]]; then\n'
        '      name="${resource#secret/}"\n'
        '      printf \'{"apiVersion":"v1","kind":"Secret","metadata":'
        '{"name":"%s","namespace":"swegen-pipeline","uid":"old-uid",'
        '"resourceVersion":"7","managedFields":[{}]},"type":"Opaque",'
        '"data":{"dummy":"dmFsdWU="}}\' "${name}"\n'
        "    fi\n"
        "    exit 0\n"
        "  fi\n"
        "done\n"
        'if [[ " $* " == *" create namespace "* ]]; then\n'
        '  printf \'apiVersion: v1\\nkind: Namespace\\nmetadata:\\n  name: '
        'swegen-pipeline-tester\\n\'\n'
        "  exit 0\n"
        "fi\n"
        'if [[ " $* " == *" apply -f - "* ]]; then\n'
        '  counter_file="${FAKE_APPLY_DIR}/counter"\n'
        '  counter="$(test -r "${counter_file}" && cat "${counter_file}" || printf 0)"\n'
        '  counter=$((counter + 1))\n'
        '  printf "%s" "${counter}" >"${counter_file}"\n'
        '  cat >"${FAKE_APPLY_DIR}/${counter}"\n'
        "fi\n"
    )
    fake_kubectl.chmod(0o755)

    environment = dict(os.environ)
    environment.update(
        {
            "SWEGEN_CONFIG_SOURCE": str(config),
            "SWEGEN_SECRET_ROOT": str(tmp_path / "missing-local-secrets"),
            "SWEGEN_KUBECTL": str(fake_kubectl),
            "FAKE_KUBECTL_LOG": str(tmp_path / "kubectl.log"),
            "FAKE_APPLY_DIR": str(apply_dir),
        }
    )
    result = subprocess.run(
        [str(root / "deploy/k3s/create-secrets.sh")],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    cloned = [json.loads((apply_dir / str(index)).read_text()) for index in range(2, 6)]
    assert {secret["metadata"]["name"] for secret in cloned} == {
        "swegen-runtime-proxy",
        "swegen-private-files",
        "swegen-docker-config",
        "swegen-database",
    }
    for secret in cloned:
        assert secret["metadata"] == {
            "name": secret["metadata"]["name"],
            "namespace": "swegen-pipeline-tester",
        }
        assert secret["type"] == "Opaque"
        assert secret["data"] == {"dummy": "dmFsdWU="}
    assert "swegen-pipeline-tester patch secret/swegen-private-files" in (
        tmp_path / "kubectl.log"
    ).read_text()
