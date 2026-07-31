import tomllib

from swegen.create.task_skeleton import (
    SkeletonParams,
    generate_dockerfile,
    generate_task_toml,
)

PARAMS = SkeletonParams(
    repo_url="https://github.com/example/project.git",
    head_sha="a" * 40,
    base_sha="b" * 40,
    pr_number=123,
)


def test_dockerfile_omits_proxy_ca_when_not_configured() -> None:
    dockerfile = generate_dockerfile(PARAMS)

    assert "swegen-proxy-ca.crt" not in dockerfile
    assert "update-ca-certificates" not in dockerfile


def test_dockerfile_installs_proxy_ca_before_git_clone() -> None:
    dockerfile = generate_dockerfile(
        PARAMS,
        proxy_ca_filename="swegen-proxy-ca.crt",
    )

    copy_index = dockerfile.index("COPY swegen-proxy-ca.crt")
    update_index = dockerfile.index("update-ca-certificates")
    clone_index = dockerfile.index("RUN git clone")

    assert copy_index < update_index < clone_index


def test_dockerfile_exports_proxy_ca_for_node_and_npm() -> None:
    dockerfile = generate_dockerfile(
        PARAMS,
        proxy_ca_filename="swegen-proxy-ca.crt",
    )

    trusted_ca = "/usr/local/share/ca-certificates/swegen-proxy-ca.crt"
    assert f"NODE_EXTRA_CA_CERTS={trusted_ca}" in dockerfile
    assert f"NPM_CONFIG_CAFILE={trusted_ca}" in dockerfile


def test_generated_task_toml_uses_two_hour_timeouts() -> None:
    config = tomllib.loads(generate_task_toml({}))

    assert config["verifier"]["timeout_sec"] == 7200.0
    assert config["agent"]["timeout_sec"] == 7200.0
    assert config["environment"]["build_timeout_sec"] == 7200.0
