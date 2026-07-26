from swegen.create.task_skeleton import SkeletonParams, generate_dockerfile

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
