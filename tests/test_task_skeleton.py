from swegen.create.task_skeleton import (
    SkeletonParams,
    generate_dockerfile,
    generate_solve_sh,
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
    assert "apt-get -o Acquire::Retries=5 update" in dockerfile


def test_dockerfile_installs_proxy_ca_before_git_clone() -> None:
    dockerfile = generate_dockerfile(
        PARAMS,
        proxy_ca_filename="swegen-proxy-ca.crt",
    )

    copy_index = dockerfile.index("COPY swegen-proxy-ca.crt")
    update_index = dockerfile.index("update-ca-certificates")
    clone_index = dockerfile.index("RUN git clone")

    assert copy_index < update_index < clone_index
    assert "chmod 0644 /usr/local/share/ca-certificates/swegen-proxy-ca.crt" in dockerfile
    assert "cat /tmp/swegen-proxy-ca.crt >> /etc/ssl/certs/ca-certificates.crt" in dockerfile


def test_dockerfile_exports_proxy_ca_for_git_node_and_npm() -> None:
    dockerfile = generate_dockerfile(
        PARAMS,
        proxy_ca_filename="swegen-proxy-ca.crt",
    )

    trusted_ca = "/usr/local/share/ca-certificates/swegen-proxy-ca.crt"
    assert f"GIT_SSL_CAINFO={trusted_ca}" in dockerfile
    assert "UV_NATIVE_TLS=true" in dockerfile
    assert f"NODE_EXTRA_CA_CERTS={trusted_ca}" in dockerfile
    assert f"NPM_CONFIG_CAFILE={trusted_ca}" in dockerfile
    assert "NPM_CONFIG_LEGACY_PEER_DEPS=true" in dockerfile


def test_dockerfile_applies_bug_patch_tolerating_crlf_checkouts() -> None:
    # `patch` rejects a hunk with "different line endings" when .gitattributes
    # gives the container a CRLF worktree but bug.patch carries the repository's
    # LF endings. git apply --ignore-whitespace accepts it and still refuses a
    # patch that genuinely does not apply.
    dockerfile = generate_dockerfile(PARAMS)

    assert "git apply --ignore-whitespace /tmp/bug.patch" in dockerfile
    assert "patch -p1 < /tmp/bug.patch" not in dockerfile


def test_solve_sh_applies_fix_patch_without_a_git_directory() -> None:
    # solve.sh runs after `rm -rf .git`, so it prefers git apply (which works
    # outside a repository) and falls back to patch when git is absent.
    solve_sh = generate_solve_sh()

    assert "git apply --ignore-whitespace /solution/fix.patch" in solve_sh
    assert "patch -p1 < /solution/fix.patch" in solve_sh
    assert solve_sh.index("git apply") < solve_sh.index("patch -p1")
