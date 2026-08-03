import subprocess
from pathlib import Path

import pytest

from swegen.tools.dockerfile_mirrors import rewrite_ubuntu_mirrors


def test_rewrite_ubuntu_mirrors_updates_sources_and_download_urls(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "RUN curl -O https://cdimage.ubuntu.com/releases/image.iso \\\n"
        " && curl -O http://cloud-images.ubuntu.com/noble/image.img \\\n"
        " && curl -O https://releases.ubuntu.com/24.04/ubuntu.iso\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_UBUNTU_MIRRORS") == 1
    assert "http://mirrors.tools.huawei.com/ubuntu" in rendered
    assert "http://mirrors.tools.huawei.com/ubuntu-ports" in rendered
    assert "http://mirrors.tools.huawei.com/ubuntu-cdimage/releases/image.iso" in rendered
    assert "http://mirrors.tools.huawei.com/ubuntu-cloud-images/noble/image.img" in rendered
    assert "http://mirrors.tools.huawei.com/ubuntu-releases/24.04/ubuntu.iso" in rendered
    assert "curl -O https://cdimage.ubuntu.com" not in rendered


def test_rewrite_ubuntu_mirrors_injects_each_ubuntu_stage_only(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM --platform=linux/amd64 ubuntu:22.04 AS build\n"
        "RUN apt-get update\n"
        "FROM alpine:3.20\n"
        "RUN true\n"
        "FROM registry.example/team/ubuntu:24.04\n"
        "RUN apt-get update\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_UBUNTU_MIRRORS") == 2
    assert rendered.count("apt-get -o Acquire::Retries=5 update") == 2
    assert rendered.index("# SWEGEN_UBUNTU_MIRRORS") < rendered.index(
        "RUN apt-get -o Acquire::Retries=5 update"
    )


def test_rewrite_ubuntu_mirrors_rewrites_explicit_ports_without_ubuntu_base(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM scratch\n# https://ports.ubuntu.com/ubuntu-ports/dists/noble/InRelease\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert "http://mirrors.tools.huawei.com/ubuntu-ports/dists/noble/InRelease" in (
        dockerfile.read_text()
    )
    assert "# SWEGEN_UBUNTU_MIRRORS" not in dockerfile.read_text()


def test_rewrite_ubuntu_mirrors_configures_npm_before_first_use(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\nRUN apt-get update && apt-get install -y npm && npm ci\nRUN npm test\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_NPM_MIRROR") == 1
    assert (
        "apt-get install -y npm && if command -v git >/dev/null 2>&1; then "
        'git config --global url."https://github.com/".insteadOf git://github.com/; '
        "fi && npm config set strict-ssl false && "
        "npm config set registry https://registry.npmmirror.com/ && "
        "npm cache clean -f && (npm ci || npm install)"
    ) in rendered
    assert rendered.index("npm config set strict-ssl false") < rendered.index("npm ci")


def test_rewrite_ubuntu_mirrors_configures_each_npm_stage(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        'FROM node:22 AS build\nRUN npm install\nFROM node:22\nRUN ["npm", "test"]\n'
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_NPM_MIRROR") == 2
    assert rendered.count("npm config set strict-ssl false") == 2
    assert (
        "RUN if command -v git >/dev/null 2>&1; then "
        'git config --global url."https://github.com/".insteadOf git://github.com/; '
        "fi && npm config set strict-ssl false && "
        "npm config set registry https://registry.npmmirror.com/ && "
        "npm cache clean -f && npm install"
    ) in rendered
    assert (
        "RUN if command -v git >/dev/null 2>&1; then "
        'git config --global url."https://github.com/".insteadOf git://github.com/; '
        "fi && npm config set strict-ssl false && "
        "npm config set registry https://registry.npmmirror.com/ && "
        "npm cache clean -f\n"
    ) in rendered


def test_rewrite_ubuntu_mirrors_updates_existing_huawei_npm_block(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM node:22\n"
        "# SWEGEN_NPM_MIRROR\n"
        "RUN npm config set strict-ssl false && "
        "npm config set registry http://mirrors.tools.huawei.com/npm/ && "
        "npm cache clean -f && npm ci\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_NPM_MIRROR") == 1
    assert "https://registry.npmmirror.com/" in rendered
    assert "mirrors.tools.huawei.com/npm" not in rendered
    assert (
        "RUN if command -v git >/dev/null 2>&1; then "
        'git config --global url."https://github.com/".insteadOf git://github.com/; '
        "fi && npm config set strict-ssl false && "
        "npm config set registry https://registry.npmmirror.com/ && "
        "npm cache clean -f && (npm ci || npm install)\n"
    ) in rendered


def test_rewrite_ubuntu_mirrors_uses_meteor_bundled_npm(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM ubuntu:24.04\nRUN cd test-app && meteor npm install\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert (
        "meteor npm config set strict-ssl false && "
        "meteor npm config set registry https://registry.npmmirror.com/ && "
        "meteor npm cache clean -f && meteor npm install"
    ) in rendered
    assert "&& npm config" not in rendered


def test_rewrite_ubuntu_mirrors_repairs_old_broken_meteor_npm_block(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "# SWEGEN_NPM_MIRROR\n"
        "RUN meteor npm config set strict-ssl false && "
        "npm config set registry https://registry.npmmirror.com/ && "
        "npm cache clean -f && npm install\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "meteor npm cache clean -f && meteor npm install" in rendered
    assert "&& npm config" not in rendered


def test_rewrite_ubuntu_mirrors_uses_jammy_for_redis_stack(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "RUN printf '%s\\n' 'deb https://packages.redis.io/deb noble main' "
        "> /etc/apt/sources.list.d/redis.list \\\n"
        " && apt-get update \\\n"
        " && apt-get install -y redis-stack-server\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "packages.redis.io/deb jammy main" in rendered
    assert "packages.redis.io/deb noble main" not in rendered


def test_rewrite_ubuntu_mirrors_imports_proxy_ca_into_java_truststore(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "COPY swegen-proxy-ca.crt /tmp/swegen-proxy-ca.crt\n"
        "RUN apt-get update && apt-get install -y openjdk-17-jdk-headless \\\n"
        " && cp /tmp/swegen-proxy-ca.crt "
        "/usr/local/share/ca-certificates/swegen-proxy-ca.crt \\\n"
        " && update-ca-certificates \\\n"
        " && rm /tmp/swegen-proxy-ca.crt\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "keytool -importcert -noprompt -trustcacerts" in rendered
    assert "-alias swegen-proxy-ca-explicit" in rendered
    assert rendered.index("keytool -importcert") < rendered.index("rm /tmp/swegen-proxy-ca.crt")


def test_rewrite_ubuntu_mirrors_patches_legacy_node_gyp_for_python_312(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "ARG NODE_VERSION=12.22.12\n"
        "RUN curl -fsSLO https://nodejs.org/dist/v${NODE_VERSION}/node.tar.xz\n"
        "RUN npm ci\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_NODE_GYP_PYTHON_COMPAT") == 1
    assert "apt-get install -y --no-install-recommends python3" in rendered
    assert "npm install --global npm@6.14.18" in rendered
    assert "npm config set python /usr/bin/python3" in rendered
    assert "sed -i \"s/'rU'/'r'/g\" \"$input_py\"" in rendered
    assert rendered.index("SWEGEN_NODE_GYP_PYTHON_COMPAT") < rendered.index("npm ci")


def test_rewrite_ubuntu_mirrors_upgrades_existing_npm_block_with_node_gyp_patch(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM node:12\n"
        "# SWEGEN_NPM_MIRROR\n"
        "RUN npm config set strict-ssl false && "
        "npm config set registry https://registry.npmmirror.com/ && "
        "npm cache clean -f && npm ci\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_NPM_MIRROR") == 1
    assert rendered.count("# SWEGEN_NODE_GYP_PYTHON_COMPAT") == 1


def test_rewrite_ubuntu_mirrors_converts_git_protocol_for_npm_dependencies(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM node:4\nRUN npm install\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True

    rendered = dockerfile.read_text()
    assert 'url."https://github.com/".insteadOf git://github.com/' in rendered


def test_rewrite_ubuntu_mirrors_upgrades_node4_and_old_npm_pin(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "ARG NODE_VERSION=4.9.1\n"
        "RUN curl -fsSLO https://nodejs.org/v${NODE_VERSION}/node.tar.xz \\\n"
        " && npm install -g npm@2.15.12\n"
        "RUN npm install --unsafe-perm\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "ARG NODE_VERSION=6.17.1" in rendered
    assert "npm@2.15.12" not in rendered
    assert "npm@6.14.18" in rendered


def test_rewrite_ubuntu_mirrors_retries_curl_and_removes_stale_node_checksum(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "ARG NODE_VERSION=10.24.1\n"
        "RUN curl -fsSLO https://nodejs.org/dist/v${NODE_VERSION}/node.tar.xz \\\n"
        ' && echo "deadbeef node-v${NODE_VERSION}-linux-x64.tar.xz" '
        "| sha256sum -c - \\\n"
        " && tar -xJf node.tar.xz -C /usr/local\n"
        "RUN npm install\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "curl --retry 5 --retry-all-errors --retry-delay 2 -fsSLO" in rendered
    assert "sha256sum -c" not in rendered
    assert "&& tar -xJf" in rendered


def test_rewrite_ubuntu_mirrors_replaces_python2_for_legacy_node(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "ARG NODE_VERSION=6.17.1\n"
        "RUN apt-get update && apt-get install -y python2.7\n"
        "ENV PYTHON=/usr/bin/python2.7\n"
        "RUN npm install\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "python2.7" not in rendered
    assert "ENV PYTHON=/usr/bin/python3" in rendered


def test_rewrite_ubuntu_mirrors_runs_legacy_yarn_through_node(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "ARG NODE_VERSION=6.17.1\n"
        "RUN curl -fsSL https://example.invalid/yarn.tgz | tar -xz -C /opt \\\n"
        " && ln -s /opt/package/bin/yarn.js /usr/local/bin/yarn\n"
        "RUN yarn install --frozen-lockfile\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "node /opt/package/bin/yarn.js install --frozen-lockfile" in rendered
    assert "apt-get install -y --no-install-recommends python3" in rendered


def test_rewrite_ubuntu_mirrors_retries_playwright_downloads(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM node:22\n"
        "RUN npm ci \\\n"
        " && npx playwright install --with-deps chromium firefox webkit\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=300000") == 3
    assert "sleep 10" in rendered
    assert "sleep 20" in rendered


def test_rewrite_playwright_retry_does_not_consume_next_instruction(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM node:22\n"
        "RUN npm ci \\\n"
        " && npx playwright install --with-deps chromium firefox webkit\n"
        "COPY bug.patch /tmp/bug.patch\n"
        "RUN patch -p1 < /tmp/bug.patch\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "COPY bug.patch /tmp/bug.patch\n" in rendered
    assert "COPY bug.patch)" not in rendered
    assert "RUN patch -p1 < /tmp/bug.patch\n" in rendered


def test_rewrite_ubuntu_mirrors_relaxes_pnpm_frozen_lockfile(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM node:22\nRUN pnpm install --frozen-lockfile && pnpm build\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_PNPM_FROZEN_LOCKFILE_FALLBACK") == 1
    assert "(pnpm install --frozen-lockfile || pnpm install --no-frozen-lockfile)" in rendered
    assert "&& pnpm build" in rendered


def test_rewrite_ubuntu_mirrors_replaces_corepack_pnpm_bootstrap(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "ARG NODE_VERSION=20.12.2\n"
        "RUN corepack enable \\\n"
        " && corepack prepare pnpm@9.1.0 --activate\n"
        "RUN pnpm install --frozen-lockfile\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "corepack prepare" not in rendered
    assert "npm install --global pnpm@9.1.0" in rendered
    assert "registry https://registry.npmmirror.com/" in rendered


def test_rewrite_ubuntu_mirrors_trusts_explicit_debian_compat_repo(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "RUN printf '%s\\n' 'deb http://deb.debian.org/debian bullseye main' "
        "> /etc/apt/sources.list.d/bullseye.list \\\n"
        " && apt-get update\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "deb [trusted=yes] http://deb.debian.org/debian bullseye main" in rendered


def test_rewrite_ubuntu_mirrors_installs_dotnet_icu(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "RUN apt-get update && apt-get install -y curl\n"
        "RUN curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh \\\n"
        " && bash /tmp/dotnet-install.sh --version 5.0.200\n"
        "RUN dotnet restore app.sln\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_DOTNET_ICU") == 1
    assert "apt-get install -y --no-install-recommends libicu-dev" in rendered
    assert "DOTNET_NUGET_SIGNATURE_VERIFICATION=false" in rendered
    assert "NUGET_CERT_REVOCATION_MODE=offline" in rendered


def test_rewrite_ubuntu_mirrors_exports_deno_ca(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "COPY swegen-proxy-ca.crt /tmp/swegen-proxy-ca.crt\n"
        "RUN cp /tmp/swegen-proxy-ca.crt "
        "/usr/local/share/ca-certificates/swegen-proxy-ca.crt \\\n"
        " && update-ca-certificates\n"
        "RUN deno cache test_deps.ts\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "chmod 0644 /usr/local/share/ca-certificates/swegen-proxy-ca.crt" in rendered
    assert rendered.count("# SWEGEN_DENO_CA") == 1
    assert "DENO_TLS_CA_STORE=system" in rendered
    assert "DENO_CERT=/usr/local/share/ca-certificates/swegen-proxy-ca.crt" in rendered


def test_rewrite_ubuntu_mirrors_removes_unresolved_env_template(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM ubuntu:24.04\n"
        "ENV CI=true \\\n"
        "    ELECTRON_MIRROR=https://npmmirror.com/mirrors/electron/ \\\n"
        "    ELECTRON_CUSTOM_DIR={{ version }}\n"
        "RUN echo ok\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "{{ version }}" not in rendered
    assert (
        "ENV CI=true \\\n    ELECTRON_MIRROR=https://npmmirror.com/mirrors/electron/\n" in rendered
    )


def test_rewrite_ubuntu_mirrors_removes_yarn_classic_env_for_berry(
    tmp_path: Path,
) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM node:22\n"
        "ENV NPM_CONFIG_FETCH_RETRIES=5 \\\n"
        "    NPM_CONFIG_MAXSOCKETS=4 \\\n"
        "    YARN_NETWORK_TIMEOUT=600000 \\\n"
        "    YARN_HTTP_TIMEOUT=600000 \\\n"
        "    YARN_NETWORK_CONCURRENCY=4\n"
        "RUN yarn install --immutable\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert "YARN_NETWORK_TIMEOUT" not in rendered
    assert "YARN_HTTP_TIMEOUT" not in rendered
    assert "YARN_NETWORK_CONCURRENCY" not in rendered
    assert "ENV NPM_CONFIG_FETCH_RETRIES=5 \\\n    NPM_CONFIG_MAXSOCKETS=4\n" in rendered


def test_pnpm_frozen_lockfile_fallback_repeats_trailing_flags(tmp_path: Path) -> None:
    # Wrapping only the bare command stranded the trailing flags after the
    # closing parenthesis, so /bin/sh aborted with "word unexpected" before pnpm
    # ever ran. Both branches must carry the same flags.
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM node:22\nRUN pnpm install --frozen-lockfile --ignore-scripts\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True

    rendered = dockerfile.read_text()
    assert (
        "(pnpm install --frozen-lockfile --ignore-scripts "
        "|| pnpm install --no-frozen-lockfile --ignore-scripts)" in rendered
    )
    assert ") --ignore-scripts" not in rendered


def test_pnpm_frozen_lockfile_fallback_keeps_chained_commands_outside(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM node:22\nRUN pnpm install --frozen-lockfile --prefer-offline && pnpm run build\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True

    rendered = dockerfile.read_text()
    assert (
        "(pnpm install --frozen-lockfile --prefer-offline "
        "|| pnpm install --no-frozen-lockfile --prefer-offline) && pnpm run build" in rendered
    )


@pytest.mark.parametrize(
    "command",
    [
        "pnpm install --frozen-lockfile",
        "pnpm install --frozen-lockfile --ignore-scripts",
        "pnpm install --frozen-lockfile --prefer-offline --ignore-scripts",
        "pnpm install --frozen-lockfile && pnpm run build",
        "pnpm install --frozen-lockfile --ignore-scripts && pnpm run build",
    ],
)
def test_pnpm_frozen_lockfile_fallback_emits_valid_shell(tmp_path: Path, command: str) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(f"FROM node:22\nRUN {command}\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True

    run_line = next(
        line for line in dockerfile.read_text().splitlines() if line.startswith("RUN (")
    )
    # `sh -n` parses without executing, which is exactly the check the failing
    # builds were tripping over.
    assert subprocess.run(["sh", "-n", "-c", run_line[len("RUN ") :]]).returncode == 0


def test_bun_gets_the_mirror_registry_and_proxy_ca(tmp_path: Path) -> None:
    # Bun reads neither npm config nor .npmrc for TLS, so the npm-oriented
    # strict-ssl line left it failing with SELF_SIGNED_CERT_IN_CHAIN.
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM oven/bun:1\n"
        "COPY swegen-proxy-ca.crt /usr/local/share/ca-certificates/swegen-proxy-ca.crt\n"
        "RUN bun install --frozen-lockfile\n"
    )

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_BUN_ENV") == 1
    assert "ENV NPM_CONFIG_REGISTRY=https://registry.npmmirror.com/" in rendered
    assert (
        "ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/swegen-proxy-ca.crt" in rendered
    )
    assert rendered.index("# SWEGEN_BUN_ENV") < rendered.index("RUN bun install")


def test_bun_without_a_proxy_ca_only_sets_the_registry(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM oven/bun:1\nRUN bun install --frozen-lockfile\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True

    rendered = dockerfile.read_text()
    assert "ENV NPM_CONFIG_REGISTRY=https://registry.npmmirror.com/" in rendered
    assert "NODE_EXTRA_CA_CERTS" not in rendered


def test_pnpm_gets_the_mirror_registry(tmp_path: Path) -> None:
    # pnpm ignores `npm config set registry`, so it resolved against
    # registry.npmjs.org and absorbed that registry's 429 rate limiting.
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM node:22\nRUN pnpm install --frozen-lockfile\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_PNPM_REGISTRY") == 1
    assert "ENV NPM_CONFIG_REGISTRY=https://registry.npmmirror.com/" in rendered
    # Composes with the frozen-lockfile fallback rather than fighting it.
    assert "(pnpm install --frozen-lockfile || pnpm install --no-frozen-lockfile)" in rendered


def test_bun_and_pnpm_rewrites_skip_unrelated_dockerfiles(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM python:3.12\nRUN pip install -r requirements.txt\n")

    rewrite_ubuntu_mirrors(dockerfile)

    rendered = dockerfile.read_text()
    assert "SWEGEN_BUN_ENV" not in rendered
    assert "SWEGEN_PNPM_REGISTRY" not in rendered


def test_go_toolchain_gets_the_internal_module_proxy(tmp_path: Path) -> None:
    # Go reads neither the npm nor the apt mirror: it reaches proxy.golang.org
    # directly through the corporate proxy, which is the largest single source
    # of validator build failures.
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM golang:1.22\nRUN go mod download && go build ./...\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_GO_PROXY") == 1
    assert "GOPROXY=http://mirrors.tools.huawei.com/goproxy" in rendered
    assert "GO111MODULE=on" in rendered
    # The mirror does not serve the public checksum database.
    assert "GOSUMDB=off" in rendered
    # GOPRIVATE would make Go bypass the proxy and clone from the origin over
    # git, which is the unreachable path this rewrite exists to avoid: measured
    # 977ms through the proxy versus a 164s failure with GOPRIVATE=*.
    assert "GOPRIVATE" not in rendered
    assert rendered.index("# SWEGEN_GO_PROXY") < rendered.index("RUN go mod download")


def test_cargo_gets_the_internal_sparse_registry(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM rust:1.80\nRUN cargo fetch && cargo build\n")

    assert rewrite_ubuntu_mirrors(dockerfile) is True
    assert rewrite_ubuntu_mirrors(dockerfile) is False

    rendered = dockerfile.read_text()
    assert rendered.count("# SWEGEN_CARGO_MIRROR") == 1
    assert 'registry = \\"sparse+http://mirrors.tools.huawei.com/cargo/\\"' in rendered.replace(
        '"', '\\"'
    )
    assert rendered.index("# SWEGEN_CARGO_MIRROR") < rendered.index("RUN cargo fetch")


def test_cargo_config_is_valid_toml_after_the_shell_expands_it(tmp_path: Path) -> None:
    # The printf body is single-quoted for the shell, so a nested single quote
    # would be eaten and produce an unquoted `replace-with = mirror`.
    import os
    import tomllib

    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM rust:1.80\nRUN cargo fetch\n")
    rewrite_ubuntu_mirrors(dockerfile)

    command = next(
        line for line in dockerfile.read_text().splitlines() if line.startswith("RUN mkdir -p")
    )
    # Cargo ignores config.toml when a legacy config sits beside it, so the
    # rewrite must remove the legacy file rather than write both.
    assert '"$HOME/.cargo/config"' not in command.split("&& rm -f")[0]
    assert "rm -f" in command
    home = tmp_path / "home"
    home.mkdir()
    completed = subprocess.run(
        ["sh", "-c", command[len("RUN ") :]],
        env={**os.environ, "HOME": str(home), "CARGO_HOME": str(home / "cargo")},
    )

    assert completed.returncode == 0
    parsed = tomllib.loads((home / ".cargo" / "config.toml").read_text())
    assert parsed["source"]["crates-io"]["replace-with"] == "mirror"
    assert parsed["source"]["mirror"]["registry"].startswith("sparse+http")


def test_go_and_cargo_rewrites_skip_unrelated_dockerfiles(tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM python:3.12\nRUN pip install -r requirements.txt\n")

    rewrite_ubuntu_mirrors(dockerfile)

    rendered = dockerfile.read_text()
    assert "SWEGEN_GO_PROXY" not in rendered
    assert "SWEGEN_CARGO_MIRROR" not in rendered
