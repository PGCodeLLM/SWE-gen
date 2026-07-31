from pathlib import Path

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
    assert "curl -O http://cloud-images.ubuntu.com" not in rendered


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
    assert rendered.index("# SWEGEN_UBUNTU_MIRRORS") < rendered.index("RUN apt-get update")


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
        "apt-get install -y npm && npm config set strict-ssl false && "
        "npm config set registry http://mirrors.tools.huawei.com/npm/ && "
        "npm cache clean -f && npm ci"
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
        "RUN npm config set strict-ssl false && "
        "npm config set registry http://mirrors.tools.huawei.com/npm/ && "
        "npm cache clean -f && npm install"
    ) in rendered
    assert (
        "RUN npm config set strict-ssl false && "
        "npm config set registry http://mirrors.tools.huawei.com/npm/ && "
        "npm cache clean -f\n"
    ) in rendered
