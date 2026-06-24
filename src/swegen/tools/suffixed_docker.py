from __future__ import annotations

from pathlib import Path

from harbor.environments.docker.docker import DockerEnvironment
from harbor.models.task.config import EnvironmentConfig
from harbor.models.trial.paths import TrialPaths

DOCKER_IMAGE_SUFFIX = "-swegenimage"
_IMAGE_COMPOSE_TEMPLATE = """services:
  main:
    image: ${MAIN_IMAGE_NAME}
"""


def _append_suffix_once(name: str, suffix: str) -> str:
    return name if name.endswith(suffix) else f"{name}{suffix}"


def with_swegen_image_suffix(name: str) -> str:
    """Append the swegen Docker image suffix once."""
    return _append_suffix_once(name, DOCKER_IMAGE_SUFFIX)


class SwegenDockerEnvironment(DockerEnvironment):
    """Docker environment that tags SWE-gen-built images predictably."""

    def __init__(
        self,
        environment_dir: Path,
        environment_name: str,
        session_id: str,
        trial_paths: TrialPaths,
        task_env_config: EnvironmentConfig,
        *args,
        **kwargs,
    ):
        super().__init__(
            environment_dir,
            environment_name,
            session_id,
            trial_paths,
            task_env_config,
            *args,
            **kwargs,
        )
        self._env_vars.main_image_name = with_swegen_image_suffix(
            self._env_vars.main_image_name
        )
        self._swegen_compose_path = (
            self.trial_paths.trial_dir / "docker-compose.swegen-image.yaml"
        )

    @property
    def _docker_compose_paths(self) -> list[Path]:
        paths = list(super()._docker_compose_paths)
        if self._use_prebuilt:
            return paths

        self._swegen_compose_path.write_text(_IMAGE_COMPOSE_TEMPLATE)
        return [*paths, self._swegen_compose_path]
