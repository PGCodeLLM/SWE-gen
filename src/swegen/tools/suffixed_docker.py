from __future__ import annotations

from pathlib import Path

from harbor.environments.docker.docker import DockerEnvironment
from harbor.models.task.config import EnvironmentConfig
from harbor.models.trial.paths import TrialPaths

DOCKER_NAME_SUFFIX = "-swegencontainer"
_COMPOSE_TEMPLATE = """services:
  main:
    {image_config}
    container_name: {container_name}
    command: [ "sh", "-c", "sleep infinity" ]
    network_mode: ${{NETWORK_MODE:-bridge}}
    environment:
      - TEST_DIR=${{TEST_DIR}}
    volumes:
      - ${{HOST_VERIFIER_LOGS_PATH}}:${{ENV_VERIFIER_LOGS_PATH}}
      - ${{HOST_AGENT_LOGS_PATH}}:${{ENV_AGENT_LOGS_PATH}}
    deploy:
      resources:
        limits:
          cpus: ${{CPUS}}
          memory: ${{MEMORY}}
"""


def with_swegen_container_suffix(name: str) -> str:
    """Append the swegen Docker suffix once."""
    return name if name.endswith(DOCKER_NAME_SUFFIX) else f"{name}{DOCKER_NAME_SUFFIX}"


def _compose_safe_name(name: str) -> str:
    return name.lower().replace(".", "-")


class SwegenDockerEnvironment(DockerEnvironment):
    """Docker environment that makes Harbor Docker resources easy to identify."""

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
        suffixed_session_id = with_swegen_container_suffix(session_id)
        super().__init__(
            environment_dir,
            environment_name,
            suffixed_session_id,
            trial_paths,
            task_env_config,
            *args,
            **kwargs,
        )
        self._env_vars.main_image_name = with_swegen_container_suffix(
            self._env_vars.main_image_name
        )
        self._swegen_container_name = _compose_safe_name(suffixed_session_id)
        self._swegen_compose_path = self.trial_paths.trial_dir / "docker-compose.swegen.yaml"

    @property
    def _docker_compose_path(self) -> Path:
        image_config = (
            "image: ${PREBUILT_IMAGE_NAME}"
            if self._use_prebuilt
            else "build:\n      context: ${CONTEXT_DIR}\n    image: ${MAIN_IMAGE_NAME}"
        )
        self._swegen_compose_path.write_text(
            _COMPOSE_TEMPLATE.format(
                image_config=image_config,
                container_name=self._swegen_container_name,
            )
        )
        return self._swegen_compose_path
