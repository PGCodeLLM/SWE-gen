import json
import subprocess
import tarfile
from pathlib import Path

import pytest

import slurm_two_node as slurm


def write_source(path: Path) -> None:
    records = [
        {"repo": "owner/a", "pull_number": 1},
        {"repo": "owner/a", "pull_number": 2},
        {"repo": "owner/b", "pull_number": 3},
        {"repo": "owner/c", "pull_number": 4},
    ]
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


def test_topology_is_exactly_two_nodes_and_48_workers() -> None:
    nodes = slurm.validate_nodes(slurm.DEFAULT_NODES)

    assert [node.node_ip for node in nodes] == ["7.244.3.78", "7.244.2.110"]
    assert [node.initial_delay_seconds for node in nodes] == [0, 30]
    assert len(slurm.shard_names(nodes)) == 12
    assert slurm.WORKERS_PER_NODE == 24
    assert slurm.TOTAL_WORKERS == 48
    for node in nodes:
        names = [name for name in slurm.shard_names(nodes) if f"-n{node.index}-" in name]
        assert len(names) == 6
        assert sum(name.startswith("r6-sg-") for name in names) == 2
        assert sum(name.startswith("r6-hk-") for name in names) == 2
        assert sum(name.startswith("r6-de-") for name in names) == 2


def test_duplicate_or_wrong_node_count_is_rejected() -> None:
    with pytest.raises(ValueError, match="exactly two"):
        slurm.validate_nodes([slurm.DEFAULT_NODES[0]])
    with pytest.raises(ValueError, match="unique"):
        slurm.validate_nodes([slurm.DEFAULT_NODES[0], slurm.DEFAULT_NODES[0]])


def test_private_swegen_config_is_required_and_mode_0600(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="missing private SWE-gen config"):
        slurm.private_swegen_config(tmp_path)

    config = tmp_path / "swegen.toml"
    config.write_text('[github]\ngh_tokens = ["test-token"]\n')
    config.chmod(0o644)
    with pytest.raises(PermissionError, match="mode 0600"):
        slurm.private_swegen_config(tmp_path)

    config.chmod(0o600)
    assert slurm.private_swegen_config(tmp_path) == config


def test_prepare_shards_filters_completed_and_preserves_repo_ownership(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    write_source(source)
    run_dir = tmp_path / "runs" / "run"
    (run_dir / "tasks" / "owner__a-1").mkdir(parents=True)
    (run_dir / "create.jsonl").write_text(json.dumps({"task_id": "owner__a-1"}) + "\n")
    shard_dir = tmp_path / "shards"
    nodes = slurm.validate_nodes(slurm.DEFAULT_NODES)

    manifest, counts = slurm.prepare_shards(source, run_dir, shard_dir, "run", nodes)

    assert counts == {"source_entries": 4, "completed": 1, "remaining": 3}
    assert manifest["totals"]["entries"] == 3
    assert len(manifest["shards"]) == 12
    repo_locations: dict[str, set[str]] = {}
    for shard in manifest["shards"]:
        for line in Path(shard["path"]).read_text().splitlines():
            record = json.loads(line)
            repo_locations.setdefault(record["repo"], set()).add(shard["name"])
    assert all(len(locations) == 1 for locations in repo_locations.values())


def test_submit_argv_contains_no_credentials(tmp_path, monkeypatch) -> None:
    worker = tmp_path / "src" / "slurm_node_worker.sh"
    worker.parent.mkdir()
    worker.write_text("#!/bin/bash\n")
    captured: list[str] = []

    def fake_run(argv, **_kwargs):
        captured.extend(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=b"42\n", stderr=b"")

    monkeypatch.setattr(slurm, "_run", fake_run)
    monkeypatch.setattr(slurm, "command_prefix", lambda: [])
    node = slurm.validate_nodes(slurm.DEFAULT_NODES)[0]

    job_id = slurm.submit_node(tmp_path, node, "/tmp/work", "run", "shards")

    assert job_id == "42"
    rendered = " ".join(captured)
    assert "--export=NONE" in captured
    assert "OPENAI_API_KEY" not in rendered
    assert "ANTHROPIC_API_KEY" not in rendered
    assert "sk-" not in rendered


def test_tracked_files_runs_git_as_runtime_user_when_prefixed(tmp_path, monkeypatch) -> None:
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("ok\n")
    captured: list[str] = []

    def fake_run(argv, **_kwargs):
        captured.extend(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=b"tracked.txt\0", stderr=b"")

    monkeypatch.setattr(slurm, "command_prefix", lambda: ["sudo", "-u", "alex", "-H"])
    monkeypatch.setattr(slurm.subprocess, "run", fake_run)

    assert slurm._tracked_files(tmp_path) == [tracked]
    assert captured[:5] == ["sudo", "-u", "alex", "-H", "git"]


def test_bundle_contains_only_node_shards_and_private_env_files(tmp_path, monkeypatch) -> None:
    workspace = tmp_path
    (workspace / "src").mkdir()
    (workspace / "src" / "slurm_two_node.py").write_text("# launcher\n")
    (workspace / "src" / "slurm_node_worker.sh").write_text("# worker\n")
    monkeypatch.setattr(slurm, "_tracked_files", lambda _workspace: [])
    shard_dir = workspace / "shards"
    shard_dir.mkdir()
    manifest_path = shard_dir / "manifest.json"
    manifest_path.write_text("{}\n")
    shards = []
    for name in slurm.shard_names(slurm.validate_nodes(slurm.DEFAULT_NODES)):
        path = shard_dir / f"{name}.jsonl"
        path.write_text("{}\n")
        shards.append({"name": name, "path": str(path)})
    manifest = {"manifest_path": str(manifest_path), "shards": shards}
    env_files = {}
    for route, name in slurm.ROUTE_ENV_FILES.items():
        path = workspace / name
        path.write_text(f"ROUTE={route}\n")
        env_files[route] = path
    uv = workspace / "uv"
    uv.write_bytes(b"uv")
    swegen_config = workspace / "swegen.toml"
    swegen_config.write_text('[github]\ngh_tokens = ["test-token"]\n')
    swegen_config.chmod(0o600)
    node = slurm.validate_nodes(slurm.DEFAULT_NODES)[0]

    bundle = slurm.build_bundle(
        workspace,
        node,
        manifest,
        "run",
        env_files,
        uv,
        None,
        {
            "OPENAI_API_KEY": "test-openai",
            "ANTHROPIC_API_KEY": "test-anthropic",
            "ANTHROPIC_AUTH_TOKEN": "test-anthropic",
        },
    )
    try:
        with tarfile.open(bundle, "r:gz") as archive:
            members = {member.name: member for member in archive.getmembers()}
        shard_members = [name for name in members if name.endswith(".jsonl")]
        assert len(shard_members) == 6
        assert all("-n1-" in name for name in shard_members)
        assert members[".slurm-secrets/env/.env"].mode == 0o600
        assert members[".slurm-secrets/env/.env_hk"].mode == 0o600
        assert members[".slurm-secrets/env/.env_de"].mode == 0o600
        assert members[".slurm-secrets/credentials.env"].mode == 0o600
        assert members[".slurm-secrets/swegen.toml"].mode == 0o600
        assert "swegen.toml" not in members
    finally:
        bundle.unlink(missing_ok=True)


def test_slurm_worker_uses_authenticated_quota_and_isolated_docker_configs() -> None:
    worker = Path("src/slurm_node_worker.sh").read_text()
    launcher = Path("run_orchestrator.sh").read_text()

    assert "https://api.github.com/rate_limit" in worker
    assert "github_remaining" in worker
    assert "SWEGEN_DOCKER_CONFIG_DIR" in worker
    assert ".slurm-secrets/docker/$shard_name" in worker
    assert "docker_build_proxy=ok" in worker
    assert "env.HTTP_PROXY" in launcher
    assert 'export DOCKER_CONFIG="$SWEGEN_DOCKER_CONFIG_DIR"' in launcher
    assert "umask 0077" in worker
