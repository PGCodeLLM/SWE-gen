import argparse
import json
import subprocess
import tarfile
from pathlib import Path

import pytest

import slurm_four_node_restart as four_node
import slurm_two_node as slurm


def write_source(path: Path) -> None:
    records = [
        {"repo": "owner/a", "pull_number": 1},
        {"repo": "owner/a", "pull_number": 2},
        {"repo": "owner/b", "pull_number": 3},
        {"repo": "owner/c", "pull_number": 4},
    ]
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


def test_four_node_manual_launcher_keeps_preflight_enabled_by_default(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["slurm_four_node_restart.py"])
    assert four_node.parse_args().skip_preflight is False

    monkeypatch.setattr("sys.argv", ["slurm_four_node_restart.py", "--skip-preflight"])
    assert four_node.parse_args().skip_preflight is True


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


def test_fallback_topology_is_12_workers_per_node_and_one_group_per_route() -> None:
    nodes = slurm.validate_nodes(slurm.DEFAULT_NODES, groups_per_route=1)
    names = slurm.shard_names(nodes, groups_per_route=1)

    assert len(names) == 6
    assert slurm.workers_per_node(1) == 12
    assert slurm.total_workers(1) == 24
    assert slurm.manifest_name(1) == "slurm-2n-24w-manifest.json"
    assert all(node.expected_workers == 12 for node in nodes)
    for node in nodes:
        node_names = [name for name in names if f"-n{node.index}-" in name]
        assert len(node_names) == 3
        assert {name.split("-")[1] for name in node_names} == {"sg", "hk", "de"}
        assert all(name.endswith("-a") for name in node_names)


def test_invalid_groups_per_route_is_rejected() -> None:
    with pytest.raises(ValueError, match="groups per route"):
        slurm.validate_nodes(slurm.DEFAULT_NODES, groups_per_route=0)
    with pytest.raises(ValueError, match="groups per route"):
        slurm.validate_nodes(slurm.DEFAULT_NODES, groups_per_route=3)


@pytest.mark.parametrize(
    ("value", "expected_total"),
    [("4,4,4", 48), ("8,8,8", 96), ("2,6,0", 32), ("1,1,1", 12)],
)
def test_four_node_route_workers_set_total_concurrency(value, expected_total) -> None:
    route_workers = four_node.parse_route_workers(value)
    workers_per_node = sum(route_workers)
    nodes = four_node.nodes_for_workers(workers_per_node)
    names = four_node.shard_names("r9", nodes, route_workers)

    assert sum(node.expected_workers for node in nodes) == expected_total
    groups = four_node.route_groups(route_workers)
    assert sum(group_workers for _route, _suffix, group_workers in groups) == workers_per_node
    assert sum(group_workers for _route, _suffix, group_workers in groups) * len(nodes) == (
        expected_total
    )
    assert len(names) == len(groups) * len(nodes)


@pytest.mark.parametrize(
    "value",
    ["4,4", "4,4,4,4", "4,invalid,4", "0,0,0", "13,4,4", "-1,4,4"],
)
def test_four_node_route_workers_reject_invalid_vectors(value) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        four_node.parse_route_workers(value)


def test_four_node_route_groups_split_partial_second_group() -> None:
    assert four_node.route_groups((2, 6, 8)) == [
        ("sg", "a", 2),
        ("hk", "a", 4),
        ("de", "a", 4),
        ("hk", "b", 2),
        ("de", "b", 4),
    ]


def test_four_node_prior_job_history_preserves_replaced_jobs(tmp_path) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text(
        json.dumps(
            {
                "revision": "r9",
                "topology": "stage1-four-node-4-4-4",
                "expected_workers": 48,
                "proxy_workers_per_node": {"sg": 4, "hk": 4, "de": 4},
                "nodes": [
                    {"node": "node-a", "index": 1, "job_id": "101"},
                    {"node": "node-b", "index": 2, "job_id": "102"},
                ],
            }
        )
    )

    history = four_node.prior_job_history(plan, "2026-07-22T00:00:00+00:00")

    assert len(history) == 1
    assert [node["job_id"] for node in history[0]["nodes"]] == ["101", "102"]
    plan.write_text(json.dumps({"job_history": history, "nodes": history[0]["nodes"]}))
    assert four_node.prior_job_history(plan, "later") == history


def test_four_node_write_remaining_counts_only_source_successes(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    write_source(source)
    destination = tmp_path / "remaining.jsonl"

    counts = four_node.write_remaining(
        source,
        destination,
        {"owner__a-1", "unrelated__success-999"},
    )

    assert counts == {
        "source_entries": 4,
        "successful_excluded": 1,
        "remaining": 3,
    }


def write_models(path: Path, entries: list[dict]) -> Path:
    path.write_text(json.dumps({"model_list": entries}) + "\n")
    path.chmod(0o600)
    return path


def model_entry(name: str, base: str, key: str) -> dict:
    return {
        "model_name": name,
        "litellm_params": {
            "model": name,
            "api_base": base,
            "api_key": key,
        },
    }


def test_four_node_model_roles_resolve_exact_names_into_private_credentials(tmp_path) -> None:
    models = write_models(
        tmp_path / "models.yaml",
        [
            model_entry("chosen-opus-extra", "https://wrong.example", "wrong"),
            model_entry("chosen-opus", "https://gateway.example/v1", "shared-secret"),
            model_entry("chosen-sonnet", "https://gateway.example/v1", "shared-secret"),
        ],
    )

    credentials, public = four_node.load_model_credentials(
        models,
        opus_model="chosen-opus",
        sonnet_model="chosen-sonnet",
    )

    assert credentials["ANTHROPIC_DEFAULT_OPUS_MODEL"] == "chosen-opus"
    assert credentials["ANTHROPIC_DEFAULT_SONNET_MODEL"] == "chosen-sonnet"
    assert credentials["OPENAI_BASE_URL"] == "https://gateway.example/v1"
    assert credentials["ANTHROPIC_BASE_URL"] == "https://gateway.example"
    assert public["roles"] == {"opus": "chosen-opus", "sonnet": "chosen-sonnet"}
    assert "shared-secret" not in json.dumps(public)


@pytest.mark.parametrize("different_field", ["api_base", "api_key"])
def test_four_node_model_roles_fail_closed_on_incompatible_runtime(
    tmp_path, different_field
) -> None:
    opus = model_entry("chosen-opus", "https://gateway.example", "shared-secret")
    sonnet = model_entry("chosen-sonnet", "https://gateway.example", "shared-secret")
    sonnet["litellm_params"][different_field] = "different"
    models = write_models(tmp_path / "models.yaml", [opus, sonnet])

    with pytest.raises(ValueError, match="must share one non-empty"):
        four_node.load_model_credentials(
            models,
            opus_model="chosen-opus",
            sonnet_model="chosen-sonnet",
        )


def test_four_node_model_roles_allow_identical_duplicate_entries(tmp_path) -> None:
    entry = model_entry("shared", "https://gateway.example", "shared-secret")
    models = write_models(tmp_path / "models.yaml", [entry, entry.copy()])

    credentials, public = four_node.load_model_credentials(
        models,
        opus_model="shared",
        sonnet_model="shared",
    )

    assert credentials["ANTHROPIC_DEFAULT_OPUS_MODEL"] == "shared"
    assert credentials["ANTHROPIC_DEFAULT_SONNET_MODEL"] == "shared"
    assert public["models"] == ["shared"]


def test_four_node_model_pool_loads_two_complete_endpoints_without_secret_metadata(
    tmp_path,
) -> None:
    models = write_models(
        tmp_path / "models.yaml",
        [
            model_entry("chosen-opus", "https://primary.example/v1", "primary-secret"),
            model_entry("chosen-sonnet", "https://primary.example/v1", "primary-secret"),
            model_entry("chosen-opus", "http://1.95.77.23:3000", "secondary-secret"),
            model_entry("chosen-sonnet", "http://1.95.77.23:3000", "secondary-secret"),
        ],
    )

    profiles, public = four_node.load_model_credential_pool(
        models,
        opus_model="chosen-opus",
        sonnet_model="chosen-sonnet",
    )

    assert len(profiles) == 2
    assert profiles[0]["OPENAI_BASE_URL"] == "https://primary.example/v1"
    assert profiles[0]["ANTHROPIC_BASE_URL"] == "https://primary.example"
    assert profiles[1]["OPENAI_BASE_URL"] == "http://1.95.77.23:3000/v1"
    assert profiles[1]["ANTHROPIC_BASE_URL"] == "http://1.95.77.23:3000"
    assert profiles[1]["OPENAI_MODEL"] == "chosen-opus"
    assert profiles[1]["ANTHROPIC_DEFAULT_SONNET_MODEL"] == "chosen-sonnet"
    assert [profile["name"] for profile in public["profiles"]] == [
        "backend-000",
        "backend-001",
    ]
    assert public["endpoints"] == [
        "https://primary.example/v1",
        "http://1.95.77.23:3000",
    ]
    assert public["routing_strategy"].startswith("per-task-round-robin")
    rendered = json.dumps(public)
    assert "primary-secret" not in rendered
    assert "secondary-secret" not in rendered


def test_four_node_model_pool_deduplicates_exact_duplicate_entries(tmp_path) -> None:
    entries = [
        model_entry("chosen-opus", "https://primary.example", "primary-secret"),
        model_entry("chosen-sonnet", "https://primary.example", "primary-secret"),
        model_entry("chosen-opus", "https://secondary.example", "secondary-secret"),
        model_entry("chosen-sonnet", "https://secondary.example", "secondary-secret"),
    ]
    models = write_models(tmp_path / "models.yaml", [*entries, *entries])

    profiles, public = four_node.load_model_credential_pool(
        models,
        opus_model="chosen-opus",
        sonnet_model="chosen-sonnet",
    )

    assert len(profiles) == 2
    assert len(public["profiles"]) == 2


def test_four_node_model_pool_rejects_incomplete_endpoint(tmp_path) -> None:
    models = write_models(
        tmp_path / "models.yaml",
        [
            model_entry("chosen-opus", "https://primary.example", "primary-secret"),
            model_entry("chosen-sonnet", "https://primary.example", "primary-secret"),
            model_entry("chosen-opus", "https://incomplete.example", "secondary-secret"),
        ],
    )

    with pytest.raises(ValueError, match="incomplete profile.*missing sonnet"):
        four_node.load_model_credential_pool(
            models,
            opus_model="chosen-opus",
            sonnet_model="chosen-sonnet",
        )


def test_four_node_model_pool_rejects_conflicting_duplicate_role(tmp_path) -> None:
    opus = model_entry("chosen-opus", "https://primary.example", "primary-secret")
    conflicting_opus = model_entry("chosen-opus", "https://primary.example", "primary-secret")
    conflicting_opus["litellm_params"]["rpm"] = 99
    models = write_models(
        tmp_path / "models.yaml",
        [
            opus,
            conflicting_opus,
            model_entry("chosen-sonnet", "https://primary.example", "primary-secret"),
        ],
    )

    with pytest.raises(ValueError, match="conflicting entries for opus"):
        four_node.load_model_credential_pool(
            models,
            opus_model="chosen-opus",
            sonnet_model="chosen-sonnet",
        )


def test_four_node_model_pool_rejects_secret_bearing_endpoint_urls(tmp_path) -> None:
    models = write_models(
        tmp_path / "models.yaml",
        [
            model_entry("chosen-opus", "https://user:password@example.test", "secret"),
            model_entry("chosen-sonnet", "https://user:password@example.test", "secret"),
        ],
    )

    with pytest.raises(ValueError, match="invalid public api_base"):
        four_node.load_model_credential_pool(
            models,
            opus_model="chosen-opus",
            sonnet_model="chosen-sonnet",
        )


def test_four_node_model_pool_rejects_nonstandard_or_secret_bearing_paths(tmp_path) -> None:
    models = write_models(
        tmp_path / "models.yaml",
        [
            model_entry("chosen-opus", "https://example.test/token-secret", "secret"),
            model_entry("chosen-sonnet", "https://example.test/token-secret", "secret"),
        ],
    )

    with pytest.raises(ValueError, match="invalid public api_base"):
        four_node.load_model_credential_pool(
            models,
            opus_model="chosen-opus",
            sonnet_model="chosen-sonnet",
        )


def test_model_profile_assignment_is_deterministic_and_balanced_across_nodes() -> None:
    nodes = four_node.nodes_for_workers(12)
    shards = four_node.shard_names("r9", nodes, (4, 4, 4))

    assignments = four_node.assign_model_profiles(shards, 2)

    assert assignments == four_node.assign_model_profiles(shards, 2)
    assert list(assignments.values()) == [
        f"backend-{index % 2:03d}" for index in range(len(shards))
    ]
    assert list(assignments.values()).count("backend-000") == 6
    assert list(assignments.values()).count("backend-001") == 6
    for node in nodes:
        node_profiles = [assignments[shard] for shard in shards if f"-n{node.index}-" in shard]
        assert abs(node_profiles.count("backend-000") - node_profiles.count("backend-001")) <= 1


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


def test_prepare_shards_uses_fallback_manifest_and_six_shards(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    write_source(source)
    run_dir = tmp_path / "runs" / "run"
    shard_dir = tmp_path / "shards"
    nodes = slurm.validate_nodes(slurm.DEFAULT_NODES, groups_per_route=1)

    manifest, _counts = slurm.prepare_shards(
        source,
        run_dir,
        shard_dir,
        "run",
        nodes,
        groups_per_route=1,
    )

    assert len(manifest["shards"]) == 6
    assert (shard_dir / "slurm-2n-24w-manifest.json").is_file()


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

    job_id = slurm.submit_node(
        tmp_path,
        node,
        "/tmp/work",
        "run",
        "shards",
        groups_per_route=1,
    )

    assert job_id == "42"
    rendered = " ".join(captured)
    assert "--export=NONE" in captured
    assert captured[captured.index("--groups-per-route") + 1] == "1"
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


def test_stage_node_can_transfer_and_preflight_over_ssh(tmp_path, monkeypatch) -> None:
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"bundle")
    node = slurm.NodeSpec("node-1", "7.244.3.78", 1, 0)
    calls: list[tuple[list[str], bool, int]] = []

    def fake_run(argv, *, stdin=None, timeout=1800):
        calls.append((list(argv), stdin is not None, timeout))
        return subprocess.CompletedProcess(argv, 0, stdout=b"preflight ok\n", stderr=b"")

    monkeypatch.setattr(slurm, "_run", fake_run)

    result = slurm.stage_node(
        bundle,
        node,
        "/data/work/slurm-swegen/runtime/workspace",
        "run",
        "shards",
        transport="ssh",
    )

    assert result == "preflight ok"
    assert len(calls) == 2
    assert all(call[0][0] == "ssh" for call in calls)
    assert all("alex@7.244.3.78" in call[0] for call in calls)
    assert all("srun" not in call[0] for call in calls)
    assert calls[0][1] is True
    assert "tar -xzf -" in calls[0][0][-1]
    assert calls[1][2] == 3600


def test_stage_node_can_skip_preflight(tmp_path, monkeypatch) -> None:
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"bundle")
    node = slurm.NodeSpec("node-1", "7.244.3.78", 1, 0)
    commands: list[str] = []

    def fake_run(argv, *, stdin=None, timeout=1800):
        commands.append(argv[-1])
        return subprocess.CompletedProcess(argv, 0, stdout=b"", stderr=b"")

    monkeypatch.setattr(slurm, "_run", fake_run)

    slurm.stage_node(
        bundle,
        node,
        "/data/work/slurm-swegen/runtime/workspace",
        "run",
        "shards",
        transport="ssh",
        skip_preflight=True,
    )

    assert len(commands) == 2
    assert "--preflight" not in commands[1]
    assert "preflight skipped by explicit operator request" in commands[1]


def test_stage_node_retries_transient_ssh_and_reopens_bundle(tmp_path, monkeypatch) -> None:
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"bundle")
    node = slurm.NodeSpec("node-1", "7.244.3.78", 1, 0)
    transfer_payloads: list[bytes] = []
    sleeps: list[int] = []

    def fake_run(argv, *, stdin=None, timeout=1800):
        if stdin is not None:
            transfer_payloads.append(stdin.read())
            if len(transfer_payloads) < 3:
                raise slurm.SlurmLaunchError("command failed (255): server not responding")
        return subprocess.CompletedProcess(argv, 0, stdout=b"preflight ok\n", stderr=b"")

    monkeypatch.setattr(slurm, "_run", fake_run)
    monkeypatch.setattr(slurm.time, "sleep", sleeps.append)

    result = slurm.stage_node(
        bundle,
        node,
        "/data/work/slurm-swegen/runtime/workspace",
        "run",
        "shards",
        transport="ssh",
    )

    assert result == "preflight ok"
    assert transfer_payloads == [b"bundle", b"bundle", b"bundle"]
    assert sleeps == [2, 4]


def test_stage_node_runs_the_local_slurm_node_without_self_ssh(tmp_path, monkeypatch) -> None:
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"bundle")
    calls: list[list[str]] = []

    def fake_run(argv, **_kwargs):
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(slurm.socket, "gethostname", lambda: "local-node")
    monkeypatch.setattr(slurm, "command_prefix", lambda: ["sudo", "-u", "alex", "-H"])
    monkeypatch.setattr(slurm, "_run", fake_run)

    slurm.stage_node(
        bundle,
        slurm.NodeSpec("local-node", "7.244.3.200", 1, 0),
        "/data/work/runtime",
        "run",
        "shards",
        transport="ssh",
    )

    assert len(calls) == 2
    assert all(call[:5] == ["sudo", "-u", "alex", "-H", "bash"] for call in calls)
    assert all("ssh" not in call for call in calls)


def test_stage_node_rejects_an_unknown_transport(tmp_path) -> None:
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"bundle")

    with pytest.raises(ValueError, match="stage transport"):
        slurm.stage_node(
            bundle,
            slurm.NodeSpec("node-1", "127.0.0.1", 1, 0),
            "/tmp/workspace",
            "run",
            "shards",
            transport="invalid",
        )


@pytest.mark.parametrize(("groups_per_route", "expected_shards"), [(2, 6), (1, 3)])
def test_bundle_contains_only_node_shards_and_private_env_files(
    tmp_path, monkeypatch, groups_per_route: int, expected_shards: int
) -> None:
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
    nodes = slurm.validate_nodes(slurm.DEFAULT_NODES, groups_per_route)
    for name in slurm.shard_names(nodes, groups_per_route):
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
    node = nodes[0]

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
        groups_per_route,
    )
    try:
        assert bundle.parent == workspace / ".swegen-slurm-bundles"
        assert bundle.stat().st_mode & 0o777 == 0o600
        assert bundle.parent.stat().st_mode & 0o777 == 0o700
        with tarfile.open(bundle, "r:gz") as archive:
            members = {member.name: member for member in archive.getmembers()}
        shard_members = [name for name in members if name.endswith(".jsonl")]
        assert len(shard_members) == expected_shards
        assert all("-n1-" in name for name in shard_members)
        assert members[".slurm-secrets/env/.env"].mode == 0o600
        assert members[".slurm-secrets/env/.env_hk"].mode == 0o600
        assert members[".slurm-secrets/env/.env_de"].mode == 0o600
        assert members[".slurm-secrets/credentials.env"].mode == 0o600
        assert members[".slurm-secrets/model-profiles/backend-000.env"].mode == 0o600
        assert members[".slurm-secrets/model-profiles/profiles.list"].mode == 0o600
        assert members[".slurm-secrets/swegen.toml"].mode == 0o600
        assert "swegen.toml" not in members
    finally:
        bundle.unlink(missing_ok=True)


def test_bundle_removes_partial_archive_when_population_fails(tmp_path, monkeypatch) -> None:
    def fail_population(bundle, *_args, **_kwargs):
        bundle.write_text("credential-bearing partial archive")
        raise RuntimeError("bundle population failed")

    monkeypatch.setattr(slurm, "_populate_bundle", fail_population)

    with pytest.raises(RuntimeError, match="population failed"):
        slurm.build_bundle(
            tmp_path,
            slurm.NodeSpec("node", "127.0.0.1", 1, 0),
            {},
            "run",
            {},
            tmp_path / "uv",
            None,
            {"OPENAI_API_KEY": "private"},
        )

    staging_dir = tmp_path / ".swegen-slurm-bundles"
    assert staging_dir.is_dir()
    assert list(staging_dir.iterdir()) == []


def test_bundle_stages_multiple_private_profiles_and_backend_zero_fallback(
    tmp_path, monkeypatch
) -> None:
    workspace = tmp_path
    monkeypatch.setattr(slurm, "_tracked_files", lambda _workspace: [])
    shard_dir = workspace / "shards"
    shard_dir.mkdir()
    manifest_path = shard_dir / "manifest.json"
    manifest_path.write_text("{}\n")
    nodes = slurm.validate_nodes(slurm.DEFAULT_NODES, groups_per_route=1)
    shards = []
    for name in slurm.shard_names(nodes, groups_per_route=1):
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
    credential_profiles = [
        {
            "OPENAI_API_KEY": "primary-secret",
            "ANTHROPIC_API_KEY": "primary-secret",
            "OPENAI_BASE_URL": "https://primary.example/v1",
        },
        {
            "OPENAI_API_KEY": "secondary-secret",
            "ANTHROPIC_API_KEY": "secondary-secret",
            "OPENAI_BASE_URL": "http://1.95.77.23:3000/v1",
        },
    ]

    bundle = slurm.build_bundle(
        workspace,
        nodes[0],
        manifest,
        "run",
        env_files,
        uv,
        None,
        credential_profiles,
        groups_per_route=1,
    )
    try:
        with tarfile.open(bundle, "r:gz") as archive:
            members = {member.name: member for member in archive.getmembers()}
            primary = archive.extractfile(
                members[".slurm-secrets/model-profiles/backend-000.env"]
            ).read()
            secondary = archive.extractfile(
                members[".slurm-secrets/model-profiles/backend-001.env"]
            ).read()
            fallback = archive.extractfile(members[".slurm-secrets/credentials.env"]).read()
            profile_list = archive.extractfile(
                members[".slurm-secrets/model-profiles/profiles.list"]
            ).read()
        assert primary == fallback
        assert b"primary-secret" in primary
        assert b"secondary-secret" not in primary
        assert b"secondary-secret" in secondary
        assert profile_list == b"backend-000.env\nbackend-001.env\n"
        assert members[".slurm-secrets/model-profiles/backend-000.env"].mode == 0o600
        assert members[".slurm-secrets/model-profiles/backend-001.env"].mode == 0o600
        assert all("primary-secret" not in name for name in members)
        assert all("secondary-secret" not in name for name in members)
    finally:
        bundle.unlink(missing_ok=True)


def test_bundle_reuse_mode_does_not_replace_remote_model_profiles(tmp_path, monkeypatch) -> None:
    workspace = tmp_path
    monkeypatch.setattr(slurm, "_tracked_files", lambda _workspace: [])
    shard_dir = workspace / "shards"
    shard_dir.mkdir()
    manifest_path = shard_dir / "manifest.json"
    manifest_path.write_text("{}\n")
    nodes = slurm.validate_nodes(slurm.DEFAULT_NODES, groups_per_route=1)
    shards = []
    for name in slurm.shard_names(nodes, groups_per_route=1):
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

    bundle = slurm.build_bundle(
        workspace,
        nodes[0],
        manifest,
        "run",
        env_files,
        uv,
        None,
        None,
        groups_per_route=1,
    )
    try:
        with tarfile.open(bundle, "r:gz") as archive:
            names = archive.getnames()
        assert ".slurm-secrets/credentials.env" not in names
        assert not any(name.startswith(".slurm-secrets/model-profiles/") for name in names)
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
    assert "timeout --kill-after=5s 15s docker image rm" in worker
    assert "bash curl docker git jq timeout" in worker
    assert "env.HTTP_PROXY" in launcher
    assert 'export DOCKER_CONFIG="$SWEGEN_DOCKER_CONFIG_DIR"' in launcher
    assert "umask 0077" in worker
    assert "groups_per_route=3" in worker
    assert "group_index < groups_per_route" in worker
    assert "--skip-preflight) skip_preflight=1" in worker
    assert "preflight skipped by explicit operator request" in worker
    assert "SWEGEN_RUNTIME_CREDENTIALS_FILE" in worker
    assert 'export TMPDIR="$workspace/.tmp"' in worker
    assert 'export CLAUDE_CODE_TMPDIR="$workspace/.claude-tmp"' in worker
    assert "^200/(200|404)/200$" in worker
    assert "count_tokens=unsupported" in worker
    assert "SWEGEN_MODEL_PROFILE_DIR" in worker
    assert "SWEGEN_MODEL_PROFILE" in worker
    assert "global_group_index" in worker


def test_slurm_worker_round_robins_profile_fallbacks_across_nodes(tmp_path) -> None:
    workspace = tmp_path
    secrets = workspace / ".slurm-secrets"
    (secrets / "env").mkdir(parents=True)
    (secrets / "model-profiles").mkdir()
    (secrets / "swegen.toml").write_text('[github]\ngh_tokens = ["test"]\n')
    for filename in slurm.ROUTE_ENV_FILES.values():
        (secrets / "env" / filename).write_text("HTTPS_PROXY=http://proxy.example:8080\n")
    for index in range(2):
        (secrets / "model-profiles" / f"backend-{index:03d}.env").write_text(
            f"export OPENAI_API_KEY=secret-{index}\n"
        )
    (secrets / "model-profiles" / "profiles.list").write_text("backend-000.env\nbackend-001.env\n")
    (secrets / "credentials.env").write_text("export OPENAI_API_KEY=secret-0\n")
    shard_dir = workspace / "shards"
    shard_dir.mkdir()
    for node_index in (1, 2):
        for route in slurm.ROUTES:
            (shard_dir / f"r9-{route}-n{node_index}-a.jsonl").write_text("{}\n")
    (workspace / "run_orchestrator.sh").write_text(
        "#!/bin/bash\n"
        'printf \'%s %s %s %s\\n\' "$SWEGEN_SLURM_GROUP" "$SWEGEN_MODEL_PROFILE" '
        '"$SWEGEN_RUNTIME_CREDENTIALS_FILE" "$SWEGEN_MODEL_PROFILE_DIR" '
        '>>"$PWD/assignments.txt"\n'
    )
    worker = Path("src/slurm_node_worker.sh").resolve()

    for node_index in (1, 2):
        subprocess.run(
            [
                "bash",
                str(worker),
                "--workspace",
                str(workspace),
                "--run-name",
                "run",
                "--node-index",
                str(node_index),
                "--initial-delay",
                "0",
                "--shard-dir",
                "shards",
                "--shard-revision",
                "r9",
                "--route-workers",
                "4,4,4",
                "--preserve-master-config",
                "--skip-preflight",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )

    assignments = {}
    for line in (workspace / "assignments.txt").read_text().splitlines():
        shard, profile, credential_file, profile_dir = line.split()
        assignments[shard] = profile
        assert credential_file == str(secrets / "model-profiles" / f"{profile}.env")
        assert profile_dir == str(secrets / "model-profiles")
    assert assignments == {
        "r9-sg-n1-a": "backend-000",
        "r9-hk-n1-a": "backend-001",
        "r9-de-n1-a": "backend-000",
        "r9-sg-n2-a": "backend-001",
        "r9-hk-n2-a": "backend-000",
        "r9-de-n2-a": "backend-001",
    }
