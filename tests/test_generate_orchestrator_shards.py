import json

import generate_orchestrator_shards as generator


def write_jsonl(path, records) -> None:
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


def test_generate_shards_is_balanced_disjoint_and_deterministic(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    records = [
        {"repo": "large/repo", "pull_number": index, "extra": "preserved"} for index in range(5)
    ]
    records += [{"repo": "medium/repo", "pull_number": index} for index in range(3)]
    records += [
        {"repo": "small/a", "pull_number": 1},
        {"repo": "small/b", "pull_number": 1},
        {"repo": "small/c", "pull_number": 1},
        {"repo": "small/d", "pull_number": 1},
    ]
    write_jsonl(source, records)
    names = ("r3-sg-a", "r3-hk-a", "r3-sg-b")

    first = generator.generate_shards(source, tmp_path / "out", names=names)
    first_manifest = (tmp_path / "out" / "r3-shards-manifest.json").read_bytes()
    first_shards = {name: (tmp_path / "out" / f"{name}.jsonl").read_bytes() for name in names}
    second = generator.generate_shards(source, tmp_path / "out", names=names)

    assert first == second
    assert (tmp_path / "out" / "r3-shards-manifest.json").read_bytes() == first_manifest
    assert {
        name: (tmp_path / "out" / f"{name}.jsonl").read_bytes() for name in names
    } == first_shards
    assert first["totals"] == {
        "entries": 12,
        "repositories": 6,
        "unique_repo_pr_pairs": 12,
    }
    assert first["balance"] == {
        "minimum_entries": 3,
        "maximum_entries": 5,
        "entry_spread": 2,
        "worker_counts": [4, 4, 4],
    }

    repo_owner = {}
    output_pairs = set()
    for shard_index, name in enumerate(names):
        output = [
            json.loads(line)
            for line in (tmp_path / "out" / f"{name}.jsonl").read_text().splitlines()
        ]
        for record in output:
            assert repo_owner.setdefault(record["repo"], shard_index) == shard_index
            output_pairs.add((record["repo"], str(record["pull_number"])))
    assert output_pairs == {(record["repo"], str(record["pull_number"])) for record in records}
    assert any(
        json.loads(line).get("extra") == "preserved"
        for content in first_shards.values()
        for line in content.decode().splitlines()
    )


def test_duplicate_repo_pr_pair_is_rejected(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    write_jsonl(
        source,
        [
            {"repo": "owner/repo", "pull_number": 1},
            {"repo": "owner/repo", "pull_number": "1"},
        ],
    )

    try:
        generator.generate_shards(source, tmp_path / "out")
    except ValueError as error:
        assert "duplicate owner/repo#1" in str(error)
    else:
        raise AssertionError("duplicate pair was accepted")


def test_r4_de_layout_uses_de_proxy_and_revision_paths(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    write_jsonl(source, [{"repo": "owner/repo", "pull_number": 1}])

    manifest = generator.generate_shards(
        source,
        tmp_path / "out",
        names=("r4-de-a",),
        manifest_name="r4-shards-manifest.json",
    )

    shard = manifest["shards"][0]
    assert shard["route"] == "DE"
    assert shard["proxy_env_file"] == ".env_de"
    assert shard["session"] == "swegen-de-a-r4"
    assert shard["log_dir"].endswith("orchestrator-logs-de-a-r4-4w")
    assert shard["progress_jsonl"].endswith("orchestrator-progress-de-a-r4.jsonl")


def test_generate_shards_supports_per_shard_worker_counts(tmp_path) -> None:
    source = tmp_path / "source.jsonl"
    write_jsonl(
        source,
        [{"repo": f"owner/{index}", "pull_number": index} for index in range(12)],
    )

    manifest = generator.generate_shards(
        source,
        tmp_path / "out",
        names=("r10-sg-a", "r10-hk-a"),
        workers=(4, 2),
    )

    assert [shard["workers"] for shard in manifest["shards"]] == [4, 2]
    assert manifest["shards"][1]["log_dir"].endswith("r10-2w")
    assert [shard["entries"] for shard in manifest["shards"]] == [8, 4]
