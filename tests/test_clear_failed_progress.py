import json

import clear_failed_progress as clear


def write_records(path, records) -> None:
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


def test_latest_instance_records_keeps_only_latest_successes(tmp_path) -> None:
    path = tmp_path / "status.jsonl"
    write_records(
        path,
        [
            {"instance": "a", "status": "failure", "value": 1},
            {"instance": "b", "status": "success", "value": 2},
            {"instance": "a", "status": "success", "value": 3},
            {"instance": "c", "status": "success", "value": 4},
            {"instance": "c", "status": "failure", "value": 5},
            {"unrelated": True},
        ],
    )

    records, input_lines, failures = clear.latest_instance_records(path)

    assert input_lines == 6
    assert failures == 1
    assert [(record["instance"], record["value"]) for record in records] == [
        ("b", 2),
        ("a", 3),
    ]


def test_compact_file_preserves_inode_and_creates_backup(tmp_path) -> None:
    path = tmp_path / "progress.jsonl"
    write_records(
        path,
        [
            {"instance": "a", "status": "failure"},
            {"instance": "b", "status": "success"},
        ],
    )
    inode = path.stat().st_ino

    result = clear.compact_file(path, "test")

    assert path.stat().st_ino == inode
    assert result.backup is not None
    assert result.backup.read_text().count("\n") == 2
    assert [json.loads(line) for line in path.read_text().splitlines()] == [
        {"instance": "b", "status": "success"}
    ]


def test_dry_run_does_not_write_or_backup(tmp_path) -> None:
    path = tmp_path / "status.jsonl"
    write_records(path, [{"instance": "a", "status": "failure"}])
    before = path.read_bytes()

    result = clear.compact_file(path, "dry", dry_run=True)

    assert result.dropped_failures == 1
    assert result.backup is None
    assert path.read_bytes() == before
    assert not clear.backup_path(path, "dry").exists()
