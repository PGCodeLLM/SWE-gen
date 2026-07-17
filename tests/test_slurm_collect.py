import io
import json
import tarfile

import pytest

import slurm_collect as collector


def archive_bytes(name: str, content: bytes) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        info = tarfile.TarInfo(name)
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def test_safe_extract_rejects_path_traversal(tmp_path) -> None:
    with pytest.raises(ValueError, match="unsafe"):
        collector.safe_extract(archive_bytes("../escape", b"bad"), tmp_path)


def test_safe_extract_writes_inside_destination(tmp_path) -> None:
    collector.safe_extract(archive_bytes("nested/status.jsonl", b"{}\n"), tmp_path)
    assert (tmp_path / "nested" / "status.jsonl").read_bytes() == b"{}\n"


def test_active_job_srun_uses_overlap(monkeypatch) -> None:
    monkeypatch.setattr(collector, "command_prefix", lambda: [])
    argv = collector.srun_base("node-a", "123", "RUNNING")
    assert "--jobid=123" in argv
    assert "--overlap" in argv
    assert "--nodelist=node-a" in argv


def test_collect_once_writes_combined_health(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    plan = {
        "run_name": "run",
        "run_dir": str(run_dir),
        "expected_workers": 48,
        "nodes": [
            {
                "node": "node-a",
                "node_ip": "1.1.1.1",
                "job_id": "11",
                "remote_run_dir": "/tmp/a",
                "expected_workers": 24,
            },
            {
                "node": "node-b",
                "node_ip": "1.1.1.2",
                "job_id": "12",
                "remote_run_dir": "/tmp/b",
                "expected_workers": 24,
            },
        ],
    }
    plan_path = run_dir / "slurm-plan.json"
    plan_path.write_text(json.dumps(plan))
    monkeypatch.setattr(collector, "job_state", lambda job_id: "RUNNING")
    monkeypatch.setattr(collector, "collect_archive", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        collector,
        "collect_health",
        lambda *args, **kwargs: {
            "error_counts": {"api_retry": 1},
            "worker_log_files": 4,
            "latest_log_age_seconds": 1.0,
            "active_worker_processes": 24,
            "orchestrator_processes": 6,
        },
    )

    health = collector.collect_once(plan_path)

    assert health["active_workers"] == 48
    assert len(health["nodes"]) == 2
    assert all(node["collected"] for node in health["nodes"])
    written = json.loads((run_dir / "slurm-health.json").read_text())
    assert written["expected_workers"] == 48
