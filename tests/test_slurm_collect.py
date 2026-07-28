import io
import json
import subprocess
import sys
import tarfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import slurm_collect as collector


def archive_bytes(name: str, content: bytes) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        info = tarfile.TarInfo(name)
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def remote_health_result(
    tmp_path: Path,
    text: str,
    health_window_seconds: int | None = None,
) -> dict[str, object]:
    log_dir = tmp_path / "orchestrator-logs-test"
    log_dir.mkdir()
    (log_dir / "worker-0.log").write_text(text)
    argv = [sys.executable, "-c", collector.REMOTE_HEALTH_CODE, str(tmp_path), "0"]
    if health_window_seconds is not None:
        argv.append(str(health_window_seconds))
    result = subprocess.run(
        argv,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def remote_health_counts(tmp_path: Path, text: str) -> dict[str, int]:
    return remote_health_result(tmp_path, text)["error_counts"]  # type: ignore[return-value]


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


def test_ssh_collection_can_read_a_suspended_node(tmp_path, monkeypatch) -> None:
    captured: list[str] = []

    def fake_run_bytes(argv, timeout):
        captured.extend(argv)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=archive_bytes("create.jsonl", b"{}\n"),
            stderr=b"",
        )

    monkeypatch.setattr(collector, "run_bytes", fake_run_bytes)

    collector.collect_archive(
        "node-a",
        "123",
        "SUSPENDED",
        "/remote/run",
        tmp_path / "collected",
        False,
        node_ip="1.2.3.4",
        transport="ssh",
    )

    assert captured[0] == "ssh"
    assert "alex@1.2.3.4" in captured
    assert "srun" not in captured
    assert (tmp_path / "collected" / "create.jsonl").read_text() == "{}\n"


def test_remote_health_ignores_task_and_test_text(tmp_path) -> None:
    counts = remote_health_counts(
        tmp_path,
        """\
2026-07-18T00:03:08+00:00 Failed to download VS Code: request timeout after 15000ms
2026-07-18T00:03:09+00:00 Error: Timed out waiting 10000ms from config.webServer.
2026-07-18T00:03:10+00:00 GREP pattern=(timeout|ECONNRESET|502 Bad Gateway)
2026-07-18T00:03:11+00:00 test expects API Error: 504 Gateway Timeout
ordinary task output says subtype='api_retry' and ETIMEDOUT
""",
    )

    assert all(value == 0 for value in counts.values())


def test_remote_health_counts_structured_network_diagnostics_once(tmp_path) -> None:
    counts = remote_health_counts(
        tmp_path,
        """\
2026-07-18T00:08:01+00:00 [System] SystemMessage(subtype='api_retry', data={'type': 'system', 'subtype': 'api_retry', 'error_status': 502})
2026-07-18T00:08:02+00:00 [Assistant] API Error: Unable to connect to API (ECONNRESET)
2026-07-18T00:08:03+00:00 [SDK] network request failed: ReadTimeout
2026-07-18T00:08:04+00:00 [Network] TLS failure: UNEXPECTED_EOF_WHILE_READING
2026-07-18T00:08:05+00:00 [HTTP] response: 504 Gateway Timeout
""",
    )

    assert counts["api_retry"] == 1
    assert counts["http_502"] == 1
    assert counts["econnreset"] == 1
    assert counts["request_timeout"] == 1
    assert counts["tls_eof"] == 1
    assert counts["http_504"] == 1


def test_remote_health_recent_window_covers_api_failures_and_deduplicates_lines(
    tmp_path,
) -> None:
    recent = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    stale = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    recent_lines = [
        f"{recent} [System] SystemMessage(subtype='api_retry', data={{'error_status': 502}})",
        f"{recent} [HTTP] HTTP 401 Unauthorized",
        f"{recent} [HTTP] response: 403 Forbidden",
        f"{recent} [Assistant] API Error: 429 Too Many Requests",
        f"{recent} [HTTP] status code: 500 Internal Server Error",
        f"{recent} [HTTP] response: 503 Service Unavailable",
        f"{recent} [HTTP] API Error: 504 Gateway Timeout",
        f"{recent} [SDK] APITimeoutError: upstream timed out",
        f"{recent} [Network] ConnectionResetError: connection reset by peer",
        f"{recent} [Network] TLS handshake failure: UNEXPECTED_EOF_WHILE_READING",
        f"{recent} [API] AuthenticationError: invalid API key",
        f"{recent} [API] credential pool exhausted",
        f"{recent} [API] insufficient_quota: credit balance is too low",
    ]
    result = remote_health_result(
        tmp_path,
        "\n".join(
            [
                *recent_lines,
                f"{stale} [HTTP] response: 500 Internal Server Error",
                f"{recent} test fixture expects HTTP 503 and AuthenticationError",
            ]
        ),
        health_window_seconds=60,
    )

    cumulative = result["error_counts"]
    recent_counts = result["recent_error_counts"]
    assert result["health_window_seconds"] == 60
    assert cumulative["http_500"] == 2
    assert recent_counts["http_500"] == 1
    for key in (
        "api_retry",
        "http_401",
        "http_403",
        "http_429",
        "http_500",
        "http_502",
        "http_503",
        "http_504",
        "request_timeout",
        "econnreset",
        "tls_eof",
        "tls_error",
        "authentication",
        "credential_exhaustion",
        "quota_exhaustion",
    ):
        assert recent_counts[key] >= 1
    assert result["recent_api_failure_events"] == len(recent_lines)


def test_remote_health_existing_two_argument_invocation_defaults_window(tmp_path) -> None:
    result = remote_health_result(tmp_path, "")

    assert result["health_window_seconds"] == 300.0
    assert result["recent_api_failure_events"] == 0
    assert all(value == 0 for value in result["recent_error_counts"].values())


def test_collect_health_passes_window_to_remote_code(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(collector, "srun_base", lambda *args: ["srun"])

    def fake_run_bytes(argv, timeout):
        captured["argv"] = argv
        captured["timeout"] = timeout
        return subprocess.CompletedProcess(argv, 0, stdout=b"{}", stderr=b"")

    monkeypatch.setattr(collector, "run_bytes", fake_run_bytes)

    collector.collect_health("node-a", "1", "RUNNING", "/run", None, 47)

    assert captured["argv"][-1] == "47"
    assert captured["timeout"] == 300


def test_atomic_json_allows_overlapping_writers(tmp_path, monkeypatch) -> None:
    destination = tmp_path / "health.json"
    destination.write_text("{}\n")
    destination.chmod(0o664)
    barrier = threading.Barrier(2)
    sources: list[Path] = []
    source_lock = threading.Lock()
    real_replace = collector.os.replace

    def overlapping_replace(source, target):
        with source_lock:
            sources.append(Path(source))
        barrier.wait(timeout=5)
        real_replace(source, target)

    monkeypatch.setattr(collector.os, "replace", overlapping_replace)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(collector.atomic_json, destination, {"writer": writer})
            for writer in (1, 2)
        ]
        for future in futures:
            future.result(timeout=5)

    assert len(set(sources)) == 2
    assert json.loads(destination.read_text()) in ({"writer": 1}, {"writer": 2})
    assert destination.stat().st_mode & 0o777 == 0o664
    assert not list(tmp_path.glob(".health.json.*.tmp"))


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
    windows: list[int] = []

    def fake_collect_health(*args, **kwargs):
        windows.append(args[-1])
        return {
            "error_counts": {"api_retry": 1},
            "recent_error_counts": {"api_retry": 1, "http_502": 1},
            "recent_api_failure_events": 1,
            "worker_log_files": 4,
            "latest_log_age_seconds": 1.0,
            "active_worker_processes": 24,
            "orchestrator_processes": 6,
        }

    monkeypatch.setattr(collector, "collect_health", fake_collect_health)

    health = collector.collect_once(plan_path, health_window_seconds=45)

    assert health["active_workers"] == 48
    assert health["health_window_seconds"] == 45
    assert health["recent_error_counts"] == {"api_retry": 2, "http_502": 2}
    assert health["recent_api_failure_events"] == 2
    assert windows == [45, 45]
    assert len(health["nodes"]) == 2
    assert all(node["collected"] for node in health["nodes"])
    written = json.loads((run_dir / "slurm-health.json").read_text())
    assert written["expected_workers"] == 48


def test_merge_plans_sums_workers_and_uses_primary_run_identity(tmp_path) -> None:
    primary_run = tmp_path / "controller-run"
    primary_run.mkdir()
    primary_path = primary_run / "slurm-plan.json"
    primary_path.write_text(
        json.dumps(
            {
                "run_name": "shared-run",
                "run_dir": str(primary_run),
                "expected_workers": 48,
                "nodes": [
                    {"node": "node-a", "expected_workers": 24},
                    {"node": "node-b", "expected_workers": 24},
                ],
            }
        )
    )
    secondary_dir = primary_run / ".idle"
    secondary_dir.mkdir()
    secondary_path = secondary_dir / "observation-plan.json"
    secondary_path.write_text(
        json.dumps(
            {
                "run_name": "shared-run",
                "run_dir": str(secondary_dir),
                "expected_workers": 24,
                "nodes": [
                    {"node": "node-c", "expected_workers": 12},
                    {"node": "node-d", "expected_workers": 12},
                ],
            }
        )
    )

    merged, selected_primary = collector.merge_plans([primary_path, secondary_path])

    assert selected_primary == primary_path.resolve()
    assert merged["run_name"] == "shared-run"
    assert merged["run_dir"] == str(primary_run.resolve())
    assert merged["expected_workers"] == 72
    assert [node["node"] for node in merged["nodes"]] == [
        "node-a",
        "node-b",
        "node-c",
        "node-d",
    ]


def test_merge_plans_falls_back_to_node_worker_totals(tmp_path) -> None:
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "run_name": "run",
                "nodes": [
                    {"node": "node-a", "expected_workers": 3},
                    {"node": "node-b", "expected_workers": 5},
                ],
            }
        )
    )

    merged, _ = collector.merge_plans([plan_path])

    assert merged["expected_workers"] == 8


def test_merge_plans_rejects_duplicate_nodes(tmp_path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(json.dumps({"run_name": "run", "nodes": [{"node": "same-node"}]}))
    second.write_text(json.dumps({"run_name": "run", "nodes": [{"node": "same-node"}]}))

    with pytest.raises(ValueError, match="duplicate Slurm node record"):
        collector.merge_plans([first, second])


def test_merge_plans_rejects_incompatible_run_names(tmp_path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(json.dumps({"run_name": "run-a", "nodes": [{"node": "node-a"}]}))
    second.write_text(json.dumps({"run_name": "run-b", "nodes": [{"node": "node-b"}]}))

    with pytest.raises(ValueError, match="incompatible Slurm plan run name"):
        collector.merge_plans([first, second])


def test_collect_merged_collects_all_nodes_into_primary_run_dir(tmp_path, monkeypatch) -> None:
    primary_run = tmp_path / "primary"
    primary_run.mkdir()
    primary = primary_run / "slurm-plan.json"
    primary.write_text(
        json.dumps(
            {
                "run_name": "run",
                "run_dir": str(primary_run),
                "expected_workers": 4,
                "nodes": [
                    {
                        "node": "node-a",
                        "job_id": "1",
                        "remote_run_dir": "/tmp/a",
                        "expected_workers": 4,
                    }
                ],
            }
        )
    )
    secondary_dir = tmp_path / "secondary"
    secondary_dir.mkdir()
    secondary = secondary_dir / "observation-plan.json"
    secondary.write_text(
        json.dumps(
            {
                "run_name": "run",
                "run_dir": str(secondary_dir),
                "expected_workers": 2,
                "nodes": [
                    {
                        "node": "node-b",
                        "job_id": "2",
                        "remote_run_dir": "/tmp/b",
                        "expected_workers": 2,
                    }
                ],
            }
        )
    )
    monkeypatch.setattr(collector, "job_state", lambda _job_id: "RUNNING")
    monkeypatch.setattr(collector, "collect_archive", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        collector,
        "collect_health",
        lambda *args, **kwargs: {
            "active_worker_processes": 3,
            "orchestrator_processes": 1,
        },
    )

    health = collector.collect_merged([primary, secondary])

    assert health["run_name"] == "run"
    assert health["expected_workers"] == 6
    assert health["active_workers"] == 6
    assert [node["node"] for node in health["nodes"]] == ["node-a", "node-b"]
    assert (primary_run / "slurm-health.json").is_file()
    assert not (secondary_dir / "slurm-health.json").exists()


def test_cli_accepts_repeatable_plan_arguments() -> None:
    args = collector.parse_args(
        [
            "--plan",
            "primary.json",
            "--plan",
            "secondary.json",
            "--watch",
            "--health-window-seconds",
            "90",
            "--transport",
            "ssh",
        ]
    )

    assert args.plan == [Path("primary.json"), Path("secondary.json")]
    assert args.watch is True
    assert args.health_window_seconds == 90
    assert args.transport == "ssh"
