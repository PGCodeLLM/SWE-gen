import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor
from types import SimpleNamespace

import proxy_stability_monitor as monitor


def test_proxy_error_pattern_is_specific_to_proxy_transport() -> None:
    assert monitor.PROXY_LOG_ERROR_RE.search("proxy CONNECT returned 502 Bad Gateway")
    assert monitor.PROXY_LOG_ERROR_RE.search("unexpected server response: 400")
    assert monitor.PROXY_LOG_ERROR_RE.search(
        "curl: (56) CONNECT tunnel failed, response 502"
    )
    assert not monitor.PROXY_LOG_ERROR_RE.search("application endpoint returned HTTP 400")


def test_curl_proxy_http_status_extracts_connect_response() -> None:
    assert monitor.curl_proxy_http_status("CONNECT tunnel failed, response 502") == 502
    assert monitor.curl_proxy_http_status("CONNECT tunnel failed, response 400") == 400
    assert monitor.curl_proxy_http_status("SSL_ERROR_SYSCALL") is None


def test_probe_bucket_distinguishes_trigger_and_transport_failures() -> None:
    def result(returncode: int, status: int) -> monitor.ProbeResult:
        return monitor.ProbeResult("route", "socks", "proxy", returncode, status, 1.0, "")

    assert monitor.probe_bucket(result(0, 401)) == "expected_http"
    assert monitor.probe_bucket(result(0, 400)) == "http_400"
    assert monitor.probe_bucket(result(0, 502)) == "http_502"
    assert monitor.probe_bucket(result(35, 0)) == "transport_failure"
    assert monitor.probe_bucket(result(0, 503)) == "other_http"


def test_parse_args_has_independent_probe_interval(tmp_path, monkeypatch) -> None:
    required = [
        "proxy_stability_monitor.py",
        "--run-dir",
        str(tmp_path / "run"),
        "--worker-log-dir",
        str(tmp_path / "logs"),
    ]
    monkeypatch.setattr(sys, "argv", required)

    defaults = monitor.parse_args()

    assert defaults.interval == 5.0
    assert defaults.probe_interval == 60.0
    assert defaults.initial_mode == "normal"

    monkeypatch.setattr(
        sys,
        "argv",
        [*required, "--probe-interval", "30", "--initial-mode", "debug_4x4"],
    )

    overridden = monitor.parse_args()

    assert overridden.interval == 5.0
    assert overridden.probe_interval == 30.0
    assert overridden.initial_mode == "debug_4x4"


def test_pending_probe_batch_is_nonblocking() -> None:
    class PendingFuture:
        def done(self) -> bool:
            return False

        def result(self) -> monitor.ProbeResult:
            raise AssertionError("result() must not be called for a pending probe")

    route = monitor.Route("route", "socks", "proxy")
    pending = [(route, PendingFuture())]

    assert monitor.take_completed_probe_batch(pending) is None  # type: ignore[arg-type]


def test_submit_probe_batch_covers_all_routes(monkeypatch) -> None:
    def fake_probe(
        route: monitor.Route, _args: SimpleNamespace
    ) -> monitor.ProbeResult:
        return monitor.ProbeResult(
            route.label,
            route.kind,
            route.url,
            0,
            401,
            0.1,
            "",
        )

    monkeypatch.setattr(monitor, "probe", fake_probe)
    routes = monitor.default_routes()
    with ThreadPoolExecutor(max_workers=4) as executor:
        pending = monitor.submit_probe_batch(executor, routes, SimpleNamespace())
        for _, future in pending:
            future.result(timeout=1)
        results = monitor.take_completed_probe_batch(pending)

    assert results is not None
    assert {result.label for result in results} == {
        "socks87",
        "bridge87",
        "socks108",
        "bridge108",
    }


def test_take_completed_probe_batch_converts_unexpected_exception() -> None:
    route = monitor.Route("route", "socks", "proxy")
    failed: Future[monitor.ProbeResult] = Future()
    failed.set_exception(RuntimeError("boom"))

    results = monitor.take_completed_probe_batch([(route, failed)])

    assert results is not None
    assert results[0].returncode == -1
    assert results[0].http_code == 0
    assert results[0].error == "RuntimeError: boom"


def test_record_probe_batch_updates_state_once() -> None:
    state = {
        "stable": False,
        "probe_cycles": 0,
        "consecutive_unstable_cycles": 0,
        "probe_counts": {
            route.label: {
                "expected_http": 0,
                "http_400": 0,
                "http_502": 0,
                "transport_failure": 0,
                "other_http": 0,
            }
            for route in monitor.default_routes()
        },
    }
    probes = [
        monitor.ProbeResult(route.label, route.kind, route.url, 0, 401, 0.1, "")
        for route in monitor.default_routes()
    ]

    monitor.record_probe_batch(state, probes)

    assert state["stable"] is True
    assert state["probe_cycles"] == 1
    assert state["consecutive_unstable_cycles"] == 0
    assert all(
        counts["expected_http"] == 1 for counts in state["probe_counts"].values()
    )


def test_main_keeps_scanning_logs_while_probe_batch_is_pending(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "run"
    worker_log_dir = tmp_path / "logs"
    run_dir.mkdir()
    worker_log_dir.mkdir()
    args = SimpleNamespace(
        run_dir=run_dir,
        worker_log_dir=worker_log_dir,
        run_script=tmp_path / "run_orchestrator.sh",
        trigger_status=None,
        interval=0.002,
        probe_interval=60.0,
        connect_timeout=1.0,
        max_time=1.0,
        grace=0.0,
        debug_workers=8,
        debug_workers_per_endpoint=4,
        pid_file=None,
        state_file=None,
        once=True,
        dry_run=True,
        endpoint="https://example.invalid/v1/models",
        initial_mode="normal",
    )
    scan_calls = 0

    def slow_probe(
        route: monitor.Route, _args: SimpleNamespace
    ) -> monitor.ProbeResult:
        time.sleep(0.03)
        return monitor.ProbeResult(
            route.label,
            route.kind,
            route.url,
            0,
            401,
            0.03,
            "",
        )

    def scan_without_errors(_paths, _offsets) -> list[dict[str, str]]:
        nonlocal scan_calls
        scan_calls += 1
        return []

    monkeypatch.setattr(monitor, "parse_args", lambda: args)
    monkeypatch.setattr(monitor, "probe", slow_probe)
    monkeypatch.setattr(monitor, "monitored_logs", lambda _path: [])
    monkeypatch.setattr(monitor, "scan_new_log_errors", scan_without_errors)
    monkeypatch.setattr(monitor, "write_state", lambda _path, _state: None)
    monkeypatch.setattr(monitor, "emit", lambda _event, **_fields: None)
    monkeypatch.setattr(monitor.signal, "signal", lambda _signal, _handler: None)

    assert monitor.main() == 0
    assert scan_calls >= 3


def test_log_502_triggers_fallback_while_probe_batch_is_pending(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "run"
    worker_log_dir = tmp_path / "logs"
    run_dir.mkdir()
    worker_log_dir.mkdir()
    args = SimpleNamespace(
        run_dir=run_dir,
        worker_log_dir=worker_log_dir,
        run_script=tmp_path / "run_orchestrator.sh",
        trigger_status=None,
        interval=0.002,
        probe_interval=60.0,
        connect_timeout=1.0,
        max_time=1.0,
        grace=0.0,
        debug_workers=8,
        debug_workers_per_endpoint=4,
        pid_file=None,
        state_file=None,
        once=True,
        dry_run=True,
        endpoint="https://example.invalid/v1/models",
        initial_mode="normal",
    )
    scan_calls = 0
    stop_calls = 0
    launch_calls = 0
    events: list[tuple[str, dict]] = []

    def slow_probe(
        route: monitor.Route, _args: SimpleNamespace
    ) -> monitor.ProbeResult:
        time.sleep(0.03)
        return monitor.ProbeResult(
            route.label,
            route.kind,
            route.url,
            0,
            401,
            0.03,
            "",
        )

    def scan_with_one_502(_paths, _offsets) -> list[dict[str, str]]:
        nonlocal scan_calls
        scan_calls += 1
        if scan_calls == 2:
            return [
                {
                    "file": "worker-0.log",
                    "line": "proxy CONNECT returned 502 Bad Gateway",
                }
            ]
        return []

    def fake_stop_run(_run_dir, _grace, _dry_run) -> dict:
        nonlocal stop_calls
        stop_calls += 1
        return {"orchestrator_pid": 123, "worker_groups": [], "already_stopped": False}

    def fake_launch(_args, _stamp) -> dict:
        nonlocal launch_calls
        launch_calls += 1
        return {
            "launcher_pid": None,
            "orchestrator_pid": None,
            "worker_log_dir": str(tmp_path / "debug-logs"),
            "launch_log": str(tmp_path / "debug-launch.log"),
            "dry_run": True,
        }

    monkeypatch.setattr(monitor, "parse_args", lambda: args)
    monkeypatch.setattr(monitor, "probe", slow_probe)
    monkeypatch.setattr(monitor, "monitored_logs", lambda _path: [])
    monkeypatch.setattr(monitor, "scan_new_log_errors", scan_with_one_502)
    monkeypatch.setattr(monitor, "write_state", lambda _path, _state: None)
    monkeypatch.setattr(
        monitor, "emit", lambda event, **fields: events.append((event, fields))
    )
    monkeypatch.setattr(monitor.signal, "signal", lambda _signal, _handler: None)
    monkeypatch.setattr(
        monitor, "capture_diagnostics", lambda *_args: tmp_path / "diagnostic.log"
    )
    monkeypatch.setattr(monitor, "stop_run", fake_stop_run)
    monkeypatch.setattr(monitor, "stop_run_containers", lambda *_args: [])
    monkeypatch.setattr(monitor, "launch_debug_run", fake_launch)

    assert monitor.main() == 0
    assert stop_calls == 1
    assert launch_calls == 1
    assert [event for event, _ in events].count("triggered") == 1
    assert [event for event, _ in events].count("fallback_started") == 1
    trigger = next(fields for event, fields in events if event == "triggered")
    assert trigger["reason"]["type"] == "proxy_log_status"


def test_second_log_502_stops_debug_run_without_relaunch(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    worker_log_dir = tmp_path / "debug-logs"
    run_dir.mkdir()
    worker_log_dir.mkdir()
    args = SimpleNamespace(
        run_dir=run_dir,
        worker_log_dir=worker_log_dir,
        run_script=tmp_path / "run_orchestrator.sh",
        trigger_status=None,
        interval=0.002,
        probe_interval=0.005,
        connect_timeout=1.0,
        max_time=1.0,
        grace=0.0,
        debug_workers=8,
        debug_workers_per_endpoint=4,
        pid_file=None,
        state_file=None,
        once=True,
        dry_run=True,
        endpoint="https://example.invalid/v1/models",
        initial_mode="debug_4x4",
    )
    scan_calls = 0
    stop_calls = 0
    events: list[tuple[str, dict]] = []

    def slow_probe(
        route: monitor.Route, _args: SimpleNamespace
    ) -> monitor.ProbeResult:
        time.sleep(0.03)
        return monitor.ProbeResult(
            route.label,
            route.kind,
            route.url,
            0,
            401,
            0.03,
            "",
        )

    def scan_with_one_502(_paths, _offsets) -> list[dict[str, str]]:
        nonlocal scan_calls
        scan_calls += 1
        if scan_calls == 2:
            return [
                {
                    "file": "worker-0.log",
                    "line": "curl: (56) CONNECT tunnel failed, response 502",
                }
            ]
        return []

    def fake_stop_run(_run_dir, _grace, _dry_run) -> dict:
        nonlocal stop_calls
        stop_calls += 1
        return {"orchestrator_pid": 123, "worker_groups": [], "already_stopped": False}

    monkeypatch.setattr(monitor, "parse_args", lambda: args)
    monkeypatch.setattr(monitor, "probe", slow_probe)
    monkeypatch.setattr(monitor, "monitored_logs", lambda _path: [])
    monkeypatch.setattr(monitor, "scan_new_log_errors", scan_with_one_502)
    monkeypatch.setattr(monitor, "write_state", lambda _path, _state: None)
    monkeypatch.setattr(
        monitor, "emit", lambda event, **fields: events.append((event, fields))
    )
    monkeypatch.setattr(monitor.signal, "signal", lambda _signal, _handler: None)
    monkeypatch.setattr(
        monitor, "capture_diagnostics", lambda *_args: tmp_path / "diagnostic.log"
    )
    monkeypatch.setattr(monitor, "stop_run", fake_stop_run)
    monkeypatch.setattr(monitor, "stop_run_containers", lambda *_args: [])
    monkeypatch.setattr(
        monitor,
        "launch_debug_run",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("debug mode must not launch another fallback")
        ),
    )

    assert monitor.main() == 0
    assert stop_calls == 1
    assert [event for event, _ in events].count("triggered") == 1
    assert [event for event, _ in events].count("debug_stopped") == 1
    assert "fallback_started" not in [event for event, _ in events]


def test_stop_run_dry_run_finds_only_direct_swegen_groups(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "example-run"
    run_dir.mkdir()
    monkeypatch.setattr(monitor, "orchestrator_pid", lambda _name, _run_dir: 100)
    monkeypatch.setattr(
        monitor,
        "proc_rows",
        lambda: [
            {"pid": 101, "ppid": 100, "pgrp": 201, "cmd": "swegen create --repo a/b"},
            {"pid": 102, "ppid": 100, "pgrp": 202, "cmd": "unrelated child"},
            {"pid": 103, "ppid": 999, "pgrp": 203, "cmd": "swegen create --repo c/d"},
        ],
    )

    result = monitor.stop_run(run_dir, grace=0, dry_run=True)

    assert result == {
        "orchestrator_pid": 100,
        "worker_groups": [201],
        "already_stopped": False,
    }


def test_debug_dry_run_uses_four_by_four_paths(tmp_path) -> None:
    run_dir = tmp_path / "run-name"
    args = SimpleNamespace(run_dir=run_dir, dry_run=True)

    result = monitor.launch_debug_run(args, "20260717T010203Z")

    assert result["launcher_pid"] is None
    assert result["orchestrator_pid"] is None
    assert result["worker_log_dir"].endswith(
        "orchestrator-logs-debug-4x4-20260717T010203Z"
    )
    assert result["launch_log"].endswith(
        "orchestrator-debug-4x4-20260717T010203Z-launch.log"
    )
