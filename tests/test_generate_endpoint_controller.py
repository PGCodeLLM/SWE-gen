"""Unit tests for the dynamic generate-endpoint reconcile controller.

Covers the pure planner (create/scale/delete), the deployment-spec clone
(inline model env, dropped credential secret, endpoint label), the model env
contract, and the probe -> latch flow with fake k8s/DB/prober collaborators.
"""
from __future__ import annotations

from datetime import UTC, datetime

from swegen.pipeline.generate_endpoint_controller import (
    DEPLOYMENT_PREFIX,
    EndpointRow,
    ProbeResult,
    build_deployment_spec,
    deployment_name_for,
    model_env,
    plan_reconcile,
    reconcile_once,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)


def _row(slug, *, concurrency=2, enabled=True, breaker_open=False, consecutive_fail=0):
    return EndpointRow(
        slug=slug,
        base_url="http://1.95.77.23:3000",
        model_id="deepseek-v4-flash",
        auth_token="sk-secret",
        concurrency=concurrency,
        enabled=enabled,
        breaker_open=breaker_open,
        consecutive_fail=consecutive_fail,
    )


_TEMPLATE = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {"name": "swegen-generate", "namespace": "swegen-pipeline"},
    "spec": {
        "replicas": 228,
        "selector": {"matchLabels": {"app.kubernetes.io/name": "swegen-worker",
                                      "swegen.pgcode/stage": "generate"}},
        "template": {
            "metadata": {"labels": {"app.kubernetes.io/name": "swegen-worker",
                                    "swegen.pgcode/stage": "generate"}},
            "spec": {"containers": [{
                "name": "worker",
                "args": ["--stage", "generate"],
                "envFrom": [
                    {"configMapRef": {"name": "swegen-pipeline-config"}},
                    {"secretRef": {"name": "swegen-database"}},
                    {"secretRef": {"name": "swegen-runtime-proxy"}},
                    {"secretRef": {"name": "swegen-model-credentials-glm52-thinking-npu-20260804"}},
                ],
                "env": [{"name": "POD_NAME", "value": "x"},
                        {"name": "ANTHROPIC_MODEL", "value": "stale-model"}],
            }]},
        },
    },
}


# --- planner ----------------------------------------------------------------

def test_plan_creates_missing_scales_changed_deletes_orphans():
    endpoints = [
        _row("a", concurrency=4),          # exists at 2 -> scale to 4
        _row("b", concurrency=3),          # missing -> create at 3
        _row("c", concurrency=0),          # exists at 5 -> scale to 0
    ]
    existing = {"a": 2, "c": 5, "gone": 1}  # 'gone' not in registry -> delete
    plan = plan_reconcile(endpoints, existing)
    assert ("b", 3) in plan.apply
    assert ("a", 4) in plan.scale
    assert ("c", 0) in plan.scale
    assert plan.delete == ["gone"]


def test_plan_disabled_or_breaker_open_targets_zero():
    endpoints = [_row("a", breaker_open=True, concurrency=4),
                 _row("b", enabled=False, concurrency=4)]
    plan = plan_reconcile(endpoints, {"a": 4, "b": 4})
    assert ("a", 0) in plan.scale
    assert ("b", 0) in plan.scale


def test_plan_missing_and_zero_target_is_noop():
    plan = plan_reconcile([_row("a", breaker_open=True, concurrency=4)], {})
    assert plan.apply == [] and plan.scale == [] and plan.delete == []


# --- deployment spec clone --------------------------------------------------

def test_build_spec_names_labels_and_replicas():
    spec = build_deployment_spec(_row("myslug", concurrency=7), _TEMPLATE)
    assert spec["metadata"]["name"] == "swegen-generate-dyn-myslug"
    assert spec["spec"]["replicas"] == 7
    assert spec["spec"]["template"]["metadata"]["labels"]["swegen.pgcode/endpoint"] == "myslug"
    assert spec["spec"]["selector"]["matchLabels"]["swegen.pgcode/endpoint"] == "myslug"
    # stage label preserved (so it still counts as generate)
    assert spec["spec"]["template"]["metadata"]["labels"]["swegen.pgcode/stage"] == "generate"


def test_build_spec_drops_credential_secret_and_injects_inline_model_env():
    spec = build_deployment_spec(_row("s"), _TEMPLATE)
    container = spec["spec"]["template"]["spec"]["containers"][0]
    secret_names = [e.get("secretRef", {}).get("name") for e in container["envFrom"]]
    assert not any(str(n).startswith("swegen-model-credentials-") for n in secret_names)
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["ANTHROPIC_BASE_URL"] == "http://1.95.77.23:3000"
    assert env["ANTHROPIC_MODEL"] == "deepseek-v4-flash"  # stale one replaced
    assert env["ANTHROPIC_AUTH_TOKEN"] == "sk-secret"
    assert env["OPENAI_BASE_URL"] == "http://1.95.77.23:3000/v1"
    # original non-model env preserved
    assert env["POD_NAME"] == "x"
    # template not mutated (deepcopy)
    assert _TEMPLATE["metadata"]["name"] == "swegen-generate"


def test_model_env_strips_and_adds_v1_correctly():
    row = EndpointRow(slug="s", base_url="http://h:3000/v1", model_id="m",
                      auth_token="t", concurrency=1, enabled=True,
                      breaker_open=False, consecutive_fail=0)
    env = {e["name"]: e["value"] for e in model_env(row)}
    assert env["ANTHROPIC_BASE_URL"] == "http://h:3000"    # /v1 stripped for anthropic
    assert env["OPENAI_BASE_URL"] == "http://h:3000/v1"    # kept for openai


def test_deployment_name_prefix():
    assert deployment_name_for("x").startswith(DEPLOYMENT_PREFIX)


# --- fakes for reconcile ----------------------------------------------------

class _FakeCursor:
    def __init__(self, rows=None, rowcount=1):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class _FakeConn:
    """Minimal connection: returns registry rows for the SELECT, records writes."""

    def __init__(self, rows):
        self._rows = rows
        self.writes = []

    def execute(self, sql, params=()):
        if "FROM generate_endpoints" in sql and "SELECT" in sql:
            return _FakeCursor(rows=list(self._rows))
        self.writes.append((sql.strip().split()[0], params))
        return _FakeCursor(rowcount=1)

    def transaction(self):
        conn = self

        class _Txn:
            def __enter__(self_):
                return conn

            def __exit__(self_, *a):
                return False

        return _Txn()


class _FakeManager:
    def __init__(self, existing):
        self.existing = dict(existing)
        self.calls = []

    def list_dynamic(self):
        return dict(self.existing)

    def apply(self, slug, spec, replicas):
        self.calls.append(("apply", slug, replicas))
        self.existing[slug] = replicas

    def scale(self, slug, replicas):
        self.calls.append(("scale", slug, replicas))
        self.existing[slug] = replicas

    def delete(self, slug):
        self.calls.append(("delete", slug))
        self.existing.pop(slug, None)

    def sweep_pods(self, slug):
        self.calls.append(("sweep", slug))
        return 0


class _FakeProber:
    def __init__(self, result):
        self.result = result
        self.probes = 0

    def probe(self, base_url, model_id, token):
        self.probes += 1
        return self.result


def _tuple_rows(rows):
    # emulate DB tuple rows (slug, base_url, model_id, token, concurrency,
    # enabled, breaker_open, consecutive_fail)
    return [
        (r.slug, r.base_url, r.model_id, r.auth_token, r.concurrency,
         r.enabled, r.breaker_open, r.consecutive_fail)
        for r in rows
    ]


def test_reconcile_healthy_endpoint_creates_deployment():
    rows = _tuple_rows([_row("a", concurrency=2)])
    conn = _FakeConn(rows)
    manager = _FakeManager(existing={})
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))
    reconcile_once(conn, manager, prober, _TEMPLATE, now=lambda: NOW)
    assert ("apply", "a", 2) in manager.calls
    assert prober.probes == 1
    # a healthy probe records consecutive_fail=0
    assert any(w[0] == "UPDATE" for w in conn.writes)


def test_reconcile_trips_after_threshold_and_scales_zero():
    # endpoint already at 2 consecutive fails; one more unhealthy probe trips it.
    rows = _tuple_rows([_row("bad", concurrency=2, consecutive_fail=2)])
    conn = _FakeConn(rows)
    manager = _FakeManager(existing={"bad": 2})
    prober = _FakeProber(ProbeResult(ok=False, status=504, detail="http 504"))
    reconcile_once(conn, manager, prober, _TEMPLATE,
                   consecutive_trip_threshold=3, now=lambda: NOW)
    # trip UPDATE + event INSERT recorded
    verbs = [w[0] for w in conn.writes]
    assert "UPDATE" in verbs and "INSERT" in verbs


def test_reconcile_below_threshold_does_not_trip():
    rows = _tuple_rows([_row("bad", concurrency=2, consecutive_fail=0)])
    conn = _FakeConn(rows)
    manager = _FakeManager(existing={"bad": 2})
    prober = _FakeProber(ProbeResult(ok=False, status=504, detail="http 504"))
    reconcile_once(conn, manager, prober, _TEMPLATE,
                   consecutive_trip_threshold=3, now=lambda: NOW)
    # only a probe record UPDATE, no INSERT (no trip event)
    assert all(w[0] != "INSERT" for w in conn.writes)


def test_reconcile_breaker_open_endpoint_is_scaled_to_zero_not_probed():
    rows = _tuple_rows([_row("dead", concurrency=4, breaker_open=True)])
    conn = _FakeConn(rows)
    manager = _FakeManager(existing={"dead": 4})
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))
    reconcile_once(conn, manager, prober, _TEMPLATE, now=lambda: NOW)
    assert prober.probes == 0                       # open endpoints are not probed
    assert ("scale", "dead", 0) in manager.calls    # driven to zero
