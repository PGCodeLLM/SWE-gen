"""Unit tests for the dynamic generate-endpoint reconcile controller.

Covers the pure planner (create/scale/delete), the deployment-spec clone
(inline model env, dropped credential secret, endpoint label), the model env
contract, and the probe -> latch flow with fake k8s/DB/prober collaborators.
"""
from __future__ import annotations

import os
import sys
from dataclasses import replace
from datetime import UTC, datetime

from swegen.pipeline.generate_endpoint_controller import (
    DEPLOYMENT_PREFIX,
    ENDPOINT_SPEC_DIGEST_ANNOTATION,
    DeploymentState,
    EndpointRow,
    ProbeResult,
    build_deployment_spec,
    deployment_name_for,
    model_env,
    plan_reconcile,
    reconcile_once,
    spec_digest,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)


def _digest_of(spec):
    """Digest a rendered spec the way the controller stamps it."""

    return spec["metadata"]["annotations"][ENDPOINT_SPEC_DIGEST_ANNOTATION]


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
    # Literal env only: the container also carries valueFrom entries for the
    # non-credential keys carried over from the dropped credential Secret.
    env = {e["name"]: e["value"] for e in container["env"] if "value" in e}
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
    """Fake k8s manager tracking both replica count and spec digest per slug.

    ``existing`` accepts either a bare replica count (digest unknown, i.e. a
    Deployment predating drift detection) or a ``DeploymentState``.
    """

    def __init__(self, existing):
        self.existing = {
            slug: state if isinstance(state, DeploymentState) else DeploymentState(state)
            for slug, state in dict(existing).items()
        }
        self.calls = []
        self.specs = {}

    def list_dynamic(self):
        return dict(self.existing)

    def apply(self, slug, spec, replicas):
        self.calls.append(("apply", slug, replicas))
        self.specs[slug] = spec
        self.existing[slug] = DeploymentState(replicas, _digest_of(spec))

    def patch(self, slug, spec, replicas):
        self.calls.append(("patch", slug, replicas))
        self.specs[slug] = spec
        self.existing[slug] = DeploymentState(replicas, _digest_of(spec))

    def scale(self, slug, replicas):
        self.calls.append(("scale", slug, replicas))
        self.existing[slug] = DeploymentState(replicas, self.existing[slug].digest)

    def delete(self, slug):
        self.calls.append(("delete", slug))
        self.existing.pop(slug, None)

    def converged(self, slug):
        """Digest the manager would hold once this slug's spec is applied."""

        return self.existing[slug].digest

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
    """A breaker-open pool that is otherwise converged is scaled down, not rolled."""

    row = _row("dead", concurrency=4, breaker_open=True)
    conn = _FakeConn(_tuple_rows([row]))
    # Already carries the digest of its rendered spec => no template drift, so the
    # only thing left to reconcile is the replica count.
    converged = _digest_of(build_deployment_spec(row, _TEMPLATE))
    manager = _FakeManager(existing={"dead": DeploymentState(4, converged)})
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))
    reconcile_once(conn, manager, prober, _TEMPLATE, now=lambda: NOW)
    assert prober.probes == 0                       # open endpoints are not probed
    assert ("scale", "dead", 0) in manager.calls    # driven to zero
    assert not any(c[0] == "patch" for c in manager.calls)
    assert ("sweep", "dead") in manager.calls       # pods force-deleted, not left to grace


# --- credential drift: digest, patch, idempotence ----------------------------
#
# The bug these pin: apply() returned after scale() when the Deployment already
# existed, and plan_reconcile only emitted apply for an ABSENT slug. So changing
# base_url/model_id/auth_token in the registry never reached running pods, while
# the reconcile probe validated the DB row's credentials -- a green breaker over
# pods running dead credentials.


def _converged_manager(rows, replicas_by_slug, template=_TEMPLATE):
    """Manager whose live state already matches the rendered spec for each row."""

    existing = {
        row.slug: DeploymentState(
            replicas_by_slug[row.slug],
            _digest_of(build_deployment_spec(row, template)),
        )
        for row in rows
    }
    return _FakeManager(existing=existing)


def test_digest_changes_when_any_credential_changes():
    """model_id, auth_token and base_url each move the digest."""

    base = _row("s")
    baseline = _digest_of(build_deployment_spec(base, _TEMPLATE))

    for field, value in (
        ("model_id", "some-other-model"),
        ("auth_token", "sk-rotated"),
        ("base_url", "http://7.244.3.251:8088"),
    ):
        changed = replace(base, **{field: value})
        digest = _digest_of(build_deployment_spec(changed, _TEMPLATE))
        assert digest != baseline, f"{field} change must move the digest"


def test_digest_is_stable_for_an_unchanged_row():
    """Re-rendering the same row twice yields the same digest (no spurious roll)."""

    row = _row("s")
    first = _digest_of(build_deployment_spec(row, _TEMPLATE))
    second = _digest_of(build_deployment_spec(row, _TEMPLATE))
    assert first == second


def test_digest_ignores_replica_count():
    """Scaling must not be mistaken for template drift.

    Folding replicas into the digest would turn every ordinary scale-up into a
    full template patch, rolling every pod in the pool.
    """

    a = _digest_of(build_deployment_spec(_row("s", concurrency=2), _TEMPLATE))
    b = _digest_of(build_deployment_spec(_row("s", concurrency=40), _TEMPLATE))
    assert a == b


def test_build_spec_stamps_digest_annotation_on_create():
    spec = build_deployment_spec(_row("s"), _TEMPLATE)
    annotation = spec["metadata"]["annotations"][ENDPOINT_SPEC_DIGEST_ANNOTATION]
    assert annotation == spec_digest(spec)
    assert len(annotation) == 64  # sha256 hex


def test_changed_credential_produces_patch_not_scale():
    """The core regression: a rotated token rolls the pool onto a new template."""

    old = _row("a", concurrency=2)
    manager = _converged_manager([old], {"a": 2})
    # The registry now holds a different token at the SAME concurrency, so there
    # is nothing for the replica-comparison path to notice.
    new = replace(old, auth_token="sk-rotated")
    conn = _FakeConn(_tuple_rows([new]))
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))

    plan = reconcile_once(conn, manager, prober, _TEMPLATE, now=lambda: NOW)

    assert ("a", 2) in plan.patch
    assert plan.scale == []
    assert ("patch", "a", 2) in manager.calls
    # The patched spec carries the NEW token, and the stored digest was updated.
    env = {e["name"]: e["value"] for e in
           manager.specs["a"]["spec"]["template"]["spec"]["containers"][0]["env"]
           if "value" in e}
    assert env["ANTHROPIC_AUTH_TOKEN"] == "sk-rotated"
    assert manager.converged("a") == _digest_of(manager.specs["a"])


def test_changed_model_id_patches_and_updates_annotation():
    old = _row("a", concurrency=3)
    manager = _converged_manager([old], {"a": 3})
    before = manager.converged("a")
    new = replace(old, model_id="glm-5.2-thinking-npu")
    conn = _FakeConn(_tuple_rows([new]))

    reconcile_once(conn, manager, _FakeProber(ProbeResult(ok=True, status=200,
                                                          detail="ok")),
                   _TEMPLATE, now=lambda: NOW)

    assert ("patch", "a", 3) in manager.calls
    assert manager.converged("a") != before          # annotation moved
    spec = manager.specs["a"]
    assert spec["metadata"]["annotations"][ENDPOINT_SPEC_DIGEST_ANNOTATION] == \
        manager.converged("a")
    env = {e["name"]: e["value"] for e in
           spec["spec"]["template"]["spec"]["containers"][0]["env"] if "value" in e}
    assert env["ANTHROPIC_MODEL"] == "glm-5.2-thinking-npu"


def test_unchanged_row_produces_no_patch_and_no_scale():
    """Idempotence. A spurious patch here rolls every generate pod in the pool."""

    row = _row("a", concurrency=2)
    manager = _converged_manager([row], {"a": 2})
    conn = _FakeConn(_tuple_rows([row]))
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))

    plan = reconcile_once(conn, manager, prober, _TEMPLATE, now=lambda: NOW)

    assert plan.patch == [] and plan.scale == [] and plan.apply == []
    assert not any(c[0] in {"patch", "apply", "scale"} for c in manager.calls)


def test_repeated_reconcile_is_stable():
    """Three cycles over an unchanged registry issue no writes after convergence."""

    row = _row("a", concurrency=2)
    manager = _converged_manager([row], {"a": 2})
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))
    for _ in range(3):
        reconcile_once(_FakeConn(_tuple_rows([row])), manager, prober,
                       _TEMPLATE, now=lambda: NOW)
    assert manager.calls == []


def test_unchanged_row_at_wrong_replicas_scales_without_patching():
    """Preserved behaviour: a pure concurrency change is still a cheap scale."""

    row = _row("a", concurrency=6)
    manager = _converged_manager([row], {"a": 2})   # live at 2, wants 6
    conn = _FakeConn(_tuple_rows([row]))
    plan = reconcile_once(conn, manager,
                          _FakeProber(ProbeResult(ok=True, status=200, detail="ok")),
                          _TEMPLATE, now=lambda: NOW)
    assert ("a", 6) in plan.scale
    assert plan.patch == []
    assert ("scale", "a", 6) in manager.calls


def test_deployment_without_digest_annotation_is_adopted_once():
    """A pool created before drift detection gets stamped, then goes quiet."""

    row = _row("a", concurrency=2)
    manager = _FakeManager(existing={"a": DeploymentState(2, None)})
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))

    first = reconcile_once(_FakeConn(_tuple_rows([row])), manager, prober,
                           _TEMPLATE, now=lambda: NOW)
    assert ("a", 2) in first.patch                   # adopted + stamped

    second = reconcile_once(_FakeConn(_tuple_rows([row])), manager, prober,
                            _TEMPLATE, now=lambda: NOW)
    assert second.patch == [] and second.scale == []  # and never again


def test_plan_reconcile_without_digests_keeps_legacy_behaviour():
    """Callers that pass no digests get exactly the old create/scale/delete plan."""

    endpoints = [_row("a", concurrency=4), _row("b", concurrency=3),
                 _row("c", concurrency=0)]
    plan = plan_reconcile(endpoints, {"a": 2, "c": 5, "gone": 1})
    assert ("b", 3) in plan.apply
    assert ("a", 4) in plan.scale and ("c", 0) in plan.scale
    assert plan.delete == ["gone"]
    assert plan.patch == []


# --- context-cap env keys survive into the dynamic pool ----------------------


def test_context_cap_env_keys_survive_the_dropped_credential_secret():
    """CLAUDE_CODE_MAX_CONTEXT_TOKENS / AUTO_COMPACT_WINDOW must reach dyn pods.

    They live in the same model-credential Secret the clone drops from envFrom
    (create-secrets.sh writes them there), so re-adding only the 12 credential
    keys silently ran every dynamic pool without a context cap.
    """

    spec = build_deployment_spec(_row("s"), _TEMPLATE)
    container = spec["spec"]["template"]["spec"]["containers"][0]
    by_name = {e["name"]: e for e in container["env"]}

    for key in ("CLAUDE_CODE_MAX_CONTEXT_TOKENS", "CLAUDE_CODE_AUTO_COMPACT_WINDOW"):
        assert key in by_name, f"{key} was dropped with the credential secret"
        ref = by_name[key]["valueFrom"]["secretKeyRef"]
        # Sourced from the very Secret the template had mounted...
        assert ref["name"] == "swegen-model-credentials-glm52-thinking-npu-20260804"
        assert ref["key"] == key
        # ...and optional, so a rotated/renamed Secret cannot wedge the pool.
        assert ref["optional"] is True

    # The credential keys themselves are still inline (row is authoritative).
    assert by_name["ANTHROPIC_AUTH_TOKEN"]["value"] == "sk-secret"
    # ...and the Secret is still NOT mounted wholesale, so one endpoint can never
    # inherit another endpoint's token via envFrom.
    secret_names = [e.get("secretRef", {}).get("name") for e in container["envFrom"]]
    assert not any(str(n).startswith("swegen-model-credentials-") for n in secret_names)


def test_no_carried_env_when_template_has_no_credential_secret():
    """A template without a credential Secret gains no dangling secretKeyRefs."""

    import copy

    template = copy.deepcopy(_TEMPLATE)
    container = template["spec"]["template"]["spec"]["containers"][0]
    container["envFrom"] = [e for e in container["envFrom"]
                            if not str(e.get("secretRef", {}).get("name", ""))
                            .startswith("swegen-model-credentials-")]

    spec = build_deployment_spec(_row("s"), template)
    names = {e["name"] for e in
             spec["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert "CLAUDE_CODE_MAX_CONTEXT_TOKENS" not in names


# --- the template is re-read every cycle -------------------------------------


def test_template_roll_reaches_dynamic_pools_without_controller_restart():
    """A new image on the static template rolls the dyn pool on the next cycle.

    fetch_template() used to run once at startup, so an image/configmap/envFrom
    roll of swegen-generate never reached dyn pools until the controller pod
    happened to restart.
    """

    import copy

    row = _row("a", concurrency=2)
    manager = _converged_manager([row], {"a": 2})
    prober = _FakeProber(ProbeResult(ok=True, status=200, detail="ok"))

    # Same template => quiet.
    assert reconcile_once(_FakeConn(_tuple_rows([row])), manager, prober,
                          _TEMPLATE, now=lambda: NOW).patch == []

    # Static pool rolls to a new image; the controller re-reads the template.
    rolled = copy.deepcopy(_TEMPLATE)
    rolled["spec"]["template"]["spec"]["containers"][0]["image"] = "swegen-worker:e2e"

    plan = reconcile_once(_FakeConn(_tuple_rows([row])), manager, prober,
                          rolled, now=lambda: NOW)
    assert ("a", 2) in plan.patch
    assert manager.specs["a"]["spec"]["template"]["spec"]["containers"][0]["image"] \
        == "swegen-worker:e2e"


def test_main_reads_template_every_cycle(monkeypatch):
    """main() must call fetch_template() per cycle, not once at startup."""

    from swegen.pipeline import generate_endpoint_controller as ctrl

    calls = {"fetch": 0, "reconcile": 0}

    class _Manager:
        def fetch_template(self):
            calls["fetch"] += 1
            return _TEMPLATE

    class _Pool:
        def connection(self):
            class _Ctx:
                def __enter__(self_):
                    return _FakeConn([])

                def __exit__(self_, *a):
                    return False

            return _Ctx()

    def _reconcile(*args, **kwargs):
        calls["reconcile"] += 1
        if calls["reconcile"] >= 3:
            raise KeyboardInterrupt
        return ctrl.ReconcilePlan()

    monkeypatch.setattr(ctrl, "KubernetesDeploymentManager", lambda **kw: _Manager())
    monkeypatch.setattr(ctrl, "HttpEndpointProber", lambda **kw: None)
    monkeypatch.setattr(ctrl.db, "get_pool", lambda: _Pool())
    monkeypatch.setattr(ctrl.db, "close_pool", lambda: None)
    monkeypatch.setattr(ctrl, "reconcile_once", _reconcile)
    monkeypatch.setattr(ctrl.time, "sleep", lambda _s: None)
    monkeypatch.setattr(ctrl.signal, "signal", lambda *a: None)
    monkeypatch.setattr(sys, "argv", ["controller"])

    try:
        ctrl.main()
    except KeyboardInterrupt:
        pass

    # One template read per reconcile cycle, not one for the process lifetime.
    assert calls["fetch"] == calls["reconcile"] >= 3


# --------------------------------------------------------------------------- #
# HttpEndpointProber: captured error_text is full, copyable, and token-redacted
# --------------------------------------------------------------------------- #


class _FakeHTTPError(Exception):
    """Stand-in for urllib.error.HTTPError with a readable body."""

    def __init__(self, code, body):
        self.code = code
        self._body = body.encode("utf-8")

    def read(self):
        return self._body


def _prober_with_opener(opener):
    from swegen.pipeline.generate_endpoint_controller import HttpEndpointProber

    prober = HttpEndpointProber(timeout_seconds=5)
    prober._opener = opener
    return prober


def test_prober_seeds_no_proxy_from_swegen_and_honours_it_per_host(monkeypatch):
    """The prober must proxy external endpoints but bypass internal ones.

    Registered endpoints are mixed: internal cluster hosts (7.244.x) are
    reachable ONLY directly (the corporate proxy 504s them), while external
    public IPs are reachable ONLY through the proxy. An unconditional proxy (or
    unconditional bypass) falsely trips one side, so the probe must honour the
    no-proxy list per-host -- exactly like the generate workers. The pod carries
    the list only as SWEGEN_NO_PROXY, so the prober seeds no_proxy/NO_PROXY from
    it and a default opener then routes internal->direct, external->proxy.
    """

    import urllib.request

    from swegen.pipeline.generate_endpoint_controller import HttpEndpointProber

    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    # Internal 7.244.3.251 is in the list; external 1.95.77.23 is NOT.
    monkeypatch.setenv(
        "SWEGEN_NO_PROXY", ".huawei.com,127.0.0.1,localhost,7.244.3.251,10.*"
    )

    prober = HttpEndpointProber(timeout_seconds=5)

    # The prober seeded the standard vars from SWEGEN_NO_PROXY so urllib can see them.
    assert os.environ["no_proxy"] == ".huawei.com,127.0.0.1,localhost,7.244.3.251,10.*"
    assert os.environ["NO_PROXY"]

    # The opener still carries the env proxy (external hosts must use it)...
    proxy_maps = [
        h.proxies
        for h in prober._opener.handlers
        if isinstance(h, urllib.request.ProxyHandler)
    ]
    assert proxy_maps and any(m.get("http") for m in proxy_maps), (
        "prober must retain the env proxy for external endpoints"
    )
    # ...but the no-proxy list bypasses the proxy for the internal host and NOT
    # for the external one (urllib's own per-host bypass decision).
    proxies = {"http": "http://proxy.example:8080", "no": os.environ["no_proxy"]}
    assert urllib.request.proxy_bypass_environment("7.244.3.251:8088", proxies)
    assert not urllib.request.proxy_bypass_environment("1.95.77.23:3000", proxies)


def test_prober_does_not_clobber_preset_no_proxy(monkeypatch):
    """Seeding is a fallback: an already-set no_proxy (from the manifest) wins."""

    from swegen.pipeline.generate_endpoint_controller import HttpEndpointProber

    monkeypatch.setenv("SWEGEN_NO_PROXY", "7.244.3.251")
    monkeypatch.setenv("no_proxy", "preset.example,10.*")
    monkeypatch.setenv("NO_PROXY", "preset.example,10.*")

    HttpEndpointProber(timeout_seconds=5)

    assert os.environ["no_proxy"] == "preset.example,10.*"
    assert os.environ["NO_PROXY"] == "preset.example,10.*"


def test_prober_http_5xx_captures_status_body_and_url_in_error_text(monkeypatch):
    import urllib.error

    from swegen.pipeline import generate_endpoint_controller as ctrl

    # Route our _FakeHTTPError through the real HTTPError except branch.
    monkeypatch.setattr(urllib.error, "HTTPError", _FakeHTTPError)

    class _Opener:
        def open(self, request, timeout):
            raise _FakeHTTPError(504, "<html>upstream gateway timeout</html>")

    result = _prober_with_opener(_Opener()).probe(
        "http://host:8088", "some-model", "SECRET-BEARER-TOKEN"
    )
    assert result.ok is False
    assert result.status == 504
    assert result.detail == "http 504"
    # Full, multi-line, copyable text: request line + status + body + URL.
    assert "POST http://host:8088/v1/messages -> HTTP 504" in result.error_text
    assert "upstream gateway timeout" in result.error_text
    assert "\n" in result.error_text
    # The bearer token is never present in the captured error text.
    assert "SECRET-BEARER-TOKEN" not in result.error_text
    # Silence unused-import lint on ctrl (module referenced for clarity).
    assert ctrl.DEFAULT_UNHEALTHY_STATUSES


def test_prober_http_4xx_is_healthy_and_carries_no_error_text(monkeypatch):
    import urllib.error

    monkeypatch.setattr(urllib.error, "HTTPError", _FakeHTTPError)

    class _Opener:
        def open(self, request, timeout):
            raise _FakeHTTPError(401, "unauthorized")

    result = _prober_with_opener(_Opener()).probe(
        "http://host:8088", "m", "SECRET-BEARER-TOKEN"
    )
    # A non-unhealthy 4xx means the endpoint is reachable -> healthy, no error box.
    assert result.ok is True
    assert result.status == 401
    assert result.error_text == ""


def test_prober_treats_router_no_backend_400_as_unhealthy(monkeypatch):
    """A 400 saying the model group has no backend must NOT read as healthy.

    The endpoints sit behind a LiteLLM router, which answers 400 -- not 5xx --
    once a model group loses every healthy deployment. On 2026-08-14 that let a
    dashboard reset clear the breaker onto a model with no backend at all: every
    request 400d in ~1.2s, so 96 workers drained the queue at full speed with a
    100% failure rate and nothing stopped them -- 1,616 failures and zero
    successes in ten minutes. The status alone cannot tell that apart from a
    genuine client error, so the body has to be read.
    """

    import urllib.error

    monkeypatch.setattr(urllib.error, "HTTPError", _FakeHTTPError)

    body = (
        '{"error":{"message":"litellm.BadRequestError: You passed in '
        "model=GLM-52_pre-train_256K. There are no healthy deployments for this "
        'model. Received Model Group=GLM-52_pre-train_256K","code":"400"}}'
    )

    class _Opener:
        def open(self, request, timeout):
            raise _FakeHTTPError(400, body)

    result = _prober_with_opener(_Opener()).probe(
        "http://host:8088", "GLM-52_pre-train_256K", "SECRET-BEARER-TOKEN"
    )

    assert result.ok is False, "a model group with no backend is not healthy"
    assert result.status == 400
    # The operator needs the router's own words to see why it stayed latched.
    assert "no healthy deployments" in result.error_text
    assert "SECRET-BEARER-TOKEN" not in result.error_text


def test_prober_unreachable_captures_reason_and_url():
    import urllib.error

    class _Opener:
        def open(self, request, timeout):
            raise urllib.error.URLError("Connection refused")

    result = _prober_with_opener(_Opener()).probe(
        "http://host:8088", "m", "SECRET-BEARER-TOKEN"
    )
    assert result.ok is False
    assert result.status is None
    assert "unreachable" in result.detail
    assert "http://host:8088/v1/messages" in result.error_text
    assert "Connection refused" in result.error_text
    assert "SECRET-BEARER-TOKEN" not in result.error_text


def test_prober_timeout_captures_timeout_and_url():
    class _Opener:
        def open(self, request, timeout):
            raise TimeoutError()

    result = _prober_with_opener(_Opener()).probe(
        "http://host:8088", "m", "SECRET-BEARER-TOKEN"
    )
    assert result.ok is False
    assert result.detail == "timeout"
    assert "timeout after 5s" in result.error_text
    assert "http://host:8088/v1/messages" in result.error_text
