"""Dependency-light HTTP server for the distributed pipeline dashboard."""

from __future__ import annotations

import hmac
import json
import re
import secrets
import subprocess
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from swegen.dashboard.distributed_status import (
    K3sStatusCollector,
    PipelineStatusCollector,
    RemoteBuildKitFarmCollector,
    _database_dsn,
)
from swegen.pipeline.generate_endpoint_controller import (
    EndpointProber,
    HttpEndpointProber,
)

SCALE_MIN = 0
BUILD_SLOT_MIN = 1
# k8s-label-safe slug, mirroring the generate_endpoints CHECK constraint. The
# slug is embedded in the Deployment name swegen-generate-dyn-<slug>.
ENDPOINT_SLUG_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,40}[a-z0-9])?$")
# One deployment per stage. Generate used to fan out into a primary +
# "overflow" pool (split at a 92-replica cap, with the overflow's image and
# model Secret copied from the live primary on every scale-up). That sync
# silently reverted the fleet to a stale image, so generate is now a single
# deployment scaled directly like every other stage.
PRIMARY_DEPLOYMENTS = {
    "generate": "swegen-generate",
    "validate": "swegen-validate",
    "repair": "swegen-repair",
    "reward": "swegen-reward",
    "push": "swegen-push",
}


# Allowed operator-selectable timeseries lookback windows for the stacked-bar
# charts. Any other value (junk, out-of-set) falls back to the default so a
# crafted query param can never widen the DB scan or reach the SQL unvalidated.
RANGE_HOURS_ALLOWED = (24, 72, 168)
RANGE_HOURS_DEFAULT = 24


def _parse_range_hours(query: str) -> int:
    """Validate ``?range_hours=`` against the allowed set, defaulting otherwise."""

    values = parse_qs(query).get("range_hours", [])
    if values:
        try:
            candidate = int(values[0])
        except (TypeError, ValueError):
            candidate = RANGE_HOURS_DEFAULT
        if candidate in RANGE_HOURS_ALLOWED:
            return candidate
    return RANGE_HOURS_DEFAULT


class ScalingBusyError(RuntimeError):
    """Raised when another scaling operation is already in flight."""


class K3sScaler:
    """Strictly allowlisted, argv-only deployment scaling."""

    _WORKER_CONTAINER = "worker"

    def __init__(
        self,
        *,
        namespace: str = "swegen-pipeline",
        runner: Any = subprocess.run,
    ) -> None:
        self.namespace = namespace
        self.runner = runner
        self._lock = threading.Lock()

    @staticmethod
    def plan(
        stage: object,
        replicas: object,
        *,
        max_replicas: object,
    ) -> list[tuple[str, int]]:
        if not isinstance(stage, str) or stage not in PRIMARY_DEPLOYMENTS:
            raise ValueError("unknown stage")
        if isinstance(replicas, bool) or not isinstance(replicas, int):
            raise ValueError("replicas must be an integer")
        if (
            isinstance(max_replicas, bool)
            or not isinstance(max_replicas, int)
            or max_replicas < SCALE_MIN
        ):
            raise ValueError("cluster scaling capacity is unavailable")
        if not SCALE_MIN <= replicas <= max_replicas:
            raise ValueError(f"replicas must be between {SCALE_MIN} and {max_replicas}")
        return [(PRIMARY_DEPLOYMENTS[stage], replicas)]

    def _run(self, command: list[str], *, failure: str) -> subprocess.CompletedProcess[str]:
        completed = self.runner(
            command,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if completed.returncode != 0:
            message = (completed.stderr or failure)[:500]
            raise RuntimeError(message)
        return completed

    def scale(
        self,
        stage: object,
        replicas: object,
        *,
        max_replicas: object,
    ) -> list[dict[str, Any]]:
        plan = self.plan(stage, replicas, max_replicas=max_replicas)
        if not self._lock.acquire(blocking=False):
            raise ScalingBusyError("another scaling request is already running")
        try:
            applied = []
            for deployment, count in plan:
                command = [
                    "kubectl",
                    "--request-timeout=10s",
                    "-n",
                    self.namespace,
                    "scale",
                    f"deployment/{deployment}",
                    f"--replicas={count}",
                ]
                self._run(command, failure="kubectl scale failed")
                applied.append({"deployment": deployment, "replicas": count})
            return applied
        finally:
            self._lock.release()


class BuildSlotBusyError(RuntimeError):
    """Raised when another node-local BuildKit slot update is in flight."""


class K3sBuildSlotController:
    """Update an allowlisted node's mounted BuildKit slot count atomically."""

    _UPDATE_SCRIPT = """import json,os,pathlib,sys
d=pathlib.Path('/run/swegen-build-slots')
n=int(sys.argv[1])
if n < 1:
 raise ValueError('slot count must be positive')
d.mkdir(parents=True,exist_ok=True)
for i in range(n):
 (d/str(i)).touch(exist_ok=True)
tmp=d/f'.count.{os.getpid()}.tmp'
tmp.write_text(f'{n}\\n',encoding='utf-8')
os.replace(tmp,d/'count')
print(json.dumps({'slots':n}))"""

    def __init__(
        self,
        *,
        namespace: str = "swegen-pipeline",
        runner: Any = subprocess.run,
    ) -> None:
        self.namespace = namespace
        self.runner = runner
        self._lock = threading.Lock()

    @staticmethod
    def plan(
        node: object,
        slots: object,
        *,
        nodes: object,
    ) -> tuple[str, int]:
        if not isinstance(node, str) or not node:
            raise ValueError("node must be a non-empty string")
        if isinstance(slots, bool) or not isinstance(slots, int):
            raise ValueError("slots must be an integer")
        if not isinstance(nodes, list):
            raise ValueError("cluster node status is unavailable")
        node_status = next(
            (entry for entry in nodes if isinstance(entry, dict) and entry.get("name") == node),
            None,
        )
        if node_status is None:
            raise ValueError("unknown node")
        max_slots = node_status.get("build_slot_max")
        if isinstance(max_slots, bool) or not isinstance(max_slots, int) or max_slots < 1:
            raise ValueError("node BuildKit slot capacity is unavailable")
        if not BUILD_SLOT_MIN <= slots <= max_slots:
            raise ValueError(
                f"slots must be between {BUILD_SLOT_MIN} and {max_slots} for this node"
            )
        probe_pod = node_status.get("build_slot_probe_pod")
        if not isinstance(probe_pod, str) or not probe_pod:
            raise ValueError("node BuildKit slot controller is unavailable")
        return probe_pod, max_slots

    def update(
        self,
        node: object,
        slots: object,
        *,
        nodes: object,
    ) -> dict[str, Any]:
        probe_pod, max_slots = self.plan(node, slots, nodes=nodes)
        if not self._lock.acquire(blocking=False):
            raise BuildSlotBusyError("another BuildKit slot update is already running")
        try:
            command = [
                "kubectl",
                "--request-timeout=10s",
                "-n",
                self.namespace,
                "exec",
                probe_pod,
                "--",
                "python",
                "-c",
                self._UPDATE_SCRIPT,
                str(slots),
            ]
            completed = self.runner(
                command,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if completed.returncode != 0:
                message = (completed.stderr or "BuildKit slot update failed")[:500]
                raise RuntimeError(message)
            return {
                "node": node,
                "slots": slots,
                "max_slots": max_slots,
                "controller_pod": probe_pod,
            }
        finally:
            self._lock.release()


class EndpointNotFoundError(LookupError):
    """Raised when a registry operation targets an absent slug (maps to 404)."""


class EndpointConflictError(RuntimeError):
    """Raised when a register would collide with an existing slug (maps to 409)."""


def _validate_base_url(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("base_url must be a string")
    candidate = value.strip()
    if not candidate:
        raise ValueError("base_url is required")
    parsed = urlsplit(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base_url must be http(s)://host[:port]")
    return candidate


def _validate_nonblank(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _validate_concurrency(value: object, *, max_replicas: object) -> int:
    if (
        isinstance(max_replicas, bool)
        or not isinstance(max_replicas, int)
        or max_replicas < SCALE_MIN
    ):
        raise ValueError("cluster scaling capacity is unavailable")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("concurrency must be an integer")
    if not SCALE_MIN <= value <= max_replicas:
        raise ValueError(f"concurrency must be between {SCALE_MIN} and {max_replicas}")
    return value


def derive_endpoint_slug(model_id: str, base_url: str) -> str:
    """Derive a k8s-label-safe slug from model_id + endpoint host.

    Lowercases, keeps only ``[a-z0-9-]``, collapses runs of dashes, trims to 40
    chars, and strips leading/trailing dashes so the result matches
    ``ENDPOINT_SLUG_RE``.
    """

    host = urlsplit(base_url).hostname or urlsplit(base_url).netloc or ""
    raw = f"{model_id}-{host}".lower()
    cleaned = re.sub(r"[^a-z0-9-]+", "-", raw)
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")[:40].strip("-")
    if not cleaned or not ENDPOINT_SLUG_RE.match(cleaned):
        raise ValueError("could not derive a valid slug from model_id and endpoint")
    return cleaned


class GenerateEndpointRegistry:
    """Registry-table writer for dynamic generate endpoints.

    The dashboard only ever WRITES the ``generate_endpoints`` table; a separate
    controller reconciles rows to Deployments and probes/latches breakers. Every
    write is committed via a short-lived autocommit connection: a fresh psycopg
    connection's ``transaction()`` context did not durably persist here, so a
    committed transaction (autocommit) is used explicitly. The ``auth_token`` is
    stored but never read back into any response or snapshot.
    """

    def __init__(
        self,
        *,
        connect: Any = None,
        dsn: Any = None,
        prober: EndpointProber | None = None,
    ) -> None:
        self._connect = connect
        self._dsn = dsn
        self._prober: EndpointProber = prober or HttpEndpointProber()
        self._lock = threading.Lock()

    def _connection(self) -> Any:
        if self._connect is not None:
            return self._connect()
        import psycopg

        dsn = self._dsn() if callable(self._dsn) else (self._dsn or _database_dsn())
        return psycopg.connect(dsn, autocommit=True)

    def register(
        self,
        *,
        base_url: object,
        model_id: object,
        auth_token: object,
        concurrency: object,
        max_replicas: object,
    ) -> dict[str, Any]:
        url = _validate_base_url(base_url)
        model = _validate_nonblank(model_id, "model_id")
        token = _validate_nonblank(auth_token, "auth_token")
        count = _validate_concurrency(concurrency, max_replicas=max_replicas)
        slug = derive_endpoint_slug(model, url)
        with self._lock, self._connection() as conn:
            existing = conn.execute(
                "SELECT 1 FROM generate_endpoints WHERE slug = %s", (slug,)
            ).fetchone()
            if existing is not None:
                raise EndpointConflictError("endpoint already registered")
            conn.execute(
                """
                INSERT INTO generate_endpoints
                    (slug, base_url, model_id, auth_token, concurrency,
                     enabled, breaker_open)
                VALUES (%s, %s, %s, %s, %s, TRUE, FALSE)
                """,
                (slug, url, model, token, count),
            )
            self._event(conn, slug, model, "registered", "registered via dashboard")
        return {"ok": True, "slug": slug}

    def scale(self, *, slug: object, concurrency: object, max_replicas: object) -> dict[str, Any]:
        target = self._require_slug(slug)
        count = _validate_concurrency(concurrency, max_replicas=max_replicas)
        with self._lock, self._connection() as conn:
            model = self._model_for(conn, target)
            result = conn.execute(
                "UPDATE generate_endpoints SET concurrency = %s, updated_at = now() "
                "WHERE slug = %s",
                (count, target),
            )
            if getattr(result, "rowcount", 0) != 1:
                raise EndpointNotFoundError("endpoint not found")
            self._event(conn, target, model, "scaled", f"concurrency set to {count}")
        return {"ok": True, "slug": target, "concurrency": count}

    def update(
        self,
        *,
        slug: object,
        base_url: object = None,
        model_id: object = None,
        auth_token: object = None,
    ) -> dict[str, Any]:
        target = self._require_slug(slug)
        fields: list[str] = []
        params: list[object] = []
        if base_url is not None:
            fields.append("base_url = %s")
            params.append(_validate_base_url(base_url))
        if model_id is not None:
            fields.append("model_id = %s")
            params.append(_validate_nonblank(model_id, "model_id"))
        if auth_token is not None:
            fields.append("auth_token = %s")
            params.append(_validate_nonblank(auth_token, "auth_token"))
        if not fields:
            raise ValueError("no fields to update")
        with self._lock, self._connection() as conn:
            model = self._model_for(conn, target)
            result = conn.execute(
                f"UPDATE generate_endpoints SET {', '.join(fields)}, updated_at = now() "
                "WHERE slug = %s",
                (*params, target),
            )
            if getattr(result, "rowcount", 0) != 1:
                raise EndpointNotFoundError("endpoint not found")
            new_model = model_id if model_id is not None else model
            self._event(conn, target, new_model, "updated", "endpoint API updated")
        return {"ok": True, "slug": target}

    def reset(self, *, slug: object) -> dict[str, Any]:
        """Live-probe the endpoint; unlatch its breaker only if the probe passes.

        The manual reset no longer clears the breaker blindly. It reads the
        endpoint's ``(base_url, model_id, auth_token)`` server-side, fires one
        health probe, and only runs the unlatch UPDATE when the probe reports the
        endpoint healthy. A failed probe leaves ``breaker_open = TRUE`` and simply
        records the probe outcome, so the operator learns the endpoint is still
        down instead of flapping a still-broken pool back up. The token is used
        only to authenticate the probe and is never returned to the caller.
        """

        target = self._require_slug(slug)
        with self._lock, self._connection() as conn:
            endpoint = self._endpoint_for(conn, target)
            if endpoint is None:
                raise EndpointNotFoundError("endpoint not found")
            base_url, model, token = endpoint
            probe = self._prober.probe(base_url, model, token)
            now = datetime.now(UTC)
            if probe.ok:
                conn.execute(
                    """
                    UPDATE generate_endpoints
                    SET breaker_open = FALSE, breaker_reason = NULL, reset_at = now(),
                        consecutive_fail = 0, last_probe_status = %s,
                        last_probe_at = %s, updated_at = now()
                    WHERE slug = %s
                    """,
                    (probe.status, now, target),
                )
                self._event(
                    conn,
                    target,
                    model,
                    "reset",
                    "breaker latch cleared via dashboard",
                    detail=json.dumps({"probe_ok": True, "status": probe.status}),
                )
                return {
                    "ok": True,
                    "slug": target,
                    "unlatched": True,
                    "probe_status": probe.status,
                }
            # Probe failed: keep the breaker latched, record the outcome, and
            # surface the failure to the operator (but never the token).
            conn.execute(
                """
                UPDATE generate_endpoints
                SET breaker_reason = %s, last_probe_status = %s, last_probe_at = %s,
                    updated_at = now()
                WHERE slug = %s
                """,
                (f"reset probe failed: {probe.detail}", probe.status, now, target),
            )
            self._event(
                conn,
                target,
                model,
                "reset",
                f"reset probe failed: {probe.detail}",
                detail=json.dumps({"probe_ok": False, "status": probe.status}),
            )
        return {
            "ok": True,
            "slug": target,
            "unlatched": False,
            "probe_status": probe.status,
            "detail": probe.detail,
            "error_text": probe.error_text or probe.detail,
        }

    def delete(self, *, slug: object) -> dict[str, Any]:
        target = self._require_slug(slug)
        with self._lock, self._connection() as conn:
            model = self._model_for(conn, target)
            if model is None:
                raise EndpointNotFoundError("endpoint not found")
            # Write the audit event BEFORE deleting the row.
            self._event(conn, target, model, "deleted", "deleted via dashboard")
            conn.execute("DELETE FROM generate_endpoints WHERE slug = %s", (target,))
        return {"ok": True, "slug": target}

    @staticmethod
    def _require_slug(slug: object) -> str:
        if not isinstance(slug, str) or not ENDPOINT_SLUG_RE.match(slug):
            raise EndpointNotFoundError("endpoint not found")
        return slug

    @staticmethod
    def _model_for(conn: Any, slug: str) -> str | None:
        row = conn.execute(
            "SELECT model_id FROM generate_endpoints WHERE slug = %s", (slug,)
        ).fetchone()
        if row is None:
            return None
        return row[0] if not isinstance(row, dict) else row.get("model_id")

    @staticmethod
    def _endpoint_for(conn: Any, slug: str) -> tuple[str, str, str] | None:
        """Return (base_url, model_id, auth_token) for a slug, server-side only.

        The token is read here solely so the reset probe can authenticate; it is
        never selected into any snapshot and never returned to the browser.
        """

        row = conn.execute(
            "SELECT base_url, model_id, auth_token FROM generate_endpoints WHERE slug = %s",
            (slug,),
        ).fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            return (row.get("base_url"), row.get("model_id"), row.get("auth_token"))
        return (row[0], row[1], row[2])

    @staticmethod
    def _event(
        conn: Any,
        slug: str,
        model_id: object,
        event: str,
        reason: str,
        *,
        detail: str | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO generate_endpoint_events (slug, model_id, event, reason, detail)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (slug, model_id or "", event, reason, detail),
        )


class SnapshotCache:
    def __init__(self, *, refresh_seconds: float = 5.0) -> None:
        self.refresh_seconds = refresh_seconds
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._snapshot: dict[str, Any] = {
            "postgres": {},
            "k3s": {},
            "buildkit_farm": {},
            "sources": {},
        }
        self._collectors = {
            "postgres": PipelineStatusCollector(),
            "k3s": K3sStatusCollector(),
            "buildkit_farm": RemoteBuildKitFarmCollector(),
        }

    def refresh(self) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            current = dict(self._snapshot)
            sources = dict(current.get("sources", {}))
        for name, collector in self._collectors.items():
            try:
                current[name] = collector.collect()
                sources[name] = {"ok": True, "fetched_at": now, "error": None}
            except Exception as error:
                if name == "k3s" and current.get("k3s"):
                    retained = json.loads(json.dumps(current["k3s"], default=str))
                    scaling = dict(retained.get("scaling", {}))
                    configured_max = max(
                        (
                            int(stage.get("desired") or 0)
                            for stage in retained.get("stages", {}).values()
                        ),
                        default=0,
                    )
                    scaling["max_replicas"] = max(
                        int(scaling.get("max_replicas") or 0),
                        configured_max,
                    )
                    scaling["stale"] = True
                    retained["scaling"] = scaling
                    current["k3s"] = retained
                sources[name] = {
                    "ok": False,
                    "fetched_at": sources.get(name, {}).get("fetched_at"),
                    "error": f"{type(error).__name__}: {str(error)[:300]}",
                }
        current["sources"] = sources
        current["generated_at"] = now
        with self._lock:
            self._snapshot = current

    def snapshot(self, *, range_hours: int | None = None) -> dict[str, Any]:
        with self._lock:
            snapshot = json.loads(json.dumps(self._snapshot, default=str))
        if range_hours is None:
            return snapshot
        # A non-default range bypasses the cached (48h) postgres section and
        # recomputes only that section with the requested lookback, reusing the
        # cached k3s / buildkit_farm / sources so a range change never re-reads
        # the cluster. The postgres timeseries window is the only thing the
        # range dropdown affects, so keying the recompute to it keeps the whole
        # thing cheap and correct without a per-range cache.
        now = datetime.now(UTC).isoformat()
        sources = dict(snapshot.get("sources", {}))
        try:
            postgres = self._collectors["postgres"].collect(lookback_hours=range_hours)
            snapshot["postgres"] = json.loads(json.dumps(postgres, default=str))
            sources["postgres"] = {"ok": True, "fetched_at": now, "error": None}
        except Exception as error:
            sources["postgres"] = {
                "ok": False,
                "fetched_at": sources.get("postgres", {}).get("fetched_at"),
                "error": f"{type(error).__name__}: {str(error)[:300]}",
            }
        snapshot["sources"] = sources
        return snapshot

    def run(self) -> None:
        while not self._stopped.is_set():
            self.refresh()
            self._stopped.wait(self.refresh_seconds)

    def stop(self) -> None:
        self._stopped.set()


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><meta name="csrf-token" content="__CSRF_TOKEN__">
<title>SWE-gen k3s Pipeline</title><style>
:root{color-scheme:dark;--bg:#07111f;--card:#102139;--muted:#91a4bd;--ok:#32d583;--bad:#ff6b6b;--line:#233955}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:#edf4ff;font:14px system-ui,sans-serif}
main{max-width:1500px;margin:auto;padding:12px}h1{font-size:24px;margin:0 0 2px}h2{font-size:18px;margin:14px 0 6px}.muted{color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:6px;margin:8px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:8px}.big{font-size:25px;font-weight:700}
.pipeline-flow{display:grid;grid-template-columns:1fr;gap:8px;align-items:stretch;margin:8px 0;padding-bottom:4px}
.pipeline-flow>.stage-card,.pipeline-flow>.validation-loop{width:100%}
.pipeline-flow>.stage-card{min-width:0}.stage-card{display:flex;flex-direction:column;padding:11px}.stage-stats{min-width:0}.stage-stats>b{display:block;margin-bottom:3px}.stage-stats>.big{margin-bottom:1px}.stage-stats>.pod-phases{color:var(--muted);font-size:11px;line-height:1.3;margin-bottom:4px;overflow-wrap:anywhere}.stage-stats>div:not(.big):not(.pod-phases):not(.scale-controls){line-height:1.35}.validation-loop{min-width:520px;background:#0c1b2f;border:2px solid #365b82;border-radius:10px;padding:9px;display:grid;grid-template-rows:auto minmax(0,1fr) minmax(0,1fr);gap:8px;align-content:stretch}.validation-loop-title{text-align:center;color:var(--muted);font-size:11px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1px}.validation-loop>.stage-card{background:var(--card)}.stage-card-horizontal{display:grid;grid-template-columns:240px minmax(0,1fr) minmax(150px,210px);gap:12px;align-items:stretch;padding:10px}.stage-card-horizontal .stage-stats{width:240px;text-align:left;justify-self:start;align-self:start}.stage-card-horizontal .stage-chart-wrap{border-left:1px solid var(--line);padding-left:10px}.stage-card-horizontal .stage-yield{border-left:1px solid var(--line);padding-left:10px}@media(max-width:1100px){.stage-card-horizontal{grid-template-columns:240px minmax(0,1fr)}.stage-card-horizontal .stage-yield{grid-column:1/-1;border-left:0;border-top:1px solid var(--line);padding-left:0;padding-top:8px}}
.validate-queue-breakdown{margin:4px 0;padding:4px 0;border-top:1px solid var(--line);border-bottom:1px solid var(--line);color:var(--muted);font-size:11px}.validate-queue-breakdown>div{overflow-wrap:anywhere}.validate-queue-breakdown b{color:#edf4ff}
.ok{color:var(--ok)}.bad{color:var(--bad)}table{width:100%;border-collapse:collapse;background:var(--card)}
th,td{text-align:left;padding:5px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted)}
.scroll{overflow:auto;border:1px solid var(--line);border-radius:10px}details summary{cursor:pointer}.banner{padding:10px;border:1px solid var(--bad);border-radius:8px;margin:8px 0}
.stage-chart-wrap{min-width:0;min-height:0;display:flex;flex-direction:column}.stage-chart-title{display:flex;justify-content:space-between;align-items:baseline;gap:6px;color:var(--muted);font-size:10px;line-height:1.2;margin-bottom:4px}.stage-card:not(.stage-card-horizontal) .stage-chart-row{flex:1;margin-top:10px;padding-top:8px;border-top:1px solid var(--line);display:grid;grid-template-columns:minmax(0,1fr) minmax(150px,220px);gap:12px;align-items:stretch}.stage-card:not(.stage-card-horizontal) .stage-chart-row .stage-chart-wrap{flex:1}.stage-card:not(.stage-card-horizontal) .stage-chart-row .stage-yield{border-left:1px solid var(--line);padding-left:12px}@media(max-width:760px){.stage-card:not(.stage-card-horizontal) .stage-chart-row{grid-template-columns:1fr}.stage-card:not(.stage-card-horizontal) .stage-chart-row .stage-yield{border-left:0;border-top:1px solid var(--line);padding-left:0;padding-top:8px}}.chart-frame{min-width:0;min-height:112px;flex:1;display:grid;grid-template-columns:auto minmax(0,1fr);gap:5px}.chart-y-axis{min-width:24px;display:flex;flex-direction:column;justify-content:space-between;padding:5px 0 19px;color:var(--muted);font:9px ui-monospace,SFMono-Regular,Consolas,monospace;text-align:right;font-variant-numeric:tabular-nums}.chart{min-height:112px;min-width:0;display:flex;align-items:stretch;gap:2px;border-bottom:1px solid var(--line);padding:5px 1px 0;overflow-x:auto}.bucket{height:auto;min-width:8px;flex:1;display:grid;grid-template-rows:minmax(0,1fr) 18px}.bar-slot{min-height:0;display:flex;align-items:flex-end}.bar{width:100%;display:flex;flex-direction:column-reverse;justify-content:flex-start;min-height:1px}.segment{width:100%;min-height:0}.x-tick{height:18px;position:relative;color:var(--muted);font:9px ui-monospace,SFMono-Regular,Consolas,monospace}.x-tick::before{content:"";position:absolute;top:0;left:50%;height:4px;border-left:1px solid var(--muted);opacity:.75}.x-tick-label{position:absolute;top:6px;left:50%;transform:translateX(-50%);white-space:nowrap;line-height:1}.bucket:first-child .x-tick-label{left:0;transform:none}.bucket:last-child .x-tick-label{left:auto;right:0;transform:none}.chart-empty{min-height:96px;flex:1;display:grid;place-items:center;text-align:center;color:var(--muted);border-bottom:1px solid var(--line);font-size:11px}
.chart-diverging .chart-y-axis{padding:5px 0 19px}.chart-diverging .bucket{grid-template-rows:minmax(0,1fr) minmax(0,1fr) 18px}.bar-slot-up{align-items:flex-end;border-bottom:1px solid var(--line)}.bar-slot-down{align-items:flex-start}.bar-down{flex-direction:column;justify-content:flex-start}.rejected-marker{width:100%;height:3px;flex:0 0 auto;background:var(--muted);opacity:.7;margin-top:1px}.diverging-legend{display:flex;flex-wrap:wrap;gap:4px 10px;margin-top:6px;font-size:10px;color:var(--muted)}.diverging-legend-item{display:inline-flex;align-items:center;gap:4px;white-space:nowrap}.diverging-legend-swatch{width:9px;height:9px;border-radius:2px;flex:0 0 auto}.diverging-legend-note{color:var(--muted);opacity:.85}
.chart-tooltip{position:fixed;z-index:1000;max-width:min(360px,calc(100vw - 16px));padding:5px 7px;border:1px solid #45658a;border-radius:6px;background:#06101d;color:#edf4ff;box-shadow:0 4px 16px #0009;font-size:11px;line-height:1.3;pointer-events:none;white-space:pre}.chart-tooltip[hidden]{display:none}.chart-tooltip-row{display:flex;align-items:center;gap:5px}.chart-tooltip-swatch{display:inline-block;width:9px;height:9px;border-radius:2px;flex:none}
.stage-yield{min-width:0;min-height:0;display:flex;flex-direction:column}.stage-yield-title{display:flex;justify-content:space-between;align-items:baseline;gap:6px;color:var(--muted);font-size:10px;line-height:1.2;margin-bottom:4px}.yield-list{margin-top:0;flex:1;min-height:0;overflow-y:auto}.yield-row{display:grid;grid-template-columns:1fr auto auto;gap:5px;padding:3px 0;border-bottom:1px solid var(--line);font-size:11px;font-variant-numeric:tabular-nums}.yield-row:last-child{border-bottom:0}.yield-value{font-variant-numeric:tabular-nums}.yield-percent{min-width:44px;text-align:right;font-weight:700}.yield-empty{flex:1;display:grid;place-items:center;text-align:center;color:var(--muted);font-size:11px;padding:8px 0}
.scale-controls{width:100%;max-width:174px;display:grid;grid-template-columns:minmax(76px,1fr) 58px;gap:4px;margin:8px auto 0;align-items:center}.scale-controls input,.scale-controls button{min-width:0;border:1px solid var(--line);border-radius:6px;padding:5px 6px;background:#09182b;color:#edf4ff}.scale-controls button{cursor:pointer;background:#174b78}.scale-controls button:disabled{cursor:wait;opacity:.55}.scale-limit{grid-column:1/-1;color:var(--muted);font-size:10px;line-height:1.2;margin-top:-1px;text-align:center}#scale-feedback{min-height:18px;margin-top:4px}.stages-controls{display:flex;flex-wrap:wrap;gap:6px 14px;align-items:baseline;justify-content:space-between}.range-select{display:inline-flex;align-items:baseline;gap:4px;font-size:12px}.range-select select{border:1px solid var(--line);border-radius:6px;padding:3px 6px;background:#09182b;color:#edf4ff;cursor:pointer}
.slot-cell{display:grid;gap:3px;min-width:190px}.slot-status{font-size:12px;line-height:1.25}.slot-controls{display:grid;grid-template-columns:72px 52px;gap:4px;width:128px;align-items:center}.slot-controls input,.slot-controls button{min-width:0;border:1px solid var(--line);border-radius:6px;padding:4px 5px;background:#09182b;color:#edf4ff}.slot-controls button{cursor:pointer;background:#174b78}.slot-controls button:disabled{cursor:wait;opacity:.55}.slot-limit{grid-column:1/-1;color:var(--muted);font-size:9px;line-height:1.15}#build-slot-feedback{min-height:18px;margin:2px 0 4px}
.endpoint-form{display:flex;flex-wrap:wrap;gap:6px;align-items:end;margin:6px 0}.endpoint-form label{display:flex;flex-direction:column;gap:2px;font-size:11px;color:var(--muted)}.endpoint-form input{border:1px solid var(--line);border-radius:6px;padding:5px 6px;background:#09182b;color:#edf4ff;min-width:0}.endpoint-form .url-field{flex:2 1 240px}.endpoint-form .model-field{flex:2 1 200px}.endpoint-form .token-field{flex:1 1 160px}.endpoint-form .conc-field{flex:0 0 90px}.endpoint-form button{border:1px solid var(--line);border-radius:6px;padding:6px 10px;background:#174b78;color:#edf4ff;cursor:pointer}.endpoint-form button:disabled{cursor:wait;opacity:.55}#endpoint-feedback{min-height:18px;margin:4px 0}.endpoint-actions{display:flex;flex-wrap:wrap;gap:4px}.endpoint-actions button{border:1px solid var(--line);border-radius:6px;padding:3px 8px;background:#09182b;color:#edf4ff;cursor:pointer;font-size:12px}.endpoint-actions button.danger{background:#3a1220;border-color:#7a2740}.endpoint-actions button:disabled{cursor:wait;opacity:.5}.endpoint-breaker-ok{color:var(--ok)}.endpoint-breaker-latched{color:var(--bad)}#endpoint-reset-error{margin:4px 0}#endpoint-reset-error:empty{display:none}.reset-error-box{border:1px solid #7a2740;border-radius:8px;background:#1a0d13;padding:8px;margin:4px 0}.reset-error-head{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:4px}.reset-error-head .bad{font-size:12px}.reset-error-copy{border:1px solid var(--line);border-radius:6px;padding:3px 8px;background:#174b78;color:#edf4ff;cursor:pointer;font-size:12px}.reset-error-text{width:100%;min-height:96px;resize:vertical;border:1px solid var(--line);border-radius:6px;background:#06101d;color:#ffd79a;padding:6px;font:12px ui-monospace,SFMono-Regular,Consolas,monospace;white-space:pre;overflow:auto}
.warning{padding:8px;border:2px solid #f79009;background:#3b2605;color:#ffd79a;border-radius:8px;margin:6px 0}.path{display:block;max-width:520px;overflow-wrap:anywhere;font:12px ui-monospace,SFMono-Regular,Consolas,monospace;color:#b8d8ff}.storage-note{margin-top:3px;color:var(--muted);font-size:12px}
.farm-status{margin:3px 0 6px}.farm-detail{color:var(--muted);font-size:12px;margin-top:3px;overflow-wrap:anywhere}
.node-details>summary{list-style-position:inside;cursor:pointer}.node-summary{display:grid;grid-template-columns:minmax(250px,1fr) minmax(190px,.75fr) minmax(190px,.75fr) minmax(280px,1.05fr) minmax(190px,.75fr);gap:12px;align-items:center}.pod-list{margin:12px 0 2px 22px;display:grid;gap:5px}.pod-row{display:grid;grid-template-columns:minmax(300px,2fr) 110px 100px 80px;gap:10px;padding:5px 8px;border-left:2px solid var(--line);font:12px ui-monospace,SFMono-Regular,Consolas,monospace}.stage-counts{color:var(--muted);font-size:11px;margin-left:22px}@media(max-width:800px){.node-summary{grid-template-columns:1fr}.pod-row{grid-template-columns:1fr 1fr}.node-table-head{display:none}}
</style></head><body><div id="chart-tooltip" class="chart-tooltip" role="tooltip" hidden></div><main><h1>SWE-gen k3s + PGMQ</h1><div id="stamp" class="muted"></div><div id="errors"></div>
<h2>Generate model endpoints</h2><div class="muted">Register a model endpoint to spin up a dedicated generate worker pool. A controller reconciles pods and latches a breaker on repeated endpoint 5xx/429; the token is stored server-side and never shown.</div>
<form id="endpoint-form" class="endpoint-form" autocomplete="off">
<label class="url-field">Endpoint URL<input id="endpoint-url" type="url" placeholder="https://host:port" required></label>
<label class="model-field">Model ID<input id="endpoint-model" type="text" placeholder="model-id" required></label>
<label class="token-field">Bearer token<input id="endpoint-token" type="password" placeholder="token" required></label>
<label class="conc-field">Concurrency<input id="endpoint-concurrency" type="number" min="0" step="1" value="0"></label>
<button id="endpoint-register" type="submit">Register</button>
</form>
<div id="endpoint-feedback" class="muted"></div>
<div id="endpoint-reset-error"></div>
<div class="scroll"><table><thead><tr><th>Model</th><th>Endpoint</th><th>Target / running</th><th>5m success / fail</th><th>Breaker</th><th>Actions</th></tr></thead><tbody id="endpoints"></tbody></table></div>
<h2>Stages</h2><div class="stages-controls"><span class="muted">Each stage card carries its live outcome chart plus an hourly yield mini-panel (success / all terminal outcomes, last 12h).</span><label class="range-select muted">Range: last <select id="range-hours" aria-label="Timeseries range"><option value="24">24h</option><option value="72">72h</option><option value="168">168h</option></select></label></div><div id="scale-feedback" class="muted"></div><div id="stages" class="pipeline-flow"></div>
<h2>Cluster resources</h2><div id="resource-status" class="muted"></div><div id="build-slot-feedback" class="muted"></div><div id="resource-summary" class="grid"></div><div id="resource-scroll" class="scroll"><table><thead class="node-table-head"><tr><th>Node / IP and scheduled pods</th><th title="Actual metrics usage / sum of Kubernetes CPU requests / node allocatable CPU. Both percentages use allocatable CPU as denominator.">CPU used / allocated / allocatable</th><th>Memory used / allocatable</th><th>Disk I/O read / write · IOPS · busy</th><th>Local BuildKit slots / waiters</th></tr></thead><tbody id="resource-nodes"></tbody></table></div>
<h2>Harbor task storage</h2><div id="storage-warning"></div><div id="storage-summary" class="grid"></div><div class="scroll"><table><thead><tr><th>Deployment / stage</th><th>Node</th><th>Container path</th><th>Backing storage</th><th>Durability</th></tr></thead><tbody id="storage-mounts"></tbody></table></div>
<h2>Remote BuildKit farm</h2><div id="buildkit-farm-status" class="farm-status muted"></div><div id="buildkit-farm-warning"></div><div id="buildkit-farm-summary" class="grid"></div><div class="scroll"><table><thead><tr><th>Sampled farm node / backend</th><th>Read / write throughput</th><th>Read / write IOPS</th><th>Busy / inflight pressure</th></tr></thead><tbody id="buildkit-farm-disk-io"></tbody></table></div>
<h2>SWR push-registry sync</h2><div id="swr-sync-status" class="muted"></div><div id="swr-sync-summary" class="grid"></div><div class="scroll"><table><thead><tr><th>Out-of-sync image / instance</th><th>Pushed to</th><th>Missing from</th></tr></thead><tbody id="swr-sync-instances"></tbody></table></div>
<h2>Recent tasks</h2><div class="scroll"><table><thead><tr><th>Task</th><th>State</th><th>Stage</th><th>Elapsed</th><th>Task directory / durable source</th><th>Timeline</th></tr></thead><tbody id="tasks"></tbody></table></div>
<script>
const stages=['generate','validate','repair','reward','push']; const el=id=>document.getElementById(id);
const stageNames={generate:'SWEgen',validate:'NOP / Oracle',repair:'Repair',reward:'Reward hack',push:'SWR push'};
const csrfToken=document.querySelector('meta[name="csrf-token"]').content;
const secs=n=>n==null?'—':n<60?`${Math.round(n)}s`:n<3600?`${(n/60).toFixed(1)}m`:`${(n/3600).toFixed(1)}h`;
const agoUnit=(n,unit)=>`${n} ${unit}${n===1?'':'s'} ago`;
/* An ISO collection timestamp as an operator-readable age; unparseable input degrades
to a neutral phrase rather than printing a raw timestamp back at the reader. */
const relativeAge=iso=>{const t=Date.parse(iso);if(!Number.isFinite(t))return 'an unknown time ago';const s=Math.max(0,(Date.now()-t)/1000);return s<60?agoUnit(Math.round(s),'second'):s<3600?agoUnit(Math.round(s/60),'minute'):s<86400?agoUnit(Math.round(s/3600),'hour'):agoUnit(Math.round(s/86400),'day')};
/* Backend errors arrive as "ExceptionName: message" (and kubectl adds its own "error:"
prefix); strip both so the banner shows the cause, not Python plumbing. */
const friendlyError=msg=>String(msg||'').replace(/^[A-Za-z_][A-Za-z0-9_.]*(Error|Exception|Timeout|Interrupt)\s*:\s*/,'').replace(/^error:\s*/i,'').trim();
const cpu=m=>m==null?'—':`${(m/1000).toFixed(2)} cores`;
const compactCores=m=>Number((m/1000).toFixed(m<1000?3:m<10000?2:1)).toString();
const cpuPart=(value,allocatable)=>Number.isFinite(value)&&Number.isFinite(allocatable)&&allocatable>0?`${compactCores(value)} (${Number((value*100/allocatable).toFixed(1))})%`:'—';
const formatCpuTriple=(used,allocated,allocatable)=>`${cpuPart(used,allocatable)} / ${cpuPart(allocated,allocatable)} / ${Number.isFinite(allocatable)&&allocatable>0?compactCores(allocatable):'—'} cores`;
const memory=b=>b==null?'—':`${(b/1024/1024/1024).toFixed(1)} GiB`;
const bytes=b=>b==null?'—':b<1024*1024?`${(b/1024).toFixed(1)} KiB`:`${(b/1024/1024).toFixed(1)} MiB`;
const rateBytes=b=>b==null?'—':b<1024*1024?`${(b/1024).toFixed(1)} KiB/s`:b<1024*1024*1024?`${(b/1024/1024).toFixed(1)} MiB/s`:`${(b/1024/1024/1024).toFixed(2)} GiB/s`;
const rateOps=n=>n==null?'—':`${Number(n).toFixed(n<10?1:0)}`;
const formatDiskIo=io=>!io?`R — · W — · IOPS —/— · busy —`:`R ${rateBytes(io.read_bytes_per_second)} · W ${rateBytes(io.write_bytes_per_second)} · IOPS ${rateOps(io.read_iops)}/${rateOps(io.write_iops)} · busy ${io.busy_percent==null?'—':io.busy_percent.toFixed(1)+'%'}`;
const formatBuildSlots=slots=>!slots?.available?'slots unavailable':`${slots.used}/${slots.total} used (${slots.utilization_percent?.toFixed(1)??'—'}%) · waiters ${slots.waiters??'unknown'}`;
const formatPodPhases=(phases,evicted)=>{const parts=Object.entries(phases||{}).filter(([phase,count])=>phase!=='Running'&&count>0).map(([phase,count])=>`${phase} ${count}`);if(evicted>0)parts.push(`Evicted ${evicted}`);return parts.length?parts.join(' · '):'no other pod states'};
const compactChartCount=value=>value>=1000000?`${Number((value/1000000).toFixed(1))}m`:value>=1000?`${Number((value/1000).toFixed(1))}k`:String(value);
const compactChartTimestamp=value=>new Date(value).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',hour12:false});
// Categorical hues stepped for the dark chart surface (dataviz default palette),
// assigned to model_ids in fixed order — never cycled, never rank-ordered — so a
// model keeps its colour as the set of running models changes. Success stacks up
// at full opacity; failure stacks down at reduced opacity in the same hue.
const MODEL_PALETTE=['#3987e5','#199e70','#c98500','#008300','#9085e9','#e66767','#d55181','#d95926'];
/* Colour is a STABLE function of the model_id string, never of its position in the (sorted) models array — a new model_id must not reshuffle the colours of models that sort after it, and the same model_id must keep one colour across every stage card and range change. We persist model_id -> palette index in uiState.modelColors for the session: a model keeps its colour forever once assigned; a new one starts at its string hash and linear-probes to the next unused slot so distinct models stay distinct until the palette is exhausted. */
const modelColorIndex=name=>{let h=0;const s=String(name);for(let i=0;i<s.length;i++){h=(h*31+s.charCodeAt(i))>>>0}return h%MODEL_PALETTE.length};
const modelColor=model=>{const key=String(model);const store=uiState.modelColors||(uiState.modelColors={});if(store[key]!=null)return MODEL_PALETTE[store[key]];const used=new Set(Object.values(store));let idx=modelColorIndex(key);for(let probe=0;probe<MODEL_PALETTE.length&&used.has(idx);probe++){idx=(idx+1)%MODEL_PALETTE.length}store[key]=idx;return MODEL_PALETTE[idx]};
const chartTickEvery=(pointCount,chartWidth)=>Math.max(1,Math.ceil(52/Math.max(8,chartWidth/Math.max(1,pointCount))));
const chartScrollSnapshot=chart=>{const maxScroll=Math.max(0,chart.scrollWidth-chart.clientWidth),left=Math.min(Math.max(chart.scrollLeft,0),maxScroll);return {left,followLatest:maxScroll-left<=4}};
function setText(node,value){node.textContent=value==null?'—':String(value)}
const RANGE_HOURS_ALLOWED=[24,72,168];const uiState={chartScroll:{},expandedTasks:new Set(),expandedNodes:new Set(),resourceScroll:{left:0,top:0},scaleDrafts:{},scaling:false,buildSlotDrafts:{},buildSlotUpdating:false,endpointBusy:false,rangeHours:24,modelColors:{},stageCards:null,polling:false,lastPollMs:null};
function captureUiState(){document.querySelectorAll('.chart[data-stage]').forEach(chart=>{if(chart.dataset.restoringScroll!=='true')uiState.chartScroll[chart.dataset.stage]=chartScrollSnapshot(chart)});document.querySelectorAll('#tasks details[data-task-key]').forEach(details=>{if(details.open)uiState.expandedTasks.add(details.dataset.taskKey);else uiState.expandedTasks.delete(details.dataset.taskKey)});document.querySelectorAll('#resource-nodes details[data-node-key]').forEach(details=>{if(details.open)uiState.expandedNodes.add(details.dataset.nodeKey);else uiState.expandedNodes.delete(details.dataset.nodeKey)});const resourceScroll=el('resource-scroll');if(resourceScroll){uiState.resourceScroll={left:resourceScroll.scrollLeft,top:resourceScroll.scrollTop}}}
function chartScrollTarget(saved,maxScroll){return saved===undefined||saved.followLatest?maxScroll:Math.min(Math.max(saved.left,0),maxScroll)}
function restoreChartScroll(chart,stage){const saved=Object.prototype.hasOwnProperty.call(uiState.chartScroll,stage)?uiState.chartScroll[stage]:undefined,followLatest=saved===undefined||saved.followLatest;chart.dataset.restoringScroll='true';const apply=()=>{const maxScroll=Math.max(0,chart.scrollWidth-chart.clientWidth);chart.scrollLeft=chartScrollTarget(saved,maxScroll);uiState.chartScroll[stage]={left:chart.scrollLeft,followLatest}};requestAnimationFrame(()=>requestAnimationFrame(()=>{apply();requestAnimationFrame(()=>{apply();delete chart.dataset.restoringScroll})}))}
function positionChartTooltip(event){const tooltip=el('chart-tooltip'),target=event.currentTarget;let x=event.clientX,y=event.clientY;if(!Number.isFinite(x)||!Number.isFinite(y)||event.type==='focus'){const rect=target.getBoundingClientRect();x=rect.left+rect.width/2;y=rect.top}const gap=12,maxLeft=Math.max(8,window.innerWidth-tooltip.offsetWidth-8),maxTop=Math.max(8,window.innerHeight-tooltip.offsetHeight-8);tooltip.style.left=`${Math.min(Math.max(8,x+gap),maxLeft)}px`;tooltip.style.top=`${Math.min(Math.max(8,y+gap),maxTop)}px`}
function showChartTooltip(event,value,nodes){const tooltip=el('chart-tooltip');if(nodes){tooltip.replaceChildren(...nodes)}else{setText(tooltip,value)}tooltip.hidden=false;positionChartTooltip(event)}
function hideChartTooltip(){el('chart-tooltip').hidden=true}
function updateChartTicks(chart){const ticks=[...chart.querySelectorAll('.x-tick')],every=chartTickEvery(ticks.length,chart.clientWidth);ticks.forEach((tick,index)=>{tick.hidden=index!==0&&index!==ticks.length-1&&index%every!==0})}
function scaleControls(stage,desired,maxReplicas){const capacityAvailable=Number.isSafeInteger(maxReplicas)&&maxReplicas>=0;const controls=document.createElement('div');controls.className='scale-controls';const input=document.createElement('input');input.type='number';input.min='0';input.max=capacityAvailable?String(maxReplicas):String(desired||0);input.step='1';input.value=uiState.scaleDrafts[stage]??desired??0;input.setAttribute('aria-label',`${stageNames[stage]} total workers`);input.addEventListener('input',()=>{uiState.scaleDrafts[stage]=input.value});const button=document.createElement('button');button.type='button';button.dataset.capacityAvailable=String(capacityAvailable);button.disabled=uiState.scaling||!capacityAvailable;setText(button,'Apply');button.addEventListener('click',()=>submitScale(stage,input.value,maxReplicas));const limit=document.createElement('span');limit.className='scale-limit';setText(limit,capacityAvailable?`cluster CPU ceiling: ${maxReplicas}`:'cluster CPU ceiling unavailable');controls.append(input,button,limit);return controls}
function buildSlotControls(node,slots){const maxSlots=node.build_slot_max,controllerAvailable=Boolean(node.build_slot_probe_pod)&&slots?.available&&Number.isSafeInteger(maxSlots)&&maxSlots>=1;const cell=document.createElement('div');cell.className='slot-cell';cell.title=slots?.waiters_source?`waiters from ${slots.waiters_source}`:(slots?.error||'Wrapper does not persist waiter depth; unknown is explicit.');const status=document.createElement('span');status.className=`slot-status ${(slots?.utilization_percent??0)>=90?'bad':''}`;setText(status,formatBuildSlots(slots));const controls=document.createElement('div');controls.className='slot-controls';controls.addEventListener('click',event=>event.stopPropagation());const input=document.createElement('input');input.type='number';input.min='1';input.max=controllerAvailable?String(maxSlots):String(slots?.total||1);input.step='1';input.value=uiState.buildSlotDrafts[node.name]??slots?.total??1;input.disabled=!controllerAvailable;input.setAttribute('aria-label',`${node.name} local BuildKit slots`);input.addEventListener('input',()=>{uiState.buildSlotDrafts[node.name]=input.value});const button=document.createElement('button');button.type='button';button.dataset.controllerAvailable=String(controllerAvailable);button.disabled=uiState.buildSlotUpdating||!controllerAvailable;setText(button,'Apply');button.addEventListener('click',()=>submitBuildSlots(node.name,input.value,maxSlots));const limit=document.createElement('span');limit.className='slot-limit';setText(limit,controllerAvailable?`node ceiling: ${maxSlots}`:'node controller unavailable');controls.append(input,button,limit);cell.append(status,controls);return cell}
/* Vertical zoom: normalise the diverging bars + y-axis ticks to the max of ONLY
the buckets currently visible in the horizontally-scrolling .chart pane, so a
spike elsewhere in the window never squashes the scrolled-to region. The per-cell
up/down totals and per-model segment fractions are stored on chart._buckets at
render time; rescaleVisible recomputes bar heights and tick labels from that,
without re-reading DOM text. Visibility is measured from each cell's offset box
against [scrollLeft, scrollLeft+clientWidth]. */
function rescaleVisible(chart){const cells=chart._buckets;if(!cells||!cells.length)return;const viewLeft=chart.scrollLeft||0,viewRight=viewLeft+(chart.clientWidth||0);const visible=cells.filter(c=>{const left=c.cell.offsetLeft||0,width=c.cell.offsetWidth||0;return width<=0?true:(left<viewRight&&left+width>viewLeft)});const scope=visible.length?visible:cells;const visibleUpMax=Math.max(1,...scope.map(c=>c.up));const visibleDownMax=Math.max(1,...scope.map(c=>c.down));cells.forEach(c=>{c.upBar.style.height=`${Math.min(100,c.up/visibleUpMax*100)}%`;c.downBar.style.height=`${Math.min(100,c.down/visibleDownMax*100)}%`;c.upSegs.forEach(s=>{s.seg.style.height=`${c.up?s.value/c.up*100:0}%`});c.downSegs.forEach(s=>{s.seg.style.height=`${c.down?s.value/c.down*100:0}%`})});const yAxis=chart._yAxis;if(yAxis){const ticks=yAxis.children;[visibleUpMax,0,-visibleDownMax].forEach((value,index)=>{const tick=ticks[index];if(tick)setText(tick,value<0?`-${compactChartCount(-value)}`:compactChartCount(value))})}}
function scheduleRescaleVisible(chart){if(chart._rescalePending)return;chart._rescalePending=true;requestAnimationFrame(()=>{chart._rescalePending=false;rescaleVisible(chart)})}
/* A chart's DOM shape is fixed by its bucket COUNT and its model set; only the
numbers inside change between polls. divergingSignature captures exactly that
shape, so updateDivergingChart can mutate an existing chart in place on every
poll and fall back to a full rebuild only when the shape genuinely changes
(a new bucket rolls in every 15m, or a model appears/disappears). Rebuilding
wholesale on every 5s poll is what made the bars flash and briefly show other
models' colours mid-paint. */
const divergingSignature=stageData=>`${(stageData?.buckets||[]).length}|${(stageData?.models||[]).join(',')}`;
const divergingBucketTotal=(bucket,models,key)=>models.reduce((sum,m)=>sum+((bucket.by_model?.[m]?.[key])||0),0);
/* Compact multi-line tooltip (rendered via white-space:pre) instead of the old wide middle-dot line that overflowed on hover. Header carries the up-total and labels the columns; one short line per active model. Rejected shows only when present, as a third success|failed|rejected column, so per-model lines stay short. Returns both the plain string (aria-label) and the rich nodes (per-model colour swatches). */
function divergingBucketTooltip(bucket,models,up,rejected){const showRej=rejected>0;const header=`total ${up} - success|failed${showRej?'|rejected':''}`;const activeModels=models.filter(m=>((bucket.by_model?.[m]?.succeeded)||0)||((bucket.by_model?.[m]?.failed)||0)||((bucket.by_model?.[m]?.rejected)||0));const modelLine=m=>{const c=bucket.by_model?.[m]||{};return `${m}: ${c.succeeded||0}|${c.failed||0}${showRej?`|${c.rejected||0}`:''}`};const text=[compactChartTimestamp(bucket.t),header,...activeModels.map(modelLine)].join('\n');const tsLine=document.createElement('div');setText(tsLine,compactChartTimestamp(bucket.t));const headerLine=document.createElement('div');setText(headerLine,header);const nodes=[tsLine,headerLine];activeModels.forEach(m=>{const row=document.createElement('div');row.className='chart-tooltip-row';const sw=document.createElement('span');sw.className='chart-tooltip-swatch';sw.style.background=modelColor(m);const txt=document.createElement('span');setText(txt,modelLine(m));row.append(sw,txt);nodes.push(row)});return {text,nodes}}
const divergingChartTitle=(stage,hours)=>stage==='generate'?`15m outcomes by model · last ${hours}h`:`15m outcomes by generating model · last ${hours}h`;
const divergingRejectedNote=anyRejected=>anyRejected?'faded = failed (below axis) · ▏ = rejected':'faded = failed (below axis)';
function divergingModelTimeSeries(stageData,stage,rangeHours){const models=stageData?.models||[];const buckets=stageData?.buckets||[];const hours=Number.isFinite(rangeHours)?rangeHours:48;const scrollKey=`${stage}-model`;const wrap=document.createElement('div');wrap.className='stage-chart-wrap';wrap._signature=divergingSignature(stageData);const title=document.createElement('div');title.className='stage-chart-title';const label=document.createElement('span');setText(label,divergingChartTitle(stage,hours));const legendNote=document.createElement('span');setText(legendNote,'success up · failed down');title.append(label,legendNote);wrap.append(title);wrap._titleLabel=label;if(!buckets.length||!models.length){const empty=document.createElement('div');empty.className='chart-empty';setText(empty,'No completed tasks');wrap.append(empty);return wrap}const bucketTotal=(bucket,key)=>divergingBucketTotal(bucket,models,key);const anyRejected=buckets.some(bucket=>bucketTotal(bucket,'rejected')>0);const upMax=Math.max(1,...buckets.map(bucket=>bucketTotal(bucket,'succeeded')));const downMax=Math.max(1,...buckets.map(bucket=>bucketTotal(bucket,'failed')));const frame=document.createElement('div');frame.className='chart-frame';const yAxis=document.createElement('div');yAxis.className='chart-y-axis';[upMax,0,-downMax].forEach(value=>{const tick=document.createElement('span');setText(tick,value<0?`-${compactChartCount(-value)}`:compactChartCount(value));yAxis.append(tick)});const chart=document.createElement('div');chart.className='chart chart-diverging';chart.dataset.stage=scrollKey;chart._yAxis=yAxis;wrap._chart=chart;const cellData=[];chart._buckets=cellData;chart.addEventListener('scroll',()=>{if(chart.dataset.restoringScroll!=='true')uiState.chartScroll[scrollKey]=chartScrollSnapshot(chart);scheduleRescaleVisible(chart)},{passive:true});buckets.forEach(bucket=>{const up=bucketTotal(bucket,'succeeded'),down=bucketTotal(bucket,'failed'),rejected=bucketTotal(bucket,'rejected');const tip=divergingBucketTooltip(bucket,models,up,rejected);const cell=document.createElement('div');cell.className='bucket';cell.tabIndex=0;cell.setAttribute('aria-label',tip.text);/* The hover/focus handlers read the tooltip off the bucket RECORD, not a captured local, so an in-place update refreshes tooltips without rebinding listeners. */const record={cell,up,down,upBar:null,downBar:null,upSegs:[],downSegs:[],tooltip:tip.text,tooltipNodes:tip.nodes,rejectedMarker:null,xLabel:null};cell.addEventListener('mouseenter',event=>showChartTooltip(event,record.tooltip,record.tooltipNodes));cell.addEventListener('mousemove',positionChartTooltip);cell.addEventListener('mouseleave',hideChartTooltip);cell.addEventListener('focus',event=>showChartTooltip(event,record.tooltip,record.tooltipNodes));cell.addEventListener('blur',hideChartTooltip);const upSlot=document.createElement('div');upSlot.className='bar-slot bar-slot-up';const upBar=document.createElement('div');upBar.className='bar';upBar.style.height=`${Math.min(100,up/upMax*100)}%`;const upSegs=[];/* One segment per model, ALWAYS created (zero-valued ones simply render at 0% height). A fixed segment set per bucket is what lets an update mutate heights in place instead of adding/removing nodes as a model's count crosses zero. */models.forEach(m=>{const value=(bucket.by_model?.[m]?.succeeded)||0;const seg=document.createElement('div');seg.className='segment';seg.style.height=`${up?value/up*100:0}%`;seg.style.background=modelColor(m);upBar.append(seg);upSegs.push({seg,value})});upSlot.append(upBar);const downSlot=document.createElement('div');downSlot.className='bar-slot bar-slot-down';const downBar=document.createElement('div');downBar.className='bar bar-down';downBar.style.height=`${Math.min(100,down/downMax*100)}%`;const downSegs=[];models.forEach(m=>{const value=(bucket.by_model?.[m]?.failed)||0;const seg=document.createElement('div');seg.className='segment';seg.style.height=`${down?value/down*100:0}%`;seg.style.background=modelColor(m);seg.style.opacity='0.55';downBar.append(seg);downSegs.push({seg,value})});downSlot.append(downBar);const marker=document.createElement('div');marker.className='rejected-marker';marker.hidden=!(rejected>0);downSlot.append(marker);const xTick=document.createElement('div');xTick.className='x-tick';const xLabel=document.createElement('span');xLabel.className='x-tick-label';setText(xLabel,compactChartTimestamp(bucket.t));xTick.append(xLabel);cell.append(upSlot,downSlot,xTick);chart.append(cell);Object.assign(record,{upBar,downBar,upSegs,downSegs,rejectedMarker:marker,xLabel});cellData.push(record)});frame.append(yAxis,chart);wrap.append(frame);const legend=document.createElement('div');legend.className='diverging-legend';models.forEach(m=>{const item=document.createElement('span');item.className='diverging-legend-item';const swatch=document.createElement('span');swatch.className='diverging-legend-swatch';swatch.style.background=modelColor(m);const text=document.createElement('span');setText(text,m);item.append(swatch,text);legend.append(item)});const note=document.createElement('span');note.className='diverging-legend-note';setText(note,divergingRejectedNote(anyRejected));legend.append(note);wrap._legendNote=note;wrap.append(legend);requestAnimationFrame(()=>{updateChartTicks(chart);rescaleVisible(chart)});if(typeof ResizeObserver!=='undefined'){const observer=new ResizeObserver(()=>{updateChartTicks(chart);rescaleVisible(chart)});observer.observe(chart);chart._tickObserver=observer}restoreChartScroll(chart,scrollKey);return wrap}
/* Non-destructive refresh: same shape -> mutate the existing bars, segment
values, tooltips, x-tick labels and legend note in place, then let the existing
rescaleVisible recompute heights + y-axis ticks from the stored per-bucket
totals (preserving the independent up/down scaling and the visible-window zoom).
Scroll position, tooltip listeners and model colours all survive untouched
because no node is replaced. Returns the wrap to install (the same one when
updated in place, a freshly built one when the shape changed). */
function updateDivergingChart(wrap,stageData,stage,rangeHours){if(!wrap||wrap._signature!==divergingSignature(stageData))return divergingModelTimeSeries(stageData,stage,rangeHours);const models=stageData?.models||[];const buckets=stageData?.buckets||[];const hours=Number.isFinite(rangeHours)?rangeHours:48;if(wrap._titleLabel)setText(wrap._titleLabel,divergingChartTitle(stage,hours));const chart=wrap._chart;if(!chart||!chart._buckets)return wrap;let anyRejected=false;chart._buckets.forEach((record,index)=>{const bucket=buckets[index];if(!bucket)return;const up=divergingBucketTotal(bucket,models,'succeeded'),down=divergingBucketTotal(bucket,models,'failed'),rejected=divergingBucketTotal(bucket,models,'rejected');if(rejected>0)anyRejected=true;record.up=up;record.down=down;models.forEach((m,slot)=>{if(record.upSegs[slot])record.upSegs[slot].value=(bucket.by_model?.[m]?.succeeded)||0;if(record.downSegs[slot])record.downSegs[slot].value=(bucket.by_model?.[m]?.failed)||0});if(record.rejectedMarker)record.rejectedMarker.hidden=!(rejected>0);const tip=divergingBucketTooltip(bucket,models,up,rejected);record.tooltip=tip.text;record.tooltipNodes=tip.nodes;record.cell.setAttribute('aria-label',tip.text);if(record.xLabel)setText(record.xLabel,compactChartTimestamp(bucket.t))});if(wrap._legendNote)setText(wrap._legendNote,divergingRejectedNote(anyRejected));rescaleVisible(chart);return wrap}
function validateQueueLine(label,queue){const line=document.createElement('div');line.innerHTML=`<b></b>: total <b></b> · ready <b></b> · leased/in-flight <b></b>`;const values=line.querySelectorAll('b');line._values=values;setText(values[0],label);setText(values[1],queue?.length??'—');setText(values[2],queue?.visible??'—');setText(values[3],queue?.in_flight??'—');return line}
/* Refresh the three counts in place; the label never changes so it is left as-is. */
function updateValidateQueueLine(line,queue){const values=line?._values;if(!values)return;setText(values[1],queue?.length??'—');setText(values[2],queue?.visible??'—');setText(values[3],queue?.in_flight??'—')}
function formatInstanceCoverage(count,total){const numerator=Number.isFinite(count)?count:0;if(!Number.isFinite(total)||total<=0)return `${numerator} / —`;const pct=(numerator/total*100).toFixed(2);return `${numerator} / ${total} (${pct}%)`}
/* The stat lines a poll can change, as [selector-ish key -> text] pairs. Building
the text separately from the DOM lets stageCard write it once and updateStageCard
rewrite only the text nodes, leaving the elements (and the operator's in-progress
scale input) alone. */
function stageStatLines(stage,pg,k){const q=pg.queues?.stages?.[stage]||{},a=pg.activity?.stages?.[stage]||{},w=k.stages?.[stage]||{},t=pg.throughput?.windows?.['300']?.[stage]||{},lifetime=pg.throughput?.lifetime_processed?.[stage]||0;const cov=pg.instance_coverage||{},unique=cov.unique_instances_processed?.[stage]||0,universe=cov.universe_total;const stale=Number(a.stale||0),leaseNote=` · leased ${q.in_flight||0}${stale?` · stale ${stale}`:''}`;return {running:`${w.pod_phases?.Running||0} Running`,podPhases:formatPodPhases(w.pod_phases,w.evicted||0),queueSummary:`desired ${w.desired||0} · queue ${q.visible||0} · active ${a.fresh||0}${leaseNote}`,throughput:`5m success ${t.succeeded||0} (${((t.instances_per_second||0)*60).toFixed(2)}/min)`,lifetime:`lifetime processed ${lifetime}`,unique:`unique iids ${formatInstanceCoverage(unique,universe)}`,restarts:`restarts ${w.restarts||0}`,desired:w.desired||0}}
function stageCard(stage,pg,k,maxReplicas,horizontalChart=false){const lines=stageStatLines(stage,pg,k);const card=document.createElement('section');card.className=`card stage-card${horizontalChart?' stage-card-horizontal':''}`;card.dataset.stage=stage;const stats=document.createElement('div');stats.className='stage-stats';stats.innerHTML=`<b>${stageNames[stage]}</b><div class="big"></div><div class="pod-phases" title="Live k3s pod states. Terminating is a Running pod with a deletion timestamp; Evicted records are retained by the node and never run work."></div><div class="stage-queue-summary"></div><div class="stage-throughput"></div><div class="stage-lifetime"></div><div class="unique-iids" title="Distinct pipeline instance ids this stage has processed, over the feature-PR universe."></div><div class="stage-restarts"></div>`;const refs={big:stats.querySelector('.big'),podPhases:stats.querySelector('.pod-phases'),queueSummary:stats.querySelector('.stage-queue-summary'),throughput:stats.querySelector('.stage-throughput'),lifetime:stats.querySelector('.stage-lifetime'),unique:stats.querySelector('.unique-iids'),restarts:stats.querySelector('.stage-restarts')};card._statRefs=refs;if(stage==='validate'){const queues=document.createElement('div');queues.className='validate-queue-breakdown';const repaired=validateQueueLine('Freshly repaired',pg.queues?.validate_repaired),brandNew=validateQueueLine('Brand-new tasks',pg.queues?.validate_new);queues.append(repaired,brandNew);refs.queueSummary.after(queues);card._validateQueues={repaired,brandNew}}stats.append(scaleControls(stage,lines.desired,maxReplicas));const chart=divergingModelTimeSeries(pg.stage_model_timeseries?.stages?.[stage],stage,pg.stage_model_timeseries?.lookback_hours);const yieldRows=pg.hourly_yield?.stages?.[stage]||[];const yieldView=stageHourlyYield(yieldRows,stage);card._chartWrap=chart;card._yieldWrap=yieldView;card._horizontal=horizontalChart;if(horizontalChart){card.append(stats,chart,yieldView)}else{const chartRow=document.createElement('div');chartRow.className='stage-chart-row';chartRow.append(chart,yieldView);card.append(stats,chartRow);card._chartRow=chartRow}applyStageStatLines(card,lines);return card}
function applyStageStatLines(card,lines){const refs=card._statRefs;if(!refs)return;setText(refs.big,lines.running);setText(refs.podPhases,lines.podPhases);setText(refs.queueSummary,lines.queueSummary);setText(refs.throughput,lines.throughput);setText(refs.lifetime,lines.lifetime);setText(refs.unique,lines.unique);setText(refs.restarts,lines.restarts)}
/* In-place stage-card refresh. Text nodes, the validate queue breakdown, the
chart and the hourly-yield panel are updated on the EXISTING card; the chart is
only rebuilt (and swapped) when its bucket count or model set changed, and the
yield panel likewise only when its row count changed. Nothing else is touched,
so the operator's scroll position, expanded rows and half-typed scale value all
survive a poll. */
function updateStageCard(card,stage,pg,k,maxReplicas){applyStageStatLines(card,stageStatLines(stage,pg,k));if(card._validateQueues){updateValidateQueueLine(card._validateQueues.repaired,pg.queues?.validate_repaired);updateValidateQueueLine(card._validateQueues.brandNew,pg.queues?.validate_new)}const nextChart=updateDivergingChart(card._chartWrap,pg.stage_model_timeseries?.stages?.[stage],stage,pg.stage_model_timeseries?.lookback_hours);if(nextChart!==card._chartWrap){card._chartWrap.replaceWith(nextChart);card._chartWrap=nextChart}const yieldRows=pg.hourly_yield?.stages?.[stage]||[];const nextYield=updateStageHourlyYield(card._yieldWrap,yieldRows,stage);if(nextYield!==card._yieldWrap){card._yieldWrap.replaceWith(nextYield);card._yieldWrap=nextYield}}
/* Persistent stage cards. The stages container is populated once; later polls
mutate the cached cards in place, so the charts are never torn down mid-paint.
This is the fix for the bars flashing and transiently showing other models'
colours on every 5s refresh. */
function renderStageFlow(pg,k,maxReplicas){const flow=el('stages');const cards=uiState.stageCards;if(cards&&flow.firstChild){stages.forEach(stage=>updateStageCard(cards[stage],stage,pg,k,maxReplicas));renderButtonsDisabled();return}const built={generate:stageCard('generate',pg,k,maxReplicas,true),validate:stageCard('validate',pg,k,maxReplicas,true),repair:stageCard('repair',pg,k,maxReplicas,true),reward:stageCard('reward',pg,k,maxReplicas,true),push:stageCard('push',pg,k,maxReplicas,true)};uiState.stageCards=built;const validationLoop=document.createElement('div');validationLoop.className='validation-loop';validationLoop.setAttribute('aria-label','NOP / Oracle and Repair retry group');const groupTitle=document.createElement('div');groupTitle.className='validation-loop-title';setText(groupTitle,'Validation / repair retry group');validationLoop.append(groupTitle,built.validate,built.repair);flow.replaceChildren(built.generate,validationLoop,built.reward,built.push)}
async function submitScale(stage,rawValue,maxReplicas){if(uiState.scaling)return;const feedback=el('scale-feedback');if(!Number.isSafeInteger(maxReplicas)||maxReplicas<0){feedback.className='bad';setText(feedback,'Cluster scaling capacity is unavailable; no change was made.');return}if(!/^\d+$/.test(rawValue)){feedback.className='bad';setText(feedback,`Worker total must be a whole number from 0 to ${maxReplicas}.`);return}const replicas=Number(rawValue);if(!Number.isSafeInteger(replicas)||replicas<0||replicas>maxReplicas){feedback.className='bad';setText(feedback,`Worker total must be between 0 and ${maxReplicas}.`);return}uiState.scaling=true;feedback.className='muted';setText(feedback,`Applying ${stageNames[stage]} total ${replicas}…`);renderButtonsDisabled();try{const response=await fetch('/api/pipeline/scale',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRF-Token':csrfToken},body:JSON.stringify({stage,replicas})});const body=await response.json();if(!response.ok)throw new Error(body.error||`HTTP ${response.status}`);delete uiState.scaleDrafts[stage];feedback.className='ok';setText(feedback,`${stageNames[stage]} configured for ${replicas} workers.`);if(body.status)render(body.status);else await poll()}catch(error){feedback.className='bad';setText(feedback,`Scaling failed: ${error.message}`)}finally{uiState.scaling=false;renderButtonsDisabled()}}
async function submitBuildSlots(node,rawValue,maxSlots){if(uiState.buildSlotUpdating)return;const feedback=el('build-slot-feedback');if(!Number.isSafeInteger(maxSlots)||maxSlots<1){feedback.className='bad';setText(feedback,'Node BuildKit slot capacity is unavailable; no change was made.');return}if(!/^\d+$/.test(rawValue)){feedback.className='bad';setText(feedback,`BuildKit slots must be a whole number from 1 to ${maxSlots}.`);return}const slots=Number(rawValue);if(!Number.isSafeInteger(slots)||slots<1||slots>maxSlots){feedback.className='bad';setText(feedback,`BuildKit slots must be between 1 and ${maxSlots}.`);return}uiState.buildSlotUpdating=true;feedback.className='muted';setText(feedback,`Applying ${slots} local BuildKit slots on ${node}…`);renderButtonsDisabled();try{const response=await fetch('/api/pipeline/build-slots',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRF-Token':csrfToken},body:JSON.stringify({node,slots})});const body=await response.json();if(!response.ok)throw new Error(body.error||`HTTP ${response.status}`);delete uiState.buildSlotDrafts[node];feedback.className='ok';setText(feedback,`${node} configured for ${slots} local BuildKit slots; in-flight builds on retired slots finish normally.`);if(body.status)render(body.status);else await poll()}catch(error){feedback.className='bad';setText(feedback,`BuildKit slot update failed: ${error.message}`)}finally{uiState.buildSlotUpdating=false;renderButtonsDisabled()}}
function renderButtonsDisabled(){document.querySelectorAll('.scale-controls button').forEach(button=>{button.disabled=uiState.scaling||button.dataset.capacityAvailable!=='true'});document.querySelectorAll('.slot-controls button').forEach(button=>{button.disabled=uiState.buildSlotUpdating||button.dataset.controllerAvailable!=='true'})}
function renderResourcesV2(metrics,clusterNodes){
 const status=el('resource-status'),summary=el('resource-summary'),body=el('resource-nodes');summary.replaceChildren();body.replaceChildren();
 if(!metrics?.available){status.className='bad';setText(status,`Resource metrics unavailable${metrics?.error?`: ${friendlyError(metrics.error)}`:''}`)}else if(metrics.stale){status.className='bad';setText(status,`Showing metrics collected ${relativeAge(metrics.collected_at)}; refresh failed${metrics.error?` (${friendlyError(metrics.error)})`:''}`)}else if(metrics.allocation_error){status.className='bad';setText(status,`Live usage metrics; CPU allocation unavailable: ${friendlyError(metrics.allocation_error)}`)}else{status.className='muted';setText(status,`Live metrics collected ${relativeAge(metrics.collected_at)}`)}
 const aggregate=metrics?.aggregate||{};
 const cpuCard=document.createElement('div');cpuCard.className='card';cpuCard.title='Actual metrics usage / sum of Kubernetes CPU requests / cluster allocatable CPU. Both percentages use allocatable CPU as denominator.';const cpuTitle=document.createElement('b');setText(cpuTitle,'CPU used / allocated / allocatable');const cpuValue=document.createElement('div');cpuValue.className='big';setText(cpuValue,formatCpuTriple(aggregate.cpu_used_millicores,aggregate.cpu_allocated_millicores,aggregate.cpu_allocatable_millicores));cpuCard.append(cpuTitle,cpuValue);summary.append(cpuCard);
 const memoryCard=document.createElement('div');memoryCard.className='card';const memoryTitle=document.createElement('b');setText(memoryTitle,'Memory utilization');const memoryValue=document.createElement('div');memoryValue.className='big';setText(memoryValue,aggregate.memory_percent==null?'—':`${aggregate.memory_percent.toFixed(1)}%`);const memoryDetail=document.createElement('div');setText(memoryDetail,`${memory(aggregate.memory_used_bytes)} / ${memory(aggregate.memory_allocatable_bytes)}`);memoryCard.append(memoryTitle,memoryValue,memoryDetail);summary.append(memoryCard);
 const workloadByName=Object.fromEntries((clusterNodes||[]).map(node=>[node.name,node]));
 (metrics?.nodes||[]).forEach(node=>{const workload=workloadByName[node.name]||{};const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=5;const details=document.createElement('details');details.className='node-details';details.dataset.nodeKey=node.name;details.open=uiState.expandedNodes.has(node.name);details.addEventListener('toggle',()=>{if(details.open)uiState.expandedNodes.add(node.name);else uiState.expandedNodes.delete(node.name)});const rowSummary=document.createElement('summary');rowSummary.className='node-summary';const identity=document.createElement('span');const stageCounts=Object.entries(workload.pods_by_stage||{}).map(([stage,count])=>`${stageNames[stage]||stage} ${count}`).join(' · ');setText(identity,`${node.name}${node.ip?` / ${node.ip}`:''} — ${workload.pod_count||0} pods${stageCounts?` (${stageCounts})`:''}`);const cpuCell=document.createElement('span');cpuCell.title='Actual metrics usage / sum of Kubernetes CPU requests / node allocatable CPU. Both percentages use allocatable CPU as denominator.';setText(cpuCell,formatCpuTriple(node.cpu_used_millicores,node.cpu_allocated_millicores,node.cpu_allocatable_millicores));const memoryCell=document.createElement('span');setText(memoryCell,node.available?`${memory(node.memory_used_bytes)} / ${memory(node.memory_allocatable_bytes)} (${node.memory_percent?.toFixed(1)??'—'}%)`:'memory unavailable');const diskCell=document.createElement('span');diskCell.className=(node.disk_io?.busy_percent??0)>=80?'bad':'';diskCell.title=node.disk_io?.error||'30-second cAdvisor rate sample';setText(diskCell,formatDiskIo(node.disk_io));const slotCell=buildSlotControls(workload,node.build_slots);rowSummary.append(identity,cpuCell,memoryCell,diskCell,slotCell);const list=document.createElement('div');list.className='pod-list';if(!(workload.pods||[]).length){const empty=document.createElement('span');empty.className='muted';setText(empty,'No pipeline pods scheduled on this node.');list.append(empty)}else{workload.pods.forEach(pod=>{const podRow=document.createElement('div');podRow.className='pod-row';[pod.name,stageNames[pod.stage]||pod.stage,`${pod.phase}${pod.ready?' / Ready':' / NotReady'}`,`${pod.restarts} restarts`].forEach(value=>{const span=document.createElement('span');setText(span,value);podRow.append(span)});list.append(podRow)})}details.append(rowSummary,list);td.append(details);tr.append(td);body.append(tr)});
 const resourceScroll=el('resource-scroll');resourceScroll.scrollLeft=uiState.resourceScroll.left;resourceScroll.scrollTop=uiState.resourceScroll.top;
}
function renderStorage(storage){const warning=el('storage-warning'),summary=el('storage-summary'),mounts=el('storage-mounts');warning.replaceChildren();summary.replaceChildren();mounts.replaceChildren();if(storage?.warning){warning.className='warning';setText(warning,`⚠ ${storage.warning}`)}else{warning.className='';setText(warning,'')}[["Runtime workspace root",storage?.workspace_root||'unknown'],["Durable source of truth",storage?.source_of_truth||'unknown']].forEach(([label,value])=>{const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,label);const path=document.createElement('code');path.className='path';setText(path,value);card.append(title,path);summary.append(card)});(storage?.mounts||[]).forEach(mount=>{const tr=document.createElement('tr');[`${mount.deployment||'—'} / ${stageNames[mount.stage]||mount.stage||'—'}`,mount.node_ip||'unspecified',mount.mount_path||'—',mount.source_path||mount.kind||'—',mount.durability||'unknown'].forEach(value=>{const td=document.createElement('td');const code=document.createElement('code');code.className='path';setText(code,value);td.append(code);tr.append(td)});mounts.append(tr)})}
function farmCard(summary,label,value,detail){const card=document.createElement('div');card.className='card';const title=document.createElement('b');setText(title,label);const main=document.createElement('div');main.className='big';setText(main,value);const note=document.createElement('div');note.className='farm-detail';setText(note,detail);card.append(title,main,note);summary.append(card)}
function renderRemoteBuildKit(farm,tracking){const status=el('buildkit-farm-status'),warning=el('buildkit-farm-warning'),summary=el('buildkit-farm-summary'),diskBody=el('buildkit-farm-disk-io');warning.replaceChildren();summary.replaceChildren();diskBody.replaceChildren();const gateway=farm?.gateway||{},ready=farm?.ready||{},resources=farm?.resources||{};const resourceSampledAt=resources.last_success_at;const statusSampledAt=resourceSampledAt||ready.last_success_at||gateway.last_success_at;const sampleState=resources.error?'last successful sample':resources.available?'live sample':'resource sample unavailable';status.className=gateway.ok&&ready.ok&&!resources.error?'farm-status muted':'farm-status bad';setText(status,`${farm?.sampling?'Sampling farm; ':''}gateway ${gateway.status||'unknown'} · readiness ${ready.status||'unknown'} · ${statusSampledAt?`sampled ${statusSampledAt}`:'awaiting first sample'} · ${farm?.poll_interval_seconds||30}s minimum poll`);if(resources.schema_warning||resources.error){warning.className='warning';setText(warning,resources.error||resources.schema_warning)}else{warning.className='';setText(warning,'')}farmCard(summary,'Gateway / ready',`${gateway.ok?'up':'down'} / ${ready.ok?'ready':'not ready'}`,`HTTP ${gateway.http_status??'—'} / ${ready.http_status??'—'}`);const available=resources.available_backend_count==null?'—':resources.available_backend_count;const sampled=resources.backend_count??resources.sampled_worker_count??0;const scope=resources.is_global?'global aggregate':`${resources.scope||'worker-local sample'}${resources.sampled_worker?` · ${resources.sampled_worker}`:''}`;const sampleDetail=`${scope} · ${resourceSampledAt?`${sampleState} ${resourceSampledAt}`:sampleState}`;farmCard(summary,'Backend workers',`${sampled} sampled / ${available} available`,sampleDetail);const queueLabel=resources.is_global?'Farm live queue':'Sampled live queue';farmCard(summary,queueLabel,resources.queue_length==null?'unavailable':`${resources.queue_length} / ${resources.queue_capacity??'—'}`,sampleDetail);const activeLabel=resources.is_global?'Farm live active':'Sampled live active';farmCard(summary,activeLabel,resources.running_builds==null?'unavailable':`${resources.running_builds} running`,`${resources.inflight_builds??'—'} inflight · ${sampleDetail}`);const recentCounts=tracking?.recent_status_counts||{};const recentBreakdown=Object.entries(recentCounts).filter(([,count])=>count>0).map(([name,count])=>`${name} ${count}`).join(' · ');const recentWindow=secs(tracking?.recent_window_seconds);const ledgerDetail=tracking?.available?`${recentBreakdown||'no recent nonterminal records'} · ${tracking?.stale??0} stale excluded · database submission ledger only, not live farm state · updated within ${recentWindow}${tracking?.latest_updated_at?` · latest ${tracking.latest_updated_at}`:''}`:'submission tracking unavailable';farmCard(summary,'SWEgen submission ledger',tracking?.available?`${tracking?.recent??0} recent records`:'unavailable',ledgerDetail);const diskRows=resources.node_disk_io||[];if(!diskRows.length){const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=4;setText(td,'Remote per-node disk I/O telemetry is not exposed by the current worker-local API sample.');tr.append(td);diskBody.append(tr)}else{diskRows.forEach(row=>{const tr=document.createElement('tr');const busy=row.busy_percent==null?'—':`${row.busy_percent.toFixed(1)}%`;[[row.node||'unknown'],[`${rateBytes(row.read_bytes_per_second)} / ${rateBytes(row.write_bytes_per_second)}`],[`${rateOps(row.read_iops)} / ${rateOps(row.write_iops)}`],[`${busy} / ${row.io_current??'—'} inflight`]].forEach(([value])=>{const td=document.createElement('td');setText(td,value);tr.append(td)});diskBody.append(tr)})}}
function stageHourlyYield(rows,stage){const wrap=document.createElement('div');wrap.className='stage-yield';const title=document.createElement('div');title.className='stage-yield-title';const label=document.createElement('span');setText(label,'Hourly yield · last 12h');const note=document.createElement('span');setText(note,'success / terminal');title.append(label,note);wrap.append(title);const list=document.createElement('div');list.className='yield-list';if(!(rows||[]).length){const empty=document.createElement('div');empty.className='yield-empty';setText(empty,'No terminal outcomes in the last 12 hours');wrap.append(empty);wrap._yieldCells=[];wrap._yieldRowCount=0;return wrap}const yieldCells=[];rows.forEach(row=>{const line=document.createElement('div');line.className='yield-row';const stamp=document.createElement('span');setText(stamp,new Date(row.bucket).toLocaleString([],{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}));const value=document.createElement('span');value.className='yield-value';setText(value,`${row.succeeded} / ${row.processed}`);const percent=document.createElement('span');percent.className='yield-percent';setText(percent,row.yield_percent==null?'—':`${row.yield_percent.toFixed(1)}%`);line.append(stamp,value,percent);list.append(line);yieldCells.push({stamp,value,percent})});wrap._yieldCells=yieldCells;wrap._yieldRowCount=rows.length;wrap.append(list);return wrap}
/* The hourly-yield panel has one row per hour, so its row COUNT only changes when
an hour rolls over. Same count -> rewrite the three spans per row in place; a
changed count rebuilds (the caller swaps the returned node in). */
function updateStageHourlyYield(wrap,rows,stage){const list=rows||[];if(!wrap||wrap._yieldRowCount!==list.length||!wrap._yieldCells)return stageHourlyYield(list,stage);wrap._yieldCells.forEach((cells,index)=>{const row=list[index];if(!row)return;setText(cells.stamp,new Date(row.bucket).toLocaleString([],{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}));setText(cells.value,`${row.succeeded} / ${row.processed}`);setText(cells.percent,row.yield_percent==null?'—':`${row.yield_percent.toFixed(1)}%`)});return wrap}
function renderSwrPushSync(sync){const status=el('swr-sync-status'),summary=el('swr-sync-summary'),body=el('swr-sync-instances');summary.replaceChildren();body.replaceChildren();if(!sync?.available){status.className='bad';setText(status,'SWR push-registry sync unavailable: public.pushed_images is absent or unreadable.');const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=3;setText(td,'No push-registry sync data.');tr.append(td);body.append(tr);return}const outOfSync=sync.out_of_sync_total||0;status.className=outOfSync?'bad':'muted';setText(status,`${outOfSync} images out of sync${sync.out_of_sync_list_truncated?` · showing first ${sync.out_of_sync_list_limit}`:''} · in sync ${sync.in_sync||0}`);farmCard(summary,'Pushed to -platform',compactChartCount(sync.platform_count||0),`${sync.platform_count||0} distinct images on data-platform`);farmCard(summary,'Pushed to -trajectory',compactChartCount(sync.trajectory_count||0),`${sync.trajectory_count||0} distinct images on data-trajectory`);farmCard(summary,'Platform only (missing -trajectory)',compactChartCount(sync.platform_only||0),'pushed to platform but not trajectory');farmCard(summary,'Trajectory only (missing -platform)',compactChartCount(sync.trajectory_only||0),'pushed to trajectory but not platform');const rows=sync.out_of_sync_instances||[];if(!rows.length){const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=3;setText(td,'All images are in sync across both registries.');tr.append(td);body.append(tr)}else{rows.forEach(row=>{const tr=document.createElement('tr');const pushedTo=row.registry==='platform'?'data-platform':'data-trajectory';const missingFrom=row.registry==='platform'?'data-trajectory':'data-platform';[[row.instance],[pushedTo],[missingFrom]].forEach(([value])=>{const td=document.createElement('td');const code=document.createElement('code');code.className='path';setText(code,value);td.append(code);tr.append(td)});body.append(tr)})}}
async function postEndpoint(path,body,pendingMessage,successMessage){if(uiState.endpointBusy)return;const feedback=el('endpoint-feedback');uiState.endpointBusy=true;feedback.className='muted';setText(feedback,pendingMessage);renderEndpointButtonsDisabled();try{const response=await fetch(path,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRF-Token':csrfToken},body:JSON.stringify(body)});const payload=await response.json();if(!response.ok)throw new Error(payload.error||`HTTP ${response.status}`);feedback.className='ok';setText(feedback,successMessage(payload));await poll();return payload}catch(error){feedback.className='bad';setText(feedback,`Endpoint action failed: ${error.message}`)}finally{uiState.endpointBusy=false;renderEndpointButtonsDisabled()}}
function renderEndpointButtonsDisabled(){document.querySelectorAll('#endpoint-form button,.endpoint-actions button').forEach(button=>{button.disabled=uiState.endpointBusy})}
async function submitEndpointRegister(){const url=el('endpoint-url').value.trim(),model=el('endpoint-model').value.trim(),token=el('endpoint-token').value,concurrency=Number(el('endpoint-concurrency').value||'0');const feedback=el('endpoint-feedback');if(!url||!model||!token){feedback.className='bad';setText(feedback,'Endpoint URL, model ID and bearer token are all required.');return}const payload=await postEndpoint('/api/generate/endpoints',{base_url:url,model_id:model,auth_token:token,concurrency},`Registering ${model}…`,body=>`Registered ${model} as ${body.slug}.`);if(payload){el('endpoint-token').value='';el('endpoint-url').value='';el('endpoint-model').value=''}}
function submitEndpointScale(slug,model){const raw=prompt(`Target concurrency (pods) for ${model||slug}`);if(raw==null)return;if(!/^\d+$/.test(raw.trim())){const feedback=el('endpoint-feedback');feedback.className='bad';setText(feedback,'Concurrency must be a whole number.');return}postEndpoint('/api/generate/endpoints/scale',{slug,concurrency:Number(raw.trim())},`Scaling ${slug}…`,body=>`${slug} target concurrency set to ${body.concurrency}.`)}
function submitEndpointEdit(slug,model){const url=prompt(`New endpoint URL for ${model||slug} (blank to keep)`,'');if(url==null)return;const newModel=prompt(`New model ID for ${model||slug} (blank to keep)`,'');if(newModel==null)return;const token=prompt(`New bearer token for ${model||slug} (blank to keep)`,'');if(token==null)return;const body={slug};if(url.trim())body.base_url=url.trim();if(newModel.trim())body.model_id=newModel.trim();if(token)body.auth_token=token;if(!body.base_url&&!body.model_id&&!body.auth_token){const feedback=el('endpoint-feedback');feedback.className='muted';setText(feedback,'No changes entered.');return}postEndpoint('/api/generate/endpoints/update',body,`Updating ${slug} API…`,()=>`${slug} API updated.`)}
function clearResetError(){const box=el('endpoint-reset-error');if(box)box.replaceChildren()}
function renderResetError(slug,text){const box=el('endpoint-reset-error');if(!box)return;box.replaceChildren();const wrap=document.createElement('div');wrap.className='reset-error-box';const head=document.createElement('div');head.className='reset-error-head';const label=document.createElement('span');label.className='bad';setText(label,`${slug} reset probe failed — breaker kept latched. Full endpoint error:`);const copyBtn=document.createElement('button');copyBtn.type='button';copyBtn.className='reset-error-copy';setText(copyBtn,'Copy');copyBtn.addEventListener('click',()=>{ta.select();try{navigator.clipboard.writeText(ta.value)}catch(e){document.execCommand('copy')};setText(copyBtn,'Copied');setTimeout(()=>setText(copyBtn,'Copy'),1500)});head.append(label,copyBtn);const ta=document.createElement('textarea');ta.className='reset-error-text';ta.readOnly=true;ta.value=text||'';wrap.append(head,ta);box.append(wrap)}
async function submitEndpointReset(slug){const payload=await postEndpoint('/api/generate/endpoints/reset',{slug},`Probing ${slug}…`,body=>body.unlatched?`${slug} probe OK (HTTP ${body.probe_status??'—'}); breaker cleared.`:`${slug} probe failed (HTTP ${body.probe_status||'unreachable'}); breaker kept latched.`);if(payload&&!payload.unlatched){const feedback=el('endpoint-feedback');feedback.className='bad';renderResetError(slug,payload.error_text||payload.detail||'')}else if(payload&&payload.unlatched){clearResetError()}}
function submitEndpointDelete(slug,model){if(!confirm(`Delete endpoint ${model||slug}? This removes its Deployment and all its pods.`))return;postEndpoint('/api/generate/endpoints/delete',{slug},`Deleting ${slug}…`,()=>`${slug} deleted; controller will remove its pods.`)}
function renderEndpoints(section,podCounts){const body=el('endpoints');body.replaceChildren();const endpoints=section?.endpoints||[];if(!section?.available){const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=6;td.className='muted';setText(td,'Endpoint registry unavailable (schema not migrated).');tr.append(td);body.append(tr);return}if(!endpoints.length){const tr=document.createElement('tr');const td=document.createElement('td');td.colSpan=6;td.className='muted';setText(td,'No generate endpoints registered.');tr.append(td);body.append(tr);return}endpoints.forEach(ep=>{const running=(podCounts&&podCounts[ep.slug])??ep.running??0;const tr=document.createElement('tr');const modelCell=document.createElement('td');setText(modelCell,ep.model_id||ep.slug);const hostCell=document.createElement('td');setText(hostCell,ep.host||ep.base_url||'—');const targetCell=document.createElement('td');setText(targetCell,`${ep.concurrency??0} / ${running}`);const outcomeCell=document.createElement('td');const s=document.createElement('span');s.className='ok';setText(s,ep.recent_succeeded??0);const sep=document.createElement('span');setText(sep,' / ');const f=document.createElement('span');f.className='bad';setText(f,ep.recent_failed??0);outcomeCell.append(s,sep,f);const breakerCell=document.createElement('td');if(ep.breaker_open){breakerCell.className='endpoint-breaker-latched';const probe=ep.last_probe_status!=null?` · last HTTP ${ep.last_probe_status}`:'';setText(breakerCell,`LATCHED${ep.breaker_reason?` — ${ep.breaker_reason}`:''}${probe}`)}else{breakerCell.className='endpoint-breaker-ok';setText(breakerCell,ep.enabled?'OK':'disabled')}const actionsCell=document.createElement('td');const actions=document.createElement('div');actions.className='endpoint-actions';const scaleBtn=document.createElement('button');scaleBtn.type='button';setText(scaleBtn,'Scale');scaleBtn.addEventListener('click',()=>submitEndpointScale(ep.slug,ep.model_id));const editBtn=document.createElement('button');editBtn.type='button';setText(editBtn,'Edit API');editBtn.addEventListener('click',()=>submitEndpointEdit(ep.slug,ep.model_id));actions.append(scaleBtn,editBtn);if(ep.breaker_open){const resetBtn=document.createElement('button');resetBtn.type='button';setText(resetBtn,'Reset');resetBtn.addEventListener('click',()=>submitEndpointReset(ep.slug));actions.append(resetBtn)}const deleteBtn=document.createElement('button');deleteBtn.type='button';deleteBtn.className='danger';setText(deleteBtn,'Delete');deleteBtn.addEventListener('click',()=>submitEndpointDelete(ep.slug,ep.model_id));actions.append(deleteBtn);actionsCell.append(actions);tr.append(modelCell,hostCell,targetCell,outcomeCell,breakerCell,actionsCell);body.append(tr)});renderEndpointButtonsDisabled()}
function render(data){captureUiState();const took=uiState.lastPollMs==null?'':` · last fetch ${(uiState.lastPollMs/1000).toFixed(1)}s`;el('stamp').textContent=`Updated ${data.generated_at||'—'} · refreshes ${POLL_INTERVAL_MS/1000}s after each response${took}`; el('errors').replaceChildren();
 Object.entries(data.sources||{}).forEach(([n,s])=>{if(!s.ok){const d=document.createElement('div');d.className='banner bad';setText(d,`${n} unavailable: ${s.error}`);el('errors').append(d)}});
 const pg=data.postgres||{}, k=data.k3s||{},maxReplicas=k.scaling?.max_replicas;renderStageFlow(pg,k,maxReplicas);
 renderResourcesV2(k.resource_metrics,k.nodes);
 renderStorage(k.storage);
 renderRemoteBuildKit(data.buildkit_farm||{},pg.remote_builds||{});
 renderSwrPushSync(pg.swr_push_sync||{});
 renderEndpoints(pg.generate_endpoints,k.generate_endpoint_pods||{});
 el('tasks').replaceChildren();(pg.tasks||[]).forEach(task=>{const taskKey=`${task.task_id}:${task.task_version}`;const tr=document.createElement('tr');const timeline=(task.stages||[]).map(s=>`${s.stage}: ${s.state} wait ${secs(s.wait_seconds)} run ${secs(s.run_seconds)}${s.worker_id?' @ '+s.worker_id:''}`).join('\n');[task.task_id,task.state,task.current_stage,secs(task.total_elapsed_seconds)].forEach(v=>{const td=document.createElement('td');setText(td,v);tr.append(td)});const storage=task.storage||{};const storageCell=document.createElement('td');const runtimePath=document.createElement('code');runtimePath.className='path';setText(runtimePath,storage.runtime_path_pattern||'No runtime path recorded');const storageNote=document.createElement('div');storageNote.className='storage-note';setText(storageNote,`${storage.generated_on_node?`node ${storage.generated_on_node} · `:''}${storage.runtime_directory_state||'unknown lifecycle'} · PostgreSQL: ${storage.stored_file_count||0} files / ${bytes(storage.stored_bytes||0)}`);storageCell.append(runtimePath,storageNote);tr.append(storageCell);const td=document.createElement('td');const details=document.createElement('details');details.dataset.taskKey=taskKey;details.open=uiState.expandedTasks.has(taskKey);details.addEventListener('toggle',()=>{if(details.open)uiState.expandedTasks.add(taskKey);else uiState.expandedTasks.delete(taskKey)});const detailsSummary=document.createElement('summary');setText(detailsSummary,'show');const pre=document.createElement('pre');setText(pre,timeline);details.append(detailsSummary,pre);td.append(details);tr.append(td);el('tasks').append(tr)});
}
/* Self-scheduling poll loop. setInterval(poll,5000) fired regardless of whether
the previous request had come back, so a 4-7s response meant overlapping in-flight
requests that queued up and compounded the latency. The guard flag makes a poll a
no-op while one is already running, and the next tick is scheduled only AFTER the
current one settles: the cadence is POLL_INTERVAL_MS between the end of one
response and the start of the next, so it stays ~5s when the server is fast and
degrades gracefully (never overlapping) when it is slow. */
const POLL_INTERVAL_MS=5000;let pollTimer=null;
async function poll(){if(uiState.polling)return;uiState.polling=true;const started=Date.now();try{const r=await fetch(`/api/pipeline/status?range_hours=${uiState.rangeHours}`,{cache:'no-store'});if(!r.ok)throw Error(`HTTP ${r.status}`);uiState.lastPollMs=Date.now()-started;render(await r.json())}catch(e){el('stamp').textContent=`Dashboard fetch failed: ${e}`}finally{uiState.polling=false}}
function scheduleNextPoll(){if(pollTimer!==null)clearTimeout(pollTimer);pollTimer=setTimeout(pollLoop,POLL_INTERVAL_MS)}
async function pollLoop(){await poll();scheduleNextPoll()}
el('endpoint-form').addEventListener('submit',event=>{event.preventDefault();submitEndpointRegister()});
{const rangeSelect=el('range-hours');if(rangeSelect){rangeSelect.value=String(uiState.rangeHours);rangeSelect.addEventListener('change',()=>{const value=Number(rangeSelect.value);uiState.rangeHours=RANGE_HOURS_ALLOWED.includes(value)?value:24;/* Restart the loop so the new range is fetched now and the next tick is measured from this response, instead of racing the pending timer. */scheduleNextPoll();pollLoop()})}}
pollLoop();
</script></main></body></html>"""


def make_handler(
    cache: SnapshotCache,
    scaler: K3sScaler,
    build_slot_controller: K3sBuildSlotController,
    csrf_token: str,
    endpoint_registry: GenerateEndpointRegistry | None = None,
) -> type[BaseHTTPRequestHandler]:
    endpoint_registry = endpoint_registry or GenerateEndpointRegistry()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            route = parsed.path
            if route == "/":
                body = HTML.replace("__CSRF_TOKEN__", csrf_token).encode()
                self._send(200, "text/html; charset=utf-8", body)
            elif route == "/api/pipeline/status":
                range_hours = _parse_range_hours(parsed.query)
                body = json.dumps(cache.snapshot(range_hours=range_hours)).encode()
                self._send(200, "application/json", body)
            elif route == "/healthz":
                self._send(200, "application/json", b'{"ok":true}')
            else:
                self._send(404, "application/json", b'{"error":"not found"}')

        def do_POST(self) -> None:
            if self.path not in {
                "/api/pipeline/scale",
                "/api/pipeline/build-slots",
                "/api/generate/endpoints",
                "/api/generate/endpoints/scale",
                "/api/generate/endpoints/update",
                "/api/generate/endpoints/reset",
                "/api/generate/endpoints/delete",
            }:
                self._send_json(404, {"error": "not found"})
                return
            if not self._request_is_same_origin(csrf_token):
                self._send_json(403, {"error": "invalid origin or CSRF token"})
                return
            if self.headers.get_content_type() != "application/json":
                self._send_json(415, {"error": "Content-Type must be application/json"})
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json(400, {"error": "invalid Content-Length"})
                return
            if not 0 < content_length <= 1024:
                self._send_json(400, {"error": "request body must be between 1 and 1024 bytes"})
                return
            try:
                payload = json.loads(self.rfile.read(content_length))
                if not isinstance(payload, dict):
                    raise ValueError("request body must be a JSON object")
                current_status = cache.snapshot()
                max_replicas = (
                    current_status.get("k3s", {}).get("scaling", {}).get("max_replicas")
                )
                if self.path == "/api/pipeline/scale":
                    stage = payload.get("stage")
                    replicas = payload.get("replicas")
                    applied: object = scaler.scale(
                        stage,
                        replicas,
                        max_replicas=max_replicas,
                    )
                    response_fields = {
                        "stage": stage,
                        "replicas": replicas,
                        "applied": applied,
                    }
                elif self.path == "/api/pipeline/build-slots":
                    node = payload.get("node")
                    slots = payload.get("slots")
                    applied = build_slot_controller.update(
                        node,
                        slots,
                        nodes=current_status.get("k3s", {}).get("nodes"),
                    )
                    response_fields = {
                        "node": node,
                        "slots": slots,
                        "applied": applied,
                    }
                elif self.path == "/api/generate/endpoints":
                    response_fields = endpoint_registry.register(
                        base_url=payload.get("base_url"),
                        model_id=payload.get("model_id"),
                        auth_token=payload.get("auth_token"),
                        concurrency=payload.get("concurrency"),
                        max_replicas=max_replicas,
                    )
                elif self.path == "/api/generate/endpoints/scale":
                    response_fields = endpoint_registry.scale(
                        slug=payload.get("slug"),
                        concurrency=payload.get("concurrency"),
                        max_replicas=max_replicas,
                    )
                elif self.path == "/api/generate/endpoints/update":
                    response_fields = endpoint_registry.update(
                        slug=payload.get("slug"),
                        base_url=payload.get("base_url"),
                        model_id=payload.get("model_id"),
                        auth_token=payload.get("auth_token"),
                    )
                elif self.path == "/api/generate/endpoints/reset":
                    response_fields = endpoint_registry.reset(slug=payload.get("slug"))
                else:  # /api/generate/endpoints/delete
                    response_fields = endpoint_registry.delete(slug=payload.get("slug"))
            except (BuildSlotBusyError, ScalingBusyError, EndpointConflictError) as error:
                self._send_json(409, {"error": str(error)})
                return
            except EndpointNotFoundError as error:
                self._send_json(404, {"error": str(error)})
                return
            except (json.JSONDecodeError, ValueError) as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                self._send_json(502, {"error": f"control request failed: {str(error)[:500]}"})
                return
            cache.refresh()
            self._send_json(
                200,
                {
                    "ok": True,
                    **response_fields,
                    "status": cache.snapshot(),
                },
            )

        def _request_is_same_origin(self, expected_token: str) -> bool:
            supplied_token = self.headers.get("X-CSRF-Token", "")
            if not hmac.compare_digest(supplied_token, expected_token):
                return False
            origin = self.headers.get("Origin", "")
            host = self.headers.get("Host", "")
            if not origin or not host:
                return False
            parsed = urlsplit(origin)
            return parsed.scheme in {"http", "https"} and parsed.netloc == host

        def _send_json(self, status: int, value: dict[str, Any]) -> None:
            self._send(status, "application/json", json.dumps(value).encode())

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def serve(host: str = "0.0.0.0", port: int = 8766) -> None:
    cache = SnapshotCache()
    scaler = K3sScaler()
    build_slot_controller = K3sBuildSlotController()
    endpoint_registry = GenerateEndpointRegistry()
    csrf_token = secrets.token_urlsafe(32)
    cache.refresh()
    thread = threading.Thread(target=cache.run, name="dashboard-refresh", daemon=True)
    thread.start()
    server = ThreadingHTTPServer(
        (host, port),
        make_handler(cache, scaler, build_slot_controller, csrf_token, endpoint_registry),
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        cache.stop()
        server.server_close()
        thread.join(timeout=2)
