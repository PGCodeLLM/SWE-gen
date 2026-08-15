"""Reconcile dynamic generate model-endpoint pools to their registry state.

An operator registers ``(base_url, model_id, bearer token, concurrency)`` tuples
in the ``generate_endpoints`` table (via the dashboard). This controller is the
reconcile loop that converges the cluster to that desired state:

* one Deployment ``swegen-generate-dyn-<slug>`` per enabled row, cloned from the
  static ``swegen-generate`` template but with the model endpoint/model/token
  injected as inline container env (no per-endpoint Secret);
* replicas driven to the row's ``concurrency``;
* a spec digest stamped as a Deployment annotation and compared each cycle, so
  an operator changing ``base_url`` / ``model_id`` / ``auth_token`` (or a roll of
  the static template) rolls the pool onto the new pod template instead of
  silently leaving the running pods on the credentials they started with;
* deleted / disabled / breaker-open rows scaled to zero (and their pods swept),
  removed rows' Deployments deleted;
* an active health probe per endpoint that latches a durable per-endpoint
  breaker on HTTP 5xx/429 (real status, unlike the CLI's free-text task errors),
  scaling only that endpoint's Deployment to zero. A latch clears only on an
  explicit operator reset.

The controller only ever creates/patches/deletes Deployments whose name starts
with ``swegen-generate-dyn-`` so it can never touch the static stage pipeline.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import ssl
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from swegen import db

_SERVICE_ACCOUNT_ROOT = Path("/var/run/secrets/kubernetes.io/serviceaccount")

# Hard prefix guard: the controller must only ever manage its own dynamic
# Deployments, never the static stage deployments (swegen-generate, etc.).
DEPLOYMENT_PREFIX = "swegen-generate-dyn-"

# Annotation carrying the digest of the pod-defining part of the desired spec.
# The registry row owns real credentials, so "the Deployment already exists" is
# NOT the same as "the Deployment is correct": an operator changing base_url /
# model_id / auth_token used to leave the running pods on the old credentials
# forever (the DB was authoritative only at creation time). Stamping a digest on
# create and comparing it every cycle turns that silent divergence into a real
# template patch.
ENDPOINT_SPEC_DIGEST_ANNOTATION = "swegen.pgcode/endpoint-spec-digest"

# HTTP statuses from an endpoint that count as "endpoint unhealthy".
DEFAULT_UNHEALTHY_STATUSES = frozenset({429, 500, 502, 503, 504, 529})

# How long an endpoint may take to answer the health probe. Every prober reads
# this, so the controller's reconcile loop and the dashboard's Reset button
# cannot drift apart. Raise it for a gateway that is slow but working: probing
# faster than the endpoint can answer marks a healthy model unhealthy and
# latches its breaker while real requests are still succeeding.
ENDPOINT_PROBE_TIMEOUT_ENV = "SWEGEN_ENDPOINT_PROBE_TIMEOUT_SECONDS"
DEFAULT_PROBE_TIMEOUT_SECONDS = 20.0

# Router bodies that mean "this model has no backend", regardless of status code.
#
# The endpoints sit behind a LiteLLM router, which answers HTTP 400 -- not 5xx --
# when a model group has lost every healthy deployment:
#
#   400 litellm.BadRequestError: You passed in model=GLM-52_pre-train_256K.
#   There are no healthy deployments for this model.
#
# Treating that as "reachable and answering" cleared the breaker onto a model
# with no backend at all on 2026-08-14: each request 400s in ~1.2s, so 96 workers
# drained the queue at full speed with a 100% failure rate and nothing stopped
# them -- 1,616 failures and zero successes in ten minutes. The status code alone
# cannot distinguish that from a genuine client error, so the body is inspected.
_NO_BACKEND_BODY_MARKERS = (
    "no healthy deployments",
    "no deployments available",
)


def deployment_name_for(slug: str) -> str:
    return f"{DEPLOYMENT_PREFIX}{slug}"


# --------------------------------------------------------------------------- #
# Registry access
# --------------------------------------------------------------------------- #

_SELECT_ENDPOINTS_SQL = """
    SELECT slug, base_url, model_id, auth_token, concurrency, enabled,
           breaker_open, consecutive_fail
    FROM generate_endpoints
    ORDER BY slug
"""

_RECORD_PROBE_SQL = """
    UPDATE generate_endpoints
    SET last_probe_status = %s, last_probe_at = %s, consecutive_fail = %s,
        updated_at = now()
    WHERE slug = %s
"""

_TRIP_SQL = """
    UPDATE generate_endpoints
    SET breaker_open = TRUE, breaker_reason = %s, tripped_at = %s,
        consecutive_fail = %s, last_probe_status = %s, last_probe_at = %s,
        updated_at = now()
    WHERE slug = %s AND NOT breaker_open
"""

_INSERT_EVENT_SQL = """
    INSERT INTO generate_endpoint_events (slug, model_id, event, reason, detail)
    VALUES (%s, %s, %s, %s, %s)
"""


class Connection(Protocol):
    def execute(self, sql: str, params: Sequence[object] = ...) -> Any: ...
    def transaction(self) -> Any: ...


@dataclass(frozen=True)
class EndpointRow:
    slug: str
    base_url: str
    model_id: str
    auth_token: str
    concurrency: int
    enabled: bool
    breaker_open: bool
    consecutive_fail: int


def load_endpoints(connection: Connection) -> list[EndpointRow]:
    rows = list(connection.execute(_SELECT_ENDPOINTS_SQL).fetchall())
    endpoints: list[EndpointRow] = []
    for row in rows:
        # Support both tuple rows and dict_row.
        if isinstance(row, Mapping):
            endpoints.append(
                EndpointRow(
                    slug=row["slug"],
                    base_url=row["base_url"],
                    model_id=row["model_id"],
                    auth_token=row["auth_token"],
                    concurrency=int(row["concurrency"]),
                    enabled=bool(row["enabled"]),
                    breaker_open=bool(row["breaker_open"]),
                    consecutive_fail=int(row["consecutive_fail"]),
                )
            )
        else:
            endpoints.append(
                EndpointRow(
                    slug=row[0],
                    base_url=row[1],
                    model_id=row[2],
                    auth_token=row[3],
                    concurrency=int(row[4]),
                    enabled=bool(row[5]),
                    breaker_open=bool(row[6]),
                    consecutive_fail=int(row[7]),
                )
            )
    return endpoints


def record_probe(
    connection: Connection,
    slug: str,
    *,
    status: int | None,
    consecutive_fail: int,
    now: datetime,
) -> None:
    connection.execute(_RECORD_PROBE_SQL, (status, now, consecutive_fail, slug))


def trip_endpoint(
    connection: Connection,
    row: EndpointRow,
    *,
    status: int | None,
    reason: str,
    consecutive_fail: int,
    now: datetime,
) -> bool:
    """Latch this endpoint's breaker; returns True on the real transition."""

    result = connection.execute(
        _TRIP_SQL, (reason, now, consecutive_fail, status, now, row.slug)
    )
    tripped = getattr(result, "rowcount", 0) == 1
    if tripped:
        connection.execute(
            _INSERT_EVENT_SQL,
            (
                row.slug,
                row.model_id,
                "tripped",
                reason,
                json.dumps({"last_probe_status": status}),
            ),
        )
    return tripped


# --------------------------------------------------------------------------- #
# Endpoint health probe
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    status: int | None
    detail: str
    # Full, copyable, multi-line error text for a failed probe: the request line,
    # the HTTP status, the probed URL, and (on an HTTPError) the response body.
    # Empty on success. Kept separate from ``detail`` (the terse one-line summary
    # used in DB trip reasons) and defaulted so existing callers/tests still work.
    error_text: str = ""


def _seed_no_proxy_from_swegen() -> None:
    """Populate no_proxy/NO_PROXY from SWEGEN_NO_PROXY if the standard vars are unset.

    The pod carries the no-proxy host list only under SWEGEN_NO_PROXY (a
    configMap key); urllib's proxy-bypass logic reads no_proxy/NO_PROXY. Seeding
    the standard vars lets a default opener route internal endpoints direct and
    external ones through the proxy. A no-op when SWEGEN_NO_PROXY is empty or a
    standard var is already set (the deploy manifest maps no_proxy explicitly).
    """

    swegen_no_proxy = os.environ.get("SWEGEN_NO_PROXY", "").strip()
    if not swegen_no_proxy:
        return
    for name in ("no_proxy", "NO_PROXY"):
        if not os.environ.get(name):
            os.environ[name] = swegen_no_proxy


class EndpointProber(Protocol):
    def probe(self, base_url: str, model_id: str, token: str) -> ProbeResult: ...


class HttpEndpointProber:
    """Probe a model endpoint's Anthropic messages API for real HTTP health."""

    def __init__(self, *, timeout_seconds: float | None = None) -> None:
        # Default from the same env var the controller reads, so the dashboard's
        # Reset button and the controller's reconcile loop agree on how long an
        # endpoint may take to answer. They disagreed before: the controller was
        # raised to 300s for a gateway that answers in 45-70s, while the
        # dashboard kept the 20s literal, so Reset timed out against an endpoint
        # the controller considered healthy and the breaker could never clear.
        if timeout_seconds is None:
            timeout_seconds = float(
                os.environ.get(ENDPOINT_PROBE_TIMEOUT_ENV, DEFAULT_PROBE_TIMEOUT_SECONDS)
            )
        self._timeout = timeout_seconds
        # Registered endpoints are a MIX of hosts: internal ones (e.g. 7.244.x on
        # the cluster network) are reachable ONLY directly and 504 through the
        # corporate proxy, while external ones (public IPs) are reachable ONLY
        # through the proxy the controller pod inherits via swegen-runtime-proxy.
        # So the probe must honour the no_proxy list per-host, exactly like the
        # generate workers reaching the same endpoints -- an unconditional proxy
        # (or unconditional bypass) falsely trips one side or the other.
        #
        # urllib's proxy bypass reads the standard no_proxy/NO_PROXY env, but the
        # pod only carries SWEGEN_NO_PROXY (a configMap key); seed the standard
        # var from it so a default build_opener() (which installs
        # ProxyHandler(getproxies()) and checks proxy_bypass per request) routes
        # internal->direct and external->proxy. The deploy manifest also maps
        # no_proxy for the same reason; this is a defensive fallback.
        _seed_no_proxy_from_swegen()
        self._opener = urllib.request.build_opener()

    def probe(self, base_url: str, model_id: str, token: str) -> ProbeResult:
        url = base_url.rstrip("/") + "/v1/messages"
        payload = json.dumps(
            {
                "model": model_id,
                "max_tokens": 8,
                "messages": [{"role": "user", "content": "ping"}],
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                "content-type": "application/json",
                "x-api-key": token,
                "authorization": f"Bearer {token}",
                "anthropic-version": "2023-06-01",
            },
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                response.read(256)
                return ProbeResult(ok=True, status=response.status, detail="ok")
        except urllib.error.HTTPError as error:
            try:
                body = error.read().decode("utf-8", "replace")[:4000]
            except Exception:
                body = ""
            # A 4xx that is not 429 (e.g. 401) means the endpoint is reachable
            # and answering, so only 429 + 5xx are unhealthy by status alone.
            # The exception is a router reporting that the model group has no
            # backend: that arrives as a 400 but means the model cannot serve a
            # single request, which is strictly worse than a 5xx blip.
            healthy = error.code not in DEFAULT_UNHEALTHY_STATUSES and not any(
                marker in body.lower() for marker in _NO_BACKEND_BODY_MARKERS
            )
            error_text = self._redact(
                f"POST {url} -> HTTP {error.code}\n{body}".rstrip(), token
            )
            return ProbeResult(
                ok=healthy,
                status=error.code,
                detail=f"http {error.code}",
                error_text="" if healthy else error_text,
            )
        except urllib.error.URLError as error:
            error_text = self._redact(f"POST {url} -> unreachable: {error.reason}", token)
            return ProbeResult(
                ok=False,
                status=None,
                detail=f"unreachable: {error.reason}",
                error_text=error_text,
            )
        except TimeoutError:
            error_text = self._redact(
                f"POST {url} -> timeout after {self._timeout:g}s", token
            )
            return ProbeResult(
                ok=False, status=None, detail="timeout", error_text=error_text
            )

    @staticmethod
    def _redact(text: str, token: str) -> str:
        """Scrub the probe token and known secret patterns from captured text.

        The URL and response body should not contain the bearer token, but this
        defensively removes any literal occurrence of it (and applies the repo's
        standard secret redaction) before the text is surfaced to an operator.
        """

        from swegen.create.claude_code_utils import redact_sensitive_text

        if token:
            text = text.replace(token, "<REDACTED>")
        return redact_sensitive_text(text)


# --------------------------------------------------------------------------- #
# Kubernetes deployment manager
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DeploymentState:
    """Live state of one dynamic Deployment: replica count + spec digest.

    ``digest`` is ``None`` for a Deployment created before drift detection
    existed (no annotation). That is treated as "unknown, therefore drifted" so
    the very first reconcile adopts it and stamps a digest.
    """

    replicas: int
    digest: str | None = None


class DeploymentManager(Protocol):
    def list_dynamic(self) -> Mapping[str, DeploymentState | int]: ...
    def apply(self, slug: str, spec: Mapping[str, object], replicas: int) -> None: ...
    def patch(self, slug: str, spec: Mapping[str, object], replicas: int) -> None: ...
    def scale(self, slug: str, replicas: int) -> None: ...
    def delete(self, slug: str) -> None: ...
    def sweep_pods(self, slug: str) -> int: ...


class KubernetesDeploymentManager:
    """In-cluster client that manages only ``swegen-generate-dyn-*`` Deployments."""

    def __init__(self, namespace: str = "swegen-pipeline") -> None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "").strip()
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443").strip()
        if not host:
            raise RuntimeError("KUBERNETES_SERVICE_HOST is not set")
        self._token = (_SERVICE_ACCOUNT_ROOT / "token").read_text(encoding="utf-8").strip()
        context = ssl.create_default_context(cafile=str(_SERVICE_ACCOUNT_ROOT / "ca.crt"))
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=context),
        )
        self._namespace = namespace
        self._api_root = f"https://{host}:{port}"
        self._deployments_url = (
            f"{self._api_root}/apis/apps/v1/namespaces/{namespace}/deployments"
        )

    def _request(
        self,
        method: str,
        url: str,
        payload: dict[str, object] | None = None,
        *,
        content_type: str = "application/json",
    ) -> dict[str, object]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            method=method,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/json",
                "Content-Type": content_type,
            },
        )
        try:
            with self._opener.open(request, timeout=15) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")[-800:]
            raise RuntimeError(
                f"Kubernetes API {method} failed with HTTP {error.code}: {detail}"
            ) from error
        except urllib.error.URLError as error:
            raise RuntimeError(f"Kubernetes API {method} failed: {error.reason}") from error

    @staticmethod
    def _guard(slug: str) -> str:
        name = deployment_name_for(slug)
        if not name.startswith(DEPLOYMENT_PREFIX) or len(name) <= len(DEPLOYMENT_PREFIX):
            raise ValueError(f"refusing to manage non-dynamic deployment {name!r}")
        return name

    def fetch_template(self, source_deployment: str = "swegen-generate") -> dict[str, object]:
        """Read the live static generate Deployment to use as the clone template.

        Avoids mounting the manifest: the controller already has API read access,
        and the live Deployment is the authoritative pod-spec source.
        """

        return self._request("GET", f"{self._deployments_url}/{source_deployment}")

    def list_dynamic(self) -> dict[str, DeploymentState]:
        """Return {slug: DeploymentState} for existing dynamic Deployments."""

        listing = self._request("GET", self._deployments_url)
        items = listing.get("items")
        out: dict[str, DeploymentState] = {}
        if not isinstance(items, list):
            return out
        for item in items:
            if not isinstance(item, Mapping):
                continue
            metadata = item.get("metadata")
            name = metadata.get("name") if isinstance(metadata, Mapping) else None
            if not isinstance(name, str) or not name.startswith(DEPLOYMENT_PREFIX):
                continue
            spec = item.get("spec")
            replicas = spec.get("replicas") if isinstance(spec, Mapping) else 0
            annotations = (
                metadata.get("annotations") if isinstance(metadata, Mapping) else None
            )
            digest = (
                annotations.get(ENDPOINT_SPEC_DIGEST_ANNOTATION)
                if isinstance(annotations, Mapping)
                else None
            )
            out[name[len(DEPLOYMENT_PREFIX):]] = DeploymentState(
                replicas=int(replicas or 0),
                digest=digest if isinstance(digest, str) and digest else None,
            )
        return out

    def apply(self, slug: str, spec: Mapping[str, object], replicas: int) -> None:
        name = self._guard(slug)
        url = f"{self._deployments_url}/{name}"
        try:
            self._request("GET", url)
            exists = True
        except RuntimeError:
            exists = False
        if exists:
            # Pre-existing Deployment: converge the template too, not just the
            # replica count. Returning after scale() here is what let a changed
            # base_url/model_id/auth_token never reach running pods.
            self.patch(slug, spec, replicas)
            return
        manifest = dict(spec)
        self._request("POST", self._deployments_url, dict(manifest))

    def patch(self, slug: str, spec: Mapping[str, object], replicas: int) -> None:
        """Roll an existing dynamic Deployment onto a new pod template.

        Sends the desired ``spec`` (pod template, selector labels, replicas) plus
        the refreshed digest annotation as a merge patch, so the Deployment
        controller performs a normal rolling update onto the new credentials.

        ``spec.selector`` is deliberately NOT patched: it is immutable on an
        existing Deployment and the API server rejects a change to it. The
        selector only ever contains the endpoint slug, which cannot change for a
        given Deployment name, so omitting it is safe.
        """

        name = self._guard(slug)
        url = f"{self._deployments_url}/{name}"
        desired = dict(spec)
        desired_spec = desired.get("spec")
        desired_spec = desired_spec if isinstance(desired_spec, Mapping) else {}
        metadata = desired.get("metadata")
        annotations = (
            metadata.get("annotations") if isinstance(metadata, Mapping) else None
        )
        payload: dict[str, object] = {
            "metadata": {"annotations": dict(annotations or {})},
            "spec": {
                "template": desired_spec.get("template"),
                "replicas": max(0, int(replicas)),
            },
        }
        self._request(
            "PATCH", url, payload, content_type="application/merge-patch+json"
        )

    def scale(self, slug: str, replicas: int) -> None:
        name = self._guard(slug)
        url = f"{self._deployments_url}/{name}"
        self._request(
            "PATCH",
            url,
            {"spec": {"replicas": max(0, int(replicas))}},
            content_type="application/merge-patch+json",
        )

    def delete(self, slug: str) -> None:
        name = self._guard(slug)
        url = f"{self._deployments_url}/{name}?propagationPolicy=Foreground"
        try:
            self._request("DELETE", url)
        except RuntimeError:
            pass
        self.sweep_pods(slug)

    def sweep_pods(self, slug: str) -> int:
        self._guard(slug)  # prefix-safety guard; sweep is by label, not name
        selector = f"app.kubernetes.io/name%3Dswegen-worker,swegen.pgcode%2Fendpoint%3D{slug}"
        pods_url = f"{self._api_root}/api/v1/namespaces/{self._namespace}/pods?labelSelector={selector}"
        try:
            listing = self._request("GET", pods_url)
        except RuntimeError:
            return 0
        items = listing.get("items")
        if not isinstance(items, list):
            return 0
        deleted = 0
        for item in items:
            metadata = item.get("metadata") if isinstance(item, Mapping) else None
            pod = metadata.get("name") if isinstance(metadata, Mapping) else None
            if not isinstance(pod, str) or not pod:
                continue
            url = f"{self._api_root}/api/v1/namespaces/{self._namespace}/pods/{pod}"
            try:
                self._request("DELETE", url, {"gracePeriodSeconds": 0})
                deleted += 1
            except RuntimeError:
                continue
        return deleted


# --------------------------------------------------------------------------- #
# Deployment spec builder (clones the static generate template)
# --------------------------------------------------------------------------- #

# Env var names the endpoint row OWNS and overrides inline. This is the
# credential subset of the model-credential secret contract
# (deploy/k3s/create-secrets.sh) -- deliberately NOT the whole contract. That
# secret also carries non-credential tuning keys (CLAUDE_CODE_MAX_CONTEXT_TOKENS,
# CLAUDE_CODE_AUTO_COMPACT_WINDOW) which the endpoint row has no opinion about;
# those must keep flowing through the template's ``envFrom`` untouched. Inline
# container ``env`` takes precedence over ``envFrom`` for a duplicate key, so
# listing a key here is what makes the row authoritative for it, and leaving a
# key out is what lets the secret's value survive.
_MODEL_ENV_KEYS = (
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_MODEL",
    "OPENAI_API_KEY",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_SMALL_FAST_MODEL",
    "SWEGEN_CLAUDE_FAST_MODEL",
)

# Non-credential keys that live in the same model-credential Secret but are pure
# worker tuning, identical for every endpoint. Dropping that Secret from
# ``envFrom`` (which we must, so one endpoint can never inherit another's token)
# used to take these with it, silently running every dynamic pool without a
# context cap while the static generate pool had one. They are re-attached by
# name via ``secretKeyRef`` -- explicitly, rather than by keeping the whole
# Secret mounted, so a future credential key added to that Secret is still not
# inherited by a dynamic pool.
_CARRIED_SECRET_ENV_KEYS = (
    "CLAUDE_CODE_MAX_CONTEXT_TOKENS",
    "CLAUDE_CODE_AUTO_COMPACT_WINDOW",
)


def model_env(row: EndpointRow) -> list[dict[str, str]]:
    base = row.base_url.rstrip("/")
    openai_base = base if base.endswith("/v1") else base + "/v1"
    anthropic_base = base[: -len("/v1")] if base.endswith("/v1") else base
    values = {
        "ANTHROPIC_BASE_URL": anthropic_base,
        "ANTHROPIC_MODEL": row.model_id,
        "ANTHROPIC_AUTH_TOKEN": row.auth_token,
        "ANTHROPIC_API_KEY": row.auth_token,
        "OPENAI_BASE_URL": openai_base,
        "OPENAI_MODEL": row.model_id,
        "OPENAI_API_KEY": row.auth_token,
        "ANTHROPIC_DEFAULT_OPUS_MODEL": row.model_id,
        "ANTHROPIC_DEFAULT_SONNET_MODEL": row.model_id,
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": row.model_id,
        "ANTHROPIC_SMALL_FAST_MODEL": row.model_id,
        "SWEGEN_CLAUDE_FAST_MODEL": row.model_id,
    }
    return [{"name": key, "value": values[key]} for key in _MODEL_ENV_KEYS]


def carried_secret_env(secret_names: Sequence[str]) -> list[dict[str, object]]:
    """Re-attach the non-credential tuning keys dropped with the credential Secret.

    Returns ``valueFrom.secretKeyRef`` entries (marked ``optional``) for each key
    in ``_CARRIED_SECRET_ENV_KEYS``, pointing at the model-credential Secret the
    template had mounted. ``optional: true`` matters: an endpoint pool must not
    become unschedulable just because a credential Secret was rotated to a new
    name -- it falls back to the worker's own default, exactly as before.

    The reference is resolved by the kubelet, not by this controller, so this
    needs no ``secrets`` RBAC (the controller Role grants deployments + pods only).
    """

    entries: list[dict[str, object]] = []
    for secret_name in secret_names:
        for key in _CARRIED_SECRET_ENV_KEYS:
            entries.append(
                {
                    "name": key,
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": secret_name,
                            "key": key,
                            "optional": True,
                        }
                    },
                }
            )
    return entries


def build_deployment_spec(row: EndpointRow, template: Mapping[str, object]) -> dict[str, object]:
    """Clone the static generate Deployment template for this endpoint.

    ``template`` is the parsed ``swegen-generate`` Deployment (re-read from the
    live cluster every reconcile cycle, so an image/configmap/envFrom roll of the
    static pool propagates here). The clone keeps the whole pod spec but: renames
    to ``swegen-generate-dyn-<slug>``, adds a ``swegen.pgcode/endpoint`` label so
    the pod sweep and dashboard can attribute pods to this endpoint, drops the
    model-credential secret from ``envFrom`` while preserving its non-credential
    tuning keys, appends the inline model env, sets replicas, and stamps the
    spec digest annotation used for drift detection.
    """

    import copy

    manifest = copy.deepcopy(dict(template))
    name = deployment_name_for(row.slug)
    metadata = manifest.setdefault("metadata", {})
    metadata["name"] = name
    # Strip server-populated identity/bookkeeping: the template is read back from
    # the live static Deployment, and carrying its resourceVersion/uid/generation
    # into a POST is rejected, while its revision + last-applied annotations would
    # make the digest churn on every unrelated roll of swegen-generate.
    for volatile in ("resourceVersion", "uid", "creationTimestamp", "annotations",
                     "generation", "managedFields", "ownerReferences", "selfLink"):
        metadata.pop(volatile, None)
    # ``status`` is read-only server state; it must never be sent back nor feed
    # the digest (it changes on every pod restart of the static pool).
    manifest.pop("status", None)

    spec = manifest.setdefault("spec", {})
    spec["replicas"] = max(0, int(row.concurrency))

    # Label both selector and pod template with the endpoint slug.
    tpl = spec.setdefault("template", {})
    tpl_meta = tpl.setdefault("metadata", {})
    labels = dict(tpl_meta.get("labels") or {})
    labels["swegen.pgcode/endpoint"] = row.slug
    tpl_meta["labels"] = labels
    selector = spec.setdefault("selector", {})
    sel_labels = dict(selector.get("matchLabels") or {})
    sel_labels["swegen.pgcode/endpoint"] = row.slug
    selector["matchLabels"] = sel_labels

    containers = tpl.get("spec", {}).get("containers", [])
    for container in containers:
        if container.get("name") != "worker":
            continue
        dropped_secrets = [
            str(source.get("secretRef", {}).get("name", ""))
            for source in container.get("envFrom", [])
            if str(source.get("secretRef", {}).get("name", "")).startswith(
                "swegen-model-credentials-"
            )
        ]
        env_from = [
            source
            for source in container.get("envFrom", [])
            if not str(source.get("secretRef", {}).get("name", "")).startswith(
                "swegen-model-credentials-"
            )
        ]
        container["envFrom"] = env_from
        overridden = set(_MODEL_ENV_KEYS) | set(_CARRIED_SECRET_ENV_KEYS)
        env = [
            entry
            for entry in container.get("env", [])
            if entry.get("name") not in overridden
        ]
        env.extend(carried_secret_env(dropped_secrets))
        env.extend(model_env(row))
        container["env"] = env

    # Stamp the drift digest LAST, over the finished spec, so it covers both the
    # row's credentials and everything inherited from the template.
    metadata["annotations"] = {
        ENDPOINT_SPEC_DIGEST_ANNOTATION: spec_digest(manifest),
    }
    return manifest


def spec_digest(manifest: Mapping[str, object]) -> str:
    """Stable sha256 over the pod-defining part of a desired Deployment spec.

    Covers ``spec.template`` (image, envFrom, and the inline model env carrying
    base_url/model_id/auth_token) and ``spec.selector``. Deliberately EXCLUDES
    ``spec.replicas``: replica count is already reconciled by the scale path, and
    folding it in would turn every ordinary scale-up into a full template patch
    that rolls every pod in the pool.

    ``sort_keys`` makes the digest independent of dict ordering, so re-reading
    the same template from the API twice cannot produce a spurious patch.
    """

    spec = manifest.get("spec")
    spec_map: Mapping[str, object] = spec if isinstance(spec, Mapping) else {}
    material = {
        "template": spec_map.get("template"),
        "selector": spec_map.get("selector"),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Reconcile
# --------------------------------------------------------------------------- #


@dataclass
class ReconcilePlan:
    apply: list[tuple[str, int]] = field(default_factory=list)   # (slug, replicas) create/ensure
    patch: list[tuple[str, int]] = field(default_factory=list)   # (slug, replicas) template drift
    scale: list[tuple[str, int]] = field(default_factory=list)   # (slug, replicas)
    delete: list[str] = field(default_factory=list)              # slugs


def _as_state(value: DeploymentState | int) -> DeploymentState:
    """Accept a bare replica count as well as a DeploymentState.

    ``list_dynamic`` used to return ``{slug: replicas}``. Keeping the planner
    tolerant of the old shape means a fake/manager that has not been updated (or
    a caller passing a plain int map) still plans correctly -- it simply has no
    digest, and so is treated as drifted exactly once.
    """

    return value if isinstance(value, DeploymentState) else DeploymentState(int(value))


def plan_reconcile(
    endpoints: Sequence[EndpointRow],
    existing: Mapping[str, DeploymentState | int],
    desired_digests: Mapping[str, str] | None = None,
) -> ReconcilePlan:
    """Pure planner: given registry rows and live state, decide actions.

    A row that is disabled or breaker-open targets 0 replicas (kept, scaled down);
    an enabled healthy row targets its concurrency. An existing dynamic
    Deployment whose slug is no longer in the registry is deleted.

    ``desired_digests`` maps slug -> digest of the freshly rendered spec. When it
    disagrees with the live Deployment's annotation the row is routed to
    ``patch`` (a real template roll) instead of ``scale``, which is how a changed
    base_url/model_id/auth_token reaches running pods. A slug missing from
    ``desired_digests`` is not drift-checked, so callers that do not render specs
    keep the old create/scale/delete behaviour exactly.

    ``patch`` supersedes ``scale`` for a given slug: the patch carries the target
    replica count itself, so emitting both would issue two writes for one change.
    """

    plan = ReconcilePlan()
    digests = desired_digests or {}
    registry_slugs = {row.slug for row in endpoints}
    for row in endpoints:
        target = 0 if (not row.enabled or row.breaker_open) else max(0, row.concurrency)
        if row.slug not in existing:
            if target > 0:
                plan.apply.append((row.slug, target))
            # target 0 and not existing: nothing to do.
            continue
        state = _as_state(existing[row.slug])
        desired_digest = digests.get(row.slug)
        # Only a KNOWN mismatch is drift. If we did not render a digest for this
        # slug, or the live Deployment predates the annotation, fall through to
        # the replica comparison rather than rolling the pool on every cycle.
        drifted = (
            desired_digest is not None
            and state.digest is not None
            and state.digest != desired_digest
        )
        # A Deployment with no digest annotation yet is adopted once: patch it so
        # it gets stamped, but only when it is actually running pods or is meant
        # to. Adopting a scaled-to-zero pool costs nothing and rolls no pods.
        unstamped = desired_digest is not None and state.digest is None
        if drifted or unstamped:
            plan.patch.append((row.slug, target))
        elif state.replicas != target:
            plan.scale.append((row.slug, target))
    for slug in existing:
        if slug not in registry_slugs:
            plan.delete.append(slug)
    return plan


def reconcile_once(
    connection: Connection,
    manager: DeploymentManager,
    prober: EndpointProber,
    template: Mapping[str, object],
    *,
    consecutive_trip_threshold: int = 3,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ReconcilePlan:
    """One reconcile + probe pass. Returns the plan that was applied."""

    endpoints = load_endpoints(connection)

    # --- Health probe + breaker (only for enabled, not-yet-open endpoints) ---
    for row in endpoints:
        if not row.enabled or row.breaker_open:
            continue
        result = prober.probe(row.base_url, row.model_id, row.auth_token)
        stamp = now()
        if result.ok:
            with connection.transaction():
                record_probe(connection, row.slug, status=result.status, consecutive_fail=0, now=stamp)
        else:
            fails = row.consecutive_fail + 1
            with connection.transaction():
                if fails >= consecutive_trip_threshold:
                    reason = (
                        f"endpoint unhealthy: {result.detail} "
                        f"({fails} consecutive probe failures)"
                    )
                    trip_endpoint(
                        connection,
                        row,
                        status=result.status,
                        reason=reason,
                        consecutive_fail=fails,
                        now=stamp,
                    )
                else:
                    record_probe(
                        connection, row.slug, status=result.status, consecutive_fail=fails, now=stamp
                    )

    # Re-read after probing so trips this cycle are reflected in the plan.
    endpoints = load_endpoints(connection)
    existing = manager.list_dynamic()

    # Render every desired spec up front: the digest of the rendered spec is what
    # the planner compares against the live Deployment's annotation, so a changed
    # credential becomes a template patch rather than a silent no-op.
    desired_specs = {
        row.slug: build_deployment_spec(row, template) for row in endpoints
    }
    desired_digests = {
        slug: spec_digest(spec) for slug, spec in desired_specs.items()
    }

    plan = plan_reconcile(endpoints, existing, desired_digests)

    for slug, replicas in plan.apply:
        manager.apply(slug, desired_specs[slug], replicas)
    for slug, replicas in plan.patch:
        manager.patch(slug, desired_specs[slug], replicas)
        # A patch to zero replicas still needs the sweep: scaling down only marks
        # pods for deletion and generate carries an 18000s grace period.
        if replicas == 0:
            manager.sweep_pods(slug)
    for slug, replicas in plan.scale:
        manager.scale(slug, replicas)
        if replicas == 0:
            manager.sweep_pods(slug)
    for slug in plan.delete:
        manager.delete(slug)
    return plan


# --------------------------------------------------------------------------- #
# Template loading + entrypoint
# --------------------------------------------------------------------------- #


def load_generate_template(manifest_path: Path) -> dict[str, object]:
    """Load the static swegen-generate Deployment from the pipeline manifest."""

    import yaml

    for document in yaml.safe_load_all(manifest_path.read_text(encoding="utf-8")):
        if (
            isinstance(document, Mapping)
            and document.get("kind") == "Deployment"
            and document.get("metadata", {}).get("name") == "swegen-generate"
        ):
            return dict(document)
    raise RuntimeError(f"swegen-generate Deployment not found in {manifest_path}")


def _config_from_environment() -> dict[str, object]:
    return {
        "namespace": os.environ.get("SWEGEN_ENDPOINT_NAMESPACE", "swegen-pipeline"),
        "poll_seconds": float(os.environ.get("SWEGEN_ENDPOINT_POLL_SECONDS", "20")),
        "probe_timeout": float(
            os.environ.get(ENDPOINT_PROBE_TIMEOUT_ENV, DEFAULT_PROBE_TIMEOUT_SECONDS)
        ),
        "trip_threshold": int(os.environ.get("SWEGEN_ENDPOINT_TRIP_THRESHOLD", "3")),
        "manifest": os.environ.get(
            "SWEGEN_ENDPOINT_TEMPLATE_PATH", "/etc/swegen/swegen-pipeline.yaml"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    config = _config_from_environment()
    manager = KubernetesDeploymentManager(namespace=str(config["namespace"]))
    manifest_path = Path(str(config["manifest"]))

    def current_template() -> Mapping[str, object]:
        """Re-read the clone template every cycle.

        Loading this once at startup meant a roll of the static swegen-generate
        Deployment (new image, new configmap key, new envFrom) never reached the
        dynamic pools until the controller pod happened to restart. Reading it
        per cycle makes the live static Deployment authoritative continuously;
        the digest comparison then turns a real change into one rolling patch and
        an unchanged template into no writes at all.

        Prefers the live Deployment (no manifest mount); falls back to the
        mounted manifest path if the source Deployment is absent.
        """

        try:
            return manager.fetch_template()
        except RuntimeError:
            return load_generate_template(manifest_path)

    prober = HttpEndpointProber(timeout_seconds=float(config["probe_timeout"]))
    pool = db.get_pool()
    stop = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while not stop:
            try:
                template = current_template()
                with pool.connection() as connection:
                    plan = reconcile_once(
                        connection,
                        manager,
                        prober,
                        template,
                        consecutive_trip_threshold=int(config["trip_threshold"]),
                    )
                print(
                    f"reconcile applied={len(plan.apply)} patched={len(plan.patch)} "
                    f"scaled={len(plan.scale)} deleted={len(plan.delete)}",
                    flush=True,
                )
            except Exception as error:
                print(
                    f"endpoint reconcile failed: {type(error).__name__}: {error}",
                    flush=True,
                )
            if args.once:
                return
            time.sleep(float(config["poll_seconds"]))
    finally:
        db.close_pool()


if __name__ == "__main__":
    main()
