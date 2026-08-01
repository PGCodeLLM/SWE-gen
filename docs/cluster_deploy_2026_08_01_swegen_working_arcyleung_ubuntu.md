# SWE-gen Arcyleung Ubuntu cluster deployment record — 2026-08-01

This records the known-working Arcyleung model and reward path on the SWE-gen
K3s cluster. It is an overlay on the full cluster bootstrap guide in
`deploy/k3s/README.md`; use that guide to recreate K3s, PostgreSQL/PGMQ, Docker,
node storage, the proxy CA, and the base pipeline first.

No credential values belong in Git. Keep the source environment files outside
the repository with mode `0600`, create Kubernetes Secrets from them, and
verify only Secret names, key names, and non-secret endpoint metadata.

## Known-working result

The working request path is:

```text
Generate and Reward workers
  -> swegen-arc-gateway Service :3130
  -> LiteLLM container :4000
  -> pooled aiohttp gateway container 127.0.0.1:3130
  -> proxyhk-spl.huawei.com:8080
  -> https://arcyleung-ubuntu.tailb940e6.ts.net:443
```

The Kubernetes Service deliberately exposes port `3130` but targets the
LiteLLM container's named port `litellm` (`4000`). LiteLLM translates Anthropic
Messages requests to OpenAI chat completions, then sends those OpenAI requests
to the private aiohttp gateway on pod loopback port `3130`. The gateway owns a
64-connection per-host pool, keeps idle connections for 600 seconds, and makes
up to four transport attempts.

The validated deployment used:

- gateway node label `swegen.pgcode/node-ip=7.244.3.200`;
- base image `swegen-worker:proxy-noproxy-fix-20260801-0356`;
- LiteLLM image `swegen-worker:arc-pool-litellm-20260801`;
- model `gpt-5.6-sol`;
- immutable model Secret `swegen-model-credentials-pooled-20260801`;
- immutable Reward Secret `swegen-reward-credentials-arcyleung-20260801`;
- mutable proxy/BuildKit Secret `swegen-runtime-proxy`.

OpenAI chat, Anthropic Messages, Anthropic streaming, and tool-use requests all
returned HTTP 200 in the final smoke tests. Gateway counters showed successful
HK-routed requests.

## Why Arc traffic must not use ATS

Apache Traffic Server on `7.244.3.78` remains useful for ordinary HTTP caching,
but it is not in the Arcyleung request path. Tests through ATS ports `3128` and
`3129` could receive an HTTP 200 response to CONNECT while the subsequent TLS
exchange stalled. Direct use of the SG corporate parent also produced frequent
504 responses under load. The HK parent completed the same Arc requests
reliably.

Do not point `OPENAI_BASE_URL`, `ANTHROPIC_BASE_URL`, or
`SWEGEN_REWARD_ENDPOINT` at ATS. Do not interpret CONNECT 200 by itself as a
successful proxy test; verify a complete TLS request and response body.

## Files in this deployment overlay

```text
deploy/k3s/arc-gateway/Dockerfile
deploy/k3s/arc-gateway/litellm-config.yaml
deploy/k3s/arc-gateway/swegen-arc-gateway.py
deploy/k3s/arc-gateway/deployment.yaml
deploy/k3s/arc-gateway/kustomization.yaml
deploy/k3s/arc-gateway/build-import.sh
deploy/k3s/arc-gateway/configure-pipeline.sh
deploy/k3s/credential-guard.yaml
```

`credential-guard.yaml` permits the established
`swegen-model-credentials-v2` Secret and the pooled Arc Secret. It rejects a
Generate Deployment that shadows centrally managed model variables with
explicit container environment values.

## Rebuild from an empty K3s cluster

### 1. Recreate the base platform

Follow `deploy/k3s/README.md` from the beginning. Preserve this topology unless
the manifests are updated at the same time:

| Role | Node |
| --- | --- |
| Initial K3s server and Arc gateway | `7.244.3.200` |
| Additional K3s servers | `7.244.3.78`, `7.244.2.110` |
| K3s agent | `7.244.1.209` |

The gateway manifest requires the `7.244.3.200` node to have this label:

```bash
kubectl label node <node-name-for-7.244.3.200> \
  swegen.pgcode/node-ip=7.244.3.200 --overwrite
```

Restore PostgreSQL and the SQL-only PGMQ objects, apply the required migrations,
and create the normal database, repair-model, private-file, Docker-config, and
proxy-CA Secrets as described in the base guide. During recovery, set all
pipeline worker replica counts to zero until the Arc overlay and credentials
have been applied. This prevents workers from starting with a stale endpoint.

### 2. Restore secret source files outside Git

Create a root-owned directory such as `/etc/swegen/credentials` with mode
`0700`. Store each environment file below with mode `0600`. Use `sudoedit` or a
secret manager; do not paste real values into shell history.

The direct corporate proxy URL must contain percent-encoded credentials:

```text
http://ENCODED_USER:ENCODED_PASSWORD@proxysg-spl.huawei.com:8080
```

The gateway reads `HTTPS_PROXY` as its SG template and derives the HK route by
replacing only the hostname with `proxyhk-spl.huawei.com`. Therefore the scheme,
credentials, and port must be valid for both corporate parent proxies.

`runtime-proxy.env` must contain these keys:

```dotenv
HTTP_PROXY=http://ENCODED_USER:ENCODED_PASSWORD@proxysg-spl.huawei.com:8080
HTTPS_PROXY=http://ENCODED_USER:ENCODED_PASSWORD@proxysg-spl.huawei.com:8080
http_proxy=http://ENCODED_USER:ENCODED_PASSWORD@proxysg-spl.huawei.com:8080
https_proxy=http://ENCODED_USER:ENCODED_PASSWORD@proxysg-spl.huawei.com:8080
SWEGEN_REMOTE_BUILDKIT_REGISTRY_USERNAME=REPLACE_FROM_SECURE_STORE
SWEGEN_REMOTE_BUILDKIT_REGISTRY_PASSWORD=REPLACE_FROM_SECURE_STORE
SWEGEN_REMOTE_BUILDKIT_PULL_USERNAME=REPLACE_FROM_SECURE_STORE
SWEGEN_REMOTE_BUILDKIT_PULL_PASSWORD=REPLACE_FROM_SECURE_STORE
```

`arc-model.env` must contain:

```dotenv
OPENAI_API_KEY=REPLACE_FROM_SECURE_STORE
OPENAI_BASE_URL=http://swegen-arc-gateway.swegen-pipeline.svc.cluster.local:3130/v1
OPENAI_MODEL=gpt-5.6-sol
ANTHROPIC_API_KEY=REPLACE_FROM_SECURE_STORE
ANTHROPIC_AUTH_TOKEN=REPLACE_FROM_SECURE_STORE
ANTHROPIC_BASE_URL=http://swegen-arc-gateway.swegen-pipeline.svc.cluster.local:3130
ANTHROPIC_MODEL=gpt-5.6-sol
ANTHROPIC_DEFAULT_OPUS_MODEL=gpt-5.6-sol
ANTHROPIC_DEFAULT_SONNET_MODEL=gpt-5.6-sol
```

`arc-reward.env` must contain:

```dotenv
SWEGEN_REWARD_API_KEY=REPLACE_FROM_SECURE_STORE
OPENAI_API_KEY=REPLACE_FROM_SECURE_STORE
ANTHROPIC_API_KEY=REPLACE_FROM_SECURE_STORE
```

Use the Arcyleung credential authorized for this endpoint. If one provider
token is used for all supported protocols, place the same value under the three
required key names without printing it.

### 3. Create the three Arc-related Secrets

The following commands read the environment files without displaying their
contents. The versioned model and Reward Secrets are immutable by design.

```bash
kubectl create namespace swegen-pipeline --dry-run=client -o yaml \
  | kubectl apply -f -

kubectl -n swegen-pipeline create secret generic swegen-runtime-proxy \
  --from-env-file=/etc/swegen/credentials/runtime-proxy.env \
  --dry-run=client -o yaml \
  | kubectl apply -f -

kubectl -n swegen-pipeline create secret generic \
  swegen-model-credentials-pooled-20260801 \
  --from-env-file=/etc/swegen/credentials/arc-model.env \
  --dry-run=client -o json \
  | jq '.immutable=true' \
  | kubectl create -f -

kubectl -n swegen-pipeline create secret generic \
  swegen-reward-credentials-arcyleung-20260801 \
  --from-env-file=/etc/swegen/credentials/arc-reward.env \
  --dry-run=client -o json \
  | jq '.immutable=true' \
  | kubectl create -f -
```

For an existing cluster, do not overwrite or delete an immutable Secret in
place. Create a new versioned name, add that name to `credential-guard.yaml`,
and pass it to `configure-pipeline.sh` through
`SWEGEN_ARC_MODEL_SECRET` or `SWEGEN_ARC_REWARD_SECRET`.

Verify key names only:

```bash
for secret in \
  swegen-runtime-proxy \
  swegen-model-credentials-pooled-20260801 \
  swegen-reward-credentials-arcyleung-20260801
do
  kubectl -n swegen-pipeline get secret "${secret}" -o json \
    | jq '{name:.metadata.name, immutable:(.immutable // false), keys:(.data|keys)}'
done
```

### 4. Build and import the images

The base worker image must exist in the deployment host's Docker store and in
K3s containerd on every node that may run workers. Build it from the recovered
commit and import it with the normal helper:

```bash
SWEGEN_WORKER_IMAGE='swegen-worker:proxy-noproxy-fix-20260801-0356' \
SWEGEN_K3S_NODES='7.244.3.200 7.244.3.78 7.244.2.110 7.244.1.209' \
SWEGEN_K3S_SSH_USER='root' \
SWEGEN_BUILD_CA='/etc/swegen/credentials/combined-ca.crt' \
  ./deploy/k3s/build-import-worker.sh
```

Build the LiteLLM image from that base, import it into the gateway node's K3s
containerd, apply the gateway manifests, and wait for readiness:

```bash
SWEGEN_WORKER_IMAGE='swegen-worker:proxy-noproxy-fix-20260801-0356' \
SWEGEN_ARC_GATEWAY_IMAGE='swegen-worker:arc-pool-litellm-20260801' \
SWEGEN_ARC_GATEWAY_NODE='7.244.3.200' \
  ./deploy/k3s/arc-gateway/build-import.sh
```

If an image archive is restored instead of rebuilt, import it on the correct
node with `k3s ctr -n k8s.io images import <archive>` and confirm it with
`k3s crictl inspecti <image>`.

### 5. Apply and wire the pipeline

Apply the base pipeline with zero replicas, then the gateway and wiring overlay:

```bash
kubectl apply -f deploy/k3s/swegen-pipeline.yaml
kubectl apply -k deploy/k3s/arc-gateway

SWEGEN_ARC_MODEL_SECRET='swegen-model-credentials-pooled-20260801' \
SWEGEN_ARC_REWARD_SECRET='swegen-reward-credentials-arcyleung-20260801' \
  ./deploy/k3s/arc-gateway/configure-pipeline.sh

kubectl apply -f deploy/k3s/credential-guard.yaml
```

The configure helper performs all endpoint-sensitive mutations:

- sets `SWEGEN_REWARD_ENDPOINT` to the gateway Service;
- adds the gateway Service names, `.svc`, and `.cluster.local` to
  `SWEGEN_NO_PROXY`;
- changes Generate and optional Generate-overflow to the immutable pooled model
  Secret;
- changes Reward's `SWEGEN_REWARD_API_KEY` reference to the versioned Reward
  Secret;
- uses rolling updates and waits for gateway, Generate, and Reward readiness.

The gateway Service must stay in `NO_PROXY`; otherwise workers can send an
in-cluster address to the corporate proxy and fail unexpectedly.

### 6. Smoke test before restoring concurrency

Start with one Generate and one Reward worker:

```bash
kubectl -n swegen-pipeline scale deployment/swegen-generate --replicas=1
kubectl -n swegen-pipeline scale deployment/swegen-reward --replicas=1
kubectl -n swegen-pipeline rollout status deployment/swegen-arc-gateway --timeout=5m
kubectl -n swegen-pipeline rollout status deployment/swegen-generate --timeout=10m
kubectl -n swegen-pipeline rollout status deployment/swegen-reward --timeout=10m
```

Check the private pooled gateway without exposing a key:

```bash
kubectl -n swegen-pipeline exec deployment/swegen-arc-gateway -c gateway -- \
  python3 -c 'import json,urllib.request; print(json.load(urllib.request.urlopen("http://127.0.0.1:3130/healthz")))'

kubectl -n swegen-pipeline exec deployment/swegen-arc-gateway -c gateway -- \
  python3 -c 'import json,urllib.request; print(json.load(urllib.request.urlopen("http://127.0.0.1:3130/metrics")))'
```

Confirm the public Service and endpoint wiring:

```bash
kubectl -n swegen-pipeline get service,endpoints,pod \
  -l app.kubernetes.io/name=swegen-arc-gateway -o wide

kubectl -n swegen-pipeline get configmap swegen-pipeline-config -o json \
  | jq '{reward_endpoint:.data.SWEGEN_REWARD_ENDPOINT,
         no_proxy:.data.SWEGEN_NO_PROXY}'

kubectl -n swegen-pipeline get deployment swegen-generate swegen-reward -o json \
  | jq '[.items[] | {name:.metadata.name,
      secret_refs:[.spec.template.spec.containers[]
        | select(.name=="worker")
        | ((.envFrom // [])[]?.secretRef.name),
          ((.env // [])[]?.valueFrom.secretKeyRef.name)]}]'
```

Run one real Generate request and one Reward task before scaling further. A
healthy metrics response should increase `route_hk` and a `status_200` counter
without a growing `gateway_failures` counter. Scale in steps and watch pod
restarts, queue throughput, gateway `active`, and retry counters.

## Rollback

Keep the previous versioned model and Reward Secrets until the smoke test and a
representative production window have passed. To roll back:

1. Scale Generate and Reward to zero.
2. Restore their previous Secret references and the previous Reward endpoint.
3. Roll both Deployments and run a one-pod smoke test.
4. Only after traffic has moved away, scale the gateway Deployment to zero.

Do not delete immutable Secrets during the rollback. Do not route the rollback
through ATS `3128` or `3129`; use the previously verified direct/provider path.

```bash
kubectl -n swegen-pipeline scale deployment/swegen-generate \
  deployment/swegen-reward --replicas=0
kubectl -n swegen-pipeline scale deployment/swegen-arc-gateway --replicas=0
```

Record any new Secret names, immutable image tags, endpoint changes, and proxy
route changes in a new dated document rather than rewriting this known-working
record.
