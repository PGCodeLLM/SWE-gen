# Deploying test SWE-gen workers on the shared k3s cluster

Use the restricted `swegen-worker-deployer` identity for coworker experiments.
It can create, update, scale, inspect, and remove Deployments named
`swegen-test-*`; it cannot mutate Secrets or production Deployments.

## One-time cluster setup by an administrator

From this repository checkout:

```bash
kubectl apply -f deploy/k3s/worker-deployer-rbac.yaml
kubectl apply -f deploy/k3s/credential-guard.yaml
```

The model credential source is the immutable Secret
`swegen-pipeline/swegen-model-credentials-v2`. Do not place API keys, model names,
or model endpoints in worker manifests. The generic `swegen-runtime-proxy`
Secret intentionally contains only proxy and remote-BuildKit settings.

Generate a short-lived token for the coworker instead of sharing the admin
kubeconfig:

```bash
kubectl -n swegen-pipeline create token swegen-worker-deployer --duration=8h
```

Put that token in a separate kubeconfig using the same cluster server and CA as
the administrator kubeconfig. Set its default namespace to `swegen-pipeline`.
Tokens expire; generate a new one when needed.

## Test worker manifest rules

1. Name every Deployment `swegen-test-<experiment>`.
2. Use a unique selector label such as `swegen.pgcode/test-run` so it cannot
   overlap a production Deployment.
3. Keep the normal stage label, for example `swegen.pgcode/stage: generate`.
4. Reference the central ConfigMap and Secrets; never copy their values.
5. Use an immutable image tag, not a moving tag such as `latest` or `e2e`.
6. Start with one replica and inspect its environment fingerprint and logs
   before scaling.

Example Generate worker:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: swegen-test-coworker-generate
  namespace: swegen-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: swegen-worker
      swegen.pgcode/stage: generate
      swegen.pgcode/test-run: coworker-generate
  template:
    metadata:
      labels:
        app.kubernetes.io/name: swegen-worker
        swegen.pgcode/stage: generate
        swegen.pgcode/test-run: coworker-generate
    spec:
      automountServiceAccountToken: false
      containers:
        - name: worker
          image: swegen-worker:<immutable-test-tag>
          imagePullPolicy: Never
          args: ["--stage", "generate"]
          envFrom:
            - configMapRef:
                name: swegen-pipeline-config
            - secretRef:
                name: swegen-database
            - secretRef:
                name: swegen-runtime-proxy
            - secretRef:
                name: swegen-model-credentials-v2
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
```

Apply and observe it with the restricted kubeconfig:

```bash
kubectl apply -f coworker-worker.yaml
kubectl rollout status deployment/swegen-test-coworker-generate --timeout=10m
kubectl logs -f deployment/swegen-test-coworker-generate --tail=100
```

Scale or remove only the test Deployment:

```bash
kubectl scale deployment/swegen-test-coworker-generate --replicas=4
kubectl delete deployment/swegen-test-coworker-generate
```

## Credential verification without disclosure

An administrator can compare fingerprints without printing the key:

```bash
pod=$(kubectl -n swegen-pipeline get pod \
  -l swegen.pgcode/test-run=coworker-generate \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n swegen-pipeline exec "$pod" -- python -c \
  'import hashlib,os; key=os.environ.get("OPENAI_API_KEY", ""); print(os.environ.get("OPENAI_BASE_URL")); print(os.environ.get("OPENAI_MODEL")); print(len(key), hashlib.sha256(key.encode()).hexdigest()[:12])'
```

Never paste `/proc/<pid>/environ`, Secret YAML/JSON, tokens, or raw API keys
into chat, logs, tickets, or commits.

## Deliberate credential rotation

The model Secret is immutable. Rotation therefore requires an administrator and
a maintenance window:

1. Scale Generate workers to zero or pause their queue consumption.
2. Back up only fingerprints and key names, never plaintext.
3. Choose a new versioned model Secret name and update the production manifests
   and admission guard to reference it. Reusing a deleted immutable Secret name
   can leave kubelet serving the old cached value.
4. Run `deploy/k3s/create-secrets.sh` from the approved secret source.
5. Roll Generate workers and verify endpoint, model, and key fingerprint.
6. Run one smoke task before restoring normal concurrency.

Normal deployments must not delete or recreate the central Secret.
