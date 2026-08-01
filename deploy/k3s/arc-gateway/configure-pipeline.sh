#!/usr/bin/env bash
set -euo pipefail

namespace="${SWEGEN_NAMESPACE:-swegen-pipeline}"
model_secret="${SWEGEN_ARC_MODEL_SECRET:-swegen-model-credentials-pooled-20260801}"
reward_secret="${SWEGEN_ARC_REWARD_SECRET:-swegen-reward-credentials-arcyleung-20260801}"
gateway_url="${SWEGEN_ARC_GATEWAY_URL:-http://swegen-arc-gateway.swegen-pipeline.svc.cluster.local:3130}"

kubectl -n "${namespace}" get secret "${model_secret}" >/dev/null
kubectl -n "${namespace}" get secret "${reward_secret}" >/dev/null
kubectl -n "${namespace}" get service swegen-arc-gateway >/dev/null

kubectl -n "${namespace}" get configmap swegen-pipeline-config -o json \
  | jq --arg endpoint "${gateway_url}" '
      .data.SWEGEN_REWARD_ENDPOINT=$endpoint
      | .data.SWEGEN_NO_PROXY=(
          .data.SWEGEN_NO_PROXY
          + ",.svc,.cluster.local,swegen-arc-gateway,swegen-arc-gateway.swegen-pipeline.svc.cluster.local"
          | split(",") | unique | join(",")
        )
    ' \
  | kubectl apply -f -

for deployment in swegen-generate swegen-generate-overflow; do
  if ! deployment_json="$(
    kubectl -n "${namespace}" get deployment "${deployment}" -o json 2>/dev/null
  )"; then
    if [[ "${deployment}" == "swegen-generate-overflow" ]]; then
      printf 'Optional deployment %s is absent; skipping it.\n' "${deployment}"
      continue
    fi
    printf 'Required deployment %s is absent.\n' "${deployment}" >&2
    exit 1
  fi

  jq --arg secret "${model_secret}" '
      ([.spec.template.spec.containers[]
        | select(.name=="worker")
        | .envFrom[]?
        | select(has("secretRef"))
        | select(.secretRef.name | startswith("swegen-model-credentials"))]
       | length) as $matches
      | if $matches != 1 then
          error("expected exactly one model credential envFrom entry")
        else
          (.spec.template.spec.containers[]
            | select(.name=="worker")
            | .envFrom[]
            | select(has("secretRef"))
            | select(.secretRef.name | startswith("swegen-model-credentials"))
            | .secretRef.name)=$secret
        end
    ' <<<"${deployment_json}" \
    | kubectl apply -f -
done

kubectl -n "${namespace}" get deployment swegen-reward -o json \
  | jq --arg secret "${reward_secret}" '
      ([.spec.template.spec.containers[]
        | select(.name=="worker")
        | .env[]?
        | select(.name=="SWEGEN_REWARD_API_KEY")]
       | length) as $matches
      | if $matches != 1 then
          error("expected exactly one SWEGEN_REWARD_API_KEY env entry")
        else
          (.spec.template.spec.containers[]
            | select(.name=="worker")
            | .env[]
            | select(.name=="SWEGEN_REWARD_API_KEY")
            | .valueFrom.secretKeyRef.name)=$secret
        end
    ' \
  | kubectl apply -f -

kubectl -n "${namespace}" patch deployment swegen-generate --type=merge -p \
  '{"spec":{"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxUnavailable":16,"maxSurge":16}}}}'
kubectl -n "${namespace}" patch deployment swegen-reward --type=merge -p \
  '{"spec":{"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxUnavailable":2,"maxSurge":2}}}}'

kubectl -n "${namespace}" rollout restart deployment/swegen-generate
kubectl -n "${namespace}" rollout restart deployment/swegen-reward
kubectl -n "${namespace}" rollout status deployment/swegen-arc-gateway --timeout=5m
kubectl -n "${namespace}" rollout status deployment/swegen-generate --timeout=10m
kubectl -n "${namespace}" rollout status deployment/swegen-reward --timeout=10m
