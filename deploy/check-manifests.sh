#!/usr/bin/env bash
# Offline structural check of the KEDA / Kubernetes manifests (plan 07-16, D-W22).
#
# Greps every manifest for the keys the scale-to-zero setup depends on, rejects tabs,
# and, when PyYAML is importable, parses every document with yaml.safe_load_all.
# It never talks to a cluster: no Kubernetes client or package manager is invoked.
set -euo pipefail

cd "$(dirname "$0")"

fail=0
err() {
    echo "check-manifests: $*" >&2
    fail=1
}

# need FILE PATTERN...: every fixed-string pattern must occur in FILE.
need() {
    local f="$1"
    shift
    local p
    for p in "$@"; do
        grep -qF -- "$p" "$f" || err "$f: missing '$p'"
    done
}

files=(
    keda/scaledobject-metrics-api.yaml
    keda/scaledobject-prometheus.yaml
    keda/trigger-auth.yaml
    k8s/worker-deployment.yaml
    k8s/server-statefulset.yaml
)

for f in "${files[@]}"; do
    [ -f "$f" ] || {
        err "$f: missing"
        continue
    }
    if grep -q $'\t' "$f"; then
        err "$f: contains a tab (YAML indentation must be spaces)"
    fi
    grep -q '^apiVersion: ' "$f" || err "$f: no apiVersion"
    grep -q '^kind: ' "$f" || err "$f: no kind"
done

need keda/scaledobject-metrics-api.yaml \
    'apiVersion: keda.sh/v1alpha1' \
    'kind: ScaledObject' \
    'name: ostro-worker-heavy' \
    'minReplicaCount: 0' \
    'maxReplicaCount: 50' \
    'pollingInterval: 15' \
    'cooldownPeriod: 120' \
    'type: metrics-api' \
    '/v1/queues/heavy/demand' \
    'valueLocation: "demand"' \
    'targetValue: "8"' \
    'activationTargetValue: "0"' \
    'authMode: "tls"' \
    'name: ostro-demand-auth'

need keda/scaledobject-prometheus.yaml \
    'kind: ScaledObject' \
    'minReplicaCount: 0' \
    'type: prometheus' \
    'sum(ostro_queue_worker_demand{queue="heavy"})' \
    'activationThreshold: "0"'

need keda/trigger-auth.yaml \
    'kind: TriggerAuthentication' \
    'name: ostro-demand-auth' \
    'parameter: ca' \
    'kind: Secret'
# Placeholders only: no real key material in the repository.
if grep -q 'BEGIN [A-Z ]*PRIVATE KEY' keda/trigger-auth.yaml k8s/*.yaml; then
    err "a manifest contains a private key"
fi

need k8s/worker-deployment.yaml \
    'kind: Deployment' \
    'name: ostro-worker-heavy' \
    '- worker' \
    '- --join' \
    '- --ca-hash' \
    '- --labels' \
    '- --slots' \
    '- --cache-dir' \
    'name: OSTRO_WORKER_TOKEN' \
    'secretKeyRef:' \
    'configMapKeyRef:' \
    'emptyDir:' \
    'terminationGracePeriodSeconds:' \
    'resources:'
if grep -qE 'privileged: *true|allowPrivilegeEscalation: *true|hostNetwork: *true|hostPID: *true' \
    k8s/worker-deployment.yaml; then
    err "k8s/worker-deployment.yaml: privileged settings"
fi
# The token is never passed on the command line.
if grep -q -- '- --token' k8s/worker-deployment.yaml; then
    err "k8s/worker-deployment.yaml: token on the command line"
fi

need k8s/server-statefulset.yaml \
    'kind: StatefulSet' \
    '- server' \
    'containerPort: 7700' \
    'containerPort: 7701' \
    'mode = "files"' \
    'volumeClaimTemplates:' \
    'path: /readyz' \
    'terminationGracePeriodSeconds:'

# The worker slots and the ScaledObject target agree (one replica per --slots runs).
slots=$(grep -A1 -- '- --slots' k8s/worker-deployment.yaml | tail -1 | tr -dc '0-9')
target=$(grep 'targetValue:' keda/scaledobject-metrics-api.yaml | grep -v activation | tr -dc '0-9')
[ "$slots" = "$target" ] || err "worker --slots ($slots) != ScaledObject targetValue ($target)"

if command -v python3 >/dev/null 2>&1 && python3 -c 'import yaml' 2>/dev/null; then
    python3 - "${files[@]}" <<'PY' || fail=1
import sys
import yaml

bad = 0
for path in sys.argv[1:]:
    try:
        with open(path, encoding="utf-8") as fh:
            docs = [d for d in yaml.safe_load_all(fh) if d is not None]
    except yaml.YAMLError as e:
        print(f"check-manifests: {path}: YAML error: {e}", file=sys.stderr)
        bad = 1
        continue
    if not docs:
        print(f"check-manifests: {path}: no documents", file=sys.stderr)
        bad = 1
    for d in docs:
        if not isinstance(d, dict) or "apiVersion" not in d or "kind" not in d:
            print(f"check-manifests: {path}: a document lacks apiVersion/kind", file=sys.stderr)
            bad = 1
            continue
        if not d.get("metadata", {}).get("name"):
            print(f"check-manifests: {path}: {d['kind']} without metadata.name", file=sys.stderr)
            bad = 1
        if d["kind"] == "ScaledObject":
            spec = d["spec"]
            if spec.get("minReplicaCount") != 0:
                print(f"check-manifests: {path}: minReplicaCount must be 0", file=sys.stderr)
                bad = 1
            for t in spec.get("triggers", []):
                md = t.get("metadata", {})
                act = md.get("activationTargetValue", md.get("activationThreshold"))
                if act != "0":
                    print(f"check-manifests: {path}: activation must be \"0\" (got {act!r})", file=sys.stderr)
                    bad = 1
        if d["kind"] == "Deployment":
            pod = d["spec"]["template"]["spec"]
            if not pod.get("terminationGracePeriodSeconds"):
                print(f"check-manifests: {path}: terminationGracePeriodSeconds missing", file=sys.stderr)
                bad = 1
sys.exit(bad)
PY
    echo "check-manifests: YAML parse step done (PyYAML)"
else
    echo "check-manifests: PyYAML not available; structural grep checks only"
fi

if [ "$fail" -ne 0 ]; then
    echo "check-manifests: FAILED" >&2
    exit 1
fi
echo "check-manifests: OK (${#files[@]} files)"
