#!/usr/bin/env bash
# Offline validation of the Ostro Helm chart (Phase 14, 14-13, D-P14-26, DIST-02).
#
# usage: scripts/helm-check.sh
#
# For every deploy/helm/ostro/ci/*-values.yaml:
#   1. helm lint --strict (values.schema.json is enforced);
#   2. helm template ... --kube-version 1.34.12 | kubeconform -strict against the
#      Kubernetes 1.34.12 schemas and the KEDA 2.21.1 CRD schemas in deploy/helm/schemas
#      (converted from the pinned CRD file, see deploy/helm/schemas/SOURCE.md);
# then validates deploy/helm/ostro/examples/keda/*.yaml the same way and runs structural
# assertions on the rendered manifests.
#
# It never installs anything and never contacts a cluster: KUBECONFIG=/dev/null, only
# `helm lint` and `helm template` (no --validate, no lookup). Network use: the pinned helm
# and kubeconform downloads (target/tools) and the Kubernetes JSON schemas kubeconform
# fetches into target/tools/kubeconform-cache on first run.
#
# Tools: `helm` from $HELM or PATH, else helm v3.17.0 downloaded into target/tools (sha256
# pinned below; it equals the .sha256sum get.helm.sh publishes, checked 2026-11-27);
# kubeconform 0.8.0 from scripts/fetch-tools.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/deploy/helm/ostro"
SCHEMAS="$ROOT/deploy/helm/schemas"
TOOLS="$ROOT/target/tools"
CACHE="$TOOLS/kubeconform-cache"
KUBE_VERSION=1.34.12
HELM_VERSION=3.17.0
HELM_URL="https://get.helm.sh/helm-v$HELM_VERSION-linux-amd64.tar.gz"
HELM_SHA256=fb5d12662fde6eeff36ac4ccacbf3abed96b0ee2de07afdde4edb14e613aee24

export KUBECONFIG=/dev/null
unset HELM_KUBECONTEXT HELM_KUBEAPISERVER HELM_KUBETOKEN

die() { echo "helm-check: FAIL: $*" >&2; exit 1; }

fetch_helm() {
  local dest="$TOOLS/helm-$HELM_VERSION"
  if [ -x "$dest/helm" ] && [ -f "$dest/.ostro-tool-ok" ]; then
    echo "$dest/helm"
    return
  fi
  local dl ex
  dl=$(mktemp -d "$ROOT/target/helm-dl.XXXXXX")
  ex=$(mktemp -d "$ROOT/target/helm-ex.XXXXXX")
  curl -sSLf --retry 3 -o "$dl/helm.tgz" "$HELM_URL" >&2
  local got
  got=$(sha256sum "$dl/helm.tgz" | cut -d' ' -f1)
  if [ "$got" != "$HELM_SHA256" ]; then
    local published
    published=$(curl -sSLf --retry 3 "$HELM_URL.sha256sum" | cut -d' ' -f1)
    rm -rf "$dl" "$ex"
    die "helm $HELM_VERSION checksum mismatch: got $got, pinned $HELM_SHA256 (get.helm.sh publishes $published)"
  fi
  tar -xzf "$dl/helm.tgz" -C "$ex" linux-amd64/helm
  rm -rf "$dest"
  mkdir -p "$dest"
  mv "$ex/linux-amd64/helm" "$dest/helm"
  touch "$dest/.ostro-tool-ok"
  rm -rf "$dl" "$ex"
  echo "$dest/helm"
}

mkdir -p "$TOOLS" "$CACHE"
if [ -n "${HELM:-}" ]; then
  :
elif command -v helm >/dev/null 2>&1; then
  HELM=$(command -v helm)
else
  HELM=$(fetch_helm)
fi
KUBECONFORM=$(bash "$ROOT/scripts/fetch-tools.sh" kubeconform | sed -n 's/^kubeconform=//p')
[ -x "$KUBECONFORM" ] || die "kubeconform not available"
echo "helm-check: $("$HELM" version --short), kubeconform $("$KUBECONFORM" -v), Kubernetes $KUBE_VERSION"

for f in scaledobject_v1alpha1 triggerauthentication_v1alpha1 scaledjob_v1alpha1 clustertriggerauthentication_v1alpha1; do
  [ -f "$SCHEMAS/$f.json" ] || die "missing KEDA schema $SCHEMAS/$f.json"
done

kubeconform_run() { # files... (default: stdin)
  "$KUBECONFORM" -strict -summary -output text \
    -kubernetes-version "$KUBE_VERSION" \
    -schema-location default \
    -schema-location "$SCHEMAS/{{ .ResourceKind }}_{{ .ResourceAPIVersion }}.json" \
    -cache "$CACHE" "${@:--}"
}

render() { # values file -> manifests on stdout
  "$HELM" template t "$CHART" -f "$1" --namespace ostro --kube-version "$KUBE_VERSION"
}

check_summary() { # kubeconform output; label
  echo "$1"
  if grep -Eq "Invalid: [1-9]|Errors: [1-9]|Skipped: [1-9]" <<<"$1"; then
    die "kubeconform reported invalid, error or skipped resources ($2)"
  fi
  grep -q "Summary:" <<<"$1" || die "no kubeconform summary ($2)"
}

shopt -s nullglob
values=("$CHART"/ci/*-values.yaml)
[ "${#values[@]}" -ge 4 ] || die "expected at least four ci/*-values.yaml files"
for v in "${values[@]}"; do
  name=$(basename "$v")
  echo "== $name"
  "$HELM" lint --strict "$CHART" -f "$v" --kube-version "$KUBE_VERSION"
  out=$(render "$v")
  [ -n "$out" ] || die "empty render ($name)"
  summary=$(kubeconform_run <<<"$out" 2>&1) || { echo "$summary"; die "kubeconform failed ($name)"; }
  check_summary "$summary" "$name"
done

echo "== examples/keda"
examples=("$CHART"/examples/keda/*.yaml)
[ "${#examples[@]}" -eq 3 ] || die "expected three examples/keda manifests"
summary=$(kubeconform_run "${examples[@]}" 2>&1) \
  || { echo "$summary"; die "kubeconform failed (examples)"; }
check_summary "$summary" "examples"

# ---------------------------------------------------------------------------------------
# Structural assertions (python3 -I with PyYAML when present, else grep-level checks).
echo "== structural assertions"
assert() { # description; command...
  local d=$1; shift
  if "$@"; then echo "ok   $d"; else die "$d"; fi
}
doc_named() { # manifests; kind; name -> that one document
  awk -v k="kind: $2" -v n="  name: $3" 'BEGIN{RS="\n---\n"} {
    split($0, L, "\n"); hk = 0; hn = 0
    for (i in L) { if (L[i] == k) hk = 1; if (L[i] == n) hn = 1 }
    if (hk && hn) print }' <<<"$1"
}
count_kind() { awk -v k="kind: $2" '$0 == k {n++} END {print n + 0}' <<<"$1"; }

def=$(render "$CHART/ci/default-values.yaml")
sts=$(doc_named "$def" StatefulSet t-ostro-server)
assert "default: one StatefulSet" test "$(count_kind "$def" StatefulSet)" -eq 1
assert "default: StatefulSet replicas 3" grep -q "^  replicas: 3$" <<<"$sts"
assert "default: volumeClaimTemplates" grep -q "^  volumeClaimTemplates:" <<<"$sts"
assert "default: podManagementPolicy Parallel" grep -q "podManagementPolicy: Parallel" <<<"$sts"
assert "default: pod anti-affinity" grep -q "podAntiAffinity:" <<<"$sts"
assert "default: runAsNonRoot" grep -q "runAsNonRoot: true" <<<"$sts"
assert "default: readOnlyRootFilesystem" grep -q "readOnlyRootFilesystem: true" <<<"$sts"
assert "default: node id from the pod name" grep -q -- "/etc/ostro/config/\$(POD_NAME).toml" <<<"$sts"
assert "default: readiness /readyz" grep -q "path: /readyz" <<<"$sts"
assert "default: liveness /healthz" grep -q "path: /healthz" <<<"$sts"
assert "default: PDB minAvailable 2" grep -q "minAvailable: 2" <<<"$def"
assert "default: headless Service publishes not-ready pods" grep -q "publishNotReadyAddresses: true" <<<"$def"
for i in 0 1 2; do
  assert "default: config for ordinal $i (node_id $((i + 1)))" \
    grep -Pzq "t-ostro-server-$i\\.toml: \\|\\n(.*\\n)*?    node_id = $((i + 1))\\n" <<<"$def"
done
assert "default: two peer tables per node (2 x 3)" test "$(grep -c '^    \[\[cluster.peers\]\]$' <<<"$def")" -eq 6
assert "default: shared cluster CA files" grep -q 'ca_key_file = "/etc/ostro/cluster-ca/ca.key"' <<<"$def"
worker=$(doc_named "$def" Deployment t-ostro-worker)
assert "default: worker terminationGracePeriodSeconds" grep -q "terminationGracePeriodSeconds: 120" <<<"$worker"
assert "default: worker has no Service" test -z "$(doc_named "$def" Service t-ostro-worker)"
assert "default: generated Secrets kept across upgrades" test "$(grep -c "helm.sh/resource-policy: keep" <<<"$def")" -eq 3
assert "default: no NetworkPolicy, no ScaledObject" test "$(count_kind "$def" NetworkPolicy)$(count_kind "$def" ScaledObject)" = "00"

# The generated cluster CA: CN "ostro cluster <cluster_id>", a CA, and ca-hash = sha256(DER)
# (what `ostro worker --ca-hash` pins). Needs openssl; skipped with a note without it.
ca=$(doc_named "$def" Secret t-ostro-cluster-ca)
assert "default: generated CA Secret" test -n "$ca"
if command -v openssl >/dev/null 2>&1; then
  b64() { sed -n "s/^  $1: \"\(.*\)\"\$/\1/p" <<<"$ca" | base64 -d; }
  crt=$(b64 ca.crt)
  subj=$(openssl x509 -noout -subject -nameopt RFC2253 <<<"$crt")
  assert "default: CA CN is 'ostro cluster t-ostro'" test "$subj" = "subject=CN=ostro cluster t-ostro"
  assert "default: CA basicConstraints CA:TRUE" grep -q "CA:TRUE" <<<"$(openssl x509 -noout -ext basicConstraints <<<"$crt")"
  assert "default: CA key is EC P-256" grep -q "prime256v1\|P-256" <<<"$(b64 ca.key | openssl pkey -noout -text)"
  want="sha256:$(openssl x509 -outform DER <<<"$crt" | sha256sum | cut -d' ' -f1)"
  assert "default: ca-hash = sha256(CA DER)" test "$(b64 ca-hash)" = "$want"
else
  echo "note: openssl missing, generated-CA checks skipped"
fi

# existingSecret everywhere: no Secret object, no literal bearer or key material.
prod=$(render "$CHART/ci/keda-values.yaml")
assert "existingSecret: no Secret rendered" test "$(count_kind "$prod" Secret)" -eq 0
if grep -Eq "ostro_otk_|BEGIN (EC |RSA )?PRIVATE KEY" <<<"$prod"; then
  die "existingSecret: a literal bearer or private key was rendered"
fi
echo "ok   existingSecret: no literal bearer or private key"
assert "keda: ScaledObject" test "$(count_kind "$prod" ScaledObject)" -eq 1
assert "keda: TriggerAuthentication" test "$(count_kind "$prod" TriggerAuthentication)" -eq 1
assert "keda: minReplicaCount 0" grep -q "minReplicaCount: 0" <<<"$prod"
assert "keda: bearer authMode" grep -q "authMode: bearer" <<<"$prod"
assert "keda: one metrics-api trigger per queue" test "$(grep -c "type: metrics-api" <<<"$prod")" -eq 2
if grep -q "^  replicas:" <<<"$(doc_named "$prod" Deployment t-ostro-worker)"; then
  die "keda: the worker Deployment sets replicas (KEDA owns them)"
fi
echo "ok   keda: worker replicas owned by KEDA"
assert "keda: old sealing key files" grep -q 'old_sealing_key_files = \["/etc/ostro/sealing-old/key-2026-06"\]' <<<"$prod"

# NetworkPolicies: five; the peer policy admits only server pods on cluster/replication.
np=$(render "$CHART/ci/netpol-values.yaml")
assert "netpol: five NetworkPolicies" test "$(count_kind "$np" NetworkPolicy)" -eq 5
peers=$(doc_named "$np" NetworkPolicy t-ostro-server-peers)
assert "netpol: peer policy exists" test -n "$peers"
assert "netpol: peer policy sources are server pods only" \
  test "$(grep -c "app.kubernetes.io/component:" <<<"$peers")" -eq 3
if grep -q "app.kubernetes.io/component: worker" <<<"$peers"; then
  die "netpol: the peer policy admits worker pods"
fi
if grep -q "namespaceSelector" <<<"$peers"; then
  die "netpol: the peer policy admits other namespaces"
fi
echo "ok   netpol: peer policy admits no worker pods and no namespaces"
assert "netpol: cluster + replication ports in the peer policy" \
  test "$(grep -cE "port: (7701|7702)$" <<<"$peers")" -eq 2
assert "netpol: DNS egress" test "$(grep -c "port: 53$" <<<"$np")" -eq 4
assert "netpol: prometheus scaler query" grep -q 'ostro_queue_worker_demand{queue=~' <<<"$np"

one=$(render "$CHART/ci/single-node-values.yaml")
assert "single-node: replicas 1" grep -q "^  replicas: 1$" <<<"$one"
assert "single-node: no PDB" test "$(count_kind "$one" PodDisruptionBudget)" -eq 0
assert "single-node: no worker Deployment" test "$(count_kind "$one" Deployment)" -eq 0
if grep -q '^    \[\[cluster.peers\]\]$' <<<"$one"; then
  die "single-node: [[cluster.peers]] rendered"
fi
echo "ok   single-node: no peers"

# Negative: combinations the server would refuse fail at render time.
neg() { # description; helm args...
  local d=$1; shift
  if "$HELM" template t "$CHART" "$@" >/dev/null 2>&1; then die "render accepted: $d"; fi
  echo "ok   refuses $d"
}
neg "replicas 2" --set server.replicas=2
neg "auth none without allowInsecureNone" --set auth.mode=none
neg "malformed bootstrap token" --set auth.bootstrapToken=nope
neg "files TLS without a Secret" --set server.apiTls.mode=files
neg "existing CA without worker.caHash" --set tls.clusterCa.existingSecret=x
neg "unknown value key" --set server.bogus=1

echo "helm-check: PASS"
