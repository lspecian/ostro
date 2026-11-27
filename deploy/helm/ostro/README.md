# Ostro Helm chart

Ostro `1.0.0-rc.1` on Kubernetes (>= 1.29): a static 3-voter server StatefulSet, a remote
worker pool, optional KEDA 2.21 scaling and five NetworkPolicies (Phase 14, D-P14-26,
DIST-02).

> **Status.** The chart is validated offline only (`scripts/helm-check.sh`: `helm lint
> --strict`, `helm template | kubeconform -strict` against the Kubernetes 1.34.12 and KEDA
> 2.21.1 schemas, structural assertions). It has not been installed on a cluster yet; a
> live install (kind/k3d or a cluster you control) is an open human step.

## What it renders

| Object | Purpose |
|--------|---------|
| StatefulSet `<fullname>-server` | `server.replicas` voters (1, 3 or 5), `podManagementPolicy: Parallel`, PVC per pod (`/data`), pod anti-affinity, `/readyz` readiness, `/healthz` liveness + startup probe, SIGTERM = controlled shutdown (partition handoff), `terminationGracePeriodSeconds: 60` |
| Service `<fullname>-server-headless` | stable pod DNS for `[cluster] advertise` and `[[cluster.peers]]`; `publishNotReadyAddresses: true` |
| Service `<fullname>-server` | client port: REST, gRPC, `/ui`, `/metrics`, `/v1/queues/<q>/demand`, worker joins |
| PodDisruptionBudget | `minAvailable: 2` (metadata quorum), only when replicas > 1 |
| ConfigMap `<fullname>-server-config` | one node config per ordinal (`<fullname>-server-<i>.toml`) |
| Secrets `<fullname>-{auth,sealing,cluster-ca}` | only when no `existingSecret` is given, generated at install (see "Secrets") |
| Deployment `<fullname>-worker` | remote workers (`ostro worker`), SIGTERM drain, no Service |
| ScaledObject (+ TriggerAuthentication) | `keda.enabled`; prometheus or metrics-api, `minReplicaCount: 0` |
| 5 NetworkPolicies | `networkPolicy.enabled` (matrix below) |

### Node identity

The scratch image has no shell, so the chart does not compute ids at runtime. Each pod
starts with `ostro server --config /etc/ostro/config/$(POD_NAME).toml` (`POD_NAME` from the
downward API); the ConfigMap holds one file per ordinal with `[node] node_id = <i> + 1`,
`[cluster] advertise = ["<pod>.<headless>.<ns>.svc.<domain>"]` and every other ordinal as
`[[cluster.peers]]` (`addr` on the cluster port, `repl_addr` on the replication port).
The server binds `node_id` and `cluster_id` into `<data>/node.toml` at first boot.

All voters share the cluster CA (`[cluster.tls] ca_cert_file` / `ca_key_file`, Secret
mounted at `/etc/ostro/cluster-ca`); each issues its own node certificate for its pod DNS
name. The CA's CN must be `ostro cluster <clusterId>`.

## Install (human step)

```sh
# Production: create the Secrets first (see "Secrets"), then
helm install ostro oci://<registry>/charts/ostro --version 1.0.0-rc.1 \
  --namespace ostro --create-namespace -f my-values.yaml
```

The OCI location is set when the chart is published (release workflow); until then install
from a checkout: `helm install ostro deploy/helm/ostro -n ostro --create-namespace -f
my-values.yaml`.

After install:

1. Read the bootstrap admin bearer (NOTES.txt prints the command), log in with
   `OSTRO_SERVER=https://<client endpoint> OSTRO_TOKEN=<bearer> ostro auth whoami`.
2. Create a worker join token (`ostro worker token create --ttl 720h`) and store it in the
   join Secret (`<fullname>-worker-join`, key `token`): the worker pods start once it
   exists.
3. With KEDA metrics-api: create a read token (`ostro auth token create --name keda
   --scope read --system`) and store it in `keda.metricsApi.tokenSecret.name`.

## Secrets

| Secret | Keys | Used as | Generated when |
|--------|------|---------|----------------|
| `auth.existingSecret` | `token` (`auth.existingSecretKey`) | `OSTRO_BOOTSTRAP_TOKEN`: the bootstrap admin bearer (`ostro_otk_<16>_<52>`), created once by the meta leader when no admin exists | neither `existingSecret` nor `bootstrapToken` is set (token mode) |
| `sealing.existingSecret` | `key` (`sealing.existingSecretKey`) | `OSTRO_SEALING_KEY`: base64 of 32 bytes, seals stored secrets, the token key and the cluster CA in metadata | no `existingSecret` |
| `sealing.oldKeys.existingSecret` | `sealing.oldKeys.keys` | `[secrets] old_sealing_key_files` (opening only, during a rotation) | never |
| `tls.clusterCa.existingSecret` | `ca.crt`, `ca.key` (`certKey`/`keyKey`) | `[cluster.tls] ca_cert_file` / `ca_key_file` | no `existingSecret` (ECDSA P-256, 10 years, plus `ca-hash`) |
| `worker.joinToken.existingSecret` | `token` | `OSTRO_WORKER_TOKEN` (`wkr_…`) | never (default name `<fullname>-worker-join`) |
| `server.apiTls.existingSecret` | `tls.crt`, `tls.key` | `[api.tls] mode = "files"` (reloaded on renewal) | never |

Generated Secrets are a dev convenience. They are rendered only at `helm install` and carry
`helm.sh/resource-policy: keep`: upgrades never regenerate them (a new sealing key would
make sealed data unreadable, a new CA would split the cluster) and `helm uninstall` leaves
them in place. Back them up. In production create them yourself, for example:

```sh
kubectl -n ostro create secret generic ostro-sealing --from-literal=key="$(head -c 32 /dev/urandom | base64)"
kubectl -n ostro create secret generic ostro-cluster-ca --from-file=ca.crt --from-file=ca.key
```

A CA from your own PKI must be a CA certificate with CN `ostro cluster <clusterId>`; pass
`worker.caHash` = `sha256:<hex of the CA certificate DER>` (`openssl x509 -in ca.crt
-outform DER | sha256sum`).

**Sealing key rotation:** put the old key into a Secret, list it in `sealing.oldKeys`,
replace the key in `sealing.existingSecret`, upgrade, run `ostro secret reseal`, then drop
the old key.

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `ostro/ostro` | image (neutral default, set at publish time) |
| `image.tag` | appVersion | image tag |
| `image.pullPolicy` | `IfNotPresent` | |
| `imagePullSecrets` | `[]` | |
| `clusterDomain` | `cluster.local` | DNS suffix of peer addresses |
| `serviceAccount.create` / `.name` / `.annotations` | `true` / fullname / `{}` | no API token is mounted |
| `podSecurityContext` | non-root 65532, fsGroup 65532, seccomp RuntimeDefault | |
| `securityContext` | read-only root FS, drop ALL, no privilege escalation | |
| `server.replicas` | `3` | 1, 3 or 5 voters (static; see "Upgrades") |
| `server.clusterId` | fullname | `[cluster] cluster_id` |
| `server.partitions` | `16` | `[node] partitions` (fixed at first boot) |
| `server.ports.client` / `.cluster` / `.replication` | `7700` / `7701` / `7702` | client listener; cluster listener (MetaPeer, Replication, worker protocol, mTLS); `[cluster] replication_listen` (0 = off) |
| `server.logLevel` | `info` | `[log] level` (JSON format) |
| `server.apiTls.mode` | `self-signed` | `self-signed` (CA-issued), `files`, `off` |
| `server.apiTls.existingSecret` | `""` | TLS Secret for `files` |
| `server.ui.enabled` | `false` | `[ui] enabled` |
| `server.resources` | 1 CPU / 2 Gi request, 8 Gi limit | |
| `server.storage.size` / `.storageClass` | `20Gi` / default | PVC per pod |
| `server.antiAffinity` | `soft` | `soft` (preferred) or `hard` (required) |
| `server.pdb.enabled` / `.minAvailable` | `true` / `2` | |
| `server.terminationGracePeriodSeconds` | `60` | covers `[cluster] shutdown_handoff_timeout` (30 s) |
| `server.service.type` / `.annotations` | `ClusterIP` / `{}` | client Service |
| `server.extraEnv` | `[]` | e.g. `OSTRO_<SECTION>__<KEY>` overrides |
| `server.extraConfig` | `""` | raw TOML appended to every node config (only tables the chart does not render: `[engine]`, `[replication]`, `[backup]`, `[telemetry]`, `[tenants]`, `[audit]`, `[security.modules]`, …) |
| `server.podAnnotations` / `.nodeSelector` / `.tolerations` | empty | |
| `worker.enabled` | `true` | worker Deployment |
| `worker.replicas` | `1` | ignored with KEDA |
| `worker.queues` | `[default]` | queues KEDA scales on |
| `worker.labels` | `{}` | `OSTRO_WORKER_LABELS` (queue selectors) |
| `worker.slots` | `4` | `OSTRO_WORKER_SLOTS` |
| `worker.memoryBytes` | `1073741824` | `OSTRO_WORKER_MEMORY_BYTES` |
| `worker.joinToken.existingSecret` / `.key` | `<fullname>-worker-join` / `token` | join token Secret |
| `worker.caHash` | from the generated CA | `OSTRO_WORKER_CA_HASH`; required with `tls.clusterCa.existingSecret` |
| `worker.terminationGracePeriodSeconds` | `120` | drain budget |
| `worker.cacheSize` | `2Gi` | AOT cache emptyDir |
| `worker.resources` | 1 CPU / 1 Gi request, 2 Gi limit | |
| `auth.mode` | `token` | `token` or `none` |
| `auth.allowInsecureNone` | `false` | required for `none` |
| `auth.existingSecret` / `.existingSecretKey` | `""` / `token` | bootstrap bearer Secret |
| `auth.bootstrapToken` | `""` | literal bearer (dev only) |
| `sealing.existingSecret` / `.existingSecretKey` | `""` / `key` | sealing key Secret |
| `sealing.oldKeys.existingSecret` / `.keys` | `""` / `[]` | old sealing keys |
| `tls.clusterCa.existingSecret` / `.certKey` / `.keyKey` | `""` / `ca.crt` / `ca.key` | shared cluster CA |
| `keda.enabled` | `false` | ScaledObject for the worker Deployment |
| `keda.scaler` | `prometheus` | `prometheus` or `metrics-api` |
| `keda.minReplicas` / `.maxReplicas` | `0` / `10` | |
| `keda.target` | `4` | demand per replica (= `worker.slots`) |
| `keda.pollingInterval` / `.cooldownPeriod` | `15` / `120` | seconds |
| `keda.prometheus.serverAddress` | `""` | required for prometheus |
| `keda.metricsApi.tokenSecret.name` / `.key` | `""` / `token` | read-scoped bearer (required for metrics-api in token mode) |
| `keda.metricsApi.unsafeSsl` | `false` | skip verification of the client listener certificate |
| `networkPolicy.enabled` | `false` | the five policies |
| `networkPolicy.clientFromNamespaces` | `[]` | namespaces allowed on the client port besides the release namespace |
| `networkPolicy.dnsNamespace` | `kube-system` | DNS egress target |
| `networkPolicy.serverEgress` / `.workerEgress` | `[]` | extra egress rules |

## KEDA

`keda.scaler: prometheus` queries `sum(ostro_queue_worker_demand{queue=~"<queues>"})`: each
server exports the demand of the partitions it leads, so the sum is the cluster demand.
`keda.scaler: metrics-api` polls `GET /v1/queues/<q>/demand` on the client Service (one
trigger per queue, `valueLocation: demand`) with a bearer from a TriggerAuthentication.
Both use activation value `0` (the first ready run wakes a pool at zero) and
`ceil(demand / target)` replicas; the pool returns to zero only after `cooldownPeriod` with
no demand, and demand counts runs still executing on workers.

The default self-signed listener certificate does not name the Service, so KEDA cannot
verify it: use `server.apiTls.mode: files` with a certificate for
`<fullname>-server.<namespace>.svc` (cert-manager), or `keda.metricsApi.unsafeSsl: true`.
Standalone manifests are in `examples/keda/` (they supersede `deploy/keda/` from Phase 7).

## NetworkPolicy matrix

| # | Policy | Selects | Direction | Allows |
|---|--------|---------|-----------|--------|
| 1 | `<fullname>-server-client` | server pods | ingress | client port from any pod in the release namespace and from `clientFromNamespaces` |
| 2 | `<fullname>-server-peers` | server pods | ingress | cluster + replication ports from server pods only |
| 3 | `<fullname>-server-workers` | server pods | ingress | cluster port (worker protocol) from worker pods only |
| 4 | `<fullname>-worker-egress` | worker pods | egress | servers (client + cluster ports) and DNS, plus `workerEgress` |
| 5 | `<fullname>-server-egress` | server pods | egress | servers (client, cluster, replication), DNS, plus `serverEgress` |

The worker protocol shares the cluster listener with MetaPeer and Replication, so policy 3
opens only that port to workers; the dedicated replication port stays server-only.
Prometheus (metrics on the client port), KEDA and ingress controllers need their namespaces
in `clientFromNamespaces`. Connectors, the backup bucket, OTLP collectors and secret
providers need `serverEgress` rules.

## Upgrades

- Image upgrades roll one pod at a time (RollingUpdate, highest ordinal first); the PDB
  keeps two voters up. Each pod hands off its partitions on SIGTERM.
- `server.replicas` is a static voter set: changing it on a running release makes the
  configured peers differ from the metadata voters, which the server refuses ("grow via
  Phase 13"). Grow or shrink a running cluster with `ostro cluster join` / `ostro cluster
  remove` (docs/cluster-operations.md) instead.
- `server.clusterId`, `server.partitions` and the cluster CA are bound at first boot and
  must not change.
- Generated Secrets are not regenerated on upgrade (see "Secrets").
