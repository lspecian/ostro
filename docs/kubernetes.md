# Ostro on Kubernetes

The Helm chart in [deploy/helm/ostro](../deploy/helm/ostro/README.md) runs Ostro on Kubernetes
1.29 or newer: a StatefulSet of 1, 3 or 5 server voters with a PersistentVolumeClaim each, a
Deployment of remote workers, optional KEDA scaling of the workers (to zero), and five optional
NetworkPolicies. This guide walks through an install and the choices it involves; the chart
README holds the complete values table.

> **Status.** The chart is validated offline (`bash scripts/helm-check.sh`: `helm lint
> --strict` and `helm template | kubeconform -strict` against the Kubernetes and KEDA schemas).
> It has not yet been installed on a live cluster as part of the release checks. Every `helm`
> and `kubectl` command below runs against **a cluster you choose**; nothing in the repository
> runs them for you.

## 1. What gets deployed

| Object | Purpose |
|---|---|
| StatefulSet `<release>-server` | the voters (`server.replicas`: 1, 3 or 5), PVC per pod at `/data`, `/readyz` readiness, `/healthz` liveness, soft anti-affinity; SIGTERM hands off the pod's partitions before it stops |
| Services `<release>-server`, `<release>-server-headless` | the client port (REST, gRPC, UI, `/metrics`, worker joins) and stable pod DNS for the voters |
| PodDisruptionBudget | keeps 2 of 3 voters up (only with more than one replica) |
| ConfigMap | one node configuration per ordinal (`node_id`, advertise address, peers) |
| Secrets | bootstrap admin bearer, sealing key, cluster CA, generated at install unless you supply them (§3) |
| Deployment `<release>-worker` | remote workers for `worker.queues` (drain on SIGTERM) |
| ScaledObject | with `keda.enabled` (§5) |
| NetworkPolicies | with `networkPolicy.enabled` (§6) |

Pods run the scratch image ([install.md](install.md#5-container-image)) as uid/gid 65532 with a
read-only root filesystem, all capabilities dropped and `RuntimeDefault` seccomp.

## 2. Install and upgrade

```sh
# From a release (the OCI location is in the release notes):
helm install ostro oci://<registry>/charts/ostro --version <chart version> \
  --namespace ostro --create-namespace -f my-values.yaml

# From a checkout:
helm install ostro deploy/helm/ostro -n ostro --create-namespace -f my-values.yaml

# Upgrades (image or values):
helm upgrade ostro deploy/helm/ostro -n ostro -f my-values.yaml
```

After the install:

1. Read the bootstrap admin bearer (the chart's `NOTES.txt` prints the command) and check it:
   `OSTRO_SERVER=https://<client endpoint> OSTRO_TOKEN=<bearer> ostro auth whoami`.
   Create scoped tokens from it ([tenancy-and-auth.md](tenancy-and-auth.md#4-tokens)).
2. Create a worker join token (`ostro worker token create --ttl 720h`) and store it in the
   Secret `<release>-worker-join`, key `token`. The worker pods start once it exists; the chart
   cannot create it because only the running cluster can issue it.
3. With the KEDA `metrics-api` scaler: create a read token
   (`ostro auth token create --name keda --scope read --system`) and store it in the Secret
   named by `keda.metricsApi.tokenSecret.name`.

Upgrades roll one server pod at a time, highest ordinal first; each pod hands off its partitions
on SIGTERM (`terminationGracePeriodSeconds: 60`). For upgrades across Ostro versions follow
[upgrade-runbook.md](upgrade-runbook.md): the same rules apply in pods.

Fixed at install, never change them on a running release: `server.replicas` (the voter set is
static; grow or shrink a running cluster with `ostro cluster join` / `ostro cluster remove`,
[cluster-operations.md](cluster-operations.md)), `server.clusterId`, `server.partitions` and the
cluster CA.

## 3. Values that matter

The complete table is in the [chart README](../deploy/helm/ostro/README.md#values). The main
choices:

| Value | Default | Choose |
|---|---|---|
| `server.replicas` | `3` | 1 (no HA), 3 or 5 voters |
| `server.partitions` | `16` | the partition count `P` ([operations-sizing.md](operations-sizing.md#partitions-p)) |
| `server.resources`, `server.storage.size` | 1 CPU / 2 GiB request, 8 GiB limit; 20 GiB | per [operations-sizing.md](operations-sizing.md) |
| `server.apiTls.mode` | `self-signed` | `files` with `server.apiTls.existingSecret` (for example from cert-manager) for clients and KEDA; `off` only behind a mesh |
| `server.ui.enabled` | `false` | serve `/ui` (token sign-in) |
| `server.maxConnections` | `0` (half the soft open-file limit, at most 16384) | client connections per server; the container runtime's `nofile` limit applies ([chart README](../deploy/helm/ostro/README.md#open-files)) |
| `server.extraConfig` | `""` | raw TOML for sections the chart does not render: `[engine]`, `[backup]`, `[telemetry]`, `[tenants]`, `[audit]`, `[security.modules]`, ... |
| `server.extraEnv` | `[]` | `OSTRO_<SECTION>__<KEY>` overrides |
| `auth.mode` | `token` | keep `token`; `none` needs `auth.allowInsecureNone` |
| `worker.enabled`, `worker.queues`, `worker.slots` | `true`, `[default]`, `4` | the queues the pool serves and its slots per pod |
| `keda.enabled` | `false` | §5 |
| `networkPolicy.enabled` | `false` | §6 |

## 4. Secrets

| Secret | Holds | Value to set |
|---|---|---|
| bootstrap admin bearer | `OSTRO_BOOTSTRAP_TOKEN` | `auth.existingSecret` (key `auth.existingSecretKey`) |
| sealing key | `OSTRO_SEALING_KEY` (base64 of 32 bytes) | `sealing.existingSecret` |
| old sealing keys | `[secrets] old_sealing_key_files`, during a rotation | `sealing.oldKeys.existingSecret`, `sealing.oldKeys.keys` |
| cluster CA | `[cluster.tls] ca_cert_file` / `ca_key_file` (CN `ostro cluster <clusterId>`) | `tls.clusterCa.existingSecret` |
| worker join token | `OSTRO_WORKER_TOKEN` | `worker.joinToken.existingSecret` (default `<release>-worker-join`) |
| API listener certificate | `[api.tls] mode = "files"` | `server.apiTls.existingSecret` (`kubernetes.io/tls`) |

When you set none, the chart generates the bearer, the sealing key and the cluster CA **at
install only**, marked `helm.sh/resource-policy: keep`: upgrades never regenerate them and
`helm uninstall` leaves them. This is a convenience for evaluation. For production, create the
Secrets yourself and back them up; a lost sealing key makes every sealed secret unreadable, and
a new CA splits the cluster:

```sh
kubectl -n ostro create secret generic ostro-sealing --from-literal=key="$(head -c 32 /dev/urandom | base64)"
kubectl -n ostro create secret generic ostro-cluster-ca --from-file=ca.crt --from-file=ca.key
```

With your own CA, set `worker.caHash` to `sha256:<hex of the CA certificate DER>`.

**Sealing-key rotation** follows [secrets.md §5.2](secrets.md#52-rotation): put the old key into
a Secret listed in `sealing.oldKeys`, put the new key into `sealing.existingSecret`,
`helm upgrade` (a rolling restart), run `ostro secret reseal`, check `ostro secret sealing`,
then remove the old key with another upgrade.

## 5. KEDA: scaling workers to zero

With `keda.enabled: true` the chart renders a KEDA 2.21 ScaledObject for the worker Deployment
(`minReplicaCount` `keda.minReplicas`, default 0; `maxReplicaCount` 10; target demand per
replica `keda.target`, keep it equal to `worker.slots`):

| `keda.scaler` | Source |
|---|---|
| `prometheus` (default) | `sum(ostro_queue_worker_demand{queue=~"<queues>"})` from `keda.prometheus.serverAddress`: each server exports the demand of the partitions it leads, so the sum is the cluster's demand |
| `metrics-api` | `GET /v1/queues/<q>/demand` on the client Service, one trigger per queue, with a read-scoped bearer from a TriggerAuthentication |

The first ready run wakes a pool at zero (activation value 0). The pool returns to zero only
after `keda.cooldownPeriod` (120 s) without demand; runs still executing on workers count as
demand. The default self-signed listener certificate does not name the Service, so for
`metrics-api` use `server.apiTls.mode: files` with a certificate for
`<release>-server.<namespace>.svc`, or `keda.metricsApi.unsafeSsl: true`. How demand is
computed is in [workers.md](workers.md#5-demand-and-keda-work-05-d-w12-d-w22).

## 6. NetworkPolicies

With `networkPolicy.enabled: true` the chart renders five NetworkPolicy objects (this matrix):

| # | Policy | Selects | Direction | Allows |
|---|---|---|---|---|
| 1 | `<release>-server-client` | servers | ingress | client port from the release namespace and `networkPolicy.clientFromNamespaces` |
| 2 | `<release>-server-peers` | servers | ingress | cluster and replication ports from servers only |
| 3 | `<release>-server-workers` | servers | ingress | cluster port (the worker protocol) from workers only |
| 4 | `<release>-worker-egress` | workers | egress | servers (client and cluster ports) and DNS, plus `networkPolicy.workerEgress` |
| 5 | `<release>-server-egress` | servers | egress | servers, DNS, plus `networkPolicy.serverEgress` |

Add the namespaces of Prometheus, KEDA and your ingress controller to `clientFromNamespaces`.
Server egress is closed by default: add `serverEgress` rules for everything the servers call
out to, namely connector targets (HTTP APIs, databases, object stores), the backup bucket, the
OTLP collector, Vault, and Kafka brokers for [Kafka triggers](kafka-triggers.md). Workers
reach only the servers: effects run on the servers ([workers.md](workers.md#6-effects-stay-on-the-leader-d-w3-user-review)).

## 7. Backups and restore

Configure bottomless backups through `server.extraConfig` (a `[backup]` section with the bucket
URL) and pass the bucket credentials as environment variables from a Secret in
`server.extraEnv` ([bottomless.md](bottomless.md#config)); allow the bucket in
`networkPolicy.serverEgress`. All server pods share the bucket prefix; each partition is uploaded
by its current leader.

- Restoring one node, or a single-node release: [bottomless.md](bottomless.md#restore-runbook).
- Restoring a whole cluster to one consistent cut: [cluster-restore.md](cluster-restore.md).
  Restore into fresh volumes before the server pods start (for example from a one-off pod that
  mounts the new PVCs), with `OSTRO_SEALING_KEY` set: a sealed cluster CA is opened with it.
- The audit journal is node-local and not in any backup ([audit.md](audit.md#7-retention-disk-and-export)).
- The generated Secrets (sealing key, cluster CA) are needed to read a restored cluster: back
  them up with the bucket.
