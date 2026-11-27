---
phase: 14-multi-tenancy-security-1-0-release
plan: 13
subsystem: deploy
tags: [helm, kubernetes, keda, networkpolicy, DIST-02]
requires: ["14-01b"]
provides: ["deploy/helm/ostro", "deploy/helm/schemas (KEDA 2.21.1 kubeconform schemas)", "scripts/helm-check.sh", "scripts/crd2jsonschema.py"]
affects: ["14-27 (CI step for scripts/helm-check.sh; release publishing of the chart)", "14-25 (docs index may link deploy/helm/ostro/README.md)"]
tech-stack:
  added: ["helm 3.17.0 (pinned download, validation only)", "kubeconform 0.8.0 (fetch-tools.sh)"]
  patterns: ["per-ordinal ConfigMap keys selected with $(POD_NAME) (no shell in the image)", "install-only generated Secrets with helm.sh/resource-policy: keep"]
key-files:
  created:
    - deploy/helm/ostro/Chart.yaml
    - deploy/helm/ostro/values.yaml
    - deploy/helm/ostro/values.schema.json
    - deploy/helm/ostro/.helmignore
    - deploy/helm/ostro/README.md
    - deploy/helm/ostro/templates/{_helpers.tpl,NOTES.txt,serviceaccount,configmap,secret,server-statefulset,server-service,server-headless,server-pdb,worker-deployment,networkpolicy,keda}.yaml
    - deploy/helm/ostro/ci/{default,keda,netpol,single-node}-values.yaml
    - deploy/helm/ostro/examples/keda/{scaledobject-prometheus,scaledobject-metrics-api,trigger-auth}.yaml
    - deploy/helm/schemas/{SOURCE.md,scaledobject_v1alpha1.json,triggerauthentication_v1alpha1.json,scaledjob_v1alpha1.json,clustertriggerauthentication_v1alpha1.json}
    - scripts/crd2jsonschema.py
    - scripts/helm-check.sh
  modified: []
decisions:
  - "Node id: one ConfigMap key per ordinal (<fullname>-server-<i>.toml, node_id = i + 1, peers = the other ordinals) selected by `--config /etc/ostro/config/$(POD_NAME).toml`; the server has no --node-id-from-hostname flag and the image has no shell."
  - "Generated Secrets (bootstrap bearer, sealing key, cluster CA + ca-hash) render only at install with helm.sh/resource-policy: keep (no lookup allowed): upgrades never regenerate them; existingSecret is the documented production path."
  - "The chart always configures the shared cluster CA, also for one voter, so the worker ca-hash is known (generated CA: ca-hash key computed in-template as sha256 of the DER)."
  - "server.replicas restricted to 1, 3 or 5 and documented as fixed at install (static [[cluster.peers]]); growth goes through ostro cluster join."
  - "Worker protocol shares the cluster listener (D-P12-13): NetworkPolicy 3 opens the cluster port to workers; 2 opens cluster + replication ports to servers; replication_listen on 7702 by default keeps replication server-only."
  - "Default [api.tls] mode self-signed (explicit mode, no allow_self_signed needed); KEDA metrics-api needs files mode or unsafeSsl because the self-signed certificate does not name the Service."
metrics:
  duration: "~1h30"
  completed: 2026-11-27
---

# Phase 14 Plan 13: Helm chart Summary

Helm chart `deploy/helm/ostro` (1.0.0-rc.1) with a static 3-voter server StatefulSet,
a remote worker pool, KEDA 2.21 scaling and five NetworkPolicies, validated offline only
(helm lint --strict, helm template | kubeconform -strict against Kubernetes 1.34.12 and
KEDA 2.21.1 schemas, structural and negative assertions). It has not been installed on any
cluster.

## Commits

| Task | Commit | Files |
|---|---|---|
| 1: chart | 394cf14 | deploy/helm/ostro/** |
| 2: schemas + helm-check | 405b05a | deploy/helm/schemas/*, scripts/crd2jsonschema.py, scripts/helm-check.sh |

## Validation (dell-4 lane 14-13, `bash scripts/helm-check.sh`, helm v3.17.0, kubeconform 0.8.0)

```
== default-values.yaml      Summary: 10 resources found parsing stdin - Valid: 10, Invalid: 0, Errors: 0, Skipped: 0
== keda-values.yaml         Summary: 9 resources found parsing stdin - Valid: 9, Invalid: 0, Errors: 0, Skipped: 0
== netpol-values.yaml       Summary: 16 resources found parsing stdin - Valid: 16, Invalid: 0, Errors: 0, Skipped: 0
== single-node-values.yaml  Summary: 8 resources found parsing stdin - Valid: 8, Invalid: 0, Errors: 0, Skipped: 0
== examples/keda            Summary: 4 resources found in 3 files - Valid: 4, Invalid: 0, Errors: 0, Skipped: 0
helm lint --strict: 1 chart(s) linted, 0 chart(s) failed (x4; only INFO "icon is recommended")
structural assertions: 46 ok (incl. generated CA: CN "ostro cluster t-ostro", CA:TRUE,
EC P-256 key, ca-hash = sha256(CA DER) checked with openssl), 6 negative renders refused
helm-check: PASS
```

The plan's Task 1 verify (four strict lints, 5 NetworkPolicies, ScaledObject +
minReplicaCount 0) and Task 2 verify (no Invalid/Errors/Skipped, schema file present,
sha256 in SOURCE.md, no kubectl/helm install/upgrade/kube-context in helm-check.sh) are
covered by the same run.

**Server config check (dell-4 lane 14-13):** built `ostro` from main (5badb0e + this
chart) and ran `ostro server --config <file> --print-config` on all 10 node configs the four
CI values files render (ConfigMap keys extracted from `helm template --show-only
templates/configmap.yaml`): every file parsed by the real loader (`deny_unknown_fields`
sections), with node_id 1..3, cluster_id, replication_listen and old_sealing_key_files as
rendered. Log: target/dell4-ci-logs/20261127-130746-14-13-5badb0e0-cmd.log.

## Node-id mechanism

`ostro server --config /etc/ostro/config/$(POD_NAME).toml`, POD_NAME from the downward API.
The ConfigMap holds `<fullname>-server-<i>.toml` per ordinal with `[node] node_id = i + 1`,
`[cluster] advertise = ["<pod>.<headless>.<ns>.svc.<domain>"]`, `cluster_id`,
`replication_listen`, `[cluster.tls] ca_cert_file/ca_key_file` and every other ordinal as
`[[cluster.peers]] {id, addr (cluster port), repl_addr}`. The server has no
`--node-id-from-hostname`; the image has no shell. The headless Service publishes
not-ready addresses so voters resolve each other before /readyz passes.

## Config keys used (all exist in ostro-server ServerConfig on main)

`[node] node_id, data_dir, partitions`; `[api] listen`; `[api.tls] mode, cert_file,
key_file`; `[log] level, format`; `[auth] mode, allow_insecure_none`; `[secrets]
old_sealing_key_files`; `[ui] enabled`; `[cluster] listen, advertise, cluster_id,
replication_listen`; `[cluster.tls] ca_cert_file, ca_key_file`; `[[cluster.peers]] id,
addr, repl_addr`. Env: `OSTRO_SEALING_KEY`, `OSTRO_BOOTSTRAP_TOKEN` (server);
`OSTRO_WORKER_JOIN, _TOKEN, _CA_HASH, _SLOTS, _MEMORY_BYTES, _CACHE_DIR, _LABELS` (worker).
Health/metrics: `/healthz`, `/readyz`, `/metrics` on the client port (anonymous routes in
authn_layer); demand `/v1/queues/<q>/demand` needs the read scope. No required setting was
missing.

## Values table

See `deploy/helm/ostro/README.md` "Values" (complete table). Main keys: `image.*`,
`server.{replicas 3, clusterId, partitions 16, ports.{client 7700, cluster 7701,
replication 7702}, apiTls.{mode self-signed, existingSecret}, ui.enabled, resources,
storage.{size 20Gi, storageClass}, antiAffinity soft, pdb.{enabled, minAvailable 2},
terminationGracePeriodSeconds 60, extraConfig, extraEnv}`, `worker.{enabled, replicas 1,
queues [default], labels, slots 4, memoryBytes, joinToken.{existingSecret, key}, caHash,
terminationGracePeriodSeconds 120, cacheSize}`, `auth.{mode token, allowInsecureNone,
existingSecret, existingSecretKey, bootstrapToken}`, `sealing.{existingSecret,
existingSecretKey, oldKeys.{existingSecret, keys}}`, `tls.clusterCa.{existingSecret,
certKey, keyKey}`, `keda.{enabled false, scaler prometheus|metrics-api, minReplicas 0,
maxReplicas 10, target 4, pollingInterval, cooldownPeriod, prometheus.serverAddress,
metricsApi.{tokenSecret, unsafeSsl}}`, `networkPolicy.{enabled false,
clientFromNamespaces, dnsNamespace, serverEgress, workerEgress}`.

## Deviations from Plan

1. **[Rule 3] helm is not pinned in scripts/fetch-tools.sh** (not in this plan's file
   list). helm-check.sh uses `$HELM`, else `helm` on PATH, else downloads helm v3.17.0 into
   target/tools with a pinned sha256 (fb5d1266…aee24, equal to get.helm.sh's published
   .sha256sum, checked on dell-4). Adding helm to fetch-tools.sh later is optional.
2. **[Local-machine limit, orchestrator 2026-11-27]** All helm/kubeconform runs happened
   on dell-4 (lane 14-13), not locally. kubeconform and the KEDA CRD file had already been
   fetched locally before the instruction (CRD sha256 verified; the conversion ran locally
   with python3 -I, nothing else).
3. **Values additions beyond the plan list** (all optional): `clusterDomain`,
   `server.{clusterId, logLevel, apiTls, ui, antiAffinity, pdb, service, extraEnv,
   podAnnotations, nodeSelector, tolerations}`, `worker.{labels, slots, memoryBytes,
   joinToken, caHash, cacheSize, ...}`, `sealing.oldKeys` (for old_sealing_key_files),
   `keda.{pollingInterval, cooldownPeriod, metricsApi.*}`, `networkPolicy.{dnsNamespace,
   workerEgress}`.
4. **Worker join token** cannot be produced by the chart (it is created through the running
   cluster): the Deployment references `<fullname>-worker-join` and pods start once the
   operator creates it (NOTES.txt explains).

## Gate

gate incomplete — consolidated gate pending on dell-4. The plan's verification names
`ostro-k8s-ci --parallel` (cluster use withdrawn); this plan changes no Rust code, so the
only plan-specific check is `scripts/helm-check.sh` (green on dell-4 above). Unverified:
- the full ci-local run at this commit (consolidated dell-4 gate);
- a live install (kind/k3d or a user-approved cluster) — human_needed: probes, peer DNS
  bootstrap, generated-CA import by the server, worker join with the ca-hash, KEDA scaling
  and NetworkPolicy enforcement have never run on Kubernetes.

## ci.yml hunk (for 14-27; not applied, workflows untouched)

```yaml
  helm-chart:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - name: Offline Helm chart validation (lint + kubeconform)
        run: bash scripts/helm-check.sh
```

ci-local: add a step `check_helm_chart` running `bash scripts/helm-check.sh` (network for
the pinned helm/kubeconform downloads and the Kubernetes schemas on first run).

## Notes for the orchestrator

- The chart's generated-Secret path depends on `.Release.IsInstall`; `helm template`
  renders as an install, so helm-check covers it, but upgrade behaviour (Secrets kept,
  not re-rendered) is unverified without a cluster.
- Queue names with a tenant prefix (`<t>/q`) are URL-encoded in the metrics-api URL
  (`t%2Fq`); whether the REST route accepts that form was not checked.

## Self-Check: PASSED

Commits 394cf14 and 405b05a are on main; networkpolicy.yaml, values.schema.json, helm-check.sh (100755, contains schema-location), crd2jsonschema.py (100755), the four KEDA schemas and SOURCE.md (with the CRD sha256) exist in main.
