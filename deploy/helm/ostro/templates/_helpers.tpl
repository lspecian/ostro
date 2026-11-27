{{/*
Ostro chart helpers (Phase 14, 14-13, D-P14-26).
*/}}

{{- define "ostro.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified release name. Kept to 40 characters: StatefulSet pod names add
"-server-<ordinal>" and the controller-revision-hash label must stay within 63.
*/}}
{{- define "ostro.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 40 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 40 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "ostro.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "ostro.labels" -}}
helm.sh/chart: {{ include "ostro.chart" . }}
app.kubernetes.io/name: {{ include "ostro.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "ostro.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ostro.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "ostro.serverSelectorLabels" -}}
{{ include "ostro.selectorLabels" . }}
app.kubernetes.io/component: server
{{- end }}

{{- define "ostro.workerSelectorLabels" -}}
{{ include "ostro.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{- define "ostro.server" -}}
{{- printf "%s-server" (include "ostro.fullname" .) }}
{{- end }}

{{- define "ostro.headless" -}}
{{- printf "%s-server-headless" (include "ostro.fullname" .) }}
{{- end }}

{{- define "ostro.worker" -}}
{{- printf "%s-worker" (include "ostro.fullname" .) }}
{{- end }}

{{- define "ostro.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ostro.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "ostro.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{/*
[cluster] cluster_id: 1-128 characters of [A-Za-z0-9._-]; the cluster CA's CN is
"ostro cluster <cluster_id>" and the server refuses a CA with another CN.
*/}}
{{- define "ostro.clusterId" -}}
{{- default (include "ostro.fullname" .) .Values.server.clusterId }}
{{- end }}

{{/* DNS name of server pod <ordinal> (through the headless Service). */}}
{{- define "ostro.podHost" -}}
{{- $root := index . 0 }}{{- $i := index . 1 }}
{{- printf "%s-%d.%s.%s.svc.%s" (include "ostro.server" $root) (int $i) (include "ostro.headless" $root) $root.Release.Namespace $root.Values.clusterDomain }}
{{- end }}

{{/* The client Service DNS name (join URL host, KEDA metrics-api host). */}}
{{- define "ostro.clientHost" -}}
{{- printf "%s.%s.svc.%s" (include "ostro.server" .) .Release.Namespace .Values.clusterDomain }}
{{- end }}

{{/* http or https for the client listener ([api.tls] mode). */}}
{{- define "ostro.clientScheme" -}}
{{- if eq .Values.server.apiTls.mode "off" }}http{{ else }}https{{ end }}
{{- end }}

{{/* Secret names: an existing Secret wins, else the chart-generated one. */}}
{{- define "ostro.authSecret" -}}
{{- default (printf "%s-auth" (include "ostro.fullname" .)) .Values.auth.existingSecret }}
{{- end }}

{{- define "ostro.sealingSecret" -}}
{{- default (printf "%s-sealing" (include "ostro.fullname" .)) .Values.sealing.existingSecret }}
{{- end }}

{{- define "ostro.caSecret" -}}
{{- default (printf "%s-cluster-ca" (include "ostro.fullname" .)) .Values.tls.clusterCa.existingSecret }}
{{- end }}

{{- define "ostro.workerTokenSecret" -}}
{{- default (printf "%s-worker-join" (include "ostro.fullname" .)) .Values.worker.joinToken.existingSecret }}
{{- end }}

{{/* Fail early on combinations the server would refuse at boot. */}}
{{- define "ostro.validate" -}}
{{- $r := int .Values.server.replicas }}
{{- if and (ne $r 1) (ne $r 3) (ne $r 5) }}
{{- fail "server.replicas must be 1, 3 or 5 (a static voter set; grow a running cluster with `ostro cluster join`, see README)" }}
{{- end }}
{{- if not (regexMatch "^[A-Za-z0-9._-]{1,128}$" (include "ostro.clusterId" .)) }}
{{- fail "server.clusterId must be 1-128 characters of [A-Za-z0-9._-]" }}
{{- end }}
{{- if and (eq .Values.auth.mode "none") (not .Values.auth.allowInsecureNone) }}
{{- fail "auth.mode=none on a pod listener needs auth.allowInsecureNone=true ([auth] allow_insecure_none); use token mode outside throwaway clusters" }}
{{- end }}
{{- if and .Values.auth.existingSecret .Values.auth.bootstrapToken }}
{{- fail "set auth.existingSecret or auth.bootstrapToken, not both" }}
{{- end }}
{{- if and .Values.auth.bootstrapToken (not (regexMatch "^ostro_otk_[a-z2-7]{16}_[a-z2-7]{51}[aq]$" .Values.auth.bootstrapToken)) }}
{{- fail "auth.bootstrapToken must be an Ostro bearer: ostro_otk_<16 base32>_<52 base32> (or leave it empty to generate one at install)" }}
{{- end }}
{{- if and (eq .Values.server.apiTls.mode "files") (not .Values.server.apiTls.existingSecret) }}
{{- fail "server.apiTls.mode=files needs server.apiTls.existingSecret (a kubernetes.io/tls Secret, e.g. from cert-manager)" }}
{{- end }}
{{- if and .Values.worker.enabled .Values.tls.clusterCa.existingSecret (not .Values.worker.caHash) (ne .Values.server.apiTls.mode "files") }}
{{- fail "worker.caHash (sha256:<hex of the CA DER>) is required with tls.clusterCa.existingSecret: the worker pins the cluster CA during the join" }}
{{- end }}
{{- if .Values.keda.enabled }}
{{- if not .Values.worker.enabled }}
{{- fail "keda.enabled scales the worker Deployment: set worker.enabled=true" }}
{{- end }}
{{- if eq (len .Values.worker.queues) 0 }}
{{- fail "keda.enabled needs at least one entry in worker.queues" }}
{{- end }}
{{- if and (eq .Values.keda.scaler "prometheus") (not .Values.keda.prometheus.serverAddress) }}
{{- fail "keda.scaler=prometheus needs keda.prometheus.serverAddress" }}
{{- end }}
{{- if and (eq .Values.keda.scaler "metrics-api") (eq .Values.auth.mode "token") (not .Values.keda.metricsApi.tokenSecret.name) }}
{{- fail "keda.scaler=metrics-api needs keda.metricsApi.tokenSecret.name: a Secret holding a bearer with the read scope (`ostro auth token create --name keda --scope read --system`)" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
The node configuration of server <ordinal> (the TOML file the pod's `--config` names).
Only keys of ostro-server's ServerConfig ([node], [api], [cluster], [replication],
[secrets], [auth], [ui], [log]) appear here; server.extraConfig is appended verbatim.
*/}}
{{- define "ostro.nodeToml" -}}
{{- $root := index . 0 }}{{- $i := int (index . 1) }}
{{- $v := $root.Values }}
{{- $p := $v.server.ports -}}
# Ostro server {{ $i }} ({{ include "ostro.server" $root }}-{{ $i }}); rendered by the Helm chart.
[node]
node_id = {{ add $i 1 }}
data_dir = "/data"
partitions = {{ int $v.server.partitions }}

[api]
listen = "0.0.0.0:{{ int $p.client }}"
max_connections = {{ int (default 0 $v.server.maxConnections) }}

[api.tls]
mode = {{ $v.server.apiTls.mode | quote }}
{{- if eq $v.server.apiTls.mode "files" }}
cert_file = "/etc/ostro/api-tls/tls.crt"
key_file = "/etc/ostro/api-tls/tls.key"
{{- end }}

[log]
level = {{ $v.server.logLevel | quote }}
format = "json"

[auth]
mode = {{ $v.auth.mode | quote }}
{{- if eq $v.auth.mode "none" }}
allow_insecure_none = true
{{- end }}

[secrets]
{{- if $v.sealing.oldKeys.existingSecret }}
old_sealing_key_files = [{{ range $j, $k := $v.sealing.oldKeys.keys }}{{ if $j }}, {{ end }}"/etc/ostro/sealing-old/{{ $k }}"{{ end }}]
{{- else }}
old_sealing_key_files = []
{{- end }}

[ui]
enabled = {{ $v.server.ui.enabled }}

[cluster]
listen = "0.0.0.0:{{ int $p.cluster }}"
advertise = ["{{ include "ostro.podHost" (list $root $i) }}"]
cluster_id = {{ include "ostro.clusterId" $root | quote }}
{{- if $p.replication }}
replication_listen = "0.0.0.0:{{ int $p.replication }}"
{{- end }}

[cluster.tls]
ca_cert_file = "/etc/ostro/cluster-ca/{{ $v.tls.clusterCa.certKey }}"
ca_key_file = "/etc/ostro/cluster-ca/{{ $v.tls.clusterCa.keyKey }}"
{{- range $j := until (int $v.server.replicas) }}
{{- if ne $j $i }}

[[cluster.peers]]
id = {{ add $j 1 }}
addr = "{{ include "ostro.podHost" (list $root $j) }}:{{ int $p.cluster }}"
{{- if $p.replication }}
repl_addr = "{{ include "ostro.podHost" (list $root $j) }}:{{ int $p.replication }}"
{{- end }}
{{- end }}
{{- end }}
{{- with $v.server.extraConfig }}

# server.extraConfig
{{ . }}
{{- end }}
{{- end }}

{{/* worker.labels as k=v,k2=v2 (sorted). */}}
{{- define "ostro.labelList" -}}
{{- $out := list }}
{{- range $k := keys . | sortAlpha }}
{{- $out = append $out (printf "%s=%s" $k (index $ $k)) }}
{{- end }}
{{- join "," $out }}
{{- end }}

{{/* Egress rule: cluster DNS (port 53 UDP/TCP in networkPolicy.dnsNamespace). */}}
{{- define "ostro.dnsEgress" -}}
- to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: {{ .Values.networkPolicy.dnsNamespace }}
  ports:
    - port: 53
      protocol: UDP
    - port: 53
      protocol: TCP
{{- end }}
