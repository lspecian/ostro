# Audit log

Every Ostro node keeps a local, append-only, hash-chained **audit journal** of administrative
actions. An audited action is accepted only if its record can be written: the node reserves a
journal slot before it acts and makes the record durable before it answers. This guide covers
what is recorded, the record format, verification, the fail-closed behaviour and its trade-off,
retention and export.

Related: [tenancy-and-auth.md](tenancy-and-auth.md) (principals and scopes),
[signed-modules.md](signed-modules.md), [secrets.md](secrets.md).

## 1. What is recorded

| Area | Actions |
|---|---|
| Modules and workflows | `module.upload` (rejected signatures included), `module.signatures.attach`, `workflow.activate`, `module.retire`, `module.gc`, `run.migrate` |
| Policies | `workflow.policy.set`, `queue.policy.set`, `queue.flow.set`, `failure_policy.set` |
| Runs | `run.cancel`, `run.terminate`, `run.retry`, `run.pause`, `run.resume`, `run.rerun` |
| Queues and workflows | `queue.pause`, `queue.resume`, `workflow.pause`, `workflow.resume` |
| Secrets, grants, connections, egress | `secret.set`, `secret.delete`, `grant.put`, `grant.delete`, `connection.put`, `connection.delete`, `egress.put`, `egress.delete` |
| Secret access | `secret.access` (coalesced, §4): every resolution of a sealed or provider secret by a running workflow |
| Schedules and webhooks | `schedule.create`, `schedule.update`, `schedule.delete`, `schedule.pause`, `schedule.resume`, `schedule.trigger`, `webhook.create`, `webhook.update`, `webhook.delete` |
| DLQ and bulk jobs | `dlq.requeue`, `dlq.dismiss`, `bulk.create`, `bulk.cancel` |
| Workers | `worker.join`, `worker.renew`, `worker.drain`, `worker.token.create`, `worker.token.revoke` |
| Cluster | `cluster.init`, `cluster.join`, `cluster.token.create`, `cluster.remove`, `cluster.drain`, `cluster.reassign`, `cluster.rebalance`, `cluster.upgrade.finalize`, `cluster.upgrade.hold`, `cluster.upgrade.release` |
| Backup | `backup.download` |
| Tenants and tokens | `tenant.create`, `tenant.update`, `tenant.suspend`, `tenant.resume`, `tenant.delete`, `token.create`, `token.rotate`, `token.revoke` |
| Security | `security.policy.set`, `security.key.add`, `security.key.remove`, `security.sealing.reseal`, `system.reseal` (the re-seal pass itself, with its counts) |
| Failures | `auth.failure` (authentication) and `authz.denied` (authorization), both coalesced |

**Not recorded:** reads, run starts, signals and await-token completions. Their volume is high,
and they are already in the partition log of the run they touch (`ostro history`).

Known gaps in 1.0:

- A tenant principal that addresses another tenant's run id gets the 404 of the tenant rule
  ([tenancy-and-auth.md](tenancy-and-auth.md#the-404-rule)) before the audit step, so that
  attempt is not recorded. The same call by a system principal is recorded as
  `failed:not_found`.
- UI requests refused for a missing scope are answered by the UI before the audited API call
  and are not recorded. UI sign-in failures are recorded as `auth.failure`.
- `worker.join` and `worker.renew` are written asynchronously: they are recorded, but a full
  journal does not refuse the join.
- Bearer headers that fail to parse before the authenticator runs (a `Basic` header, two
  `Authorization` headers) are not recorded. `details.client_ip` of `auth.failure` is always
  `unknown`, and `request_id` is always `null`.

## 2. Record format

The journal lives in `<data_dir>/audit/` (`[audit] dir`), as segment files named
`audit-<first seq, 20 digits>.jsonl`, one JSON record per line:

```json
{"v":1,"seq":42,"ts_ms":1795781929000,"node":1,"incarnation":3,"actor":"otk_…","actor_kind":"token","tenant":"acme","action":"run.cancel","target":"<run id>","outcome":{"result":"ok"},"request_id":null,"details":{},"prev_hash":"<64 hex>","hash":"<64 hex>"}
```

| Field | Meaning |
|---|---|
| `seq` | position in this node's journal, contiguous from 1 |
| `ts_ms`, `node`, `incarnation` | time, node id, process incarnation that wrote it |
| `actor`, `actor_kind` | who: a token id (`token`), `dev`, `system`, a worker id (`worker`), `malformed`/`missing` for anonymous failures (`anonymous`), `run:<id>` for secret access |
| `tenant` | the tenant of the target, or `null` |
| `action`, `target` | what and on what: a run id, a qualified name, a module sha, a token or key id, `node:<id>`, `partition:<n>`, `workflow:`/`queue:` scopes. Never a request body or a secret value |
| `outcome` | `{"result":"ok"}`, `{"result":"failed:<api error code>"}` or `{"result":"denied:<reason>"}` (for example `denied:missing_scope`, `denied:signature_required`) |
| `details` | action-specific fields, sorted by key; for coalesced records also `count`, `first_ts_ms`, `last_ts_ms` |
| `prev_hash`, `hash` | the chain (§3) |

Each segment starts with a `segment_open` control record and every process start opens a new
segment. Control records (`segment_open`, `audit_recovered`, `retention_trim`) have actor and
actor kind `system`.

## 3. The hash chain and verification

Every record carries `hash = sha256(prev_hash ‖ canonical JSON of the record without hash)`
and the previous record's hash as `prev_hash`. The first record of a node chains from a genesis
hash of the node id and incarnation. A line is valid only if it is byte-identical to the
canonical re-encoding of its fields.

Verify a node's journal offline:

```sh
ostro audit verify --data-dir /var/lib/ostro [--config /etc/ostro/node.toml]
# audit journal OK: 1234 records in 3 segments, seq 1..1234, head <hash>
```

It checks every line, the seq order across files, every `prev_hash` link and every segment
boundary, and reports the first seq where the chain breaks. Any modified, inserted, deleted or
reordered record fails. Run it on the node (or a copy of its audit directory); it reads only.

On startup, a torn tail (a crash in the middle of a write) is truncated and recorded as
`audit_recovered{truncated_bytes, last_good_seq}`. A valid record after an invalid region is
not a torn tail but a modified journal: the node refuses to start instead of destroying the
evidence.

### Chain-head export: detecting truncation

A journal cut short at its end is still a valid chain. To catch that, each node publishes its
chain head `(node, incarnation, seq, hash)` every `[audit] head_export_interval` (default 1 h)
and at shutdown:

- as an INFO log line `chain_head` (log target `ostro::audit`);
- as the gauge `ostro_audit_chain_seq`;
- as an `ostro.audit.head` OTLP log record when OTLP logs are on.

Ship these off the node (log pipeline, metrics store). If a later `ostro audit verify` reports a
head older than one exported earlier, records were removed after that export. This protects
against an attacker with access to the node rewriting or truncating history that was already
exported; it cannot protect records written after the last export.

## 4. Coalescing

High-volume events are summarised instead of written one by one. Coalescing never blocks the
action:

| Action | Key | Window |
|---|---|---|
| `secret.access` | run, secret, origin and outcome (granted and refused are separate) | `[audit] secret_coalesce` (60 s) |
| `auth.failure` | token id (or `malformed` / `missing`) and client address | `[audit] auth_failure_coalesce` (60 s) |
| `authz.denied` | principal and method | `[audit] auth_failure_coalesce` (60 s) |

One record per key and window carries `count`, `first_ts_ms` and `last_ts_ms` in `details`.
Pending summaries are written when the window ends and at shutdown. Secret-access records name
the secret (`<tenant|_>/<name>`), the module, the origin and the run; they never contain the
value.

## 5. Fail closed, and what it costs

An audited action follows two steps:

1. **Reserve** a journal slot before acting. If none is free within `[audit] reserve_timeout`
   (default 2 s), because the writer is stalled, the queue (`[audit] queue`, 4096) is full, or
   the writer has failed, the action is refused with **503 `audit_unavailable`** and
   `details.retry_after_ms`. Nothing happened; retrying later is always safe.
2. **Commit** the record after acting and before answering. Records are group-committed
   (`[audit] group_commit`, 5 ms) and fsynced. If the write or the fsync fails, the action has
   **already happened**: the answer is **500 `audit_commit_failed`** with
   `details.executed = true`, and the full record is logged at ERROR so it is not lost.

After a commit failure the node's writer stays failed until the process restarts: every later
audited action on that node answers 503 `audit_unavailable`, while reads, starts and signals
keep working, and other nodes are unaffected. Fix the disk (space, permissions, I/O errors) and
restart the node.

This is the availability trade-off of the design: **a node that cannot record administrative
actions stops accepting them** rather than acting without a trace. Watch
`ostro_audit_unavailable_total` and the disk of the audit directory.

### Retrying after `audit_commit_failed`

The action ran, so a blind retry may do it twice. Retry through another node, or after the
restart:

| Safe to retry (idempotent) | Check before retrying |
|---|---|
| `run.cancel`, `run.terminate`, `run.pause`, `run.resume` | `token.create`, `token.rotate`: a retry creates another token. List tokens (`ostro auth token ls`) and revoke the extra one |
| `queue.*`, `workflow.pause`, `workflow.resume`, `workflow.activate` | `worker.token.create`, `cluster.token.create`: a retry creates another join token |
| `*.policy.set`, `queue.flow.set`, `failure_policy.set` | `module.upload`: the module is stored; a retry of the same bytes returns it, but check the active version (`ostro inspect`, the workflow list) before activating again |
| `secret.set`, `grant.put`, `connection.put`, `egress.put` (puts overwrite) | `run.retry` (restart mode), `run.rerun`, `schedule.trigger`: each creates a new run. Look for it first (`ostro ls`) |
| every `*.delete`, `module.retire` (a repeat answers 404) | `bulk.create`: a retry starts a second job (`ostro bulk ls`) |
| `tenant.suspend`, `tenant.resume`, `tenant.update`, `security.policy.set` | `tenant.create`, `schedule.create`, `webhook.create`, `security.key.add`: a repeat answers 409; the first one exists |
| `dlq.dismiss`, `module.signatures.attach` | `dlq.requeue`: check the DLQ entry and the run before requeueing again |

## 6. Querying

`GET /v1/audit` (and `ostro audit ls`) queries every member of the cluster, merges the records
by `(ts_ms, node, seq)` and pages with an opaque cursor. A member that does not answer within
10 s is named in `partial_nodes`, and the answer contains the others' records:

```sh
ostro audit ls --since 1h --action 'token.*' --tenant acme --limit 100
ostro audit ls --actor otk_… --json
ostro audit ls --cursor <cursor from the previous page>
```

Filters: `--since` / `--until` (a duration like `1h`, ms or RFC 3339), `--action` (a name or a
`prefix.*`), `--tenant` (`_` for records without a tenant), `--actor`, `--limit` (default 100,
at most 1000). Querying needs a tenant admin token; a tenant admin sees only its own tenant's
records. Control records appear only when `--action` names them.

## 7. Retention, disk and export

| Key | Default | Meaning |
|---|---|---|
| `[audit] enabled` | `true` | `false` writes no journal at all (and nothing fails closed) |
| `[audit] dir` | `<data_dir>/audit` | journal directory |
| `[audit] segment_bytes` | 64 MiB | segment size (at least 1 MiB) |
| `[audit] retention` | `2160h` | segments whose records are all older are deleted |
| `[audit] max_bytes` | 10 GiB | oldest segments are deleted while the journal is larger |
| `[audit] queue` | 4096 | records reserved but not yet durable |
| `[audit] group_commit` | `5ms` | group-commit window |
| `[audit] reserve_timeout` | `2s` | longest wait for a slot before 503 |
| `[audit] secret_coalesce`, `auth_failure_coalesce` | `60s` | coalescing windows (§4) |
| `[audit] head_export_interval` | `1h` | chain-head export period (§3) |
| `[audit] otlp` | `true` | also export records as OTLP logs when OTLP logs are on |

Retention deletes whole segments, oldest first and never the active one. Before deleting, the
writer appends a `retention_trim{first_kept_seq, first_kept_hash}` record, so verification
starts from that anchor. Plan disk for `max_bytes` per node (see
[operations-sizing.md](operations-sizing.md#audit-disk)); `ostro_audit_records_total` and
`ostro_audit_fsyncs_total` show the write rate.

**OTLP export.** With `[telemetry] otlp_endpoint` set, `[telemetry] logs` on and `[audit] otlp`
on, every committed record is also emitted as an OTLP log record (`event.name = "ostro.audit"`,
body = the action) through the telemetry pipeline ([observability.md](observability.md)). The
export never slows the journal: when its queue is full, the export copy is dropped and counted in
`ostro_audit_export_dropped_total`; the journal record is kept.

**Backups do not contain the audit journal.** Neither bottomless backups nor `ostro backup`
include `<data_dir>/audit`, so a restored node or cluster starts with an empty audit log
([cluster-restore.md](cluster-restore.md#restrictions)). Keep the journal directory (or its
OTLP export) separately if you need it beyond the life of the node.
