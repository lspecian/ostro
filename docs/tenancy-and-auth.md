# Tenancy and authentication

This guide covers how Ostro identifies callers (API tokens), what they may do (scopes), how
tenants keep workloads apart, and how tenant quotas work, including how a quota is split across
the nodes of a cluster. The route-by-route scope table is generated from the code and lives in
the security documentation (`docs/security/auth-matrix.md`, with `docs/security-model.md`).

Related guides: [audit.md](audit.md) (what every admin action records),
[signed-modules.md](signed-modules.md) (who may deploy what), [secrets.md](secrets.md) (the
sealed store and its key).

## 1. Authentication modes

`[auth] mode` selects how the client listener (REST, gRPC and the UI) authenticates callers:

| Mode | Default for | Behaviour |
|---|---|---|
| `none` | `ostro dev` | No authentication. Every request runs as the dev principal with every permission. The UI is open. |
| `token` | `ostro server` | Every request needs `Authorization: Bearer <token>`. A missing, malformed, unknown, revoked or expired bearer is 401 `unauthenticated`. |

`ostro server` refuses to start with `mode = "none"` on a non-loopback listener unless
`[auth] allow_insecure_none = true`. In token mode only these routes answer without a bearer:

- `GET /healthz`, `GET /readyz`, `GET /metrics`, `GET /` and the gRPC health services;
- `/ui` and `/ui/*`, which use their own session sign-in (§9);
- webhook deliveries (`POST /hooks/{name}`, `POST /hooks/{tenant}/{name}`), which are verified
  by the provider signature ([webhooks.md](webhooks.md));
- worker and node joins (`POST /v1/workers/join`, `POST /v1/cluster/join`), where the join
  token in the body is the credential ([workers.md](workers.md));
- presigned object URLs (`/v1/objects/*`), where the query signature is the credential
  ([connectors.md](connectors.md#43-presigned-urls)).

Everything else, including `/openapi.json` and the queue demand endpoint, needs a bearer.

| Key | Default | Meaning |
|---|---|---|
| `[auth] mode` | `token` (server), `none` (dev) | `none` or `token` |
| `[auth] allow_insecure_none` | `false` | allow `mode = "none"` on a non-loopback listener |
| `[auth] bootstrap_token_file` | unset | file holding the bootstrap admin bearer (§4) |
| `[auth] default_token_ttl` | `2160h` | lifetime of a token created without `--ttl` |
| `[auth] ui_session_ttl` | `8h` | lifetime of a UI session cookie (§9) |

Every key can also be set from the environment, for example `OSTRO_AUTH__MODE=token`.

## 2. Tenants and names

A **tenant** is a namespace for workflows, queues, runs, schedules, webhooks, secrets, grants,
egress policies and tokens. Tenant ids match `^[a-z0-9][a-z0-9-]{0,62}$`. The **default
tenant** `_` always exists and holds everything created without a tenant, including every
resource of a single-user installation.

Resources of a tenant have **qualified names** `<tenant>/<name>` (for example `acme/orders`).
What a caller sees depends on its principal:

- A **tenant principal** (a token of tenant `acme`) uses plain names: it deploys `orders`,
  starts `orders`, lists `orders`. The server qualifies them to `acme/orders` and strips the
  prefix in every answer. A tenant request that carries a qualified name, even with its own
  prefix, is 400 `invalid_name`.
- A **system principal** (a token without a tenant, or the dev principal) sees every tenant
  and uses qualified names (`acme/orders`). Plain names mean the default tenant.

Runs inherit the tenant of their workflow. Cron fires, webhook deliveries, on-failure hooks,
bulk jobs and Kafka-triggered starts stay in the tenant of the schedule, webhook, hook workflow,
job creator or trigger that started them.

**Connections are cluster-scoped in 1.0.** SQL, object-store and HTTP-target connections
([connectors.md](connectors.md)) are shared by every tenant and managed by system admins only:
a tenant principal lists none, gets 404 for one, and is refused (403) on create or delete.
Secrets, grants and egress policies are per tenant.

### The 404 rule

A tenant principal never learns that another tenant's object exists. A run id, token id,
trusted-key id or bulk job of another tenant answers exactly like an unknown one: 404 with the
same body. Lists only contain the caller's own tenant.

### Managing tenants

Tenant administration needs a system admin token (`OSTRO_TOKEN`):

```sh
ostro tenant create acme --weight 2 --max-concurrency 8 --memory 512MiB --event-rate 100/1s+10
ostro tenant ls
ostro tenant get acme
ostro tenant set acme --fuel-per-s 1000 --clear max-concurrency
ostro tenant suspend acme
ostro tenant resume acme
ostro tenant rm acme
```

| REST | CLI |
|---|---|
| `GET/POST /v1/tenants` | `ostro tenant ls`, `ostro tenant create` |
| `GET/PATCH/DELETE /v1/tenants/{id}` | `ostro tenant get`, `ostro tenant set`, `ostro tenant rm` |
| `POST /v1/tenants/{id}/suspend`, `/resume` | `ostro tenant suspend`, `ostro tenant resume` |

`PATCH` changes the fields it names; a JSON `null` clears a quota (`--clear <field>` on the
CLI). Suspend and resume are idempotent. A tenant can be deleted only when it is suspended and
empty: no token, no secret, no active workflow version and no unfinished run. Otherwise the
delete is 409 `tenant_not_empty`, naming what is left. The default tenant `_` cannot be created,
changed or deleted.

## 3. Scopes

A token carries one or more scopes:

| Scope | Allows |
|---|---|
| `read` | reading runs, histories, results, logs, workflows, modules, queues, schedules, webhooks, DLQ, limits; the server-side replay check (`ostro replay` also uploads the module, which needs `deploy`) |
| `trigger` | starting runs, sending signals, completing await tokens, webhook deliveries |
| `operate` | cancel, terminate, retry, pause and resume runs, queues, workflows and schedules; queue flow and policies; DLQ requeue and dismiss; bulk jobs; rerun |
| `deploy` | uploading modules, activating versions, workflow and failure policies, attaching signatures, `migrate-runs`, retiring modules |
| `admin` | everything above, plus administration (below) |

`admin` covers every other scope. No other scope implies another: a `deploy` token cannot read
runs unless it also has `read`.

On top of the scope, some methods need a particular kind of principal:

| Requirement | Who | Methods (examples) |
|---|---|---|
| tenant admin | an `admin` token of any tenant (narrowed to its own tenant) or a system admin | secrets, grants, egress policies, schedule and webhook create/update/delete, API tokens, the audit log, trusted keys, a tenant's signing requirement |
| system admin | an `admin` token **without** a tenant | tenants, connections, workers and worker tokens, cluster membership and upgrades, backups, module GC, the cluster signing policy, sealing status and reseal |
| system read | a `read` or `admin` token without a tenant | cluster, partition, worker and tenant status |
| authenticated | any valid token | `GET /v1/auth/whoami` |

A denial is 403 with `details.reason = "missing_scope"`, the method and the scope it needed
(plus `authority: "system"` when a tenant-less token was required). A tenant principal
addressing another tenant (for example `--tenant globex` on a token of `acme`) is 403
`forbidden_tenant`. Tokens without a tenant can only carry `admin` or `read`.
Authorization happens once, on the node that receives the request; calls it forwards to other
nodes carry the caller's identity.

## 4. Tokens

### Format

Bearers look like `ostro_otk_<16 base32>_<52 base32>`. The first part is the token id
(`otk_…`, shown in listings and the audit log); the second part is the secret. The server
stores only a SHA-256 hash of the secret and shows the bearer exactly once, when the token is
created or rotated (the answer carries `Cache-Control: no-store`).

### Lifecycle

```sh
export OSTRO_TOKEN=ostro_otk_…                        # the CLI sends it as the bearer
ostro auth whoami                                     # token id, tenant, scopes
ostro auth token create --name ci --scope deploy --scope read --tenant acme --ttl 720h
ostro auth token create --name keda --scope read --system
ostro auth token ls [--tenant acme]                   # never shows a secret
ostro auth token rotate otk_… [--keep-old] [--ttl 720h]
ostro auth token revoke otk_…
```

| REST | Meaning |
|---|---|
| `GET/POST /v1/auth/tokens` | list, create |
| `DELETE /v1/auth/tokens/{id}` | revoke: the next request with that bearer is 401 |
| `POST /v1/auth/tokens/{id}/rotate` | a new bearer with the same name, scopes, tenant and lifetime; the old one is revoked unless `revoke_old: false` (`--keep-old`) |
| `GET /v1/auth/whoami` | the calling principal |

Creation rules:

- Every requested scope must be allowed by the caller's own scopes (`admin` allows all).
- A tenant principal creates tokens for its own tenant only (unset `--tenant` means its own).
- A system token (`--system`, no tenant) carries only `admin` or `read`.
- The tenant must exist (`_` always does).
- `--no-expiry` is for system admins only. Without `--ttl` a token expires after
  `[auth] default_token_ttl` (2160 h, 90 days).

### The first admin token (bootstrap)

In token mode, the first server start creates one system admin token when none exists. The
bearer comes from, in order:

1. `OSTRO_BOOTSTRAP_TOKEN` (a bearer you generated, in the `ostro_otk_…` format);
2. `[auth] bootstrap_token_file`;
3. otherwise a generated bearer, written to `<data_dir>/admin.token` (mode 0600) before the
   token is stored. The log names the file and the token id, never the secret.

Bootstrap is idempotent: a restart, or another node of the cluster, does not create a second
token while an unexpired system admin exists. Use that token to create the real ones, then
revoke it or delete `admin.token`.

If every admin token is lost, stop the node and run `ostro auth bootstrap --data-dir <dir>`
(optionally `--expires 1h`). It works only on a stopped, single-voter node and prints a new
system admin bearer once. On a running cluster, create tokens through the API instead.

The Helm chart passes the bootstrap bearer through a Secret ([kubernetes.md](kubernetes.md)).

## 5. Quotas

Each tenant can carry quotas. Unset means unlimited. **Quotas delay work, they never fail an
accepted run**: a run that is over its tenant's budget waits in its queue until the budget
allows it.

| Quota | CLI flag | Effect |
|---|---|---|
| `weight` | `--weight N` | fair-share weight of the tenant in the scheduler (default 1, clamped to 1..10000). Tenants share a node in proportion to their weights, then queues within a tenant (by queue weight), then priority bands and keys ([flow-control.md](flow-control.md#3-fairness-weights-priority-bands-keys-d-w10)) |
| `max_concurrency` | `--max-concurrency N` | leased activations of the tenant running at once (sleeping and waiting runs count nothing) |
| `memory_bytes` | `--memory 512MiB` | instance memory reserved at grant and released at the end of the activation. A tenant that holds nothing is always admitted once, so a quota below one instance delays work but never blocks it forever |
| `fuel_per_s`, `fuel_burst` | `--fuel-per-s N`, `--fuel-burst N` | a fuel token bucket refilled every second (the burst defaults to one second of refill). Each activation is charged the fuel it used afterwards; the balance can go negative, and a tenant in debt is not dispatched until it is positive again |
| `event_rate` | `--event-rate 100/1s+10` | starts, signals, await-token completions, webhook deliveries, Kafka-triggered starts and schedule trigger-now, admitted at `limit` per `period` with a burst of `burst` (at least 1). Excess is 429 |
| `suspended` | `ostro tenant suspend` | ingress refused, dispatch held (below) |

### Ingress: `event_rate` and suspension

Admission runs on the node that would propose the event, before anything is written. The rules,
in order:

1. A suspended tenant gets 403 `permission_denied` with `details.reason = "tenant_suspended"`
   for every kind of event, including a start whose idempotency key already exists.
2. A start whose idempotency key already names a run is free: it returns that run and charges
   nothing.
3. With an `event_rate`, a non-conforming event gets 429 `rate_limited` with
   `details.reason = "tenant_event_rate"`, `details.retry_after_ms` and a `Retry-After` header
   (gRPC `RESOURCE_EXHAUSTED`). Nothing is proposed. One budget covers every kind of event.

Webhook providers retry a 429 on their own schedule. A Kafka trigger pauses its partition on 429
or 403 and retries the record; it never skips one ([kafka-triggers.md](kafka-triggers.md)).

**Cron fires are not charged** against `event_rate`, but a suspended tenant's schedules do not
fire: **a fire that falls due while the tenant is suspended is dropped, not caught up**. After
`ostro tenant resume`, each schedule continues from its next due time, whatever its catch-up
policy ([cron.md](cron.md)). Each dropped fire counts in
`ostro_tenant_ingress_rejected_total{kind="cron"}`.

While a tenant is suspended, runs it already had are **held**: ready runs are not dispatched,
running activations finish, and nothing fails. They continue after the resume.

### Cluster-wide quotas: the per-node leadership share

Quotas are enforced by each node on its own, without coordination between nodes. A node
enforces its **share** of each quota, proportional to the number of partitions it leads:

```text
share(x) = max(1, ceil(x × led / P))
```

where `P` is the cluster's partition count (`[node] partitions`) and `led` is the number of
partitions the node currently leads. The share is recomputed whenever leadership changes.

- **On a single node** `led = P`, so the share is the quota itself: quotas are exact.
- **In a cluster** they are approximate. A tenant's runs hash uniformly across partitions, so
  a node leading a third of the partitions sees about a third of the tenant's work and gets
  about a third of its budget. Rounding up can let the cluster as a whole run slightly more
  than the quota.

Example: `P = 16` on three nodes leading 6, 5 and 5 partitions. A tenant with
`max_concurrency = 10` may run `ceil(10 × 6/16) = 4`, `ceil(10 × 5/16) = 4` and 4 activations on
the three nodes: 12 at most, cluster-wide. With `event_rate = 100/1s` the nodes admit 38, 32
and 32 events per second for the partitions they lead (102 in total). After a failover, when
one node leads all 16 partitions, it enforces the full 10 and 100/s.

The share applies to `max_concurrency`, `memory_bytes`, `fuel_per_s` and `fuel_burst`, and to
the `limit` of `event_rate`. **The `burst` of `event_rate` is not scaled**: each node allows the
configured burst on its own, so a cluster of three nodes can admit up to three bursts at once.
`weight` is relative and is not scaled. `[tenants] quota_share = "leadership"` is the only
policy in 1.0; cluster-exact quotas are planned after 1.0.

Queue flow limits ([flow-control.md](flow-control.md)) are different: each node enforces a
queue's full limits on its own activations, without a share.

### Quota metrics

| Metric | Meaning |
|---|---|
| `ostro_tenant_running` | leased activations (gauge) |
| `ostro_tenant_memory_reserved_bytes` | reserved instance memory (gauge) |
| `ostro_tenant_fuel_balance` | fuel bucket balance (gauge) |
| `ostro_tenant_blocked_total{reason}` | dispatch held: `suspended`, `concurrency`, `memory`, `fuel` |
| `ostro_tenant_ingress_rejected_total{reason,kind}` | ingress refused: `reason` = `suspended` or `event_rate`; `kind` = `start`, `webhook`, `kafka`, `cron`, `signal`, `token` |

`/metrics` is unauthenticated, so these series carry **no `tenant` label by default** (the
gauges are node totals). `[tenants] metrics_tenant_labels = true` adds the label, capped at
`[tenants] metrics_label_limit` (default 1000) distinct tenants; the rest share `other`. Turn it
on only when `/metrics` is reachable by trusted scrapers alone.

## 6. Errors

| Status | `reason` | When |
|---|---|---|
| 401 | `unauthenticated` | no bearer, a malformed, unknown, revoked or expired one |
| 403 | `missing_scope` | the token lacks the scope or the system authority |
| 403 | `forbidden_tenant` | a tenant principal names another tenant, or retires a module another tenant also uses |
| 403 | `tenant_suspended` | ingress for a suspended tenant |
| 404 | — | another tenant's object (§2, the 404 rule) |
| 409 | `tenant_not_empty` | deleting a tenant that still holds something |
| 429 | `tenant_event_rate` | over the tenant's `event_rate`; retry after `Retry-After` |

Authentication and authorization failures are recorded in the audit log as coalesced
`auth.failure` and `authz.denied` records ([audit.md](audit.md)).

## 7. Secrets and tenancy

A tenant's sealed secrets are stored as `<tenant>/<name>`; its workflows refer to them by plain
name in `{{secret:NAME}}` templates. Grants name the qualified module, and egress policies the
tenant's modules. The sealed store, its key and the key rotation procedure are in
[secrets.md](secrets.md#5-the-sealed-store) and
[secrets.md §5.2](secrets.md#52-rotation). In token mode, configure an operator sealing key
(`OSTRO_SEALING_KEY` or `[secrets] sealing_key_file`): without one the server stores no secrets,
and the token key and cluster CA in metadata stay unsealed (a startup warning says so).

## 8. Signed modules and audit

- With `require_signed_modules` on, uploads and activations need a valid signature from a
  trusted key; a tenant admin can tighten (never relax) the rule for its own tenant and add
  tenant-only keys. See [signed-modules.md](signed-modules.md).
- Every administrative action is recorded in a hash-chained, node-local audit journal before it
  answers. See [audit.md](audit.md).

## 9. The UI in token mode

In token mode the embedded UI ([ui.md](ui.md)) asks for an API token at `/ui/login`.
`POST /ui/login` exchanges the bearer for an `ostro_session` cookie:

- The cookie holds only the token id and an expiry, signed with a cluster-wide key
  (`__ostro/ui-session-key`); the bearer never reaches the browser after sign-in. Every node of
  the cluster accepts it.
- Attributes: `Path=/ui; HttpOnly; SameSite=Strict; Max-Age=…` (`[auth] ui_session_ttl`,
  default 8 h), plus `Secure` (below).
- Every request re-checks the token behind the session, so revoking or rotating the token, or
  its expiry, ends the session on the next request.
- UI pages need a token with `read`; actions (cancel, retry, requeue, pause, ...) need
  `operate`. A token with neither (for example deploy-only) gets 403. The UI applies the same
  tenant rules as the API: a tenant token sees only its tenant.
- Sign-in and sign-out need the page's CSRF token. A failed sign-in shows one generic message;
  the reason goes to the audit log.

**`Secure` and TLS proxies.** The cookie is marked `Secure` when the client listener itself
serves TLS (`[api.tls] mode` = `files`, `acme` or `self-signed`, or `auto` on a non-loopback
address), or when the request URI is `https`. Ostro does not read `X-Forwarded-Proto` or
`Forwarded`. Behind a TLS-terminating proxy that talks plain HTTP to Ostro
(`[api.tls] mode = "off"`), the cookie is therefore **not** marked `Secure`. Either keep TLS on
the Ostro listener behind the proxy (for example `self-signed` or `files`, with the proxy
re-encrypting), or have the proxy add `Secure` to the `ostro_session` `Set-Cookie` header.

With `[auth] mode = "none"` the UI stays open, as in `ostro dev`. The UI is served by default
only when the API listener is a loopback address; `[ui] enabled = true` serves it on other
addresses, and `[ui] allowed_hosts` lists extra `Host` names to accept.
