# Connectors: HTTP, SQL and object effects

This document covers the Phase 6 effects bridge (`crates/ostro-bridge`): how a guest's
`http-fetch`, `sql-execute` and `object-op` calls are executed, what each connector guarantees,
how it is configured, and which errors a guest can see.

Related documents:
- [security-egress.md](security-egress.md): the SSRF model, egress allow-lists and the bypass table.
- [secrets.md](secrets.md): `{{secret:NAME}}` templating, grants, scrubbing, the sealed store
  and the secret providers.
- [storage-format.md](storage-format.md) §16: the partition `effects` rows and `CompleteEffect`.

The frozen semantics (W1–W8) live in the doc comments of `wit/ostro-runtime-0.1.0/runtime.wit`
(`interface effects`). 06-01 confirmed that they are all present there. That file is the
contract. This document explains it and adds the operator-facing detail. Where the two seem to
disagree, runtime.wit wins, and the disagreement is a documentation bug.

---

## 1. The effect lifecycle

```
guest http-fetch / sql-execute / object-op        (live activation)
  └─ host journals EffectScheduled{op, request}    (headers named in `redact` → "[redacted]")
     and keeps the full request in memory          (real header values, retry policy)
guest wait([op]) → the batch with EffectScheduled is committed to the partition log
  └─ only now: EffectDriver::start(run, attempt, effect)      ← FX-01: intent before I/O
     (derive: EffectScheduled → Intent::RegisterEffect → an `effects` row, keyspace #12)
bridge task (node-owned, one per (partition, run, op); survives eviction):
  send gate → rate limit → connector.execute → scrub → encode once
  → Cmd::CompleteEffect{run, op, outcome}           (no attempt token; first completion wins)
apply CompleteEffect: row present → journal EffectCompleted, delete the row, wake the run
                      row absent  → Duplicate (nothing written)
guest's wait resolves from the journaled EffectCompleted (the same bytes live and on replay)
```

**Intent before I/O.** The engine starts an effect only after the append that carries its
`EffectScheduled` has returned `Ok`. A fenced or conflicting append starts nothing. A crash
before the commit therefore never leaves an untracked side effect.

**Node-owned tasks.** The bridge keeps a registry keyed by `(partition, run_id, op)`. A second
`start` for the same key (a live start in activation N and a re-drive in activation N+1)
attaches to the running task instead of sending twice. Tasks outlive the activation. A run that
evicts on a hot-wait timeout while its HTTP call runs still gets the completion.

**`CompleteEffect`.** Completions go only through `Cmd::CompleteEffect` (Cmd tag 13). Its
`outcome` is the fully encoded `EffectCompleted` journal payload (a blob ref above 256 KiB).
Apply stores it verbatim at the next seq. It is attempt-independent and first-wins, like
`CompleteToken`. The guest sees a completion only as a journaled input, so a live activation and
a replay see byte-identical results.

**Re-drive only in replay.** Nothing re-sends from the journal. There is no sweeper. When an
activation replays an `EffectScheduled` that has no completion anywhere in the loaded journal,
the engine calls `start` again with `redrive = true` and the guest's re-issued request (which
carries the real header values and the retry policy that the journal does not). HTTP then runs
the unknown-outcome window check (§2.6). SQL relies on its token (§3.1).

**The leader-start wake.** A run that evicted with an effect in flight and whose node then
crashed would never be dispatched again: nothing is due. So when a partition accepts a
`LeaderAnnounced`, it scans the `effects` rows and gives every `Scheduled` run that still has
one a ready row. The run replays and re-drives. Runs without effects rows are unaffected, so the
wake changes nothing for logs written before Phase 6.

**Cancellation.** `cancel-op` on an effect journals `OpCancelled`. The derive step turns it into
`Intent::CancelEffect`, which deletes the row, and the bridge cancels the task (best effort: the
side effect may already have happened). A completion that arrives later finds no row and is a
`Duplicate`.

### 1.1 The send gate and the at-least-once boundary

Immediately before resolving secrets and before every send or retry, the task checks that:
- the node still leads the partition at an epoch not older than the effect's;
- the run exists and is not terminal;
- the `effects` row for `(run, op)` still exists.

If any check fails, the task stops without sending and submits nothing. This narrows, but cannot
close, the window in which a fenced node sends after losing its lease. The idempotency key (HTTP)
or the token row (exactly-once SQL) is the only protection on the external side. **HTTP is
at-least-once on the wire. It is exactly-once only towards a provider that honours the key.**

### 1.2 Completions for terminal runs

A response that arrives after the run became terminal is dropped. `CompleteEffect` is rejected
with `RunTerminal` and the completion sink treats it as `Dropped`. **The side effect may still
have happened.** Ostro cannot undo it.

Resumable terminals (Terminated, TimedOut and executor-level failures) keep their `effects` rows.
A resumed run therefore replays, finds the effect without a completion, and re-drives it with the
same key (HTTP) or the same token (SQL). Non-resumable terminals and retention expiry delete the
rows.

### 1.3 Connector panics

A connector that panics completes its effect `non-retryable{kind: "internal_error", message:
"bridge connector panicked"}` exactly once. The task is not respawned, so the run never hangs.

---

## 2. HTTP (`http-fetch`)

### 2.1 Idempotency key

Every HTTP effect carries a stable key:

```
key = "ostro-" + lower-hex( sha256( b"ostro/idem/v1" ‖ run_id_utf8 ‖ op.to_le_bytes() )[..16] )
```

The key is 38 characters long. It fits Stripe (255), Adyen (64) and Square payments (45). It
never contains the attempt, so it is the same on every retry, every re-drive and every resume. A
retry-as-new run and a continue-as-new run get new run ids and therefore new keys.

Pinned vectors (`http::key` tests):

| run_id | op | key |
|--------|----|-----|
| `0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b` | 1 | `ostro-bde26f8232abe4e7e94943f970fc2c33` |
| `0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b` | 0x0102030405060708 | `ostro-07d79386f8bb969ca49792f371a64344` |
| `run-1` | 1 | `ostro-6ac4a780ae09b9770d9fb056ef9a5364` |
| `run-1` | 0x0102030405060708 | `ostro-25d328cd03c15ed4fc3e117a5e82626f` |

- The key is sent as `Idempotency-Key`. The header name is configurable per destination
  (`idempotency_header`, §2.7). `null` or `""` suppresses the header.
- **Override:** a guest-supplied `http-request.idempotency-key` is sent verbatim instead.
- A guest-set header with the same name as the key header also wins. It is dropped on
  cross-origin redirect hops.

**`ctx.idempotency_key(name)` (Rust SDK).** The guest cannot know its op id before the call. For
providers that read the key from the body (Square) or for keys that must survive a code change
that shifts op ids, the SDK computes a semantic key in the guest:

```
"ostro-" + lower-hex( sha256( b"ostro/idem/v1/name" ‖ run_id ‖ 0x00 ‖ name )[..16] )
```

Pass it as the override, and embed it in the body when the provider wants it there:

```rust
let key = ctx.idempotency_key("charge");
let resp = ctx.http().post("https://api.stripe.com/v1/charges")
    .secret_header("Authorization", "Bearer {{secret:stripe/live}}")
    .json(&serde_json::json!({ "amount": 1000, "idempotency_key": key }))?
    .idempotency_key(&key)
    .send()?;
```

### 2.2 Retries

In-attempt retries follow the guest's `retry-policy`, but only for conditions where a retry is
safe:

| Class | Conditions | Retried |
|-------|------------|---------|
| never left | DNS failure, connection refused, TLS handshake failure | always, within the attempt budget |
| after send | timeout awaiting the response, reset mid-response, body stream failure | only with a policy, when the key header is sent (or the method is RFC 9110 idempotent) and the window allows |
| retry status | 408, 425, 429, 5xx, and a 409 that says the same key is still in progress | only with a policy |
| final | anything else | never: the response is the outcome |

- With no guest policy: up to 3 attempts, "never left" only (200 ms initial, ×2, max 5 s).
- With a policy: `max-attempts` in total and the policy's backoff. `Retry-After` (delta seconds)
  replaces the computed delay, capped at `max-interval`.
- A `non-retryable-kinds` entry of `http_transport` disables transport retries.
- Every retry reuses the same headers, so the same key. The send gate is re-checked before each.

### 2.3 Timeouts and size cap

- `timeout-ms` bounds **one attempt**, not all retries together. The default is the
  destination's `timeout`, else `[egress] default_timeout` (30 s). The connect timeout is
  `[egress] connect_timeout` (5 s).
- The response body is read as a stream with a hard byte cap: the destination's
  `max_response_bytes`, else `[egress] max_response_bytes` (4 MiB), always clamped to
  `hard_max_response_bytes` (64 MiB). Over the cap → `non-retryable{response_too_large}`.
- Outcomes over 256 KiB are journaled as blob refs, so a large response does not bloat the
  partition log (it is still stored).

### 2.4 Any status is a success

Any final status, including a 4xx or a 5xx after the retry budget runs out, completes the effect
**successfully** with `http-response{status, headers, body}`. The guest decides what a status
means. Failures are reserved for "no usable response" (§6).

### 2.5 Redirects and compression

- The client never follows redirects itself. **A request that contains a `{{secret:` placeholder
  never follows redirects:** the 3xx is returned as the response.
- An untemplated request follows redirects in the connector, up to `[egress] max_redirects` (5)
  hops. Every hop is re-checked against the module's allow-list and the IP-literal classifier.
  303 (and 301/302 for POST) become GET without a body. Cross-origin hops drop `Authorization`,
  `Cookie`, `Proxy-Authorization` and the key header. Past the limit, the last 3xx is the
  response.
- **Templated requests force `Accept-Encoding: identity`** (the guest's `Accept-Encoding` and `TE`
  are dropped) and **refuse a compressed response** (`Content-Encoding` other than identity →
  `non-retryable{http_transport}`), because compression would hide a secret from the scrubber.
  Untemplated requests send no `Accept-Encoding` at all and never decompress.

### 2.6 The unknown-outcome window

The decision is made **only at re-drive time**: replay found `EffectScheduled` without a
completion, and no bridge task for it is running. A live first dispatch always sends.

```
elapsed = wall_now - EffectScheduled.ts                 (conservative: ts ≤ first send)
if method ∈ {GET, HEAD, OPTIONS, PUT, DELETE} and !force_window   → re-send (same key)
else if resend == "never"                                         → effect_outcome_unknown
else if elapsed + safety_margin < idempotency_window              → re-send (same key)
else                                                              → effect_outcome_unknown
```

- The method match is **case-sensitive** (RFC 9110 methods are upper case). `post` is not `POST`
  and is treated as non-idempotent.
- `effect_outcome_unknown` is `non-retryable` with details `{"op", "key", "elapsed_ms"}`. **Nothing
  is sent.** The guest must reconcile, for example by looking the payment up by its key.
- Defaults: `idempotency_window = "24h"`, `safety_margin = "5m"` (`[egress]`), overridable per
  destination.

Provider windows are configuration. Ostro cannot discover them. Known values:

| Provider | Key location | Window | Confidence |
|----------|--------------|--------|------------|
| Stripe API v1 | `Idempotency-Key` header | keys may be pruned once at least 24 h old | HIGH (Stripe docs) |
| Stripe API v2 | header | 30 days | LOW, unverified |
| PayPal | `PayPal-Request-Id` header | per endpoint, no number published | MEDIUM |
| Adyen | `Idempotency-Key` header | 7–14 days | LOW |
| Square | `idempotency_key` **in the body** | not stated | LOW; use `ctx.idempotency_key` + `idempotency_header = ""` |
| Nuvei / Yoco / Lead Bank | varies | 15 min / 24 h / 48 h | LOW (shows the spread) |

> **Exactly-once HTTP requires a provider that honours the key.** Against a provider without
> idempotency keys, a crash between the provider's commit and Ostro's `CompleteEffect` makes the
> re-drive send the request again (within the window) or report `effect_outcome_unknown`
> (outside it). Configure `resend = "never"` for such destinations if a duplicate is worse than
> an unknown outcome.

### 2.7 Per-destination config: `http-target`

A `Connection{kind: "http-target"}` whose **name is the normalised origin** configures one
destination. REST paths percent-encode the name.

```json
{"v":1, "idempotency_window":"24h", "idempotency_header":"Idempotency-Key",
 "resend":"window", "force_window":false, "safety_margin":"5m",
 "rate":"100/s", "burst":100, "max_response_bytes":4194304, "timeout":"30s"}
```

| Key | Default | Meaning |
|-----|---------|---------|
| `idempotency_window` | `[egress] idempotency_window` (24h) | how long the provider remembers a key |
| `idempotency_header` | `Idempotency-Key` | key header name; `null` or `""` suppresses it |
| `resend` | `window` | `window` or `never` (never re-send a non-idempotent request) |
| `force_window` | `false` | apply the window to idempotent methods too |
| `safety_margin` | `[egress] safety_margin` (5m) | subtracted from the window (clock skew, pruning slack) |
| `rate`, `burst` | none | per-target quota (§5) |
| `max_response_bytes` | `[egress] max_response_bytes` | body cap, clamped to the hard max |
| `timeout` | `[egress] default_timeout` | per-attempt timeout |

Every key is optional. An unknown `"v"`, an unknown key or an invalid value fails the effect with
`connection_unknown`: a typo must never fall back to a default silently, because the window
decides whether a payment is re-sent.

```sh
ostro connection set https://api.stripe.com --kind http-target --config-file stripe.json
```

### 2.8 Redaction

- Request header values whose names appear in `redact` (ASCII case-insensitive) are journaled as
  `[redacted]`. The live request keeps the real value. The command hash is computed so that it is
  unaffected.
- Response headers named in `redact` are returned as `[redacted]`.
- Separately, every resolved secret value is scrubbed from the response body and every response
  header value as `[REDACTED]` ([secrets.md](secrets.md) §4). The two spellings are deliberate:
  `[redacted]` = the guest asked for it, `[REDACTED]` = the scrubber found a secret.

---

## 3. SQL (`sql-execute`)

Named PostgreSQL (`sql-postgres`) and MySQL (`sql-mysql`) connections, through lazily built
sqlx 0.9.1 pools. A module may use connection `X` only if its egress policy lists
`connection:X` (else `egress_denied`, checked before the existence check, so an unauthorised
module learns nothing). An unknown connection is `connection_unknown`.

### 3.1 The exactly-once protocol (`exactly-once: true`)

```
key = sha256("ostro/sql-effect/v1" ‖ cluster_id ‖ run_id ‖ op_le)      (32 bytes)
1. BEGIN
2. INSERT INTO ostro_effects(effect_key, run_id, op)       -- token FIRST: concurrent dupes block on the PK
     unique violation (PG 23505 / MySQL 1062) → ROLLBACK; SELECT result:
       NULL → non-retryable{sql_exactly_once_violated}
       else → the stored completion bytes, verbatim
3. the user statement (one statement, parameters bound; rows capped → ROLLBACK, response_too_large)
4. UPDATE ostro_effects SET result = <the completion bytes> WHERE effect_key = key
5. COMMIT
6. CompleteEffect(<the same bytes>)
```

The stored `result` is `codec_ver (1 byte) ‖ RecordedResult bytes`, exactly the completion the
driver submits, so a live completion and a re-drive's completion are byte-identical. This
layout is a persistent contract.

Pinned effect-key vectors:

| cluster_id | run_id | op | key |
|---|---|---|---|
| `c1` | `0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b` | 1 | `74342fdf6c604ee6c26bf2e51daf8d49a5d8c125cfe7eab5f201c834faa066c5` |
| `cluster-a` | `run-1` | 0x0102030405060708 | `3f3716faef97c69c8d359603d15f651ea8088b71450d58a14be6fa3df6d77cf6` |

**SQL after COMMIT (B4).** Once COMMIT has been sent, the connector **never** completes
Retryable. A COMMIT error or a lost connection starts a bounded reconcile loop with the same key
on a fresh pooled connection (5 attempts, 200 ms doubling to 5 s):
- a stored result → returned verbatim;
- a NULL result → `sql_exactly_once_violated`;
- no row (the COMMIT never landed) → the whole protocol runs again, which is safe because nothing
  was committed.

When the loop runs out, the effect completes `non-retryable{effect_outcome_unknown}`. Errors
**before** COMMIT stay as mapped in §3.6.

**Re-drive horizon (W12).** A re-drive later than `sql_effects_retention` − 1 h after the
effect was scheduled completes `effect_outcome_unknown` **without touching the database**. The
token row may already have been garbage-collected, so running the statement again could apply it
twice.

SC2 is proven by `crates/ostro/tests/fx_sql_crash.rs`: 50 runs held after COMMIT and before
`effect_completed`, then kill -9 and restart, end with exactly 50 rows.

### 3.2 Statement allow-list

Exactly-once statements must pass an **allow-list** gate before anything runs (an allow-list
survives new server versions; a deny-list does not). Leading whitespace and comments (`--`,
`/* */`, and `#` on MySQL) are stripped. Statements starting with `/*!` or `/*+` (MySQL
executable comments and hints) are rejected. The first keyword must then be one of:

`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `WITH`, `VALUES`, `TABLE`, `REPLACE` (MySQL),
`MERGE` (PostgreSQL 15+).

- `SELECT … INTO OUTFILE | DUMPFILE` is rejected (it writes outside the transaction).
- `CALL` is allowed only with the connection option `allow_call = true`, because procedures can
  contain implicit commits.
- Everything else fails `non-retryable{sql_not_transactional}` with nothing executed.

Statements run through the extended/prepared protocol (`sqlx::query`, never `raw_sql`), so one
effect is always exactly one statement.

**MySQL 8.4 statements that cause an implicit commit** (MySQL 8.4 Reference Manual §15.3.3). All
are rejected by the allow-list:

- *DDL:* ALTER EVENT, ALTER FUNCTION, ALTER PROCEDURE, ALTER SERVER, ALTER TABLE, ALTER
  TABLESPACE, ALTER VIEW, CREATE DATABASE, CREATE EVENT, CREATE FUNCTION, CREATE INDEX, CREATE
  PROCEDURE, CREATE ROLE, CREATE SERVER, CREATE SPATIAL REFERENCE SYSTEM, CREATE TABLE, CREATE
  TABLESPACE, CREATE TRIGGER, CREATE VIEW, DROP DATABASE, DROP EVENT, DROP FUNCTION, DROP INDEX,
  DROP PROCEDURE, DROP ROLE, DROP SERVER, DROP SPATIAL REFERENCE SYSTEM, DROP TABLE, DROP
  TABLESPACE, DROP TRIGGER, DROP VIEW, INSTALL PLUGIN, RENAME TABLE, TRUNCATE TABLE, UNINSTALL
  PLUGIN. `CREATE/DROP TEMPORARY TABLE` does not commit but cannot be rolled back (rejected too);
  `CREATE TABLE … SELECT` commits before and after.
- *Implicit use of `mysql` tables:* ALTER USER, CREATE USER, DROP USER, GRANT, RENAME USER,
  REVOKE, SET PASSWORD.
- *Transaction control and locking:* BEGIN, LOCK TABLES, `SET autocommit = 1`, START
  TRANSACTION, UNLOCK TABLES; also COMMIT, ROLLBACK, SAVEPOINT, RELEASE SAVEPOINT, `XA …` and
  SET TRANSACTION.
- *Data loading:* LOAD DATA.
- *Administrative:* ANALYZE TABLE, CACHE INDEX, CHECK TABLE, FLUSH, LOAD INDEX INTO CACHE,
  OPTIMIZE TABLE, REPAIR TABLE, RESET.
- *Replication control:* START/STOP/RESET REPLICA, CHANGE REPLICATION SOURCE TO, and the older
  START/STOP SLAVE, CHANGE MASTER TO, RESET MASTER.
- *Dynamic SQL:* PREPARE, EXECUTE, DEALLOCATE (can hide DDL).

MySQL stored functions and triggers cannot commit, so DML that fires triggers is safe. MariaDB
has its own list and is best effort.

**PostgreSQL notes.** PostgreSQL has no implicit commits. Statements that cannot run inside a
transaction block (CREATE/DROP DATABASE, VACUUM, `CREATE INDEX CONCURRENTLY`, ALTER SYSTEM, …)
would fail loudly and roll back; the allow-list rejects them up front for a clear error. The
dangerous ones are the transaction-control statements (`COMMIT`, `END`, `ROLLBACK`, `ABORT`,
`PREPARE TRANSACTION`, `COMMIT PREPARED`), which would end the open transaction. They are
rejected. A `CALL` of a procedure that commits errors inside the transaction block, which is
safe.

> **Warnings the gate cannot check.**
> - **MyISAM, MEMORY and other non-transactional MySQL engines** silently break atomicity: the
>   user write is not rolled back with the token. Use InnoDB for every table an exactly-once
>   statement touches.
> - **dblink / postgres_fdw / federated writes** are not atomic with the token row.
> - The token table must live in the **same database / schema** as the user tables.

### 3.3 `exactly-once: false`

Autocommit execution on a pooled connection. **At-least-once on re-drive:** a statement whose
effect was applied before a crash runs again. Any statement is allowed. A result over the row or
byte cap fails `response_too_large`, but the statement has already been applied (there is no
transaction to roll back).

### 3.4 The `ostro_effects` token table

PostgreSQL:
```sql
CREATE TABLE IF NOT EXISTS ostro_effects (effect_key bytea PRIMARY KEY, run_id text NOT NULL,
  op bigint NOT NULL, result bytea, created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS ostro_effects_created_at ON ostro_effects (created_at);
```
MySQL:
```sql
CREATE TABLE IF NOT EXISTS ostro_effects (effect_key BINARY(32) PRIMARY KEY,
  run_id VARCHAR(64) NOT NULL, op BIGINT UNSIGNED NOT NULL, result LONGBLOB,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY ostro_effects_created_at (created_at)) ENGINE=InnoDB;
```

- The DDL runs when the pool is built, **outside** any effect transaction (on MySQL, DDL commits
  implicitly).
- `manage_schema = false`: Ostro does not run the DDL and logs it at info level instead, for the
  DBA to apply.
- `effects_table` renames the table (a plain identifier).
- **GC:** every 10 minutes, batched `DELETE … WHERE created_at < now() - retention` in batches of
  1000. The retention is `[connectors] sql_effects_retention` (168h, at least the run retention).

### 3.5 Connection config

```json
{"v":1, "host":"db.internal", "port":5432, "database":"app", "schema":"public",
 "user":"ostro", "password":"{{secret:db-pass}}", "ssl_mode":"require", "ca_file":"/etc/ca.pem",
 "pool_max":10, "effects_table":"ostro_effects", "manage_schema":true, "allow_call":false,
 "max_rows":10000, "max_bytes":4194304, "rate":"100/s", "burst":100}
```

- `password` must be a `{{secret:NAME}}` template, resolved through the operator path (no module
  grant needed) when the pool is built. A literal password is refused: it would sit in plain text
  in the meta store.
- `schema` is PostgreSQL only and becomes the `search_path` of every pooled connection.
- `max_rows` / `max_bytes` override `[connectors] sql_max_rows` (10000) and `sql_max_bytes`
  (4 MiB).
- Unknown keys or an unknown `"v"` are `connection_unknown`.
- **TLS (D-FX2):** native roots plus an optional per-connection `ca_file`. PostgreSQL `ssl_mode`:
  `disable | prefer | require | verify-ca | verify-full` (default `prefer`, as libpq). MySQL:
  `disabled | preferred | required | verify_ca | verify_identity` (default `required`, because
  MySQL 8 `caching_sha2_password` needs TLS without the RSA feature, which Ostro never enables).
- sqlx reads libpq environment variables (including `PGPASSWORD`) for a connection without a
  password template. That environment belongs to the operator.

### 3.6 Value mapping and SQL errors

| Database type | `sql-value` |
|---------------|-------------|
| bool | `boolean` |
| int2/int4/int8, MySQL TINYINT…BIGINT | `int8` (MySQL BIGINT UNSIGNED above i64::MAX → `numeric`) |
| float4/float8 | `float8` |
| numeric/decimal | `numeric` (string; PostgreSQL keeps the column's scale) |
| text/varchar/char/name/enum, uuid | `text` |
| bytea/blob/binary | `bytea` |
| json/jsonb | `json` |
| timestamptz, MySQL TIMESTAMP | `timestamptz` |
| timestamp without time zone, DATETIME | `timestamptz`, interpreted as UTC |
| date/time/interval/arrays/other | `non-retryable{sql_unsupported_type}`: cast in SQL (`::text`) |

Pre-1970 timestamps cannot be represented (`datetime.seconds` is unsigned) and fail
`sql_unsupported_type`. Parameters map the reverse way.

Errors before COMMIT: PostgreSQL `40001`/`40P01` and MySQL `1213`/`1205` → `retryable{sql_error}`
(same key on retry); transport errors (I/O, pool timeout) → `retryable{sql_error}`; any other
database error → `non-retryable{sql_error, details: {sqlstate, message}}`. Messages are the
database's text with the connection password and URL credentials scrubbed.

---

## 4. Object store (`object-op`)

Object effects run through the shared `ostro-objstore` crate (D-FX1: rusty-s3 over reqwest +
rustls; no `object_store`, no chrono). Phase 8 owns the crate (backups, blob GC); Phase 6 added
`put_with` and `presign`.

### 4.1 Backends

- **S3-compatible:** AWS S3, SeaweedFS, Cloudflare R2, Ceph. Path- or virtual-host-style URLs.
- **Local filesystem** (`backend = "local"`): tmp + fsync + rename writes; content types in a
  `<key>.ostro-meta.json` sidecar (keys ending in that suffix are refused).
- **GCS: unverified.** It should work as `backend = "s3"` with
  `endpoint = "https://storage.googleapis.com"`, `region = "auto"` and GCS HMAC keys, through the
  XML API's S3 interoperability. Nobody has run that yet (human_needed); do not rely on it until
  it is verified.
- **Azure Blob: deferred** until a native backend exists in `ostro-objstore` (it has no S3 API).

Object-store endpoints are **operator-configured and trusted**. They use the operator client,
which is exempt from module allow-lists and the private-range block (an S3 store on localhost is
legitimate) but still ignores env proxies. Module access is governed by `connection:<name>` in
the module's egress policy.

### 4.2 Ops (W7)

| op | output (`op-output.object`) |
|----|-----------------------------|
| `get` | the object bytes; over `max_get_bytes` → `response_too_large` |
| `put` | UTF-8 JSON `{"e_tag"}`; body required; over `max_put_bytes` → `response_too_large` |
| `head` | UTF-8 JSON `{"size", "last_modified" (RFC 3339), "e_tag", "version"}` |
| `delete` | empty (idempotent) |
| `presign-get`, `presign-put` | the UTF-8 URL, valid for the connection's `presign_expiry` |
| anything else | `non-retryable{unsupported_op}` |

Connector-specific kinds: `not_found` (get/head of an absent key), `invalid_request` (bad key,
missing put body) and `object_error` (backend failure: retryable for transport, I/O, 5xx and 429;
non-retryable otherwise). S3 error bodies are never copied into failures (AWS echoes the access
key id there).

### 4.3 Presigned URLs

- **S3:** SigV4 query signing through rusty-s3. `presign-put` signs the connection's
  `content_type`, so the uploader must send that exact `Content-Type`. GET URLs may carry
  `response-content-type`. S3 accepts 1 s to 7 days; URLs signed with session tokens expire with
  the token.
- **Local backend:** Ostro serves `GET|PUT /v1/objects/{connection}/{key}?exp=<unix>&sig=<hex>`.
  The signature is `HMAC-SHA256(key, "ostro/presign/v1\n<METHOD>\n<connection>\n<key>\n<exp>")`
  with a per-connection 32-byte key stored as the sealed secret `__ostro/presign/<connection>`
  (created on first use). The comparison is constant-time. The route sits **outside** the REST
  middleware stack (no auth, no trace layer, since the query carries the signature): the
  signature is the authentication. Bad, tampered or expired signatures get 403. The base URL is
  `[connectors] public_base_url`, else the API listen address.
- The expiry comes from the connection config (`presign_expiry`, default
  `[connectors] presign_default_expiry` = 15m, range 1 s to 168 h). WIT 0.1.0 has no expiry
  argument (D-FX8).

### 4.4 Connection config

```json
{"v":1, "backend":"s3", "endpoint":"https://s3.eu-west-1.amazonaws.com", "bucket":"files",
 "region":"eu-west-1", "url_style":"path", "allow_http":false, "prefix":"tenant-a",
 "credentials":{"access_key_id":"{{secret:s3-ak}}", "secret_access_key":"{{secret:s3-sk}}"},
 "presign_expiry":"15m", "max_get_bytes":16777216, "max_put_bytes":67108864,
 "content_type":"application/octet-stream", "rate":"100/s", "burst":100}
{"v":1, "backend":"local", "root":"/var/lib/ostro/objects", "prefix":"a"}
```

- `secret_access_key` and `session_token` must be templates; `access_key_id` may be literal.
  Credentials resolve when the backend is built and are rebuilt after `[connectors] backend_ttl`
  (5m).
- `prefix` is prepended to every guest key. `max_get_bytes` defaults to
  `[connectors] object_max_get_bytes` (16 MiB), `max_put_bytes` to 64 MiB.
- `content_type` is sent with every `put`: `object-request` has no content-type field in WIT
  0.1.0.

---

## 5. Rate limits

Rate limits are **per node** (D-FX7). Cluster-wide, partition-owned buckets (FR-4.5) are Phase
7/13 work.

- One governor limiter per target: the origin for HTTP, the connection name for SQL and object.
- The quota comes from the target's `rate` (`"100/s"`, `"600/m"`) and `burst` (default: the
  per-second rate). Targets without a quota use `[connectors] default_rate`, if set; otherwise
  they are unlimited.
- An effect waits for a permit for at most `min([egress] rate_max_wait (30s), its timeout)`, then
  fails `retryable{rate_limited}`.
- Metric: `ostro_effect_rate_limited_total`.

---

## 6. Error kinds (normative)

`ErrorInfo.kind` values a guest can see from an effect. The string is the contract: it is matched
against `retry-policy.non-retryable-kinds` and must never be renamed.

| Kind / failure | Connector | When | Retryable |
|----------------|-----------|------|-----------|
| `effect_outcome_unknown` | http, sql | HTTP re-drive outside the window (§2.6); SQL re-drive past the horizon (W12) or the reconcile loop after COMMIT ran out (B4) | no |
| `egress_denied` | all | URL/redirect hop not allowed or resolving to a blocked address; placeholder in URL, method or header name; module lacks `connection:<name>` | no |
| `secret_not_granted` | http (+ config templates) | no grant for this module, secret and origin; body placeholder without `body` scope; invalid or reserved name; value under 8 bytes | no |
| `secret_not_found` | all | granted secret absent from every provider | no; **yes** when at least one provider was unavailable |
| `rate_limited` | all | no permit within `rate_max_wait` (§5) | yes |
| `response_too_large` | all | HTTP body, SQL rows/bytes, object get/put over the cap | no |
| `connection_unknown` | all | unknown connection, wrong kind, or an invalid `http-target`/SQL/object config | no |
| `sql_not_transactional` | sql | exactly-once statement rejected by the allow-list (§3.2) | no |
| `sql_exactly_once_violated` | sql | token row exists with a NULL result (an implicit commit split the transaction) | no |
| `unsupported_op` | object, any | unknown object op; no connector registered for the effect kind | no |
| `http_transport` | http | transport failure after the retry budget; templated request got a compressed response | yes for transport loss; no for refused encoding |
| `sql_error` | sql | database error (§3.6) | yes for deadlock, serialization, lock wait and transport; no otherwise |
| `sql_unsupported_type` | sql | a column type `sql-value` cannot represent | no |
| `not_found` | object | get/head of an absent key | no |
| `invalid_request` | object | bad key, put without body | no |
| `object_error` | object | backend failure | yes for transport, I/O, 5xx, 429; no otherwise |
| `internal_error` | all | the connector panicked (§1.3) | no |
| `sealing_key_missing` | object (local presign), admin | no sealing key is configured, so the presign key (or a secret) cannot be stored | no (the operator must configure a key) |
| `failure::timeout` | engine | a wait timeout on the effect op elapsed | per policy |
| `failure::cancelled` | engine | the op was cancelled (`cancel-op`, run cancel) | no |
| `unsupported` | engine | an effect with no driver (a node without bridge services; child workflows until Phase 9) | no |

Notes:
- A per-attempt HTTP timeout is not `failure::timeout`: it is a transport failure (`http_transport`)
  that may be retried (§2.2).
- `sealing_key_missing` on the admin API is HTTP 412 / `FAILED_PRECONDITION` with
  `details.reason = "sealing_key_missing"`.

---

## 7. Configuration

`node.toml` sections (durations use the server's parser; write hours, not days: `168h`, never
`7d`). Unknown keys inside these sections are ignored, like the rest of the node config.

```toml
[egress]
allow_private      = []        # CIDRs exempt from the private-range block; see security-egress.md
connect_timeout    = "5s"
default_timeout    = "30s"     # per attempt
max_response_bytes = "4MiB"    # hard max 64 MiB
max_redirects      = 5
rate_max_wait      = "30s"
idempotency_window = "24h"
safety_margin      = "5m"

[connectors]
sql_max_rows           = 10000
sql_max_bytes          = "4MiB"
sql_effects_retention  = "168h"
object_max_get_bytes   = "16MiB"
presign_default_expiry = "15m"
backend_ttl            = "5m"
# default_rate         = "100/s"            # unset = unlimited
# public_base_url      = "https://ostro.example.com"   # default: the API listen address

[secrets]                      # see secrets.md
```

Environment overrides use the usual form, for example `OSTRO_EGRESS__ALLOW_PRIVATE`.

---

## 8. Admin surface

| Object | REST | gRPC | CLI |
|--------|------|------|-----|
| connections | `PUT/GET/DELETE /v1/connections/{name}`, `GET /v1/connections` | `ostro.api.v1.Connectors` | `ostro connection set/ls/get/rm` |
| egress policies | `PUT/GET/DELETE /v1/egress/{module}`, `GET /v1/egress` | `ostro.api.v1.Connectors` | `ostro egress set/ls/get/rm` |
| secrets, grants | see [secrets.md](secrets.md) §7 | `ostro.api.v1.Secrets` | `ostro secret`, `ostro grant` |
| local presign | `GET/PUT /v1/objects/{connection}/{key}?exp=&sig=` | — | — |

`PUT /v1/connections/{name}` validates the config with the same parser the connector uses (an
`http-target` name must equal its normalised origin; a SQL connection with a literal password is
400). Connection configs are returned verbatim: they hold templates, never secret values.

Metrics: `ostro_effect_duration_seconds{kind}`, `ostro_effect_completed_total{kind,outcome}`,
`ostro_effect_inflight`, `ostro_effect_rate_limited_total`.

---

## 9. Known limitations and follow-ups

- **SQL results are fully buffered before the caps apply.** A statement with result columns runs
  with `fetch_all`; the row and byte caps are checked while decoding. An oversized result still
  rolls back and fails `response_too_large`, but it is held in memory first. Streaming with an
  early stop needs `futures-core` as a direct ostro-bridge dependency (follow-up).
- Rate limits are per node (D-FX7).
- `allow_private` is node-level only; per-rule `allow_private` is deferred (P-6).
- GCS interoperability is unverified; Azure Blob is deferred.
- Presign expiry is per connection until a WIT 0.1.N `object-presign` function exists (D-FX8).
- The SC3 canary scan of the OTLP export runs at the Phase 8 gate (D-FX6).
