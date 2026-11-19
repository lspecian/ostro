# Phase 14: Multi-Tenancy, Security & 1.0 Release - Research

**Researched:** 2026-11-19
**Domain:** tenant isolation, auth/scopes, quotas and fair dispatch, signed modules, audit, fuzzing, fault injection, benchmarks, release engineering
**Confidence:** HIGH for the pre-freeze findings (all verified against the code at `9a79ca8` and the Phase 5–9 plans). MEDIUM for the outline of the rest of the phase (tool versions checked; designs not yet planned in detail).

**Scope note.** Most of this document is **pre-freeze findings**. Phase 5 plans 05-07 (meta encoding), 05-05 (WIT doc comments), 05-13 (API) and 05-20 (freeze flip) have not run yet. This research says what Phase 14 needs from the formats they freeze. The rest of Phase 14 (fuzzing, Jepsen-style runs, soak, benchmarks, distribution, docs) is outlined only, for later planning.

<user_constraints>
## User Constraints

No `14-CONTEXT.md` exists, because `/gsd:discuss-phase` has not run for Phase 14. The binding constraints come from the orchestrator brief and the project rules:

### Locked (project rules that bind this phase)
- **Freeze (D18/D20).** 05-20 freezes WIT 0.1 (byte-identical sha256 in `wit/FROZEN.toml`), the command set, the log, `node.toml`, the blob layout and the meta encoding.
  - After the freeze, any meta change is a **new `MetaRecord` variant appended in RESERVATIONS order, plus a `META_FORMAT` bump with decoders for every older format**. Fields are never added to existing meta structs.
  - WIT changes after the freeze go only through a new `0.1.N` package. Doc comments can't change either, because the freeze test checks bytes.
- **Ordinals and reserved names** are allocated only from `.planning/RESERVATIONS.md`, through a docs-only commit before any code (rule 2).
- **Environment.**
  - Don't use the reachable microk8s cluster.
  - No Docker locally (P8-D18): Docker-only legs are human_needed.
  - No git remotes and no pushes. Tags stay local.
  - Never reference a Chronik clone path.
- **Licence.** No `chrono`, `bincode`, `openssl-sys`, `object_store` or wasmtime `cache` feature (deny.toml).
- **Toolchain.** Rust 1.100.0, single static binary.
- **Platform-bound criteria are human_needed:** macOS notarization (needs an Apple Developer ID), the native perf box (D15), the 3-machine LAN run, the 7-day soak, the external security review and the remote CI legs.

### Claude's Discretion
Everything else: the tenancy model, scope semantics, quota mechanics, audit design, fuzz targets, harness design and release tooling.

### Deferred Ideas (OUT OF SCOPE)
- Keyless cosign (Fulcio/Rekor).
- Per-tenant sealing keys.
- Cluster-wide flow-control quotas beyond what Phase 13 provides.
- Multi-region.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| SEC-02 | Scoped tokens (`admin`, `deploy`, `operate`, `trigger`, `read`) and per-tenant tokens; tenants see only their own resources | P14-F1, F5, F6, F11, F12; § Auth layer |
| SEC-03 | Optional signed-module enforcement (minisign/cosign) | P14-F2, F4, F7; § Signed modules |
| SEC-04 | Audit log of deploy, activate, secret access, cancel/terminate, token changes | P14-F8; § Audit journal |
| SEC-05 | Continuous fuzzing (wasm-smith components, malformed WIT values); external review with no open critical/high findings | § Fuzzing, § Security review |
| DISP-06 | Per-tenant concurrency, fuel/s, memory and event-rate quotas with weighted fair dispatch | P14-F3, F9; § Quotas |
| QA-04 | Benchmarks verify acknowledged work and bytes on disk: single node ≥ 10k steps/s, 3-node RF=3 ≥ 5k steps/s, p99 commit < 20 ms | § Benchmarks |
| QA-05 | Jepsen-style 3-node run: 0 lost acknowledged runs; 7-day soak at 70% load | § Jepsen-style harness, § Soak |
| DIST-01 | Static linux x86_64/aarch64 binaries, signed and notarized macOS binaries, `FROM scratch` image, install script | § Distribution |
| DIST-02 | Helm chart (StatefulSet servers, Deployment workers), KEDA examples, NetworkPolicies | § Distribution |
| DIST-03 | Docs: getting started per language, determinism, connectors, operations/sizing, migrations | § Docs set |
</phase_requirements>

## Summary

The code already carries a tenant on runs:
- `StartRun.tenant` (field 16, `MAX_TENANT_LEN` = 128);
- `RunRecord.tenant`;
- `RunStarted.tenant` and WIT `run-info.tenant: option<string>`;
- `ReadyValue.tenant`, added pre-freeze by 05-02 (Phase 7 F3).

Meta records do **not** carry a tenant. `MetaKey` variants are bare strings, `(String, u32)` for workflows or `[u8; 32]` for modules. `ApiToken{id, hash, scopes: Vec<String>, tenant: Option<String>, created_at_ms}` exists and gains `expires_at_ms` in 05-07. Nothing checks it: ostro-api and ostro-server have **no auth layer at all**. The `tenant` start option is a free, client-supplied label.

Two facts of the current format decide the tenancy design:
1. **Idempotency dedupe and keyed routing are scoped by workflow name only.** `dedupe_key(workflow, key)` is in `ostro-partition/src/store/keys.rs`. `partition_for_key(workflow, key, P)` is in `ostro-types/src/routing.rs`, and its golden vectors have been pinned since Phase 2. Today two tenants using the same workflow name and idempotency key collide: with `ReturnExisting`, one tenant gets the other's run.
2. **Under D20, adding a tenant field to the ~10 tenant-scoped meta structs after the freeze would cost ~10 new variants and a META_FORMAT bump.**

So the recommended model is **tenant-qualified names**. Every tenant-scoped name is stored as `"<tenant>/<name>"`. The default tenant is unqualified. Phase 6's sealed-secret convention `"<tenant|_>/<name>"` already follows this pattern. Storing the qualified name in `StartRun.workflow` makes dedupe, keyed routing, cron ownership (`__cron/<schedule>`) and webhook idempotency tenant-scoped **with no format change**. The model needs only a name grammar (no `/`, no `:`, no leading `__`) and a few WIT doc comments, both of which must land before 05-20.

The other high-value pre-freeze change is in 05-07: give every meta record a trailing **`ext` TLV field**. Then Phase 14 and Phases 12–13 can add fields without D20 bumps: tenant quotas, token metadata, module signatures, cluster security policy. Three related items must also land pre-freeze:
- `Tenant` quota fields;
- a **deployed module name** on `WorkflowVersion`. 06-14 already assumes a "Module record name" that no Phase 5 plan creates. A sha-keyed shared `ModuleRecord` can't hold per-tenant names safely.
- tenant inheritance in continue-as-new and retry-restart, which 05-11 and 05-13 don't list.

The audit log and the DISP-06 fair dispatch need **no frozen-format change**.

**Primary recommendation:** before 05-07 runs, adopt P14-F1 (qualified names plus name grammar), F2 (`ext` TLV on every meta record), F3 (`Tenant` quota fields) and F4 (`WorkflowVersion.module`). Fold F10/F11 into 05-05/05-11/05-13, and reserve the F12 names now. Everything else in Phase 14 is additive.

---

## PRIMARY OUTPUT: Pre-Freeze Findings

### Findings at a glance

| ID | Finding | Kind | Phase 5 owner | Can wait (D20)? | Verdict |
|----|---------|------|---------------|-----------------|---------|
| **P14-F1** | Tenant namespacing = qualified names `<tenant>/<name>`, plus name and tenant-id grammar | Doc + API validation (no encoding change) | Decision record (05-DECISIONS D23); validation in 05-13 (add `control/modules.rs` to its files); doc in 05-17 / storage-format | **No.** Waiting strands names containing `/`. The field-based alternative would need a routing/dedupe change, and routing golden is frozen. | **Land now** |
| **P14-F2** | Trailing `ext: Vec<u8>` (TLV codec) on every meta record that lacks it | **Format** (meta) | 05-07 | Yes, but each later field costs a variant and a META_FORMAT bump (Phases 12–14 all) | **Land now** (highest leverage) |
| **P14-F3** | `Tenant` gains quota/weight/suspended fields | **Format** (meta) | 05-07 | Yes: one `TenantPolicy` variant plus a bump (~1 plan), or free via F2's ext | Land now (cheap) |
| **P14-F4** | Deployed **module name** per tenant: `WorkflowVersion.module` (06-14 assumes a field that doesn't exist) | **Format** (meta) + API (`UploadModule.name`) | 05-07 (field); 05-13/05-14 (upload name) | **No.** Phase 6 runs right after the freeze and has declared 0 meta needs. | **Land now** (cross-phase gap) |
| P14-F5 | Idempotency/dedupe/cron/webhook keys are tenant-scoped through F1 | Doc-only | 05-17 docs | Yes (rides on F1) | Doc with F1 |
| P14-F6 | Token scope vocabulary and token format; `ApiToken` stays as is | Doc-only, plus name reservation | none (RESERVATIONS §11) | Yes | Reserve names now |
| P14-F7 | Signed modules: persisted signatures + cluster-wide trust policy | Format via F2 ext (or later variants) | 05-07 (only through F2) | Yes at acceptable cost if F2 lands; otherwise batch into one Phase 14 bump | Covered by F2 |
| P14-F8 | Audit log: node-local hash-chained journal, not in log or meta | None (name reservations) | none | Yes | Reserve names now |
| P14-F9 | DISP-06: `ReadyValue.tenant` (05-02) is enough; no other partition change | None (verify 05-02 lands F3) | 05-02 (already planned) | n/a | Verify only |
| **P14-F10** | WIT doc comments: tenant semantics, unqualified names, in-tenant name resolution | Doc-only (WIT bytes) | 05-05 Task 3 | After the freeze, only in external docs (low cost) | Land now if possible |
| **P14-F11** | Tenant inheritance: CAN successor (05-11) and retry-restart (05-13) must copy `tenant` and qualify the workflow | Behaviour (not format) | 05-11, 05-13 | Technically yes, but runs created in between stay mis-tenanted forever | **Land now** (one line each + test) |
| P14-F12 | Name reservations for Phase 14 (config, REST, gRPC, data dir, failpoints, crates, id prefixes); avoid `ostro.api.v1.Tokens` / `/v1/tokens/{id}` | Doc-only | RESERVATIONS §11 (docs commit) | Yes, but collisions grow as Phases 6–9 add names | Reserve now |

**Cost model behind "can wait".** A post-freeze meta change under D20 takes:
- new `MetaKind`, `MetaKey` and `MetaRecord` variants;
- a new `MetaState` map appended (positional postcard, so an old snapshot lacks the trailing field);
- a `META_FORMAT` bump;
- a permanent decoder for each older snapshot/command format (old struct copies kept forever);
- golden lines;
- under Phase 13 N/N-1 upgrades, a feature gate so old nodes never see the new format.

That is about one plan per bump, plus permanent maintenance. Batching all Phase 14 meta into a single bump is the fallback if F2/F3/F4 are rejected.

### P14-F1: Tenant namespacing by qualified names (HIGH)

**Evidence.**
- `MetaKey::{Workflow(String,u32), Active(String), Queue(String), Schedule(String), Secret(String), Grant(String), Connection(String), Egress(String)}` plus 05-07's `WorkflowPolicy`/`QueuePolicy`/`Webhook` keyed by name. None has a tenant (`crates/ostro-meta/src/command.rs`).
- `dedupe_key(workflow, idem)` (`ostro-partition/src/store/keys.rs:115`); `partition_for_key(workflow, idem, P)` (`ostro-types/src/routing.rs:10`). `routing.txt` is pinned by `golden_frozen`.
- `start_run` passes `req.tenant` through unchecked (`ostro-server/src/control/runs.rs:265`).
- The only workflow-name check is non-empty (`control/modules.rs:182`).
- Phase 6 already stores sealed secrets as `"<tenant|_>/<name>"` (06-DECISIONS Conventions).

**Recommendation (decide before 05-07).**
1. **Tenant id grammar:** `^[a-z0-9][a-z0-9-]{0,62}$`. `_` is the reserved spelling of the **default tenant** wherever a non-empty token is required: sealed names (already in Phase 6), `ApiToken.tenant`, audit records and metric labels. In `StartRun.tenant`, `RunRecord.tenant`, `ReadyValue.tenant` and `run-info.tenant`, the default tenant is `""`/`None`, and `None ⇔ ""`.
2. **Resource name grammar:** `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. It applies to workflows (`describe()` names), queues, schedules, webhooks, deployed module names, non-origin connections and secrets (the user part).
   - It forbids `/` and `:` and a leading `__`. `__` is reserved for `__ostro/` and `__cron/`.
   - `http-target` connections keep their origin names (`https://host[:port]`). These can never be mistaken for a tenant prefix, because `https:` fails the tenant grammar.
3. **Storage form.** The internal key is `"<tenant>/<name>"` for a non-default tenant and `"<name>"` for the default tenant. The parse rule: a key is qualified iff its prefix up to the first `/` matches the tenant grammar. Applied to:
   - meta keys;
   - `StartRun.workflow` and `RunRecord.workflow`;
   - `StartRun.queue`;
   - `Schedule.workflow`, `Webhook.workflow` and `WorkflowPolicy.workflow`;
   - `EgressPolicy.name` (module);
   - Grant ids (`secret:<tenant>/<module>:<secret>`).
4. **Journal form.** `RunStarted.workflow` holds the **module-local (unqualified)** name. `build_run_started` strips the prefix, because the engine passes it to the exported `run(name, …)`. The tenant goes in `RunStarted.tenant`. Old journals with a qualified name stay replayable if the engine strips a valid prefix before `run`, so this rule can also be added later at no cost.
5. **Invariant enforced at the API, never in apply:** `tenant(StartRun.workflow) == StartRun.tenant`. Apply stays permissive. A new apply rejection after the freeze would change replay of the format corpus.
   - For runs created before Phase 14, `RunRecord.tenant` is authoritative for visibility.
   - The meta namespace is authoritative for name resolution.
6. **Why not a tenant field?** Field-scoped keys would need `dedupe_key(tenant, …)` (a state change) and a tenant-aware routing function. Routing golden is frozen, so it could only be added as a second function used when the tenant is non-empty. Meta keys would still need qualified strings, because MetaKey shapes are frozen too. Qualified names give one rule everywhere, with zero encoding change.

**Owners.**
- The 05-DECISIONS entry (D23) goes in a docs-only commit.
- 05-13 validates tenant id and names in the start/signal/policy paths. Add `crates/ostro-server/src/control/modules.rs` to 05-13's `files_modified` for `describe()` name validation, or create a small 05-xx plan, because no Phase 5 plan owns `modules.rs`.
- 05-05 adds the WIT doc comments (F10).
- 05-17 or `docs/storage-format.md` documents the convention.

**Pre-Phase-14 behaviour.** Until auth exists, the API qualifies nothing: the tenant stays a label, which is today's behaviour. Phase 14 switches start/list/get to token-derived tenants. Only the grammar rules must hold from the freeze on.

### P14-F2: `ext` TLV on every meta record (HIGH leverage, format, 05-07)

**Evidence.** 05-07 adds `ext: Vec<u8>` only to `Schedule`, `Webhook`, `WorkflowPolicy` and `QueuePolicy`. It describes ext as "postcard-encoded future options" without defining a codec. These records have no `ext`: `ClusterInfo`, `Member`, `PartitionAssignment`, `ModuleRecord`, `WorkflowVersion`, `ActiveVersion`, `QueueConfig`, `ApiToken` (after `expires_at_ms`), `Tenant`, `SealedSecret`, `Grant`, `Connection` and `EgressPolicy`. No Phase 6–9 plan reads or writes any `ext`, so the codec can still be defined.

**Recommendation.**
- 05-07 appends `ext: Vec<u8>` as the **last** field of every record above.
- It defines one codec for all `ext` fields: a sequence of `(tag: varint u32, len: varint u32, bytes)` entries.
  - Tags are strictly increasing.
  - Unknown tags are preserved byte-for-byte on read-modify-write and ignored by readers.
  - An empty `Vec` means no extensions.
  - Each entry's value is postcard of a small struct owned by the tag.
- It adds a "§10b meta ext tags" table to RESERVATIONS, with per-phase tag blocks.
- It amends D20 to read: "or a registered ext tag on an existing record".
- The meta golden covers one record of each kind with a non-empty ext.

**What it buys Phase 14** (all without a bump):
- `ApiToken`: name/description, created_by, rotation link.
- `ModuleRecord`: signature evidence (F7) and the set of uploader tenants.
- `ClusterInfo`: cluster-wide security policy (`require_signed_modules`, trusted key ids, auth mode), so every node enforces the same rule. Node-local config would let one misconfigured node accept unsigned uploads.
- `Tenant`: future quota knobs.
- `SealedSecret`: created/rotated timestamps.

Phases 12/13 (`Member`, `PartitionAssignment`) benefit equally.

**If rejected:** Phase 14 needs about 2–3 new variants (`TenantPolicy`, `ModuleSignature`, `SecurityPolicy`) in one batched bump. That is acceptable but avoidable.

### P14-F3: `Tenant` quota fields (format, 05-07)

Today `Tenant{id, created_at_ms}`. Recommended append order:

```rust
pub struct Tenant {
    pub id: String, pub created_at_ms: u64,
    // P14-F3 (stored only; enforced in Phase 14):
    pub weight: Option<u32>,              // fair-dispatch weight, default 1
    pub max_concurrency: Option<u32>,     // concurrent activations (node-wide in P14, cluster-wide when P13 allows)
    pub fuel_per_s: Option<u64>,          // token bucket refill, fuel units/s
    pub fuel_burst: Option<u64>,          // bucket size; default = fuel_per_s
    pub memory_bytes: Option<u64>,        // sum of live instance memory limits
    pub event_rate: Option<RateSpec>,     // ingress events (starts, signals, token completions, webhook deliveries)
    pub suspended: bool,                  // reject ingress + stop dispatch for this tenant
    pub ext: Vec<u8>,                     // P14-F2
}
```

`RateSpec` already exists (05-07, Phase 7 F2). Validation in the meta apply: every `Some` value is greater than 0, and `period_ms` is greater than 0. Waiting is acceptable at the cost of one variant plus a bump, and costs nothing if F2 lands. Recommend landing it now because 05-07 is open.

### P14-F4: Deployed module name: a cross-phase gap (HIGH, format + API)

**Evidence.**
- 06-14 Task 2: "`start` resolves the deployed module name for the run (`RunRecord.module_sha` → meta Module record name, cached per sha)".
- 06-RESEARCH: "'module' is the deployed module name, not the sha, so redeploys keep grants".
- Grants (`secret:<module>:<secret>`) and `EgressPolicy{name: <module>}`, the guest authority boundary, are keyed by it.
- But `ModuleRecord` has **no name** (`records.rs`), `UploadModule` has no name (`ostro-api/src/domain.rs:34`), and no Phase 5 plan adds one.

**Security issue if solved naively.** `ModuleRecord` is keyed by sha and shared: a re-upload of the same bytes is an idempotent no-op (`control/modules.rs` §Upload 2). Suppose a `name` field were put on `ModuleRecord`. Tenant B uploading A's exact bytes under another name would then overwrite or contend A's name, and A's runs would resolve grants and egress under B's name. That is cross-tenant privilege confusion.

**Recommendation.**
- 05-07 appends `module: String` to `WorkflowVersion`: the deployed module name, tenant-qualified per F1. It sits before the F2 `ext`. The upload request gains `name` (REST query/header plus proto field). `ostro deploy` defaults it to the file stem (05-14).
- Phase 6 resolves the module name as `(RunRecord.workflow, workflow_version) → WorkflowVersion.module`, not through `ModuleRecord`. RunRecord has both fields.
- Module visibility for tenants needs no field: `ModuleRecord.workflows` already lists qualified `(name, version)` pairs, and a tenant sees a module iff one of those pairs carries its prefix.
- Re-upload by another tenant must still register that tenant's `WorkflowVersion` records. Today's "existing sha = no-op" short-circuit must not skip registration once tenants exist (Phase 14 API change, not format).
- Hand this to the orchestrator as a **Phase 5/6 seam fix**. It blocks 06-14 as written, independently of Phase 14.

### P14-F5: Idempotency and dedupe scope (doc-only, rides on F1)

With qualified `StartRun.workflow`, these keys are tenant-scoped with no code change:
- `dedupe_key(workflow, key)`;
- `partition_for_key(workflow, key)`;
- Phase 8's `schedule_owner("__cron/" ‖ name)` and `__cron/<schedule>/<fire_time_ms>` keys. Parse these **from the right**, because qualified schedule names contain `/`.
- webhook `start` idempotency keys.

Signal `request_id` (D13) and await tokens are run-scoped. Await tokens are HMAC bearer capabilities under the global `__ostro/token-key`. Phase 14 additionally requires the `trigger` scope and `RunRecord.tenant == token tenant` on `/v1/tokens/complete`. Document this in the tenancy doc.

### P14-F6: Token scopes and token format (doc-only)

`ApiToken{id, hash: [u8;32], scopes: Vec<String>, tenant: Option<String>, created_at_ms, expires_at_ms}` is sufficient. Strings are extensible. Rules to document (Phase 14 implements them):

**Scopes**
- `admin` covers everything. Only `admin` can manage tenants, tokens, sealed secrets and cluster policy.
- `deploy`: upload, activate, workflow policies.
- `operate`: cancel, terminate, retry, pause/resume, signal, queue policies, schedule pause/trigger, DLQ.
- `trigger`: start runs, signal, complete tokens.
- `read`: get/list/history/logs.

Implications are **not** implicit: a token lists exactly what it grants, except that `admin` covers all. Phase 7's `worker:join` is a **non-API** scope: such tokens must be rejected by the API auth layer, and vice versa.

**Tenant and secret**
- `tenant: None` means a cross-tenant system token. It is allowed only with `admin` or `read`, for operators.
- `Some(t)` confines every request to `t` (default tenant = `Some("_")`).
- `hash = sha256(secret)` is fine for 256-bit random secrets. Compare in constant time. No `last_used` writes to meta, ever.
- Bearer format: `ostro_<token id>_<base32 secret>`. API token ids get the prefix `otk_`; Phase 7 uses `wkr_`.

**Name collision.** Phase 5 reserves gRPC `ostro.api.v1.Tokens` and REST `/v1/tokens/complete` for **await tokens**. API-token CRUD must therefore use `ostro.api.v1.ApiTokens` and `/v1/auth/tokens`. `/v1/tokens/{id}` would shadow `complete`. See F12.

### P14-F7: Signed modules (format only through F2)

Enforcement happens at **upload**. The request carries a detached signature (minisign `.minisig`, or a cosign keyed signature). It is verified against the trusted keys before any meta write. Unsigned or invalid uploads are rejected with 400/403 and audited.

It is re-checked at **activation**, so a module uploaded before enforcement was switched on can't be activated. That needs persisted evidence: `ModuleRecord.ext` tag `signatures: Vec<{alg: u8, key_id: [u8;8 or 32], sig: Vec<u8>}>` (F2). Trust policy lives in `ClusterInfo.ext`, with an optional per-tenant `Tenant.ext` override.

No log, journal or WIT change is needed. Without F2, Phase 14 adds `ModuleSignature` and `SecurityPolicy` variants in one bump: acceptable.

### P14-F8: Audit log (no frozen-format need)

**Design (recommended).** A node-local, append-only audit journal in `<data>/audit/`. It is separate from the partition log and the meta Raft log.
- **Segments:** `audit-<seq 20d>.jsonl` (or a postcard-framed equivalent).
- **Record fields:** `ts`, `node`, `actor` (token id, `system`, or worker id), `tenant`, `action`, `target`, `outcome`, `request_id`, `prev_hash`, `hash`.
- **Hash chain:** each record's sha256 is chained to the previous one, giving tamper evidence. fsync follows the same group-commit pattern.
- **Export:** OTLP LogRecords (`event.name = "ostro.audit"`) through the Phase 8 pipeline, and `GET /v1/audit`. A cluster-wide query fans out across members.
- **Capture points:**
  - API layer: deploy, activate, token/tenant/secret/grant/connection/egress CRUD, cancel/terminate/retry/pause, policy changes, auth failures;
  - bridge: secret resolution, by wrapping 06-05's `SecretResolver::resolve`, coalesced per `(run, module, secret, origin)` per minute to bound volume;
  - Phase 7 join/renew/revoke.

**Why not the log or meta.**
- Meta-changing ops are already Raft entries, but `MetaCommand::Put` has no actor field. Adding one after the freeze needs a new command variant.
- Secret access is node-local in the bridge anyway.
- Putting audit rows in partition state would make them part of the digest and snapshots.

Optional and cheap after the freeze: append an `actor` prost field to `RequestCancel`, `TerminateRun` and `PauseRun` (additive prost, any field number per RESERVATIONS "other message fields"), so the durable log names who cancelled.

**Pre-freeze action:** only reservations (F12). Also check that `Node::open`, Phase 8 backup/restore and blob GC ignore unknown data-dir entries such as `audit/`. storage-format §1 lists the layout but states no rule for unknown entries.

### P14-F9: DISP-06 needs nothing more from the partition format

- `ReadyValue{concurrency_key, workflow, tenant, first}` lands in 05-02 (Phase 7 F3/F7), so the dispatcher rebuild sees the tenant of every backlog entry with no run-record read. `workflow` is qualified, so the tenant is derivable even if `tenant` is `None` on pre-Phase-14 runs.
- The `running` keyspace value has no tenant. On leader failover, per-tenant in-flight counts are rebuilt by one `RunRecord` point read per running row. That is bounded by concurrency and acceptable.
- Weights and quotas come from the meta `Tenant` record (F3) through the meta watch.
- Phase 7's `leader/fair.rs` (weighted DRR queue → band → key) gets a tenant level on top (07-DECISIONS "Phase 14").

**Action:** verify only, in 14-01, that 05-02 shipped F3 and F7.

### P14-F10: WIT doc comments (doc-only, 05-05 Task 3; D14-compatible)

`run-info.tenant: option<string>` stays exposed. It is the tenant's own code, and P4 SaaS builders need it. Doc text to add before the freeze:
1. `run-info.tenant`: `none` = default tenant. It is immutable for a run and inherited by children, continue-as-new successors and retries.
2. `run-info.workflow` and `workflow-meta.name`: module-local, unqualified names matching `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. `/` and `:` are reserved, and so is a leading `__`.
3. These names resolve **only inside the run's tenant**; no cross-tenant reference can be expressed: the `start-child` workflow, `continuation` workflow, `sql-request` / `object-request` connection names and `{{secret:name}}` placeholders.
4. Quotas never fail guest calls. They delay dispatch, so no new `error-info.kind` is needed. This keeps the WIT unchanged.

After the freeze, the same text can only live in `docs/` (low cost), because `wit/FROZEN.toml` pins the bytes.

### P14-F11: Tenant inheritance gaps (behaviour, 05-11 and 05-13)

- **05-11, continue-as-new.** The successor `StartRun` lists the workflow/version, module_sha, input, `continued_from`, parent (PF-F2), retention/s2s and run_timeout, but **not `tenant`** (nor tags or traceparent).
- **05-13, retry-restart.** The new `StartRun` lists the workflow, version, module_sha, input, queue and `retry_of`, but **not `tenant`**.

A successor or retry without a tenant escapes into the default tenant, and the journaled `StartRun` keeps that permanently. Phase 9 child starts already say "tenant inherited" (D-P9-8).

**Fix (one line each plus a test):** copy `tenant` (and tags/traceparent) from the predecessor's `RunRecord`. Resolve a continuation's workflow name inside the predecessor's tenant (F1). Phase 8 cron and webhook drivers set `StartRun.tenant` from the qualified schedule/webhook name. Phase 14 adds that line, since Phase 8 runs before tenants exist.

### P14-F12: Name reservations (doc-only; add to RESERVATIONS §11 now)

| Kind | Name |
|------|------|
| Config sections | `[auth]`, `[tenants]`, `[audit]`, `[security]` (incl. `[security.modules]`) |
| Data dir | `<data>/audit/` |
| REST | `/v1/tenants`, `/v1/auth/tokens`, `/v1/audit`, `POST /hooks/{tenant}/{name}` (tenant webhooks; Phase 8 owns `/hooks/{name}`) |
| gRPC | `ostro.api.v1.Tenants`, `ostro.api.v1.ApiTokens`, `ostro.api.v1.Audit` |
| Failpoints | `auth::`, `audit::` |
| Crates / dirs | `fuzz/` (separate cargo workspace, `ostro-fuzz`), `ostro-jepsen`, `deploy/helm/ostro`, `deploy/image` |
| Id prefixes | `otk_` API tokens (`wkr_` = Phase 7) |
| Tenant ids | `_` (default tenant spelling) reserved, never creatable |
| Meta fallback | variants 24–25 reserved P14 (used only if F2/F3/F4 are rejected) |
| Metric prefixes | `ostro_auth_`, `ostro_audit_`, `ostro_tenant_` |

---

## SECONDARY OUTPUT: Outline of the Rest of Phase 14

### Standard Stack

| Library / tool | Version (checked 2026-11-19) | Purpose | Note |
|---|---|---|---|
| `minisign-verify` | 0.3.0 (zero deps) | minisign signature verification (SEC-03) | HIGH: crates.io |
| `aws-lc-rs` (already in tree via rustls) | workspace pin | cosign **keyed** signatures (ECDSA P-256/SHA-256 over the blob), token hashing | MEDIUM: check the cosign v3 bundle vs raw-signature output when planning |
| `sigstore` crate | 0.15.0 | **Do not use.** Depends on `chrono` and `openssl` (banned) | HIGH: crates.io deps |
| `cargo-fuzz` / `libfuzzer-sys` | 0.13.2 / 0.4.13 | coverage-guided fuzzing; needs a **dated nightly** (local nightly-2024-04-20 is too old for Wasmtime 50) | HIGH |
| `wasm-smith` | pin **=0.261.0** to match `wit-component =0.261.0` (latest 0.263.0) | random core modules (+ unfinished `component-model` feature) | MEDIUM: component generation is marked work-in-progress, so wrap core modules with `wit-component` instead |
| `arbitrary` | 1.5.0 | structured fuzz inputs | HIGH |
| `rcodesign` (apple-codesign) | current | sign and notarize Mach-O from Linux with an App Store Connect API key | MEDIUM; human_needed (Developer ID) |
| `helm` | local binary available | chart lint/template | `kubeconform` must be installed for offline schema checks |
| KEDA | 2.21.0 (2026-09-23) | ScaledObject examples (`prometheus` / `metrics-api` scalers on `ostro_queue_worker_demand`) | MEDIUM |
| `crane` (go-containerregistry) or `umoci` | current | build a `FROM scratch` OCI image **without Docker** (append a layer to an empty base) | MEDIUM; push is human_needed |

### Architecture patterns

1. **Auth layer.** One axum middleware plus one tonic interceptor on the shared port.
   - It extracts the bearer, splits `ostro_<id>_<secret>`, looks up `ApiToken` in the meta view (memory), checks expiry and does a constant-time hash compare.
   - It produces a `Principal{token_id, tenant, scopes}` passed to every `ControlPlane` method.
   - Each method declares its required scope, and a table-driven test covers every route × scope.
   - Dev profile: auth off by default, with a loud log line. Server profile: auth required unless `[auth] mode = "none"`.
   - Bootstrap: `ostro token create --admin --data-dir` works offline, or the first admin token is printed once at `cluster init`.
   - Phase 7's demand endpoint gets bearer enforcement (07-DECISIONS).
2. **Tenant scoping in the ControlPlane.** Qualify incoming names with `principal.tenant` and strip them on output.
   - Get/act by run id: compare `RunRecord.tenant` and return **404, not 403**, so run ids aren't revealed.
   - List: filter by tenant. A post-freeze tenant index keyspace (ordinal ≥ 20, placeholder rule) is optional if scan cost matters.
   - Meta lists: BTreeMap prefix range `"<tenant>/"`.
3. **Quotas (DISP-06).**
   - Hierarchical DRR: tenant (quantum = `Tenant.weight`) → queue → band → key, in Phase 7's `fair.rs`.
   - Node-level per-tenant counters for `max_concurrency` and `memory_bytes`. `memory_bytes` is reserved at grant time from the instance memory limit.
   - A fuel token bucket charged **after** each activation from the consumed fuel (debt model: a tenant in debt isn't dispatched).
   - `event_rate` is checked at API/webhook/cron ingress (429 with `Retry-After`).
   - `suspended` blocks ingress and dispatch.
   - Quotas delay work and never fail it (F10). Measure fairness with Jain's index, as 07-17 does.
4. **Signed modules.** Verify before compile. Keys come from `ClusterInfo.ext` (F2/F7). The audit record holds the signer key id.
5. **Audit journal.** As in F8, with a writer task fed by a bounded channel. If the channel is full, the request fails with 503 rather than skipping the audit (for admin actions). Secret-access events are coalesced.

### Don't hand-roll

| Problem | Don't build | Use instead |
|---|---|---|
| minisign parsing/verification | custom Ed25519 + format parser | `minisign-verify` |
| ECDSA verification | own crypto | `aws-lc-rs` `signature::UnparsedPublicKey` |
| Constant-time compare | `==` on hashes | `aws_lc_rs::constant_time` / `subtle` |
| Mach-O signing/notarization | scripted `codesign` on CI only | `rcodesign` (Linux) or `notarytool` (macOS runner) |
| Container image without Docker | tar layering by hand | `crane append` / `umoci` |
| Fuzz harness engine | custom mutator | `cargo-fuzz` + `arbitrary`; wasm-smith for modules |
| Network partitions in tests | iptables (root) | per-link userspace TCP proxies controlled by the nemesis (see Jepsen section) |

### Fuzzing (SEC-05)

- **Layout.** A separate `fuzz/` cargo workspace (excluded from the main workspace, with its own lockfile and a dated nightly). Main-workspace deny and `--locked` are unaffected.
- **Corpus replay on stable.** A stable `#[test]` replays every committed corpus/crash file on each `ci-local` run, so regressions are caught without nightly.
- **Targets (priority order):**
  1. `apply_command`: arbitrary bytes → prost `Command` decode → apply on a scratch store. Apply never panics; it rejects with `Invalid`.
  2. `journal_event_codec` and `meta_decode` (postcard, all META_FORMAT decoders).
  3. `snapshot_frames` (OSNP v1 reader, Phase 8) and `wal_recovery_scan` (corrupt segment bytes).
  4. **`evil_guest`:** a fixed core module whose behaviour is driven by fuzz input. It calls every `ostro:runtime` import with arbitrary canonical-ABI arguments (bad UTF-8, out-of-bounds pointers, invalid discriminants, huge lists, deep payloads, over-limit names). Goal: the host never panics, one attempt fails, and node budgets hold. This is the "malformed WIT values" requirement.
  5. **`smith_component`:** wasm-smith core modules configured with the ostro world's lowered imports and exports, wrapped by `wit_component::ComponentEncoder`, then validate → AOT → instantiate → `describe`/`run` under fuel and epoch limits. This is the "wasm-smith components" requirement.
  6. Parsers: REST JSON bodies, webhook verifiers, cron expressions, egress origin/URL patterns, secret templates, token bearer parsing, minisign/cosign inputs.
- **Continuous fuzzing.**
  - A GitHub Actions scheduled job: nightly, about 30 min per target, with the corpus cached. Remote: human_needed until a push.
  - Optional ClusterFuzzLite/OSS-Fuzz.
  - Locally: `scripts/fuzz-smoke.sh` runs 60 s per target.
- **Security review.** Book it **before** Phase 12 ends. docs/05 says to start in week 4, and late findings are rated high-risk. Prepare a threat-model pack: `docs/03 §9` plus the tenancy doc, the auth matrix and the fuzz coverage report. Gate: no open critical/high findings (human_needed).

### Jepsen-style harness without Docker (QA-05)

Depends on Phases 12–13 (RF=3 cluster).
- **Topology.** Three `ostro` server processes on one host (or three hosts when available).
  - All cluster and worker links go through **per-link userspace TCP proxies** started by the harness, a toxiproxy-style crate in `ostro-jepsen`.
  - The nemesis can blackhole, delay, drop or one-way-partition any link. This needs no root, no netns and no Docker.
- **Faults.**
  - `kill -9` and restart (05-19 machinery);
  - **clock skew** through a failpoint-build-only clock offset knob read by the node's time source. libfaketime does not affect static musl binaries;
  - **disk stalls** through the existing `wal::`/`state::` failpoints with `return(wait:ms)` / fsync delay;
  - leader isolation and minority/majority partitions.
- **Workload and checker.**
  - Reuse 05-19's crash-saga workload, ack log and checker: every acked run exists, no duplicate journal entries, invariants.
  - Add a **fencing checker**: per partition, no two leaders' commands are applied under the same epoch, and stale `(epoch, attempt)` commands are rejected. Use history analysis over the exported journals.
  - Runs are seeded and reproducible. Reports go to `target/jepsen/<seed>`.
- **Gates.** A 10-minute local smoke runs in ci-local. A multi-hour run on 3 machines is human_needed.

### Soak and benchmarks (QA-04, QA-05)

- **Benchmarks.** Extend 05-16's `perf.rs` (D16 step definition: acknowledged **and** found on disk after a restart; profile A is the gate).
  - Single node: ≥ 10k steps/s.
  - 3-node RF=3: ≥ 5k steps/s with p99 commit < 20 ms.
  - Report bytes on disk per step and verify a post-restart count, following the Chronik lesson that benchmarks must verify acknowledged bytes.
  - Native NVMe 8 vCPU box and a real LAN: human_needed (D15). WSL numbers are advisory.
- **Soak.** A long-running workload driver at 70% of the measured rated load for 7 days.
  - It samples RSS, fds, disk use, WAL/snapshot floors, compaction stalls, latency percentiles, restarts and audit-chain verification hourly.
  - The pass criteria are written before the run: flat RSS after warm-up, no unexplained restarts, latency SLOs held.
  - A 1-hour local "soak smoke"; the 7-day run is human_needed.

### Distribution (DIST-01, DIST-02)

- **Binaries.**
  - Static linux x86_64/aarch64 musl builds through the existing `cargo-zigbuild` path.
  - Release script: build, `file`/`readelf` static check, sha256sums, a minisign signature over `SHA256SUMS`, optional cosign keyed signature and GitHub build-provenance attestation (remote).
- **macOS** (human_needed: Apple Developer ID).
  - Sign with hardened runtime (`codesign -o runtime --timestamp`, or `rcodesign sign`) and notarize via `notarytool`/`rcodesign notary-submit`.
  - A bare Mach-O/zip can't be stapled, so also ship a signed, notarized and **stapled `.pkg`**.
  - The install script uses curl, which doesn't set the quarantine attribute, so it can take the bare binary.
- **Image.** `FROM scratch`: the static binary, CA roots (webpki bundle, per the P8 note), `USER 65532:65532`, `/data` volume, multi-arch index. Built with crane/umoci without Docker; registry push is human_needed.
- **Install script.** `install.sh`: detect OS/arch, download, verify the sha256 and minisign signature, install to `~/.local/bin` or `/usr/local/bin`. Shellcheck it and test it against a local file server.
- **Helm chart** (`deploy/helm/ostro`):
  - StatefulSet servers (3 replicas, headless Service, PVC per pod, PodDisruptionBudget, anti-affinity) and a worker Deployment.
  - Secrets for the sealing key and admin token.
  - KEDA `ScaledObject` examples: `prometheus` scaler on `ostro_queue_worker_demand{queue}` and `metrics-api` on `GET /v1/queues/{q}/demand` with a bearer token, including scale-to-zero.
  - NetworkPolicies:
    - servers accept the client port from allowed namespaces;
    - the cluster port only from server pods;
    - the worker port only from worker pods;
    - workers egress only to servers and DNS (the bridge runs on servers);
    - server egress is configurable for connectors.
  - Local checks: `helm lint`, `helm template | kubeconform -strict`. A live install on kind/k3d is human_needed (no Docker; microk8s off-limits).

### Docs set (DIST-03)

Getting started for Rust, Python and TypeScript (deploy → trigger → observe), plus:
- a determinism guide (already seeded by 05-06/09);
- connectors and secrets/egress (Phase 6);
- **tenancy and auth** (F1/F5/F6 rules, scope matrix, token lifecycle);
- operations and sizing (P, memory per language, Python instance budget, snapshots/bottomless, backup/restore, upgrades);
- the security model (threat model, signed modules, audit);
- Helm/KEDA;
- migrations from cron/queue tables, Temporal (workflow/activity mapping, signals, child workflows, continue-as-new) and Restate (handler model vs hosted Wasm).

Every code snippet in the docs is compiled or run by a doc test or example script, and a link checker runs in ci-local.

## Common Pitfalls

1. **Shipping a tenant field instead of qualified names.** The field must then reach dedupe and routing, both frozen. Avoid it with F1.
2. **Mis-tenanted successors.** CAN, retry, cron or webhook runs created without a tenant land in the default tenant forever (F11). Write a test for every StartRun construction site.
3. **403 instead of 404 on foreign run ids.** It leaks existence. Always 404.
4. **`worker:join` tokens accepted by the API**, or API tokens accepted by join. Reject them in both directions.
5. **Per-node security config in a cluster.** One node without enforcement accepts unsigned modules. Keep the policy in meta (F2/F7).
6. **Audit dropped under load.** For admin actions, fail closed (503) rather than skip the audit. Coalesce only secret-access events.
7. **The `sigstore` crate** pulls chrono and openssl and fails cargo-deny. Use keyed cosign through aws-lc-rs.
8. **Stale nightly for cargo-fuzz.** The installed nightly-2024-04-20 is older than Wasmtime 50's MSRV. Pin a dated recent nightly in `fuzz/rust-toolchain.toml`.
9. **libfaketime for clock skew** doesn't work on static musl binaries. Use an in-process clock offset in failpoint builds.
10. **Bare-binary notarization assumptions.** Tickets can't be stapled to a bare binary or zip. Ship a stapled `.pkg`.

## Code Examples

```rust
// P14-F1: qualify/parse (ostro-types or ostro-meta helper; pure, golden-tested)
pub fn is_tenant_id(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty() && b.len() <= 63 && s != "_"
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}
pub fn qualify(tenant: Option<&str>, name: &str) -> String {
    match tenant { None | Some("") | Some("_") => name.to_owned(), Some(t) => format!("{t}/{name}") }
}
pub fn split_qualified(key: &str) -> (Option<&str>, &str) {
    match key.split_once('/') { Some((t, rest)) if is_tenant_id(t) => (Some(t), rest), _ => (None, key) }
}
```

```rust
// P14-F2: ext TLV (one codec for every meta record's trailing `ext`)
// entry = varint(tag) ‖ varint(len) ‖ bytes; tags strictly increasing; unknown tags preserved.
pub fn ext_get(ext: &[u8], tag: u32) -> Option<&[u8]> { /* scan */ }
pub fn ext_put(ext: &[u8], tag: u32, val: &[u8]) -> Vec<u8> { /* merge, keep order + unknowns */ }
```

## State of the Art

| Old approach | Current approach | Impact |
|---|---|---|
| cosign raw signature files | cosign v3 defaults to Sigstore bundles | Accept keyed bundles by extracting the signature when planning; keyless is out of scope |
| `altool` notarization | `notarytool` / `rcodesign notary-submit` | `altool` is retired; use an API key |
| wasm-smith components | still marked WIP | Wrap core modules with wit-component |

## Open Questions

1. **Cluster-wide per-tenant concurrency and event rate.**
   - What we know: Phase 14 can enforce these node-wide.
   - What's unclear: cluster-wide enforcement depends on Phase 13 mechanisms.
   - Recommendation: in 14-RESEARCH v2, after Phase 13 is planned, decide between a meta-assigned "quota owner" partition (like FR-4.5 rate limits) and approximate per-node shares.
2. **Secret-access audit volume.**
   - What's unclear: whether the coalescing window (1 min per run/module/secret/origin) satisfies reviewers.
   - Recommendation: confirm it in the security-review prep.
3. **cosign v3 bundle format.**
   - What's unclear: the exact keyed-signature extraction.
   - Recommendation: spike it at plan time, with minisign as the primary format.
4. **Default-tenant visibility of pre-Phase-14 runs carrying a label tenant.**
   - What we know: F1 makes `RunRecord.tenant` authoritative.
   - Recommendation: confirm it with the user at discuss-phase.
5. **Signature carriage at upload** (header vs multipart vs separate `PUT /v1/modules/{sha}/signatures`).
   - This is an API decision only.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test / cargo-nextest (installed), proptest, ostro-crashtest, `#[ignore]` perf suites |
| Config file | workspace `Cargo.toml`, `scripts/ci-local.sh`, `.github/workflows/ci.yml` |
| Quick run command | `cargo nextest run -p ostro-server -p ostro-api -p ostro-meta --locked` |
| Full suite command | `bash scripts/ci-local.sh` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| (pre-freeze) | ext TLV and Tenant/WorkflowVersion fields pinned | golden | `cargo test -p ostro-meta --test meta_golden` | Part of 05-07 (add lines) |
| (pre-freeze) | name/tenant grammar; CAN/retry inherit tenant | unit + e2e | `cargo test -p ostro-server --test lifecycle_api tenant_inherit` | ❌ (05-11/05-13 additions) |
| SEC-02 | route × scope matrix; tenant isolation (404 on foreign ids, filtered lists, qualified meta) | integration | `cargo test -p ostro-server --test auth_scopes --test tenant_isolation` | ❌ Wave 0 |
| SEC-03 | unsigned/invalid rejected when enforced; activation re-check | integration | `cargo test -p ostro-server --test signed_modules` | ❌ Wave 0 |
| SEC-04 | each audited action produces one chained record; chain verify detects tampering | integration | `cargo test -p ostro-server --test audit_log` | ❌ Wave 0 |
| SEC-05 | corpus replay on stable; 60 s per-target smoke on nightly | fuzz regression | `cargo test -p ostro-fuzz-corpus`; `scripts/fuzz-smoke.sh` | ❌ Wave 0 |
| DISP-06 | weighted fairness (Jain ≥ 0.9), concurrency/fuel/memory/event-rate caps | integration | `cargo test -p ostro-server --test tenant_quotas` | ❌ Wave 0 |
| QA-04 | steps/s acknowledged and on disk | perf (`#[ignore]`) | `cargo test -p ostro-server --test perf -- --ignored` | Extends 05-16; native box human_needed |
| QA-05 | 3-process proxy-partition smoke, 0 lost acks, fencing holds | fault | `cargo test -p ostro-jepsen --test smoke -- --ignored` | ❌; full run human_needed |
| DIST-01 | static binary checks, signature verify, install.sh against a local server | script | `bash scripts/release-dry-run.sh` | ❌; notarization/push human_needed |
| DIST-02 | chart lint + schema | script | `helm lint deploy/helm/ostro && helm template … \| kubeconform -strict` | ❌; live install human_needed |
| DIST-03 | doc snippets build; links resolve | script | `bash scripts/docs-check.sh` | ❌ |

### Sampling Rate
- **Per task commit:** the quick run command for the touched crates.
- **Per wave merge:** `bash scripts/ci-local.sh --no-musl`.
- **Phase gate:** full `ci-local` green, plus fuzz smoke, Jepsen smoke and the human_needed records.

### Wave 0 Gaps
- [ ] `crates/ostro-server/tests/{auth_scopes,tenant_isolation,signed_modules,audit_log,tenant_quotas}.rs`
- [ ] `fuzz/` workspace + `fuzz/rust-toolchain.toml` (dated nightly) + stable corpus-replay test
- [ ] `crates/ostro-jepsen` (proxy nemesis, clock-offset failpoint, fencing checker)
- [ ] `scripts/{fuzz-smoke,release-dry-run,docs-check}.sh`; install `kubeconform`

## Sources

### Primary (HIGH confidence)
- Code at `9a79ca8`:
  - `crates/ostro-meta/src/{records,command,state}.rs`;
  - `crates/ostro-partition/src/store/{keys,values}.rs`, `apply/run.rs`;
  - `crates/ostro-types/src/{routing,command}.rs`;
  - `crates/ostro-server/src/{control/runs.rs,control/modules.rs,run_started.rs}`;
  - `crates/ostro-api/src/domain.rs`;
  - `wit/ostro-runtime-0.1.0/runtime.wit`.
- Plans and docs: `.planning/{ROADMAP,REQUIREMENTS,PROJECT,STATE,RESERVATIONS}.md`; 05-DECISIONS (D14, D18, D20, F1–F10, PF-*); 05-07, 05-11, 05-13, 05-20 plans; 06-DECISIONS (conventions); 06-14; 07-DECISIONS (D-W1, D-W4, seams); 08-DECISIONS (P8-D10/D12/D15); 09-DECISIONS (D-P9-8); `docs/03-architecture.md` §9; `docs/storage-format.md` §1.
- crates.io API (2026-11-19): minisign-verify 0.3.0 (no deps), sigstore 0.15.0 (deps include chrono, openssl), wasm-smith 0.261–0.263, cargo-fuzz 0.13.2, libfuzzer-sys 0.4.13, arbitrary 1.5.0.

### Secondary (MEDIUM confidence)
- [wasm-smith component.rs (docs.rs)](https://docs.rs/wasm-smith/latest/src/wasm_smith/component.rs.html): component support present but marked WIP.
- [rcodesign notarizing docs](https://gregoryszorc.com/docs/apple-codesign/main/apple_codesign_rcodesign_notarizing.html) and [Signing and notarising a single CLI binary for Mac](https://octet-stream.net/b/scb/guide-to-signing-notarising-single-cli-binary-mac.html): no stapling to zip/bare binary; pkg recommended.
- [KEDA 2.21.0 release](https://newreleases.io/project/github/kedacore/keda/release/v2.21.0) and [endoflife.ai KEDA 2.21](https://endoflife.ai/keda/2.21).

### Tertiary (LOW confidence)
- cosign v3 bundle default and keyed extraction: from training, verify at plan time.
- Whether cargo-fuzz `--sanitizer none` works on stable: unverified, so plan on a dated nightly.

## Metadata

**Confidence breakdown:**
- Pre-freeze findings: HIGH. Each is backed by file/line evidence and the plan texts.
- Standard stack: MEDIUM-HIGH. Versions come from crates.io; KEDA and rcodesign come from release pages.
- Architecture outline: MEDIUM. It is not planned in detail and depends on Phases 12–13.
- Pitfalls: MEDIUM-HIGH.

**Research date:** 2026-11-19
**Valid until:** the pre-freeze findings expire when 05-07 / 05-20 execute. The outline is valid about 30 days, so re-research Phase 14 after Phase 13 is planned.
