# Pitfalls Research

**Domain:** Polyglot Wasm durable-execution engine in Rust (single binary: log-first storage, deterministic replay, Wasmtime component host, follower-pull ISR replication)
**Researched:** 2026-11-18
**Confidence:** MEDIUM overall. Chronik first-hand lessons, Wasmtime docs, Temporal/Restate docs, and fsyncgate are HIGH. componentize-py/js production figures and some Wasmtime defaults are LOW to MEDIUM because no current authoritative numbers turned up; Spike S2 has to measure them.

> Scope note: `docs/04-chronik-reuse.md` §4 already covers these Chronik rules: one replication path including RF=1, ISR from measured LEOs, benchmark the user path, elect only from ISR, durable-gated tail cache, separate wait and read timeouts, a dedicated replication address, benchmarks that verify bytes on disk, and conformance first. This file **does not repeat them**. It adds the Chronik lessons that §4 leaves out (from `ROADMAP_REPLICATION.md` RP-3/5/7/9) and everything outside replication.

Phase key (from `docs/05-roadmap.md`): **P0** spikes, **P1** single-node core + Rust, **P2** I/O bridge/workers/S3, **P3** Python/TS SDKs/CLI/UI, **P4** cluster + 1.0.

---

## Critical Pitfalls

### Pitfall 1: Host-side non-determinism. The Wasm is deterministic but the host isn't.

**What goes wrong:**
Every in-flight run fails replay with `NonDeterminism` after an Ostro or Wasmtime upgrade, even though no user module changed. Possible causes:
- The host changed how it serialises `op-output` (header order, JSON canonicalisation, float formatting).
- A WASI shim returns different values.
- Cranelift produces different NaN bit patterns.
- Relaxed-SIMD results differ between x86_64 and aarch64 workers.

**Why it happens:**
Teams treat "Wasm is deterministic" as a property of the whole system. Wasmtime's own deterministic-execution guide says you have to opt in. You need NaN canonicalisation (`cranelift_nan_canonicalization`) and deterministic relaxed SIMD (`relaxed_simd_deterministic`). Those features are non-deterministic across CPUs by default. Fuel is deterministic only for a fixed Wasmtime version. Host code that feeds values to the guest (HTTP response headers from hyper, which arrive in a `HeaderMap` order; SQL row values; `HashMap` iteration in the bridge) has the same hazards.

**How to avoid:**
- Configure the `Engine` from day 1 with NaN canonicalisation and deterministic relaxed SIMD (or with relaxed SIMD disabled).
- Store recorded results in the journal as **opaque canonical bytes** and hand them back verbatim on replay. Never re-derive them from structured host data.
- The command hash must cover kind + canonical argument bytes only. It must not include host version or timestamps.
- Keep a **cross-version replay corpus** in CI: journals recorded by release N must replay byte-identically on N+1, on x86_64 and on aarch64.

**Warning signs:** `ostro_replay_divergence_total` spikes right after a deploy of Ostro itself (not of user code). Divergence shows up only on one CPU architecture of workers.

**Phase to address:** P1 (Engine config, canonical journal encoding, corpus started). P2 (aarch64 workers in the corpus). P4 (rolling-upgrade gate).

---

### Pitfall 2: Version-skew divergence hiding in branches that only old histories reach

**What goes wrong:**
A new module version is deployed and new runs are fine. Days later, long-sleeping runs wake up, replay into a changed branch and land in the DLQ. Temporal documents exactly this: *"The changed branch only executes when an old history reaches it. New executions may never traverse it, so the deploy looks healthy."* Restate's RT0016 "journal mismatch" has the same root cause. Its official answer is immutable, versioned deployments.

**Why it happens:**
`ostro replay --sample N` samples *recent* histories, and recent histories are the ones least likely to hit the old path. Module GC deletes a `module_sha` that a 30-day sleeper still references. `migrate-runs` re-pins runs because a dry-run on a sample passed.

**How to avoid:**
- Runs are pinned to `module_sha` (already in the design). Make module GC reference-counted against **non-terminal runs plus retained history**, never time-based.
- Stratify replay sampling by workflow version **and by the furthest `seq` reached / wait kind**, not just recency. Always include the oldest non-terminal runs.
- `migrate-runs` should dry-run **every** run it re-pins, not a sample, and should refuse when any run diverges.
- `patched(change-id)` writes a `patch_marker` on first evaluation. On replay, a missing marker means `false` and a present marker means `true`. Test the case "patch removed while old runs still exist".

**Warning signs:** DLQ `nondeterminism` entries whose run `started_at` is much older than the deploy. Divergence clusters at a high `seq`.

**Phase to address:** P1 (pinning, `patched`, divergence at the right seq is already an exit criterion). P3 (`ostro replay` stratified sampling in all languages). P4 (module GC, `migrate-runs`).

---

### Pitfall 3: Pre-initialisation snapshots bake in "random" state

**What goes wrong:**
componentize-py and componentize-js both rely on Wizer-style pre-initialisation: the interpreter state is snapshotted at build time. Anything seeded during init gets frozen:
- Python's `random` module seed (taken from `os.urandom` at import).
- The `str` hash seed, which drives `set` iteration order.
- SpiderMonkey's `Math.random` state.

Two consequences:
1. **Security/correctness:** every run of a module draws the *same* "random" sequence if user code calls `random.random()`, `uuid.uuid4()` or `Math.random()` directly instead of `ctx.random`. Mozilla bug 1909217 raises exactly this concern for Wizer-snapshotted SpiderMonkey.
2. **Determinism across rebuilds:** rebuilding the *same source* produces a different snapshot seed. `set` iteration order changes, and runs re-pinned to the rebuilt module diverge.

**Why it happens:**
The toolchains are general-purpose and do not know about durable execution. Behaviour depends on the version and is not documented. I could not confirm whether current componentize-py/js reseed after snapshotting (LOW confidence either way).

**How to avoid:**
- After resume, the SDK monkey-patches or reseeds `random`, `uuid`, `time`, `datetime.now`, `secrets` (Python) and `Math.random`, `Date`, `crypto.getRandomValues`, `performance.now` (JS). All of these go through `ctx.random-*` / `ctx.now`, seeded per run from journaled data.
- Build with a fixed hash seed (`PYTHONHASHSEED=0` in the build env) so builds are reproducible. `ostro build` should verify reproducibility by building twice and comparing the sha.
- Add a conformance test: two different runs of the same module must observe different `ctx.random` values but identical values across replays of one run.

**Warning signs:** Two runs produce identical UUIDs. `ostro build` gives a different sha for unchanged source.

**Phase to address:** P0 (Spike S2 must test this explicitly). P3 (SDK patches + conformance).

---

### Pitfall 4: The guest event loop isn't as deterministic as it looks

**What goes wrong:**
- `asyncio.wait` and `asyncio.as_completed` iterate over a **`set`**. Temporal's Python SDK tracks this as non-deterministic (sdk-python issue #518) and ships its own `workflow.wait`.
- Python 3.12 changed `asyncio.wait_for` from `call_later` to `call_at`, which broke Temporal's custom loop until it was patched. A custom loop has to implement the exact loop methods each CPython version uses.
- In JS, completion delivery that resolves several promises in one host turn makes `Promise.race` order depend on microtask interleaving. `WeakRef` and `FinalizationRegistry` callbacks depend on GC timing. `setTimeout` without virtualised time runs on wall time.

DBOS's write-up ("Async Python is Secretly Deterministic") points out that only *task start order* is deterministic. Completion order and interleaving after the first `await` are not, unless the host imposes them.

**Why it happens:**
The design's delivery rule (resolve `wait` completions one at a time in journaled order and drain the ready queue between them) is right. But every stdlib combinator and every loop hook is a separate way around it.

**How to avoid:**
- The SDK owns the loop: a custom `AbstractEventLoop` subclass whose `time()` returns journaled logical time. `call_later`/`call_at` map to durable timers or raise. `run_in_executor` and threads raise.
- Override or ban `asyncio.wait` and `as_completed` in favour of SDK versions with an ordered-list contract.
- Pin the CPython version that componentize-py embeds, and run the loop conformance suite whenever it changes.
- In JS, ban or stub `WeakRef`/`FinalizationRegistry`, and route timers through `ctx.sleep`.
- Fuzz random gather/race/cancel trees and compare journals across 1,000 replays (already planned in P3). Include **cancellation**: a cancelled task that already scheduled an effect must journal `cancel-op` deterministically.

**Warning signs:** Flaky conformance results for "race" scenarios only. Divergence on the `wait_resolved` event specifically.

**Phase to address:** P1 (Rust mini-executor sets the contract). P3 (Python/TS loops).

---

### Pitfall 5: Idempotency keys outlive the provider's dedupe window

**What goes wrong:**
An effect is `effect_scheduled` and then the executor crashes. The run is re-driven by replay much later, after long backoff, a node outage, DLQ requeue or a manual retry days afterwards. The same idempotency key goes out again, but the provider has already forgotten it. Stripe v1 keeps keys for "at least 24 hours" and says *"We generate a new request if a key is reused after the original is pruned."* The result is a duplicate charge, and Ostro's "same key" guarantee lulls everyone into trusting it.

**Why it happens:**
Outbox designs assume key memory is infinite. Replay-driven re-execution (ARCH §5.2: "no separate sweeper") re-sends regardless of elapsed time.

**How to avoid:**
- Journal the `effect_scheduled` timestamp. When re-driving an in-flight effect, compare the elapsed time against a per-connection or per-host `idempotency_window` (default 24h for unknown hosts). If it is exceeded, **do not resend**. Fail the op with a distinct `effect_outcome_unknown` failure the workflow must handle, or park it for operator decision. Stripe itself advises checking out-of-band before repeating an operation whose outcome is unknown.
- Derive keys from `(run_id, op seq)` and **never** from the attempt number, so they stay stable across attempts, `migrate-runs` and patching.
- DLQ requeue of a run with an unresolved effect must show the window warning in UI/CLI.

**Warning signs:** Re-driven effects with `now - scheduled_at > 24h` appear in metrics. Support tickets about duplicates "despite idempotency keys".

**Phase to address:** P2 (bridge). P3 (UI surfacing).

---

### Pitfall 6: "Exactly-once" SQL that silently isn't

**What goes wrong:**
The `ostro_effects` token insert and the user statement are supposed to commit atomically, and here are some ways they don't:
- **MySQL implicit commits:** DDL, `LOCK TABLES` and similar commit the open transaction, so the token and the statement split.
- The token table is in a different database or schema than the user's table.
- `exactly-once` statements that call external procedures or `dblink`.
- The stored result row is too large.
- A SERIALIZABLE retry error is treated as a fatal failure.

**Why it happens:**
The transactional idempotency-token pattern only holds when everything happens inside one ACID transaction on one database.

**How to avoid:**
- Validate at deploy time or first use: reject `exactly_once=true` for statements the connector classifies as implicitly committing (MySQL DDL list).
- Create `ostro_effects` in the same connection and search path. Insert the token **first** in the transaction so concurrent duplicates block on the PK instead of both running.
- Map serialization failures and deadlocks to `retryable`.
- Run a crash test between COMMIT and `effect_completed` (already in the P2 exit criteria) against Postgres **and** MySQL.

**Warning signs:** Exactly-once test passes on Postgres and fails on MySQL. `ostro_effects` rows have no matching row change.

**Phase to address:** P2.

---

### Pitfall 7: A Wasmtime fiber stack overflow takes down the whole node

**What goes wrong:**
With `async_support`, host functions run on the guest's fiber stack. Wasmtime guarantees host code only `async_stack_size - max_wasm_stack` bytes (defaults are reportedly 2 MiB and 512 KiB; verify against the pinned version). Ostro's host imports are not trivial: serde of large `list<u8>`, journal comparison, rustls handshakes if the bridge is accidentally called inline, deep `tracing` spans. If one of them overflows, Wasmtime aborts the **process**, not the guest. Every partition on the node goes down with it. One malicious or large-payload guest becomes a node-level DoS.

**Why it happens:**
The stack budget is invisible in normal testing because guests don't use their full wasm stack.

**How to avoid:**
- Keep host imports thin. Any non-trivial work (I/O, TLS, compression, large serde) is handed to Tokio tasks off-fiber, and the fiber awaits the result.
- Set `async_stack_size` explicitly with a large margin over `max_wasm_stack`.
- Add a test guest that recurses to *exactly* `max_wasm_stack` and then calls every host import with maximum-size arguments (the technique from the Wasmtime host-stack-overflow write-up).
- Pooling `total_stacks` must be sized as well.

**Warning signs:** SIGABRT or SIGSEGV with fiber frames in core dumps. Crashes correlated with large payloads.

**Phase to address:** P1 (deep-recursion exit criterion: extend it to "deep recursion + host call").

---

### Pitfall 8: The pooling allocator exhausts virtual address space, or hoards RSS

**What goes wrong:**
- Each 32-bit linear memory slot reserves roughly 4 GiB plus guard regions. A Zulip thread reports about 6 GiB with older 2 GiB guard defaults. Wasmtime's docs say about 32k memory slots exhaust a typical 47-bit address space. Setting `total_memories` to "max concurrent runs" (say 50k) fails at startup, or fails inside containers with `ulimit -v`, strict `vm.overcommit_memory=2`, or low `vm.max_map_count` (MPK adds a VMA per slot).
- Conversely, `linear_memory_keep_resident` and the lazy decommit queue mean freed slots keep RSS. Wasmtime documents that "the total set of used slots ... can impact the overall RSS". With 30 to 50 MB Python heaps, the node's memory budget accounting is wrong.

**Why it happens:**
The pool's limits are fixed up front, and the defaults target edge FaaS (small modules, many slots), not Python-sized instances.

**How to avoid:**
- Use separate pooling configs, or separate `Engine`s, per language class: small slots for Rust, large and few for Python.
- Fall back to the on-demand allocator when pool creation fails, and log the reason (container limits).
- Account node memory budget from `PoolingAllocatorMetrics` (`unused_memory_bytes_resident`) plus live instances, not just live `StoreLimits`.
- `ostro doctor` checks overcommit and `max_map_count`.

**Warning signs:** `Engine::new` fails only in Kubernetes. RSS stays high after load drops.

**Phase to address:** P1 (pool sizing for Rust). P0/P3 (Python sizing from Spike S2).

---

### Pitfall 9: The evict-and-replay model goes quadratic for long or loopy runs

**What goes wrong:**
Every wake-up of an evicted run re-instantiates and replays the full journal. Example: a polling workflow with `sleep(60s)` in a loop for one day is 1,440 activations × O(events) replay ≈ 1M event replays, plus 1,440 Python instantiations. The 50k-event cap is reached well before anyone notices that CPU is going to replay. Temporal caps history at 51,200 events / 50 MB (warning at 10,240 / 10 MB), and its main mitigations are a sticky worker cache and continue-as-new.

**Why it happens:**
Hot-wait (≤2 s) covers short effects only. Journals ship in `Activate` every time.

**How to avoid:**
- Emit `continue_as_new` as a first-class WIT op in 0.1, not a "hint". Add a `ctx.continue_as_new_suggested()` that returns true by event count and bytes.
- Track `ostro_replay_events_total / activations` per workflow and alert on a ratio growing over time.
- Consider a per-node "warm run" LRU (keep the `Store` for recently evicted runs while memory allows) before resorting to a full replay.
- Send journal **pages/cursors** to workers, not full journals. Tonic's default max decode message size is 4 MiB, so a large journal fails `Activate` outright.

**Warning signs:** CPU per step grows with run age. Worker `Activate` messages fail with "message too large".

**Phase to address:** P1 (continue-as-new op, metric). P2 (paged journals over gRPC).

---

### Pitfall 10: Derived state gets ahead of durable, committed log

**What goes wrong:**
`state.redb` is applied from the log. Some ways it ends up ahead of the log, or ahead of what's committed:
- The state store reflects an entry that was later truncated (cluster divergence, RP-3).
- `applied_offset` is persisted in a different transaction than the data.
- WAL segments are deleted up to `applied_offset` when that offset was only committed with low durability.

The first one is irreversible: you cannot truncate redb.

**Why it happens:**
- In a cluster, it's tempting to apply up to the local LEO for faster failover.
- On a single node, it's tempting to use `Durability::None` for redb throughput. redb's own docs note that with `None`, pages are only freed by durable commits (risk of `OutOfSpace` and file growth). A past release fixed a corruption bug when a `None` commit was followed by a crashing durable commit.

**How to avoid:**
- **Apply only ≤ HW**, on leaders and followers alike. On a single node, HW is the fsynced LEO.
- Write `applied_offset` in the **same** redb write transaction as the batch.
- WAL retention may delete segments only below the `applied_offset` of the last *durable* redb commit **and** below the last uploaded snapshot. Chronik RP-1.1 is the follower-retention analogue.
- If redb uses lower durability, force an `Immediate` commit periodically and before any WAL deletion. Release persistent savepoints.
- On truncation below `applied_offset`, discard and rebuild state from snapshot + log rather than "fixing" it.

**Warning signs:** "Delete state.redb and rebuild" hash differs from live state (already a P1 exit criterion; run it in CI under crash injection, not just clean shutdown). redb file grows without bound.

**Phase to address:** P1 (apply ≤ HW invariant + retention interlock even at RF=1). P4.

---

### Pitfall 11: Treating fsync failure as retryable (fsyncgate)

**What goes wrong:**
After a failed `fsync`, Linux may mark the dirty pages clean and drop the error. A retried fsync then "succeeds" with the data gone. PostgreSQL acknowledged durable commits this way, and its fix was to **PANIC** on fsync failure (`data_sync_retry=off`).

Chronik RP-11 shows the same shape in-house: every non-transactional storage failure was mapped to `ErrorCode::None`, so it reached the client as a success. Ostro's group commit must never return `RunStarted` / `AppendAck` success after an I/O error.

**How to avoid:**
- An fsync or write error in group commit is **fatal for the partition**: stop accepting writes, fail every waiter in the batch, and restart recovery from the log.
- fsync the **directory** after creating, renaming or deleting segments and after the `tmp+fsync+rename` of `state`/`HardState`.
- Exhaustively map every storage error to a failure code. Add a test, `append_failure_is_never_reported_as_success`, using a failpoint-injected EIO.
- Tolerate CRC mismatch **only at the tail** (torn write). A mid-log CRC failure halts the partition and never skips the record.

**Warning signs:** Any `match` on storage errors with a `_ =>` arm that returns success. Benchmarks that report rates without bytes on disk (RP-11 rule).

**Phase to address:** P1 (crash-injection harness with EIO and ENOSPC failpoints, not only `kill -9`).

---

### Pitfall 12: Startup and retention races that destroy live data

**What goes wrong:**
Chronik RP-3.3: on restart, WAL recovery finished before the metadata catalog loaded. The `WalIndexer` then saw every topic as "orphaned" and **deleted live WALs 2 ms after recovery loaded them**. Ostro has exactly the same opportunities:
- Retention (`RunsExpired`), module GC, blob GC and AOT cache cleanup all decide "unreferenced → delete".
- Partition directories with no metadata entry yet look abandoned during startup.

Chronik also hit a startup-ordering bug: the epoch warm-up ran 553 µs *after* the fetcher started, so reconciliation silently found no history.

**How to avoid:**
- Any "unreferenced → delete" decision must require the reference source to be **fully recovered** (an explicit readiness barrier), and must see the item missing on N consecutive passes (Chronik's `ORPHAN_CONFIRM_PASSES`).
- Startup is an explicit ordered state machine: log recovery → state catch-up → epoch history → metadata ready → dispatcher/fetcher/GC start. Add a test that asserts the order.
- Log unconditionally at `info` what each GC/warm-up pass did, including "did nothing". Chronik found that "warmed nothing" and "never ran" looked identical in the logs.

**Warning signs:** Data disappears only across restarts. GC logs show deletions in the first seconds after boot.

**Phase to address:** P1 (single-node GC/retention). P2 (blob/S3 GC). P4.

---

### Pitfall 13: Replicated state with no causality token, and truncation that destroys data

These are the Chronik lessons that 04 §4 doesn't cover (RP-3/5/7/9):

**What goes wrong:**
- **Stale metadata overwrote newer state.** A returning node replayed its stale metadata WAL and its old `PartitionAssigned` overwrote the newer failover assignment *on every node*. The fix was to use the leader epoch as a causality token: ignore older epochs, accept equal ones (anti-entropy re-asserts them).
- **Epoch dropped in transit.** Replicated assignments were rebuilt with `leader_epoch: 0`, so every follower believed epoch 0 forever and truncation could never fire.
- **ISR lived only in the leader's memory**, so the failover controller (the Raft leader) could not consult it. ISR has to be *published into metadata*.
- **Liveness input was a constant.** raft-rs `recent_active` is cleared only when `check_quorum` is on, and it defaults to off, so dead peers looked alive forever.
- **Failover shrank the replica set**, so the returning node was no longer a replica and never reconciled.
- **The log end got stranded below the watermark** (two atomics, one gating the other). An elected replica reassigned existing offsets and the ex-leader truncated 61 committed records. Divergence tests failed 3/10 runs.
- **A no-op truncation reset the log end to 0** (`None` meant both "empty" and "untouched").
- The truncation delete loop **unlinked the open zero-length segment**, so writes were fsynced into an orphaned inode.
- An **in-flight group-commit batch survived truncation** and resurrected discarded records.
- **Truncation is local:** a follower's divergent tail already uploaded to object storage survives under a different key. This applies directly to Ostro's bottomless mode.
- The leader **parsed but never validated `current_leader_epoch`** on fetch, so nothing was fenced up front.
- A `-1` meaning "I can't answer" must never be interpreted as an offset.

**How to avoid:**
- Every replicated metadata record (assignment, ISR, module, workflow version, queue policy, schedule) carries a monotonic version, and apply rejects older ones. Audit this as a checklist item.
- Make the plan functions (`plan_failover`, `plan_reconciliation`, `plan_truncation`) pure, with a test for every branch that *doesn't* delete.
- Only leaders upload segments, or uploads are gated on records being ≤ HW.
- Validate the epoch on every fetch and append.
- Enforce `log_end ≥ HW` in one function.
- Turn on `check_quorum` or own the liveness sampler, and test that a killed peer is reported dead within the window.

**Warning signs:** Reconcile logs say "nothing to truncate" right next to a detected straddle (D1). Hot reconcile loops with no backoff (D2). `epoch 0 ends at -1`.

**Phase to address:** P1 (log_end/HW invariant, truncation primitive safety, S3 upload gating even at RF=1). P2 (bottomless upload ≤ HW). P4 (the rest; conformance suite week 1).

---

### Pitfall 14: Fencing that stops the journal but not the world

**What goes wrong:**
A worker pauses (VM stall, GC, network partition) past `lease_ttl`. The run is re-dispatched as attempt+1. The zombie wakes and **still performs its HTTP call**. Its `Append` is rejected (good), but the external effect has already happened. There's a second trap: if the "bump attempts of in-flight runs" step on a new leader is soft state, a second failover can **reuse** attempt numbers. Two live executors then both pass the `attempt == current_attempt` check.

**Why it happens:**
Fencing tokens protect the store that checks them, not third parties. Leases are explicitly not logged (ARCH §2.2).

**How to avoid:**
- Make the fencing token `(leader_epoch, attempt)`, or log the attempt bump as a command, so tokens are monotonic across any number of failovers.
- `ResolveSecret` and the I/O bridge validate the current token **immediately before** sending, so a zombie can't get secrets. The idempotency key (same across attempts) is the only protection for the external side. Document this as the at-least-once boundary.
- Workers stop all I/O when they lose the stream or miss heartbeat acks for `lease_ttl/2` (self-fencing). Chronik's TCP lesson applies: a successful socket write proves nothing about liveness.
- The P2 risk-table test, "partition the worker mid-effect", must assert that **a single external call reaches the mock**, not just a single journaled completion.

**Warning signs:** Mock API sees two calls with the same key a lease-TTL apart. Attempt numbers reset after a double failover.

**Phase to address:** P1 (token shape). P2 (workers, self-fencing). P4 (epoch component).

---

### Pitfall 15: Secrets exfiltrated through allowed egress

**What goes wrong:**
"Secrets never enter guest memory" is necessary but not sufficient:
- A guest puts `{{secret:stripe}}` in a header to *any* allow-listed host, including an echo endpoint, a logging SaaS, or `https://httpbin.org/anything`. It reads the secret back in the response body.
- Error bodies from APIs often echo request headers. These get journaled, logged and shown in the UI.
- An automatic redirect forwards `Authorization` to a different host. Check whether the pinned reqwest/hyper version strips sensitive headers on cross-origin redirects (MEDIUM confidence that reqwest does).

**How to avoid:**
- **Bind each secret grant to destination origins** (scheme+host+port), not just to the module. Template expansion fails if the request's resolved origin isn't in that secret's binding.
- Disable automatic redirects for requests carrying secrets, or re-check the binding on every hop.
- Scan responses: replace any occurrence of a resolved secret value (and base64/URL-encoded forms) in response headers and body before journaling or returning to the guest.
- Extend the P2 canary scanner to OTel attributes, tracing fields, panic messages and DLQ reasons.

**Warning signs:** Canary appears in a journaled response. Secret grant has no origin field.

**Phase to address:** P2.

---

### Pitfall 16: An SSRF guard bypassed by proxies, redirects and address encodings

**What goes wrong:**
- **DNS rebinding:** validate-then-connect resolves twice.
- **Redirects** to internal hosts.
- **IPv4-mapped IPv6** (`::ffff:169.254.169.254`), NAT64 (`64:ff9b::/96`), `0.0.0.0`, decimal/octal IP literals, IPv6 link-local, and the AWS IPv6 metadata address `fd00:ec2::254`.
- **reqwest honours `HTTP(S)_PROXY` env vars by default**, so the proxy does the resolving and the guard checks nothing.
- Hosts that resolve to multiple IPs, some private.

**How to avoid:**
- Use a custom resolver that classifies **every** resolved address after unwrapping mapped/NAT64 forms, and connect only to the vetted `SocketAddr`. This is the "resolve once, pin" pattern used by several Rust SSRF crates.
- Re-validate every redirect hop.
- Build the client with `.no_proxy()` unless an operator explicitly configures an egress proxy.
- Reject the request if *any* resolved address is blocked.
- Keep a test table of bypass payloads in CI.

**Warning signs:** Guard implemented as a URL-string check. Client built with `reqwest::Client::new()`.

**Phase to address:** P2.

---

### Pitfall 17: componentize-py/js reality vs. the NFRs

**What goes wrong:**
Python components are tens of MB. componentize-py wheels alone are about 16 MB. Golem's guide calls the output "large" and says stripping debug sections roughly halves it. The roadmap assumes 10–50 MB of RSS per instance. Consequences:
- Cold compile of a Python component with Cranelift takes seconds of CPU.
- Native-extension packages (numpy, pandas, pydantic-core) need special WASI wheels. The community `wasi-wheels` repos describe themselves as needing maintainers.
- componentize-py can only import dependencies at build time. Lazy submodule imports fail (issue #23).
- Threads aren't available. Socket support is partial.
- componentize-js output is roughly 5–8 MB of SpiderMonkey. Weval AOT has been disabled in some versions over LLVM incompatibilities.
- WASI 0.3 shipped in Wasmtime 46 (June 2026, component-model-async on by default). Guest toolchains will migrate their WASI imports, so the host's import surface keeps moving.

**Why it happens:**
These are young toolchains maintained by small teams. The numbers are not documented, so plans are built on optimism.

**How to avoid:**
- Spike S2 measures, for the **pinned** versions: size before and after strip, Cranelift compile time, instantiate p50 with pre-init, and idle and peak RSS.
- Publish a supported-packages matrix, and reject unsupported imports at `ostro build` time, not at runtime.
- Route Python to worker-friendly queues by default.
- Keep `ostro:runtime@0.1` free of exotic WIT. The roadmap already has a fallback from `resource step` to a `u64` handle; decide it in P0 rather than P3.
- Pin toolchain versions inside the hermetic cache, and keep a CI compatibility matrix.
- The host must keep satisfying the WASI 0.2 imports even after WASI 0.3 guests appear.

**Warning signs:** Spike S2 numbers missing NFR-2 by more than 2×. An upstream release changes generated import names.

**Phase to address:** P0 (S2). P3.

---

### Pitfall 18: AOT cache invalidation and compile storms

**What goes wrong:**
A `.cwasm` only loads on the **same Wasmtime version** with compatible CPU features and target ("Module was compiled with incompatible Wasmtime version '36.0.1'"). Every Wasmtime upgrade, which happens about monthly and often for security fixes, therefore invalidates the whole cache. After the restart, every module recompiles at once, Python components take seconds each, and first-activation latency spikes cluster-wide. Shipping `.cwasm` files between heterogeneous workers (AVX2 vs non-AVX2, x86 vs arm) fails or, worse, gets "fixed" by disabling the version check. `Module::deserialize` is `unsafe`: tampered bytes mean arbitrary code execution.

**How to avoid:**
- Cache key = sha256(module) + `Engine::precompile_compatibility_hash()` + Ostro version. Keep the HMAC (already designed).
- Never set the module version strategy to `None`.
- Workers compile locally. If artefacts are shared, compile for a baseline CPU target per architecture.
- After an upgrade, recompile lazily on first activation with bounded compile concurrency (a dedicated compile pool), plus background pre-warming of modules referenced by ready runs.
- Track compile time and queue depth as metrics.

**Warning signs:** p99 first-step latency spikes after every upgrade. Workers log deserialize failures.

**Phase to address:** P1 (cache key). P2 (workers). P4 (rolling upgrade).

---

### Pitfall 19: Static single binary vs. musl, macOS and Cranelift

**What goes wrong:**
- **musl's default allocator** is badly contended. Tweag measured a musl-built wasmtime benchmark at about 93 s, and switching to mimalloc made it faster than glibc. Other reports show musl stays about 26% slower from memcpy/memcmp even with mimalloc.
- **jemalloc bakes in page size at build time**, which breaks on 16K/64K-page aarch64 kernels.
- **macOS can't be fully static** (libSystem is mandatory).
- Notarization requires the hardened runtime, and Cranelift's executable memory needs `com.apple.security.cs.allow-jit` (MAP_JIT) or `allow-unsigned-executable-memory`. Without the right entitlement, a notarized binary crashes on first compile. Verify which one Wasmtime's mmap path needs (LOW confidence).
- C dependencies (zstd-sys, `aws-lc-rs` if chosen for rustls) complicate musl and aarch64 cross builds.

**How to avoid:**
- Use mimalloc as the global allocator on musl. Benchmark musl vs glibc builds in CI on the P1 throughput gate.
- Prefer the `ring` crypto provider over aws-lc-rs for musl simplicity, unless FIPS is required.
- Run signing, notarization and entitlements in the release pipeline from P1. Smoke-test the *notarized* artefact compiling a module.
- Track binary size per release (wasmtime+cranelift+tonic+UI).

**Warning signs:** musl build hitting under 50% of glibc throughput. macOS release crashes only when downloaded via a browser (quarantine + Gatekeeper).

**Phase to address:** P1 (CI release matrix, cross-cutting).

---

### Pitfall 20: Noisy neighbours below the quota layer

**What goes wrong:**
Per-tenant concurrency and fuel quotas exist, but these shared resources aren't covered:
- **Partition log / group commit:** one tenant's fan-out of 10k children saturates the append path that every tenant hashed to that partition shares.
- **Compile pool:** one tenant deploying a 40 MB Python module 50 times.
- **Timer wheel memory:** millions of sleeps.
- **Replay CPU** (Pitfall 9).
- **Blob storage:** large payloads.

Epoch slicing bounds wall time per slice but not total CPU across many short activations.

**How to avoid:**
- Add per-tenant quotas on event append rate, compile CPU, outstanding timers, children per run and blob bytes. Weighted fair dispatch has to cover appends and compiles, not just activations.
- Rate-limit deploys.
- Account fuel per tenant per second.

**Warning signs:** p99 commit latency rises for all tenants when one tenant spikes.

**Phase to address:** P2 (basic limits). P4 (tenants + fair dispatch).

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Single-node path that bypasses HW/ISR ("RF=1 is special") | Faster P1 | Chronik's 9-month bug class: the cluster path is untested until P4 | Never (D5) |
| Journal stores structured results, re-encoded on replay | Smaller code | Host upgrades break replay (Pitfall 1) | Never |
| `continue_as_new` as a "hint" only | Smaller WIT | Unbounded histories, quadratic replay; a WIT change after the freeze | Never; put it in 0.1 |
| Time-based module GC | Simple | Deletes modules referenced by long sleepers | Never |
| Sample-only `migrate-runs` dry run | Fast | Re-pins runs that diverge | Never for re-pinning; OK for `ostro replay` CI hints |
| redb `Durability::None` without periodic durable commits | Throughput | File growth/OutOfSpace; WAL retention unsafe | Only with forced durable checkpoints gating WAL deletion |
| Kafka Fetch internally (Option A) | Reuse tested path | Protocol baggage; epoch validation missing in Chronik today | P4 MVP, provided epoch validation is added |
| Disabling the Wasmtime module version check | Avoids recompiles | Loads incompatible machine code | Never |
| `reqwest::Client::new()` in the bridge | One line | Env proxies bypass SSRF guard | Never |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Stripe / payment APIs | Assume idempotency keys are remembered forever | 24h window (v1); refuse re-send beyond window → `effect_outcome_unknown` |
| MySQL exactly-once | Run DDL inside the token transaction | Reject implicitly committing statements for `exactly_once` |
| Postgres via PgBouncer (transaction mode) | Session-level `SET` or advisory locks across statements | Keep the token + statement in one explicit transaction; no session state |
| Webhooks (Stripe/GitHub) | Verify HMAC over re-serialised JSON; no timestamp tolerance | Verify over raw body bytes; enforce timestamp tolerance; dedupe by event id via start idempotency key |
| Cron | Fire on wall clock in local TZ; no catch-up policy | Store UTC fire times; explicit DST semantics; catch-up policy (skip / one / all) after downtime; key = schedule ‖ fire_time (designed) |
| tonic worker stream | Default 4 MiB decode limit | Page journals; set explicit limits; blobs via `FetchBlob` chunks |
| S3 bottomless | Followers upload segments; uploads before commit | Upload only ≤ HW from the leader; delete divergent objects on truncation |
| componentize-py packages | Lazy submodule imports (`import x; x.y`) | `from x import y` at top level (issue #23); build-time import validation |
| raft-rs | Rely on `recent_active` with `check_quorum=false` | Own the liveness sampler or enable `check_quorum` (Chronik RP-5) |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Full-journal replay on every wake | CPU per step grows with run age | continue-as-new, warm-run LRU, replay-ratio metric | Runs with more than ~1k events woken frequently |
| Python instantiate per activation | Activation p50 ≫ 10 ms; RSS spikes | Pre-init snapshot, pooling with CoW image, hot-wait, Python-to-workers default | Fan-out above a few hundred concurrent Python runs per node |
| Fuel metering everywhere | ~10%+ slower than epochs (Wasmtime docs: fuel is "significantly more expensive") | Epochs for wall-time slicing; fuel only where deterministic limits matter; measure in P1 | CPU-heavy guests |
| musl system allocator | Throughput far below glibc build | mimalloc global allocator | Any multithreaded load |
| Re-reading the active segment per fetch | Throughput collapse under replication | Already inherited (RP-9); keep the sparse index on the history read path too | History API / replay reads of hot partitions |
| Timer wheel rebuild from redb on leadership | Failover p99 > 10 s | Hierarchical wheel loaded lazily by due-window; measure rebuild time per million timers | ~1M+ pending timers per partition |
| Compile storm after Wasmtime upgrade | First-activation latency spike | Bounded compile pool + pre-warm | Every upgrade |
| Logging per fetch/batch at info | Chronik wrote 23 MB/node/25 s of logs | Hot-path logs at debug; sampling | Under any real load |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Secret grants not bound to destination origin | Exfiltration via echo endpoints | Origin-bound grants; response secret scrubbing |
| Following redirects with secret-bearing headers | Credential sent to a third party | No auto-redirect for templated requests |
| URL-string SSRF checks | Rebinding/encoding bypass | Resolver-level classification + pinned connect |
| Unsafe `Module::deserialize` from shared/remote cache | Arbitrary code execution | HMAC + local-only compile on workers |
| Snapshotted PRNG used by guests | Predictable "random" tokens across runs | SDK reseeding/patching (Pitfall 3) |
| Late Wasmtime security upgrades | Guest escape (Wasmtime has had pooling-allocator CVEs such as CVE-2022-39392; a 2026 advisory reports a guest-triggerable stack overflow via mismatched async callback types, LOW confidence on details) | Track the Wasmtime advisory feed; upgrade within days; AOT-recompile plan (Pitfall 18) |
| Webhook endpoints without replay protection | Duplicate workflow starts | Timestamp tolerance + event-id idempotency key |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| NonDeterminism error shows only `seq 412 mismatch` | Developer can't tell what changed | Show expected vs. actual command (kind, name, args diff) and the source location the SDK recorded |
| Guest logs duplicated on every replay | Noisy, misleading logs | SDK suppresses logs while `is_replaying` (designed); UI marks replayed spans |
| Redeploying "version 3" with different code | Silent divergence of in-flight runs | Versions are immutable per `module_sha`; deploy refuses the same name+version with a different sha unless `--force` |
| Python package fails only at runtime in Wasm | "Works locally" frustration | `ostro build` import validation + supported-packages matrix |
| `is-replaying` used for business logic | Divergence | Lint in SDKs; docs; the conformance test detects it |
| Effect outcome unknown hidden as a generic failure | Operators re-run and double-charge | Distinct state + UI action "mark done / retry anyway" |

## "Looks Done But Isn't" Checklist

- [ ] **Crash safety:** tested with `kill -9` *and* EIO/ENOSPC failpoints on write and fsync. Verify fsync errors halt the partition.
- [ ] **State rebuild:** "delete state.redb → identical hash" run **after crash injection**, not only after clean shutdown.
- [ ] **Determinism:** journals replay byte-identically on the *next* Ostro version and on the other CPU architecture.
- [ ] **Pre-init:** two runs of the same module see different `ctx.random`/UUIDs. A rebuild of unchanged source gives the same sha.
- [ ] **Python/TS loop:** `asyncio.wait`, `as_completed`, `wait_for`, `asyncio.timeout`, cancellation; JS `Promise.race`, timers, `WeakRef`. All conform.
- [ ] **Fencing:** chaos test asserts the external mock saw one call per key, not just one journaled completion. Attempt tokens survive double failover.
- [ ] **Idempotency window:** an effect re-driven after more than 24h is not re-sent.
- [ ] **SSRF:** bypass table (mapped IPv6, NAT64, decimal IPs, redirects, env proxy) all blocked.
- [ ] **Secrets:** canary never appears in journal, response bodies returned to the guest, OTel, panics, DLQ reasons or S3.
- [ ] **Host stack:** max-depth guest calling every host import with max-size args does not abort the process.
- [ ] **Pooling:** server starts under `ulimit -v`, `overcommit=2` and Kubernetes defaults, or falls back with a clear log.
- [ ] **AOT:** Wasmtime bump → caches invalidated and recompiled with bounded concurrency; no deserialize of a foreign `.cwasm`.
- [ ] **GC/retention:** nothing deleted until reference sources are recovered and the item is missing on N passes.
- [ ] **Bottomless:** only ≤ HW uploaded. Restore on a fresh machine matches the state hash.
- [ ] **Replicated metadata:** every record type has a version and stale apply is rejected (Chronik RP-5 audit).
- [ ] **Test harness honesty:** each conformance test proven to FAIL on a deliberately broken build (Chronik RP-0: "the suite itself lied twice"; "better behaviour silently invalidated the test").
- [ ] **macOS:** the notarized artefact, downloaded through a browser, compiles and runs a module.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Mass divergence after a host upgrade | HIGH | Roll back Ostro (old journals still valid); add the corpus case; fix canonicalisation; re-release. Requires N/N-1 binary compatibility of storage formats |
| Version-skew divergence of user runs | MEDIUM | Redeploy the old module sha (retained), `migrate-runs` back, add `patched()` guard, then re-pin with full dry-run |
| Duplicate effect beyond the idempotency window | HIGH (external) | Reconcile with the provider; add the window check; audit `effect_scheduled` older than the window |
| State store diverged from the log | MEDIUM | Delete `state.redb`, rebuild from snapshot + log; investigate apply non-determinism via the state-hash bisect |
| Divergent replica / bad truncation | HIGH if committed data lost | Stop the partition; compare per-replica logs offline; never auto-truncate below HW; restore from the replica holding the committed prefix or from S3 |
| Pool VA exhaustion in production | LOW | Lower `total_memories`, shrink guard regions, or switch to the on-demand allocator via config |
| Compile storm | LOW | Throttle the compile pool; pre-warm; temporarily route to warm workers |
| Secret leaked into the journal | HIGH | Rotate the secret; redact journal entries by a tombstone command (needs a designed redaction path); purge S3 copies |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| 1 Host-side non-determinism | P1, P2, P4 | Cross-version + cross-arch replay corpus in CI |
| 2 Version-skew branches | P1, P3, P4 | Stratified replay; `migrate-runs` full dry-run; GC refcount tests |
| 3 Pre-init baked randomness | P0 (S2), P3 | Conformance: distinct randomness per run, reproducible builds |
| 4 Event-loop determinism | P1 (Rust), P3 | Gather/race/cancel fuzzing, 1,000 replays |
| 5 Idempotency window | P2 | Chaos test with a re-drive after a simulated 25h |
| 6 SQL exactly-once gaps | P2 | Crash test on Postgres and MySQL |
| 7 Fiber stack overflow | P1 | Max-depth + host-call guest |
| 8 Pooling VA/RSS | P1, P3 | Start under container limits; RSS-after-load test |
| 9 Quadratic replay | P1, P2 | Replay-ratio metric; long-loop benchmark |
| 10 State ahead of the log | P1, P4 | Apply ≤ HW assertion; rebuild-after-crash hash |
| 11 fsyncgate / false success | P1 | EIO failpoints; `append_failure_is_never_reported_as_success` |
| 12 Startup/GC races | P1, P2, P4 | Restart-under-load tests; ordered startup assertion |
| 13 Replication causality/truncation | P1 (invariants), P4 | RP-0-style suite incl. divergence, stale-metadata revert, S3 tail |
| 14 Zombie executors | P1, P2, P4 | Partition-mid-effect test counting external calls |
| 15 Secret exfiltration | P2 | Echo-endpoint exfil test; canary scanner |
| 16 SSRF bypass | P2 | Bypass payload table |
| 17 componentize reality | P0, P3 | S2 measurements vs. NFR-2; compat matrix |
| 18 AOT invalidation | P1, P2, P4 | Upgrade test; foreign-cwasm rejection |
| 19 Static binary/macOS | P1 | Release matrix; notarized smoke test; musl perf gate |
| 20 Noisy neighbours | P2, P4 | Single-tenant spike test vs. others' p99 |

## Sources

**First-hand (HIGH):**
- Chronik `docs/ROADMAP_REPLICATION.md` (RP-0..RP-12): indexer deleting live WAL on restart, D0 unclean election, epoch dropped in replicated assignments, stale-metadata revert, `check_quorum` liveness, log-end stranding, truncation bugs, object-store divergent tails, unvalidated `current_leader_epoch`, error-to-success mapping (RP-11), harness false results.

**Official docs (HIGH/MEDIUM):**
- Wasmtime deterministic execution: https://docs.wasmtime.dev/examples-deterministic-wasm-execution.html
- Wasmtime interrupting (fuel vs epoch): https://docs.wasmtime.dev/examples-interrupting-wasm.html
- Wasmtime `PoolingAllocationConfig`: https://docs.wasmtime.dev/api/wasmtime/struct.PoolingAllocationConfig.html
- Wasmtime pooling Zulip thread (6 GiB per slot): https://bytecodealliance.github.io/zulip-archive/stream/217126-wasmtime/topic/.E2.9C.94.20Issue.20with.20pooling.20allocator.html
- Wasmtime async stack sizing (C API docs): https://docs.wasmtime.dev/c-api/async_8h.html
- Wasmtime precompiled modules: https://docs.wasmtime.dev/examples-pre-compiling-wasm.html ; https://docs.wasmtime.dev/api/wasmtime/struct.Module.html
- WASI 0.3 release: https://wasi.dev/releases/wasi-p3 ; https://bytecodealliance.org/articles/WASI-0.3
- Temporal limits: https://docs.temporal.io/workflow-execution/limits ; execution failures: https://docs.temporal.io/troubleshooting/execution-failures
- Temporal Python sandbox: https://docs.temporal.io/develop/python/python-sdk-sandbox ; asyncio loop blog: https://temporal.io/blog/durable-distributed-asyncio-event-loop
- Temporal sdk-python #518 (asyncio.wait/as_completed sets): https://www.github.com/temporalio/sdk-python/issues/518
- Temporal community, supported asyncio constructs (3.12 wait_for): https://community.temporal.io/t/python-supported-asyncio-constructs/10111
- Restate versioning: https://docs.restate.dev/services/versioning.md ; https://restate.dev/blog/solving-durable-executions-immutability-problem
- Stripe idempotency: https://docs.stripe.com/api/idempotent_requests ; https://docs.stripe.com/error-low-level
- redb changelog/design: https://github.com/cberner/redb/blob/master/CHANGELOG.md ; https://docs.rs/redb/1.0.0/redb/enum.Durability.html
- Kafka KIP-966: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/263426970/KIP-966+Eligible+Leader+Replicas
- Apple hardened runtime: https://developer.apple.com/documentation/security/hardened-runtime

**Community / secondary (MEDIUM/LOW):**
- fsyncgate: https://danluu.com/fsyncgate/
- DBOS, Async Python is Secretly Deterministic: https://www.dbos.dev/blog/async-python-is-secretly-deterministic
- Tweag, static musl + mimalloc (wasmtime benchmark): https://www.tweag.io/blog/2023-08-10-rust-static-link-with-mimalloc
- componentize-py README (issue #23): https://github.com/bytecodealliance/componentize-py ; PyPI: https://pypi.org/project/componentize-py
- Golem building Python components (size/strip): https://learn.golem.cloud/docs/python-language-guide/building-components
- ComponentizeJS: https://github.com/bytecodealliance/componentizejs
- Mozilla bug 1909217 (Wizer snapshot PRNG): https://bugzilla.mozilla.org/show_bug.cgi?id=1909217
- Wasmtime host-function stack overflow write-up: https://hashnode-hub.vercel.app/posts/658f8a1c354256671b223d12
- SSRF crates (resolve-and-pin patterns): https://docs.rs/crate/antissrf/0.1.1 ; https://docs.rs/crate/nab/0.8.5/source/src/ssrf.rs
- CVE-2022-39392 (Wasmtime pooling): https://nvd.nist.gov/vuln/detail/CVE-2022-39392 ; CVE-2026-115384 (LOW, details unverified): https://access.redhat.com/security/cve/CVE-2026-115384

---
*Pitfalls research for: polyglot Wasm durable execution engine (Ostro)*
*Researched: 2026-11-18*
