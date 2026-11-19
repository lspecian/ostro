# Phase 9: Polyglot SDKs & Conformance - Research

**Researched:** 2026-11-19
**Domain:** Python (componentize-py) and TypeScript (jco: QuickJS / StarlingMonkey) workflow SDKs over the frozen sync WIT 0.1; a language-neutral conformance suite; child workflows (WF-06); a saga helper (WF-10)
**Confidence:** MEDIUM-HIGH. Toolchain behaviour was measured locally in the S2 harness (wasmtime 50.0.2) on 2026-11-19. Server/engine claims come from reading the code at HEAD `66be068`. Upstream versions were checked against the GitHub, PyPI, npm and crates.io APIs.

<user_constraints>
## User Constraints

There is no `09-CONTEXT.md` (no `/gsd:discuss-phase` was run). The binding constraints are the roadmap entry, the requirements and the locked project decisions below. They are copied from ROADMAP.md, REQUIREMENTS.md, STATE.md, 05-DECISIONS.md and 06-DECISIONS.md.

### Locked decisions (must be honoured)
- **Sync WIT with async host imports.** Wasmtime host imports are `async | trappable` over a sync WIT. Hosts ship an Ostro-owned deterministic WASI 0.2 shim (18 interfaces in `crates/ostro-engine/wit-wasi/shim.wit`) and do not use `wasmtime-wasi` (Research 3, 03-04).
- **WIT 0.1 freezes at 05-20.** After that, records, variants and enums never gain fields or cases, and changes are additive through `0.1.N` packages only. 05-20 pins the sha256 of the whole `runtime.wit` file, **comments included**, so every doc-comment finding must land in **05-05 Task 3** before 05-20 (D14).
- **Step handle = `resource step`.** The engine never relies on `HostStep::drop`. SDKs wrap steps in `with` (Python), `using`/dispose (TS) or RAII (Rust) (01-08).
- **D7, step-local RNG (03-11, frozen).** Randomness inside a live step comes from `sha256("ostro/step-rng/v1" ‖ seed ‖ seq)`, and every attempt gets a fresh stream. "Phase 9 SDKs must reseed at step boundaries the same way."
- **D11.** `Op::wait()` observes cancellation by default (`wait([op, CANCELLATION], Any)` → `Err(Cancelled)`); code inside `ctx.shielded(..)` does not. SDK-only, no WIT change.
- **D6:** `patched` is memoised per change-id, and unqueried markers are skipped. **D5:** `cancel-op` completes the op with `Err(Cancelled)`, and the first completion in journal order wins. **D10:** continue-as-new is in scope (Phase 5). **D21:** apply journals submitter-encoded event bytes verbatim (signals = `SignalPayload` 96 + `SignalMatched` 97).
- **Phase 7 F4:** `run-info.attempt` is not replay-stable. It is for logging and metrics only, never control flow.
- **P-11 (06-DECISIONS):** `ctx.idempotency_key(name)` = `"ostro-" + hex(sha256("ostro/idem/v1/name" ‖ run_id ‖ 0x00 ‖ name))[..16 bytes]`, computed in the guest (38 chars).
- **S2:** Python is embedded by default (6.5 MB RSS per instance). Pools are sized per language class: Python = 38 core instances per instance, and the default pool fits 26 Python instances. Python/TS AOT costs about 1 s and 0.5 GB peak RSS per module.
- **RESERVATIONS.md is the only ordinal registry.** Phase 9 currently has **no block** (see PF-F5).
- **Standing rules:** no chrono, bincode or wasmtime `cache`; no Chronik paths; no remotes or pushes (so publishing to PyPI/npm is human_needed); commits end with the Co-Authored-By line; `scripts/ci-local.sh` is the gate; clippy runs with `-D warnings`.

### Requirement wording that research recommends revisiting (USER REVIEW)
- SDK-02 says "SDK-owned asyncio loop with gather/race; built with pinned componentize-py (`--stub-wasi`)". Research keeps this exactly.
- SDK-03 says "SDK-owned job loop with Promise.all/race; built with pinned jco (no WASI imports)". Research keeps this exactly, and it was verified to be achievable on both jco backends with **zero WASI imports**. The backend choice (QuickJS vs StarlingMonkey) is new and needs sign-off (D-P9-1).
- SC2 ("rebuilding the same source yields the same module sha") **is not achievable for Python/TS with the released toolchains** (S2 confirmed; this research reproduced and narrowed it). It needs either an upstream componentize-py patch (or a pinned fork), or the user accepting a documented residual (D-P9-3).

### Deferred / out of scope
- WIT `0.1.N` additions (child run id accessor, object presign, ...) and WASI 0.3 / CM-async guests.
- The hermetic `ostro build`/`init`/`replay` CLI (Phase 10, CLI-03/04/05), time-skipping test kits (SDK-06) and thin clients (SDK-07).
- Go/other SDKs.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SDK-02 | Python SDK: `@workflow`, `@step`, ctx, effects, SDK-owned asyncio loop with gather/race; pinned componentize-py (`--stub-wasi`) | §Pattern 1 (OstroLoop over `effects.wait`, based on componentize-py's own bundled `poll_loop.PollLoop`), §Pattern 3 (step lock), §Pattern 5 (ambient patches), the validator change (componentize-py exports `exports`), packaging |
| SDK-03 | TS SDK: `workflow()`, `step()`, ctx, effects, SDK-owned job loop with Promise.all/race; pinned jco (no WASI imports) | §Pattern 2: **verified** on QuickJS and StarlingMonkey with a synchronously drained SDK promise + Rolldown `transform.target: "es2016"`, zero WASI imports. §Pattern 3, §Pattern 5, BigInt/UTF-8 pitfalls |
| SDK-04 | Reseed after pre-init: runs differ, replay reproduces; reproducible builds | §Pattern 4 (host-routed randomness satisfies D7 automatically), §Reproducible builds (measured: Python 191 B / QuickJS 12 B residual under a fixed-entropy build; StarlingMonkey 1.6 MB) |
| SDK-05 | Conformance suite passes for Rust, Python, TS | §Conformance architecture (JSON spec, journal projection, canonical JSON, engine-level + server-level legs), scenario catalogue |
| WF-06 | Start/await/detach children in any language; cancellation propagates | §Children bridge: **Phase 9 owns server-side child plumbing** (children are still "unsupported" after Phase 6). Pre-freeze PF-F1/F2/W7 |
| WF-10 | Saga helper runs compensations in reverse on failure | §Pattern 7: SDK-only helper in all three SDKs (Rust included), shielded, LIFO |
</phase_requirements>

## Summary

**Nothing in WIT 0.1 needs a shape change for Python or TypeScript.** The risky part, mapping a native async event loop onto a sync WIT whose `wait` blocks in the host, was verified in the S2 harness on both jco backends:
- **TS:** a sync export can drive `async/await` code to completion when the SDK installs its own promise implementation as `globalThis.Promise`, jco's Rolldown bundler lowers `async/await` to generators (`--bundle-config` with `transform.target: "es2016"`), and the export drains the SDK's job queue synchronously. Host imports work from inside those jobs. The build has zero WASI imports, and the same job count (11) ran on QuickJS and StarlingMonkey.
- **Python:** componentize-py ships a custom `asyncio.AbstractEventLoop` (`bundled/poll_loop.py`, `PollLoop`). The Ostro loop is the same design with `wasi:io/poll.poll` replaced by `effects.wait(interested_ops, any)`.

The WIT does need about ten **doc-comment** clarifications that SDK authors cannot infer from the code: the open-step rule, the exact `wait` semantics, u64/op-id conventions, string rules, the payload interop convention, retry/fuel portability, and the whole child-workflow contract. They must land in 05-05 before 05-20 pins the file sha.

**The biggest finding is not about the SDKs: child workflows are still unbridged.** Phase 5 keeps children "unsupported", and so does Phase 6 (06-14 P-18). Phase 9 must build the server side of WF-06: derive `StartChild`, journal `ChildCompleted` into the parent, map failures, propagate `cancel-op`, handle detached children, and cascade parent close. A D21-compliant child completion needs **two prost fields that Phase 5 should add before its format freeze** (PF-F1). Phase 5's continue-as-new also drops parent linkage, so a child that continues-as-new orphans its parent and breaks cancel propagation (PF-F2). Both are cheap pre-freeze and expensive post-freeze.

**Toolchains:** componentize-py **0.25.1** (2026-09-11, latest release, embeds CPython **3.14**), jco **1.42.0** (2026-11-13) with componentize-js **0.23.0** (StarlingMonkey) and the bundled **componentize-qjs 0.5.0** (`--backend quickjs`; crates.io has 0.6.0). Measured today in the S2 harness:

| | Python | TS QuickJS | TS StarlingMonkey |
|---|---|---|---|
| `.wasm` | 18.5 MB | **1.47 MB** | 12.0 MB |
| `.cwasm` | 32.9 MB | **4.1 MB** | 33.6 MB |
| RSS per live instance | 6.5 MB (S2) | **425 KB** | 2.44 MB |
| Instantiate + first call (p50, advisory: load avg ~17) | about 2 ms | **about 0.18 ms** | about 0.8 ms |

Warm-instantiation targets (< 10 ms Python, < 5 ms TS) are met by more than 5x in every configuration.

**Reproducibility:**
- Rust builds are reproducible.
- Under an LD_PRELOAD "fixed entropy" jail (fixed getrandom, wall clock and monotonic clock), Python's diff shrank from about 6 MB to **191 bytes**: build-time file mtimes in the snapshot, measured. The shim-module order shuffle disappeared. QuickJS shrank from 102 to **12 bytes**. StarlingMonkey still differed by 1.6 MB.
- So bit-reproducible Python needs a small upstream componentize-py change: a deterministic pre-init `WasiCtx`, `PYTHONHASHSEED=0` and normalised extracted-file mtimes. The fixed hash seed also matters for **replaying old journals on a rebuilt module** (str set-iteration order is baked per build).

**Primary recommendation:**
1. Fold the pre-freeze findings below into 05-01/05-02/05-05/05-08/05-09/05-11 now.
2. Build the TS SDK on jco `--backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi`, with an SDK-owned synchronous promise job loop.
3. Build the Python SDK on componentize-py 0.25.1 `--stub-wasi`, with an Ostro asyncio loop over `effects.wait`.
4. Route all language-native randomness to host `random-u64`/`random-bytes`, which makes D7 automatic and identical across languages.
5. Run a JSON-spec conformance suite whose cross-language comparison is a normalised journal projection plus canonical-JSON outcomes, and whose per-language check is strict replay identity.

## Pre-Freeze Findings (key output)

Status: Phase 5 is fully planned and **not executed**, so everything below can still be folded in. **No finding requires a WIT shape change.** Legend: **DOC** = doc comment in `runtime.wit` (05-05 Task 3, must precede 05-20); **FMT** = Phase 5 format/semantics change (prost fields, RunRecord, apply/derive behaviour); **SDK** = no Phase 5 change, recorded for Phase 9.

| ID | Kind | Finding | Where it lands |
|----|------|---------|----------------|
| **PF-F1** | **FMT** | **Child completion has no D21-compliant path.** Apply builds outbox `ChildCompleted{parent_run, child_run, op, status, result: PayloadRef}` from `CompleteRun.result`. The parent must journal `ChildCompleted{op, outcome: postcard(Result<OpOutput::Child(payload), Failure>)}` (kind 67), but apply may not decode, fetch or wrap a blob (D21; the BlobRef invariant). The fix is to **add `CompleteRun.parent_event: Option<PayloadRef>`** (next free field, 5), holding the submitter-encoded kind-67 event bytes built by the child's derive, and **`outbox ChildCompleted.event: Option<PayloadRef>`** (field 6) to carry them. The parent's apply journals it verbatim at `next_seq` (the 04-13 TimerFired pattern: Running → `InputAppended`, else make ready). When `event` is absent (hard stops built by apply: Terminated/TimedOut/cancel-before-start), parent apply encodes a no-user-data `Err(failure)` itself, which D21 permits. **Fallback if not pre-freeze:** a new outbox kind (tag 11, unallocated) plus a new intent (tag 19), which is clumsier because apply's automatic ChildCompleted emission would also need suppressing. | 05-01 (contract + goldens), 05-02 (`InboxMsg::ChildCompleted` may be removed; Phase 9 journals directly) |
| **PF-F2** | **FMT** | **Continue-as-new loses parent linkage.** 05-11 maps `RunContinuedAsNew` → `SendMessage(OutboxMsg::StartRun{continued_from})` + `CompleteRun{ContinuedAsNew}`, and 05-RESEARCH says the CAN StartRun has "no parent linkage". For a child, apply's `CompleteRun` currently emits `ChildCompleted(status=ContinuedAsNew)` to the parent (`apply/run.rs` ~L507). The parent would then see a meaningless completion, the successor's completion would be dropped, and the parent's cancel cascade would target the dead predecessor (the children row is keyed by the old run id). Fix: (a) the successor `StartRun.parent` = the predecessor's `ParentRef{run, op}` (field 12 exists); (b) `CompleteRun{ContinuedAsNew}` never emits `ChildCompleted`; (c) add **`RunRecord.continued_to: Option<RunId>`**; (d) inbound `CancelRun` to a `ContinuedAsNew` terminal run is **forwarded** to `continued_to` instead of being dropped as a state miss. | 05-02 (RunRecord field), 05-09/05-11 (derive + apply), 05-08 (CancelRun forward) |
| **PF-F3** | **FMT (Phase 9 may own)** | **Parent-close policy is undefined.** Apply cascades graceful `CancelRun` to children on cancel request (05-08) and on Terminate/TimedOut (05-09), but not when a parent reaches `Completed`/`Failed` with attached children still running. Those children run on, and their `ChildCompleted` hits a terminal parent and is dropped. Recommended: on every parent terminal, request graceful cancel of the attached children (Temporal's default is terminate; graceful is safer with D11 cleanup). No pre-Phase-9 log contains children, so Phase 9 can implement this without a format change, but **the WIT doc must state it now** (PF-W7). | WIT doc now; apply in Phase 9 (09-05) |
| PF-F4 | FMT | If PF-F1/F2 are not adopted pre-freeze, Phase 9 must use the post-freeze fallback (new outbox kind + intent from the unallocated ranges) and carry a RunRecord value-format migrator once Phase 8 trims logs. | RESERVATIONS fallback rows |
| **PF-F5** | **FMT (registry)** | **RESERVATIONS.md has no Phase 9 block.** Add one (docs-only, rule 2) before 05-01 runs:<br>- `CompleteRun.parent_event` = 5 and `outbox ChildCompleted.event` = 6 (if PF-F1 lands in P5, mark them planned P5);<br>- `RunRecord.continued_to` (05-02);<br>- spare `InvalidCode` and outbox/intent slots for the fallback;<br>- names: crate `ostro-conformance` (SDK conformance; Phase 12's replication suite should pick a different name), failpoint prefix `child::`, error kinds `child_terminated` and `sdk_protocol`, SDK dirs `sdks/python` and `sdks/ts`, package names `ostro` (PyPI, npm; both free on 2026-11-19). | RESERVATIONS.md now |
| **PF-W1** | DOC | **Open-step rule.** Between `begin-step` returning `execute` and `complete` (or drop), every journaled call (`begin-step`, all `effects.*`, `wait`, `patched`, `set-status`) is a protocol violation that fails the activation (`OPEN_STEP` in `replay/cursor.rs`). `context` reads (`now`, `random-*`, `uuid-v7`, `info`, `is-replaying`, `continue-as-new-suggested`) and `log.emit` are allowed, and randomness then comes from the step-local stream. This is the constraint that forces Python/TS SDKs to **serialise steps** (Pattern 3). | 05-05 (journal interface + `resource step`) |
| **PF-W2** | DOC | **`wait` semantics.**<br>- `any` returns **every** op in `ops` that has completed (at least one), ordered by the journal seq of each completion, not just the first.<br>- `all` returns all of them.<br>- Re-waiting an op that an earlier `wait` already returned returns it again.<br>- Duplicates in `ops` count once.<br>- `any` with an empty list is a protocol violation.<br>- An unsatisfied wait appends nothing and may suspend the activation (it evicts, or hot-parks only timers and in-flight effects).<br>- Each satisfied wait journals one `WaitResolved`, which consumes a seq. | 05-05 (`effects.wait`) |
| **PF-W3** | DOC | **op-id conventions.** Op ids are opaque u64s (today, the journal seq of the scheduling event). The `cancellation` op id is the reserved value 2^64−1 (`CANCELLATION_OP`), and the range `u64::MAX-15..` is host-reserved. JS bindings deliver u64 as `bigint`, and SDKs must never round-trip op ids through `Number` (2^64−1 is not representable). | 05-05 (`types.op-id`, `effects.cancellation`) |
| **PF-W4** | DOC | **Strings are UTF-8 Unicode scalar values.** Toolchains differ on lone surrogates: StarlingMonkey silently replaces them with U+FFFD (measured: `beginStep("bad\uD800name")` → `execute`), while QuickJS **traps** (measured). SDKs must reject such strings before calling the host, with a non-retryable `invalid_string`. | 05-05 (`types` header) |
| **PF-W5** | DOC | **Payload interop convention.** `application/json` payloads are UTF-8 JSON without a BOM, and empty `data` decodes as JSON `null` (the Rust SDK already does this). Other content types pass through untouched. Without this, Python/TS/Rust children cannot interoperate reliably. | 05-05 (`types.payload`) |
| **PF-W6** | DOC | **Step options portability.**<br>- `retry: none` means **no host retries**; SDK-level defaults such as the Rust macro's 3/1s/60s/2.0 are SDK conveniences that must be identical across SDKs.<br>- `max-attempts` counts the first attempt (0 and 1 mean no retry).<br>- A NaN, infinite or < 1 coefficient is treated as 1, and delays saturate.<br>- `fuel` counts Wasm instructions **of the guest runtime**. Budgets are not portable across languages: a Python step burns orders of magnitude more fuel than the same Rust step.<br>- `random-bytes` `len` ≤ 65536. | 05-05 (`types.retry-policy`, `step-options`, `context.random-bytes`) |
| **PF-W7** | DOC | **Child-workflow contract (`start-child`, `child-options`).**<br>- **Outcome mapping:** Completed → `ok(child(output))`; Failed → `err(<child's failure>)`; Cancelled → `err(cancelled)`; TimedOut → `err(timeout)`; Terminated → `err(non-retryable{kind:"child_terminated"})`.<br>- A child that continues-as-new is followed to the last run of the chain.<br>- **`cancel-op` on a child op** completes it with `err(cancelled)` (D5) **and** requests graceful cancellation of the child run and its descendants.<br>- **Detached:** no parent linkage. The child's `run-info.parent-run-id` is `none`, it is not cancelled with the parent, and the op completes as soon as the start is accepted with `child(payload{application/json, {"run_id": "<id>"}})`.<br>- **Attached children** receive a graceful cancel when the parent reaches any terminal status (PF-F3).<br>- Child run ids are derived deterministically from (parent run, op), so re-delivery never duplicates a child. | 05-05 (`effects.start-child`, `child-options`) |
| PF-W8 | DOC | **Run lifecycle contract for interpreted guests.**<br>- `run` is called once per activation on a fresh instance.<br>- The host may end an activation by trapping inside any import (eviction, fencing, cancel), so guest `finally` blocks and finalizers do **not** run at eviction, and guests must not depend on them.<br>- Imports trap if called during component initialisation (pre-init/Wizer snapshot), so module-level code must not touch `context`/`effects`.<br>- `describe` must be pure and return unique names. | 05-05 (`workflows` interface) |
| PF-W9 | DOC | `uuid-v7` returns lowercase canonical 8-4-4-4-12. `random-u64` and `random-bytes` share the ctx stream; `uuid-v7` uses its own stream. This is needed so the conformance suite can assert identical draws across SDKs. | 05-05 (`context`) |
| SDK-1 | SDK | D7 is satisfied by **routing language-native randomness to the host** (Python `random`, `os.urandom`, `uuid4`, `secrets`; JS `Math.random`, `crypto.getRandomValues`, `crypto.randomUUID`). The host swaps streams at step open/close, so no guest PRNG state needs saving or restoring. A seeded guest PRNG would silently diverge between live and replay, because step bodies are skipped on replay. | Phase 9 |
| SDK-2 | SDK | D11 maps to Python `contextvars` (Tasks copy the context, so `async with ctx.shielded():` covers child tasks) and to an **explicit shielded ctx** in TS (`ctx.shielded(async (s) => …)`). Neither SM nor QJS ships AsyncContext, so a depth counter is unsound across `await`. | Phase 9 |
| SDK-3 | SDK | `attempt` (F4): SDK docs only. | Phase 9 |
| SDK-4 | SDK | Drop: Python `with`; TS `using` plus SDK auto-dispose; QuickJS drops completed steps on its own (measured `step_drops: 1`), StarlingMonkey does not (0). | Phase 9 |
| ENG-1 | Engine (Phase 9) | `validate.rs` rejects componentize-py's extra bare export `exports` (`init(app-name, symbols, stub-wasi)`; S2 `imports.jsonl`, TODO at `REQUIRED_EXPORT`). Allow exactly that name with an exact type-shape check. | 09-03 |
| ENG-2 | Engine/server (Phase 9) | `engine.class` defaults to Rust and is **not exposed** in `[engine]` server config, so the pool reserves 3 core instances per instance. A server running Python modules exhausts `total_core_instances` after 3000/38 ≈ **78** live Python instances. Pools need mixed-language sizing (per-module core-instance count at validate time + instance-budget admission). | 09-03 |

**Verdict per question asked:**
- **async/await mapping:** works over the sync WIT (TS verified on both backends, Python by componentize-py's own PollLoop design); no WIT change.
- **Resource lifetimes without drop:** fine; the WIT already says drop is observability only.
- **String/bytes encodings:** doc only (PF-W4/W5).
- **Step-local RNG:** SDKs match it by host routing (SDK-1); doc already planned (D7).
- **attempt:** doc already planned (F4).
- **D11:** SDK-only (SDK-2).
- **Children:** need PF-F1/F2 format work plus PF-W7 doc. This is the only finding with format cost.

## Standard Stack

### Core

| Tool | Version (pin exactly) | Purpose | Why |
|------|----------------------|---------|-----|
| componentize-py | **0.25.1** (PyPI + crates.io, 2026-09-11; latest release; a canary of 2026-10-30 exists) | Python → component, `--stub-wasi`, CPython **3.14.0** (dicej fork, wasi-sdk-30) embedded | The only maintained CPython component toolchain. S2-validated: imports only `ostro:*`. **HIGH** |
| jco | **1.42.0** (2026-11-13) | TS bundling (Rolldown 1.3.3, TypeScript built in) + componentize driver | S2-validated. `--bundle-config` merges `transform` (verified in `dist/bundle.js`). **HIGH** |
| componentize-qjs (via `jco --backend quickjs`) | **0.5.0** as bundled by jco 1.42.0 (crates.io latest 0.6.0, 2026-11-07) | QuickJS-NG/rquickjs component runtime; flags `--backend-qjs-disable-async --backend-qjs-stub-wasi` | Measured today: 1.47 MB, 425 KB/instance, zero WASI imports, near-reproducible, auto-drop. **MEDIUM** (young: 0.x, one primary maintainer, but shipped by jco as an official backend) |
| componentize-js / StarlingMonkey | 0.23.0 / 0.5.x (2026-09-21 / 2026-11-13) | Alternative TS backend (`--disable all`) | Spec-complete (TextEncoder, crypto, structuredClone). Same SDK runs on it unchanged (verified). Keep as fallback backend. **HIGH** |
| wit-bindgen (Rust SDK) | 0.63.0 | Unchanged | Rust SDK is the reference implementation |
| Node.js | 24 LTS (local v24.13.0) | TS SDK build and unit tests (`node --test`, built-in TS type stripping) | No extra test deps |
| uv | local 0.7.15; CI `astral-sh/setup-uv` | Runs pinned componentize-py (`uv tool run --from componentize-py==0.25.1`), pytest, `uv build` | Already used by `spikes/s2-guests/build.sh` and ci-local |

### Supporting

| Library | Purpose | When |
|---------|---------|------|
| pytest (via `uv run --with pytest==8.*`) | Python SDK host-side unit tests with a fake bindings module | Wave 0 |
| `node:test` + `node:assert` | TS SDK unit tests (Node 24 strips types natively) | Wave 0 |
| serde_json (already in the workspace) | Conformance spec parsing, canonical JSON comparison | Conformance harness |
| Existing `ostro-engine::testkit`, `crates/ostro-engine/tests/common::guest_wasm`, ostro-server in-process harness, `ostro-crashtest` / the 05-18 failpoint binary | Engine-level conformance legs, server-level children/crash legs | Waves 3–4 |

### Alternatives Considered

| Instead of | Could use | Tradeoff |
|------------|-----------|----------|
| QuickJS backend | StarlingMonkey (jco default) | +spec completeness, +faster compute, +jco default. Costs: 2.4 MB/instance (5.7x), 33 MB AOT per version, never drops completed resources, snapshot differs by 1.6 MB even under a fixed-entropy jail (SC2 effectively unreachable). Same SDK works on both, so the switch is a build flag. |
| SDK-owned promise + async lowering | Native promises + StarlingMonkey event loop (export returns a Promise; measured: SM drives it to completion) | Needs a quiescence hook. `setTimeout(0)` traps under `--disable all` (measured), so it needs `--enable clocks`, which adds 6 WASI imports (allowed by the shim, but contradicts SDK-03), and it is SM-only. QuickJS `--sync` neither drains microtasks after a call (measured `step_completes: 0`) nor awaits returned promises (measured trap). |
| SDK-owned promise | Custom QuickJS runtime (`componentize-qjs --runtime`) exposing a `drainJobs()` builtin | Native async semantics, but Ostro would maintain a runtime build. Revisit only if lowering proves unworkable. |
| `--stub-wasi` + Python monkeypatches | Python without `--stub-wasi`, so the WASI shim supplies logical clocks and seeded random automatically | CPython's wasi-libc pulls `wasi:filesystem` (and likely sockets), which `validate.rs` rejects. Would need a selective-stub feature upstream. |
| Host-routed randomness | Guest PRNG reseeded per activation + save/restore at step boundaries | More code, per-language PRNG drift, and cross-language draws differ. Host call cost is about 1 µs. |
| Python `asyncio` | Bare coroutine scheduler without asyncio | Smaller snapshot, but SDK-02 promises `asyncio.gather`; users expect asyncio. Measure the snapshot growth (Wave 0) instead. |

**Installation (build-time only; nothing enters the server binary):**
```bash
# Python guest build (pinned)
uv tool run --from componentize-py==0.25.1 componentize-py \
  -d sdks/python/ostro/wit -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p . -p .venv-site app -o app.wasm
# TS guest build (pinned in sdks/ts/package.json: "@bytecodealliance/jco": "1.42.0")
npx --no-install jco componentize src/index.ts --wit node_modules/ostro/wit --world-name workflow \
  --backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi \
  --bundle-config node_modules/ostro/rolldown.ostro.mjs -o app.wasm
```

## Architecture Patterns

### Recommended layout
```
sdks/
├── rust/                      # existing; gains saga + random_f64 + conformance guest helpers
├── python/                    # PyPI "ostro" (pure-python wheel)
│   ├── pyproject.toml
│   ├── ostro/
│   │   ├── componentize-py.toml   # declares WIT dir + bindings module (componentize-py discovery)
│   │   ├── wit/               # copy of wit/ostro-runtime-0.1.0 (byte-identical, test-enforced)
│   │   ├── _host.py           # the ONLY module touching generated bindings (fakeable in tests)
│   │   ├── loop.py            # OstroLoop(asyncio.AbstractEventLoop)
│   │   ├── ctx.py ops.py step.py saga.py effects.py json.py errors.py
│   │   ├── _determinism.py    # patches installed at import (pre-init)
│   │   └── export.py          # registry + WitWorld/Workflows impl (describe/run)
│   └── tests/                 # pytest, host CPython, fake _host
├── ts/                        # npm "ostro" (ESM, ships .ts sources + d.ts)
│   ├── package.json  rolldown.ostro.mjs  wit/
│   ├── src/{promise,loop,ctx,ops,step,saga,effects,json,utf8,sha256,determinism,export}.ts
│   └── test/*.test.ts         # node --test
conformance/
├── spec/*.json                # language-neutral scenarios
├── guests/{rust,python,ts}/   # one module per language exporting conf_* workflows (+ v2/v3 variants)
crates/ostro-conformance/      # harness (dev-only, publish=false): spec runner, projection, comparator
```

### Pattern 1: Python OstroLoop (SDK-owned asyncio loop over `effects.wait`)
**What:** a subclass of `asyncio.AbstractEventLoop` modelled on componentize-py's bundled `poll_loop.PollLoop`, with `poll.poll(pollables)` replaced by one `effects.wait(sorted(interested_ops) [+ CANCELLATION], any)`.
**Algorithm:**
1. Run ready handles until the ready queue is empty. That is exact quiescence.
2. If the root future is done, return.
3. Otherwise collect "interested" ops: op futures that exist and are not done. A future is created when an `Op` is first awaited.
4. If there are none, the root awaits something that can never settle: raise `ostro.Deadlock` (non-retryable `sdk_protocol`).
5. Otherwise call `wait`, resolve futures **in returned order**, and loop.

**Rules:**
- **Effect calls are eager:** the op id is allocated at call time, exactly like Rust. **Waiting is lazy.**
- The wait list is sorted ascending, so it is deterministic and conformance-stable.
- `CANCELLATION` is included unless every interested op is shielded.
- After the root completes, remaining tasks never run.
- `time()` returns logical seconds (`context.now`).
- `call_at`/`call_later` map to durable `effects.sleep-until` ops, and `TimerHandle.cancel()` maps to `cancel-op`. So `asyncio.sleep`, `asyncio.timeout` and `wait_for` become durable and deterministic. Alternatively raise with a pointer to `ctx.sleep`; decide in 09-06, but mapping is friendlier.
- Every unsupported method (sockets, subprocess, executors) raises `NotImplementedError` with an Ostro message.

**Why it is safe:** asyncio's scheduling order is FIFO over `call_soon`, and every input to the loop comes from the journal, so the order is deterministic per module sha.

### Pattern 2: TS SDK-owned job loop (verified)
**What:** at module evaluation (pre-init) the SDK installs `SdkPromise` as `globalThis.Promise` and overrides `queueMicrotask`. jco's Rolldown lowers every `async`/`await` (user code and bundled `node_modules`) to generators with `transform.target: "es2016"` in the bundle config. The lowering helper does `new Promise(..)` and so picks up `SdkPromise`.

The export `run` is synchronous:
```
root = registry[name](ctx, input)    // SdkPromise
loop { drain(); if settled(root) break; ops = interested(); if none -> deadlock; completions = effects.wait(ops, any); resolve(completions in order) }
```
**Verified (2026-11-19, S2 harness):**
- `Promise.all([inner(1), inner(2)])`, `Promise.race([...])` and a host `beginStep` after several awaits all complete inside one sync export call.
- Output `{a:4,b:6,r:22}` and `jobsRun: 11`, identical on QuickJS and StarlingMonkey.
- QuickJS build imports only `ostro:*`.

**Ops are thenables:** `then()` registers interest (lazy wait). `Promise.all`/`race` call `then` from a job, which is deterministic. Op ids are `bigint` throughout.

### Pattern 3: Step serialisation (step lock), both SDKs
**Why:** PF-W1. `Promise.all([step("a", asyncFn), step("b", asyncFn)])` would call `begin-step(b)` while `a` is open, which is a protocol violation that fails the activation.

**Rules:**
- `step()` acquires an SDK-internal FIFO lock and calls `begin-step`. A `replayed` outcome returns immediately.
- On `execute`, the SDK runs the body. A sync body completes inline. An async body is driven under the lock, and other tasks keep running but any **journaled** SDK call they make is queued behind the lock (context reads are not).
- If the loop goes quiescent with a step open and no runnable jobs, the body awaited an Ostro op inside a step: complete the step `non-retryable{kind:"sdk_protocol", message:"step bodies cannot await durable operations"}`.
- The result is that step order equals call order, the same as Rust's sequential `run_step`, so cross-language command projections match.

### Pattern 4: Determinism surface (both SDKs; installed at SDK import = pre-init)

| Source | Python | TS |
|---|---|---|
| Randomness (D7 via host) | `random` module: rebind every `random.__all__` function to a `HostRandom(random.Random)` overriding `random()` and `getrandbits()`. Patch `random._urandom` and `os.urandom` → `random-bytes`; `uuid.uuid4` → `random-bytes(16)` with version bits; `secrets` via `os.urandom` | `Math.random`, `crypto.getRandomValues`, `crypto.randomUUID` → host |
| f64 from u64 (conformance-pinned) | `(u >> 11) * 2**-53` | same, with BigInt shift then Number |
| Wall time | `time.time/time_ns/monotonic/perf_counter`; `datetime.datetime` replaced by a subclass overriding `now/utcnow/today`; `datetime.date.today` | `Date.now`; `Date` subclass whose no-arg constructor uses ctx time; `performance.now` = logical monotonic |
| stdio | `sys.stdout/stderr` → `log.emit` (with `--stub-wasi` a raw write traps; tracebacks would mask errors) | `console.*` → `log.emit` |
| Finalizers | `gc.callbacks` guard: an SDK host call during a GC pass raises (finalizer timing differs live vs replay, because step bodies are skipped). `gc.freeze()` at `export()` time (pre-init) to keep snapshot pages clean (MEDIUM) | `WeakRef` → strong-ref shim; `FinalizationRegistry` → no-op (callbacks would fire at GC-dependent points) |
| Hash order | str `hash()`/set order baked per build: deterministic per sha, but **differs across rebuilds** until PYTHONHASHSEED is fixed at pre-init | Object/Map/Set order is spec-defined |
| JSON | `json.dumps(separators=(",", ":"), ensure_ascii=False, allow_nan=False)` with NaN/±inf → `null` (matching serde_json and JSON.stringify) | `JSON.stringify` (NaN → null measured on both backends) + an SDK UTF-8 codec (QuickJS has no TextEncoder, measured) |
| Strings | reject lone surrogates (PF-W4) | `isWellFormed()` is available on SM (measured `false` for a lone surrogate); verify QJS, else a manual scan |

### Pattern 5: Cancellation (D11) and shielding
- Python: unshielded op futures get `set_exception(ostro.Cancelled(...))` when the `CANCELLATION` completion arrives. `ostro.Cancelled` subclasses `Exception`, not `asyncio.CancelledError`, so gather and Task machinery are not triggered.
- TS: pending unshielded op promises are rejected with `CancelledError`.
- Cancellation is remembered: later unshielded waits reject immediately without a host call. Journal shapes therefore differ from Rust (Rust re-issues `wait([op, CANCEL])`); the projection normalises this.

### Pattern 6: Children server bridge (WF-06; Phase 9 owns it)
- **Derive** `ChildStarted` → `SendMessage(StartChild{StartRun{...}})` in the same `EventsAppended`:
  - `run_id` = deterministic(parent, op);
  - workflow/version: options, else the target's active version resolved from meta at derive time, with `module_sha`;
  - queue: options, else the workflow default;
  - input as a PayloadRef (blob above 256 KiB);
  - `parent: ParentRef{run, op}` unless detached;
  - fresh `rng_seed`; tenant inherited; traceparent propagated; `run_timeout_ms`.
- **Remove the "unsupported" path** for `ChildStarted` in `PartitionSink::unsupported_input` and `repair_unsupported`.
- **Child terminal:** the child's derive encodes the parent's kind-67 event into `CompleteRun.parent_event` (PF-F1). Parent apply journals it verbatim.
- `OpCancelled` on a child op → `SendMessage(CancelRun)` to the child.
- **Detached:** the sink appends `ChildCompleted{op, ok(child({"run_id"}))}` right after the flush, the same mechanism as today's "unsupported" completion.
- **Parent close** (PF-F3) and **CAN forwarding** (PF-F2).
- Cancel propagation down a TS → Python → Rust chain is then purely server-side (05-08 cascade at each level), and each SDK surfaces it through D11.

### Pattern 7: Saga helper (WF-10), all three SDKs
```
saga = ctx.saga()                       # Rust: Saga::new(&ctx)
saga.add(lambda: refund_step(order))    # compensation = a closure issuing steps/effects
...
except Exception as e:
    await saga.compensate()             # LIFO, sequential, inside ctx.shielded()
    raise
```
- Defaults: sequential reverse order; **shielded**, so a cancelled workflow still compensates; continue past a failing compensation and attach the failures to the re-raised error (`details`).
- Option `parallel=True` uses gather (later).
- No WIT or host support needed.
- Conformance asserts the journal order of compensation steps.

### Pattern 8: Conformance suite
**Spec (JSON, language-neutral):**
```json
{"id":"C-SIG-01","workflow":"conf_signals","input":{"n":2},
 "leg":"engine",
 "script":[{"input":"signal","name":"go","payload":{"v":1},"before_start":true},
           {"input":"signal","name":"go","payload":{"v":2}}],
 "expect":{"status":"completed","result":{"got":[{"v":1},{"v":2}]},
           "projection":["step:prepare","wait-signal:go#1","wait-signal:go#2","terminal:completed"]}}
```
**Comparison:**
1. **Per language (strict):** replaying the produced journal appends 0 events, and two runs with the same seed give byte-identical journals.
2. **Across languages:** a canonical-JSON outcome (sorted keys, JSON numbers compared as f64 when non-integral; floats like `1.0` vs `1` equal) plus a **normalised projection**:
   - drop `WaitResolved`, `ts_nanos`, command hashes (HTTP body or child-input byte formatting differs per language), `fuel_used`;
   - rename op ids to scheduling ordinals (`#1, #2, …`);
   - keep the step names, effect kinds and canonical args, the recorded outcomes as canonical JSON, the patch markers, the status JSON and the terminal.
   - Inputs are compared as a multiset keyed by ordinal.
3. **Cross-language value checks:** `ctx.random_u64` draws, `random_f64`, `uuid_v7` and `idempotency_key("x")` must be **equal across the three SDKs** for the same seed and run id. This is a strong probe of the host-routed design.

**Legs:**
- **engine** (ostro-engine testkit; inputs fed in order like `sdk_cancellation.rs`): most scenarios, fast.
- **server** (in-process ostro-server): children, continue-as-new, await tokens via the API, signals via the API, saga with real effects (Phase 6 mock HTTP).
- **crash** (failpoint `ostro` binary, kill -9): TS → Python → Rust chain, exactly one child per level, final result correct.

### Anti-Patterns to Avoid
- **Native `async` in a TS sync export without lowering:** never progresses on QuickJS, and on StarlingMonkey it leaves microtasks to run *after* `run` returns. Measured: SM drained a post-return microtask that called `beginStep` (`step_completes: 1`). That is how commands get journaled after the terminal.
- **Seeding a guest PRNG once per activation:** step bodies are skipped on replay, so the PRNG position drifts and values silently diverge. Use host routing.
- **Op ids as JS `Number`.**
- **`Promise.race` loser auto-cancel:** the SDK cannot know intent. Expose `ctx.cancel(op)` (Rust parity).
- **Relying on `try/finally` for cleanup at eviction:** eviction is a trap.
- **Default `json.dumps`:** it emits `NaN` (invalid JSON) and `", "` separators.

## Don't Hand-Roll

| Problem | Don't build | Use instead | Why |
|---------|-------------|-------------|-----|
| Python event loop skeleton | A loop from scratch | Fork the structure of componentize-py `bundled/poll_loop.py` (`PollLoop`, Apache-2.0) | Already proven against CPython 3.14 asyncio internals (`Handle`, `Task`, `_set_running_loop`) |
| TS async lowering | Own transpiler | jco's Rolldown via `--bundle-config` `{transform:{target:"es2016"}}` | Verified; oxc's async-to-generator handles user code and `node_modules` |
| JS bundling of TS | esbuild step | jco built-in bundling (automatic for TS) | jco 1.42 bundles TS itself |
| WIT binding codegen | Hand-written ABI glue | componentize-py generated `wit_world` / jco-generated imports | ABI correctness |
| Determinism virtualisation in Rust guests | SDK patches | The existing deterministic WASI shim | Already there (std HashMap, SystemTime) |
| Step retry/backoff | SDK retry loops | Host `retry-policy` (`retry-now` + `StepAttemptFailed`) | Attempts must come from the journal (Pitfall 8 in 04-04) |
| Child dedupe | SDK child-id bookkeeping | Server deterministic child run id + outbox exactly-once | Survives crashes |
| Canonical JSON in the harness | A new crate | serde_json `Value` (BTreeMap maps) + number normalisation (~40 lines in test code) | No deny churn |

**Key insight:** interpreted runtimes hide non-determinism in four places: snapshot entropy, finalizers, hash order and microtask scheduling. Each has to be pinned by construction (host routing, an SDK-owned scheduler, deterministic pre-init), never by convention.

## Common Pitfalls

### Pitfall 1: Commands after the terminal (TS/StarlingMonkey)
**What goes wrong:** leftover jobs run after the workflow settled and call the host. **Avoid:** after the root settles, mark the SDK closed; every journaled entry point throws without calling the host. **Detect:** a conformance check that the last guest command is the terminal.

### Pitfall 2: Pool exhaustion with Python modules (ENG-2)
**What goes wrong:** about 78 live Python instances at default config, then instantiate errors. **Avoid:** size `total_core_instances` from per-module core-instance counts (38 for componentize-py 0.25.1; measure QJS) and admit activations by core-instance weight. **Detect:** a test with 100 concurrent parked Python runs.

### Pitfall 3: Deploy rejects every Python module (ENG-1)
`validate.rs` rejects the `exports` export. Fix in 09-03 before any Python e2e.

### Pitfall 4: Snapshot entropy breaks cross-rebuild replay
**What goes wrong:** a rebuilt Python module (patch release) iterates `set[str]` in a different order, so commands diverge and NonDeterminism fires, or worse, data silently differs. **Avoid:** fix PYTHONHASHSEED at pre-init (upstream patch), and lint/docs: never feed unsorted sets of str into commands. **Detect:** conformance C-REBUILD-01, which replays a journal against an independently rebuilt module.

### Pitfall 5: `print()` traps
With `--stub-wasi`, stdout is a trap stub. The SDK must swap `sys.stdout/stderr` at import, before user code, so the swap lands in the snapshot. Same for TS `console`.

### Pitfall 6: Lone surrogates and toolchain divergence
PF-W4: SM replaces, QJS traps. Validate in the SDK.

### Pitfall 7: Step bodies awaiting durable ops
Deadlock or protocol trap. The step-lock quiescence check turns it into a clear non-retryable error (Pattern 3).

### Pitfall 8: Fuel budgets copied from Rust
Python burns far more fuel per logical operation. Do not port Rust step `fuel` defaults; measure per language (09-13), and document PF-W6.

### Pitfall 9: Native promises from engine APIs (TS)
`crypto.subtle.digest` (SM) returns a native promise that never settles inside a sync export under the SDK loop. Ship a sync sha256 (needed anyway for P-11 on QJS), and turn "quiescent with nothing interested" into the `Deadlock` error.

### Pitfall 10: Compile memory in tests
About 0.5 GB and 1 s per Python/TS module AOT compile. The conformance harness must compile each module once (`Runtime::prepare_cached`) and serialise compiles. GitHub runners have 7 GB.

### Pitfall 11: First-call cost per activation
Python's first export call is about 0.9 ms (lazy init) on every fresh instance. Fine for the targets, but publish it next to instantiate p50 (SC5 asks for warm instantiation; NFR-2.4 counts start-to-first-step).

## Code Examples

### Python loop core (shape; based on componentize-py `poll_loop.PollLoop`)
```python
# Source: github.com/bytecodealliance/componentize-py bundled/poll_loop.py (PollLoop.run_until_complete), adapted
class OstroLoop(asyncio.AbstractEventLoop):
    def run_until_complete(self, coro):
        root = asyncio.ensure_future(coro, loop=self)
        asyncio.events._set_running_loop(self)
        try:
            while not root.done():
                while self._ready:                      # exact quiescence: drain ready handles
                    h = self._ready.popleft()
                    if not h._cancelled:
                        h._run()
                if root.done():
                    break
                ops = sorted(op for op, fut in self._ops.items() if not fut.done())
                if not ops:
                    raise Deadlock("workflow awaits something no Ostro op can complete")
                if self._needs_cancellation_op():
                    ops.append(CANCELLATION_OP)        # 2**64 - 1, sorts last anyway
                for c in _host.wait(ops, any=True):     # completions in journal order
                    self._complete(c)                   # set_result / set_exception (Cancelled)
            return root.result()
        finally:
            asyncio.events._set_running_loop(None)
```

### TS export driving the SDK job loop (verified shape)
```ts
// Source: research probe sdkloop/probe.ts (S2 harness, QuickJS + StarlingMonkey), generalised
export function run(name: string, input: Payload): RunResult {
  const root = registry.get(name)!(ctx, decode(input)) as SdkPromise<unknown>;
  for (;;) {
    jobs.drain();                                   // SDK-owned queue, synchronous
    if (root.settled) break;
    const ops = interestedOps();                    // bigint[], ascending
    if (ops.length === 0) throw sdkProtocol("deadlock: awaited a non-Ostro promise");
    for (const c of effects.wait(withCancellation(ops), "any")) resolveOp(c);
  }
  closed = true;                                    // Pitfall 1
  return toRunResult(root);
}
```
```js
// rolldown.ostro.mjs, passed via `jco componentize --bundle-config` (merged into jco's Rolldown config)
export default { transform: { target: "es2016" } };
```

### P-11 key (must be byte-identical in three languages)
```python
def idempotency_key(run_id: str, name: str) -> str:
    h = hashlib.sha256(b"ostro/idem/v1/name" + run_id.encode() + b"\x00" + name.encode()).hexdigest()
    return "ostro-" + h[:32]
```

## Reproducible Builds (SC2 / SDK-04)

Measured 2026-11-19 (3 builds each; "jail" = research-only LD_PRELOAD shim with fixed `getrandom`/`getentropy`/`syscall(SYS_getrandom)`, fixed CLOCK_REALTIME and a deterministic monotonic counter):

| Toolchain | Plain builds | Under jail | Remaining cause |
|---|---|---|---|
| Rust | identical (S2) | n/a | none |
| Python (componentize-py 0.25.1) | ~6 MB data-section diff + shim-module order shuffle (S2) | **191 bytes**, same length, **order shuffle gone** | 4-byte Unix timestamps differing by the build gap, i.e. **mtimes of files componentize-py extracts to tempdirs** (stdlib/bundled), cached by importlib. Read through cap-std/rustix raw syscalls, so LD_PRELOAD cannot fix them |
| TS QuickJS | 102 bytes | **12 bytes** | one 8-byte value ×2; source unidentified (LOW) |
| TS StarlingMonkey | ~2.1 MB | **1.6 MB, lengths differ** | SpiderMonkey/Wizer internals; not addressable by the jail |

**Recommendation:**
1. Upstream a small componentize-py PR (its 0.22.1 already made binding generation deterministic, so maintainers accept this class of fix): pre-init `WasiCtxBuilder` with fixed `secure_random`/`insecure_random`/`insecure_random_seed`, fixed wall and monotonic clocks, env `PYTHONHASHSEED=0` and `PYTHONDONTWRITEBYTECODE=1`, extracted files touched to a constant mtime, and BTreeMap/IndexMap wherever RandomState still leaks.
2. Until it ships, Phase 9's build script uses a pinned fork. Building componentize-py from source downloads CPython `v3.14.0-wasi-sdk-30` and needs wasi-sdk: heavy, so cache it in CI. If even that is too heavy, record SC2 for Python as `human_needed` with the residual documented.
3. Ostro's build wrapper stages user sources with `SOURCE_DATE_EPOCH` mtimes.
4. For QuickJS, file an issue with componentize-qjs, and test whether `--module-root` mtimes or a time-seeded `Math.random` state explain the 12 bytes.
5. StarlingMonkey reproducibility is out of reach, which is another reason for QuickJS as the default.

Replay safety is unaffected either way: replay always uses the deployed artefact.

## Warm Instantiation, Memory, Pool, AOT

| Metric | Python | TS QuickJS | TS SM | Source |
|---|---|---|---|---|
| steady instantiate p50 (quiet) | 440–454 µs | not measured quiet | 10–19 µs | S2 confirm |
| instantiate p50 / first call p50 (load avg ~17, advisory) | 660–709 / 1280–1370 µs | 75–78 / 107–110 µs | 15–29 / 788–791 µs | this research |
| RSS per live instance (n=50) | 6.5 MB (trivial guest) | **425 KB** | 2.44 MB | S2 / this research |
| declared memory (pool floor) | 13.2 MB | 1.44 MB | 9.5 MB | S2 / this research |
| core instances per instance | 38 | **unmeasured (Wave 0)** | 3 | S2 |
| `.cwasm` / compile | 32.9 MB / ~0.85 s / 470 MB | 4.1 MB / ~0.8 s / 97 MB | 33.6 MB / ~1.1 s / 532 MB | S2 / this research |

- SC5 targets (< 10 ms Python, < 5 ms TS) hold with a wide margin even under load.
- **Re-measure with the real SDKs** (asyncio + json + dataclasses + datetime imports grow the Python snapshot; S2 asked for this) on a quiet machine, and publish `docs/sdk-resources.md` with idle RSS per language and backend (09-13).
- Pooling barely helps Python (S2). Size the pool by guest mix (ENG-2).

## State of the Art

| Old approach | Current approach | When | Impact |
|---|---|---|---|
| componentize-py PollLoop over `wasi:io/poll` (p2) | `componentize_py_async_support` for CM-async/WASI p3 exports | 2026 (p3 examples, 0.22.0) | Ostro stays on the sync world; use the PollLoop pattern, not p3 async support |
| componentize-js only (SpiderMonkey) | jco `--backend quickjs` / `perry`, componentize-js mid-rewrite to Rust/wit-dylib | 2026 | Backend choice is now a flag; QuickJS is component-native and small |
| esbuild before componentize | jco bundles TS via Rolldown (1.3.3) | jco 1.4x | The `--bundle-config` hook enables the async lowering |
| `HostStep::drop` for cleanup | Drop = observability only | S2 / 01-08 | SDK RAII is still recommended |

**Deprecated/outdated:** Javy (core modules, stdin/stdout); cargo-component (stale); relying on componentize-js weval AOT (`--aot`) for determinism (untested here, and it adds build variance).

## Open Questions / Decisions to Confirm

1. **D-P9-1: TS backend.** Recommended QuickJS default, StarlingMonkey as a supported alternative built by the same SDK, with a conformance smoke subset on SM. *Alternative:* SM default (spec completeness), giving up density and SC2. USER REVIEW.
2. **D-P9-2: `asyncio` timers.** Map `call_later/call_at` to durable sleeps (recommended) or raise. Planner discretion.
3. **D-P9-3: SC2 path for Python.** Upstream PR + pinned fork (recommended), or accept a documented residual (`human_needed`). USER REVIEW.
4. **D-P9-4: Pre-freeze adoption.** PF-F1/F2 into Phase 5 (recommended), or post-freeze fallback ordinals. Orchestrator decision **before 05-01 executes**.
5. **QuickJS core-instance count and pool floor:** unmeasured; Wave 0 (`s2-harness memory`/pool-limits on the QJS build).
6. **Python snapshot growth with asyncio:** unmeasured; Wave 0.
7. **jco `result<…>` export mapping from a sync export** (throwing vs returning an err value) on QJS: check in Wave 0.
8. **QuickJS `String.prototype.isWellFormed`:** unverified; Wave 0.
9. **Rolldown lowering coverage** (async generators / `for await`, top-level await in deps): verify in Wave 0 with a fixture.

## Proposed Plan Breakdown (waves / deps)

Precondition: Phase 6 complete (effects bridge). Phase 5 has absorbed PF-F1/F2/F5 and PF-W1..W9 (otherwise 09-04/05 use the fallback ordinals).

| Plan | Wave | Depends | Content | Reqs |
|------|------|---------|---------|------|
| 09-01 | 1 | — | Contract + Wave 0: RESERVATIONS check, a STATE decisions copy (D-P9-1..4), `conformance/spec` schema + scenario catalogue (empty bodies), harness crate skeleton, pytest/node:test scaffolds, a check that `sdks/{python,ts}/wit` is byte-identical to `wit/ostro-runtime-0.1.0` | all |
| 09-02 | 1 | — | Toolchain spike → `docs/spikes/S9-polyglot.md`: real-WIT Python OstroLoop and TS SdkPromise prototypes on the **real engine** (testkit), QJS vs SM measurements (core instances, memory floor, instantiate, first call), the async-lowering fixture, `result` export mapping, surrogate checks, snapshot growth with asyncio, the reproducibility matrix. Decides D-P9-1/2 | SDK-02/03/04 |
| 09-03 | 2 | 09-02 | Engine: allow the componentize-py `exports` export (exact shape); per-module core-instance count at validate time; mixed-language pool sizing and admission; `[engine]` config exposure; per-language fuel notes | SDK-02/03 |
| 09-04 | 2 | 09-01 | Server children (1/2): derive `StartChild` (deterministic id, version/queue/module resolution, ParentRef, detached), remove "unsupported" for children, D21 `ChildCompleted` journaling in the parent, failure mapping | WF-06 |
| 09-05 | 3 | 09-04 | Server children (2/2): `cancel-op` on child → CancelRun, detached completion, parent-close cascade, CAN linkage forwarding (if not in P5), failpoints `child::*` | WF-06 |
| 09-06 | 2 | 09-02 | Python SDK core (Patterns 1, 3, 4, 5), registry/export, JSON/errors, host unit tests with fake `_host`, guest build script | SDK-02/04 |
| 09-07 | 2 | 09-02 | TS SDK core (Patterns 2, 3, 4, 5), SdkPromise conformance tests (ordering, adoption, all/race/allSettled/any), UTF-8, sha256, BigInt ops, build config, node tests | SDK-03/04 |
| 09-08 | 3 | 09-06, 09-07 | Effects builders (Phase 6 parity: http/sql/object + `idempotency_key` P-11 vectors shared across 3 SDKs) and saga helper in Python, TS **and Rust** (+ `random_f64` in Rust) | WF-10, SDK-02/03 |
| 09-09 | 3 | 09-01 | Conformance harness: spec runner, projection, canonical JSON comparator, engine leg, Rust conformance guest + v2/v3 variants, the Rust pass | SDK-05 |
| 09-10 | 4 | 09-08, 09-09, 09-03 | Python + TS conformance guests; engine-leg pass in all three languages; fix SDK deviations | SDK-05 |
| 09-11 | 4 | 09-05, 09-10 | Server/crash legs: TS → Python → Rust chain under kill -9 (failpoint binary), cancel propagation to every descendant, saga e2e with mock HTTP, signals/tokens via API | WF-06, WF-10, SDK-05 |
| 09-12 | 5 | 09-06, 09-07 | Reproducible builds (deterministic build scripts, source staging, upstream PR / pinned fork per D-P9-3), SC2 check; packaging (`uv build` wheel+sdist, `npm pack`, dry-run publish), CI legs (setup-node 24, setup-uv, cached toolchains) in ci.yml + ci-local steps | SDK-04 |
| 09-13 | 5 | 09-10 | Quiet-machine measurements with the real SDKs: warm instantiate p50, first call, idle RSS per language/backend, pool sizing; `docs/sdk-resources.md`; SC5 gate (WSL2 advisory, D15) | SDK-02/03 |
| 09-14 | 6 | all | Phase gate: verification, SDK READMEs (frozen semantics incl. D6/D7/D11/F4/PF-W*), STATE/REQUIREMENTS by hand, human_needed list (publish, remote CI, upstream PR status) | all |

Parallelism: Phase 9 runs alongside Phases 8/10/11.
- Shared files: `crates/ostro-server/src/{derive,sink}.rs` (09-04/05 vs Phase 8 triggers), `validate.rs`/`config.rs` (09-03), RESERVATIONS, ci-local/ci.yml.
- Every plan carries the Phase 6-style "start from clean file, append-only" rule.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | cargo test (Rust 1.100, existing); pytest 8 via `uv run --with pytest` (Python SDK host tests); `node --test` (Node 24, TS SDK host tests) |
| Config file | none yet: `sdks/python/pyproject.toml [tool.pytest]` and `sdks/ts/package.json` scripts are created in Wave 0 (09-01) |
| Quick run command | `uv run --with 'pytest==8.*' pytest sdks/python/tests -q && node --test sdks/ts/test/ && cargo test -p ostro-conformance --test engine_leg -- --test-threads=2` |
| Full suite command | `bash scripts/ci-local.sh` (gains steps: "polyglot guests build", "python sdk tests", "ts sdk tests", "conformance engine+server legs") + `cargo test -p ostro-conformance --test crash_leg -- --ignored` (kill -9 leg) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| SDK-02 | OstroLoop quiescence, gather/race ordering, step lock, D11, patches | unit (host CPython, fake `_host`) | `uv run --with 'pytest==8.*' pytest sdks/python/tests -q` | ❌ Wave 0 |
| SDK-02 | Python guest builds, validates (incl. `exports`), runs on engine | integration | `cargo test -p ostro-engine --test py_guest --locked` | ❌ Wave 0 |
| SDK-03 | SdkPromise semantics, job-loop quiescence, BigInt ops, UTF-8/surrogates, sha256 vectors | unit (Node) | `node --test sdks/ts/test/` | ❌ Wave 0 |
| SDK-03 | TS guest (QJS) zero WASI imports, runs on engine; SM smoke | integration | `cargo test -p ostro-engine --test ts_guest --locked` | ❌ Wave 0 |
| SDK-04 | runs differ in `ctx.random`/native random; replay reproduces; cross-language draws equal | conformance | `cargo test -p ostro-conformance --test engine_leg c_rand` | ❌ Wave 0 |
| SDK-04 | same source → same sha (Rust always; Python/TS per D-P9-3) | build check | `bash sdks/scripts/repro-check.sh` (3 builds, compare sha256) | ❌ Wave 0 |
| SDK-05 | full catalogue in 3 languages: projection + canonical outcome equal; replay appends 0 events | conformance | `cargo test -p ostro-conformance --test engine_leg --test server_leg` | ❌ Wave 0 |
| WF-06 | attached/detached children, failure mapping, cancel-op propagation, CAN chain, parent close | integration (server) | `cargo test -p ostro-server --test children --locked` | ❌ Wave 0 |
| WF-06 | TS → Python → Rust chain under kill -9; cancel reaches every descendant | crash (`#[ignore]`) | `cargo test -p ostro-conformance --test crash_leg -- --ignored` | ❌ Wave 0 |
| WF-10 | compensations LIFO on failure; still run when cancelled (shielded) | conformance | `cargo test -p ostro-conformance --test engine_leg c_saga` | ❌ Wave 0 |
| SC5 | warm instantiate p50 < 10 ms Py / < 5 ms TS; idle RSS published | measurement (`#[ignore]`, quiet machine, advisory on WSL2) | `cargo test -p ostro-conformance --test resources -- --ignored --nocapture` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** the quick run command for the touched SDK, plus `cargo test -p <touched crate>`.
- **Per wave merge:** `bash scripts/ci-local.sh --no-musl`.
- **Phase gate:** full `ci-local.sh` green + crash leg + resources measurement recorded before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `sdks/python/{pyproject.toml,tests/conftest.py}`: fake `_host` fixture recording calls and returning scripted completions.
- [ ] `sdks/ts/{package.json,test/helpers.ts}`: fake host bindings.
- [ ] `crates/ostro-conformance/` skeleton (`publish = false`), spec schema, `tests/{engine_leg,server_leg,crash_leg,resources}.rs` stubs.
- [ ] A polyglot guest build helper (like `tests/common::guest_wasm`): build once per process with uv/npm, cache by source hash under `target/polyglot-guests`.
- [ ] ci-local + ci.yml steps for Node 24 / uv (actionlint covers ci.yml); the toolchain presence check fails loudly, never skips.
- [ ] `sdks/scripts/repro-check.sh`.

## Sources

### Primary (HIGH confidence)
- Local measurements, 2026-11-19, S2 harness (wasmtime 50.0.2) with scratch probes only (no repo code changed): QJS/SM imports, probes, instantiate, memory; microtask/event-loop behaviour (sync export, promise-returning export, setTimeout under `--disable all`, post-return microtask drain); SDK-promise + Rolldown lowering on both backends; surrogate handling; reproducibility under a fixed-entropy jail.
- Repo code at `66be068`: `wit/ostro-runtime-0.1.0/runtime.wit`, `crates/ostro-engine/src/{replay/{wait,cursor,rng}.rs,host/{context,journal,state}.rs,host/wasi/*,validate.rs,config.rs,retry.rs}`, `crates/ostro-types/src/{journal,command}.rs`, `crates/ostro-partition/src/apply/{run,inbound}.rs`, `crates/ostro-server/src/{sink,config}.rs`, `sdks/rust/ostro-sdk/src/{ops,step,ctx}.rs`.
- `.planning/` ROADMAP, REQUIREMENTS, PROJECT, STATE, RESERVATIONS; 05-DECISIONS (D1–D21, F1–F8); 05-05/05-06/05-09/05-11 plans; 06-DECISIONS (P-11, P-18), 06-09/06-14 plans; `docs/spikes/S2-guest-toolchains.md` + `data/S2/*`.
- GitHub API releases: bytecodealliance/componentize-py (v0.25.1 2026-09-11, canary 2026-10-30), jco (jco-v1.42.0 2026-11-13), ComponentizeJS (0.23.0 2026-09-21), StarlingMonkey (v0.5.1 2026-11-13).
- componentize-py source (main): `src/lib.rs` (pre-init WasiCtx env, `add_to_linker`, `Ctx`), `src/stubwasi.rs`, `build.rs` (CPython `v3.14.0-wasi-sdk-30`), `bundled/poll_loop.py`, `bundled/componentize_py_types.py`.
- PyPI JSON (componentize-py 0.25.1; `ostro` free), npm registry (jco 1.42.0, componentize-js 0.23.0 deps; `ostro` free), crates.io (componentize-py 0.25.1, componentize-qjs 0.6.0).
- Installed jco 1.42.0: `jco componentize --help`, `dist/bundle.js` (transform merge), `node_modules/componentize-qjs` 0.5.0 README.

### Secondary (MEDIUM confidence)
- [componentize-qjs README (docs.rs 0.4.0)](https://docs.rs/crate/componentize-qjs/0.4.0/source/README.md): `--sync`, `--stub-wasi`, Wizer pre-init.
- [wasmCloud community meeting 2026-03-18](https://wasmcloud.website.cncfstack.com/community/2026-03-18-community-meeting/): componentize-qjs on QuickJS-NG; componentize-js rewrite to Rust/wit-dylib.
- [componentize-py GitHub](https://github.com/bytecodealliance/componentize-py) README.

### Tertiary (LOW confidence)
- The cause of the 12-byte QuickJS residual; whether Wasmtime's allocation strategy affects `.cwasm` compatibility (relevant only if two engines are considered for ENG-2).

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH (versions from registries/APIs; behaviour measured).
- Architecture: MEDIUM-HIGH. The TS loop was verified end-to-end in the harness (not yet in the Ostro engine). The Python loop rests on componentize-py's own PollLoop pattern (not yet run against Ostro's WIT). Children design comes from code reading.
- Pre-freeze findings: HIGH for PF-F1/F2/W1/W2/ENG-1/ENG-2 (from code); MEDIUM for the PF-W7 outcome-mapping choices (recommendations).
- Pitfalls: HIGH for measured ones (surrogates, microtask drain, setTimeout, print with stub-wasi); MEDIUM for the gc/finalizer guidance.
- Reproducibility: HIGH for the measurements; MEDIUM for the attribution (mtimes).

**Research date:** 2026-11-19
**Valid until:** 2026-12-19. jco/componentize-qjs move fast (componentize-qjs released 3 versions in 2 months); re-check pins at 09-02.
