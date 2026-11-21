---
phase: 07-workers-placement-flow-control
plan: 22
subsystem: phase gate (docs, CI, reservations, validation) + Phase 7 follow-up fixes
tags: [gate, ci, docs, reservations, tls, workers, flow-control, cli]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-01..07-21 (all SUMMARYs incl. post-plan fixes fix-0705 and fix-0721 / D-W28)"
provides:
  - "docs/workers.md, docs/flow-control.md, docs/tls.md; docs/03 §5.3/§8 and docs/02 NFR-3.5 corrected"
  - "ci-local + ci.yml Phase 7 step; optional workflow_dispatch jobs acme-pebble and keda-kind"
  - "RESERVATIONS: Cmd 20/21 and RejectReason 15 exists (P7)"
  - "07-VALIDATION gate results, human_needed, follow-ups with owners; 07-DECISIONS spike outcomes"
  - "Follow-up fixes: ACME clock injection, explicit TLS profile, bound join port, CLI https, run pause mark, revoked-worker exit, files/acme join, one Wasmtime constant, plain piped logs, refused StartEffect log"
affects: [phase-7 verification, phase-7 gap closure, 08-*, 10, 11, 12, 13]
tech-stack:
  added: []
  patterns: ["CLI TLS: platform roots or --ca-file only", "ServerOptions carries the binary's TLS profile"]
key-files:
  created:
    - docs/workers.md
    - docs/flow-control.md
    - docs/tls.md
  modified:
    - crates/ostro-server/src/p7.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro/src/main.rs
    - crates/ostro-worker/src/{session,join,lib}.rs
    - crates/ostro-cli/src/client.rs
    - crates/ostro-api/src/{rest,grpc,domain,phase7,fake}.rs
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - .planning/RESERVATIONS.md
key-decisions:
  - "Fixed the small follow-ups in this plan at the orchestrator's request (beyond the plan's docs-only files_modified); dispatcher changes were left for gap closure"
  - "A worker whose re-join is refused with a 4xx exits (exit 1) instead of retrying"
  - "--ca-hash against a files/acme listener: one retry with the platform verifier before any request byte; the response CA must still match the pin (D-W20 as written)"
  - "ostro-cli enables reqwest's rustls feature (already enabled by ostro-worker/ostro-server; Cargo.lock unchanged)"
  - "G1 (throttled first activation starves a re-activation on the same partition) recorded as a BLOCKING gap, not fixed here"
requirements-completed: []
metrics:
  duration: "~5h (gate 2 h 05 min + proptest 26 min + reruns)"
  completed: 2026-11-21
---

# Phase 7 Plan 22: PHASE-7 GATE Summary

**Phase 7 documentation, CI sync, reservations flip and validation record are done. Thirteen follow-ups from the Phase 7 SUMMARYs are fixed. The full ci-local gate (with musl) ran once: 21 of 24 steps passed. Two of the three red steps were fixed and re-verified: an actionlint warning in the new CI job, and an ext4 inode-reuse assumption in a test. The third is a real dispatcher bug, recorded as BLOCKING gap G1: a throttled first activation starves a re-activation on the same partition.**

## Commits

| Hash | Message |
|------|---------|
| b43f5e5 | fix(07-22): close the Phase 7 seam follow-ups (ACME clock, TLS profile, join port, clock seam) |
| ba5da99 | fix(07-22): worker follow-ups (refused re-join exits, files/acme join, one Wasmtime constant, logs) |
| 454bbe0 | feat(07-22): pause mark on GET /v1/runs/{id} (OPS-01 observability) |
| 80fade4 | feat(07-22): CLI reaches https:// servers (platform roots or --ca-file) |
| 323f4e8 | test(07-22): sound lease-gauge bound in flow_control, every terminal status in is_terminal |
| 66a50bd | ci(07-22): Phase 7 suites in ci-local/ci.yml, optional Pebble and KEDA jobs |
| 309e7db | docs(07-22): flip Phase 7 reservations |
| 9212095 | ci(07-22): keda-kind burst loop variable unused (actionlint SC2034) |
| 45d73c6 | test(07-22): worker_e2e module_cache tolerates ext4 inode reuse |
| (this) | docs(07-22): Phase 7 docs, validation results and gate record |

## Gate table (`bash scripts/ci-local.sh` with musl, at 309e7db, 2 h 05 min)

```text
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  2s
cargo clippy -D warnings             PASS  6s
cargo build (warning-free)           PASS  77s
sdk workspace fmt + clippy           PASS  20s
sdk workspace test + wasm build      PASS  24s
guests build (wasm32-wasip2)         PASS  0s
order-saga example                   PASS  21s
external: OSTRO_TEST_PG_URL          SKIP  -
external: OSTRO_TEST_MYSQL_URL       SKIP  -
external: OSTRO_TEST_S3_URL          SKIP  -
cargo test                           FAIL  514s
semantics sleepers (serial)          PASS  22s
failpoint list sync (ci.yml)         PASS  0s
failpoint suites                     PASS  4670s
server campaign smoke (20 kills)     PASS  786s
phase 7 failpoint legs + manifests   PASS  60s
cargo deny check                     PASS  2s
check-vendored.sh                    PASS  2s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  FAIL  8s
zig + cargo-zigbuild present         PASS  1s
musl release (static, mimalloc)      PASS  636s
musl zigbuild test                   FAIL  639s
```

What each red step was:
- **cargo test:** `flow_e2e::throttle_vs_rate_limit` failed with `run 000038fy9k0cgw2kkxqkpkaqr0shy not terminal within 90s` (crates/ostro/tests/flow_e2e.rs:195). This is **G1**, a real bug and not load: run alone it failed 3 of 7 times. cargo stops at the first failing binary, so the workspace was re-run with `--no-fail-fast`: 2,287 passed, 1 failed (`worker_e2e::module_cache`, below).
- **workflow validation:** `.github/workflows/ci.yml:463:9: shellcheck reported issue in this script: SC2034:warning:3:1: i appears unused`. Fixed in 9212095. The YAML parse and SHA-pin check were re-run. actionlint was not re-run: my direct `docker run rhysd/actionlint` call was refused by the permission classifier.
- **musl zigbuild test:** `worker_e2e::module_cache` failed with `assertion left != right failed: the artefact was not rewritten after the tamper, left: 418170, right: 418170`. ext4 hands the inode the worker freed straight to the next tmp file (reproduced with rm + create + mv). The test now compares (inode, mtime) (45d73c6); `module_cache` passed 3/3 and the full `worker_e2e` 8/8. musl `flow_e2e` passed in this run.

Other gate checks:
- deny is clean.
- The normal tree has no chrono, ring, openssl-sys, webpki-roots or native-tls, and no banned crate is duplicated.
- `git diff 000dced HEAD -- wit/` is empty; goldens are append-only (commands.txt +3/-0).
- `PROPTEST_CASES=256` rebuild_digest: 7 passed (1,542 s). replay_corpus: 5 passed.
- The Phase 7 failpoint legs passed: `chaos_smoke` 1 passed (100 runs) and `sigstop_zombie` 1 passed.
- Failpoint suites: 1,053 passed. Campaign smoke: 0 violations.

## Reservations script output (`07-22-rescheck.py` = the 07-01 script, now expecting the shipped ordinals)

```text
Cmd             20 PauseRun                     -> exists (P7, 07-03; apply 07-04) OK
Cmd             21 ResumeRun                    -> exists (P7, 07-03; apply 07-04) OK
RejectReason    12 ReservedP6_12                -> reserved P6 OK
RejectReason    13 ReservedP6_13                -> reserved P6 OK
RejectReason    14 ReservedP6_14                -> reserved P6 OK
RejectReason    15 RunPaused                    -> exists (P7, 07-03) OK
InvalidCode     18..21 ReservedP7_18..21        -> P7 OK
META_FORMAT: 2
P7 block Cmd           : OCCUPIED [20, 21] (expected [20, 21]) OK
P7 block Intent / StartRun / EventKind / TerminalStatus / Keyspace / InvalidCode / Outbox / MetaRecord : empty OK
P7 block RejectReason  : OCCUPIED [15] (expected [15]) OK
RESERVATIONS CHECK: PASS   (147 OK, 0 FAIL)
```

Keyspaces 17/18 and meta 19/20 stay unused. The §11 Phase 7 names match the code.

## Clock seam (Phase 10 F5)

Grep over p7.rs, workers/, flow/, tls/, control/p7_*.rs and leader/{fair,flow,placement}.rs. The exempt hits:
- `crates/ostro-server/src/tls/ca.rs:53`: `SystemTime::now()`, clock-exempt (X.509 validity timestamps checked by peers).
- `crates/ostro-server/src/workers/spill.rs:369`: `Instant::now()`, clock-exempt (the getrusage CPU-delta wall denominator).
- `crates/ostro-server/src/tls/acme.rs:99`: the `SystemClock` fallback, clock-exempt as an injection root, because `p7::init` now calls `set_clock`.

Two hits are not time reads: an import (`acme.rs:46`) and a doc comment (`acme.rs:92`).

Fixed: `workers/remote_executor.rs` used `tokio::time::Instant::now()` for the post-Cancel bound; it is now a pinned `tokio::time::sleep`. Demand ownership: `ostro_queue_worker_demand` is set only in `workers/demand.rs`; dispatcher.rs only describes it.

## Chaos report (QA-03, `target/fencing-chaos-fix0721/report.json`, 07-21 re-run at e82d99e)

- Verdict: pass.
- 10,000 runs, all 10,000 completed, `charges_eq_1` 10,000.
- 0 double charges, 0 lost, 0 fenced sends.
- 170 raw duplicates, all attributed to leader kills; 0 duplicates without a leader kill and 0 unattributed.
- 10 provider 409s, all recovered (D-W28).

The 10,000-run campaign was not re-run at the 07-22 commits. `chaos_smoke` (100 runs) passed in the gate.

## Follow-ups: fixed vs recorded

**Fixed:**
- `acme::set_clock` is now called.
- The binary passes an explicit TLS profile instead of the auto_sealing_key heuristic.
- The join command shows the bound port instead of `:0`.
- The CLI accepts `https://` with the platform roots or `--ca-file` / `OSTRO_CA_FILE`, including `ostro backup`.
- Run pause state is on REST, gRPC and `ostro inspect`.
- `is_terminal` covers every terminal status.
- A revoked worker exits.
- Logs are plain text when piped.
- A refused zombie StartEffect is logged on the leader.
- The flow_control flake assertion now uses a sound bound.
- The Wasmtime version is one constant.

Found and fixed while documenting:
- A worker given `--ca-hash` against a `files` / `acme` listener could not join, which broke the deploy/k8s manifests.
- The remote_executor clock read above.

**Recorded in 07-VALIDATION "Follow-ups", with owners:**
- F1: no server→worker Drain message (Phase 13).
- F2: worker append batching for long activations, a design limitation (Phase 13).
- F3: the guest for the auto-pause "completes after resume" leg (gap closure).
- F4: `attempt_failures` after resume (USER REVIEW).
- F5-F12: smaller items.
- G1: BLOCKING gap.

## Docs changed

- New: `docs/workers.md`, `docs/flow-control.md`, `docs/tls.md`.
- `docs/03-architecture.md`: §5.3 rewritten (control stream + bulk RPCs, effects bridged to the leader); §5.2 drops the ResolveSecret sentence; §8 carries the demand definition and the KEDA example.
- `docs/02-prd.md` NFR-3.5: 30 s default.

## Human-needed

- H1: real KEDA, via the `keda-kind` job or an approved cluster; never the local microk8s.
- H2: Pebble in CI (`acme-pebble`). It passed locally in 07-10.
- H3: Let's Encrypt staging on a public host.
- H4: remote CI legs.
- H5: review of D-W3, D-W8, D-W11 + D-W28, W6 and F4.
- H6: local PG/MySQL/S3 legs. They SKIPped because there is no `.ostro-test-env`.

## Deviations from Plan

1. **[Orchestrator scope] Product and test code changed** beyond the plan's docs/CI files_modified. The orchestrator asked for the small follow-ups to be fixed. Shared files touched: p7.rs, server.rs, main.rs, ostro-api, the proto (additive field 21), ostro-cli Cargo.toml (a feature only, lock unchanged), logging.rs (Phase 8) and engine cache.rs (an additive constant; key bytes unchanged).
2. **[Rule 1] files/acme join with `--ca-hash`** (ba5da99) was found while documenting the manifests.
3. **[Rule 1] Two test defects** surfaced by the gate were fixed (inode reuse, actionlint).
4. **G1 not fixed.** It is a dispatcher fairness change in a 07-05/Phase 8 shared file, so it is recorded as BLOCKING for gap closure.
5. **ci-local Phase 7 step** runs only the failpoint-only legs plus the manifests. The other Phase 7 spawned suites are not `#[ignore]`d and already run in the workspace `cargo test` step. FAILPOINT_CRATES is unchanged.
6. **STATE Current Position** Phase 7 line updated per the orchestrator brief; the plan said not to touch it.

## Self-Check: PASSED

- The commits listed above exist on main.
- docs/workers.md (220 lines), flow-control.md and tls.md exist.
- VALIDATION has "Gate results" and "Human-needed"; DECISIONS has "Spike outcomes"; STATE has `[Phase 07 / 07-22]`.
