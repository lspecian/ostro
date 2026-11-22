---
phase: 10-developer-cli-test-kits
plan: 01
subsystem: planning-gate
tags: [gate, reservations, decisions, coordination, cli, test-kits]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "20/20 SUMMARYs, 09-VERIFICATION human_needed (by-design only), D-P9-28/29/30 outputs, Runtime::describe + ENG-1 exports rule"
  - phase: 08-triggers-bottomless-observability
    provides: "injected clock seam F5 (08-04): one Arc<dyn Clock> in server.rs with a builder override, RuntimeOptions.clock"
provides:
  - "PHASE-10 GATE passed; baseline ci-local --no-musl at 2c8642a (code) green apart from a mid-run artefact in the failpoint list sync step, which passes at ca8a62e"
  - "RESERVATIONS.md §11 Phase 10 name rows (D-P10-19) + 'Phase 10 allocates no ordinal (F7); no P10 block.'"
  - "STATE.md: D-P10-1..D-P10-21, [USER REVIEW] D-P10-11 / D-P10-6, [HUMAN] publishing / SC1, Phase 10 <-> Phase 7/8/11 coordination"
affects: [10-02, 10-03, 10-04, 10-05, 10-06, 10-07, 10-08, 10-09, 10-10, 10-11, 10-12, 10-13, 10-14, 10-15, 10-16, 10-17, 10-18, 10-19, 10-20]
tech-stack:
  added: []
  patterns: ["docs-only gate commits through a private index (index-10-01) with a CAS update-ref; the docs worktree is separate from the CI worktree"]
key-files:
  created:
    - .planning/phases/10-developer-cli-test-kits/10-01-SUMMARY.md
  modified:
    - .planning/RESERVATIONS.md
    - .planning/STATE.md
    - .planning/ROADMAP.md
key-decisions:
  - "Phase 10 allocates no ordinal (F7). The reservations check passes; the code has no P10 ordinal and RESERVATIONS.md has no P10 block"
  - "10-11 reuses the existing ostro_engine::replay_check(rt, module: &Prepared, run, journal) -> ReplayVerdict (11-10) unchanged. The type is Prepared, not PreparedModule as D-P10-7 words it"
  - "10-09 moves the delimited `// --- Phase 11 (11-14) ---` block of ostro-cli/src/client.rs into ostro-client as-is"
  - "The Phase 10/11 mutual-exclusion pairs are moot: 11-03, 11-04, 11-09 and 11-14 are done"
  - "Docker is present locally now (Docker Desktop WSL, 29.1.3). D-P10-15/D-P10-20 assumed it was absent. The recording shim stays the test; 10-15/10-19 may add real local runs"
requirements-completed: []
duration: ~3h10m (of which ~2h55m was the baseline ci-local run)
completed: 2026-11-22
---

# Phase 10 Plan 01: PHASE-10 GATE Summary

**The gate passed:**
- **Phase 9 is complete.** All 20 SUMMARYs exist, and 09-VERIFICATION is human_needed with 4 items, all by design.
- **The Phase 9 outputs exist.** These are D-P9-28/29/30, `Runtime::describe` and the ENG-1 `exports` rule.
- **The Phase 8 clock seam (F5) is in the code.**
- **The code matches RESERVATIONS.md.** The P10 ordinal block is empty.
- **The baseline `ci-local --no-musl` passed every code step.** The run started at 2c8642a. One step failed: "failpoint list sync". That failure is an artefact: 11-18 changed ci.yml and ci-local.sh mid-run, and the gate's own commit then checked that change out into the CI worktree. The step passes when re-run at ca8a62e. 2c8642a..ca8a62e contains no code change.

## Performance
- 2026-11-22; ~3h10m wall time, of which the baseline ci-local took ~2h55m (step sum 10,420 s).
- Tasks: 2/2.
- Commits: ca8a62e (RESERVATIONS), b2e58ce (STATE), plus the final docs commit.

## Check 1: Phase 9 complete: PASS
- `ls 09-{01..20}-SUMMARY.md`: all 20 exist.
- `09-VERIFICATION.md` has `status: human_needed`, with no `gaps_found`.
- Classification script (`10-01-classify.sh`) output:

```
OK (a) publishing :: Publish the `ostro` Python package to PyPI and the `ostro` TS package to npm (D-P9-19); ad
OK (b) remote CI polyglot run :: Run the `polyglot` job of .github/workflows/ci.yml on remote CI (FOUND-05 pattern)
OK (c)+(d)+(f) upstream PR/issue, SC2 residual, D-P9-3 review :: SC2 Python/TS rebuild sha (D-P9-3 step 3): file the componentize-py determinism PR and the
OK (e) SC5 native perf machine :: SC5 on the native perf machine: run the resources suite with OSTRO_SC5_ENFORCE=1 (quiet, n
classification fail=0
```

## Check 2: Phase 9 outputs: PASS
- `sdks/scripts/build-guest.sh:41: UV_VERSION="0.7.15"`.
- `sdks/scripts/toolchain/package-lock.json` exists.
- `cmp wit/ostro-runtime-0.1.0/runtime.wit sdks/rust/ostro-sdk/wit/runtime.wit` finds them identical. The Python (`sdks/python/ostro/wit/runtime.wit`) and TS (`sdks/ts/wit/runtime.wit`) copies are also byte-identical to the frozen WIT.
- `sdks/rust/ostro-sdk/src/bindings.rs:10:    path: "wit",`. No `../../../wit` reference remains.
- `crates/ostro-engine/src/runtime.rs:605: pub async fn describe(&self, p: &Prepared) -> Result<Vec<WorkflowMeta>, EngineError>`.
- ENG-1 rule: `validate.rs:59` says a component may have exactly one extra export named `exports`. The constant is `COMPONENTIZE_PY_EXPORT = "exports"` (line 65).
- CI pins in ci.yml:
  - `node-version: "24.13.0"` (line 539). D-P10-3 uses exactly this version.
  - `astral-sh/setup-uv@1db35dd… # v10.4.0` with `version: "0.7.15"` (lines 547–549).
- The docs/sdk-builds.md "Artefact for Phase 10" section, quoted in full:

> Phase 10's hermetic `ostro build` ships the **released componentize-py 0.25.1 wheel** from PyPI (D-P9-30), with the reproducibility gap above documented: Python builds are not bit-reproducible and the str hash secret differs per build. The D-P9-3 fork stays a CI-built, opt-in toolchain (`OSTRO_COMPONENTIZE_PY`, built by `sdks/scripts/build-componentize-py-fork.sh`), because no hosted fork artefact exists for Phase 10 to download. Even with the fork the residual is 73–76 bytes (timestamps from the WASI filesystem at pre-init); with the released wheel it is the whole snapshot layout. CI does not build the fork (a cold build needs ~3.6 GB and ~6–30 min).

## Check 3: Phase 8 clock seam (F5): PASS
- `08-04-SUMMARY.md` exists.
- `server.rs` contains the injected clock and its override:
  - line 64, doc: "the single `Arc<dyn Clock>` handed to `InitCluster`'s timestamp, …";
  - line 198: `pub clock: Arc<dyn Clock>,`;
  - line 230: `pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self` (the builder override);
  - line 844: the field;
  - line 952: the `clock()` accessor.
- `server.rs` has no `fn now_ms()`.
- `crates/ostro-engine/src/runtime.rs:71: pub clock: Arc<dyn Clock>,`.
- **Dispatcher clock wake:** there is none. The `dispatcher.rs` module doc (lines 105–107) says: "A `ManualClock` that is advanced does not wake the loop by itself (the timer service polls; this does not)." The `changed()` channels that exist are for placement, flow control and shutdown only. 10-10 must add the clock wake and the skip barrier, as planned.

**Phase 7/8 status:**
- Phase 7 is complete: 23 SUMMARYs, VERIFICATION human_needed (by design).
- Phase 8 is complete: 23 SUMMARYs, VERIFICATION passed.
- Neither has plans in flight.

## Check 4: Phase 11 (and Phase 12) status and overlaps
Phase 11 has 17 of 18 SUMMARYs; the 11-18 gate is still running. Its plans' `files_modified` entries that overlap the 10-DECISIONS "Shared files" table:

| Plan | State | Overlapping files |
|------|-------|-------------------|
| 11-03 | done | Cargo.toml, Cargo.lock, deny.toml, crates/{ostro-ui,ostro-server,ostro}/Cargo.toml |
| 11-04 | done | ostro-api phase11.rs/lib.rs/tests (lifecycle_surface, parity, rest), server.rs, control/mod.rs, ci-local.sh, ci.yml |
| 11-06/07/12 | done | guests/p11-*/Cargo.toml (guest workspaces only) |
| 11-09 | done | ostro-api phase11/lib/domain/rest/grpc/fake/openapi.rs, tests/parity.rs, phase11_surface.rs, OpenAPI snapshot |
| 11-14 | done | ostro-cli commands/{rerun,dlq,bulk,limits,runs,mod}.rs, ostro-cli/src/client.rs (delimited block lines 395–454), crates/ostro/src/main.rs |
| 11-18 | **open** | scripts/ci-local.sh, .github/workflows/ci.yml (landed b460ad7 during this gate: 4 failpoint suites appended) |

Phase 12 overlaps, which the 10-DECISIONS table does not list (Phase 12 runs in parallel):
- 12-01 (done): Cargo, deny and ci files.
- 12-02 (done during this gate): server `config.rs`/`server.rs`.
- 12-08: partition `dispatcher.rs`, `control/mod.rs`, ci-local.sh.
- 12-14: `control/mod.rs`, `server.rs`.
- 12-17: `config.rs`.
- 12-18: ci files.

These are recorded in STATE.md.

**Seams the coordinator named, verified in code:**
- `ostro_engine::replay_check(rt: &Runtime, module: &Prepared, run: RunRef, journal: &[Event]) -> ReplayVerdict {Ok, OkFrontier, Diverged{seq,detail}, Error}` is at `crates/ostro-engine/src/replay_check.rs:137`.
- The 11-14 client block is delimited by `// --- Phase 11 (11-14) ---` / `// --- end Phase 11 (11-14) ---`.

## Check 5: Working tree (stale-blob rule): no contention
In the main checkout, `git status --porcelain crates sdks clients scripts .github docs wit Cargo.toml Cargo.lock deny.toml` lists 843 paths:
- 622 ` D` / 29 `D ` (deleted);
- 142 ` M`, 28 `MM`, 16 `MD`, 6 `M `.

Each modified working-tree blob was compared with HEAD and with the blob at each of the path's last 20 commits (and their parents). **Real uncommitted edits: 0.** Every modified path is a stale blob from an older HEAD, so nothing contends with 10-02 or 10-05.

## Check 6: Baseline `bash scripts/ci-local.sh --no-musl`
- Run from the scratch worktree at **2c8642a**, slot-1 target dir, `nice -n 10`, `CARGO_BUILD_JOBS=4`. Load average was ~8–10, because the 11-18 and 12-02 gates ran in parallel.

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  3s
cargo clippy -D warnings             PASS  85s
cargo build (warning-free)           PASS  70s
sdk workspace fmt + clippy           PASS  24s
sdk workspace test + wasm build      PASS  27s
guests build (wasm32-wasip2)         PASS  20s
order-saga example                   PASS  23s
external: OSTRO_TEST_PG_URL          SKIP  -
external: OSTRO_TEST_MYSQL_URL       SKIP  -
external: OSTRO_TEST_S3_URL          SKIP  -
cargo test                           PASS  4377s
polyglot toolchains present          PASS  1s
python sdk tests                     PASS  1s
ts sdk tests                         PASS  6s
polyglot conformance                 PASS  364s
clippy (polyglot feature)            PASS  66s
semantics sleepers (serial)          PASS  64s
failpoint list sync (ci.yml)         FAIL  0s   <- mid-run artefact, see below
failpoint suites                     PASS  4563s
server campaign smoke (20 kills)     PASS  669s
phase 7 failpoint legs + manifests   PASS  52s
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  2s
check-deny-fixtures.sh               PASS  0s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  1s
zig/musl steps                       SKIP  (--no-musl)
```

**The sync failure is an artefact.** While the run was going, 11-18 landed b460ad7, which appended `rerun_rearm`, `dlq_api`, `bulk_ops` and `ops_kill` to FAILPOINT_CRATES and to ci.yml. 10-01's RESERVATIONS commit then checked that commit out into the CI worktree. Bash had already read the old FAILPOINT_CRATES into memory, but the sync step reads the new ci.yml from disk, so the two lists differed by exactly those 4 lines. The check was re-run standalone at ca8a62e (the current scripts) and printed `failpoint list sync at ca8a62e: PASS`. `git diff 2c8642a ca8a62e -- crates sdks guests Cargo.toml Cargo.lock` is empty.

So the baseline did not run the 4 new 11-18 failpoint suites. Those belong to the 11-18 gate.

**Lesson for later gates:** never move the CI worktree while ci-local is running. 10-01 committed Task 2 from a separate docs worktree for this reason.

## Check 7: Toolchains
| Tool | Version |
|------|---------|
| node | v24.13.0 (= the ci.yml pin) |
| npm | 11.6.2 |
| uv | 0.7.15 |
| go | go1.23.10 linux/amd64 (≥ 1.23 OK) |
| cc | /usr/bin/cc present |
| docker | **present**: `/usr/bin/docker` → Docker Desktop WSL integration (symlink dated 2026-11-22), client/server 29.1.3. The plan expected it absent |
| zig | not on PATH; pip `ziglang` 0.17.1 (`python3 -m ziglang version`) |
| rustup | 1.29.3; cargo 1.100.0; cargo-deny 0.21.0 |
| glibc | `ldd (Ubuntu GLIBC 2.35-0ubuntu3.18) 2.35` (the reason for D-P10-2) |

## Check 8: Reservations check: PASS
`10-01-rescheck.py` is a fresh script modelled on 09-01/11-01/12-01; none of those committed their script. It extracts every ordinal space from code:
- Cmd, StartRun, Intent, outbox Kind (prost tags);
- EventKind and TerminalStatus (discriminants);
- the TABLES order;
- the RejectReason and InvalidCode orders;
- MetaRecord, MetaKind and MetaCommand (variant order);
- `EXT_*` tags.

Each code ordinal must fall on a RESERVATIONS row that reads `exists …`, `planned P<n>` (the phase that added it), or the §10b owner column, or the code item must be a canonical `Reserved*` placeholder.

The script's core:
```python
cov = [r for r in rows.get(sid, []) if r[0] <= o <= r[1]]
st = cov[0][3] if cov else "<no row>"
ok = st.startswith("exists") or bool(re.match(r'planned P\d+', st)) or name.startswith("Reserved")
if not ok and sid == "10b" and re.match(r'P\d+ \(\d\d-\d\d\)', st): ok = True   # §10b owner column
...
p10_ord = [l for l in p10 if re.match(r'^\|\s*\d', l)]    # F7: no P10 ordinal row
```
The output was 180 OK lines and 0 FAIL. The tail:
```
MetaKind order: ['Cluster', 'Member', 'Partition', 'Module', 'Workflow', 'Active', 'Queue', 'Schedule', 'Token', 'Tenant', 'Secret', 'Grant', 'Connection', 'Egress', 'WorkflowPolicy', 'QueuePolicy', 'Webhook']
META_FORMAT: 2
Code tops: Cmd max 45 | StartRun max 41 | Intent max 19 | EventKind max 97 | TerminalStatus max 5 | Keyspace(TABLES) max 16 | RejectReason max 15 | InvalidCode max 30 | OutboxKind max 6 | MetaRecord max 16 | MetaCommand max 6 | ExtTag max 80
P7 ordinals in code: 7: Cmd 20 PauseRun; Cmd 21 ResumeRun; RejectReason 15 RunPaused; InvalidCode 18 ReservedP7_18; InvalidCode 19 ReservedP7_19; InvalidCode 20 ReservedP7_20; InvalidCode 21 ReservedP7_21
P8 ordinals in code: 5: Cmd 27 ScheduleFire; Keyspace(TABLES) 16 ScheduleState; InvalidCode 22 BadScheduleFire; InvalidCode 23 ScheduleOwnerMismatch; InvalidCode 24 ReservedP8_24
P9 ordinals in code: 5: Intent 19 ChildJournal; InvalidCode 25 ReservedP9_25; InvalidCode 26 ReservedP9_26; InvalidCode 27 ReservedP9_27; InvalidCode 28 ReservedP9_28
P11 ordinals in code: 5: Cmd 45 DlqUpdate; StartRun 40 rerun_prefix; StartRun 41 rerun_from_seq; InvalidCode 29 BadRerunPrefix; InvalidCode 30 BadDlqUpdate
P12 ordinals in code: 4: MetaCommand 4 ElectLeader; MetaCommand 5 AlterIsr; MetaCommand 6 ResignLeader; ExtTag 80 EXT_ELECTION_INFO
P10 ordinal rows in RESERVATIONS.md: 0 (F7 expects 0)
RESERVATIONS CHECK: PASS
```
Phase 12's MetaCommand 4–6 and ext tag 80 are in code while RESERVATIONS still reads `planned P12 (12-02 …)`. Flipping those rows belongs to Phase 12, not to this gate. The ext tags in code are 15 (golden/test) and 80.

## Check 9: RESERVATIONS.md (commit ca8a62e)
§11 gained 15 P10 rows (owner `P10 (10-01)`), each checked against the code for collisions:
- crates `ostro-build`, `ostro-client`;
- features `testing`, `polyglot`. The name `polyglot` repeats P9's feature, but on different crates;
- the dirs;
- `ostro.toml` (the server config has no default filename) and `target/ostro/manifest.json`;
- the three REST paths;
- `[dev]`;
- the two error codes;
- the env vars. `OSTRO_BIN` already exists in `crates/ostro/tests/common` with the same meaning;
- the packages, the docker image and exit code 4;
- the JSON kinds, `ostro_dev_`, the frozen ready line and the six CLI verbs.

The line "Phase 10 allocates no ordinal (F7); no P10 block." follows the table. Both plan verify commands pass.

## Task 2: STATE.md (commit b2e58ce)
Edits are confined to Decisions, Pending Todos and Blockers/Concerns:
- **Decisions:** the D-P10-1..21 block.
- **Pending Todos:** two [USER REVIEW] items (D-P10-11, D-P10-6) and two [HUMAN] items (publishing, SC1).
- **Blockers/Concerns:** the coordination concern, which includes:
  - the coordinator's notes: reuse replay_check, move the 11-14 block verbatim, the 10-18 completions check for rerun/dlq/bulk/limits in `crates/ostro/tests/cli_p11.rs`, and that the mutual exclusion is moot;
  - the Phase 11/12 overlaps;
  - the polyglot/Go/Docker note, plus the update that Docker is now present;
  - the macOS `--ephemeral` no-tmpfs item, as a candidate for 10-10/10-13 (not implemented).

## Deviations from Plan
1. **[Rule 3] The reservations script was rewritten.** The 09-01 script was never committed, so `10-01-rescheck.py` is a fresh script; its core logic is shown above.
2. **The baseline's sync step failed because of a mid-run tree change.** The cause was this gate's own CAS commit checking 11-18's ci changes out into the CI worktree. The step was re-run standalone and passes. Killing and restarting the run was not possible (the kill was denied).
3. **Docker is present.** This contradicts a plan assumption (check 7 "expect absent"). It is recorded, but D-P10-15/20 were not changed.

## Open items for the user
- [USER REVIEW] D-P10-11: the notify CC0-1.0 deny exception.
- [USER REVIEW] D-P10-6: the auto-skip trigger.
- macOS `ostro dev --ephemeral` misses the 1 s budget because macOS has no tmpfs. The candidate is an in-memory/fsync-free ephemeral mode in 10-10/10-13.

## Self-Check: PASSED
- FOUND: ca8a62e docs(10-01): record Phase 10 name reservations
- FOUND: b2e58ce docs(10-01): record Phase 10 decisions, review items and coordination
- FOUND: .planning/RESERVATIONS.md "Phase 10 allocates no ordinal"
- FOUND: .planning/STATE.md "Phase 10 decisions (adopted defaults; user may override)"
