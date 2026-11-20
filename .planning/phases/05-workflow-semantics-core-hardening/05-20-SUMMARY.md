---
phase: 05-workflow-semantics-core-hardening
plan: 20
subsystem: qa-gate
tags: [phase-gate, crash-campaign, qa-01, qa-02, wit-07, freeze, d26, perf]

requires:
  - phase: 05-19
    provides: "sharded resumable whole-server kill campaign, QA-01 checker with negative controls"
  - phase: 05-27
    provides: "D25 step retry backoff is a cancellation point (failpoint binary ddbd2bbb..., built at c6b1c09)"
  - phase: 05-26
    provides: "exclusive <data>/LOCK"
provides:
  - "Final 10,000-kill gate campaign on one binary and one checker: 0 violations, RESULT: PASS"
  - "D26: the checker does not count a rejected terminate (409 run_terminal, no ack) as sent for the retry-exhaustion failed exception"
  - "Replay corpus 30 entries (order-saga x3, crash-campaign-s00 a/b/c) and on-disk format corpus v0.1 (82c9333)"
  - "Freeze flip: wit/FROZEN.toml, golden + meta-golden re-pin, storage-format FROZEN v0.1, RESERVATIONS Phase 5 rows -> exists, local tag wit/ostro-runtime-v0.1.0 (fc47569)"
  - "05-VALIDATION.md: statuses, Gate results, Human-needed"
affects: [06, 08, 09, 11, 12, 13]

tech-stack:
  added: []
  patterns:
    - "A checker relaxation is narrowed to one rule and paired with positive and negative controls, one of which fails against the previous checker"

key-files:
  created:
    - wit/FROZEN.toml
    - tests/format-corpus/v0.1/
    - tests/replay-corpus/{order-saga-happy,order-saga-compensated,order-saga-cancelled,crash-campaign-s00-a,crash-campaign-s00-b,crash-campaign-s00-c}/
    - .planning/phases/05-workflow-semantics-core-hardening/05-20-SUMMARY.md
  modified:
    - crates/ostro-crashtest/src/check.rs
    - crates/ostro-crashtest/tests/check_controls.rs
    - crates/ostro-crashtest/src/server_target.rs
    - wit/ostro-runtime-0.1.0/runtime.wit
    - crates/ostro-types/tests/golden_frozen.rs
    - crates/ostro-witcheck/tests/wit_freeze.rs
    - docs/storage-format.md
    - docs/wit-0.1-freeze-review.md
    - .planning/RESERVATIONS.md
    - .planning/phases/05-workflow-semantics-core-hardening/05-DECISIONS.md
    - .planning/phases/05-workflow-semantics-core-hardening/05-VALIDATION.md
    - .planning/phases/05-workflow-semantics-core-hardening/deferred-items.md

key-decisions:
  - "The freeze commit fc47569 and its tag landed before the final campaign passed; the user chose to keep them unchanged (Decision 2 do A)"
  - "D26 (user-approved, Decision 1 do A): a terminate with stop_rejected and no terminate ack does not block the retry-exhaustion failed exception; the terminated arm still accepts any terminate intent because a retried terminate is rejected after a first attempt whose 200 was lost to a kill"
  - "The failpoint binary was not rebuilt: since c6b1c09 only WIT doc comments changed (no engine/server source), sha256 ddbd2bbb... unchanged"
  - "ci-local is red on check-vendored.sh (session path inside six 05-20 corpus wasm files); not fixed, it needs a user decision; ROADMAP 05-20 left unticked"

requirements-completed: []

duration: "~3h15m this pass (campaign wall 1h54m, ci-local 46 min)"
completed: 2026-11-20
---

# Phase 5 Plan 20: Phase gate + freeze flip Summary

**The final gate campaign passed on one binary and one checker. All 20 shards were re-run under
the D26 checker against failpoint binary `ddbd2bbb…`: 10,000 kills (5,865 plain / 3,048
failpoint / 1,087 ENOSPC), 0 lost acked runs, 0 duplicate journal entries, 0 non-terminal runs,
0 violations, RESULT: PASS. Profile A on WSL2 runs at about 30k steps/s with 64 clients, and the
idle start→first step p50 is 3.5 ms. The freeze is in force. The full ci-local gate is NOT green:
`check-vendored.sh` flags a local session path embedded in six corpus wasm files added by this
plan. That needs a user decision, so the plan is not marked complete in ROADMAP.**

## What happened, plainly

1. **Task 2 landed before Task 1 had passed.** The corpus commit is `82c9333`. The freeze commit
   is `fc47569`: FROZEN.toml sha256 `611d2982…d6bd98e`, golden + meta-golden re-pin, storage
   format FROZEN v0.1, and RESERVATIONS rows flipped. Its local tag `wit/ostro-runtime-v0.1.0`
   was created **before the final crash campaign had passed**. At that point the earlier
   campaign was a mixed-binary pass (13 shards before the D25 fix, 7 after; see 05-27).
2. The orchestrator then asked for a single-binary re-run on `ddbd2bbb…`. Shard 00 flagged
   `WrongStatus` on run `004038fnrafsrxswnmeq2kfv4qbn6`:
   - step4 exhausted its retries: 9 × `step_attempt_failed`, then a final `transient` error,
     then `run_failed`. That is crash-saga's documented outcome.
   - The workload's terminate then got **409 `run_terminal`**. `acks.jsonl` shows
     `stop_intent terminate`, then `stop_rejected terminate`, and no ack.
   - The checker took `terminate_sent` from the intent alone, so the retry-exhaustion exception
     did not apply. This was a checker false positive, not an engine bug.
3. **The user decided** both points: "Decision 1 do A" (narrow checker fix + negative controls,
   then re-run all 20 shards) and "Decision 2 do A" (keep `fc47569` and its tag unchanged).
   The checker change is **D26, made with the user's approval** (commit `3a6c592`).
4. All 20 shards were re-run from an empty `target/campaign-gate` under the new checker. The
   old shard state, its batch logs and the violation histories moved to
   `target/campaign-gate/superseded-checker-v1/`.

## D26 checker change (`3a6c592`)

`check_final` only. A new `terminate_rejected` (terminate intent + `stop_rejected` + no terminate
ack) is allowed in the `("failed", None)` retry-exhaustion guard:
`if (!terminate_sent || terminate_rejected) && failure contains "transient" && retries_exhausted`.
Nothing else changed:
- `("terminated", _) if terminate_sent` still accepts any terminate intent. The workload
  retries a stop after a 5xx or transport error, so a terminate whose 200 was lost to a kill
  is rejected on retry even though the run was terminated.
- An acked terminate + `failed` → still `WrongStatus`.
- An unacked terminate with no rejection + `failed` → still `WrongStatus`.
- Result, non-terminal and journal checks are unchanged.

New control `control_d26_rejected_terminate` in `tests/check_controls.rs`:

| case | expected |
|---|---|
| rejected terminate + failed by retry exhaustion | no violation (positive) |
| rejected terminate + failed without exhaustion | `WrongStatus` |
| rejected terminate + completed, wrong result | `WrongResult` |
| rejected terminate + completed, right result | no violation |
| rejected terminate + running | `NonTerminal` |
| rejected terminate + terminated | no violation (unchanged) |
| acked terminate + failed by exhaustion | `WrongStatus` |
| ack then rejected retry + failed by exhaustion | `WrongStatus` |
| unacked terminate, no rejection + failed by exhaustion | `WrongStatus` (unchanged) |
| rejected cancel only + failed by exhaustion | no violation (unchanged) |

`cargo test -p ostro-crashtest --test check_controls`: 16 passed (all existing controls
unchanged and green). Against the previous `check.rs` the new control fails at the positive
assertion (line 527): 15 passed, 1 failed. fmt and clippy `-D warnings` (with `failpoints`)
are clean.

## Task 1: final gate campaign

- Seed `0x0520`, `FAULTS=10000`, `SHARDS=20`, `PARALLEL=6`. The batches ran in the
  background in this order:

  | batch | wall |
  |---|---|
  | 0-5 | 1671 s |
  | 6-11 | 1721 s |
  | 12-17 | 2017 s |
  | 18-19 | 1444 s |

  Total 04:14:32 → 06:08:46 (1 h 54 min).
- Binary: `target/failpoints/release/ostro` sha256
  `ddbd2bbb01f0f928ec6c0b1a76da8285ca5f3246115c8e78352c88055ecaabe6`, built at `c6b1c09`.
  Not rebuilt: `git diff c6b1c09 HEAD` touches no engine or server source, only WIT doc
  comments, fixtures, docs and planning files.
- Checker: `check.rs` blob `bebc425b94c167fdb79c5c76fa31754dd9b69403` (was `c7e2c3d5…`).
  Harness built from `3a6c592` in worktree `wt-05-20c` (`--target-dir target/wt-05-20c`).
- The batch script `campaign-batch.sh` hard-codes worktree `wt-05-20` at `7289d56`, which has
  the old checker. The run used a copy (`campaign-batch-0520c.sh`) that changes only the
  worktree path and the harness target dir; every `OSTRO_CAMPAIGN_*` setting and
  `OSTRO_FAILPOINT_BIN` is identical.
- The crash-saga guest was built from unchanged sources in the new worktree. Its sha256 is
  `24f66182…` vs `9bf235e7…`: same size, and the only difference is the embedded build path.

```text
kills=10000 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0
faults_target=10000 shards=20/20 incomplete=[] by_variant=plain:5865 failpoint:3048 enospc:1087 failpoint_hits=4582 extra_kills=4155 rebuilds=420 acked_runs=120888
shard  kills  plain  fp  enospc  hits  extra  rebuilds  runs  kill_s  s/kill  final
    0    500    283  153      64   218    218        21  5850    1620    3.24  ok
    1    500    303  149      48   224    198        21  5857    1522    3.04  ok
    2    500    276  168      56   222    225        21  5972    1621    3.24  ok
    3    500    295  147      58   202    206        21  5925    1620    3.24  ok
    4    500    298  137      65   242    203        21  5884    1496    2.99  ok
    5    500    281  157      62   211    220        21  5842    1559    3.12  ok
    6    500    291  153      56   236    210        21  5600    1561    3.12  ok
    7    500    305  148      47   184    196        21  6261    1697    3.39  ok
    8    500    294  149      57   263    207        21  6010    1574    3.15  ok
    9    500    302  155      43   208    199        21  6150    1649    3.30  ok
   10    500    292  168      40   230    209        21  6166    1578    3.16  ok
   11    500    284  164      52   194    217        21  5995    1659    3.32  ok
   12    500    298  154      48   239    203        21  6149    1846    3.69  ok
   13    500    308  142      50   231    193        21  6456    1964    3.93  ok
   14    500    288  158      54   245    213        21  6171    1834    3.67  ok
   15    500    297  148      55   278    204        21  6525    1964    3.93  ok
   16    500    282  152      66   240    219        21  6285    1850    3.70  ok
   17    500    313  131      56   210    188        21  5974    1732    3.46  ok
   18    500    280  166      54   247    221        21  5798    1378    2.76  ok
   19    500    295  149      56   258    206        21  6018    1414    2.83  ok
RESULT: PASS
```

The ack logs hold 846 `stop_rejected terminate` records. The report is in
`target/campaign-gate/report.txt`, with hashes and settings in `report-provenance.txt`.

Earlier attempts, kept for diagnosis:
- `first-attempt-08-10/`: open timeouts in `rebuild_and_compare`. That was a harness limit,
  fixed by `7289d56`.
- `superseded/`: shards 12 and 17, the D25 engine finding fixed by 05-27.
- `superseded-prefix/`: the mixed-binary pass.
- `superseded-checker-v1/`: the single-binary re-run with the shard-00 D26 false positive.

## Task 2: corpora and freeze (committed earlier, unchanged here)

- `82c9333`: the replay corpus now has 30 entries and 383 events, all green on gnu and musl.
  (Superseded by `da6c7a9`: the six new entries were re-recorded path-neutrally for
  check-vendored check 4; the corpus is now 30 entries and 377 events.)
  New: `order-saga-happy/-compensated/-cancelled`, exported after the 05-17 CLI flows, and
  `crash-campaign-s00-a/b/c`, completed runs 7-8 leader epochs apart.
- `82c9333`: format corpus `tests/format-corpus/v0.1`: P=2, 48 runs, 418,768 bytes, one blob,
  StartRun tag 23 inline and with a blob PayloadRef. It also adds
  `unknown_data_dir_entries_ignored` (F8).
- `fc47569`: runtime.wit doc comments only, verified as comment-stripped identical: P13-F3,
  PF-11-1 and D25.
- `fc47569`: witcheck fixtures re-blessed.
- `fc47569`: `wit/FROZEN.toml` sha256
  `611d2982b88e0884d55f0acb8f47691edbcc4c4b2a8918efaad4a7092d6bd98e`.
- `fc47569`: golden pins extended to full files. The ostro-types goldens are unchanged since
  05-05 (21 command vectors, 27 event lines), and the meta golden `meta_v2.txt` is pinned per
  section (17 MetaRecord variants …).
- `fc47569`: `docs/storage-format.md` marked FROZEN v0.1.
- `fc47569`: RESERVATIONS Phase 5 rows set to `exists (P5)`.
- `fc47569`: local tag. There is no remote and nothing was pushed.

## Task 3: ci-local, perf, startup

### ci-local (full, with musl), tree `3a6c592`: 1 step FAILED

```text
================ ci-local summary ================
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  2s
cargo clippy -D warnings             PASS  54s
cargo build (warning-free)           PASS  90s
sdk workspace fmt + clippy           PASS  33s
sdk workspace test + wasm build      PASS  38s
guests build (wasm32-wasip2)         PASS  0s
order-saga example                   PASS  36s
cargo test                           PASS  622s
semantics sleepers (serial)          PASS  50s
failpoint list sync (ci.yml)         PASS  0s
failpoint suites                     PASS  774s
server campaign smoke (20 kills)     PASS  261s
cargo deny check                     PASS  1s
check-vendored.sh                    FAIL  1s
check-deny-fixtures.sh               PASS  0s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  0s
zig + cargo-zigbuild present         PASS  1s
musl release (static, mimalloc)      PASS  193s
musl zigbuild test                   PASS  604s
==================================================
1 step(s) FAILED
```

```text
FAIL  4 session-path leaks or upstream build references
      tests/replay-corpus/crash-campaign-s00-a/module.wasm
      tests/replay-corpus/crash-campaign-s00-b/module.wasm
      tests/replay-corpus/crash-campaign-s00-c/module.wasm
      tests/replay-corpus/order-saga-cancelled/module.wasm
      tests/replay-corpus/order-saga-compensated/module.wasm
      tests/replay-corpus/order-saga-happy/module.wasm
```

Why it fails:
- `82c9333` exported these six entries from guests built inside a session scratchpad
  worktree, so their wasm panic-location strings contain that local path.
- 05-19's ci-local passed this check; this is the first ci-local run since `82c9333`.
- The entries replay green; only the path-leak check fails.

Why it is not fixed here:
- Re-recording the entries changes their `module_sha256`.
- Exempting `module.wasm` from check 4 weakens the gate.
- Either way it is a user decision. It is recorded in deferred-items.md and in 05-VALIDATION
  Human-needed item f.

### Perf (profile A, WSL2 advisory, quiet machine, raw fsync p50 2.56-2.71 ms)

| clients | steps/s (3 runs) | start→first step p50 / p99 | target | status |
|---|---|---|---|---|
| 64 (2,000 runs) | 30,517 / 30,168 / 30,116 | 9.76-10.22 / 18.1-21.3 ms | ≥ 5,000 (Phase 5 exit); NFR-2.5 10,000 | met on WSL2 |
| 1 (300 runs) | 936 / 975 / 959 | **3.52 / 6.16, 3.50 / 5.62, 3.59 / 6.22 ms** | p50 < 5 ms (NFR-2.4) | met on WSL2 |

All steps were verified on disk after restart (D16). The gate itself is native NVMe and is
human_needed.

### `ostro dev` startup (D8, debug build)

| mode | time | budget |
|---|---|---|
| `--ephemeral` | 100 ms | < 1 s |
| warm restart | 164 ms | < 1 s |
| cold first run | 1,407-1,474 ms (median 1,459) | measured only |

## Deviations from Plan

1. **[User decision, D26] Checker changed during the gate.** See above. The user approved it;
   every shard was re-run afterwards.
2. **Order: freeze before campaign pass.** `fc47569` and its tag predate the final passing
   campaign. The user chose to keep them.
3. **[Rule 3] Batch script adapted.** The worktree path and the harness target dir changed so
   that the harness used the D26 checker. No campaign setting changed.
4. **Not fixed: check-vendored.sh failure** (pre-existing since `82c9333`, needs a decision).
5. **ROADMAP not ticked.** The plan's must-have "ci-local (full, incl. musl) is green on the
   frozen tree" is not met. A prepared ROADMAP hunk is left for the orchestrator.
6. docs/benchmarks.md was not updated in this pass; the numbers are recorded here and in
   05-VALIDATION.md.

## Human-needed

See 05-VALIDATION.md § Human-needed:
- a: native NVMe perf gate
- b: aarch64-musl / macOS CI legs
- c: docker clean-machine quickstart
- d: real-ENOSPC tmpfs leg
- e: review of D7/D8/D14 and the freeze flip
- f: decision on the check-vendored corpus-path failure

## Self-Check

- Commit `3a6c592` exists on main; check.rs blob `bebc425b…` matches the campaign harness
  source.
- `target/campaign-gate/report.txt` says RESULT: PASS, 20/20 shards, kills=10000.
- `target/failpoints/release/ostro` sha256 `ddbd2bbb…` was re-checked after the campaign.
- ci-local: 20/21 PASS. The check-vendored.sh failure is reported above, verbatim.
