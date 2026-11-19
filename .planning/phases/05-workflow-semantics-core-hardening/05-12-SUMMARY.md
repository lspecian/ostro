---
phase: 05-workflow-semantics-core-hardening
plan: 12
subsystem: qa-corpora
tags: [replay-corpus, qa-02, determinism, wasmtime-bump-gate, format-corpus, d18]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-03 engine semantics (SignalMatched, WaitTimedOut, cancel-op, patched memo); 05-06 D11 SDK; 05-08/05-09 apply shapes (D21 signals, B1, RunTerminated); 05-10 test guests + phase5_guests driver; 05-22 ReadyValue/attempt_failures/status_seq/parent_event; 05-23 D22 retry clock (e7997a7)"
provides:
  - "tests/replay-corpus/: 24 committed entries (module.wasm + journal.bin + entry.toml), 3.7 MB, README rules (never re-record, Wasmtime bump gate, export key rule)"
  - "crates/ostro-engine/tests/replay_corpus.rs: QA-02 replay of every entry in the default suite (all CI legs) + negative controls + det-basic == golden snapshot"
  - "crates/ostro-engine/examples/record_corpus.rs: testkit recorder (D21/D4/token/cancel/effect/child inputs, B1 pre-inputs, race-fuzz feed, D22 requeue clock), --dump inspector"
  - "crates/ostro-server/examples/export_run.rs: stopped dev/test data dir -> corpus entry (read_journal, blob-store module, meta token key, row-checked journal.bin, replay before write)"
  - "crates/ostro-partition/examples/gen_format_corpus.rs + tests/format_corpus.rs: D18 on-disk format corpus generator (P=2, every Cmd / outbox kind / intent) and verifier (generated-on-the-fly until 05-20 commits tests/format-corpus/v0.1)"
affects: [05-19, 05-20, phase-6, wasmtime-51-bump]

tech-stack:
  added: []
  patterns:
    - "Corpus entries are append-only: an entry that stops replaying is a determinism/compat bug, never re-blessed"
    - "Per-table digests (not the full digest) pin the frozen on-disk corpus, so later placeholder keyspaces (RESERVATIONS §6) cannot invalidate it; unrecorded tables must be empty"
    - "Example files double as test modules via #[path] (record_corpus shares tests/common; format_corpus includes gen_format_corpus)"

key-files:
  created:
    - tests/replay-corpus/README.md
    - tests/replay-corpus/.gitattributes
    - tests/replay-corpus/<24 entries>/{entry.toml,journal.bin,module.wasm}
    - crates/ostro-engine/tests/replay_corpus.rs
    - crates/ostro-engine/examples/record_corpus.rs
    - crates/ostro-server/examples/export_run.rs
    - crates/ostro-partition/examples/gen_format_corpus.rs
    - crates/ostro-partition/tests/format_corpus.rs
  modified:
    - crates/ostro-engine/Cargo.toml
    - crates/ostro-partition/Cargo.toml
    - Cargo.lock
    - .planning/ROADMAP.md
    - .planning/REQUIREMENTS.md

key-decisions:
  - "terminal_payload_sha256 = sha256(output bytes) for completed; sha256(encode_result(Failure|Continuation).bytes) for failed/cancelled/continued_as_new. Replay also requires the full outcome to equal the journal's terminal event"
  - "Replay runtime: entry token key + entry partition/run_id, node clock frozen 1 h after the last journaled ts (a faithful replay stamps nothing live)"
  - "journal.bin records must be canonical prost bytes with inline payloads, and every event must re-encode identically; entry dirs hold exactly the three files"
  - "export_run copies the data dir and opens the copy (meta campaign + Inspect node), so the source is never written; its JournalEvent encoder is hand-written (ostro-server has no prost dep, Cargo.toml not in scope) and checked byte-for-byte against the stored Events rows"
  - "Recorder default token key [9; 32] (engine suites' test key), partition 7, run id run-<entry>; det-basic recorded with the golden's exact spec (seed [1;32], det-run, partition 7)"
  - "Extra seeds beyond the plan list: sdk-retry-backoff (D22 requeue), signals-b1 (B1), race-fuzz-s8 (plain join), crash-saga-{completed,compensated,cancelled} (05-10 hand-off)"
  - "cancel-cleanup uses the wit-surface cancel-cleanup workflow (plan option 1)"
  - "Format corpus WAL rotation 4 KiB (8 segments); segments are not preallocated, so no trimming is needed"
  - "QA-02 left Partial in REQUIREMENTS (FOUND-05 precedent): mechanism complete and green on x86_64; aarch64/macOS replay runs on the CI legs (remote run human_needed); final entries at 05-20"

patterns-established:
  - "Replay corpus entry format + never-re-record rule"
  - "Format corpus per-table digest pinning"

requirements-completed: []

duration: ~75min
completed: 2026-11-19
---

# Phase 5 Plan 12: Replay Corpus + Format Corpus Tooling Summary

**QA-02 is in place. 24 committed replay-corpus entries replay byte-for-byte in the default `cargo test --workspace` run, which means on every CI leg. Each entry replays through the production linker with its recorded partition, run id and token key; the terminal payload must match and nothing may be appended. A testkit recorder and a server export tool add entries but never overwrite them. A D18 format-corpus generator and verifier pin the on-disk format per table, ready for 05-20 to commit v0.1.**

## Performance

- **Duration:** about 75 min (09:59Z to about 11:15Z), including a full gate run at load average 13 to 30.
- **Tasks:** 3 of 3, plus a small tooling commit (`record_corpus --dump`).
- **Files:** 7 code/doc files created, 3 manifests modified, 72 corpus files plus `.gitattributes`.

## Task Commits

1. **Task 1: corpus format, replay test, recorder, server export, README.** `d69df94` (feat)
2. **Tooling: `record_corpus --dump <entry-dir>`**, used to inspect entry shapes before committing. `facc141` (feat)
3. **Task 2: 24 seed entries** (the commit message lists them). `562f9d2` (test)
4. **Task 3: format-corpus generator and test.** `3cff369` (feat)
5. **Plan metadata** (this SUMMARY, ROADMAP, REQUIREMENTS): final docs commit.

TDD note: the tests were written together with the tools and proven on scratch corpora before any entry was committed:
- `OSTRO_REPLAY_CORPUS=<scratch>` held a det-basic recording and a server-exported sdk-basic run;
- the four negative controls exercise each failure path;
- the format test carries four negative controls of its own.

There are no separate RED commits.

## Accomplishments

### Replay corpus (QA-02)

- **Format:** `tests/replay-corpus/<entry>/{entry.toml, journal.bin, module.wasm}`.
  - `entry.toml` carries the fields below, plus informational `[recorded_with]` (ostro git describe, wasmtime pin, target, date, tool, command):
    - name, workflow, input_json?, expected;
    - **partition, run_id, token_key_hex, terminal_payload_sha256**;
    - journal_events, module_sha256, journal_sha256.
  - `journal.bin` holds `u32 LE len ‖ JournalEvent` protobufs with inline payloads, the same bytes as the `Events` rows.
- **`replay_corpus.rs`** runs in the default suite, so on every CI leg. For each entry it checks:
  - the entry has exactly the three files;
  - the sha256s and event count match;
  - every record is canonical protobuf and re-encodes identically;
  - `input_json` matches `RunStarted`;
  - the module prepares through the production linker;
  - one activation over the whole journal (entry token key, entry `RunRef`, node clock frozen 1 h past the last event) gives the outcome kind `expected`;
  - the outcome equals the journal's terminal event and the payload sha matches;
  - **0 events are appended**.

  Failures name the entry. An empty corpus passes with a bootstrap message.
- **Negative controls** (on temp copies):
  - a flipped journal byte or module byte → sha mismatch;
  - a truncated journal (last event dropped) → sha mismatch. Even with resealed metadata, the replay appends the terminal event and is detected;
  - wrong `expected`, wrong payload sha or bad token hex → reported.
- **`det_basic_entry_equals_golden_snapshot`:** the det-basic entry, rendered in the snapshot line format, equals `determinism__golden_det_basic_journal.snap` verbatim. The snapshot is untouched.
- **`record_corpus`** (engine example):
  - drives a guest with scripted inputs in the shapes the partition leader commits: D21 `SignalPayload`+`SignalMatched` (immediate, buffered then late match, or B1 `pre` before the first activation), `WaitTimedOut`, `TokenCompleted`, `CancelRequested`, canned effect and child completions, and `TimerFired` at `fire_at`;
  - moves the frozen clock to `retry_at` on a `StepBackoff` eviction (D22);
  - `--race-seed N` reproduces `phase5_guests.rs`'s race-fuzz input and feed exactly;
  - self-replays before writing and refuses an existing entry;
  - `--dump` prints an entry's events.
- **`export_run`** (server example):
  - copies a stopped data dir and never writes the source (checked: md5 of every file unchanged);
  - reads the token key from meta (campaign on the copy) and refuses to export without it;
  - opens the copy in Inspect mode, requires a terminal run, reads `read_journal` and the module from the blob store by `module_sha` (sha verified);
  - writes `journal.bin` with a hand-written prost-identical encoder, **byte-checked against every stored inline `Events` row**, replays the result, then writes the entry.

  Proven end to end: `ostro dev` → sdk-basic run → SIGINT → export. 16 rows were byte-checked, the export replays green, and a second export refuses to overwrite.

### Seed entries (24, 3,667,761 bytes = 3.7 MB, under the 8 MB cap)

| Entry | Expected | Events | Bytes |
|-------|----------|--------|-------|
| det-basic | completed | 13 | 106,676 |
| wit-surface | completed | 24 | 112,056 |
| cancel-cleanup (wit-surface) | cancelled | 6 | 110,409 |
| sdk-basic | completed | 16 | 171,543 |
| sdk-native | completed | 3 | 152,748 |
| sdk-retry-exhaust | failed | 7 | 119,710 |
| sdk-retry-recover | completed | 6 | 119,681 |
| sdk-retry-backoff | completed | 5 | 119,540 |
| race-fuzz-s1 / s2 / s3 / s8 | completed | 18 / 19 / 14 / 15 | ~179 k each |
| signals-fifo / timeout / before / b1 | completed | 14 / 5 / 9 / 6 | ~161 k each |
| tokens-complete / tokens-timeout | completed | 6 / 6 | ~159 k each |
| patch-v1 / patch-v2 | completed | 7 / 9 | 155,173 / 156,727 |
| can-chain | continued_as_new | 2 | 108,176 |
| crash-saga-completed / compensated | completed | 23 / 8 | ~181 k each |
| crash-saga-cancelled | cancelled | 11 | 181,083 |

Shape checks were made with `--dump` before committing:
- `sdk-retry-backoff` has exactly **one** SAF (Requeue, retry_at T0+3 s) and the completion stamped at retry_at (D22, recorded after `e7997a7`);
- `sdk-retry-recover` / `-exhaust` stamp in-activation attempts at their retry_at;
- `signals-fifo` has the immediate match in the payload batch, then late matches with `payload_seq` pointing back;
- `signals-b1` puts the payload at seq 1, before any guest command;
- `cancel-cleanup` has a `WaitResolved` on `CANCELLATION`, then the cleanup step;
- race-fuzz s1..s3 cover wait / select / select_join with every outcome class (timer, cancelled, signal, timeout), and s8 adds a plain `join`.

Every SDK entry was built with the D11 SDK. Modules are the release `guest_wasm` builds from the repo checkout. Recorded at `d69df94` (s8 at `facc141`), clean tree.

### Format corpus (D18 tooling; committed v0.1 is 05-20's)

- **`gen_format_corpus --out DIR`:**
  - P=2, **WAL rotation 4 KiB** (8 segments; segments are not preallocated, so no trimming);
  - **47 runs**, two leader terms;
  - every `Cmd` kind, every outbox kind inside `InboundBatch`, every `Derived` intent (a 26-name log census, all present);
  - 05-22 coverage: ReadyValue tenant Some/None, first true/false, `attempt_failures` > 0, `status_seq` set, outbox ChildCompleted with an inline and a blob event (one real 256 KiB + 1 blob).

  Outcomes: 0 unexpected rejections. The duplicates and the one RetryRun rejection are deliberate. A SeqMismatch race against the timer service (a past-due wait timeout journaled between read and submit) is retried as an executor would.

  **Size: 418,952 to 419,407 bytes of files (about 1 MB on disk with block rounding), against the < 2 MB target.**

  `format.toml` records:
  - per partition: `{digest, state_hash, leo, applied}`;
  - per partition: `[partition.tables]` blake3 per table, in the full digest's layout restricted to one table, with `checkpointed_offset` skipped;
  - per partition: `[partition.rows]`;
  - per partition: `[partition.commands]` (the census);
  - corpus-wide: total bytes, rotation and the ostro version.
- **`format_corpus.rs`** uses `tests/format-corpus/v0.1` if present, otherwise it generates into a TempDir. It copies the corpus, opens the copy in Inspect mode, and checks:
  - state_hash, LEO, applied and **each recorded per-table digest** equal format.toml;
  - tables not in format.toml are empty;
  - a stateless copy rebuilt from the log equals the opened copy (full digest included; the full digest is **not** compared to format.toml);
  - the census is complete.

  Negative controls: a wrong table digest, a wrong state hash, a missing partition record, and a non-empty table dropped from the record. It runs in about 15 s, generation included.
- Committed mode was exercised twice, and both copies were deleted afterwards (nothing under `tests/format-corpus/` is committed):
  - generating with the example into `tests/format-corpus/v0.1`;
  - an `rsync -m` copy with every empty directory pruned, as a git checkout would. fjall's empty `keyspaces/N/tables` dirs are not needed.

## Verification

- `cargo test -p ostro-engine --test replay_corpus --locked`: 5 passed; 24 entries and 252 events replayed in about 1.5 s.
- `cargo build -p ostro-engine --example record_corpus --locked` and `cargo build -p ostro-server --example export_run --locked`: OK.
- `cargo test -p ostro-partition --test format_corpus --locked`: 1 passed (generated mode, plus committed mode by hand).
- `cargo clippy -p ostro-partition -p ostro-engine -p ostro-server --all-targets --examples --locked -- -D warnings`: clean. `rustfmt --check`: clean.
- `ls -d tests/replay-corpus/*/ | wc -l` = 24 (≥ 17).
- Cargo.lock adds no new crates: only the member dependency lists changed (`bytes`, `prost`, `serde_json`, `toml` under ostro-engine; `toml` under ostro-partition).

## Final gate

`bash scripts/ci-local.sh --no-musl` ran in the detached scratch worktree `scratchpad/wt-05-12` at `3cff369`, with `CARGO_TARGET_DIR=target/wt-05-12`:
- PASS: toolchain, fmt, clippy `-D warnings`, warning-free build, SDK workspace (fmt/clippy/test/wasm), guests build, order-saga, cargo deny, check-vendored, deny fixtures, forbidden crates, workflow validation.
- FAIL, both known load flakes at load average around 30:
  - `cargo test`: `ostro dev_startup::warm_restart_under_1s` (warm restart 1.34 s against a 1 s budget);
  - failpoint suites: `ostro-wal group_commit::the_first_write_to_every_new_partition_commits`.

  Both steps stop at their first failure, so I reran them fully:
  - `cargo test --workspace --locked --no-fail-fast`: **exit 0**, 137 test binaries ok, dev_startup passing this time, replay_corpus and format_corpus included;
  - all five `FAILPOINT_CRATES` with `--features failpoints --no-fail-fast -- --test-threads=1`: **all exit 0**, including ostro-wal's first-write test and ostro-partition's format_corpus.

  The concurrent commit `ab623e2 test(05): harden load-sensitive tests` (another agent) targets exactly these two tests. No failure involves a 05-12 file.

## Deviations from Plan

1. **[Rule 2 - Coverage] Extra seed entries.** These were added on top of the plan's list:
   - `sdk-retry-backoff` (the D22 requeue path the 05-23 hand-off asked for);
   - `signals-b1` (the B1 shape from the 05-08 hand-off);
   - `race-fuzz-s8` (seeds 1..3 have no plain `join`);
   - `crash-saga-{completed,compensated,cancelled}` (the 05-10 hand-off's seed sources).

   Total size stays at 3.7 MB.
2. **[Rule 3 - Blocking] `export_run` encodes `JournalEvent` by hand.** ostro-server has no direct prost dependency, and its Cargo.toml is not in this plan's files. The encoder follows prost's rules (tag order, proto3 zero omission, message/oneof always present) and is byte-checked against the stored `Events` rows on every export. A divergence aborts the export.
3. **[Rule 2] `export_run` replays before writing.** It also opens a *copy* of the data dir (meta campaigns write to the meta log, and the WAL recovers on open), so "read-only" holds literally. The `entry.toml` it writes carries a header comment that `token_key_hex` is the dev dir's ephemeral key.
4. **[Rule 2] `OSTRO_REPLAY_CORPUS=<dir>`** lets the replay test check an entry before it is committed. **`tests/replay-corpus/.gitattributes`** (`* -text`, binary wasm/bin) stops EOL conversion from corrupting hashed bytes.
5. **[Rule 2] `--dump` mode for `record_corpus`** (separate commit `facc141`). `race-fuzz-s8` was first recorded while that change was uncommitted, so it carried `-dirty`. It was deleted and recorded again at `facc141` *before* any commit, so no committed entry was re-recorded.
6. **Format corpus extras:** `[partition.rows]` and `[partition.commands]` (census) in format.toml, plus the census assertion in the test. These are informational and checked from the log, not from the record.
7. `cancel-cleanup` uses wit-surface's `cancel-cleanup` workflow, which is the plan's first option.

## Issues Encountered

- `gen` is a reserved keyword in edition 2024, so the test module that includes the generator is named `corpus`.
- The format generator had a SeqMismatch race against the timer service (see above), handled by an executor-style retry.
- The shared working tree also carried other agents' uncommitted edits. Every commit went through a private index (`GIT_INDEX_FILE` seeded with `read-tree HEAD`), followed by `git reset -q -- <paths>`. `git show --stat` confirmed only 05-12 files in each commit.

## STATE notes (for the orchestrator; no gsd-tools mutators were run)

- Position: 05-12 complete (wave 4). Phase 5: **16/24 SUMMARYs** at the docs commit (05-11's SUMMARY landed first, `869e945`).
- Decisions to record:
  - corpus entries are append-only and never re-recorded;
  - `terminal_payload_sha256` is defined as above (output bytes / `encode_result` postcard);
  - the replay runtime uses the entry's token key, partition and run_id, with the clock frozen 1 h past the last event;
  - the format corpus is pinned by per-table digests, not the full digest, and unrecorded tables must be empty;
  - QA-02 is Partial until a remote CI run covers aarch64/macOS.
- Pending todo: the Wasmtime 51 bump (D17, after 05-20) must keep `replay_corpus` green.
- No blockers.

## Hand-off

- **05-19 (kill campaign, final crash corpus entries):**
  - export a campaign run with `cargo run -p ostro-server --example export_run -- --data-dir <stopped dir> --run <id> --entry crash-<name>`. Only completed / failed / cancelled / continued-as-new runs export; terminated and timed-out runs are refused (RunTerminated last, no RunCompleted). The data dir must be stopped and a dev/test dir.
  - For engine-level samples, `record_corpus crash-saga <entry> --input '<json>' --inputs '{"auto":["fire_timers",{"signal":{"name":"go","payload":"…","buffer":false}},{"timeout":"extra"}]}'` reproduces `saga_feed`.
  - Check every new entry first: `OSTRO_REPLAY_CORPUS=<scratch corpus> cargo test -p ostro-engine --test replay_corpus`.
- **05-20 (freeze):**
  - **Format corpus:** generate at the freeze commit with `cargo run -p ostro-partition --example gen_format_corpus -- --out tests/format-corpus/v0.1`, then commit `data/` and `format.toml`. `format_corpus.rs` switches to committed mode automatically.
    - Do this **after 05-16** if 05-16 makes apply write `RunStarted` at `StartRun` (the generator sets `StartRun.run_started` on some starts). Otherwise the per-table `events` digest is pinned to pre-05-16 apply behaviour.
    - fjall's empty `keyspaces/N/tables` dirs need not be committed; this was verified with a pruned copy. The `state/pNNNN/lock` files are empty and are tracked fine.
    - `.gitignore` excludes nothing in the corpus (`*.log` WAL segments included).
    - About 1 MB on disk. Record the commit in the freeze notes.
  - **D18 caveat:** per-table digests pin derived state values. Any later `VALUE_FORMAT` / `STATE_FORMAT` change that ships with a migrator (D18) changes the affected table digests of a *rebuilt* store. The freeze rules must then say how the format corpus is versioned, either a new `v0.N` directory next to `v0.1` or a migrator-aware comparison.
  - **Replay corpus:** add order-saga entries with `record_corpus examples/order-saga/target/wasm32-wasip2/release/order_saga.wasm order-saga-<case> --workflow order-saga --input '…' --inputs '…'` (a `.wasm` path needs `--workflow`). Record from a clean checkout of the commit you want in `recorded_with`.
  - RESERVATIONS flips: none needed by this plan, since no ordinals were used.
  - The README's entry table must gain a row for each new entry.
- **Wasmtime bump PRs (D17):** `cargo test -p ostro-engine --test replay_corpus` is the gate. A red entry is a bug in the bump, never a re-record.

## Self-Check: PASSED
