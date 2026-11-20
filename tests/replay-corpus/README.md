# QA-02 replay corpus

Committed workflow journals that every build must replay exactly as recorded. The corpus
is the **Wasmtime-bump gate** (pin policy: one bump PR per Wasmtime release, corpus green)
and the **cross-architecture determinism proof**.

`crates/ostro-engine/tests/replay_corpus.rs` replays every entry. It is part of the default
`cargo test --workspace` run, so every CI leg runs it: linux-x86_64-musl,
linux-aarch64-musl, macos-aarch64 and linux-x86_64-gnu. Locally, `scripts/ci-local.sh`
covers x86_64 gnu and musl (the musl leg runs the test suite under `cargo-zigbuild test`).
aarch64 is covered only by the CI legs.

## Rules

1. **Never re-record, edit or delete an entry.** If a change breaks an entry, that is a
   determinism or compatibility bug in the change. Fix the change. Never re-bless the entry.
   The test also fails when an `entry.toml` sha256 does not match its files.
2. **New behaviour gets a new entry**, with a new name. Entries are append-only.
3. **Every Wasmtime bump PR keeps all entries green.** The same applies to `wit-component`,
   `wit-parser`, toolchain and SDK bumps. A failing entry blocks the bump.
4. `[recorded_with]` is **informational** (provenance only). The test never compares it.
5. **Server-exported entries** (`export_run`) contain the ephemeral await-token key of the
   dev/test data dir they came from. It is not a secret and never a production key, because
   exports come only from throwaway dev/test data dirs. **Never export from a production
   data dir.** Recorder entries use the engine suites' fixed test key `[9; 32]`.
6. Keep the corpus small. Before committing a batch, check the total size
   (`du -sh tests/replay-corpus`). Above 8 MB, drop redundant entries *before* committing,
   never after.
7. Record SDK journals only with the current SDK, which has D11 cancellation surfacing
   (05-06), and after the D22 retry clock (05-23, `e7997a7`). Older SDK journals are not
   expected live behaviour.

## Entry format

```
tests/replay-corpus/<entry>/entry.toml   # metadata (below)
tests/replay-corpus/<entry>/module.wasm  # exact component bytes
tests/replay-corpus/<entry>/journal.bin  # repeated: u32 LE length ‖ JournalEvent protobuf
```

`journal.bin` holds exactly the bytes of the partition `Events` rows
(`ostro_types::command::JournalEvent{seq, kind, payload}`, canonical prost encoding). Seqs
are contiguous from 0. Payloads are always inline: `export_run` fetches blob payloads and
inlines them. Each payload is the journal event codec
(`ostro_types::journal::encode_event`).

`entry.toml`:

| Key | Meaning |
|-----|---------|
| `name` | Entry name, equal to the directory name (`[a-z0-9-]+`). |
| `workflow` | `RunStarted.workflow`. |
| `input_json` | Run input text when it is JSON (optional; checked against `RunStarted`). |
| `expected` | `completed` \| `failed` \| `continued_as_new` \| `cancelled` (`RunFailed{Cancelled}`). |
| `partition`, `run_id` | The `RunRef` the run had. The replay uses the same values, so `context::info` and await-token minting reproduce. |
| `token_key_hex` | Await-token key the run was recorded with (32 bytes, hex). |
| `terminal_payload_sha256` | sha256 of the terminal payload: the output bytes for `completed`, the `encode_result` (postcard, `RESULT_CODEC_VER`) bytes of the `Failure` / `Continuation` otherwise. |
| `journal_events` | Number of events in `journal.bin`. |
| `module_sha256`, `journal_sha256` | sha256 of the two files. |
| `[recorded_with]` | `ostro` (git describe), `wasmtime`, `target`, `date`, `tool`, `command`. Informational. |

The replay test runs these checks for each entry:
- the entry has exactly the three files, and the sha256s and event count match;
- every record is canonical protobuf, and every event re-encodes identically;
- the module is prepared through the production linker (`Runtime::prepare`);
- one activation over the **whole** journal runs with a recording sink. It uses a runtime
  built with the entry's token key, the entry's `partition`/`run_id`, and a node clock
  frozen an hour after the last event;
- the outcome kind equals `expected`;
- the terminal payload equals the journal's terminal event and hashes to
  `terminal_payload_sha256`;
- **zero events are appended**, and the replay does not diverge (no NonDeterminism).

Negative controls in the same file run on temp copies of an entry:
- a flipped journal or module byte gives a sha mismatch;
- a journal that loses its terminal event fails, even with resealed metadata, because the
  replay appends events;
- a wrong `expected` or payload sha is reported.

`det_basic_entry_equals_golden_snapshot` renders the `det-basic` journal in the line format
of the det-basic golden snapshot and requires the snapshot verbatim. The snapshot is never
edited.

`OSTRO_REPLAY_CORPUS=<dir> cargo test -p ostro-engine --test replay_corpus` replays another
directory. Use it to check an entry before committing it.

## Recording

**Build every recorded guest path-neutrally** with `scripts/corpus-guest-build.sh`. A
guest's panic and `Location` strings embed the absolute source paths (repository, cargo
registry, the toolchain's rust-src), and a committed module keeps them forever. The script
runs the release wasm32-wasip2 build with `--remap-path-prefix` (repository root ->
`/ostro`, cargo home -> `/cargo`, rustup home -> `/rustup`, target dir -> `/target`,
`<sysroot>/lib/rustlib/src/rust` -> `/rustc/<rustc commit-hash>`), then checks the output:

```sh
scripts/corpus-guest-build.sh guests/Cargo.toml target/guests-corpus -p <guest>
scripts/corpus-guest-build.sh examples/order-saga/Cargo.toml target/order-saga-corpus
scripts/corpus-guest-build.sh --check <file.wasm>...   # home dir, /tmp, repo root, cargo/rustup home, session paths
```

`record_corpus` builds named guests this way (`target/guests-corpus`), and the crash
campaign deploys a crash-saga built this way. Both `record_corpus` and `export_run` run
`--check` on the written `module.wasm` and `journal.bin` and remove and refuse an entry that
fails it. For a server export, deploy the module from the script's output, not from a plain
`cargo build`. `scripts/check-vendored.sh` check 4 also rejects committed session paths.

**Engine recorder** (`crates/ostro-engine/examples/record_corpus.rs`). It drives a guest on
the engine testkit with scripted inputs, in the same shapes the partition leader commits:
- D21 `SignalPayload` + `SignalMatched` (immediate, buffered then late, or B1 before the
  first activation);
- D4 `WaitTimedOut`;
- `TokenCompleted`;
- `CancelRequested`;
- `TimerFired` at `fire_at`;
- a requeued retry moves the node clock to `retry_at` (D22).

The recorder self-checks the replay before writing, and refuses to overwrite an entry. See
the example's module docs for every flag and the `--inputs` action language. Run it from a
clean checkout of the commit you want in `recorded_with`:

```sh
cargo run -p ostro-engine --example record_corpus -- <guest|path.wasm> <entry> \
    --input '<run input>' [--inputs '<json script>'] [--race-seed N] ...
```

**Server export** (`crates/ostro-server/examples/export_run.rs`). It exports a terminal run
from a **stopped dev/test** data dir. The data dir is copied and never written. The tool
reads meta for the token key and opens the partitions in Inspect mode. It reads the journal
with `journal_io::read_journal` and checks the records against the stored `Events` rows. It
reads the module from the blob store, replays the run, then writes the entry:

```sh
cargo run -p ostro-server --example export_run -- --data-dir <dir> --run <run-id> --entry <name>
```

## Entries

The `command` key in each `entry.toml` reproduces the recording, for reading only. Never
re-run it over an existing entry.

| Entry | Guest / workflow | Covers | Expected |
|-------|------------------|--------|----------|
| `det-basic` | det-basic / `det` | std time, HashMap, rand, timer, patch marker; bytes equal the golden snapshot | completed |
| `wit-surface` | wit-surface / `surface` | every WIT 0.1 import: context, status, log, step, http/sql/object effects, sleep, sleep-until + cancel-op, signal wait, child, await token | completed |
| `cancel-cleanup` | wit-surface / `cancel-cleanup` | graceful cancel wins a wait, journaled cleanup step | cancelled |
| `sdk-basic` | sdk-basic | SDK steps, typed JSON, status | completed |
| `sdk-native` | sdk-native | std time / HashMap / rand inside and outside steps (WF-07) | completed |
| `sdk-retry-exhaust` | sdk-retry `exhaust` | 3 in-activation StepAttemptFailed, then the failed step | failed |
| `sdk-retry-recover` | sdk-retry `recover_hot` | in-activation retries stamped at `retry_at` (D22) | completed |
| `sdk-retry-backoff` | sdk-retry `long_backoff` | requeued retry: exactly one SAF, completion at `retry_at` (D22) | completed |
| `race-fuzz-s1`, `-s2`, `-s3`, `-s8` | race-fuzz `--race-seed N` | wait / select / select_join (s1-s3) and join (s8); cancelled losers, signal vs timeout races | completed |
| `signals-fifo` | signals `fifo` | SignalPayload+SignalMatched in one batch, then buffered payloads late-matched FIFO | completed |
| `signals-timeout` | signals `timeout` | WaitTimedOut | completed |
| `signals-before` | signals `before` | payload buffered during a sleep, matched at registration | completed |
| `signals-b1` | signals `long_wait` | B1: pending payload journaled right after seq 0, matched later | completed |
| `tokens-complete` | tokens | await token published via status, TokenCompleted once | completed |
| `tokens-timeout` | tokens | token wait timeout | completed |
| `patch-v1`, `patch-v2` | patch-v1 / patch-v2 `patch-demo` | before/after `patched("add-c")` (marker) | completed |
| `can-chain` | can-chain `{n:0,max:3}` | first link of a continue-as-new chain | continued_as_new |
| `crash-saga-completed` | crash-saga | timers, signal with timeout, join over timers + signal, retries | completed |
| `crash-saga-compensated` | crash-saga | step failure, reverse compensations | completed |
| `crash-saga-cancelled` | crash-saga | cancel during sleep, shielded undo steps | cancelled |
| `order-saga-happy` | examples/order-saga (server export, 05-20) | `ostro dev` + CLI: 5 SDK steps, durable timer, `approve`=true raced with cancellation | completed |
| `order-saga-compensated` | examples/order-saga (server export, 05-20) | `approve`=false: shielded reverse compensations (`refund_payment`, `release_inventory`) | completed |
| `order-saga-cancelled` | examples/order-saga (server export, 05-20) | graceful cancel during the settle timer / approval wait, shielded compensations | cancelled |
| `crash-campaign-s00-a` | crash-saga (server export from a shard-00 run of the 05-20 gate settings: seed 0x0520, 20 shards, 500 kills in the shard) | run `s00-562`: completed, start and completion 8 leader epochs apart (it lived through repeated kill -9 + restart), 5 grants, a step retry | completed |
| `crash-campaign-s00-b` | crash-saga (same shard-00 run) | run `s00-1421`: 7 epochs apart, 5 grants, step retries (one step failing twice) | completed |
| `crash-campaign-s00-c` | crash-saga (same shard-00 run, later in the shard) | run `s00-3929`: 7 epochs apart, 5 grants, step retries | completed |
| `fx-http-pay` | fx-http (server export, 06-20) | key-carrying POST `/pay` through the effects bridge: `EffectScheduled` with the guest request, `EffectCompleted` http 200 from the journal | completed |
| `fx-http-redrive` | fx-http (server export, 06-20, failpoint binary) | `bridge::effect::before_complete_submit=return(hang)`, charge, kill -9, restart: the replay re-drove `/pay` with the same derived key; one `EffectCompleted` (http 200, `idempotent-replayed: true`); provider saw 2 requests, 1 key, 1 charge | completed |
| `fx-sql-exactly-once` | fx-sql (server export, 06-20, PostgreSQL 14) | exactly-once `INSERT … RETURNING` on a `sql-postgres` connection with a templated password: tagged `text`/`int8` cells, `rows_affected` 1 | completed |
| `fx-object-local` | fx-object (server export, 06-20) | local object connection: put → get → head → presign-get (Ostro HMAC route URL) → delete → get `not_found` (W7 encodings) | completed |
| `fx-canary-scrubbed` | fx-canary (server export, 06-20) | `X-Api-Key: {{secret:canary-key}}` towards an echo provider: the echoed header is `[REDACTED]`; a canary scan of the entry (raw, base64 at 3 alignments, hex, percent) found 0 hits | completed |

05-20 adds the final entries (order-saga and crash-campaign samples) at the freeze.

06-20 (Phase 6 gate) adds the `fx-*` effect entries. They were exported from throwaway
`ostro server` data dirs against a loopback mock provider and a private PostgreSQL 14 cluster,
with guests built by `scripts/corpus-guest-build.sh`. They replay with **no `EffectDriver`**:
every completion comes from the journal, so the replay never contacts a provider or database.
Before committing, every file of the five entries was scanned for the canary secret and the
database password (raw, base64 std/url at all three alignments, hex, percent): 0 hits.

The six 05-20 entries (`order-saga-*`, `crash-campaign-s00-*`) were first exported from
guests built inside a session worktree, so their modules embedded that local path and
failed `check-vendored.sh` check 4. Before anything pinned their hashes, they were
re-recorded once (same scenarios, same tools) from guests built with
`scripts/corpus-guest-build.sh`: the order-saga flows on a fresh `ostro dev`, and the crash
samples from a new shard-00 campaign run with the gate's settings (500 kills, 0 violations).
Kill timing is not reproducible, so the crash samples are other runs of the same shape
(completed, 7-8 leader epochs between start and completion, re-dispatch after kills, step
retries). This is the only re-recording; rule 1 holds from here on.

The remap removes every local path. A guest of the `guests/` workspace (crash-saga) also
builds byte-identically from any checkout path. `examples/order-saga` is a standalone
workspace whose `ostro-sdk` path dependency lies outside it, so cargo's package metadata hash
still depends on the checkout path. Its bytes are leak-free but not path-independent.
