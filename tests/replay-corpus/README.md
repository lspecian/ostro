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
| `race-fuzz-s1..s3` | race-fuzz | select / join / select_join, cancelled losers, signal vs timeout races | completed |
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

05-20 adds the final entries (order-saga and crash-campaign samples) at the freeze.
