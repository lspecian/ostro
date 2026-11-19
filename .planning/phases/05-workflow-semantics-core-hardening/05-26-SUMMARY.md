---
phase: 05-workflow-semantics-core-hardening
plan: 26
subsystem: server
tags: [data-dir, flock, lock, corruption-guard, qa-01, ops]

requires:
  - phase: 05-19
    provides: "deferred-items finding: two `ostro dev` processes served one data dir for ~30 s"
  - phase: 04-17
    provides: "Server::start bootstrap order (resolve data dir -> bind -> meta -> runtime -> node -> API)"
provides:
  - "Exclusive advisory lock `flock(LOCK_EX|LOCK_NB)` on `<data>/LOCK` in Server::start, before any other disk work"
  - "ServerError::DataDirInUse { path, lock } with a one-line message; ostro_server::LOCK_FILE"
  - "docs/storage-format.md: `<data>/LOCK` entry (append-only)"
affects: [05-20, 12, 14]

tech-stack:
  added: []
  patterns:
    - "std File::try_lock (Rust 1.89+) for advisory locks; no new crate"
    - "Lock descriptor owned by the long-lived handle, explicitly unlocked at the end of graceful shutdown"

key-files:
  created:
    - crates/ostro-server/tests/data_dir_lock.rs
    - crates/ostro/tests/data_dir_lock.rs
    - .planning/phases/05-workflow-semantics-core-hardening/05-26-SUMMARY.md
  modified:
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/lib.rs
    - docs/storage-format.md
    - .planning/ROADMAP.md
    - .planning/phases/05-workflow-semantics-core-hardening/deferred-items.md

key-decisions:
  - "The lock lives in Server::start, not Node::open: Server is the lowest layer that owns the whole data dir (meta and the AOT cache are opened outside Node), and Node::open is also the entry point of the read-only tooling"
  - "Lock taken right after data-dir resolution and before the listener bind, so with both port and dir busy the in-use error wins"
  - "Tooling that opens Node/meta directly (Inspect mode, export_run, gen_format_corpus, the crashtest checker) takes no lock and is documented as requiring a copy or a stopped directory"

requirements-completed: []

duration: ~25min
completed: 2026-11-19
---

# Phase 5 Plan 26: Exclusive data-dir lock Summary

**`Server::start` now holds `flock(LOCK_EX|LOCK_NB)` on `<data>/LOCK` (std `File::try_lock`) from before meta/node/blob/AOT open until the end of shutdown. A second `ostro` on the same directory exits 1 with one clear error line. SIGKILL releases the lock, and a leftover LOCK file is harmless.**

## What changed

- `crates/ostro-server/src/server.rs`: `lock_data_dir(&data_dir)` runs right after `resolve_data_dir`, which handles both persistent and ephemeral dirs. It opens `<data>/LOCK` (create, no truncate) and calls `try_lock()`. `TryLockError::WouldBlock` maps to `ServerError::DataDirInUse`, and any other error maps to `ServerError::DataDir`. The `File` is stored in `ServerHandle.data_lock`. `shutdown()` unlocks and drops it after node and meta have stopped and before the ephemeral dir is removed. Every early-return error path in `start` drops it automatically. The module docs (startup/shutdown order) were updated.
- Error text (Display of `ServerError::DataDirInUse`):
  `data dir <path> is in use by another ostro process (lock <path>/LOCK)`.
  The binary prints `ostro: error: data dir /x/data is in use by another ostro process (lock /x/data/LOCK)` and exits with code 1.
- `ostro_server::LOCK_FILE = "LOCK"` is exported.
- `docs/storage-format.md`: one appended bullet describing `<data>/LOCK`. Its content is unused and it is not part of the format; older code ignores it under F8.

## Tooling audit

| Tool | Opens | Lock | Status |
|------|-------|------|--------|
| `ostro-server/examples/export_run.rs` | meta + Node (Inspect) on a **copy** | none | unaffected; a copied LOCK file carries no flock |
| `ostro-partition/examples/gen_format_corpus.rs` | Node on its own fresh dir | none | unaffected |
| `ostro-crashtest` checker (`open_node(.., Inspect)`) | the data dir after the target is killed, plus a copy | none | unaffected (the server is dead) |
| `ostro-crashtest` server target | spawns `ostro dev`; `kill_stale` remains | binary takes it | an orphan now makes the resumed start fail fast instead of double-serving |

None of them opens a live directory, so none takes a shared lock. The doc line states that they need a copy or a stopped directory.

## Tests

- `crates/ostro-server/tests/data_dir_lock.rs` (4 tests):
  - `second_start_on_same_dir_fails_then_works_after_shutdown`: exact message, one line. The first server stays healthy, and the dir starts again after a graceful shutdown.
  - `foreign_lock_holder_blocks_start_and_stale_lock_file_does_not`: a pre-existing "stale" LOCK file does not block a start. A probe descriptor gets WouldBlock while the server runs and gets the lock after shutdown. A foreign holder blocks a start.
  - `fresh_dir_lock_is_taken_before_any_other_disk_work`: when the lock is held, a failed start leaves only `LOCK` in the dir (no meta/wal/state/cache).
  - `ephemeral_server_locks_its_temp_dir`.
- `crates/ostro/tests/data_dir_lock.rs` (spawned binary): `second_dev_on_same_dir_fails_and_sigkill_releases_the_lock`. A second `ostro dev --data-dir` exits 1 with exactly one `ostro: error:` line and never prints a ready line, while the first keeps serving. After `kill -KILL` on the first, the LOCK file is still there, a third start succeeds, stops cleanly on SIGINT, and a fourth start succeeds after that.
- `cargo test -p ostro-server -p ostro --locked`: all pass (28 test binaries, 0 failures). `cargo clippy -p ostro-server -p ostro --all-targets --locked` and `cargo check --workspace --all-targets --locked` show no warnings. Partition code was not touched.

## Deviations from Plan

None. The plan offered Server::start or Node::open as the lock owner, and Server::start was chosen (see key-decisions).

## Deferred Issues

None.

## Self-Check: PASSED

- Code commit `b45476a` and doc commit `a49bb5d` are on main. The created files exist.
