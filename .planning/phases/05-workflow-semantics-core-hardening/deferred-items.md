# Phase 5 deferred items

Out-of-scope discoveries logged by executors (not fixed in the plan that found them).

## From 05-19 (whole-server kill campaign)

- **No exclusive lock on the data directory.** Two `ostro dev --data-dir <same dir>`
  processes can run at once. A 05-19 development run left an orphaned server
  (FAILPOINTS `state::commit=1%return`) on a shard dir after the harness was stopped. The
  resumed harness started a second server on the same dir, and it came up and served next
  to the orphan for about 30 s; both were then killed and the dev data was discarded. The
  campaign now kills any process whose command line holds `--data-dir <shard dir>` before
  it resumes a shard (`server_target::kill_stale`). The product itself should still take
  an exclusive lock, e.g. a `flock` on `<data_dir>/LOCK` at startup with a clear error.
  Two writers on one WAL/state dir is a corruption risk. Candidate owner: the Phase 12 /
  ops hardening plans.
  **Resolved by 05-26:** `Server::start` holds `flock(LOCK_EX|LOCK_NB)` on `<data>/LOCK`;
  a second process fails with `ServerError::DataDirInUse`.
- **Executor-site faults are not logged at WARN.** `executor::before_append` /
  `after_append_before_done` faults on engine appends surface as `Unavailable` and the
  executor re-activates without a WARN line; only the RunStarted path logs one. So the
  campaign's informational `failpoint_hits` (counted from log lines) undercounts these
  sites. Correctness is unaffected; a debug or metrics counter would make the hits visible.
- **Replay-corpus wasm embeds a session path (05-20, 2026-11-20; blocks ci-local).**
  `scripts/check-vendored.sh` check 4 fails on the six entries added by `82c9333`:
  `tests/replay-corpus/{crash-campaign-s00-a,crash-campaign-s00-b,crash-campaign-s00-c,order-saga-cancelled,order-saga-compensated,order-saga-happy}/module.wasm`.
  Their guests were built inside a session scratchpad worktree, so the wasm panic-location
  strings carry that local path. The entries replay green (gnu and musl); only the
  path-leak check fails. Options (user decision): re-record the six entries from a build in
  a neutral path (their `module_sha256` changes; they are new in 05-20 and nothing depends
  on their hashes yet), or exempt corpus `module.wasm` files from check 4. Future corpus
  exports should build guests from the main checkout or a neutral path (e.g. with
  `--remap-path-prefix`).
