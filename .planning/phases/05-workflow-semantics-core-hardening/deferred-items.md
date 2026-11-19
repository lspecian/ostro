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
- **Executor-site faults are not logged at WARN.** `executor::before_append` /
  `after_append_before_done` faults on engine appends surface as `Unavailable` and the
  executor re-activates without a WARN line; only the RunStarted path logs one. So the
  campaign's informational `failpoint_hits` (counted from log lines) undercounts these
  sites. Correctness is unaffected; a debug or metrics counter would make the hits visible.
