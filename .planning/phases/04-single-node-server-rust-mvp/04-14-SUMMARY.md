---
phase: 04-single-node-server-rust-mvp
plan: 14
subsystem: partition-leader
tags: [dispatcher, leases, executor-trait, ready-queues, no-polling, fencing]
requires:
  - phase: 04-12
    provides: query::{running_runs, ready_entries}, Running keyspace, Action::InputAppended
  - phase: 02-14
    provides: leader service structure (timers.rs), spawn_all, LeaderCtx
provides:
  - ostro_partition::leader::dispatcher (re-exported at the crate root) with Executor, Job, ExecOutcome, LeaseRenewer, DispatcherConfig, DispatcherStats, spawn()
  - LeaderConfig.dispatcher: Option<DispatcherConfig> (manual Debug); spawn_all spawns the dispatcher when Some
  - DispatcherConfig::new(executor) defaults (lease_ttl 30 s, 64 slots) and DEFAULT_LEASE_TTL / DEFAULT_SLOTS
  - tests/dispatcher.rs fake-executor suite (12 tests) + 6 in-module State tests
affects: [04-16, 04-17, 04-20, 07]
tech-stack:
  added: []
  patterns:
    - "Grant in a task, then drain the action stream before handling the grant result: EnqueueReady seen while Granting predates the grant (ignored); seen while Executing means the executor requeued (lease dropped, entry queued)"
    - "Lease generation (not token) identifies the current job; any outcome with a stale generation is ignored"
    - "Slot permits live in the lease, not the executor future: a revoked zombie does not hold a slot"
    - "Lease deadline is an AtomicU64 (ms since a tokio Instant base) shared with LeaseRenewer; the loop keeps a (check_at, run) set and re-reads the atomic at check time, so renewals need no channel"
key-files:
  created:
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-partition/tests/dispatcher.rs
  modified:
    - crates/ostro-partition/src/leader/mod.rs
    - crates/ostro-partition/src/lib.rs
key-decisions:
  - "Grants run concurrently in JoinSet tasks (so group commit can batch them); a lease exists from dispatch time in phase Granting, so InputAppended seqs sent before the job exists are buffered in its channel"
  - "Leases in phase Granting never expire (the grant completes or fails on its own); expiring them could race two grants into the same n and strand the run"
  - "Retry keeps the run Running in the store and parks a re-grant entry in the delay set (ready_at = now + after_ms); Job.failures counts consecutive Retry outcomes and resets on Released / RunCompleted"
  - "Rebuild and expiry re-dispatch use ready_at 0 so re-grants go ahead of same-priority queued work; queues are served round-robin"
  - "Grant rejections (RunTerminal, RunNotFound, Fenced, StaleEpoch) drop the entry; other grant errors re-queue after 100 ms unless the machine is unhealthy, which stops the dispatcher"
  - "An executor future that panics is treated as Retry after 1 s (logged error)"
  - "DispatcherStats kept exactly the four contract fields; the startup rebuild is simply not counted in rescans"
  - "Shutdown revokes every lease, waits min(lease_ttl, 5 s) for executor futures, then aborts the JoinSet"
requirements-completed: [DISP-01, DISP-02]
duration: 45min
completed: 2026-11-19
---

# Phase 4 Plan 14: Dispatcher leader service Summary

**Each partition leader can now run a dispatcher behind an `Executor` trait. It rebuilds its ready queues from the `ready` and `running` keyspaces on leadership and re-grants runs left Running by the old epoch at once. It wakes only on actions, deadlines, free slots and job results; it has no interval timer. It also manages soft leases: a lease that is not renewed within `lease_ttl` is revoked, the run is re-granted as n+1 and re-dispatched, and the old job's late outcome is ignored. A fake executor proves all of this.**

## Performance

- **Duration:** about 45 min, including ci-local
- **Tasks:** 2. Both are in one source file, so they share one feat commit plus one test commit.
- **Files:** 2 created, 2 modified

## Accomplishments

- **Contract, created exactly as specified in the plan:** `Job`, `ExecOutcome`, `Executor`, `DispatcherConfig`, `DispatcherStats`. `LeaseRenewer` provides `renew() -> bool`, `is_revoked()`, `ttl()` and `expires_at()`. `expires_at()` lets 04-16 arm its self-fence timer.
- **State:** `queues: BTreeMap<queue, BTreeSet<(Reverse(priority), ready_at, run)>>`, plus `entries` (which carry the concurrency key), a `delayed` set, `leases`, a lease `checks` set and `failures`. BTreeMap/BTreeSet only.
- **Rebuild:** subscribes to actions before the scan, then runs `ready_entries(usize::MAX)` and `running_runs` + `run_record` inside `spawn_blocking`. Running runs are queued for an immediate re-grant.
- **Wake-ups:** biased `select!` over shutdown, `actions.recv()`, `joins.join_next_with_id()`, `slots.acquire_owned()` (only when ready work is waiting) and `sleep_until(min(next delayed ready_at, next lease check))`.
  - `Lagged` triggers a rescan and `rescans += 1`.
  - `grep "interval("` finds nothing.
- **Dispatch:** permit, then a lease in phase Granting, then a task that runs `grant_attempt` and `read_run`, then the grant result. The loop drains the actions before handling that result.
- **Outcomes:**
  - Released: drop the lease and reset failures.
  - Reactivate: re-read the record and run the same token again, with a new inputs channel and a refreshed deadline; `reactivated += 1`.
  - Retry{after_ms}: drop the lease, `failures += 1`, and add a delayed re-grant.
  - Fenced: drop the lease.
- **Leases:**
  - The deadline lives in an atomic shared with `LeaseRenewer`.
  - On expiry: `revoked = true`, the permit is freed, `lease_expired += 1`, a warn log, and the run is re-queued for grant n+1.
  - On shutdown: every lease is revoked, followed by a bounded grace period.
- **InputAppended** sends the seq to the leased job's inputs. It is ignored for runs without a lease.
- **spawn_all** spawns the dispatcher between timers and retention when `cfg.dispatcher.is_some()`. Every Phase 2 suite runs with `None`, the default.

## Task Commits

1. **Tasks 1 and 2: dispatcher core and soft leases (src):** `cb1eb05` (feat)
2. **Tasks 1 and 2: fake-executor integration suite:** `09b3f01` (test)

## Tests

`tests/dispatcher.rs` has 12 tests, about 2.7 s per run. It was stable across 4 consecutive runs.

| Test | Proves |
|---|---|
| dispatches_by_priority_with_first_token_and_concurrency_key | slots=1: priority 5 first, then same priority by run id; token (epoch, 1); concurrency key on job.record |
| future_ready_at_is_not_dispatched_early | StartRun with ready_at +400 ms: nothing at +200 ms, dispatched after ready_at |
| executor_requeue_with_future_ready_at_waits_in_the_delay_set | EnqueueReady{now+300} from the executor: re-dispatched no earlier than +280 ms as n=2 |
| rebuild_after_restart_regrants_running_under_the_new_epoch_without_polling | 2 Ready + 2 Running runs before the restart: all 4 jobs carry (new epoch, 1) and their concurrency keys; rescans stays 0 and nothing more is dispatched during 500 ms idle |
| slots_bound_concurrent_jobs | slots=1: at most 1 job in flight |
| reactivate_reuses_the_token / retry_grants_a_new_attempt_after_the_delay / fenced_drops_the_lease | the three outcomes; Retry gives (n, failures) = (1,0), (2,1), (3,2) with gaps of at least 190 ms |
| enqueue_ready_for_a_leased_run_drops_the_lease_and_requeues | the executor's own requeue revokes it; n=2 job |
| renewing_executor_keeps_its_lease | renew every ttl/3 for 3 x ttl: no expiry |
| stalled_executor_is_revoked_regranted_and_its_late_outcome_ignored | ttl 300 ms: revoked within 250..2000 ms; renew() is false afterwards; logged current_attempt n=2; lease_expired == 1; the successor's lease survives the zombie's late Released |
| shutdown_revokes_every_lease_and_waits_for_jobs | 3 jobs blocked on revoked: all are revoked and awaited, and shutdown finishes in under 4 s |

The in-module tests cover the pure `State` handling:
- InputAppended forwarding (only to the leased run)
- ordering, the delay set and the concurrency key from EnqueueReady
- stale EnqueueReady while Granting, and requeue while Executing
- RunCompleted cleanup
- the concurrency key from the scan, and re-granting unleased running runs
- round-robin across queues

## Verification

- `cargo test -p ostro-partition --test dispatcher --locked`: 12/12.
- `cargo clippy -p ostro-partition --all-targets --locked -- -D warnings`: clean.
- `cargo test -p ostro-partition --locked`, run at c49df7a plus these changes: every suite green except 04-13's RED `apply_timer_input`. That suite was expected to fail at that commit, and 04-13's feat `6fa5167` fixes it.
- `bash scripts/ci-local.sh --no-musl` was run at 177b787 (04-13 fix + 04-14 + 04-15) in a scratch worktree with a private target dir `target/wt14`.
  - Steps that passed: toolchain, fmt, clippy (15 s), warning-free build (33 s), the full workspace `cargo test` (537 s), deny, check-vendored, deny-fixtures, forbidden-deps and workflow validation.
  - **The failpoint step failed once.** In `rebuild_digest::live_state_equals_rebuilt_state` (`--features failpoints`, single-threaded), `Node::open` timed out (10 s). The machine's load average was about 12, with three agents running CI at once. The dispatcher is not involved: rebuild_digest runs with `dispatcher: None`.
  - **Re-runs at the same commit and target dir: green.** `rebuild_digest` with failpoints passed 3/3 (185 s). The rest of the failpoint step also passed: the full `ostro-partition --features failpoints -- --test-threads=1` suite, plus clippy and the tests of `ostro-crashtest --features failpoints` (exit 0).
  - The musl legs were skipped (`--no-musl`).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Correctness] Stale pre-grant `EnqueueReady` vs a genuine requeue**
- **Found during:** Task 1 design
- **Issue:** The subscription starts before the rebuild scan, and the apply thread sends actions after commit. So an `EnqueueReady` that the scan already covered can arrive after the dispatcher has leased the run. Handled naively, it would revoke a fresh lease and grant n+1.
- **Fix:** The lease has two phases. Every grant or job result is handled only after draining the action channel. Apply commits, acks and then fans out actions one chunk at a time, so every action applied before the grant is in the channel by then. An `EnqueueReady` while Granting is ignored, because the grant supersedes it. One that arrives while Executing is treated as a requeue.
- **Commit:** cb1eb05

**2. [Rule 1 - Bug avoidance] Permits held by leases instead of executor futures**
- **Issue:** The plan says to spawn the executor future "holding the permit". Then a stalled zombie that never returns would hold its slot forever, and with slots=1 the re-dispatch after lease expiry could never run.
- **Fix:** The permit lives in the `Lease`. Revocation frees it. Documented in the module docs: slots bound leased jobs, and a zombie briefly overlaps its successor.

**3. Contract addition (additive only):** `DispatcherConfig::new`, `DEFAULT_LEASE_TTL` and `DEFAULT_SLOTS`, plus `LeaseRenewer::{is_revoked, ttl, expires_at}`. `DispatcherStats` keeps exactly the four fields.

**4. Commit shape:** the plan's TDD RED/GREEN split per task was not followed. Lease state is part of the core loop, so Tasks 1 and 2 landed as one feat commit (src) and one test commit.

**5. Build isolation:** the shared `target/` was being rebuilt by parallel executors' worktrees. Cargo hashes workspace path packages relative to the workspace root, so worktrees overwrite each other's artifacts, and one test build linked a stale ostro-partition rlib. Verification used `CARGO_TARGET_DIR=/home/lspecian/Development/ostro/target/wt14`.

## Known limitations / notes for later plans

- `ready_entries(usize::MAX)` loads the whole backlog on rebuild and rescan. That is fine for Phase 4; Phase 7 (quotas, concurrency) may need a bounded window like the timer service's.
- A `ManualClock` advance does not wake the dispatcher. Delayed ready times use real-time sleeps computed from the clock. Tests use `SystemClock`.
- An `EnqueueReady` for run R placed in the same apply chunk as, and before, R's grant may not yet be in the channel when the grant result is handled. It is then treated as a requeue: one wasted attempt n+1, never a lost run.
- Concurrency keys are carried but not enforced (Phase 7).

## Hand-off: 04-16 (EngineExecutor)

- **Implement `Executor::execute(job) -> Pin<Box<dyn Future<Output = ExecOutcome> + Send>>`.**
  - `job.record` is read right after the grant. On Reactivate it is re-read, with the same token.
  - The first attempt is the one where `job.record.next_seq == 0`. Write RunStarted there.
- **Lease:**
  - Call `job.lease.renew()` after every successful append and on a heartbeat (`lease_ttl / 3`).
  - `renew()` returns false once the lease is revoked. Stop then.
  - Self-fence: bridge `job.revoked` (a `watch::Receiver<bool>`, true = stop) to the engine's `CancellationToken`, and also arm a local deadline from `job.lease.expires_at()`.
- **Inputs:** `job.inputs` yields seqs of inputs journaled while leased. Read them with `query::journal_rows(r, run, seq, 1)`. The channel is per job: a Reactivate gets a fresh channel, and seqs sent between jobs are not replayed, so read the journal from `record.next_seq` on activation.
- **Outcome mapping:**
  - Completed, Failed, Suspend accepted, or the executor's own EnqueueReady: `Released`.
  - Hot-wait elapsed, budget pressure, AppendConflict, or Suspend answered `SeqMismatch`: `Reactivate`.
  - Retryable AttemptFailed with `job.failures < max_attempt_failures`: `Retry{after_ms}`. Otherwise CompleteRun{Failed}, then `Released`.
  - `Fenced`, `StaleEpoch`, or `Invalid(NotRunning)` on Suspend: `Fenced`.
- **The dispatcher revokes and requeues by itself** when it sees an `EnqueueReady` for a run whose job is executing. If the executor requeues through `EnqueueReady`, expect `revoked` to fire right after the append, and return Released.
- **Config:** use `DispatcherConfig { executor, lease_ttl, slots: Arc<Semaphore> (node-wide), stats }` in `LeaderConfig.dispatcher`. The research default is `lease_ttl` 30 s, which must exceed the activation timeout or be renewed by a heartbeat.
- **Zombie suite (tests/leases.rs):** a stalled executor gets revoked after `lease_ttl`, and the n+1 job is dispatched while the zombie keeps running and holds no slot. The zombie's late append is rejected by apply as `Fenced`.

## Self-Check: PASSED

- FOUND: crates/ostro-partition/src/leader/dispatcher.rs (`pub trait Executor`, `subscribe_actions`, `grant_attempt`)
- FOUND: crates/ostro-partition/tests/dispatcher.rs
- FOUND: crates/ostro-partition/src/leader/mod.rs (`dispatcher::spawn`)
- FOUND: commits cb1eb05, 09b3f01
- `grep "interval(" dispatcher.rs`: no match
