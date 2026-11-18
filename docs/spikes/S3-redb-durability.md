# Spike S3: redb durability tiers (and the fjall fallback)

Status: complete (FOUND-06, S3 half). Run on 2026-11-18. The quiet-machine confirmation run, the fjall crash test and the read-modify-write benchmark (plan 01-08) are at the end of this report. **Outcome: fjall 3.1.13 adopted for STOR-05** (see "Confirmation run").
Harness: `spikes/s3-redb-bench` (a standalone cargo workspace, `redb = "=4.6.0"`, optional `fjall = "=3.1.13"`).
Runner: `spikes/s3-redb-bench/run.sh`. Raw data is in `docs/spikes/data/S3/`:
- `env.txt`
- `results.jsonl` (redb, 5 tiers x 3 reps)
- `crash.jsonl` (5 kill -9 runs)
- `fjall.jsonl` (paired redb T2 and fjall reps)
- `confirm.jsonl` (01-08: interleaved redb/fjall T2, plain and read-modify-write, 3 reps)
- `crash-confirm.jsonl` (01-08: 5 kill -9 runs per engine with the row-level torn-batch check)

## Summary

redb 4.6 does **not** reach the locked threshold in the planned durability mode. T2 is `Durability::None` per batch, plus an `Immediate` checkpoint every 100 ms or every 256 batches.
- **T2 median:** about 62k commands/s on the loaded machine, and about 66k commands/s in a quieter re-run.
- **T3 upper bound (no fsync at all):** only 85–91k commands/s. The limit is redb's CPU cost per B-tree write under random 16-byte keys, not fsync. About 30% of T2's wall time also goes to the Immediate checkpoints, at about 53 ms each.

The kill -9 crash test passed 5/5 for the T2 design:
- The database reopens.
- `applied_offset` equals the last checkpoint that was reported as durable.
- No batch was torn.

As the rule requires, the same workload then ran on fjall 3.1.13. Its T2 analogue (`WriteBatch` per batch + `persist(SyncAll)` every 100 ms) sustained a median of **242k commands/s**, 3.7x redb under the same load.

Decision rule: FAIL — T2 median = 62,404 commands/s (= 20,801 steps/s) vs threshold 90,000 commands/s

The paired quieter re-run of redb T2 (median 65,831 commands/s, load about 1.7) also fails. fjall T2 passes the same threshold with a large margin.

## Environment

From `docs/spikes/data/S3/env.txt`:

| Item | Value |
|------|-------|
| Machine | AMD Ryzen 9 7950X3D, 12 vCPUs visible to WSL2, 48 GB RAM |
| Kernel | Linux 6.6.87.2-microsoft-standard-WSL2 (x86_64) |
| Filesystem | ext4 on `/dev/sdd` (WSL2 VHDX), `rw,relatime,discard,data=ordered`; db dir `/tmp/ostro-s3` (ext4, not tmpfs, not /mnt 9p) |
| Toolchain | rustc 1.100.0, `--release` |
| Load average (main run) | 3.97 at start, 5.2–6.9 during the tier runs (parallel wave-1 builds by plans 01-01/01-03) |
| Load average (fjall paired run) | 1.6–2.5 |

**Caveats:**
- WSL2 fsync goes through the Hyper-V virtual disk (VHDX) to the Windows host. Its latency and ordering guarantees are not those of bare-metal Linux on NVMe.
- The machine was also loaded by concurrent builds during the main run.
- **Treat these numbers as relative between tiers and engines. They are not absolute and are not NFR-2.5 evidence.** The NFR measurement belongs to the Phase 14 benchmark on reference hardware.

## Workload

This emulates the partition state machine's `apply_batch`:
- **Batch:** B = 64 commands per write transaction.
- **Per command:**
  - insert 1 `runs` row: `&[u8;16]` run id → 200-byte value
  - insert 1 `runs_by_status` index row: `&[u8;17]` (status byte + run id) → `()`
  - insert 1 `outbox` row: `u64` seq → 64-byte value
  - delete 1 older outbox row (a ring of 1024, so the outbox stays at about 1024 rows)
  - That is 3 inserts + 1 delete per command.
- **Per batch:** the same transaction also writes `meta["applied_offset"]` = the command offset after the batch.
- **Keys:** random 16-byte run ids (the realistic B-tree case), from a seeded `StdRng`.
- **Size:** 200,000 measured commands (3,125 batches) after a warm-up of 19,968 commands into the same fresh db file. Wall time covers the measured part, including checkpoints and the final Immediate commit.
- **Steps/s** assumes **3 commands per workflow step** (`equivalent_steps_per_s = cmds_per_s / 3`).

**Tiers:**
- **T1:** every batch commits with `Durability::Immediate`.
- **T2:** every batch commits with `Durability::None`. A separate `Immediate` txn writes `meta["checkpointed_offset"]` when ≥100 ms have passed or 256 batches since the last one. **This is the planned design.**
- **T3:** every batch commits with `None`, with one `Immediate` at the end.
- **T2-2pc:** T2 + `set_two_phase_commit(true)`.
- **T2-qr:** T2 + `set_quick_repair(true)`.

In redb 4.6, two-phase commit and quick-repair are **per-`WriteTransaction`** settings, not `Builder` settings. The bench applies them to every transaction.

**Latency columns:**
- "commit" is the `tx.commit()` call alone.
- "txn" is begin → inserts → commit for one batch.
- "checkpoint" is the whole Immediate checkpoint txn.

## Results (redb 4.6, main run, median rep of 3)

| Tier | cmds/s (3 reps) | median cmds/s | steps/s | batches/s | commit p50/p99/max (us) | txn p50/p99/max (us) | checkpoint n, p50/p99/max (us) | db size | loadavg before→after |
|------|-----------------|---------------|---------|-----------|-------------------------|----------------------|-------------------------------|---------|----------------------|
| T1 | 9,014 / 9,863 / 10,029 | 9,863 | 3,288 | 154 | 4,808 / 15,664 / 27,972 | 5,447 / 16,657 / 28,978 | - | 74 MiB | 5.72→6.63 |
| **T2** | 62,404 / 47,931 / 63,364 | **62,404** | **20,801** | 975 | 190 / 327 / 723 | 670 / 1,044 / 3,109 | 21, 53,230 / 72,455 / 72,455 | 136 MiB | 6.88→6.49 |
| T3 | 90,612 / 84,386 / 90,887 | 90,612 | 30,204 | 1,416 | 211 / 347 / 5,109 | 658 / 1,073 / 5,573 | - | 130 MiB | 6.49→6.13 |
| T2-2pc | 63,623 / 58,852 / 64,339 | 63,623 | 21,208 | 994 | 186 / 325 / 846 | 658 / 997 / 5,060 | 21, 56,336 / 68,848 / 68,848 | 136 MiB | 6.13→6.13 |
| T2-qr | 65,534 / 61,157 / 61,393 | 61,393 | 20,464 | 959 | 189 / 353 / 726 | 670 / 1,161 / 3,356 | 22, 57,474 / 79,752 / 79,752 | 135 MiB | 5.60→5.23 |

**Observations:**
- **The bottleneck is CPU, not fsync.**
  - Even T3 (no fsync) only just touches 90k commands/s.
  - Batch txn time (about 650 us) is dominated by the 256 B-tree mutations, not by `commit()` (about 190 us).
- **Checkpoints are expensive.** An Immediate checkpoint after about 100 ms of None commits costs 53 ms p50 and 72 ms max, because it flushes every dirty page of the random-key trees. About 21 checkpoints x 53 ms is about 1.1 s of T2's 3.2 s wall time.
- **Group-commit window (informational, 10 ms reference window, an assumption):**
  - The T2 per-batch commit p99 is 0.33 ms (txn p99 1.0 ms), well inside 10 ms.
  - The checkpoint commits (53–72 ms) are **not** inside one window. Any batch that lands behind a checkpoint waits 50+ ms.
- **Throughput depends on database size.** The smoke run (20k commands, a much smaller tree) measured T2 at 128k commands/s. The 200k run's tree (136 MiB) is the relevant one, and a real partition grows further.
- **Load:** the low T2 rep 2 (47.9k, with 27 checkpoints) coincides with load at about 6.8.

## Crash sanity (T2, kill -9 after 3 s, 5 runs)

| Run | open ok | open time (incl. repair) | repair callbacks | last CKPT seen | applied_offset | checkpointed_offset | gap applied − last CKPT | pass |
|-----|---------|--------------------------|------------------|----------------|----------------|---------------------|-------------------------|------|
| 1 | yes | 100.2 ms | 3 | 189,952 | 189,952 | 189,952 | 0 | yes |
| 2 | yes | 79.0 ms | 3 | 201,152 | 201,152 | 201,152 | 0 | yes |
| 3 | yes | 82.5 ms | 3 | 206,464 | 206,464 | 206,464 | 0 | yes |
| 4 | yes | 83.6 ms | 3 | 208,256 | 208,256 | 208,256 | 0 | yes |
| 5 | yes | 77.1 ms | 3 | 197,440 | 197,440 | 197,440 | 0 | yes |

All three asserts held in every run:
- (a) the db opens without error
- (b) `checkpointed_offset ≥ last_ckpt` and `applied_offset ≥ last_ckpt`
- (c) `applied_offset % 64 == 0`

The gap is always 0. Every None commit after the last Immediate is discarded on reopen, even after a process crash where the OS page cache survives. Recovery always rolls back to the last durable commit. redb ran a full repair each time (repair callback invoked; db about 136 MiB at kill; about 80 ms). That cost grows with db size, which is the argument for quick-repair if redb were kept.

Consequence for the design: with redb T2, anything applied since the last checkpoint must be re-applied from the WAL after a crash. That was already the planned recovery model (replay WAL from `checkpointed_offset`).

## Two-phase commit and quick-repair observations

- **T2-2pc:** throughput is within noise of T2 (63.6k vs 62.4k). Checkpoint p50 is about 6% higher (56 ms vs 53 ms). The None commits are unaffected. What it buys: a guarantee that the primary commit slot is valid without checksum verification, which hardens against partial commits on crash during fsync.
- **T2-qr:** also within noise (61.4k median). Checkpoint p50 is about 8% higher (57 ms) and the max is higher (80 ms). What it buys: allocator state saved per commit (and 2PC implied), so reopen after a crash is near-instant instead of a full-tree repair. At about 80 ms for 136 MiB, the full repair would scale to seconds for multi-GB partitions.
- If redb were kept, T2-qr would be the recommended mode: negligible throughput cost, bounded recovery time.

## fjall comparison (fallback, triggered by the FAIL)

Same workload (one keyspace per table, one atomic `WriteBatch` per 64-command batch with 3 inserts + 1 remove per command + the `applied_offset` key). Mapping:
- **fjall T1:** `batch.durability(Some(PersistMode::SyncAll))` per batch.
- **fjall T2:** default batch commit (flushes the journal to OS buffers, so it survives a process crash but not power loss) + `db.persist(PersistMode::SyncAll)` every 100 ms / 256 batches.
- **fjall T3:** one `persist(SyncAll)` at the end.

Paired run: redb T2 is re-run interleaved, so both engines see the same load (1.6–2.5). Median rep of 3, from `fjall.jsonl`:

| Engine / tier | cmds/s (3 reps) | median cmds/s | steps/s | commit p50/p99/max (us) | txn p50/p99/max (us) | checkpoint n, p50/p99/max (us) | disk |
|---------------|-----------------|---------------|---------|-------------------------|----------------------|-------------------------------|------|
| redb T2 (re-run) | 63,761 / 65,831 / 67,050 | 65,831 | 21,944 | 184 / 311 / 675 | 646 / 951 / 3,266 | 21, 52,638 / 64,642 / 64,642 | 136 MiB |
| fjall T1 | 20,590 / 24,105 / 25,283 | 24,105 | 8,035 | 2,530 / 3,809 / 8,361 | 2,558 / 3,840 / 8,392 | - | 83 MiB |
| **fjall T2** | 238,100 / 242,412 / 249,384 | **242,412** | **80,804** | 223 / 339 / 536 | 245 / 368 / 572 | 13, 5,148 / 6,570 / 6,570 | 83 MiB |
| fjall T3 | 247,979 / 253,884 / 261,345 | 253,884 | 84,628 | 225 / 353 / 850 | 246 / 379 / 871 | - | 83 MiB |

- fjall T2 is **3.7x** redb T2. Its checkpoint (journal fsync) is about 5 ms, inside the 10 ms reference window, versus redb's 53 ms.
- fjall T1 (fsync per batch) is 2.4x redb T1.
- The fjall numbers include background flush/compaction threads (default `min(cores, 4)` workers). Those threads compete with the apply thread for CPU on a real node.

## Recommendation

Under the locked rule, **redb 4.6 is rejected for the partition state store in the planned T2 mode. Switch the Phase 2 state store (STOR-05) to fjall 3.1.13.** Use this durability design:
- One fjall `Database` per node, with keyspaces per table (or per partition x table).
- One atomic `WriteBatch` per apply batch, with `applied_offset` in the same batch.
- Default (buffered) journal flush on each commit.
- `db.persist(PersistMode::SyncAll)` every **100 ms** (or 256 batches), recording `checkpointed_offset`.
- Crash recovery replays the WAL from the last durable `applied_offset`.

This recommendation is conditional. It is a storage-engine switch, so it is a decision for the user/orchestrator to confirm when logging the FOUND-06 decisions (01-05/01-08), not something this plan finalises. Points to weigh:
1. **Not measured: the read path.** Real `apply_batch` reads before it writes (load run, check status). An LSM's point-read cost, which grows with levels, is not captured here. redb's B-tree reads are cheap.
2. **Not measured: a fjall crash test.** The kill -9 sanity test was only implemented for redb. Before Phase 2 freezes on fjall, the crash test must be ported and must pass, including journal recovery time.
3. **redb could be tuned.** Options are larger batches, a larger cache (`set_cache_size`), sequential/time-ordered run ids (ULID-like keys localise B-tree writes), or less frequent checkpoints. None was tested. T3 at about 90k suggests that even perfect fsync handling would leave redb at the threshold, not above it.
4. **Dependency weight and maturity:** fjall pulls in lsm-tree, LZ4 compression and background worker threads. redb is a single crate with no background threads. The cargo-deny/licence check for fjall's tree should run in plan 01-05/01-01's deny config if adopted.

## Open items

- ~~Plan 01-08 appends a quiet-machine confirmation run of T2 (redb and, given this outcome, fjall) to this report.~~ Done, see below.
- ~~Port the crash-test subcommand to `--engine fjall` (Phase 2 or 01-08).~~ Done in 01-08: 5/5 pass.
- ~~Add a read-before-write variant of the workload (get the run row before update) to compare engines on the realistic apply path.~~ Done in 01-08 (`--rmw`).
- Optional: sequential-key variant (`--keys sequential` is already implemented) to quantify how much ULID-style run ids would help redb.

## Confirmation run (quiet machine, plan 01-08)

Run on 2026-11-18 by `spikes/s3-redb-bench/confirm.sh` (built with `--features fjall`), with no other executors running. The script waits for the 1-minute load to fall below 2.0 before each run. Same machine, filesystem and db dir (`/tmp/ostro-s3`, ext4) as above.

- **Load average:** 0.19 at the first run, 0.25–1.34 across all 12 bench runs, and 1.29–1.50 during the crash tests (nproc 12, so the `nproc / 4` = 3 quiet threshold was always met). Every value is recorded per line in `confirm.jsonl` / `crash-confirm.jsonl`.
- **Parameters:** identical to the 01-02 runs: T2, 200,000 measured commands, 20,000 warm-up, B = 64, checkpoint every 100 ms or 256 batches, random 16-byte keys. Each of the 3 reps runs {redb plain, fjall plain, redb RMW, fjall RMW} back to back, so both engines see the same load.

### Read-modify-write workload (`--rmw`)

The 01-02 workload only inserts fresh runs. The real `apply_batch` first loads the run it applies a command to. The new `--rmw` mode models this per command:
1. Pick a run. 25% of commands start a new run (`--new-ratio 0.25`). The rest update an existing run, picked 90% from the 64Ki most recent runs and 10% uniformly from all runs.
2. **Read** the `runs` row (`get`). The bench counts reads whose hit or miss disagrees with the picker (`read_mismatch`). It was 0 in every run.
3. Write the updated row (status advanced, update counter bumped), move the run in `runs_by_status` (remove the old status key, insert the new one), and do the same outbox insert + ring delete as before. Write `applied_offset` once per batch.
4. **Preload:** 1,000,000 runs are inserted before warm-up and not measured (`--preload 1000000`). The db is 543 MiB (redb) / 465 MiB (fjall), so reads hit a realistically sized tree, not a cache-sized one.

redb reads inside the write transaction (it sees its own uncommitted writes). A fjall `WriteBatch` has no read-your-writes, so the fjall path reads `Keyspace::get` through a per-batch overlay map of rows already written in the batch. A single-writer partition state machine would keep the same overlay.

### Throughput (T2, median of 3)

| Engine / workload | cmds/s (3 reps) | **median cmds/s** | steps/s | 01-02 median | txn p50 / p99 / max (µs) | checkpoint n, p50 / max (µs) |
|---|---|---|---|---|---|---|
| redb T2 plain | 67,377 / 68,272 / 67,477 | **67,477** | 22,492 | 62,404 (load 5–7); 65,831 (load 1.7) | 636 / 906 / 3,151 | 20, 52,706 / 65,935 |
| fjall T2 plain | 233,784 / 228,350 / 248,857 | **233,784** | 77,928 | 242,412 (load 1.6–2.5) | 246 / 412 / 762 | 13, 5,945 / 8,183 |
| redb T2 RMW | 30,947 / 30,817 / 31,227 | **30,947** | 10,316 | (new) | 1,123 / 1,517 / 2,681 | 36, 80,173 / 95,225 |
| fjall T2 RMW | 106,873 / 106,176 / 108,573 | **106,873** | 35,624 | (new) | 513 / 754 / 122,605 | 18, 5,683 / 10,242 |

(The latency cells show the median-throughput rep; the max column is the max over the 3 reps.)

**Observations:**
- **The quiet machine moved redb only slightly.** Plain T2 went from 62.4k (loaded) and 65.8k (paired run) to 67.5k. That is still 25% short of 90k. The 01-02 verdict was not a load artefact.
- **Read-before-write halves both engines,** but redb falls much further: 31k vs 107k, so fjall is **3.45x** redb on the realistic path, about the same ratio as for the insert-only path (3.5x). The LSM point-read cost feared in 01-02 is real but affordable: with 1M+ runs, the read adds about 5 µs per command (9.4 µs vs 4.3 µs per command at the medians).
- **redb checkpoints grow with the dirty set.** RMW touches pages all over a 543 MiB tree. Its Immediate checkpoint rises to about 80 ms p50 (95 ms max), and 36 checkpoints take about 2.9 s of the 6.5 s wall. fjall's journal fsync stays at about 5.7 ms.
- **fjall tail latency: one stall of about 120 ms per RMW run.** The txn max was 120–123 ms in all 3 RMW reps, while p99 stayed under 0.85 ms. With 1M preloaded runs, this matches a write stall or memtable rotation during a background flush. It needs a Phase 2 look (memtable/journal sizing, flush worker count). It does not change throughput enough to affect the rule.

### fjall crash test (precondition 1)

`crash-test --engine fjall` mirrors the redb test. A child runs fjall T2 forever: one `WriteBatch` per 64 commands, default buffered journal flush on each commit, and every 100 ms / 256 batches it commits `checkpointed_offset` and then calls `persist(SyncAll)`. It prints `CKPT <offset>` only after the fsync returns. The parent sends SIGKILL after 3 s, reopens the database and checks:
- (a) the db opens;
- (b) `applied_offset ≥ last CKPT seen` and `checkpointed_offset ≥ last CKPT seen`;
- (c) `applied_offset % 64 == 0`;
- (d) **new, row-level torn-batch check (both engines):** the number of `runs` rows equals `applied_offset` exactly, and the highest outbox key is `applied_offset − 1`. Every crash-child command inserts exactly one fresh run and one outbox seq, so any partially applied batch, or a data/meta mismatch across keyspaces or tables, would fail it.

redb was re-run with the same stronger check, so both engines are on the same footing (`crash-confirm.jsonl`):

| Engine | runs | pass | last CKPT seen (range) | gap applied − last CKPT | `runs` rows == applied, outbox last == applied − 1 | reopen time | db at kill |
|---|---|---|---|---|---|---|---|
| redb T2 | 5 | **5/5** | 210,880 – 212,800 | 0 in all 5 | yes, all 5 | 83–97 ms (full repair, 3 callbacks) | 136 MiB |
| fjall T2 | 5 | **5/5** | 506,496 – 540,224 | 3,328 – 16,384 | yes, all 5 | **3.19–3.44 s** (journal replay) | 353 MiB |

**What differs, and why it is safe:**
- **redb rolls back to the last Immediate commit,** so `applied_offset == last durable checkpoint` (gap 0).
- **fjall keeps more.** Its buffered commits are `write(2)`-flushed to the OS page cache on every commit. A process kill (OS still up) loses none of them, so after reopen `applied_offset` is up to 256 batches (16,384 commands) ahead of the last fsynced checkpoint. Every such state was still batch-aligned and row-exact, so no batch was torn. Equality with the last checkpoint is therefore the wrong invariant for fjall. The invariant that matters is that the state is a consistent prefix at `applied_offset ≥ last durable checkpoint`, and that held 5/5.
- Under power loss (not testable here), fjall falls back to its last synced journal position. Its journal recovery truncates an incomplete tail entry. That is again a consistent prefix ≥ the last `persist`.
- This is safe under Ostro's log-first design for two reasons. First, the state machine only applies commands that are already durable in the WAL, so the state can never get ahead of the log. Second, recovery must replay the WAL from the **state's own `applied_offset`**, not from `checkpointed_offset`. Phase 2 (STOR-05/recovery) must encode that rule.
- **Recovery time is the cost.** fjall replays its whole un-flushed journal into memtables on open: 3.2–3.4 s for about 540k commands (353 MiB on disk, including the journal). That is far slower than redb's 80–100 ms full repair at this size, and it scales with journal size, not db size. Phase 2 must bound it: cap the journal size / memtable size so that flushes keep the replay window small, and measure reopen time in the recovery test against an NFR target.

### Verdict

Decision rule: PASS (fjall 3.1.13) — fjall T2 RMW median = 106,873 commands/s (= 35,624 steps/s) and plain median = 233,784 commands/s vs threshold 90,000 commands/s, at load 0.25–1.34. redb 4.6 FAILS again: T2 plain median = 67,477 commands/s, T2 RMW median = 30,947 commands/s.

- **The redb verdict did not change** from 01-02 (FAIL). The quiet re-run makes it slightly less bad on the insert-only path and much worse on the realistic read-before-write path.
- **fjall meets both preconditions from 01-02:**
  - Crash sanity: 5/5 with the stronger row-level check.
  - Read-modify-write: 106.9k ≥ 90k under the same interleaved load.
- **The RMW margin is 1.19x, not 2.6x.** Phase 2 should keep the apply path lean: one `get` per command, an overlay for in-batch writes, and no extra index reads.
- **Outcome:** STOR-05 switches from redb to **fjall 3.1.13** with this durability design:
  - One atomic `WriteBatch` per apply batch, with `applied_offset` in the batch.
  - Default buffered journal flush on each commit.
  - `persist(SyncAll)` every 100 ms or 256 batches, recording `checkpointed_offset`.
  - Recovery replays the WAL from the state's `applied_offset`.
  - Phase 2 must bound the journal/memtable size (reopen time) and look at the ~120 ms write stall.
- Logged in PROJECT.md Key Decisions ("State store: redb vs fjall").
