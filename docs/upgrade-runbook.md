# Upgrade runbook (Phase 13)

Step-by-step procedures for rolling upgrades, the downgrade window, moving live runs to a new
workflow version (`migrate-runs`), and retiring and garbage-collecting modules. The contract
behind these steps is in [compat.md](compat.md). Requirements: CLUS-06, CLUS-07.

## 1. Rolling upgrade

Preconditions: every member is `active` and live (`ostro cluster status`), and no reassignment
is in flight. To keep the option of rolling back after the last node, run
`ostro cluster upgrade hold` before you start.

For each node, one at a time:

1. **Stop it with SIGTERM** (`systemctl stop`, `kill -TERM`, a Kubernetes pod delete with a
   grace period ≥ `shutdown_handoff_timeout` + a few seconds). In cluster mode, SIGTERM is a
   controlled shutdown. The node hands off every partition it leads (election reason RESIGN),
   transfers meta leadership, then exits. No run is cancelled or failed. Never upgrade with
   `kill -9`: it works (that is plain failover), but the partitions the node led are unavailable
   for a session timeout instead of a handoff.
2. **Replace the binary and start it.** It rejoins with its node id and catches up.
3. **Verify** before the next node:
   - `ostro cluster status`: the member is `active` and `live`, and its `version` shows the new
     `level_max`;
   - `finalized` is unchanged (the cluster keeps writing at the old level while any member is
     old);
   - the node is back in the ISR of its partitions (`GET /v1/admin/replication` on it).
4. Go to the next node.

After the last node, with auto-finalize on and no hold, the meta leader finalizes the new level
within about a liveness window. `ostro cluster upgrade status` shows `finalized` = the new level.
With a hold, run `ostro cluster upgrade release` (or `finalize`) when you are satisfied.

Measured (13-19, three processes, 20 ms link delay and one 1.5 s network partition per
iteration, three iterations, native Linux under load):

| measurement | p50 | p99 | n |
|---|---|---|---|
| SIGTERM → exit | 702 ms | 873 ms | 9 |
| restart → ready | 711 ms | 841 ms | 9 |
| handoff unavailability per partition per restart | 277 ms | 1.72 s | 36 |
| first activation through the restarted node | 454 ms | 603 ms | 36 |
| baseline (same link delay, before any restart) | 425 ms | 523 ms | 36 |
| finalization after the last restart | 541 ms | 565 ms | 3 |

8,613 acknowledged runs, 0 lost, 0 duplicated journal entries, 0 exactly-once violations.

**AOT pre-warm.** After a restart, a pre-warm task compiles the modules of ready and running runs
on the partitions the node leads or follows (concurrency 2, low priority). Metric:
`ostro_cluster_prewarm_modules_total{result}`. After every restart in the run above, it compiled
2–4 modules before their first activation. As a result, the first run through a restarted node
was only p50 +29 ms / p99 +80 ms over the steady-state baseline, and the second run equalled the
baseline. The absolute values are dominated by the injected link delay.

**Workers.** Under `[workers] version_policy = "compatible"` (the default), workers one level
behind the finalized level keep working, so upgrade servers first and workers after. Workers
below the floor get `GoAway{"version too old: ..."}` once the cluster finalizes.

## 2. Downgrade window

- **Before finalization**, data is still written at the old level, so a node can go back to the
  previous binary. Repeat the rolling procedure with the old binary.
  `ostro cluster upgrade hold` keeps this window open for as long as you need.
- **After finalization**, a previous binary refuses to start. The boot fence stops it before any
  partition file is opened: `data finalized by a newer Ostro (level 2, this binary supports up
  to 1); upgrade this binary`, exit status 1. A running old node fail-stops with the running
  fence. There is no unsafe-downgrade path.

**Refused before finalization** (409 `feature_not_enabled`): `ostro cluster reassign`,
`ostro cluster rebalance` and drain replica moves (`AlterReplicas`); submitting `migrate-runs`
(`RepinRun`; a dry run works); `ostro modules rm` (retire); and uploading a module that needs a
newer WIT level.

## 3. `ostro migrate-runs`: move live runs to a new version

```
ostro migrate-runs --workflow order --from 3 --to 4 --dry-run
ostro migrate-runs --workflow order --from 3 --to 4 [--filter tag=x ...] [--limit N]
```

(`POST /v1/runs/migrate {workflow, from: {version|sha}, to_version, filter, limit, dry_run}`.)

1. **Always dry-run first.** Each partition leader lists the non-terminal runs on the source
   module and replays each journal against the target with `ostro_engine::replay_check`.
   `Ok` / `OkFrontier` pass; `Diverged` / `Error` fail with the sequence number and detail.
2. **Submit.** Without `--dry-run`, only runs that passed get `Cmd 43 RepinRun{run_id, from_sha,
   to_sha, expected_next_seq}`. It applies as a compare-and-set on the module sha, the journal
   position and "no lease".
3. **Read the report** and act on it:

| field | meaning | action |
|---|---|---|
| `passed` | replayed cleanly against the target | — |
| `repinned` | `RepinRun` accepted (empty on a dry run) | — |
| `failed[{run_id, seq, detail}]` | diverged or errored | leave on the old version, or fix the workflow and retry |
| `busy` | the run was running (leased): `RunBusy` | run the same command again later |
| `seq_mismatch` | the run progressed since its check: `SeqMismatch` | dry-run and run again |
| `already_on_target` | an earlier call of the same migration already repinned it (on the target module, started on the source) | — (not checked or submitted again) |

- `already_on_target` runs are also listed in `passed` and, unless it is a dry run, in
  `repinned`. A retried migrate (for example after a 503 from one leg) therefore reports the
  complete set. The metric `ostro_migrate_runs_total{outcome}` counts them once as
  `already_on_target`.
- Every list is sorted by run id, so the same request gives the same report at every node.
- `ostro migrate-runs` exits 4 (like `ostro replay`) when any run diverged or errored.
- **Limit split.** `--limit L` over P partitions gives partition p `L / P + (p < L % P)`
  runs. The total is exactly L and every node computes the same shares. No limit means every
  matching run. `--filter` values are tags (`tag=x` or `x`); every tag is required.
- A partition without a leader makes the call answer 503 `partition_offline`. Retry it.
- **P13-F3: `run-info.module-hash` may change after a repin.** A run's module hash, as seen
  by the guest's run-info and by APIs, is the module it runs on *now*. Use it for logs and
  metrics only, never for workflow logic or idempotency keys.

## 4. Module retire and GC

```
ostro modules rm <sha256>     # DELETE /v1/modules/{sha256}: retire
ostro modules gc              # POST /v1/modules/gc: one GC round now
```

- **Retire** marks the module and its workflow versions `Retired` (ext 102). It is refused with
  409 `module_active` while any of its versions is the active one. Retiring again is idempotent
  (200, the first `retired_at_ms`). From then on, starts, reruns on the source version,
  retry-restarts, DLQ and bulk restarts, and repins to a retired version are refused with
  409 `module_retired`. The check reads meta behind the ReadIndex barrier.
- **GC** runs on the meta leader every `[modules] gc_interval` (10 min) for modules retired
  longer than `gc_grace` (10 min). Each round asks every partition leader (`NodeOps.ModuleRefs`)
  which shas are still referenced: non-terminal runs and, with `gc_protect_resumable = true`
  (default), terminal runs that can still be retried (Failed, Cancelled, Terminated,
  TimedOut). It does this in **two scans `gc_grace / 2` apart**. Only if both are clean does it
  delete the workflow versions, then the `Module` record (CAS). Phase 8 blob GC then sweeps the
  blob.
  - A partition that is not covered (no leader at the asked epoch, unreachable) postpones the
    whole round. Safety never depends on leadership: every deletion re-reads behind a barrier.
  - A manual `ostro modules gc` runs the round on the node that serves it and answers after at
    least `gc_grace / 2`.
- **`module_gone`.** A start slower than the grace can still race the deletion. Such a run fails
  loudly and non-retryably with `{"reason_code":"module_gone", ...}`: the module's metadata
  record no longer exists. The executor re-reads the record on meta changes for 2 s before
  failing, so a node whose meta view trails does not fail a run whose module was just uploaded.
  `module_gone` is its own `ostro_dlq_total` reason. A missing blob with the record present
  stays `module_not_found`.

## 5. Checklist

- [ ] `ostro cluster status`: every member active and live, no reassignments
- [ ] optional: `ostro cluster upgrade hold`
- [ ] per node: SIGTERM → replace → start → verify (`active`, `live`, new `level_max`, in ISR)
- [ ] `ostro cluster upgrade status`: finalized = new level (or `release` / `finalize`)
- [ ] workers upgraded
- [ ] optional: `ostro migrate-runs --dry-run`, then submit
- [ ] optional: `ostro modules rm` for old modules; GC removes them after the grace
