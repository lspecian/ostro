# Metric reference

Every node serves `GET /metrics` in the Prometheus text format, on the API port, unless
`[telemetry] metrics = false` is set. See [observability.md](observability.md) for the
exporter, the recorder and scraping. This page lists every `ostro_*` series in the
codebase.

## Conventions

- **Facade only.** Every crate records through the `metrics` facade. The exporter is
  `metrics-exporter-prometheus` (P8-D15), and the `prometheus` crate is banned.
- **Histograms.** Series whose names end in `_seconds` are histograms with explicit
  buckets, so `_bucket`, `_sum` and `_count` lines are rendered.
- **Bounded labels.** Labels never carry a run id, an idempotency key, a request id or a
  payload-derived value. Each label's value set is listed below. A `workflow` label is a
  deployed workflow name, so it is bounded by the deployed modules. A `queue` label is a
  queue name, and a `schedule` or `hook` label is a name created by an operator.
  `tests/metrics_endpoint.rs` audits every sample: it fails on a label named `run_id`,
  `idempotency_key`, `key` or `request_id`, and on any label value longer than 128 bytes.
- **No hot-path locks.** Partition, log and Raft gauges are sampled every 5 s by the
  `metrics_poll` service. Counters and histograms are recorded where the event happens.
- **Leader-side only.** Partition services (dispatcher, timers, shuffler, cron, snapshots)
  publish only while their node leads the partition. With RF=1 and a single node, that is
  every partition.

Types: **C** counter, **G** gauge, **H** histogram.

## Queues and dispatch

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_queue_ready` | G | `queue` | runs | Due runs waiting for an executor slot. Each partition's dispatcher moves the per-queue series by its own share, so the value is the node total. | `ostro-partition` `leader/dispatcher.rs` | 08-16 |
| `ostro_queue_lag_seconds` | H | `queue` | s | Time from a run's `ready_at` to its attempt grant. Regrants of runs that were already running, whose ready time is unknown, are not recorded. | `leader/dispatcher.rs` | 08-16 |
| `ostro_queue_worker_demand` | G | `queue` | slots | Worker demand, used as the KEDA input. Phase 8 only **describes** this series. Phase 7's `workers/demand.rs` sets its value (07-DECISIONS), and no other code may set it. | `leader/dispatcher.rs` (describe) / Phase 7 | 08-16 / 07-16 |
| `ostro_dispatcher_dispatched_total` | C | `partition` | grants | Attempts granted and handed to the executor. | `leader/dispatcher.rs` | 04 |
| `ostro_dispatcher_lease_expired_total` | C | `partition` | leases | Soft leases revoked because they were not renewed in time. | `leader/dispatcher.rs` | 04 |

## Runs and steps

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_runs_started_total` | C | `workflow` | runs | Runs granted their first activation. | `leader/dispatcher.rs` | 08-16 |
| `ostro_runs_completed_total` | C | `workflow`, `status` (`completed`, `failed`, `cancelled`, `terminated`, `timed_out`, `continued_as_new`) | runs | Runs reaching a terminal status. Counted from the post-apply `RunCompleted` action, so a restart's log replay never counts a run twice. | `ostro-server` `services/run_spans.rs` | 08-16 |
| `ostro_steps_total` | C | `workflow`, `outcome` (`ok`, `error`, `timeout`, `cancelled`, `retry`) | steps | Live step completions (`StepCompleted`), plus failed attempts that will be retried (`StepAttemptFailed`, `retry`). Counted only once their append has committed, so replayed steps are never counted. | `ostro-engine` `activation.rs` (`MeteredSink`) | 08-16 |
| `ostro_step_duration_seconds` | H | `workflow` | s | Journal time between a step event and the event before it. A live step's begin is not journaled, so this is an upper bound that includes any guest code run in between. | `activation.rs` | 08-16 |
| `ostro_step_retries_total` | C | `backoff` (`in_activation`, `requeue`) | retries | Step retries, either slept inside the activation or requeued with `StepBackoff`. | `ostro-engine` `host/journal.rs` | 04 |
| `ostro_step_retries_cancelled_total` | C | — | retries | In-activation retry sleeps cut short by cancellation. | `host/journal.rs` | 04 |
| `ostro_step_interrupts_total` | C | `cause` (`timeout`, `fuel`) | steps | Live steps interrupted by their `timeout-ms` or their fuel budget. | `host/journal.rs` | 04-10 |
| `ostro_step_abandoned_total` | C | — | steps | Steps dropped open by the guest. | `host/journal.rs` | 04 |

## Activations, replay, fuel and memory

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_activation_outcomes_total` | C | `outcome` (`completed`, `failed`, `continued_as_new`, `evicted`, `fenced`, an attempt-failure reason code) | activations | Activation outcomes. | `ostro-engine` `activation.rs` | 03 |
| `ostro_activation_evictions_total` | C | `reason` (an `EvictReason` code) | activations | Evicted activations, by reason. | `activation.rs` | 03 |
| `ostro_activation_duration_seconds` | H | — | s | Wall time of one activation. | `activation.rs` | 03 |
| `ostro_activation_budget_evictions_total` | C | — | activations | Parked activations evicted under node memory-budget pressure. | `limits.rs` | 03 |
| `ostro_replay_events_total` | C | — | events | Committed journal events re-executed by activations (the journal length at activation start). | `activation.rs`, `replay/mod.rs` | 08-16 |
| `ostro_replay_duration_seconds` | H | — | s | Time from activation start to the end of replay: the first live command, or the end of the guest call when nothing live was journaled. Recorded only when more than `RunStarted` was replayed. | `activation.rs`, `replay/mod.rs` | 08-16 |
| `ostro_fuel_consumed_total` | C | `workflow` | fuel units | Wasmtime fuel consumed by activations: `engine.activation_fuel` minus the fuel left when the guest call ends. | `activation.rs` | 08-16 |
| `ostro_instance_memory_bytes` | G | — | bytes | Linear memory charged by live instances (parked ones included) against the node budget. | `runtime.rs` | 08-16 |
| `ostro_wasm_memory_budget_bytes` | G | — | bytes | The soft node-wide instance memory budget. | `runtime.rs` | 08-16 |
| `ostro_parked_instances` | G | — | instances | Hot-parked activations. | `runtime.rs` | 08-16 |
| `ostro_aot_cache_tamper_total` | C | — | files | AOT cache entries that failed authentication and were recompiled. | `cache.rs` | 03 |

The three instance gauges are published when each activation starts and ends.

## Dead letters

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_dlq_total` | C | `reason` (`nondeterminism`, `protocol_violation`, `fuel_exhausted`, `timeout`, `memory_exceeded`, `stack_overflow`, `guest_exit`, `trap`, `no_module`, `module_not_found`, `module_invalid`, `bad_run_record`, `other`) | runs | Runs the executor failed for an attempt-level or module-level reason. These runs are kept for `RetryRun` (D1). A workflow's own `RunFailed` is not counted. | `ostro-server` `services/run_spans.rs` | 08-16 |

**Where DLQ is counted.** No DLQ rows are written yet. The `dlq` keyspace exists, but
Phase 11 adds the rows. A run is dead-lettered when the executor's `fail_run` stores
`CompleteRun{Failed}` with a failure JSON. That code is in `ostro-server/src/executor.rs`,
which another wave-4 plan owns, and it is not in `apply/terminal.rs`. Apply also runs again
during a restart's catch-up. So the counter sits in the post-apply `RunCompleted`
consumer. That consumer reads the stored failure's `reason_code`. A run is counted when it
is `Failed`, has no terminal reason, and its code is in the list above. The `timeout` code
counts only for a resumable run, which excludes a workflow's own `Failure::Timeout`. Any
other executor code maps to `other`.

## Partition, WAL and replication (polled every 5 s)

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_partition_apply_lag` | G | `partition` | records | High watermark minus the applied offset: committed records not yet applied. | `ostro-server` `services/metrics_poll.rs` | 08-16 |
| `ostro_partition_hw_lag` | G | `partition`, `replica` (node id) | records | The leader's HW minus the replica's HW. With RF=1 the only replica is the node itself, so this is always 0. | `metrics_poll.rs` | 08-16 |
| `ostro_isr_size` | G | `partition` | replicas | In-sync replicas. RF=1: 1. | `metrics_poll.rs` | 08-16 |
| `ostro_wal_log_start` | G | `partition` | offset | First retained WAL offset. Set by the poller and by each retention trim. | `metrics_poll.rs`, `ostro-partition` `leader/retention.rs` | 08-16 / 08 |
| `ostro_wal_log_end` | G | `partition` | offset | WAL log end offset (exclusive). | `metrics_poll.rs` | 08-16 |
| `ostro_apply_batches_total` | C | `partition` | batches | Apply batches committed. | `ostro-partition` `machine.rs` | 02 |
| `ostro_apply_commands_total` | C | `partition` | commands | Commands applied. | `machine.rs` | 02 |
| `ostro_apply_batch_seconds` | H | `partition` | s | Apply time of one batch. | `machine.rs` | 02 |
| `ostro_state_checkpoint_seconds` | H | `partition` | s | State checkpoint (`persist`) duration. | `machine.rs` | 02 |
| `ostro_timer_window_entries` | G | `partition` | timers | Timers held in the in-memory window. | `leader/timers.rs` | 02 |
| `ostro_timers_fired_total` | C | `partition` | timers | Timers fired. | `leader/timers.rs` | 02 |
| `ostro_shuffler_batches_total` | C | `src`, `target` (partition ids) | batches | Outbox batches shipped between partitions. | `leader/shuffler.rs` | 02 |
| `ostro_shuffler_retries_total` | C | — | retries | Shuffler delivery retries. | `leader/shuffler.rs` | 02 |
| `ostro_outbox_rows` | G | `partition` | rows | Outbox rows awaiting delivery. | `leader/shuffler.rs` | 02 |

## Metadata Raft (polled every 5 s)

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_raft_term` | G | — | term | Current term of the metadata Raft group. | `metrics_poll.rs` via `MetaHandle::raft_status` | 08-16 |
| `ostro_raft_commit_index` | G | — | index | Highest committed log index. | `metrics_poll.rs` | 08-16 |
| `ostro_raft_applied_index` | G | — | index | Highest index applied to the metadata state machine. | `metrics_poll.rs` | 08-16 |
| `ostro_raft_is_leader` | G | — | 0/1 | 1 when this node leads the metadata group. | `metrics_poll.rs` | 08-16 |

## Snapshots, backup and blob GC

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_snapshot_duration_seconds` | H | `partition` | s | Time to take one partition snapshot. | `ostro-partition` `leader/snapshot.rs` | 08-09 |
| `ostro_snapshot_bytes` | G | `partition` | bytes | Size of the latest snapshot. | `leader/snapshot.rs` | 08-09 |
| `ostro_backup_cut_age_seconds` | G | — | s | Now minus `cut_at` of the last durable manifest. | `ostro-backup` `uploader.rs` | 08-13 |
| `ostro_backup_upload_seconds` | H | — | s | Duration of one upload round (P8-D23 part 2). | `uploader.rs` | 08-13 |
| `ostro_backup_upload_bytes_total` | C | `kind` (`chunk`, `snapshot`, `meta`, `manifest`, `blob`) | bytes | Bytes uploaded. | `uploader.rs` | 08-13 |
| `ostro_backup_failures_total` | C | `kind` (an uploader error kind, `no_snapshot`) | rounds | Failed upload rounds. | `uploader.rs` | 08-13 |
| `ostro_backup_fenced` | G | — | 0/1 | 1 once another writer has fenced this uploader. | `uploader.rs` | 08-13 |
| `ostro_backup_upload_floor_lag` | G | `partition` | records | Applied offset minus the upload floor (W13). | `uploader.rs` | 08-13 |
| `ostro_blob_gc_deleted_total` | C | — | blobs | Local blobs deleted by GC. | `ostro-server` `services/blob_gc.rs` | 08-19 |
| `ostro_blob_gc_bytes_freed_total` | C | — | bytes | Bytes freed by blob GC. | `services/blob_gc.rs` | 08-19 |
| `ostro_blob_gc_duration_seconds` | H | — | s | Duration of one GC pass. | `services/blob_gc.rs` | 08-19 |

Re-checked at the Phase 8 gate (08-22): every `"ostro_*"` series literal in `crates/*/src`
(`grep -rhoE '"ostro_[a-z0-9_]+"' crates/*/src | sort -u`, 64 series) has a row on this page,
and every row names a series that exists in the code. The blob GC series are the
`METRIC_*` constants of `services/blob_gc.rs` (08-19).

## Triggers

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_cron_fires_total` | C | `schedule`, `outcome` (`started`, `skipped`, `buffered`, `advanced`, `duplicate`, `rejected`, `start_error`) | fires | Cron fire outcomes. | `ostro-partition` `leader/cron.rs` | 08-14 |
| `ostro_webhook_requests_total` | C | `hook`, `outcome` (`ok`, `duplicate`, `unauthorized`, `not_found`, `error`) | requests | Webhook ingress outcomes. | `ostro-server` `control/webhooks.rs` | 08-15 |

## Guest logs

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_guest_log_lines_total` | C | `level` | lines | Live guest log lines accepted. | `ostro-engine` `sink.rs` | 08-17 |
| `ostro_guest_log_dropped_total` | C | `reason` (`activation_bound`, `run_rate`, `run_cap`, `node_rate`) | lines | Guest log lines dropped. | `sink.rs`, `host/log.rs` | 08-17 |

## Effects (Phase 6)

| Series | Type | Labels | Unit | Meaning | Source | Plan |
|---|---|---|---|---|---|---|
| `ostro_effect_inflight` | G | — | tasks | Effect tasks in flight. | `ostro-bridge` `registry.rs` | 06 |
| `ostro_effect_duration_seconds` | H | `kind` | s | Effect execution time. | `ostro-bridge` `driver.rs` | 06 |
| `ostro_effect_completed_total` | C | `kind`, `outcome` | effects | Effect outcomes. | `driver.rs` | 06 |
| `ostro_effect_attach_total` | C | — | effects | Effect starts attached to a task that was already running. | `driver.rs` | 06 |
| `ostro_effect_rate_limited_total` | C | `target` (configured egress target key) | effects | Effects failed with `rate_limited` after `max_wait`. | `ostro-bridge` `ratelimit.rs` | 06 |

## Matches of `ostro_[a-z_]+` that are not series

Running `grep -rhoE 'ostro_[a-z_]+' crates --include=*.rs | sort -u` also matches names
that are not series:

- crate and module paths (`ostro_api`, `ostro_engine`, `ostro_raft_driver`,
  `ostro_runtime_workflows`, …);
- prefixes used in tests (`ostro_apply_`, `ostro_wal_`, `ostro_raft_`, `ostro_effect_`);
- the `_bucket` lines of the histograms above;
- the SQL connector's default effects table and its index (`ostro_effects`,
  `ostro_effects_created_at`).
