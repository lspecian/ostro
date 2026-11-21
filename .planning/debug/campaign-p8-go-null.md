---
status: diagnosed
trigger: "Phase 8 10k gate campaign (seed 1305, phase8, 20 shards, main 02745dc): shard 04 final check reports 4 WrongResult violations: runs s04-10241, s04-10270, s04-10271, s04-10291 completed with \"go\": null while the checker expected the acked go payload"
created: 2026-11-21T05:50:00Z
updated: 2026-11-21T06:30:00Z
---

## Current Focus

hypothesis: CONFIRMED. The engine is correct. The go signal reached the server only after the go wait's 600 s wall-clock timeout had already fired, because the shard's server was down for about 95% of the 02:58:48-03:16:22Z window. The checker (`RunLedger::accepted_payload`) assumes that the first journaled `go` signal_payload is the one the wait matched, and that assumption is false for a signal that arrives after the timeout.
test: done (history export, ack-log correlation, activity-window reconstruction, variant replay)
expecting: n/a
next_action: STOP per instructions. A checker change needs user approval (see Resolution).

## Symptoms

expected: every Completed crash-saga run has result == crash_saga_expected(input, go, extra), where the checker takes go and extra from the journal
actual: 4 shard-04 runs have result.go == null, but their journals contain a `go` signal_payload whose request_id was acked
errors: `final: WrongResult run 000038fw1nv7twa4hs50617y6rp5p ... "go": Null ..., expected {... "go":{"key":"s04-10241","request_id":"s04-10241-go","signal":"go"} ...}` (plus the same for 006g38fw1r3bcxfgnd3102as14zhm, 007g38fw1s33yxx1gb3dvcyexrj7e, 001038fw1s10tw5am525c04thfx6r)
reproduction: campaign-p8-gate shard 04 final check; target/campaign-p8-gate/shard-04/
started: first seen in this campaign. The checker logic is unchanged since 05-19 (8527bdc).

## Eliminated

- hypothesis: an acked go signal was lost or reordered by snapshot install, trim, rebuild or replay and then re-accepted
  evidence: the go signal_ack line in acks.jsonl has `"duplicate": false`, so the server first saw that request_id at the moment it was journaled. Journal seqs are contiguous. The signal_payload(go) seq comes after wait_timed_out in all 4 runs. The tally shows lost_acked_signals=0 and duplicate_journal_entries=0.
  timestamp: 2026-11-21T06:05:00Z

- hypothesis: a lost wake, i.e. the go signal was journaled while the wait was pending but the wait was not woken and timed out instead
  evidence: in none of the 4 runs is any go signal_payload journaled before wait_timed_out (see histories below)
  timestamp: 2026-11-21T06:05:00Z

- hypothesis: the timer fired early (premature timeout)
  evidence: each timeout fired after the wait start + 600 000 ms. 10241: started 03:01:56.783, fired 03:11:58.156. 10270: started 03:02:21.802, fired 03:13:15.833. 10291: started 03:02:35.929, fired 03:13:15.832. 10271: started 03:03:42.604, fired 03:14:15.797. The late ones fired at server restarts.
  timestamp: 2026-11-21T06:05:00Z

- hypothesis: a single-partition stall, where requests to one partition hang while the server is up
  evidence: the 4 runs sit on partitions 0, 13, 15 and 2 (run-id prefix 0000/006g/007g/0010). Signal latencies of 100-800 s affect every partition in the window.
  timestamp: 2026-11-21T06:10:00Z

- hypothesis: a regression from 08-23 (parallel uploader)
  evidence: no product misbehaviour was found at all. check.rs and crash-saga have not changed since 05-19/05-20, and the mechanism below does not involve the uploader.
  timestamp: 2026-11-21T06:20:00Z

## Evidence

- timestamp: 2026-11-21T05:55:00Z
  checked: the histories of the 4 runs, exported with the campaign's failpoint binary (`ostro dev`, no FAILPOINTS, no OSTRO_* env, so no bucket) from a throwaway copy of shard-04/data
  found: |
    s04-10241 (p0):
      9  03:01:56.783 signal_wait_started go timeout 600000
      10 03:02:35.423 signal_payload extra
      11 03:11:58.156 wait_timed_out op 9
      15 03:11:58.182 signal_wait_started extra
      16 03:11:58.183 signal_matched op 15 payload_seq 10
      18 03:11:58.556 signal_payload go      <- 0.4 s AFTER the timeout, never matched
      23 03:13:15.927 run_completed (go null)
    s04-10270 (p13): 14 go wait 03:02:21.802; 15 wait_timed_out 03:13:15.833; 19 extra wait;
      22 signal_payload go 03:13:16.177; 25 signal_payload extra 03:14:19.706; 26 matched op 19; 28 completed
    s04-10271 (p15): 7 go wait 03:03:42.604; 8 wait_timed_out 03:14:15.797; 12 extra wait;
      17 signal_payload go 03:16:22.793; 18 signal_payload extra 03:16:24.742; 19 matched op 12; 21 completed
    s04-10291 (p2): 14 go wait 03:02:35.929; 15 signal_payload extra 03:10:40.890; 16 wait_timed_out 03:13:15.832;
      20 extra wait; 21 matched payload_seq 15; 22 signal_payload go 03:13:15.991; 28 completed
  implication: the go signal is journaled, but always after the go wait timed out and before completion. go=null is the documented guest output for a timed-out wait.

- timestamp: 2026-11-21T06:00:00Z
  checked: acks.jsonl around the go intent and ack of s04-10241 (lines 59569 and 60490), correlated with the journal time of neighbouring signal acks
  found: the intent sits among acks journaled at 03:01:43-03:01:58. The ack (line 60490, duplicate:false) sits among acks journaled at 03:11:58.3-03:11:58.9. The harness sent go at about 03:01:44 and retried it (5xx, connection refused, or a 10 s timeout; no anomalies were recorded) until the server first accepted it at 03:11:58.556.
  implication: the server did not hold the go signal for 10 minutes. It first received it 10 minutes late.

- timestamp: 2026-11-21T06:10:00Z
  checked: journal events of all 600 runs s04-10000..10599, merged and split into activity windows (gap > 4 s)
  found: |
    active 02:58:27-02:58:48 (20.9 s)  gap 62.7 s
    active 02:59:51-02:59:56 (4.9 s)   gap 95.4 s
    active 03:01:31-03:01:36           gap 6.4 s
    active 03:01:42-03:01:43           gap 13.0 s
    active 03:01:56-03:01:59           gap 20.6 s
    active 03:02:19-03:02:23           gap 11.5 s
    active 03:02:35-03:02:36           gap 26.9 s
    active 03:03:03-03:03:05           gap 36.8 s
    active 03:03:42-03:03:44           gap 59.2 s
    active 03:04:43-03:04:59 (15.9 s, bursty)  gap 94.8 s
    active 03:06:34-03:06:34 (0.5 s)   gap 245.1 s
    active 03:10:39-03:10:41           gap 76.4 s
    active 03:11:58-03:12:00           gap 75.5 s
    active 03:13:15-03:13:16 (0.4 s)   gap 59.6 s
    active 03:14:15-03:14:20           gap 65.8 s
    active 03:15:25                    gap 56.3 s
    active 03:16:22-03:16:49 (normal again)
  implication: the server was up for about 45 s of the roughly 17.5 minutes between 02:58:48 and 03:16:22. While it was up, it accepted API requests at only 15-50 per second against a backlog of about 150 queued actions (the log shows "pending 54" at kill 350 and "pending 157" at kill 360, against 6-13 normally).

- timestamp: 2026-11-21T06:12:00Z
  checked: the delay from run start to the go and extra signal_payload for every signalled run in s04-10150..10300
  found: delays of 100-830 s are common across all partitions. Runs whose go landed more than 600 s after their go wait started timed out: the 4 violators plus s04-10229, 10237, 10252 and 10256. For those 4 extra runs the go signal arrived after completion and was rejected with 409 (`signal_rejected` at acks lines 60677, 60686, 60812, 60842), so the checker passes them. s04-10211's go landed 599.9 s after its wait started, just inside the 600 s timeout.
  implication: one load-driven incident hit 8 runs. The checker flags only the subset whose late go landed between the timeout and completion.

- timestamp: 2026-11-21T06:15:00Z
  checked: a replay of `pick_variant_cfg(1305, 4, iter, true)` in Python (validated: iter 499 gives last_seed 7359515741687632342 and 18 trim kills, both matching shard-04.json)
  found: |
    the iterations around the incident (kills about 350-372):
      345 snapshot::before_rename=return     347 trim::after_delete=return
      351/353 wal::write enospc              352 executor::before_append sleep
      355 dispatcher::before_grant           356 snapshot::before_dir_fsync=return
      357, 359, 361 snapshot::install_before_swap=1*return (drop_partition_states first)
      360 dispatcher::before_grant           362 state::commit  364 wal::dir_fsync
      365 trim::after_delete  366, 370 upload::before_manifest=return
  implication: a dense run of phase8 and faulted iterations. Each one is two server startups (armed, then clean) plus an offline rebuild_and_compare after every phase8 kill. Three of them were install scenarios (startup installs local snapshots, the first install fails, so it falls back to an older snapshot or a full replay). On a heavily loaded host (6 shards in parallel, clean restarts up to max_restart_ms = 53 033 ms) this explains the 60-245 s of downtime per iteration. No single scenario corrupted anything; together they kept the server mostly down.

- timestamp: 2026-11-21T06:18:00Z
  checked: crash-saga guest (guests/crash-saga/src/lib.rs), workload.rs plan_input, check.rs check_final and accepted_payload
  found: |
    guest: `go = accepted(ctx.wait_signal("go", 600 s))`, where a timeout gives Null. Its docs say: "go = payload of the first go signal matched to the step-3 wait, or null if that wait timed out". The later wait is only for "extra", so a go that arrives late stays buffered and is never consumed.
    workload: signal_timeout_ms = 600_000 for signalled runs. go is sent 0-800 ms after the start ack and retried until 2xx or 409.
    checker: `RunLedger::accepted_payload` = the payload of the FIRST journaled signal_payload with that name. Its doc comment states the assumption: "the signal the guest's wait matched". It ignores signal_matched / wait_timed_out.
  implication: the checker's model of the expected output is wrong when a signal is accepted after its wait timed out. The engine and guest are consistent with each other.

## Resolution

root_cause: |
  A checker defect, made visible by load. The checker's `RunLedger::accepted_payload`
  (crates/ostro-crashtest/src/check.rs:104-117) derives the expected `go` value from the
  first journaled `signal_payload` named "go". It assumes that this signal is the one the
  guest's go wait matched. During 02:58:48-03:16:22Z the shard-04 server was up only about
  45 s in total: a dense streak of phase8 kills (3x snapshot install, snapshot fsync, trim,
  2x upload) and faulted kills, each with two slow startups plus an offline rebuild on a
  heavily loaded host. The harness's go POSTs were therefore first accepted more than 600 s
  after the go wait started. The engine correctly fired the expired 600 s wait timeout
  (wait_timed_out, journaled first, at the restart after the deadline), then accepted and
  buffered the late go (signal_payload after the timeout, never matched). The guest
  correctly completed with go=null. The checker then demanded the buffered payload.
  This is not a product bug: no acked signal was lost, reordered or duplicated, no wake was
  lost, no timer fired early, and 08-23 is not involved.
classification: (b) workload/checker defect. The checker conflates "journaled" with "matched". The workload's 600 s signal timeout assumes the server is reachable within 10 minutes, which this campaign's fault density and host load did not guarantee. 4 sibling runs hit the same timeout but were masked by 409 rejections.
fix: NOT APPLIED. Instructions say to stop on a checker or workload issue, and a checker change needs user approval. Suggested direction: in check_final, take go/extra from the journal's matching structure. Find the `signal_wait_started{name}` op. If a `signal_matched{op, payload_seq}` exists, use the payload of the signal_payload at payload_seq. If `wait_timed_out{op}` exists, use Null. Optionally strengthen the check: a `signal_payload{name}` journaled while that wait was pending (before its wait_timed_out) is a real lost-wake violation. Alternatively (workload side), lengthen signal_timeout_ms, but that only moves the threshold.
verification: diagnosis only. Evidence is from histories exported from a copy of shard-04/data with the campaign's own failpoint binary.
files_changed: []
