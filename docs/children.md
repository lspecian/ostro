# Child workflows (WF-06)

Phase 9 (plans 09-05, 09-08, 09-13; decisions D-P9-7..11, D-P9-24..27 and D-P9-31 in
`.planning/phases/09-polyglot-sdks-conformance/09-DECISIONS.md`). A workflow starts a child
with `start-child(workflow, input, child-options)`. The op completes when the child ends,
or at once for a detached child. Everything here holds the same for Rust, Python and
TypeScript parents and children, in any mix (C-CHILD-01..11, C-CRASH-01/02). No WIT,
format or journal-kind change was needed. Phase 9's only ordinal is the `ChildJournal`
intent (tag 19, below).

## Deterministic child ids (D-P9-7)

```
digest    = sha256("ostro/child/v1" ‖ parent RunId bytes (18) ‖ op u64 big-endian)
uuid      = digest[0..16], RFC 9562 version nibble = 8, variant bits = 10
partition = u16::from_be_bytes(digest[16..18]) % P     (P = the node's partition count)
child id  = RunId::new(partition, uuid)
```

One implementation: `ostro_types::ids::child_run_uuid` (the uuid part, shared by apply)
and `ostro_server::children::child_run_id`. Golden vectors are tested. A re-delivered
start, a re-derive after a crash or a repair re-send all yield the same id. The target
partition's `RunExists` / dedupe makes the start idempotent, so a parent never gets two
children for one op. The server and crash legs check exactly one child run per
(parent, op).

## Target resolution (D-P9-8)

When the parent's `ChildStarted` batch is flushed, the sink resolves the target through
the 05-11 `ModuleResolver`:
- workflow: the `start-child` name, qualified with the parent's tenant;
- version: `child-options.version`, else the workflow's **Active** version; module sha from
  that version's Module record;
- queue: `child-options.queue`, else the workflow's default queue;
- `run_timeout_ms` from the options; tenant inherited; traceparent propagated; a fresh
  `rng_seed`; input as a `PayloadRef` (blob above 256 KiB).

An unknown workflow or version is not a derive error. The sink journals
`ChildCompleted{op, Err(non-retryable{kind: "unknown_workflow"})}` right after the flush.

## Completion and outcome mapping (PF-F1, D-P9-9, PF-W7)

When a child with a parent reaches a guest terminal, its derive encodes the parent's
kind-67 `ChildCompleted{op, outcome}` event bytes into `CompleteRun.parent_event`. The
child partition's outbox forwards them verbatim as `ChildCompleted.event`. The parent's
apply journals them at `next_seq` and wakes the parent like any input.

| Child ends | Parent op outcome |
|------------|-------------------|
| Completed | `ok(child(output))` |
| Failed (guest) | `err(<the child's failure>)` |
| Cancelled | `err(cancelled)` |
| TimedOut (run timeout) | `err(timeout)` |
| Terminated | `err(non-retryable{kind: "child_terminated"})` |
| ContinuedAsNew | no completion; the successor reports (see chains) |

Hard stops (Terminated, TimedOut, apply-built) carry no `event`. The parent's apply then
encodes the no-user-data failure itself, with ts = the parent's `created_at_ms`.

**Opt-in journaling (D-P9-31).** The v0.1 format corpus contains inbound `ChildCompleted`
commands that apply as legacy inbox rows. Journaling every completion would change their
meaning, so journaling is opt-in. The parent's derive follows every attached
`SendMessage(StartChild)` with the `Derived` intent **`ChildJournal{child_run}` (tag 19)**.
Apply marks the parent's `children` row with value `[1]`. Only a marked row journals.
Commands logged before Phase 9 apply exactly as before. Format details:
`docs/storage-format.md` §18.

**Row guard.** The parent accepts a `ChildCompleted` only through a matching `children`
row: the exact `(parent, child)` row, or, for a chain, the row whose child uuid equals
`child_run_uuid(parent, op)`. The matched row is removed and `children` decremented, so a
completion is never journaled twice. A completion that matches **no** row keeps the v0.1
semantics (legacy inbox row and wake) instead of being dropped, because Phase 5's PF-F2
behaviour depends on it. Exactly-once still holds, because journaling needs a marked row
and consumes it. A `ChildCompleted` for a terminal or unknown parent is a per-message drop.

## Detached children (D-P9-10)

`child-options.detached = true`:
- the child's `StartRun` has no `ParentRef`, the parent gets no `children` row, the
  child's `run-info.parent-run-id` is none, and the child is never cancelled with the
  parent (a `ChildJournal` after a detached start is rejected as `BadIntent`);
- the sink journals `ChildCompleted{op, Ok(child(application/json {"run_id": "<id>"}))}`
  right after the flush that carried the start. The SDKs decode it as `ChildRun(run_id)`;
- a crash between the flush and that completion is repaired on journal load: the start is
  re-sent (duplicate-safe) and the `{run_id}` completion journaled once.

## cancel-op and parent close (D-P9-11, PF-F3)

- **cancel-op on an attached child op:** the op completes `err(cancelled)` at once (D5),
  and the same `EventsAppended` sends a graceful `CancelRun{child, reason:
  "parent_cancel_op"}`. The child runs its cancellation path (shielded cleanup included).
  A detached child op's cancel-op sends nothing. The cancelled op keeps its `children` row
  (no apply-side removal exists without a new intent). The child's later completion is
  journaled for an op the engine already resolved: the first completion wins, so the
  workflow sees `err(cancelled)`.
- **Parent close:** at every terminal transition of the parent (Completed, Failed,
  Cancelled, Terminated, TimedOut, ContinuedAsNew), apply sends a graceful `CancelRun` to
  every attached child still in the `children` keyspace, removes the rows and sets
  `children = 0` (`terminal::close_children`; no scan when `children == 0`).
  Completed/Failed/Cancelled/ContinuedAsNew use reason **`parent_closed`**. Hard stops
  (Terminated, TimedOut) keep the hard-stop reason, after the parent's own outbox
  notification, so Phase 5's outbox order is unchanged. The cascade is recursive: each
  level cascades at its own terminal.
- A `CancelRun` to a ContinuedAsNew child is forwarded along `continued_to` (PF-F2), so it
  reaches the chain's live run.
- A child that already ended drops the late cancel.

## Cancel cascade beats shielding (W2, D-P9-26)

D11 shielding protects a workflow's own waits from its own cancellation. It cannot keep a
child alive:
- a **shielded await on an in-flight child** still gets `err(cancelled)` when the parent is
  cancelled. The 05-08 cancel-request cascade cancels the child run itself, and the
  child's cancelled outcome completes the op;
- a **child started inside shielded cleanup** runs while the parent's cleanup runs, then
  receives the parent-close cancel (`parent_closed`) once the parent reaches its terminal
  status.

C-CHILD-11 proves both in all three languages
(`{"first": "cancelled", "cleanup_child": true}`).

## Continue-as-new chains (PF-F2, W3, D-P9-27)

- A child that continues-as-new sets no `parent_event`. The successor inherits the
  `ParentRef` (`StartRun.parent`), and the **last** run of the chain reports to the parent.
- The successor's run id differs from the original child's, so the parent matches the row
  through `child_run_uuid(parent, op)` (one prefix scan, only on an exact-row miss and only
  when `children > 0`). It journals the completion if the row is marked and removes the
  row: no leak.
- On that chain match, the parent sends **`CancelRun{original child, reason: "chain_done"}`**.
  Each ContinuedAsNew predecessor forwards it to `continued_to` and clears `continued_to`.
  `chain_done` is bookkeeping, never a user-visible cancel.
- **Deferred expiry.** EXPIRE of a ContinuedAsNew run that has a parent and a
  `continued_to` is deferred: its events and payloads are deleted, the `RunRecord` is kept
  (so CancelRun forwarding keeps working for the chain's lifetime), and EXPIRE is re-armed
  at `completed_at_ms + 2 × retention_ms`. When `chain_done` passes through a deferred
  record, the record deletes itself and cancels its timer. At the bound the record is
  deleted fully even if `chain_done` never arrived. Top-level chains expire as before.
- `chain_done` is reserved by convention only. A user cancel with exactly that reason
  cascades to children as `chain_done`, and they drop it.

## Resume after close (B3, D-P9-24)

1. A Terminated or TimedOut parent's attached children are cancelled at close and their
   rows removed (as above).
2. After a RetryRun resume (D1), the first activation's journal-load repair finds every
   unresolved attached child op without a `children` row and **re-sends its StartChild**,
   with the deterministic id and the `ChildJournal` marker, on an intents-only
   `EventsAppended`. Apply re-inserts the row.
3. At the target, a duplicate StartChild whose `RunRecord.parent` matches is handled by the
   child's state:
   - **still running:** no-op. The child completes normally and its own completion is
     journaled through the re-inserted row;
   - **terminal:** an event-less outbox `ChildCompleted`. The parent journals
     `err(non-retryable child_terminated)` for a child that **completed or failed while
     the parent was terminal** (its result is not delivered), `err(cancelled)` for
     Cancelled and `err(timeout)` for TimedOut;
   - **ContinuedAsNew:** the StartChild is forwarded along `continued_to`.
4. **Limit (W-b):** if the parent resumes after its retention window
   (`completed_at_ms + retention_ms` < now), the child may have expired. No StartChild is
   re-sent, and each unresolved attached child op gets `err(non-retryable child_terminated)`.

No cross-partition reads are involved. C-CHILD-10 covers the cancelled and
`child_terminated` cases in all three languages.

## Repair rules (W1, D-P9-25)

Journal-load repair never re-decides a delivered start from current meta:

| Child | `children` row | Target now | Repair |
|-------|----------------|------------|--------|
| attached | present | — | none |
| attached | absent | resolvable | re-send StartChild + `ChildJournal` |
| attached | absent | unresolvable | journal `unknown_workflow` once |
| attached | absent, parent closed longer ago than retention | — | `err(child_terminated)`, no re-send |
| detached | — | resolvable | re-send StartChild (duplicate-safe) + `{run_id}` completion |
| detached | — | unresolvable | journal `unknown_workflow` once |
| any | op has `OpCancelled` | — | none |

A failpoint test deploys the target between the crash and the repair. The repair then
starts the real child and journals no `unknown_workflow`.

## Failpoints

Compiled with the `failpoints` feature (`ostro-server`, forwarded by the `ostro` binary).
The prefix is `child::`.

| Failpoint | Site |
|-----------|------|
| `child::start_child_after_flush` | sink, after the accepted flush of any batch with a `ChildStarted` (resolved or not), before post-flush completions |
| `child::detached_after_flush` | sink, after the flush of a detached start, before its `{run_id}` completion |
| `child::parent_event_before_flush` | sink, after the child's `parent_event` is prepared, before the terminal batch is flushed |

`cargo test -p ostro-server --features failpoints --test children_failpoints --
--test-threads=1` (part of ci-local's failpoint suites). The crash leg (C-CRASH-01/02)
kills a spawned server at these sites: 100 iterations each, 0 violations at the Phase 9
gate.

## Limits and notes

- One extra point read per inbound `ChildCompleted` (the `children` row), one extra read
  for a duplicate StartChild, and one prefix scan only for a chain-end completion.
- A duplicate StartChild forwarded to an expired successor, or re-sent to an expired
  child, creates a fresh run with that id (same class as the W-b limit).
- A stray re-completion for a Phase 9 parent with no matching row leaves a legacy inbox
  row plus a wake (the v0.1 path), never a second journaled completion.
- CancelRun reasons `parent_closed`, `parent_cancel_op` and `chain_done` are documented
  strings (RESERVATIONS §11), not a registry space. They appear in run info as
  `cancel_reason`.
- Children run through the normal run path: workers execute them, spans and metrics come
  from the generic run instrumentation, and tenant quotas delay them like any run.
