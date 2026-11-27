---
phase: 14-multi-tenancy-security-1-0-release
plan: 34
subsystem: ostro-kafka read path (metadata, coordinator, ListOffsets, Fetch, codecs, standalone offsets)
tags: [kafka, trig-04, fetch, codecs, pure-rust, read-committed, offsets, fake-broker, kafka-4.1, binary-size]
requires:
  - phase: 14-01
    provides: "ostro-kafka contract (Client, Record, FetchedBatch, OffsetSpec, KafkaError), codec crates (flate2 rust_backend, snap, lz4_flex frame, ruzstd), kafka-protocol client-only"
  - phase: 14-31
    provides: "conn (Conn/Broker/WireCheck), versions (ranges, negotiate, ApiRequest), sasl, Client::connect"
provides:
  - "codec: decode_committed / decode_batches / decode_batch / peek_batch / decompress, MAX_DECOMPRESSED = 64 MiB, AbortedTxn, Decoded { records, next_offset, batches }"
  - "metadata: Client::{partitions, leader, topic_metadata, refresh_metadata, invalidate_topic}; leader cache + per-node Broker pool; retry policy (ATTEMPTS = 8, 50 ms doubling to 1 s), error codes"
  - "offsets: Client::{coordinator, committed, commit, list_offsets}; ApiRequest for FindCoordinator, OffsetFetch, OffsetCommit, ListOffsets"
  - "fetch: Client::{fetch, fetch_at} and fetch::Fetched { batch, next_offset, last_stable_offset, log_start_offset }; ApiRequest for Fetch"
  - "testkit (feature): create_topic(_with), produce, produce_transaction, encode_batch(_with), compress"
  - "tests: codec (10), support_read fake cluster (9), broker (10, Kafka 4.1.2), walker units (5); tests/fixtures/broker-{gzip,snappy,lz4,zstd}.bin + README"
affects: [14-32, 14-27]
tech-stack:
  added: []
  patterns:
    - "kafka-protocol decode_with_custom_compression drives framing + CRC; the closure decompresses with a bound and walks the records (counts checked) before kafka-protocol pre-allocates, and yields our Records (duplicate header keys kept, LogAppendTime honoured)"
    - "read_committed as the Java consumer: every batch with a producer id (data or control) activates aborted transactions starting at or before its last offset; ABORT markers clear the producer"
    - "consumers advance by Fetched::next_offset (past control and aborted batches), never by the last record offset"
key-files:
  created:
    - crates/ostro-kafka/tests/codec.rs
    - crates/ostro-kafka/tests/support_read.rs
    - crates/ostro-kafka/tests/broker.rs
    - crates/ostro-kafka/tests/fixtures/README.md
    - crates/ostro-kafka/tests/fixtures/broker-{gzip,snappy,lz4,zstd}.bin
  modified:
    - crates/ostro-kafka/src/{codec,metadata,offsets,fetch,testkit}.rs
    - crates/ostro-kafka/src/lib.rs (deviation: Client fields + stub removal)
    - docs/binary-size.md
    - docs/toolchains.md
key-decisions:
  - "Contract kept: Client::fetch(tp, offset, max_bytes) -> FetchedBatch unchanged; the extra outputs (next_offset, last stable offset, log start) are on the additive Client::fetch_at(.., max_wait) -> Fetched. 14-32 should use fetch_at"
  - "Fetch max_wait is clamped to request_timeout / 2 (fetch() uses 500 ms) so a long poll never trips 14-31's timeout-closes-connection rule"
  - "Snappy decodes both raw blocks and the xerial framing; Kafka 4.1's Java broker writes xerial (the captured fixture confirms it), so the testkit writes xerial by default too"
  - "testkit helpers take &Client (not a ClientConfig) so they reuse the leader cache"
  - "OffsetFetch sends require_stable = false (no transactional offset commits to trigger groups)"
requirements-completed: []
duration: ~2.5h
completed: 2026-11-27
---

# Phase 14 Plan 34: Kafka read path (metadata, fetch, codecs, standalone offsets) Summary

**Pure-Rust Kafka read path for the 1.0 topic-consumer trigger: named-topic Metadata with a leader cache and per-node connection pool, FindCoordinator with re-find, ListOffsets / Fetch (v4-12) under read_committed, standalone OffsetFetch / OffsetCommit (generation -1, empty member id), and v2 record-batch decoding through kafka-protocol's `decode_with_custom_compression` with bounded pure-Rust gzip / snappy (raw + xerial) / lz4 / zstd. Proven against a fake two-node cluster and Apache Kafka 4.1.2, including batches the Java broker compressed. The musl binary is 60.12 MB with `kafka`, 60.10 MB without it, so the 70 MB limit applies (already in `scripts/binary-size-limit`); docs updated.**

Requirement ids are not ticked here (14-27's job, D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| 74e3107 | 1 | feat: codec, metadata, offsets, fetch, testkit, walkers; tests/codec.rs, tests/support_read.rs; lib.rs wiring |
| 65f9317 | 2 | test: tests/broker.rs, fixtures + README, read_committed fix, testkit txn-coordinator retry, size docs |

## Codec coverage

| Codec | Decoder | Bound | Round trip (testkit) | Java-broker batch (fixture + live) |
|---|---|---|---|---|
| none | - | 64 MiB | yes | - |
| gzip | `flate2` rust_backend `GzDecoder` | `Read::take(64 MiB + 1)` | yes | yes |
| snappy | `snap::raw`, xerial chunks `[i32 len][raw]` | `decompress_len` checked before allocating, per chunk + running total | raw and xerial | yes (the Java broker writes **xerial**) |
| lz4 | `lz4_flex::frame::FrameDecoder` | `take` | yes (64 KiB independent blocks) | yes |
| zstd | `ruzstd` `StreamingDecoder` | `take` | yes (`ruzstd` Fastest encoder) | yes |

No fallback to C codecs was needed and none was taken.

Beyond codecs, `tests/codec.rs` (10 tests) covers: several batches with a partial trailing batch cut at 1/12/17/61/n-1 bytes (ignored, `next_offset` unchanged); `from` filtering inside a compressed batch; CRC mismatch -> `Protocol`; corrupt compressed data with a fixed CRC -> `Protocol` and never wrong records; a record length past the batch; a record count of `i32::MAX` (rejected before kafka-protocol's `reserve`); a hand-built zstd frame that claims and really regenerates 1 GiB from 32 KiB (stops at 64 MiB); gzip / lz4 / snappy beyond 64 MiB (exactly 64 MiB still decodes); read_committed with control batches, an aborted transaction, a later committed transaction from the same producer, and a fetch that starts at the ABORT marker; LogAppendTime uses the batch max timestamp; duplicate header keys stay in order; v0/v1 message sets rejected; the four broker fixtures decode exactly (200 records each).

Unit tests (`--lib`, 31 total, 10 new): the five response walkers (FindCoordinator v0-3, ListOffsets v1-7, Fetch v4-12, OffsetFetch v1-7, OffsetCommit v2-8). For each one: every valid encoding passes and decodes; every truncation and 300 seeded garbage or one-byte-mutated bodies are rejected or decode without panicking; an oversized partition-array count is rejected. Also batch header parsing, zig-zag varints, snappy bounds, huge record and header counts, and the retry-delay table.

## Fake cluster (`tests/support_read.rs`, 9 tests)

Two fake brokers share one state and use kafka-protocol broker encodings. Tests: partitions plus unknown topic (`Broker{Metadata,3}` after retries); the leader moves to node 2, so node 1 answers NOT_LEADER_OR_FOLLOWER on Fetch and on ListOffsets, metadata is refreshed and the retry goes to node 2; FindCoordinator answers COORDINATOR_NOT_AVAILABLE twice and is then found; the coordinator moves, so NOT_COORDINATOR triggers a re-find, plus a one-off NOT_COORDINATOR; every OffsetCommit was `(generation -1, member "", instance None)`; per-partition commit error surfaced (25); OFFSET_OUT_OF_RANGE surfaced and not retried; socket dropped on a Fetch, then reconnect (new ApiVersions) and continue; aborted batch plus marker skipped with `next_offset` past them, and `fetch()` continues past them on its own; max_wait clamped (60 s asked with a 2 s timeout gives 1000 ms; `fetch()` sends 500 ms).

## Broker runs (Apache Kafka 4.1.2, local `scripts/test-services.sh`, Temurin 21.0.13.1+2)

Task 2 verify, run verbatim at 65f9317: fixtures present, broker started, `tests/codec.rs` ok, `tests/broker.rs` **`test result: ok. 10 passed; 0 failed` in 12.92 s**, no `SKIP:`, limit 70000000, "70 MB" in both docs. Earlier full runs: 10/10 in 8-10 s. Observations:

- `every_codec_round_trips_in_order`: 5 codecs x 1,000 records (4 batches of 250 each) over 3 partitions. Read back with 16 KiB fetches that cut batches, then one 8 MiB fetch from the middle of a batch. Partitions of 2,000 / 2,000 / 1,000 records match exactly (offsets, keys incl. null, values incl. null, headers incl. null/empty values, CreateTime timestamps).
- `broker_recompressed_topics_decode`: topics with `compression.type=gzip|snappy|lz4|zstd`, records produced uncompressed and recompressed by the Java broker (7.3 / 11.3 / 11.1 / 6.9 KB batches), decoded exactly.
- `standalone_offsets_commit_and_fetch` / `two_clients_commit_disjoint_partitions_of_one_group`: a fresh group returns all `None`; commit then committed round-trips, and a re-commit moves the offset. Two clients x 5 rounds on partitions {0,1} and {2,3} of one group give the final 5/6/10/5, seen identically by both. `kafka-consumer-groups.sh --describe` lists the committed partitions with CURRENT-OFFSET/LAG and "has no active members" (a standalone group).
- `aborted_transactions_are_skipped`: the testkit's transactional producer (FindCoordinator key_type 1, InitProducerId v0-1, AddPartitionsToTxn v0-2, transactional Produce, EndTxn v0-2) writes plain 0..3, an aborted txn at 3..8 (marker 8), a committed txn at 9..13, and a tail. The aborted records never appear; the committed ones and the tail do; LSO 16.
- `socket_drops_reconnect_and_continue`: the client talks to a TCP proxy that rewrites the broker host/port in Metadata and FindCoordinator answers. The proxy drops every socket between requests and again during a 3 s long poll at the log end; the client reconnects (2 -> 5 proxy connections) and the poll returns the records produced after the drop at offset 60, with `next_offset` 65.
- `fetch_over_sasl_and_tls_listeners`: fetch, commit and committed over SASL_PLAINTEXT/SCRAM-SHA-512 and over SSL (test CA).
- `fetch_beyond_the_high_watermark_is_offset_out_of_range`: `Broker{Fetch,1}`; a fetch at the HW returns empty with `next_offset == HW`.

Kafka 4.1 behaviour worth knowing for 14-32: a read_committed Fetch near open or aborted transactions can return a single batch per response (fetch from 0 returned only [0..2][3..7]), so consumers must loop on `next_offset`.

Cluster broker run (plan step 3, `ostro-k8s-ci --cmd`): **not run, the cluster was withdrawn.** All broker runs are local.

## musl size (Task 2 step 4; local, zig 0.17.1 via the `ziglang` pip package, cargo-zigbuild 0.24.1)

| Build at 74e3107 | Bytes |
|---|---|
| `cargo zigbuild --release --locked -p ostro --target x86_64-unknown-linux-musl` (default, `kafka` on) | **60,118,064** (60.12 MB, statically linked, stripped) |
| same with `--no-default-features` | 60,101,616 (60.10 MB) |
| delta | +16,448 B |

The binary is at or above 60 MB, so the user's 70 MB rule applies. `scripts/binary-size-limit` already said 70000000 (b89e1fb, orchestrator), so this plan did not touch it. It updated `docs/binary-size.md`: the cap sentence in lines 3-4 (70 MB / 70,000,000 bytes, step "musl binary size limit", the file, release-dry-run), two measurement rows, and a "Limit raised to 70 MB" section with both sizes and the reason (Kafka client, user decision 2026-11-26). It also changed `docs/toolchains.md` line 18 to "under 70 MB". The delta is tiny because `ostro-server` only anchors `ostro-kafka` until 14-32 calls it, and fat LTO drops the unused code. **14-32 must re-measure with the trigger linked.** Without `kafka` the binary is also above 60 MB, so the crossing is ordinary growth on main, not the client. NFR-2.1 wording change for 14-27: "static musl binary < 70 MB (scripts/binary-size-limit)".

## Plan gate

**Gate incomplete — cluster withdrawn; consolidated gate pending on dell-4.** Neither `ostro-k8s-ci --parallel` nor the `ostro-k8s-ci --cmd` broker run could be run. Unverified for this plan:

- the full ci-local step list on a clean machine (lint group: workspace fmt / clippy / build / deny / grep gates / workflow validation; test1/test2 `cargo test --workspace`; failpoint suites; mutant matrices; campaign smoke; polyglot / clients; musl zigbuild test);
- the "musl binary size limit" step as ci-local runs it (the local measurement above, 60,118,064 B < 70,000,000, is the same command but was not run through ci-local);
- the broker suite in a CI pod (`fetch-tools kafka jre` + `test-services.sh` + `--test broker --test-threads=1`).

Verified locally: `cargo test -p ostro-kafka --features testkit` (lib 31, codec 10, support_read 9, transport 18, skeleton 1; broker 10/10 and broker_auth 10/10 with the broker up), `cargo clippy -p ostro-kafka --all-targets --all-features -D warnings` and the default-feature clippy, `cargo fmt --check` for the crate, `cargo tree` has no rdkafka/openssl-sys/lz4-sys/zstd-sys/native-tls, and both plan verify commands verbatim (Task 1 at the pre-commit tree, Task 2 at 65f9317). Only ostro-kafka changed; its only dependents (`ostro-server`, `ostro`) just anchor it, and they built in the musl release run.

## Proposed ci-local hunk for 14-27 (Kafka legs)

In `scripts/ci-local.sh`, inside the test-services step 14-27 adds, next to 14-31's broker_auth lines (after `eval "$(bash scripts/test-services.sh env)"`):

```bash
# 14-34: read path against the Kafka 4.1 test broker (fails on SKIP: the broker must be up)
out=$(cargo test -p ostro-kafka --locked --features testkit --test broker -- --test-threads=1 --nocapture 2>&1)
echo "$out" | tail -n 5
grep -Eq "test result: ok\. [1-9][0-9]* passed" <<<"$out"
if grep -q "SKIP:" <<<"$out"; then echo "kafka broker suite skipped: Kafka test service not running"; exit 1; fi
```

`tests/codec.rs` and `tests/support_read.rs` need no broker. They run in the normal `cargo test --workspace` only when the `testkit` feature is on: `ostro` and `ostro-server` dev-depend on `ostro-kafka` with `testkit`, so workspace test builds unify it in. To be explicit, 14-27 can add `cargo test -p ostro-kafka --locked --features testkit --test codec --test support_read` to the unit step. GitHub `ci.yml`: same recommendation as 14-31 (start the Kafka tgz with `scripts/test-services.sh` in the job, so the SASL/SSL variables exist and `fetch_over_sasl_and_tls_listeners` does not SKIP). No workflow file was edited.

## Hand-off to 14-32

- Use `Client::fetch_at(tp, offset, max_bytes, max_wait)` and advance to `Fetched::next_offset`, not to last record + 1: control and aborted batches advance it with no records. `max_wait` is clamped to `request_timeout / 2`.
- Use `Client::list_offsets(.., OffsetSpec::Earliest|Latest)` for `start_from` (Latest = LSO under read_committed), `committed` / `commit` for the standalone group (commit the next offset to consume). Per-partition commit errors come back as `Broker{OffsetCommit, code}`; 25/22 mean a real consumer group owns that group id.
- `OFFSET_OUT_OF_RANGE` is `Broker{api: "Fetch", code: 1}`. Reset via `list_offsets`.
- testkit: `create_topic(&client, ..)`, `create_topic_with(.., &[("compression.type", "zstd")])`, `produce(&client, topic, p, &records, Compression::X)` (one batch, acks -1, returns the base offset), `produce_transaction(&client, txn_id, topic, p, &records, commit)`.
- Re-measure the musl binary once the trigger calls the client (limit 70 MB).
- `hmac` / `sha2` are still only anchored in `lib.rs` (unused). Removing them is a Cargo.toml change for whoever owns manifests.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `crates/ostro-kafka/src/lib.rs` edited (not in files_modified)**
- **Found during:** Task 1
- **Issue:** the contract's `Client::{partitions, list_offsets, fetch, committed, commit}` stubs live in lib.rs, and `Client` needs the metadata and coordinator caches.
- **Fix:** two private `Client` fields (`meta`, `coordinators`); the stub bodies were removed and the same signatures are implemented in metadata/offsets/fetch; `pub use fetch::Fetched`; the codec dep anchors (`anyhow`, `flate2`, `lz4_flex`, `ruzstd`, `snap`) were deleted because they are now used (`hmac`/`sha2` kept). No contract type changed.
- **Commit:** 74e3107

**2. [Rule 1 - Bug] read_committed skipped a committed transaction after an ABORT marker**
- **Found during:** Task 2 (broker suite, `aborted_transactions_are_skipped`)
- **Issue:** aborted transactions were activated only by transactional data batches. A fetch that starts at the ABORT marker (Kafka 4.1 answered one batch per fetch there) got the aborted entry again, the marker did not clear it, and the same producer's next committed transaction was dropped.
- **Fix:** as the Java consumer does, every batch with a producer id (data or control) activates aborted entries up to its last offset before the marker is checked. Codec regression test added.
- **Commit:** 65f9317

**3. [Rule 1 - Bug] testkit batch encoding split every record into its own batch**
- **Found during:** Task 1 (codec tests)
- **Issue:** kafka-protocol starts a new batch whenever `offset - sequence` changes, and a constant `-1` sequence did exactly that.
- **Fix:** the sequence advances with the offset (`base_sequence + i`, base -1 without a producer).
- **Commit:** 74e3107

**4. [Rule 3] testkit transactional coordinator lookup per attempt** (InitProducerId answered NOT_COORDINATOR while `__transaction_state` was being created). Commit 65f9317.

### Interface notes (no contract change)

- The plan's `fetch(tp, offset, max_bytes, max_wait) -> FetchBatch { records, high_watermark, last_stable_offset }` exists as the additive `fetch_at` returning `Fetched`. The 14-01 contract `fetch(tp, offset, max_bytes) -> FetchedBatch` is unchanged; it long-polls 500 ms and continues past skipped batches for a few rounds.
- The testkit takes `&Client` instead of a `ClientConfig`.
- The module split follows the plan's artifacts (offsets.rs = FindCoordinator, OffsetFetch, OffsetCommit, ListOffsets; fetch.rs = Fetch), not the 14-01 stub comments.
- Plan steps 3 (cluster broker run) and the plan gate were not run (cluster withdrawn); see "Plan gate".

### TDD note

Task 1 is `tdd="true"`. Tests and code were written in the same session and committed together (no separate RED commit, so main never gets a non-compiling head between CAS updates).

## Self-Check

## Self-Check: PASSED

- FOUND on main: crates/ostro-kafka/src/{codec,metadata,offsets,fetch,testkit}.rs, tests/{codec,support_read,broker}.rs, tests/fixtures/broker-{gzip,snappy,lz4,zstd}.bin + README.md, docs/binary-size.md, docs/toolchains.md
- FOUND commits on main: 74e3107, 65f9317
- Artifact patterns: `decode_with_custom_compression` (codec.rs), `generation_id` (offsets.rs), `OSTRO_TEST_KAFKA_BOOTSTRAP` (tests/broker.rs); key links `codec::decode` in fetch.rs and `conn::` in offsets.rs
