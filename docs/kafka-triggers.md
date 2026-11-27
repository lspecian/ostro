# Kafka triggers

Ostro can consume topics from any broker that speaks the Kafka protocol and start one
workflow run per record. Requirement TRIG-04, design D-P14-43.

- [What it does](#what-it-does)
- [Configuration: `[[triggers.kafka]]`](#configuration-triggerskafka)
- [Delivery semantics](#delivery-semantics)
- [Placement and failover](#placement-and-failover)
- [Errors, quotas and `on_invalid`](#errors-quotas-and-on_invalid)
- [TLS and SASL](#tls-and-sasl)
- [Metrics and alerts](#metrics-and-alerts)
- [Compatibility](#compatibility)
- [Limits](#limits)
- [Slim build](#slim-build)

## What it does

A Kafka trigger is configuration, not an API object: a `[[triggers.kafka]]` table in
`node.toml`, identical on every node of a cluster. For every record of the configured
topics the trigger starts one run of the configured workflow:

- through the cluster's routed control plane, **as the server itself** (System), so tenant
  rules, idempotency, ingress quotas and audit apply exactly as for a webhook start;
- in the configured tenant, with the record mapped to the run input (`input`);
- tagged `ostro.kafka=<trigger name>`, `ostro.kafka.topic=<topic>`,
  `ostro.kafka.partition=<n>` and `ostro.kafka.offset=<n>` (the topic tag is left out when
  it would exceed the 128-byte tag limit), so `ostro runs list --tag ostro.kafka=orders`
  shows a trigger's runs;
- with idempotency key `__kafka/<name>/<topic>/<partition>/<offset>` (or
  `__kafka/<name>/k/<sha256 hex of the record key>` in `message_key` mode) and the
  `ReturnExisting` conflict policy, so a record consumed twice maps to the same run.

The consumer-group offsets are committed only after the run start for every record at or
below the committed offset is durable.

## Configuration: `[[triggers.kafka]]`

<!-- ostro:illustrative (the same shape as crates/ostro/tests/kafka_crash.rs writes) -->
```toml
[[triggers.kafka]]
name = "orders"                       # required, ^[A-Za-z0-9][A-Za-z0-9._-]{0,115}$
bootstrap = ["kafka-1:9092", "kafka-2:9092"]   # required, host:port
topics = ["orders"]                   # required, one or more topic names
group_id = "ostro-orders"             # required, the consumer group of the offsets
workflow = "order-flow"               # required (bare, or "<tenant>/<name>" of `tenant`)
# version = 3                         # explicit workflow version (default: the active one)
# queue = "orders"                    # queue override
tenant = "_"                          # "_" = the default tenant (default), else a tenant id
idempotency = "offset"                # "offset" (default) | "message_key"
input = "value_json"                  # "value_json" (default) | "envelope"
start_from = "committed_or_latest"    # "committed_or_latest" (default) | "committed_or_earliest"
max_in_flight = 256                   # un-done records per Kafka partition, 1..=10000
commit_interval = "1s"                # offset commit period (friendly duration, > 0, <= 1h)
on_invalid = "pause"                  # "pause" (default) | "skip"

[triggers.kafka.tls]                  # optional; present = TLS on (unless enabled = false)
enabled = true
ca_file = "/etc/ostro/kafka-ca.pem"   # extra CA bundle (added to the system roots)
server_name = "kafka.internal"        # SNI / certificate name override

[triggers.kafka.sasl]                 # optional
mechanism = "SCRAM-SHA-512"           # "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"
username = "ostro"
password_secret = "kafka-pass"        # a sealed secret name (see TLS and SASL)
```

| Field | Default | Meaning |
|-------|---------|---------|
| `name` | required | Trigger name: the `ostro.kafka` tag value and part of the idempotency key. The webhook name grammar, at most 116 bytes (so the tag fits 128 bytes). Unique among the triggers. |
| `bootstrap` | required | `host:port` brokers used to discover the cluster. |
| `topics` | required | Topic names (`[A-Za-z0-9._-]{1,249}`), no duplicates. Every partition of every topic is consumed. |
| `group_id` | required | Consumer group whose committed offsets are read (OffsetFetch) and written (OffsetCommit). Kafka tooling (`kafka-consumer-groups.sh --describe`) shows its offsets and lag. |
| `workflow` | required | Workflow to start, bare or qualified with the trigger's own tenant. |
| `version` | active version | Explicit workflow version. |
| `queue` | workflow default | Queue override (bare or qualified with the trigger's tenant). |
| `tenant` | `_` | Tenant of the runs (`_` = the default tenant). |
| `idempotency` | `offset` | `offset`: one run per `(topic, partition, offset)`. `message_key`: one run per distinct record key across the trigger's topics (records with a null key use the offset key). |
| `input` | `value_json` | `value_json`: the record value parsed as JSON (a tombstone is `null`; a non-JSON value is invalid). `envelope`: `{topic, partition, offset, timestamp_ms, key_b64, headers: [{name, value_b64}], value_b64}` (base64; null key, value or header value = `null`). |
| `start_from` | `committed_or_latest` | Position when the group has no committed offset for a partition (and after an out-of-range offset): the log end or the log start. |
| `max_in_flight` | `256` | Records of one Kafka partition whose run start is not done yet; fetching stops at this bound. |
| `commit_interval` | `1s` | How often the commit watermark is written (also at leadership loss and shutdown). |
| `on_invalid` | `pause` | What an invalid record does (see below). |
| `tls.enabled` / `tls.ca_file` / `tls.server_name` | off | TLS to the brokers. `ca_file` must exist at startup. |
| `sasl.mechanism` / `sasl.username` / `sasl.password_secret` | none | SASL authentication; all three are required when the table is present. |

**Invalid configuration fails startup** with a message naming the entry and the field, for
example `triggers: triggers.kafka[1] ("orders"): max_in_flight: must be 1..=10000, got 0`.
Unknown keys are rejected. The tables are checked once at startup; changing them needs a
restart (rolling, one node at a time, is fine: see below).

## Delivery semantics

- **At-least-once consumption.** A record can be read more than once: after a crash, a
  leadership move, or a commit that did not happen yet.
- **Exactly one run per offset (or key)** within the dedupe retention window: every start
  carries the record's idempotency key with `ReturnExisting`, so a second read finds the run
  the first one started. The dedupe row lives as long as the run's retention (D9:
  `[runs] retention`, or a workflow / queue policy); records re-read after their run has
  expired would start a new run. Keep retention longer than any realistic re-delivery
  (a stopped trigger resumes from its committed offsets).
- **Commit after durable start.** A record counts as done when the start API answers (the
  command is applied on the partition, hence durable) or returns the existing run. Per
  Kafka partition the committed offset is the smallest offset not done yet (or the
  position when everything is done); it never passes a record whose run start is not
  durable. Offsets are committed with OffsetCommit generation `-1` and no member id
  (standalone consumer, no group membership).
- **The position follows the broker's next offset**: control and aborted transaction
  batches advance it without records; the consumer reads with `read_committed`.
- **Ordering**: records of one partition are started in offset order but up to
  `max_in_flight` at a time, so runs may begin out of order. Set `max_in_flight = 1` for
  strictly sequential starts per partition.

Proved by `crates/ostro/tests/kafka_crash.rs`: a producer runs while the server is killed
with `kill -9` twenty times at random points (every third time held at the failpoint
`kafka::after_start_before_commit`, i.e. between a durable start and its commit); afterwards
every produced offset has exactly one run and the committed offsets equal the high
watermarks.

## Placement and failover

Each `(trigger, topic, kafka partition)` is consumed by exactly one node: the leader of
Ostro partition `kafka_owner(name, topic, kafka partition) mod P` (the first 8 bytes of
`sha256("__kafka/" ‖ name ‖ 0 ‖ topic ‖ 0 ‖ partition)`, the cron-owner pattern).

- When this node becomes the owner partition's leader it starts a consumer at the group's
  committed offset.
- When it loses leadership (handoff, `ostro cluster` drain, shutdown) the consumer stops
  fetching, abandons starts still in flight, commits what is done and exits; the new
  leader resumes from that committed offset. Records in between are read again and find
  their existing runs.
- After a crash the new leader resumes from the last committed offset (at most
  `commit_interval` plus `max_in_flight` records are re-read per partition).
- Topic partition lists are refreshed every 30 s, so partitions added to a topic are picked
  up without a restart.

`crates/ostro-repl-conformance/tests/kafka_failover.rs` checks this on a three-process
cluster: a SIGTERM handoff and a `kill -9` of the owner's leader while records flow, then
exactly one run per offset and no offset skipped.

## Errors, quotas and `on_invalid`

| Start answer | Behaviour |
|--------------|-----------|
| success, or the existing run | the record is done |
| `429 rate_limited` (tenant ingress quota), `403` (suspended tenant), `503` (unavailable, no leader, routing), `500` | the Kafka partition **pauses** (no new fetches) and the record is retried with backoff 1 s, 2 s, … 30 s. Nothing is ever skipped. |
| invalid record: non-JSON value with `input = "value_json"`, `400`, `404` (unknown workflow), `409`, `412` (no active version), `413` (input over the payload limit) | `on_invalid` |

- `on_invalid = "pause"` (default): the partition holds at that record. Its committed offset
  stays below it, an ERROR is logged once a minute, `ostro_kafka_paused` is 1. A rejected
  start is retried every 30 s (so deploying the missing workflow resumes the partition); a
  non-JSON value never becomes valid: fix it with `on_invalid = "skip"` and a restart, or
  move the group's offset past it with Kafka tooling while the trigger is stopped. Records
  of the same partition fetched before the bad one (up to `max_in_flight`) still start.
- `on_invalid = "skip"`: the record is counted in
  `ostro_kafka_skipped_total{trigger, reason}` (`not_json`, `unknown_workflow`,
  `no_active_version`, `too_large`, `conflict`, `invalid`), logged at WARN, and treated as
  done. **Deploy the workflow before enabling a `skip` trigger**: while the workflow does
  not exist yet, every record is an `unknown_workflow` skip.

Kafka starts count as `TriggerKind::Kafka` ingress, admitted like webhook starts: a
tenant's ingress quota throttles its Kafka triggers (pause, never skip) and a suspended
tenant's triggers pause until it is resumed.

## TLS and SASL

- TLS uses the system roots plus `ca_file`; `server_name` overrides the name checked in the
  broker certificate (default: the bootstrap host). Pure Rust (rustls with aws-lc-rs); no
  OpenSSL.
- SASL `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512` (over TLS or plaintext listeners).
  `password_secret` names a **sealed secret** (the webhook `secret_ref` rule: the exact
  stored name, or `_/<name>` for a bare name), for example:

  <!-- ostro:illustrative -->
  ```sh
  ostro secret set kafka-pass --from-env KAFKA_PASSWORD
  ```

  The password is read when the trigger connects, held in memory that is wiped on drop and
  never logged. A missing or unopenable secret is logged and retried with backoff (the
  rest of the server runs).

## Metrics and alerts

| Metric | Labels | Meaning |
|--------|--------|---------|
| `ostro_kafka_records_total` | `trigger`, `topic` | records handed to the start path |
| `ostro_kafka_runs_started_total` | `trigger`, `outcome` = `new` \| `existing` | done starts (`existing` = a re-read found its run) |
| `ostro_kafka_skipped_total` | `trigger`, `reason` | records skipped by `on_invalid = "skip"` |
| `ostro_kafka_lag` | `trigger`, `topic`, `partition` | high watermark − consumer position |
| `ostro_kafka_commit_age_seconds` | `trigger`, `topic`, `partition` | time since the committed offset last caught up with the done records |
| `ostro_kafka_paused` | `trigger`, `topic`, `partition` | 1 while the partition is paused (quota, unavailability, held invalid record) |
| `ostro_kafka_offset_resets_total` | `trigger`, `topic`, `partition` | out-of-range offsets reset per `start_from` |

Labels are bounded by the configuration (triggers × topics × partitions). The gauges are
reported by the node currently consuming the partition.

Suggested alerts: `ostro_kafka_paused == 1` for 5 minutes; `ostro_kafka_lag` growing for
15 minutes; `ostro_kafka_commit_age_seconds > 300`; any increase of
`ostro_kafka_skipped_total` or `ostro_kafka_offset_resets_total`.

## Compatibility

The trigger speaks the **Kafka protocol** through Ostro's own pure-Rust client (no
librdkafka, no OpenSSL, no C compression libraries; gzip, snappy, lz4 and zstd batches are
decoded in Rust). Any broker that implements these APIs in an overlapping version range
works:

| API | Key | Versions used |
|-----|-----|---------------|
| ApiVersions | 18 | 0–3 |
| Metadata | 3 | 1–12 |
| FindCoordinator | 10 | 0–3 |
| ListOffsets | 2 | 1–7 |
| Fetch | 1 | 4–12 (`read_committed`) |
| OffsetFetch | 9 | 1–7 |
| OffsetCommit | 8 | 2–8 |
| SaslHandshake | 17 | 1 (with SASL) |
| SaslAuthenticate | 36 | 0–2 (with SASL) |

A broker whose range does not overlap fails the connection with `unsupported_version`
naming the API and both ranges.

**Tested with Apache Kafka 4.1** (KRaft, PLAINTEXT, SASL_PLAINTEXT and SSL listeners) in
CI. Redpanda and Chronik are expected to work over the Kafka protocol (not tested in CI).

## Limits

- Topics are listed by name: no regex or pattern subscriptions.
- Consumer only: the trigger never produces (a produce connector is V2-05).
- No consumer-group membership (JoinGroup / SyncGroup / KIP-848): offsets are standalone
  commits of `group_id`. Do not share a `group_id` with a real consumer group: the broker
  refuses the commits (`Broker{OffsetCommit, 25|22}` in the log).
- One consumer per `(trigger, topic, partition)`, on the owner partition's leader:
  throughput per Kafka partition is bounded by `max_in_flight` and the start latency, and
  a cluster spreads partitions over its leaders.
- Exactly-once holds within the dedupe retention window (not cluster-wide forever).

## Slim build

The trigger is in the default build (Cargo feature `kafka`). `cargo build -p ostro
--no-default-features` drops it and the Kafka client; such a binary refuses to start with a
non-empty `[[triggers.kafka]]` list ("this ostro was built without the kafka feature").
