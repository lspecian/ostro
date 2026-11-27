//! Phase 14 (14-32, TRIG-04, D-P14-43): Kafka topic-consumer triggers.
//!
//! `[[triggers.kafka]]` in node.toml (identical on every node) names Kafka topics whose
//! records each start one run. This module validates the tables ([`validate`]) and runs the
//! consumers ([`start_triggers`]); the pure decisions (idempotency keys, run input, commit
//! watermark, owner placement) live in [`ostro_triggers::kafka`].
//!
//! # Placement
//!
//! Each `(trigger, topic, kafka partition)` is consumed by the node that leads Ostro
//! partition `kafka_owner(name, topic, kpartition, P)` (the cron-owner pattern). A
//! supervisor task watches the node's led partitions (`PartitionRoles::led`) and the
//! topics' partition lists (Metadata, refreshed every 30 s) and runs one consumer task per
//! owned triple. Leadership loss cancels the task: it stops fetching, abandons starts still
//! in flight (the next owner retries them), commits the watermark of what is done and exits.
//!
//! # Delivery
//!
//! A consumer starts at the group's committed offset (OffsetFetch of `group_id`), else at
//! `start_from` (ListOffsets latest/earliest). Every record becomes a `StartRunRequest`
//! through the **routed, unguarded control plane as System** (D-P14-6): the configured tenant
//! and workflow, tags `ostro.kafka=<name>` + topic/partition/offset, idempotency key
//! `__kafka/…` (the start path classifies it as `TriggerKind::Kafka`) and
//! `ConflictPolicy::ReturnExisting`. A start is done when the API answers (the command is
//! applied, hence durable) or reports the existing run. Offsets are committed (OffsetCommit,
//! generation -1, no member) every `commit_interval` and at stop, at the
//! `CommitTracker` watermark: never past a record whose start is not done. At-least-once
//! consumption + idempotent starts = exactly one run per offset (or key) within the dedupe
//! retention window, across crashes and leadership moves. The position advances with the
//! fetch's `next_offset` (control and aborted batches move it without records).
//!
//! # Errors
//!
//! 429 `rate_limited`, 403 (`tenant_suspended`), 503, 500 and routing errors pause the
//! Kafka partition (no new fetches) while the record is retried with backoff 1 s → 30 s;
//! nothing is skipped. Invalid records (non-JSON value with `input = "value_json"`, 400,
//! 404 unknown workflow, 409, 412, 413) follow `on_invalid`: `pause` holds the partition at
//! that record (retried every 30 s, ERROR logged once a minute, `ostro_kafka_paused = 1`),
//! `skip` counts `ostro_kafka_skipped_total{trigger,reason}` and moves on.
//!
//! # Metrics
//!
//! `ostro_kafka_records_total{trigger,topic}`, `ostro_kafka_runs_started_total{trigger,
//! outcome=new|existing}`, `ostro_kafka_skipped_total{trigger,reason}`,
//! `ostro_kafka_lag{trigger,topic,partition}` (high watermark − position),
//! `ostro_kafka_commit_age_seconds{trigger,topic,partition}`,
//! `ostro_kafka_paused{trigger,topic,partition}`, `ostro_kafka_offset_resets_total{trigger,
//! topic,partition}`. Labels are bounded by the configuration.
//!
//! # Failpoints (feature `failpoints`)
//!
//! `kafka::before_fetch` (`return(hang)` pends, any other return value fails the fetch) and
//! `kafka::after_start_before_commit` (`return(hang)` pends after a durable start, before
//! the record counts as done; any other return value abandons the record).
//!
//! Without the `kafka` feature a non-empty `[[triggers.kafka]]` list fails startup.

use std::sync::Arc;

use ostro_api::ControlPlane;
use ostro_partition::PartitionRoles;
use tokio::task::JoinHandle;

use super::{P14Ctx, P14Error};

#[cfg(feature = "kafka")]
pub use enabled::*;

/// Validate `[[triggers.kafka]]` and start the consumers: `Ok(None)` when none are
/// configured; a configuration error fails startup.
pub async fn start(
    ctx: &Arc<P14Ctx>,
    routed: Arc<dyn ControlPlane>,
    roles: Arc<dyn PartitionRoles>,
) -> Result<Option<JoinHandle<()>>, P14Error> {
    if ctx.cfg.triggers.kafka.is_empty() {
        return Ok(None);
    }
    #[cfg(feature = "kafka")]
    {
        let triggers = validate(&ctx.cfg.triggers.kafka).map_err(P14Error::new)?;
        Ok(Some(start_triggers(ctx, triggers, routed, roles)))
    }
    #[cfg(not(feature = "kafka"))]
    {
        let _ = (routed, roles);
        Err(P14Error::new(
            "[[triggers.kafka]] is configured, but this ostro was built without the kafka feature",
        ))
    }
}

#[cfg(feature = "kafka")]
mod enabled {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use ostro_api::auth::Principal;
    use ostro_api::domain::{ConflictPolicy, StartRunRequest};
    use ostro_api::{ApiError, ControlPlane, ErrorCode};
    use ostro_bridge::secrets::{SealedStore, SealingKeyRing};
    use ostro_kafka::{
        Client, ClientConfig, KafkaError, OffsetSpec, SaslConfig, SaslMechanism, TlsConfig,
        TopicPartition,
    };
    use ostro_meta::records::TOKEN_KEY_PLAINTEXT_ID;
    use ostro_partition::PartitionRoles;
    use ostro_triggers::kafka::{
        CommitTracker, IdempotencyMode, InputError, InputMode, RecordView, idempotency_key, input,
        is_topic_name, kafka_owner,
    };
    use serde::Deserialize;
    use tokio::task::{JoinHandle, JoinSet};
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info, warn};

    use crate::config::{FriendlyDuration, RawTable};
    use crate::control::SecretsAccess;
    use crate::p14::P14Ctx;

    /// Longest trigger name: `ostro.kafka=<name>` must fit a run tag (128 bytes).
    pub const MAX_NAME_LEN: usize = ostro_types::command::MAX_RUN_TAG_LEN - "ostro.kafka=".len();
    /// Default `max_in_flight` (per Kafka partition).
    pub const DEFAULT_MAX_IN_FLIGHT: usize = 256;
    /// Largest `max_in_flight`.
    pub const MAX_MAX_IN_FLIGHT: usize = 10_000;
    /// Default `commit_interval`.
    pub const DEFAULT_COMMIT_INTERVAL: Duration = Duration::from_secs(1);
    /// Metadata (topic partition list) refresh period.
    pub const METADATA_REFRESH: Duration = Duration::from_secs(30);
    /// First retry backoff.
    pub const BACKOFF_MIN: Duration = Duration::from_secs(1);
    /// Largest retry backoff.
    pub const BACKOFF_MAX: Duration = Duration::from_secs(30);
    /// Retry of a failed metadata refresh.
    const METADATA_RETRY: Duration = Duration::from_secs(5);
    /// Long-poll wait of one fetch (well below the request timeout).
    const FETCH_WAIT: Duration = Duration::from_millis(500);
    /// Bytes per fetch.
    const FETCH_BYTES: i32 = 4 << 20;
    /// Per-request timeout of the trigger's client.
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
    /// How long a stopping consumer waits for in-flight starts before abandoning them.
    const STOP_GRACE: Duration = Duration::from_secs(5);
    /// ERROR log period of a held (`on_invalid = "pause"`) record.
    const HOLD_LOG_EVERY_MS: u64 = 60_000;

    // -----------------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------------

    /// `on_invalid`.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum OnInvalid {
        /// Hold the Kafka partition at the record (default).
        #[default]
        Pause,
        /// Count it in `ostro_kafka_skipped_total` and move on.
        Skip,
    }

    /// `[triggers.kafka.tls]` (present and not `enabled = false`).
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct TlsSettings {
        /// Extra PEM CA bundle.
        pub ca_file: Option<PathBuf>,
        /// SNI / certificate name override.
        pub server_name: Option<String>,
    }

    /// `[triggers.kafka.sasl]`.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SaslSettings {
        /// Mechanism.
        pub mechanism: SaslMechanism,
        /// User name.
        pub username: String,
        /// Sealed meta secret holding the password (exact name, or `_/<name>` for a bare
        /// name: the webhook `secret_ref` rule).
        pub password_secret: String,
    }

    /// One validated `[[triggers.kafka]]` entry.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct KafkaTrigger {
        /// Trigger name (`ostro.kafka` tag, key prefix).
        pub name: String,
        /// `host:port` bootstrap brokers.
        pub bootstrap: Vec<String>,
        /// Topics consumed.
        pub topics: Vec<String>,
        /// Consumer group of the committed offsets.
        pub group_id: String,
        /// As configured (bare, or qualified with `tenant`); the start path qualifies it.
        pub workflow: String,
        /// Explicit workflow version.
        pub version: Option<u32>,
        /// Queue override.
        pub queue: Option<String>,
        /// `None` = the default tenant (`_`).
        pub tenant: Option<String>,
        /// Idempotency-key mode.
        pub idempotency: IdempotencyMode,
        /// Input mapping.
        pub input: InputMode,
        /// Start position without a committed offset.
        pub start_from: OffsetSpec,
        /// Un-done records per Kafka partition.
        pub max_in_flight: usize,
        /// Offset commit period.
        pub commit_interval: Duration,
        /// Invalid record policy.
        pub on_invalid: OnInvalid,
        /// `Some` when TLS is enabled.
        pub tls: Option<TlsSettings>,
        /// SASL credentials.
        pub sasl: Option<SaslSettings>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawTrigger {
        name: Option<String>,
        bootstrap: Option<Vec<String>>,
        topics: Option<Vec<String>>,
        group_id: Option<String>,
        workflow: Option<String>,
        version: Option<i64>,
        queue: Option<String>,
        tenant: Option<String>,
        idempotency: Option<String>,
        input: Option<String>,
        start_from: Option<String>,
        max_in_flight: Option<i64>,
        commit_interval: Option<String>,
        on_invalid: Option<String>,
        tls: Option<RawTls>,
        sasl: Option<RawSasl>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawTls {
        enabled: Option<bool>,
        ca_file: Option<PathBuf>,
        server_name: Option<String>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawSasl {
        mechanism: Option<String>,
        username: Option<String>,
        password_secret: Option<String>,
    }

    /// Validate every `[[triggers.kafka]]` table (D-P14-43 fields and defaults). Errors name
    /// the entry and the field: `triggers.kafka[1] ("orders"): max_in_flight: …`.
    pub fn validate(raw: &[RawTable]) -> Result<Vec<KafkaTrigger>, String> {
        let mut out: Vec<KafkaTrigger> = Vec::with_capacity(raw.len());
        for (i, t) in raw.iter().enumerate() {
            let r: RawTrigger = t
                .try_into_typed()
                .map_err(|e| format!("triggers.kafka[{i}]: {e}"))?;
            let at = match &r.name {
                Some(n) => format!("triggers.kafka[{i}] ({n:?})"),
                None => format!("triggers.kafka[{i}]"),
            };
            let trig = one(r).map_err(|(field, msg)| format!("{at}: {field}: {msg}"))?;
            if out.iter().any(|o| o.name == trig.name) {
                return Err(format!(
                    "{at}: name: duplicate trigger name {:?}",
                    trig.name
                ));
            }
            out.push(trig);
        }
        Ok(out)
    }

    type FieldErr = (&'static str, String);

    fn required<T>(field: &'static str, v: Option<T>) -> Result<T, FieldErr> {
        v.ok_or((field, "is required".to_owned()))
    }

    fn one(r: RawTrigger) -> Result<KafkaTrigger, FieldErr> {
        let name = required("name", r.name)?;
        if !ostro_meta::is_resource_name(&name) || name.len() > MAX_NAME_LEN {
            return Err((
                "name",
                format!(
                    "{name:?} must match ^[A-Za-z0-9][A-Za-z0-9._-]{{0,{}}}$",
                    MAX_NAME_LEN - 1
                ),
            ));
        }
        let bootstrap = required("bootstrap", r.bootstrap)?;
        if bootstrap.is_empty() {
            return Err(("bootstrap", "must list at least one host:port".into()));
        }
        for b in &bootstrap {
            let ok = b
                .rsplit_once(':')
                .is_some_and(|(h, p)| !h.is_empty() && p.parse::<u16>().is_ok_and(|p| p > 0));
            if !ok {
                return Err(("bootstrap", format!("{b:?} is not host:port")));
            }
        }
        let topics = required("topics", r.topics)?;
        if topics.is_empty() {
            return Err(("topics", "must list at least one topic".into()));
        }
        let mut seen = BTreeSet::new();
        for t in &topics {
            if !is_topic_name(t) {
                return Err((
                    "topics",
                    format!("{t:?} is not a Kafka topic name ([A-Za-z0-9._-]{{1,249}})"),
                ));
            }
            if !seen.insert(t.as_str()) {
                return Err(("topics", format!("{t:?} is listed twice")));
            }
        }
        let group_id = required("group_id", r.group_id)?;
        if group_id.is_empty() || group_id.chars().any(char::is_control) {
            return Err(("group_id", "must be non-empty printable text".into()));
        }
        let tenant = match r.tenant.as_deref() {
            None | Some("_") => None,
            Some(t) if ostro_meta::is_tenant_id(t) => Some(t.to_owned()),
            Some(t) => {
                return Err((
                    "tenant",
                    format!("{t:?} must be \"_\" or match ^[a-z0-9][a-z0-9-]{{0,62}}$"),
                ));
            }
        };
        let workflow = required("workflow", r.workflow)?;
        check_resource("workflow", &workflow, tenant.as_deref())?;
        let version = match r.version {
            None => None,
            Some(v) => match u32::try_from(v) {
                Ok(v) if v >= 1 => Some(v),
                _ => return Err(("version", format!("must be 1..={}, got {v}", u32::MAX))),
            },
        };
        let queue = match r.queue {
            None => None,
            Some(q) => {
                check_resource("queue", &q, tenant.as_deref())?;
                Some(q)
            }
        };
        let idempotency = match r.idempotency.as_deref() {
            None => IdempotencyMode::default(),
            Some(s) => IdempotencyMode::parse(s).ok_or((
                "idempotency",
                format!("expected \"offset\" or \"message_key\", got {s:?}"),
            ))?,
        };
        let input = match r.input.as_deref() {
            None => InputMode::default(),
            Some(s) => InputMode::parse(s).ok_or((
                "input",
                format!("expected \"value_json\" or \"envelope\", got {s:?}"),
            ))?,
        };
        let start_from = match r.start_from.as_deref() {
            None | Some("committed_or_latest") => OffsetSpec::Latest,
            Some("committed_or_earliest") => OffsetSpec::Earliest,
            Some(s) => {
                return Err((
                    "start_from",
                    format!(
                        "expected \"committed_or_latest\" or \"committed_or_earliest\", got {s:?}"
                    ),
                ));
            }
        };
        let max_in_flight = match r.max_in_flight {
            None => DEFAULT_MAX_IN_FLIGHT,
            Some(n) => match usize::try_from(n) {
                Ok(n) if (1..=MAX_MAX_IN_FLIGHT).contains(&n) => n,
                _ => {
                    return Err((
                        "max_in_flight",
                        format!("must be 1..={MAX_MAX_IN_FLIGHT}, got {n}"),
                    ));
                }
            },
        };
        let commit_interval = match r.commit_interval.as_deref() {
            None => DEFAULT_COMMIT_INTERVAL,
            Some(s) => {
                let d = s
                    .parse::<FriendlyDuration>()
                    .map_err(|e| ("commit_interval", e))?
                    .get();
                if d.is_zero() || d > Duration::from_secs(3600) {
                    return Err((
                        "commit_interval",
                        format!("must be > 0 and <= 1h, got {s:?}"),
                    ));
                }
                d
            }
        };
        let on_invalid = match r.on_invalid.as_deref() {
            None | Some("pause") => OnInvalid::Pause,
            Some("skip") => OnInvalid::Skip,
            Some(s) => {
                return Err((
                    "on_invalid",
                    format!("expected \"pause\" or \"skip\", got {s:?}"),
                ));
            }
        };
        let tls = match r.tls {
            None => None,
            Some(t) if t.enabled == Some(false) => None,
            Some(t) => {
                if let Some(p) = &t.ca_file
                    && !p.is_file()
                {
                    return Err(("tls.ca_file", format!("{} is not a file", p.display())));
                }
                if t.server_name.as_deref() == Some("") {
                    return Err(("tls.server_name", "must not be empty".into()));
                }
                Some(TlsSettings {
                    ca_file: t.ca_file,
                    server_name: t.server_name,
                })
            }
        };
        let sasl = match r.sasl {
            None => None,
            Some(s) => {
                let m = required("sasl.mechanism", s.mechanism)?;
                let mechanism = match m.to_ascii_uppercase().as_str() {
                    "PLAIN" => SaslMechanism::Plain,
                    "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
                    "SCRAM-SHA-512" => SaslMechanism::ScramSha512,
                    _ => {
                        return Err((
                            "sasl.mechanism",
                            format!(
                                "expected \"PLAIN\", \"SCRAM-SHA-256\" or \"SCRAM-SHA-512\", got {m:?}"
                            ),
                        ));
                    }
                };
                let username = required("sasl.username", s.username)?;
                if username.is_empty() {
                    return Err(("sasl.username", "must not be empty".into()));
                }
                let password_secret = required("sasl.password_secret", s.password_secret)?;
                if password_secret.is_empty() {
                    return Err(("sasl.password_secret", "must name a sealed secret".into()));
                }
                Some(SaslSettings {
                    mechanism,
                    username,
                    password_secret,
                })
            }
        };
        Ok(KafkaTrigger {
            name,
            bootstrap,
            topics,
            group_id,
            workflow,
            version,
            queue,
            tenant,
            idempotency,
            input,
            start_from,
            max_in_flight,
            commit_interval,
            on_invalid,
            tls,
            sasl,
        })
    }

    /// A workflow / queue name: bare, or qualified with the trigger's own tenant (D23).
    fn check_resource(field: &'static str, v: &str, tenant: Option<&str>) -> Result<(), FieldErr> {
        let local = match v.split_once('/') {
            Some((t, rest)) if ostro_meta::is_tenant_id(t) => {
                if Some(t) != tenant {
                    return Err((
                        field,
                        format!(
                            "{v:?} is qualified with tenant {t:?} but the trigger's tenant is {}",
                            tenant.map_or_else(|| "\"_\"".to_owned(), |t| format!("{t:?}"))
                        ),
                    ));
                }
                rest
            }
            _ => v,
        };
        if !ostro_meta::is_resource_name(local) {
            return Err((
                field,
                format!("{v:?} must match ^[A-Za-z0-9][A-Za-z0-9._-]{{0,127}}$"),
            ));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------------
    // Supervisor
    // -----------------------------------------------------------------------------

    struct Shared {
        ctx: Arc<P14Ctx>,
        routed: Arc<dyn ControlPlane>,
        p_count: u16,
    }

    struct TriggerRt {
        cfg: Arc<KafkaTrigger>,
        client: Option<Arc<Client>>,
        partitions: BTreeMap<String, Vec<i32>>,
        refresh_at_ms: u64,
        retry_at_ms: u64,
        backoff: Duration,
    }

    struct Running {
        cancel: CancellationToken,
        task: JoinHandle<()>,
    }

    type Key = (usize, String, i32);

    /// Start the supervisor of `triggers` on `routed` (the unguarded control plane, or any
    /// wrapper of it) and the node's roles. It stops when `ctx.shutdown` is cancelled.
    pub fn start_triggers(
        ctx: &Arc<P14Ctx>,
        triggers: Vec<KafkaTrigger>,
        routed: Arc<dyn ControlPlane>,
        roles: Arc<dyn PartitionRoles>,
    ) -> JoinHandle<()> {
        let sh = Arc::new(Shared {
            ctx: Arc::clone(ctx),
            routed,
            p_count: ctx.cfg.node.partitions.max(1),
        });
        info!(
            triggers = triggers.len(),
            names = ?triggers.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            "kafka triggers starting"
        );
        tokio::spawn(supervise(sh, triggers, roles))
    }

    async fn supervise(
        sh: Arc<Shared>,
        triggers: Vec<KafkaTrigger>,
        roles: Arc<dyn PartitionRoles>,
    ) {
        let stop = sh.ctx.shutdown.clone();
        let mut led_rx = roles.led();
        let mut rts: Vec<TriggerRt> = triggers
            .into_iter()
            .map(|cfg| TriggerRt {
                cfg: Arc::new(cfg),
                client: None,
                partitions: BTreeMap::new(),
                refresh_at_ms: 0,
                retry_at_ms: 0,
                backoff: BACKOFF_MIN,
            })
            .collect();
        let mut running: HashMap<Key, Running> = HashMap::new();
        let mut stopping: Vec<(Key, JoinHandle<()>)> = Vec::new();
        loop {
            if stop.is_cancelled() {
                break;
            }
            let led: BTreeSet<u16> = led_rx
                .borrow_and_update()
                .iter()
                .map(|(p, _)| p.0)
                .collect();
            let now = sh.ctx.clock.now_ms();
            let mut desired: BTreeSet<Key> = BTreeSet::new();
            if !led.is_empty() {
                for (ti, rt) in rts.iter_mut().enumerate() {
                    refresh(&sh, rt, now).await;
                    for (topic, parts) in &rt.partitions {
                        for &kp in parts {
                            if led.contains(&kafka_owner(&rt.cfg.name, topic, kp, sh.p_count)) {
                                desired.insert((ti, topic.clone(), kp));
                            }
                        }
                    }
                }
            }
            // Stop what this node no longer owns.
            let gone: Vec<Key> = running
                .keys()
                .filter(|k| !desired.contains(*k))
                .cloned()
                .collect();
            for k in gone {
                if let Some(r) = running.remove(&k) {
                    info!(trigger = %rts[k.0].cfg.name, topic = %k.1, partition = k.2,
                        "kafka consumer stopping (not the owner leader any more)");
                    r.cancel.cancel();
                    stopping.push((k, r.task));
                }
            }
            stopping.retain(|(_, t)| !t.is_finished());
            // Start what this node owns; a key still stopping waits for the old task's final
            // commit; a task that ended on its own is restarted.
            running.retain(|_, r| !r.task.is_finished());
            for k in desired {
                if running.contains_key(&k) || stopping.iter().any(|(s, _)| *s == k) {
                    continue;
                }
                let rt = &rts[k.0];
                let Some(client) = rt.client.clone() else {
                    continue;
                };
                let cancel = stop.child_token();
                let tp = TopicPartition {
                    topic: k.1.clone(),
                    partition: k.2,
                };
                info!(trigger = %rt.cfg.name, topic = %tp.topic, partition = tp.partition,
                    "kafka consumer starting (owner leader)");
                let task = tokio::spawn(consume(
                    Arc::clone(&sh),
                    Arc::clone(&rt.cfg),
                    client,
                    tp,
                    cancel.clone(),
                ));
                running.insert(k, Running { cancel, task });
            }
            let tick = if stopping.is_empty() {
                Duration::from_secs(1)
            } else {
                Duration::from_millis(100)
            };
            tokio::select! {
                () = stop.cancelled() => break,
                r = led_rx.changed() => {
                    if r.is_err() {
                        // The node is closing: idle until shutdown.
                        stop.cancelled().await;
                        break;
                    }
                }
                () = tokio::time::sleep(tick) => {}
            }
        }
        for r in running.values() {
            r.cancel.cancel();
        }
        let tasks: Vec<JoinHandle<()>> = running
            .into_values()
            .map(|r| r.task)
            .chain(stopping.into_iter().map(|(_, t)| t))
            .collect();
        for t in tasks {
            if tokio::time::timeout(STOP_GRACE * 3, t).await.is_err() {
                warn!("kafka consumer did not stop in time");
            }
        }
        info!("kafka triggers stopped");
    }

    /// Connect (with backoff) and refresh the topics' partition lists every
    /// [`METADATA_REFRESH`].
    async fn refresh(sh: &Shared, rt: &mut TriggerRt, now: u64) {
        if rt.client.is_none() {
            if now < rt.retry_at_ms {
                return;
            }
            match connect(sh, &rt.cfg).await {
                Ok(c) => {
                    info!(trigger = %rt.cfg.name, "kafka trigger connected");
                    rt.client = Some(Arc::new(c));
                    rt.backoff = BACKOFF_MIN;
                }
                Err(e) => {
                    warn!(trigger = %rt.cfg.name, error = %e, retry_in = ?rt.backoff,
                        "kafka trigger cannot connect");
                    rt.retry_at_ms = now.saturating_add(ms(rt.backoff));
                    rt.backoff = (rt.backoff * 2).min(BACKOFF_MAX);
                    return;
                }
            }
        }
        if now < rt.refresh_at_ms {
            return;
        }
        let Some(client) = rt.client.clone() else {
            return;
        };
        let mut all_ok = true;
        for topic in &rt.cfg.topics {
            match tokio::time::timeout(REQUEST_TIMEOUT, client.partitions(topic)).await {
                Ok(Ok(ps)) => {
                    rt.partitions.insert(topic.clone(), ps);
                }
                Ok(Err(e)) => {
                    all_ok = false;
                    warn!(trigger = %rt.cfg.name, topic = %topic, error = %e,
                        "kafka trigger: topic metadata unavailable");
                }
                Err(_) => {
                    all_ok = false;
                    warn!(trigger = %rt.cfg.name, topic = %topic,
                        "kafka trigger: topic metadata timed out");
                }
            }
        }
        let next = if all_ok {
            METADATA_REFRESH
        } else {
            METADATA_RETRY
        };
        rt.refresh_at_ms = now.saturating_add(ms(next));
    }

    fn ms(d: Duration) -> u64 {
        u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
    }

    async fn connect(sh: &Shared, t: &KafkaTrigger) -> Result<Client, String> {
        let sasl = match &t.sasl {
            None => None,
            Some(s) => Some(SaslConfig {
                mechanism: s.mechanism,
                username: s.username.clone(),
                password: sasl_password(sh, &s.password_secret)?.into(),
            }),
        };
        let cfg = ClientConfig {
            bootstrap: t.bootstrap.clone(),
            client_id: format!("ostro-{}", t.name),
            tls: t.tls.as_ref().map(|s| TlsConfig {
                ca_file: s.ca_file.clone(),
                server_name: s.server_name.clone(),
            }),
            sasl,
            request_timeout: REQUEST_TIMEOUT,
        };
        match tokio::time::timeout(REQUEST_TIMEOUT, Client::connect(cfg)).await {
            Ok(r) => r.map_err(|e| e.to_string()),
            Err(_) => Err("connect timed out".into()),
        }
    }

    /// The SASL password from the sealed meta secret (exact name, else `_/<name>`), opened
    /// with the node's sealing key ring. Never logged.
    fn sasl_password(sh: &Shared, name: &str) -> Result<String, String> {
        let access = SecretsAccess::new(sh.ctx.meta.view().clone());
        let rec = access
            .sealed(name)
            .or_else(|| {
                (!name.contains('/'))
                    .then(|| access.sealed(&format!("_/{name}")))
                    .flatten()
            })
            .ok_or_else(|| format!("sasl.password_secret {name:?} names no sealed secret"))?;
        let bytes = if rec.key_id == TOKEN_KEY_PLAINTEXT_ID {
            zeroize_vec(rec.ciphertext.clone())
        } else {
            let ring = load_ring(&sh.ctx.cfg, &sh.ctx.data_dir)
                .ok_or("no sealing key available to open sasl.password_secret")?;
            let v = SealedStore::new(Arc::new(ring))
                .open(&rec)
                .map_err(|_| format!("sasl.password_secret {name:?} cannot be opened"))?;
            zeroize_vec(v.expose().to_vec())
        };
        String::from_utf8(bytes).map_err(|_| "sasl.password_secret is not UTF-8".to_owned())
    }

    /// Identity; marks the plaintext hand-over (the caller wraps it in `Zeroizing`).
    fn zeroize_vec(v: Vec<u8>) -> Vec<u8> {
        v
    }

    fn load_ring(cfg: &crate::config::ServerConfig, data_dir: &Path) -> Option<SealingKeyRing> {
        let mut secrets = cfg.secrets.to_bridge().ok()?;
        let dev_key = data_dir.join(ostro_bridge::secrets::sealed::DEV_KEY_FILE);
        if secrets.sealing_key_file.is_none() && dev_key.is_file() {
            secrets.sealing_key_file = Some(dev_key);
        }
        SealingKeyRing::load(&secrets, data_dir, false)
            .ok()
            .flatten()
    }

    // -----------------------------------------------------------------------------
    // One consumer
    // -----------------------------------------------------------------------------

    /// What became of one record.
    #[derive(Debug)]
    enum Outcome {
        Started { created: bool },
        Skipped(&'static str),
        Abandoned,
    }

    /// How the start path's error is handled.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ErrClass {
        /// Pause and retry with backoff (quota, suspension, unavailability, routing).
        Retry,
        /// `on_invalid`.
        Invalid,
    }

    fn classify(e: &ApiError) -> ErrClass {
        match e.code {
            ErrorCode::RateLimited
            | ErrorCode::PermissionDenied
            | ErrorCode::Unavailable
            | ErrorCode::Internal
            | ErrorCode::Unauthenticated => ErrClass::Retry,
            ErrorCode::InvalidArgument
            | ErrorCode::NotFound
            | ErrorCode::Conflict
            | ErrorCode::PreconditionFailed
            | ErrorCode::PayloadTooLarge
            | ErrorCode::Unimplemented => ErrClass::Invalid,
        }
    }

    fn reason_of(e: &ApiError) -> &'static str {
        match e.code {
            ErrorCode::NotFound => "unknown_workflow",
            ErrorCode::PayloadTooLarge => "too_large",
            ErrorCode::PreconditionFailed => "no_active_version",
            ErrorCode::Conflict => "conflict",
            _ => "invalid",
        }
    }

    /// Evaluate a failpoint: `true` = fail. `return(hang)` pends forever.
    async fn failpoint(name: &'static str) -> bool {
        #[cfg(feature = "failpoints")]
        {
            if let Some(v) = fail::eval(name, |arg| arg) {
                if v.as_deref() == Some("hang") {
                    warn!(failpoint = name, "failpoint: hanging");
                    std::future::pending::<()>().await;
                }
                return true;
            }
        }
        let _ = name;
        std::future::ready(false).await
    }

    struct Labels {
        trigger: String,
        topic: String,
        partition: String,
    }

    impl Labels {
        fn gauge(&self, name: &'static str) -> metrics::Gauge {
            metrics::gauge!(name, "trigger" => self.trigger.clone(),
                "topic" => self.topic.clone(), "partition" => self.partition.clone())
        }
    }

    struct Consumer {
        sh: Arc<Shared>,
        t: Arc<KafkaTrigger>,
        client: Arc<Client>,
        tp: TopicPartition,
        cancel: CancellationToken,
        labels: Arc<Labels>,
        tracker: CommitTracker,
        last_committed: Option<i64>,
        last_commit_ms: u64,
        paused: Arc<AtomicUsize>,
        set: JoinSet<(i64, Outcome)>,
    }

    async fn consume(
        sh: Arc<Shared>,
        t: Arc<KafkaTrigger>,
        client: Arc<Client>,
        tp: TopicPartition,
        cancel: CancellationToken,
    ) {
        let labels = Arc::new(Labels {
            trigger: t.name.clone(),
            topic: tp.topic.clone(),
            partition: tp.partition.to_string(),
        });
        let Some((pos, committed)) = initial_position(&t, &client, &tp, &cancel).await else {
            return;
        };
        info!(trigger = %t.name, topic = %tp.topic, partition = tp.partition, position = pos,
            committed = committed.is_some(), "kafka consumer positioned");
        let now = sh.ctx.clock.now_ms();
        let mut c = Consumer {
            sh,
            t,
            client,
            tp,
            cancel,
            labels,
            tracker: CommitTracker::at(pos),
            last_committed: committed,
            last_commit_ms: now,
            paused: Arc::new(AtomicUsize::new(0)),
            set: JoinSet::new(),
        };
        c.run(pos).await;
    }

    /// The committed offset, else the `start_from` reset; `None` when cancelled first.
    async fn initial_position(
        t: &KafkaTrigger,
        client: &Client,
        tp: &TopicPartition,
        cancel: &CancellationToken,
    ) -> Option<(i64, Option<i64>)> {
        let mut backoff = BACKOFF_MIN;
        loop {
            let r = async {
                let c = client
                    .committed(&t.group_id, std::slice::from_ref(tp))
                    .await?;
                if let Some((_, Some(o))) = c.first() {
                    return Ok((*o, Some(*o)));
                }
                let l = client
                    .list_offsets(std::slice::from_ref(tp), t.start_from)
                    .await?;
                l.first()
                    .map(|(_, o)| (*o, None))
                    .ok_or_else(|| KafkaError::Protocol("ListOffsets returned no partition".into()))
            };
            let r = tokio::select! {
                () = cancel.cancelled() => return None,
                r = tokio::time::timeout(REQUEST_TIMEOUT * 2, r) => r,
            };
            match r {
                Ok(Ok(p)) => return Some(p),
                Ok(Err(e)) => warn!(trigger = %t.name, topic = %tp.topic, partition = tp.partition,
                    error = %e, "kafka consumer: cannot read the start position"),
                Err(_) => warn!(trigger = %t.name, topic = %tp.topic, partition = tp.partition,
                    "kafka consumer: start position timed out"),
            }
            if !sleep_or_cancel(backoff, cancel).await {
                return None;
            }
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    /// Sleep; `false` when cancelled first.
    async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
        tokio::select! {
            () = cancel.cancelled() => false,
            () = tokio::time::sleep(d) => true,
        }
    }

    impl Consumer {
        async fn run(&mut self, mut pos: i64) {
            let mut fetch_backoff = BACKOFF_MIN;
            let mut reset_pending = false;
            let paused_g = self.labels.gauge("ostro_kafka_paused");
            let lag_g = self.labels.gauge("ostro_kafka_lag");
            let age_g = self.labels.gauge("ostro_kafka_commit_age_seconds");
            loop {
                while let Some(r) = self.set.try_join_next() {
                    self.finish(r);
                }
                if self.cancel.is_cancelled() {
                    break;
                }
                let now = self.sh.ctx.clock.now_ms();
                if now.saturating_sub(self.last_commit_ms) >= ms(self.t.commit_interval) {
                    self.commit().await;
                }
                let age_ms = self
                    .sh
                    .ctx
                    .clock
                    .now_ms()
                    .saturating_sub(self.last_commit_ms);
                age_g.set(age_ms as f64 / 1000.0);
                let paused = self.paused.load(Ordering::SeqCst) > 0;
                paused_g.set(if paused { 1.0 } else { 0.0 });
                if reset_pending && self.tracker.in_flight() == 0 {
                    match self.reset().await {
                        Some(p) => {
                            pos = p;
                            reset_pending = false;
                        }
                        None => {
                            if !sleep_or_cancel(fetch_backoff, &self.cancel).await {
                                break;
                            }
                            fetch_backoff = (fetch_backoff * 2).min(BACKOFF_MAX);
                            continue;
                        }
                    }
                }
                let can_fetch =
                    !paused && !reset_pending && self.tracker.in_flight() < self.t.max_in_flight;
                if !can_fetch {
                    tokio::select! {
                        () = self.cancel.cancelled() => break,
                        Some(r) = self.set.join_next(), if !self.set.is_empty() => self.finish(r),
                        () = tokio::time::sleep(Duration::from_millis(100)) => {}
                    }
                    continue;
                }
                if failpoint("kafka::before_fetch").await {
                    warn!(trigger = %self.t.name, "failpoint kafka::before_fetch: the fetch fails");
                    if !sleep_or_cancel(fetch_backoff, &self.cancel).await {
                        break;
                    }
                    continue;
                }
                let r = tokio::select! {
                    () = self.cancel.cancelled() => break,
                    r = self.client.fetch_at(&self.tp, pos, FETCH_BYTES, FETCH_WAIT) => r,
                };
                match r {
                    Ok(f) => {
                        fetch_backoff = BACKOFF_MIN;
                        let mut all = true;
                        for rec in &f.batch.records {
                            if rec.offset < pos {
                                continue;
                            }
                            if self.tracker.in_flight() >= self.t.max_in_flight
                                || self.paused.load(Ordering::SeqCst) > 0
                            {
                                all = false;
                                break;
                            }
                            if let Err(e) = self.tracker.start(rec.offset) {
                                error!(trigger = %self.t.name, error = %e,
                                    "kafka consumer: offset order");
                                all = false;
                                break;
                            }
                            pos = rec.offset + 1;
                            self.dispatch(rec);
                        }
                        // Advance over control / aborted batches with the fetch's
                        // next_offset (never "last record + 1" alone).
                        if all && f.next_offset > pos && self.tracker.skip_to(f.next_offset).is_ok()
                        {
                            pos = f.next_offset;
                        }
                        lag_g.set((f.batch.high_watermark - pos).max(0) as f64);
                    }
                    Err(KafkaError::Broker { api, code: 1 }) if api == "Fetch" => {
                        warn!(trigger = %self.t.name, topic = %self.tp.topic,
                            partition = self.tp.partition, position = pos,
                            "kafka consumer: offset out of range; resetting per start_from");
                        metrics::counter!("ostro_kafka_offset_resets_total",
                            "trigger" => self.labels.trigger.clone(),
                            "topic" => self.labels.topic.clone(),
                            "partition" => self.labels.partition.clone())
                        .increment(1);
                        reset_pending = true;
                    }
                    Err(e) => {
                        warn!(trigger = %self.t.name, topic = %self.tp.topic,
                            partition = self.tp.partition, error = %e, retry_in = ?fetch_backoff,
                            "kafka consumer: fetch failed");
                        if !sleep_or_cancel(fetch_backoff, &self.cancel).await {
                            break;
                        }
                        fetch_backoff = (fetch_backoff * 2).min(BACKOFF_MAX);
                    }
                }
            }
            // Stop: a short grace for in-flight starts, abandon the rest, commit what is
            // done.
            let deadline = tokio::time::sleep(STOP_GRACE);
            tokio::pin!(deadline);
            while !self.set.is_empty() {
                tokio::select! {
                    () = &mut deadline => break,
                    Some(r) = self.set.join_next() => self.finish(r),
                }
            }
            self.set.abort_all();
            while let Some(r) = self.set.join_next().await {
                self.finish(r);
            }
            self.commit().await;
            paused_g.set(0.0);
            info!(trigger = %self.t.name, topic = %self.tp.topic, partition = self.tp.partition,
                committed = ?self.last_committed, "kafka consumer stopped");
        }

        /// Reposition per `start_from` after OFFSET_OUT_OF_RANGE (nothing in flight).
        async fn reset(&mut self) -> Option<i64> {
            let r = tokio::time::timeout(
                REQUEST_TIMEOUT,
                self.client
                    .list_offsets(std::slice::from_ref(&self.tp), self.t.start_from),
            )
            .await;
            match r {
                Ok(Ok(l)) => {
                    let p = l.first()?.1;
                    self.tracker = CommitTracker::at(p);
                    Some(p)
                }
                Ok(Err(e)) => {
                    warn!(trigger = %self.t.name, error = %e, "kafka consumer: reset failed");
                    None
                }
                Err(_) => None,
            }
        }

        fn finish(&mut self, r: Result<(i64, Outcome), tokio::task::JoinError>) {
            let Ok((offset, outcome)) = r else {
                return; // aborted (stop) or panicked: not done
            };
            let trig = self.labels.trigger.clone();
            match outcome {
                Outcome::Started { created } => {
                    let o = if created { "new" } else { "existing" };
                    metrics::counter!("ostro_kafka_runs_started_total", "trigger" => trig,
                        "outcome" => o)
                    .increment(1);
                }
                Outcome::Skipped(reason) => {
                    metrics::counter!("ostro_kafka_skipped_total", "trigger" => trig,
                        "reason" => reason)
                    .increment(1);
                }
                Outcome::Abandoned => return,
            }
            if let Err(e) = self.tracker.done(offset) {
                error!(trigger = %self.t.name, error = %e, "kafka consumer: tracker");
            }
        }

        /// Commit the watermark if it moved.
        async fn commit(&mut self) {
            let now = self.sh.ctx.clock.now_ms();
            let Some(w) = self.tracker.watermark() else {
                self.last_commit_ms = now;
                return;
            };
            if self.last_committed == Some(w) {
                self.last_commit_ms = now;
                return;
            }
            let r = tokio::time::timeout(
                REQUEST_TIMEOUT,
                self.client
                    .commit(&self.t.group_id, &[(self.tp.clone(), w)]),
            )
            .await;
            match r {
                Ok(Ok(())) => {
                    self.last_committed = Some(w);
                    self.last_commit_ms = now;
                }
                Ok(Err(e)) => warn!(trigger = %self.t.name, topic = %self.tp.topic,
                    partition = self.tp.partition, offset = w, error = %e,
                    "kafka consumer: offset commit failed"),
                Err(_) => warn!(trigger = %self.t.name, topic = %self.tp.topic,
                    partition = self.tp.partition, offset = w,
                    "kafka consumer: offset commit timed out"),
            }
        }

        /// Hand one record (already `start`ed in the tracker) to a start task.
        fn dispatch(&mut self, rec: &ostro_kafka::Record) {
            metrics::counter!("ostro_kafka_records_total",
                "trigger" => self.labels.trigger.clone(), "topic" => self.labels.topic.clone())
            .increment(1);
            let headers: Vec<(String, Option<&[u8]>)> = rec
                .headers
                .iter()
                .map(|(n, v)| (n.clone(), v.as_deref()))
                .collect();
            let view = RecordView {
                topic: &self.tp.topic,
                partition: self.tp.partition,
                offset: rec.offset,
                timestamp_ms: rec.timestamp_ms,
                key: rec.key.as_deref(),
                value: rec.value.as_deref(),
                headers: &headers,
            };
            let offset = rec.offset;
            let t = Arc::clone(&self.t);
            let req = input(t.input, &view).map(|v| StartRunRequest {
                workflow: t.workflow.clone(),
                input_json: serde_json::to_vec(&v).unwrap_or_else(|_| b"null".to_vec()),
                version: t.version,
                queue: t.queue.clone(),
                tags: tags(&t.name, &self.tp, offset),
                tenant: t.tenant.clone(),
                idempotency_key: Some(idempotency_key(
                    &t.name,
                    t.idempotency,
                    &self.tp.topic,
                    self.tp.partition,
                    offset,
                    view.key,
                )),
                conflict_policy: ConflictPolicy::ReturnExisting,
                ..StartRunRequest::default()
            });
            let sh = Arc::clone(&self.sh);
            let cancel = self.cancel.clone();
            let paused = Arc::clone(&self.paused);
            let labels = Arc::clone(&self.labels);
            self.set.spawn(async move {
                let out = match req {
                    Ok(req) => start_one(&sh, &t, req, offset, &cancel, &paused, &labels).await,
                    Err(e) => invalid_input(&t, e, offset, &cancel, &paused, &labels).await,
                };
                (offset, out)
            });
        }
    }

    fn tags(name: &str, tp: &TopicPartition, offset: i64) -> Vec<String> {
        let mut v = vec![format!("ostro.kafka={name}")];
        let topic = format!("ostro.kafka.topic={}", tp.topic);
        if topic.len() <= ostro_types::command::MAX_RUN_TAG_LEN {
            v.push(topic);
        }
        v.push(format!("ostro.kafka.partition={}", tp.partition));
        v.push(format!("ostro.kafka.offset={offset}"));
        v
    }

    /// Marks the partition paused while held.
    struct PauseGuard<'a>(&'a AtomicUsize, bool);

    impl PauseGuard<'_> {
        fn set(&mut self) {
            if !self.1 {
                self.0.fetch_add(1, Ordering::SeqCst);
                self.1 = true;
            }
        }
    }

    impl Drop for PauseGuard<'_> {
        fn drop(&mut self) {
            if self.1 {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    async fn start_one(
        sh: &Shared,
        t: &KafkaTrigger,
        req: StartRunRequest,
        offset: i64,
        cancel: &CancellationToken,
        paused: &AtomicUsize,
        labels: &Labels,
    ) -> Outcome {
        let mut guard = PauseGuard(paused, false);
        let mut backoff = BACKOFF_MIN;
        let mut logged_ms: Option<u64> = None;
        loop {
            let r = tokio::select! {
                () = cancel.cancelled() => return Outcome::Abandoned,
                r = ostro_api::auth::scope(Principal::system(), sh.routed.start_run(req.clone())) => r,
            };
            match r {
                Ok(res) => {
                    if failpoint("kafka::after_start_before_commit").await {
                        return Outcome::Abandoned;
                    }
                    return Outcome::Started {
                        created: res.created,
                    };
                }
                Err(e) => {
                    let class = classify(&e);
                    if class == ErrClass::Invalid && t.on_invalid == OnInvalid::Skip {
                        warn!(trigger = %t.name, topic = %labels.topic,
                            partition = %labels.partition, offset, error = %e,
                            "kafka record skipped (on_invalid = skip)");
                        return Outcome::Skipped(reason_of(&e));
                    }
                    guard.set();
                    let now = sh.ctx.clock.now_ms();
                    if logged_ms.is_none_or(|l| now.saturating_sub(l) >= HOLD_LOG_EVERY_MS) {
                        logged_ms = Some(now);
                        if class == ErrClass::Invalid {
                            error!(trigger = %t.name, topic = %labels.topic,
                                partition = %labels.partition, offset, error = %e,
                                "kafka partition paused on an invalid record (on_invalid = pause)");
                        } else {
                            warn!(trigger = %t.name, topic = %labels.topic,
                                partition = %labels.partition, offset, error = %e,
                                retry_in = ?backoff,
                                "kafka partition paused: the start was refused; retrying");
                        }
                    }
                    let wait = if class == ErrClass::Invalid {
                        BACKOFF_MAX
                    } else {
                        backoff
                    };
                    if !sleep_or_cancel(wait, cancel).await {
                        return Outcome::Abandoned;
                    }
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }

    /// A record that cannot become an input (`on_invalid`).
    async fn invalid_input(
        t: &KafkaTrigger,
        e: InputError,
        offset: i64,
        cancel: &CancellationToken,
        paused: &AtomicUsize,
        labels: &Labels,
    ) -> Outcome {
        if t.on_invalid == OnInvalid::Skip {
            warn!(trigger = %t.name, topic = %labels.topic, partition = %labels.partition,
                offset, error = %e, "kafka record skipped (on_invalid = skip)");
            return Outcome::Skipped(e.reason());
        }
        let mut guard = PauseGuard(paused, false);
        guard.set();
        loop {
            error!(trigger = %t.name, topic = %labels.topic, partition = %labels.partition,
                offset, error = %e,
                "kafka partition paused on an invalid record (on_invalid = pause)");
            if !sleep_or_cancel(Duration::from_millis(HOLD_LOG_EVERY_MS), cancel).await {
                return Outcome::Abandoned;
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn raw(toml: &str) -> Vec<RawTable> {
            config::Config::builder()
                .add_source(config::File::from_str(toml, config::FileFormat::Toml))
                .build()
                .expect("toml")
                .get::<Vec<RawTable>>("triggers.kafka")
                .expect("triggers.kafka")
        }

        const MIN: &str = r#"
name = "orders"
bootstrap = ["127.0.0.1:9092"]
topics = ["orders"]
group_id = "ostro-orders"
workflow = "order-flow"
"#;

        fn with(extra: &str) -> String {
            format!("[[triggers.kafka]]\n{MIN}\n{extra}")
        }

        /// MIN without `key`, plus `extra`.
        fn replacing(key: &str, extra: &str) -> String {
            let body: Vec<&str> = MIN
                .lines()
                .filter(|l| !l.starts_with(&format!("{key} ")))
                .collect();
            format!("[[triggers.kafka]]\n{}\n{extra}", body.join("\n"))
        }

        fn err(toml: &str) -> String {
            validate(&raw(toml)).expect_err("invalid")
        }

        #[test]
        fn defaults() {
            let v = validate(&raw(&with(""))).expect("valid");
            assert_eq!(v.len(), 1);
            let t = &v[0];
            assert_eq!(t.name, "orders");
            assert_eq!(t.bootstrap, vec!["127.0.0.1:9092"]);
            assert_eq!(t.topics, vec!["orders"]);
            assert_eq!(t.group_id, "ostro-orders");
            assert_eq!(t.workflow, "order-flow");
            assert_eq!(t.tenant, None);
            assert_eq!(t.idempotency, IdempotencyMode::Offset);
            assert_eq!(t.input, InputMode::ValueJson);
            assert_eq!(t.start_from, OffsetSpec::Latest);
            assert_eq!(t.max_in_flight, 256);
            assert_eq!(t.commit_interval, Duration::from_secs(1));
            assert_eq!(t.on_invalid, OnInvalid::Pause);
            assert_eq!(t.tls, None);
            assert_eq!(t.sasl, None);
            assert_eq!(t.version, None);
            assert_eq!(t.queue, None);
        }

        #[test]
        fn every_field() {
            let v = validate(&raw(&with(
                r#"
version = 3
queue = "q1"
tenant = "acme"
idempotency = "message_key"
input = "envelope"
start_from = "committed_or_earliest"
max_in_flight = 10000
commit_interval = "250ms"
on_invalid = "skip"
[triggers.kafka.tls]
server_name = "kafka.local"
[triggers.kafka.sasl]
mechanism = "scram-sha-512"
username = "ostro"
password_secret = "kafka-pass"
"#,
            )))
            .expect("valid");
            let t = &v[0];
            assert_eq!(t.version, Some(3));
            assert_eq!(t.queue.as_deref(), Some("q1"));
            assert_eq!(t.tenant.as_deref(), Some("acme"));
            assert_eq!(t.idempotency, IdempotencyMode::MessageKey);
            assert_eq!(t.input, InputMode::Envelope);
            assert_eq!(t.start_from, OffsetSpec::Earliest);
            assert_eq!(t.max_in_flight, 10_000);
            assert_eq!(t.commit_interval, Duration::from_millis(250));
            assert_eq!(t.on_invalid, OnInvalid::Skip);
            assert_eq!(
                t.tls,
                Some(TlsSettings {
                    ca_file: None,
                    server_name: Some("kafka.local".into())
                })
            );
            let s = t.sasl.as_ref().expect("sasl");
            assert_eq!(s.mechanism, SaslMechanism::ScramSha512);
            assert_eq!(s.username, "ostro");
            assert_eq!(s.password_secret, "kafka-pass");
        }

        #[test]
        fn tls_disabled_and_default_tenant() {
            let v = validate(&raw(&with(
                "tenant = \"_\"\n[triggers.kafka.tls]\nenabled = false\n",
            )))
            .expect("valid");
            assert_eq!(v[0].tls, None);
            assert_eq!(v[0].tenant, None);
        }

        #[test]
        fn unknown_keys_are_rejected() {
            let e = err(&with("max_inflight = 3"));
            assert!(e.starts_with("triggers.kafka[0]: "), "{e}");
            assert!(e.contains("max_inflight"), "{e}");
            let e = err(&with(
                "[triggers.kafka.sasl]\nmechanism = \"PLAIN\"\nuser = \"x\"",
            ));
            assert!(e.contains("user"), "{e}");
        }

        #[test]
        fn required_fields() {
            for field in ["name", "bootstrap", "topics", "group_id", "workflow"] {
                let e = err(&replacing(field, ""));
                assert!(
                    e.ends_with(&format!("{field}: is required")),
                    "{field}: {e}"
                );
            }
        }

        #[test]
        fn field_messages() {
            let long = format!("name = \"{}\"", "n".repeat(MAX_NAME_LEN + 1));
            let cases: Vec<(&str, &str)> = vec![
                ("name = \"-bad\"", "triggers.kafka[0] (\"-bad\"): name: "),
                (long.as_str(), "name: "),
                (
                    "bootstrap = []",
                    "bootstrap: must list at least one host:port",
                ),
                (
                    "bootstrap = [\"nohost\"]",
                    "bootstrap: \"nohost\" is not host:port",
                ),
                (
                    "bootstrap = [\"h:0\"]",
                    "bootstrap: \"h:0\" is not host:port",
                ),
                ("topics = []", "topics: must list at least one topic"),
                (
                    "topics = [\"a/b\"]",
                    "topics: \"a/b\" is not a Kafka topic name",
                ),
                ("topics = [\"a\", \"a\"]", "topics: \"a\" is listed twice"),
                ("group_id = \"\"", "group_id: must be non-empty"),
                ("workflow = \"__x\"", "workflow: \"__x\" must match"),
                (
                    "workflow = \"other/wf\"",
                    "workflow: \"other/wf\" is qualified with tenant \"other\"",
                ),
                ("tenant = \"Bad\"", "tenant: \"Bad\" must be"),
                ("version = 0", "version: must be 1..="),
                ("queue = \"a:b\"", "queue: \"a:b\" must match"),
                (
                    "idempotency = \"key\"",
                    "idempotency: expected \"offset\" or \"message_key\"",
                ),
                (
                    "input = \"raw\"",
                    "input: expected \"value_json\" or \"envelope\"",
                ),
                ("start_from = \"latest\"", "start_from: expected"),
                (
                    "max_in_flight = 0",
                    "max_in_flight: must be 1..=10000, got 0",
                ),
                (
                    "max_in_flight = 10001",
                    "max_in_flight: must be 1..=10000, got 10001",
                ),
                (
                    "commit_interval = \"soon\"",
                    "commit_interval: invalid duration",
                ),
                ("commit_interval = \"0s\"", "commit_interval: must be > 0"),
                (
                    "on_invalid = \"drop\"",
                    "on_invalid: expected \"pause\" or \"skip\"",
                ),
                (
                    "[triggers.kafka.tls]\nca_file = \"/nonexistent/ca.pem\"",
                    "tls.ca_file: /nonexistent/ca.pem is not a file",
                ),
                (
                    "[triggers.kafka.sasl]\nusername = \"u\"\npassword_secret = \"p\"",
                    "sasl.mechanism: is required",
                ),
                (
                    "[triggers.kafka.sasl]\nmechanism = \"GSSAPI\"\nusername = \"u\"\npassword_secret = \"p\"",
                    "sasl.mechanism: expected",
                ),
                (
                    "[triggers.kafka.sasl]\nmechanism = \"PLAIN\"\npassword_secret = \"p\"",
                    "sasl.username: is required",
                ),
                (
                    "[triggers.kafka.sasl]\nmechanism = \"PLAIN\"\nusername = \"u\"",
                    "sasl.password_secret: is required",
                ),
            ];
            for (extra, want) in cases {
                let key = extra.split([' ', '\n']).next().unwrap_or_default();
                let e = err(&replacing(key, extra));
                assert!(e.contains(want), "{extra}: got {e:?}, want {want:?}");
            }
        }

        #[test]
        fn qualified_workflow_in_own_tenant() {
            let v = validate(&raw(&replacing(
                "workflow",
                "workflow = \"acme/order-flow\"\ntenant = \"acme\"",
            )))
            .expect("valid");
            assert_eq!(v[0].workflow, "acme/order-flow");
        }

        #[test]
        fn duplicate_names() {
            let toml = format!("[[triggers.kafka]]\n{MIN}\n[[triggers.kafka]]\n{MIN}\n");
            assert_eq!(
                err(&toml),
                "triggers.kafka[1] (\"orders\"): name: duplicate trigger name \"orders\""
            );
        }

        #[test]
        fn tags_fit() {
            let tp = TopicPartition {
                topic: "t".repeat(249),
                partition: 3,
            };
            let t = tags(&"n".repeat(MAX_NAME_LEN), &tp, 7);
            assert!(t.iter().all(|t| t.len() <= 128), "{t:?}");
            assert_eq!(t[0].len(), 128);
            assert_eq!(t.len(), 3, "an over-long topic tag is left out");
            let tp = TopicPartition {
                topic: "orders".into(),
                partition: 3,
            };
            assert_eq!(
                tags("x", &tp, 7),
                vec![
                    "ostro.kafka=x",
                    "ostro.kafka.topic=orders",
                    "ostro.kafka.partition=3",
                    "ostro.kafka.offset=7"
                ]
            );
        }

        #[test]
        fn error_classes() {
            let e = |c| ApiError::new(c, "x");
            for c in [
                ErrorCode::RateLimited,
                ErrorCode::PermissionDenied,
                ErrorCode::Unavailable,
                ErrorCode::Internal,
            ] {
                assert_eq!(classify(&e(c)), ErrClass::Retry, "{c:?}");
            }
            for c in [
                ErrorCode::InvalidArgument,
                ErrorCode::NotFound,
                ErrorCode::PayloadTooLarge,
                ErrorCode::PreconditionFailed,
            ] {
                assert_eq!(classify(&e(c)), ErrClass::Invalid, "{c:?}");
            }
            assert_eq!(reason_of(&e(ErrorCode::NotFound)), "unknown_workflow");
        }
    }
}
