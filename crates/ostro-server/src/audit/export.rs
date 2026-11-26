//! Audit export (D-P14-11, 14-04): OTLP LogRecords for committed records and the periodic
//! chain-head export (W8).
//!
//! **Records** (when `[audit] otlp` and OTLP logs are on): each committed record is
//! *offered* to a bounded channel ([`Exporter::offer`]); a dedicated thread turns it into a
//! LogRecord (`event.name = "ostro.audit"`, severity INFO, timestamp `ts_ms`, body = the
//! action, attributes [`record_attributes`]) and emits it through the Phase 8 logger provider
//! (`logging::otel_logger_provider`). A full channel drops the export copy, never the
//! journal record (`ostro_audit_export_dropped_total`); the writer never waits on export.
//!
//! **Chain head** ([`emit_head`]): every `[audit] head_export_interval` and at shutdown the
//! writer publishes `(node, incarnation, seq, hash)` as an INFO `chain_head` log line
//! (target `ostro::audit`), the gauge `ostro_audit_chain_seq` and an `ostro.audit.head`
//! LogRecord, so truncation or a rewrite after an export is detectable off-node
//! ([`crate::audit::verify::verify_dir_against`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use ostro_types::audit::AuditOutcome;
use tracing::{info, warn};

use super::journal::Record;

/// `event.name` of a record LogRecord.
pub const EVENT_RECORD: &str = "ostro.audit";
/// `event.name` of a chain-head LogRecord.
pub const EVENT_HEAD: &str = "ostro.audit.head";
/// Attribute prefix of record fields.
pub const ATTR_PREFIX: &str = "ostro.audit.";
/// Attribute prefix of flattened `details`.
pub const DETAIL_PREFIX: &str = "ostro.audit.detail.";
/// Export channel depth.
pub const EXPORT_QUEUE: usize = 1024;
/// Metric: export copies dropped on a full channel.
pub const M_EXPORT_DROPPED: &str = "ostro_audit_export_dropped_total";
/// Metric: the last exported chain head seq.
pub const M_CHAIN_SEQ: &str = "ostro_audit_chain_seq";

/// A published chain head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainHead {
    pub node: u64,
    pub incarnation: u64,
    /// Seq of the last durable record (0 = empty journal).
    pub seq: u64,
    /// Its hash (hex).
    pub hash: String,
    /// When it was exported (injected clock).
    pub ts_ms: u64,
}

/// What the exporter sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExportItem {
    Record(Box<Record>),
    Head(ChainHead),
}

/// Where export items go (the OTLP pipeline in production; tests plug their own).
pub trait AuditExport: Send + Sync + 'static {
    fn emit(&self, item: &ExportItem);
}

enum Source {
    Pipeline,
    Fixed(SdkLoggerProvider),
}

/// The OTLP LogRecord export.
pub struct OtlpExport {
    source: Source,
}

impl std::fmt::Debug for OtlpExport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpExport")
            .field("pipeline", &matches!(self.source, Source::Pipeline))
            .finish()
    }
}

impl OtlpExport {
    /// Through the server's Phase 8 logs pipeline (whatever generation is installed).
    pub fn pipeline() -> Self {
        Self {
            source: Source::Pipeline,
        }
    }

    /// Through an explicit provider (tests: an in-memory exporter).
    pub fn with_provider(p: SdkLoggerProvider) -> Self {
        Self {
            source: Source::Fixed(p),
        }
    }

    fn provider(&self) -> Option<SdkLoggerProvider> {
        match &self.source {
            Source::Pipeline => crate::logging::otel_logger_provider(),
            Source::Fixed(p) => Some(p.clone()),
        }
    }
}

fn int(v: u64) -> AnyValue {
    match i64::try_from(v) {
        Ok(i) => AnyValue::Int(i),
        Err(_) => AnyValue::from(v.to_string()),
    }
}

fn outcome_parts(o: &AuditOutcome) -> (&'static str, Option<&str>) {
    match o {
        AuditOutcome::Ok => ("ok", None),
        AuditOutcome::Denied(r) => ("denied", Some(r)),
        AuditOutcome::Failed(r) => ("failed", Some(r)),
    }
}

/// The LogRecord attributes of a record: every field as `ostro.audit.<field>` (the outcome
/// as `ostro.audit.outcome` + `ostro.audit.outcome_reason`; absent optionals omitted;
/// `incarnation` as a string, other integers as ints) and `details` flattened as
/// `ostro.audit.detail.<key>`.
pub fn record_attributes(r: &Record) -> Vec<(String, AnyValue)> {
    let k = |n: &str| format!("{ATTR_PREFIX}{n}");
    let mut a: Vec<(String, AnyValue)> = vec![
        (k("v"), AnyValue::Int(i64::from(r.v))),
        (k("seq"), int(r.seq)),
        (k("ts_ms"), int(r.ts_ms)),
        (k("node"), int(r.node)),
        (k("incarnation"), AnyValue::from(r.incarnation.to_string())),
        (k("actor"), AnyValue::from(r.actor.clone())),
        (k("actor_kind"), AnyValue::from(r.actor_kind.as_str())),
        (k("action"), AnyValue::from(r.action.clone())),
        (k("target"), AnyValue::from(r.target.clone())),
    ];
    if let Some(t) = &r.tenant {
        a.push((k("tenant"), AnyValue::from(t.clone())));
    }
    let (result, reason) = outcome_parts(&r.outcome);
    a.push((k("outcome"), AnyValue::from(result)));
    if let Some(reason) = reason {
        a.push((k("outcome_reason"), AnyValue::from(reason.to_owned())));
    }
    if let Some(id) = &r.request_id {
        a.push((k("request_id"), AnyValue::from(id.clone())));
    }
    if let Some(c) = r.count {
        a.push((k("count"), int(c)));
    }
    if let Some(t) = r.first_ts_ms {
        a.push((k("first_ts_ms"), int(t)));
    }
    if let Some(t) = r.last_ts_ms {
        a.push((k("last_ts_ms"), int(t)));
    }
    a.push((k("prev_hash"), AnyValue::from(r.prev_hash.clone())));
    a.push((k("hash"), AnyValue::from(r.hash.clone())));
    for (dk, dv) in &r.details {
        a.push((format!("{DETAIL_PREFIX}{dk}"), AnyValue::from(dv.clone())));
    }
    a
}

/// The LogRecord attributes of a chain head.
pub fn head_attributes(h: &ChainHead) -> Vec<(String, AnyValue)> {
    vec![
        (format!("{ATTR_PREFIX}node"), int(h.node)),
        (
            format!("{ATTR_PREFIX}incarnation"),
            AnyValue::from(h.incarnation.to_string()),
        ),
        (format!("{ATTR_PREFIX}seq"), int(h.seq)),
        (format!("{ATTR_PREFIX}hash"), AnyValue::from(h.hash.clone())),
    ]
}

impl AuditExport for OtlpExport {
    fn emit(&self, item: &ExportItem) {
        let Some(p) = self.provider() else { return };
        let logger = p.logger("ostro.audit");
        let mut lr = logger.create_log_record();
        let (event, ts, body, attrs) = match item {
            ExportItem::Record(r) => (
                EVENT_RECORD,
                r.ts_ms,
                r.action.clone(),
                record_attributes(r),
            ),
            ExportItem::Head(h) => (
                EVENT_HEAD,
                h.ts_ms,
                "chain_head".to_owned(),
                head_attributes(h),
            ),
        };
        lr.set_event_name(event);
        lr.set_target("ostro::audit");
        let t = UNIX_EPOCH + Duration::from_millis(ts);
        lr.set_timestamp(t);
        lr.set_observed_timestamp(t);
        lr.set_severity_number(Severity::Info);
        lr.set_severity_text("INFO");
        lr.set_body(AnyValue::from(body));
        lr.add_attributes(attrs);
        logger.emit(lr);
    }
}

/// The bounded, non-blocking export channel and its thread.
pub struct Exporter {
    tx: Option<SyncSender<ExportItem>>,
    dropped: Arc<AtomicU64>,
    thread: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Exporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exporter")
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Exporter {
    /// Start the export thread over `target` with a channel of `cap` items.
    pub fn spawn(target: Arc<dyn AuditExport>, cap: usize) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::sync_channel::<ExportItem>(cap.max(1));
        let thread = std::thread::Builder::new()
            .name("ostro-audit-export".into())
            .spawn(move || {
                while let Ok(item) = rx.recv() {
                    target.emit(&item);
                }
            })?;
        Ok(Self {
            tx: Some(tx),
            dropped: Arc::new(AtomicU64::new(0)),
            thread: Some(thread),
        })
    }

    /// Offer an item; `false` = the channel was full and the copy was dropped (counted).
    pub fn offer(&self, item: ExportItem) -> bool {
        let Some(tx) = &self.tx else { return false };
        match tx.try_send(item) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                metrics::counter!(M_EXPORT_DROPPED).increment(1);
                false
            }
        }
    }

    /// Export copies dropped so far.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for Exporter {
    fn drop(&mut self) {
        // Closing the channel ends the thread once it drained what was offered.
        self.tx.take();
        if let Some(t) = self.thread.take()
            && t.join().is_err()
        {
            warn!("the audit export thread panicked");
        }
    }
}

/// Publish a chain head: the `chain_head` log line, the gauge and (with an exporter) the
/// `ostro.audit.head` LogRecord.
pub fn emit_head(head: &ChainHead, exporter: Option<&Exporter>) {
    info!(
        target: "ostro::audit",
        node = head.node,
        incarnation = head.incarnation,
        seq = head.seq,
        hash = %head.hash,
        "chain_head"
    );
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(M_CHAIN_SEQ).set(head.seq as f64);
    if let Some(x) = exporter {
        x.offer(ExportItem::Head(head.clone()));
    }
}
