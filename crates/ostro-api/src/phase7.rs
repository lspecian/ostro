//! Phase 7 API types (07-11): queue flow control and placement (OPS-01, DISP-03/04/05),
//! queue/workflow/run pause (WORK-05), worker administration and join (WORK-01).
//!
//! Unlike [`crate::domain`], these types carry serde and utoipa derives: they are the
//! REST wire DTOs **and** the [`crate::ControlPlane`] argument/result types (the 08-10
//! pattern), so REST and gRPC validate exactly the same values. Proto <-> type
//! conversions live in [`crate::grpc`]; the REST handlers in [`crate::rest`].
//!
//! [`Selector`] is the worker label-selector grammar, shared with the server's
//! placement (07-12 `workers/selector.rs`):
//!
//! ```text
//! selector    = requirement *( "," requirement )      ; ANDed, commas inside (..) do not split
//! requirement = key "=" value | key "==" value | key "!=" value
//!             | key ws "in" ws? "(" values ")" | key ws "notin" ws? "(" values ")"
//!             | key | "!" key
//! key         = 1*( ALPHA / DIGIT / "-" / "_" / "." / "/" )
//! value       = *( ALPHA / DIGIT / "-" / "_" / "." )   ; may be empty for = / == / !=
//! values      = value *( "," value )                   ; set members are non-empty
//! ```
//!
//! Semantics follow Kubernetes label selectors: `k!=v` and `k notin (..)` also match a
//! worker without label `k`; `!k` matches only without it; the empty selector matches
//! every worker.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;

/// Message of every Phase 7 [`crate::ControlPlane`] default body (P-16): the server
/// implements the methods in 07-14.
pub const P7_PENDING: &str = "phase 7: lands in 07-14";

/// At most this many [`QueueFlow::selector`] entries (meta `SELECTOR_MAX_ENTRIES`).
pub const SELECTOR_MAX_ENTRIES: usize = 32;
/// Longest accepted [`QueueFlow::selector`] entry, in bytes (meta `SELECTOR_ENTRY_MAX`).
pub const SELECTOR_ENTRY_MAX: usize = 128;
/// Id prefix of worker join tokens (`wkr_<id>`; RESERVATIONS §11).
pub const WORKER_TOKEN_PREFIX: &str = "wkr_";
/// Longest worker id, in bytes.
pub const MAX_WORKER_ID_LEN: usize = 128;
/// At most this many labels per worker.
pub const MAX_WORKER_LABELS: usize = 64;
/// Longest pause detail, in bytes (as the lifecycle reasons, `MAX_REASON_LEN`).
pub const MAX_PAUSE_DETAIL_LEN: usize = crate::domain::MAX_REASON_LEN;
/// `details.reason` of a refused join (unknown, revoked or expired token; the three are
/// not distinguished).
pub const REASON_JOIN_TOKEN_INVALID: &str = "join_token_invalid";

// ---------------------------------------------------------------------------------
// Queue flow
// ---------------------------------------------------------------------------------

/// Where a queue's runs execute.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Placement {
    /// On the server's embedded executor only.
    Embedded,
    /// On connected workers only.
    Workers,
    /// Embedded first, spilling to workers under load.
    Auto,
}

impl Placement {
    /// Wire name (`embedded` | `workers` | `auto`).
    pub fn as_str(self) -> &'static str {
        match self {
            Placement::Embedded => "embedded",
            Placement::Workers => "workers",
            Placement::Auto => "auto",
        }
    }

    /// Inverse of [`Placement::as_str`].
    pub fn parse(s: &str) -> Option<Placement> {
        match s {
            "embedded" => Some(Placement::Embedded),
            "workers" => Some(Placement::Workers),
            "auto" => Some(Placement::Auto),
            _ => None,
        }
    }
}

/// What happens when a run exhausts its retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExhaustAction {
    /// Fail the run (the default).
    Fail,
    /// Pause the run for an operator (auto-pause).
    Pause,
}

/// `limit` events per `period_ms` (> 0), with `burst` extra tokens.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RateSpecDto {
    /// Events per period.
    pub limit: u32,
    /// Period length in ms; must be > 0.
    pub period_ms: u64,
    /// Extra tokens on top of `limit`.
    #[serde(default)]
    pub burst: u32,
}

/// Auto-placement spill thresholds. Percentages are 0..=100.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SpillPolicyDto {
    /// Process CPU %.
    pub cpu_pct: u8,
    /// Embedded slot utilisation %.
    pub slots_pct: u8,
    /// Wasm memory %.
    pub mem_pct: u8,
    /// Oldest-ready queue lag in ms.
    pub lag_ms: u64,
    /// How long a signal must stay above its threshold, in ms.
    pub sustain_ms: u64,
    /// Minimum dwell before spilling stops, in ms.
    pub cooldown_ms: u64,
    /// Exit threshold as a % of the entry threshold.
    pub hysteresis_pct: u8,
}

/// A queue's flow configuration (stored in the meta `QueuePolicy` Phase 7 fields).
/// Absent fields mean no limit or the server default.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct QueueFlow {
    /// Placement; absent = `[workers] default_placement`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<Placement>,
    /// Worker label selector requirements, ANDed (see the module docs for the
    /// grammar): at most 32 entries of at most 128 bytes.
    #[serde(default)]
    pub selector: Vec<String>,
    /// Per-queue spill thresholds for `auto`; absent = `[workers.spill]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spill: Option<SpillPolicyDto>,
    /// Maximum concurrently running activations of the queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u32>,
    /// Fair-share weight (default `[flow] default_queue_weight`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u32>,
    /// Maximum concurrently running activations per concurrency key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_concurrency: Option<u32>,
    /// Activation start throttle of the queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throttle: Option<RateSpecDto>,
    /// Activation start throttle per concurrency key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_throttle: Option<RateSpecDto>,
    /// Run-start rate limit of the queue (excess starts answer 429 `rate_limited`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateSpecDto>,
    /// Run-start rate limit per concurrency key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_rate_limit: Option<RateSpecDto>,
    /// What happens when a run exhausts its retries; absent = `fail`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_retries_exhausted: Option<ExhaustAction>,
    /// The queue is paused (no new activations are granted).
    #[serde(default)]
    pub paused: bool,
}

fn rate_check(field: &str, r: &Option<RateSpecDto>) -> Result<(), ApiError> {
    match r {
        Some(r) if r.period_ms == 0 => Err(ApiError::invalid_argument(format!(
            "{field}.period_ms must be > 0"
        ))),
        _ => Ok(()),
    }
}

impl QueueFlow {
    /// The 05-07 meta validation rules (placement and `on_retries_exhausted` are
    /// range-checked by their enums), plus the selector grammar. Implementations call
    /// it before storing; both transports therefore reject the same values.
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.selector.len() > SELECTOR_MAX_ENTRIES {
            return Err(ApiError::invalid_argument(format!(
                "selector has {} entries (max {SELECTOR_MAX_ENTRIES})",
                self.selector.len()
            )));
        }
        for (i, e) in self.selector.iter().enumerate() {
            if e.len() > SELECTOR_ENTRY_MAX {
                return Err(ApiError::invalid_argument(format!(
                    "selector[{i}] is {} bytes (max {SELECTOR_ENTRY_MAX})",
                    e.len()
                )));
            }
            Selector::parse(e)
                .map_err(|err| ApiError::invalid_argument(format!("selector[{i}]: {err}")))?;
        }
        if let Some(s) = &self.spill {
            for (field, v) in [
                ("spill.cpu_pct", s.cpu_pct),
                ("spill.slots_pct", s.slots_pct),
                ("spill.mem_pct", s.mem_pct),
                ("spill.hysteresis_pct", s.hysteresis_pct),
            ] {
                if v > 100 {
                    return Err(ApiError::invalid_argument(format!(
                        "{field} = {v} is out of range (max 100)"
                    )));
                }
            }
        }
        rate_check("throttle", &self.throttle)?;
        rate_check("key_throttle", &self.key_throttle)?;
        rate_check("rate_limit", &self.rate_limit)?;
        rate_check("key_rate_limit", &self.key_rate_limit)
    }

    /// The compiled selector (every entry ANDed).
    pub fn compiled_selector(&self) -> Result<Selector, SelectorError> {
        Selector::from_entries(&self.selector)
    }
}

/// A queue's flow configuration with its record version.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct QueueFlowInfo {
    /// Queue name.
    pub queue: String,
    /// Meta record version (CAS token for `expected_version`); 0 = never stored.
    pub version: u64,
    /// The flow configuration.
    #[serde(flatten)]
    pub flow: QueueFlow,
}

/// `PUT /v1/queues/{name}/flow` body: the flow fields plus an optional CAS version.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PutQueueFlowBody {
    /// The flow configuration (replaces every flow field).
    #[serde(flatten)]
    pub flow: QueueFlow,
    /// The `version` the caller read; a stale value answers 412.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
}

/// Pause state of a queue or workflow after a pause/resume call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PauseState {
    /// Queue or workflow name.
    pub name: String,
    /// Paused after the call.
    pub paused: bool,
    /// False when it already was in the requested state (idempotent repeat).
    pub changed: bool,
}

// ---------------------------------------------------------------------------------
// Run pause
// ---------------------------------------------------------------------------------

/// `POST /v1/runs/{run_id}/pause` body (optional).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PauseRunRequest {
    /// Operator note stored with the pause mark (at most 1024 bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl PauseRunRequest {
    /// Detail length check.
    pub fn validate(&self) -> Result<(), ApiError> {
        match &self.detail {
            Some(d) if d.len() > MAX_PAUSE_DETAIL_LEN => Err(ApiError::invalid_argument(format!(
                "detail is {} bytes (max {MAX_PAUSE_DETAIL_LEN})",
                d.len()
            ))),
            _ => Ok(()),
        }
    }
}

/// Pause state of a run after a pause/resume call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunPauseState {
    /// Run id.
    pub run_id: String,
    /// Paused after the call.
    pub paused: bool,
    /// False when the run already was in the requested state.
    pub changed: bool,
}

// ---------------------------------------------------------------------------------
// Demand
// ---------------------------------------------------------------------------------

/// Worker demand of a queue (D-W12): `demand = running_on_workers + admissible
/// backlog for workers` (`embedded` queues: 0; `auto`: the backlog only while
/// spilling). The KEDA scaling signal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DemandSnapshot {
    /// Queue name.
    pub queue: String,
    /// Worker slots the queue could use now.
    pub demand: u64,
    /// Admissible ready entries waiting for a worker.
    pub backlog: u64,
    /// Activations of the queue running on workers.
    pub running_on_workers: u64,
    /// Effective placement.
    pub placement: Placement,
    /// `auto` placement is currently spilling to workers.
    pub spilling: bool,
    /// Connected workers.
    pub workers: u64,
    /// Free worker slots (all workers).
    pub worker_slots_free: u64,
}

// ---------------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------------

/// A connected worker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerInfo {
    /// Worker id (from its certificate).
    pub worker_id: String,
    /// Worker labels (matched by queue selectors).
    pub labels: BTreeMap<String, String>,
    /// Advertised slots.
    pub slots: u32,
    /// Slots not holding a lease.
    pub free_slots: u32,
    /// Leases held.
    pub leases: u32,
    /// When the session opened (Unix ms).
    pub connected_at_ms: u64,
    /// Server node holding the session.
    pub node_id: u64,
    /// Draining: no new work is assigned.
    pub draining: bool,
}

/// `GET /v1/workers` response.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerList {
    /// Workers, sorted by id.
    pub workers: Vec<WorkerInfo>,
}

/// `POST /v1/workers/{id}/drain` response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DrainResult {
    /// Worker id.
    pub worker_id: String,
    /// True when the worker already was draining.
    pub already_draining: bool,
}

/// `POST /v1/workers/tokens` body (optional).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateWorkerTokenRequest {
    /// Token lifetime in ms (> 0); absent = `[workers] join_token_ttl`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

impl CreateWorkerTokenRequest {
    /// `ttl_ms` must be > 0 when given.
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.ttl_ms == Some(0) {
            return Err(ApiError::invalid_argument("ttl_ms must be > 0"));
        }
        Ok(())
    }
}

/// A freshly created join token. `token` is shown here once and never again;
/// `Debug` redacts it (and the join command that embeds it).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerTokenCreated {
    /// Token id (`wkr_<id>`).
    pub token_id: String,
    /// `wkr_<id>.<secret>`; only in this response.
    pub token: String,
    /// A ready-to-run `ostro worker --join .. --token .. --ca-hash ..` line.
    pub join_command: String,
    /// Expiry (Unix ms).
    pub expires_at_ms: u64,
}

impl std::fmt::Debug for WorkerTokenCreated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerTokenCreated")
            .field("token_id", &self.token_id)
            .field("token", &"<redacted>")
            .field("join_command", &"<redacted>")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

/// A join token without its secret.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerTokenInfo {
    /// Token id (`wkr_<id>`).
    pub token_id: String,
    /// Creation time (Unix ms).
    pub created_at_ms: u64,
    /// Expiry (Unix ms), when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

/// `GET /v1/workers/tokens` response.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerTokenList {
    /// Tokens, sorted by id.
    pub tokens: Vec<WorkerTokenInfo>,
}

/// `POST /v1/workers/join` body: the 07-07 worker's join contract. `Debug` redacts
/// the token.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JoinRequest {
    /// `wkr_<id>.<secret>`.
    pub token: String,
    /// PEM PKCS#10 CSR of a key generated in the worker's memory.
    pub csr_pem: String,
    /// Worker id: 1..=128 bytes of `[A-Za-z0-9_.:-]`.
    pub worker_id: String,
    /// Worker labels (keys: selector keys; values: selector values).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl std::fmt::Debug for JoinRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinRequest")
            .field("token", &"<redacted>")
            .field("csr_pem", &self.csr_pem)
            .field("worker_id", &self.worker_id)
            .field("labels", &self.labels)
            .finish()
    }
}

impl JoinRequest {
    /// Shape checks shared by both transports (the token itself is checked by the
    /// implementation; error messages never contain it).
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.token.is_empty() {
            return Err(ApiError::invalid_argument("token is required"));
        }
        if !self.csr_pem.contains("-----BEGIN CERTIFICATE REQUEST-----") {
            return Err(ApiError::invalid_argument(
                "csr_pem must be a PEM certificate request",
            ));
        }
        validate_worker_id(&self.worker_id)?;
        validate_labels(&self.labels)
    }
}

/// Worker id grammar: 1..=128 bytes of `[A-Za-z0-9_.:-]`.
pub fn validate_worker_id(id: &str) -> Result<(), ApiError> {
    let ok = !id.is_empty()
        && id.len() <= MAX_WORKER_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(ApiError::invalid_argument(format!(
            "worker_id must be 1..={MAX_WORKER_ID_LEN} bytes of [A-Za-z0-9_.:-]"
        )))
    }
}

/// Worker labels: at most 64, keys are selector keys and values selector values.
pub fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), ApiError> {
    if labels.len() > MAX_WORKER_LABELS {
        return Err(ApiError::invalid_argument(format!(
            "{} labels (max {MAX_WORKER_LABELS})",
            labels.len()
        )));
    }
    for (k, v) in labels {
        if !is_key(k) {
            return Err(ApiError::invalid_argument(format!(
                "invalid label key {k:?}"
            )));
        }
        if !is_value(v) {
            return Err(ApiError::invalid_argument(format!(
                "invalid value for label {k:?}"
            )));
        }
    }
    Ok(())
}

/// `POST /v1/workers/join` response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JoinResponse {
    /// PEM client certificate (SAN URI `ostro://<cluster>/worker/<token_id>/<worker_id>`).
    pub client_cert_pem: String,
    /// PEM cluster CA certificate.
    pub ca_cert_pem: String,
    /// Cluster listener endpoints (`host:port`).
    pub endpoints: Vec<String>,
    /// Renew the certificate after this many ms.
    pub renew_after_ms: u64,
    /// Cluster id.
    pub cluster_id: String,
}

/// Non-empty name check for path parameters.
pub fn require_name(what: &str, v: &str) -> Result<(), ApiError> {
    if v.is_empty() {
        return Err(ApiError::invalid_argument(format!(
            "{what} must not be empty"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------
// Label selectors
// ---------------------------------------------------------------------------------

/// A selector syntax error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectorError(pub String);

impl std::fmt::Display for SelectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SelectorError {}

/// One selector requirement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Requirement {
    /// `k=v` / `k==v`: label present with this value.
    Eq(String, String),
    /// `k!=v`: label absent or with another value.
    NotEq(String, String),
    /// `k in (a,b)`: label present with one of the values.
    In(String, BTreeSet<String>),
    /// `k notin (a,b)`: label absent or with none of the values.
    NotIn(String, BTreeSet<String>),
    /// `k`: label present.
    Exists(String),
    /// `!k`: label absent.
    NotExists(String),
}

impl Requirement {
    /// Whether `labels` satisfy the requirement.
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        match self {
            Requirement::Eq(k, v) => labels.get(k) == Some(v),
            Requirement::NotEq(k, v) => labels.get(k) != Some(v),
            Requirement::In(k, set) => labels.get(k).is_some_and(|v| set.contains(v)),
            Requirement::NotIn(k, set) => !labels.get(k).is_some_and(|v| set.contains(v)),
            Requirement::Exists(k) => labels.contains_key(k),
            Requirement::NotExists(k) => !labels.contains_key(k),
        }
    }
}

/// A compiled label selector: requirements ANDed (empty = matches everything).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Selector {
    reqs: Vec<Requirement>,
}

fn is_key_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/')
}

fn is_value_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')
}

fn is_key(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(is_key_byte)
}

fn is_value(s: &str) -> bool {
    s.bytes().all(is_value_byte)
}

fn key(s: &str) -> Result<String, SelectorError> {
    if is_key(s) {
        Ok(s.to_owned())
    } else {
        Err(SelectorError(format!("invalid label key {s:?}")))
    }
}

fn value(s: &str) -> Result<String, SelectorError> {
    if is_value(s) {
        Ok(s.to_owned())
    } else {
        Err(SelectorError(format!("invalid label value {s:?}")))
    }
}

/// Split at commas outside parentheses.
fn split_terms(s: &str) -> Result<Vec<&str>, SelectorError> {
    let mut out = Vec::new();
    let (mut depth, mut start) = (0u32, 0usize);
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| SelectorError("unbalanced ')'".into()))?;
            }
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(SelectorError("unbalanced '('".into()));
    }
    out.push(&s[start..]);
    Ok(out)
}

fn set_members(rest: &str) -> Result<BTreeSet<String>, SelectorError> {
    let inner = rest
        .trim()
        .strip_prefix('(')
        .and_then(|r| r.strip_suffix(')'))
        .ok_or_else(|| SelectorError("expected (value, ...) after in/notin".into()))?;
    let mut set = BTreeSet::new();
    for v in inner.split(',') {
        let v = v.trim();
        if v.is_empty() {
            return Err(SelectorError("empty value in set".into()));
        }
        set.insert(value(v)?);
    }
    Ok(set)
}

fn parse_requirement(term: &str) -> Result<Requirement, SelectorError> {
    let term = term.trim();
    if term.is_empty() {
        return Err(SelectorError("empty requirement".into()));
    }
    if let Some(rest) = term.strip_prefix('!') {
        return Ok(Requirement::NotExists(key(rest.trim())?));
    }
    let end = term
        .bytes()
        .position(|b| !is_key_byte(b))
        .unwrap_or(term.len());
    let k = key(&term[..end])?;
    let after = &term[end..];
    let rest = after.trim_start();
    if rest.is_empty() {
        return Ok(Requirement::Exists(k));
    }
    if let Some(v) = rest.strip_prefix("!=") {
        return Ok(Requirement::NotEq(k, value(v.trim())?));
    }
    if let Some(v) = rest.strip_prefix("==") {
        return Ok(Requirement::Eq(k, value(v.trim())?));
    }
    if let Some(v) = rest.strip_prefix('=') {
        return Ok(Requirement::Eq(k, value(v.trim())?));
    }
    // Set forms need whitespace between the key and the operator ("zonein" is a key).
    if after.len() != rest.len() {
        for (op, negated) in [("notin", true), ("in", false)] {
            if let Some(r) = rest.strip_prefix(op)
                && (r.starts_with('(') || r.starts_with(char::is_whitespace))
            {
                let set = set_members(r)?;
                return Ok(if negated {
                    Requirement::NotIn(k, set)
                } else {
                    Requirement::In(k, set)
                });
            }
        }
    }
    Err(SelectorError(format!("cannot parse requirement {term:?}")))
}

impl Selector {
    /// Parse one selector string: comma-separated requirements (commas inside a set
    /// do not split), ANDed. The empty or all-blank string is the empty selector.
    pub fn parse(s: &str) -> Result<Selector, SelectorError> {
        if s.trim().is_empty() {
            return Ok(Selector::default());
        }
        let reqs = split_terms(s)?
            .into_iter()
            .map(parse_requirement)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Selector { reqs })
    }

    /// Parse several selector strings (e.g. [`QueueFlow::selector`]) and AND them.
    pub fn from_entries<S: AsRef<str>>(entries: &[S]) -> Result<Selector, SelectorError> {
        let mut reqs = Vec::new();
        for e in entries {
            reqs.extend(Selector::parse(e.as_ref())?.reqs);
        }
        Ok(Selector { reqs })
    }

    /// Whether every requirement matches `labels`.
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.reqs.iter().all(|r| r.matches(labels))
    }

    /// The requirements, in order.
    pub fn requirements(&self) -> &[Requirement] {
        &self.reqs
    }

    /// No requirement (matches every worker).
    pub fn is_empty(&self) -> bool {
        self.reqs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
        kv.iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn phase7_selector_table() {
        let gpu = labels(&[("gpu", "true"), ("zone", "eu-1"), ("tier", "a")]);
        let cpu = labels(&[("zone", "us-1")]);
        let none = labels(&[]);
        // (selector, gpu, cpu, none)
        let table: &[(&str, bool, bool, bool)] = &[
            ("", true, true, true),
            ("gpu=true", true, false, false),
            ("gpu == true", true, false, false),
            ("gpu!=true", false, true, true),
            ("zone in (eu-1, eu-2)", true, false, false),
            ("zone in(eu-1)", true, false, false),
            ("zone notin (eu-1,eu-2)", false, true, true),
            ("gpu", true, false, false),
            ("!gpu", false, true, true),
            ("zone,!gpu", false, true, false),
            ("zone in (eu-1,us-1),tier=a", true, false, false),
            ("tier=", false, false, false),
            ("tier!=", true, true, true),
            ("example.com/role=x", false, false, false),
        ];
        for (s, g, c, n) in table {
            let sel = Selector::parse(s).unwrap_or_else(|e| panic!("{s}: {e}"));
            assert_eq!(sel.matches(&gpu), *g, "{s} vs gpu");
            assert_eq!(sel.matches(&cpu), *c, "{s} vs cpu");
            assert_eq!(sel.matches(&none), *n, "{s} vs none");
        }
        assert!(Selector::parse(" ").unwrap().is_empty());
        let sel = Selector::from_entries(&["gpu=true", "zone in (eu-1)"]).unwrap();
        assert_eq!(sel.requirements().len(), 2);
        assert!(sel.matches(&gpu) && !sel.matches(&cpu));
    }

    #[test]
    fn phase7_selector_rejects_bad_syntax() {
        for bad in [
            "=v",
            "gpu=tr ue",
            "gpu=a=b",
            "zone in eu-1",
            "zone in ()",
            "zone in (a,,b)",
            "zone in (a",
            "zone)",
            "zonein(a)",
            "zone ~ a",
            "!",
            "!gpu=true",
            "a,,b",
            "gpu=true,",
            "k=v$",
            "k y",
        ] {
            assert!(Selector::parse(bad).is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn phase7_flow_validation() {
        let ok = QueueFlow {
            placement: Some(Placement::Workers),
            selector: vec!["gpu=true".into()],
            spill: Some(SpillPolicyDto {
                cpu_pct: 100,
                ..Default::default()
            }),
            rate_limit: Some(RateSpecDto {
                limit: 1,
                period_ms: 1,
                burst: 0,
            }),
            ..Default::default()
        };
        ok.validate().expect("valid");
        let cases: Vec<(QueueFlow, &str)> = vec![
            (
                QueueFlow {
                    selector: vec!["x".into(); SELECTOR_MAX_ENTRIES + 1],
                    ..Default::default()
                },
                "entries",
            ),
            (
                QueueFlow {
                    selector: vec!["k".repeat(SELECTOR_ENTRY_MAX + 1)],
                    ..Default::default()
                },
                "bytes",
            ),
            (
                QueueFlow {
                    selector: vec!["zone in (".into()],
                    ..Default::default()
                },
                "selector[0]",
            ),
            (
                QueueFlow {
                    spill: Some(SpillPolicyDto {
                        hysteresis_pct: 101,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                "hysteresis_pct",
            ),
            (
                QueueFlow {
                    key_throttle: Some(RateSpecDto::default()),
                    ..Default::default()
                },
                "key_throttle.period_ms",
            ),
        ];
        for (flow, needle) in cases {
            let e = flow.validate().expect_err(needle);
            assert_eq!(e.code, crate::ErrorCode::InvalidArgument);
            assert!(e.message.contains(needle), "{} vs {needle}", e.message);
        }
        // Unknown placement names are rejected by serde (REST) like unknown enum
        // values by the gRPC conversion.
        assert!(serde_json::from_str::<QueueFlow>(r#"{"placement":"gpu"}"#).is_err());
        assert!(serde_json::from_str::<QueueFlow>(r#"{"on_retries_exhausted":"x"}"#).is_err());
    }

    #[test]
    fn phase7_secrets_are_redacted_from_debug() {
        let c = WorkerTokenCreated {
            token_id: "wkr_1".into(),
            token: "wkr_1.canary-s3cret".into(),
            join_command: "ostro worker --token wkr_1.canary-s3cret".into(),
            expires_at_ms: 0,
        };
        let j = JoinRequest {
            token: "wkr_1.canary-s3cret".into(),
            csr_pem: String::new(),
            worker_id: "w".into(),
            labels: BTreeMap::new(),
        };
        for d in [format!("{c:?}"), format!("{j:?}")] {
            assert!(!d.contains("canary-s3cret"), "{d}");
        }
        // The join error never echoes the token.
        let e = j.validate().expect_err("no csr");
        assert!(!e.message.contains("canary"));
    }

    #[test]
    fn phase7_flow_info_json_is_flat() {
        let info = QueueFlowInfo {
            queue: "q".into(),
            version: 3,
            flow: QueueFlow {
                placement: Some(Placement::Auto),
                paused: true,
                ..Default::default()
            },
        };
        let v = serde_json::to_value(&info).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "queue": "q", "version": 3, "placement": "auto",
                "selector": [], "paused": true
            })
        );
        let body: PutQueueFlowBody =
            serde_json::from_str(r#"{"weight":2,"expected_version":3}"#).unwrap();
        assert_eq!(body.expected_version, Some(3));
        assert_eq!(body.flow.weight, Some(2));
    }
}
