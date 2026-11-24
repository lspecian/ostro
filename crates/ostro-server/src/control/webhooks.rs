//! Webhook controller (08-15, TRIG-01, P8-D12): `ostro_api::phase8::WebhookApi` (meta
//! `Webhook` CRUD, CAS on the record version, secrets referenced by name only) and
//! `HookIngress` (`POST /hooks/{name}`).
//!
//! # Delivery order (fixed)
//!
//! 1. **Lookup** the meta `Webhook` by name: unknown = 404 (one map lookup, nothing else
//!    happens first).
//! 2. **Read the secret**: the sealed meta record named by `secret_ref` (see "Secrets").
//! 3. **Verify** the raw body bytes and headers with
//!    [`ostro_triggers::webhook::verify`] under the verifier from
//!    `VerifierSpec::from_meta(w.verifier, &w.verifier_config, tolerance)`, with `now`
//!    from the injected clock ([`super::ControlCtx::clock`], never the system clock).
//!    `tolerance` is the webhook's own `tolerance_s`; when that is 0, Stripe, Slack and
//!    Standard Webhooks fall back to `[triggers] webhook_tolerance` (default 300 s, see
//!    [`effective_tolerance_s`]). Generic HMAC keeps 0 = no replay window.
//!    Malformed / missing header / bad signature / stale = 401 (`details.reason`), an
//!    invalid spec or unreadable secret = 500. Nothing has parsed the body yet, and
//!    nothing is submitted on any failure.
//! 4. Only then **parse**: the Slack `url_verification` challenge (200 `text/plain`), the
//!    delivery key ([`IdempotencyExtractor`]: the configured one, else the vendor default)
//!    and the action's inputs. Every body reader takes the zero-size `VerifiedDelivery`
//!    proof, so the type system enforces verify-before-parse.
//! 5. **Submit** through the API code path ([`ServerControlPlane`] as `ControlPlane`) and
//!    **await apply**: the 2xx answer is sent after the command applied, never after the
//!    workflow finished.
//!
//! # Dedupe keys
//!
//! | Use | Format |
//! |---|---|
//! | delivery key | the extracted key (<= 256 bytes); with `none` or when the extractor finds nothing: `sha256:<hex of sha256(body)>` |
//! | `start` idempotency key | `__hook/<hook name>/<delivery key>` (RESERVATIONS §11), `ConflictPolicy::ReturnExisting` |
//! | `signal` request id (D13 SignalSeen) | the delivery key when <= 64 bytes, else the 64-hex sha256 of it |
//!
//! A Slack retry (`X-Slack-Retry-Num`) carries the same body, so the same `event_id` key.
//!
//! # Signal targets
//!
//! `run_by.run_id_pointer` reads the run id from the body (the run must belong to the
//! hook's workflow, else 404). `run_by.idempotency_pointer` reads a value `v` and looks up
//! the run of the hook's workflow whose idempotency key is `__hook/<h>/<v>` for every
//! `start` webhook `h` of that workflow (name order), then `v` itself (a run started over
//! the API with that key), through the dry-run start lookup (the dedupe row). Not found =
//! 404.
//!
//! In meta, `run_ref` holds the pointer; an idempotency pointer is stored as
//! `idempotency:<pointer>` (pointers start with `/`, so the two cannot be confused).
//!
//! # Secrets
//!
//! The secret is the **sealed meta `Secret` record** ([`super::SecretsAccess`]):
//! `secret_ref` names the record exactly, or, for a bare name, `_/<name>` (the Phase 6
//! default-tenant form). `plaintext-v0` records (Phase 4 format) open as raw bytes;
//! `aes256gcm:<kid>` records open with the node's [`SealedStore`] from the effects bridge
//! (06-14: the same key ring as every other sealed secret, honouring `OSTRO_SEALING_KEY`,
//! `[secrets] sealing_key_file` and the dev auto key). A node started without a sealing key
//! falls back to discovering one lazily (`OSTRO_SEALING_KEY`, else `<data_dir>/sealing.key`
//! once it exists). The bridge's `SecretResolver` itself is not used: its provider chain
//! resolves `<tenant|_>/<name>` per tenant and adds env/file/vault lookups and the 8-byte
//! template minimum, none of which apply to an exactly named webhook verifier secret. The value lives in a zeroize-on-drop [`SecretValue`] for one verification and
//! is never logged, returned or put in an error message.
//!
//! # Status codes
//!
//! | Case | Status |
//! |---|---|
//! | applied (new run / signal / completion) | 200 `{..., "duplicate": false}` |
//! | duplicate delivery (existing run, SignalSeen, token already completed) | 200 `{..., "duplicate": true}` |
//! | signal redelivered after the run finished, its signal applied before (SignalSeen) | 200 `{..., "duplicate": true}` |
//! | signal to a finished run that never took this delivery's signal | 409 `run_terminal` |
//! | Slack `url_verification` | 200 `text/plain` challenge |
//! | unknown hook | 404 |
//! | bad / missing / malformed signature, stale timestamp | 401 (`details.reason`) |
//! | body not JSON where needed, pointer missing, key too long | 400 |
//! | signal target run not found | 404 |
//! | verifier misconfigured, secret missing or unopenable | 500 |
//! | body over `[triggers] webhook_body_limit` | 413 (router) |
//! | partition unavailable | 503 |
//!
//! Metric: `ostro_webhook_requests_total{hook, outcome}` with outcome `ok`, `duplicate`,
//! `unauthorized`, `not_found`, `error`. `hook` is the meta-bounded webhook name (an
//! unknown name is recorded as `hook=""`, so a scanner cannot grow the label set).

use std::path::Path;
use std::sync::{Arc, OnceLock};

use ostro_api::domain::{
    CompleteTokenRequest, ConflictPolicy, ControlPlane, SignalRequest, StartRunRequest,
};
use ostro_api::phase8::{
    HookIngress, HookRequest, HookResponse, RunByDto, VerifierDto, VerifierKindDto,
    WebhookActionDto, WebhookApi, WebhookDto, validate_traceparent, version_mismatch,
};
use ostro_api::{ApiError, ErrorCode};
use ostro_bridge::secrets::{SealedStore, SealingKeyRing, SecretValue, SecretsConfig};
use ostro_meta::records::{
    SealedSecret, TOKEN_KEY_PLAINTEXT_ID, WEBHOOK_ACTION_COMPLETE_TOKEN, WEBHOOK_ACTION_SIGNAL,
    WEBHOOK_ACTION_START, Webhook,
};
use ostro_meta::{MetaCommand, MetaError, MetaKey, MetaRecord, MetaResult};
use ostro_partition::store::Table;
use ostro_partition::store::keys::signal_seen_key;
use ostro_triggers::extract::{
    IdempotencyExtractor, InputSelector, default_for, json_pointer_string,
};
use ostro_triggers::webhook::{
    GenericHmacSpec, HookError, VerifiedDelivery, VerifierKind, VerifierSpec, slack_challenge,
    verify,
};
use ostro_types::command::MAX_REQUEST_ID_LEN;
use sha2::{Digest, Sha256};

use super::{ControlCtx, names, policy};

/// Idempotency-key namespace of webhook deliveries (RESERVATIONS §11).
pub const HOOK_KEY_PREFIX: &str = "__hook/";
/// `run_ref` prefix marking a signal target found by idempotency pointer.
pub const RUN_REF_IDEMPOTENCY: &str = "idempotency:";
/// Prefix of a delivery key derived from the body hash.
pub const BODY_HASH_KEY_PREFIX: &str = "sha256:";
/// The metric counting deliveries.
pub const REQUESTS_METRIC: &str = "ostro_webhook_requests_total";
/// The Phase 6 default-tenant prefix of sealed secret names.
const DEFAULT_TENANT_SECRET_PREFIX: &str = "_/";

/// Delivery outcome label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Duplicate,
    Unauthorized,
    NotFound,
    Error,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Duplicate => "duplicate",
            Outcome::Unauthorized => "unauthorized",
            Outcome::NotFound => "not_found",
            Outcome::Error => "error",
        }
    }
}

/// `WebhookApi` + `HookIngress` for the server.
pub struct WebhookController {
    ctx: ControlCtx,
    /// Fallback key ring when the bridge has none, loaded on first need (only `aes256gcm`
    /// records need it).
    ring: OnceLock<Arc<SealingKeyRing>>,
}

impl std::fmt::Debug for WebhookController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookController")
            .field("ctx", &self.ctx)
            .finish_non_exhaustive()
    }
}

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

fn record_outcome(hook: &str, o: Outcome) {
    metrics::counter!(REQUESTS_METRIC, "hook" => hook.to_owned(), "outcome" => o.as_str())
        .increment(1);
}

/// 401 with `details.reason` (no header value, no secret).
fn unauthorized(e: HookError) -> HookResponse {
    let reason = match e {
        HookError::Malformed => "malformed",
        HookError::MissingHeader(_) => "missing_header",
        HookError::Stale => "stale",
        HookError::BadSignature => "bad_signature",
        HookError::Config => "config",
    };
    HookResponse::json(
        401,
        &serde_json::json!({
            "error": {
                "code": "unauthenticated",
                "message": e.to_string(),
                "details": { "reason": reason },
            }
        }),
    )
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The start idempotency key of delivery `key` on hook `hook`.
pub fn hook_idempotency_key(hook: &str, key: &str) -> String {
    format!("{HOOK_KEY_PREFIX}{hook}/{key}")
}

/// The signal `request_id` of delivery `key` (<= [`MAX_REQUEST_ID_LEN`] bytes).
pub fn signal_request_id(key: &str) -> String {
    if key.len() <= MAX_REQUEST_ID_LEN {
        key.to_owned()
    } else {
        hex(&Sha256::digest(key.as_bytes()))
    }
}

/// The delivery key derived from the body (`none` extractor / no key found).
pub fn body_hash_key(body: &[u8]) -> String {
    format!("{BODY_HASH_KEY_PREFIX}{}", hex(&Sha256::digest(body)))
}

// ---------------------------------------------------------------------------------
// DTO <-> meta
// ---------------------------------------------------------------------------------

fn kind_to_meta(k: VerifierKindDto) -> VerifierKind {
    match k {
        VerifierKindDto::None => VerifierKind::None,
        VerifierKindDto::Stripe => VerifierKind::Stripe,
        VerifierKindDto::Github => VerifierKind::GitHub,
        VerifierKindDto::Slack => VerifierKind::Slack,
        VerifierKindDto::Standard => VerifierKind::StandardWebhooks,
        VerifierKindDto::Hmac => VerifierKind::GenericHmac,
    }
}

fn kind_from_meta(k: VerifierKind) -> VerifierKindDto {
    match k {
        VerifierKind::None => VerifierKindDto::None,
        VerifierKind::Stripe => VerifierKindDto::Stripe,
        VerifierKind::GitHub => VerifierKindDto::Github,
        VerifierKind::Slack => VerifierKindDto::Slack,
        VerifierKind::StandardWebhooks => VerifierKindDto::Standard,
        VerifierKind::GenericHmac => VerifierKindDto::Hmac,
    }
}

fn invalid_verifier(why: &str) -> ApiError {
    ApiError::invalid_argument(format!("invalid verifier: {why}"))
}

/// The verifier DTO as `(verifier, verifier_config, tolerance_s)`, validated by a
/// `VerifierSpec::from_meta` round trip.
fn verifier_to_meta(v: &VerifierDto) -> Result<(u8, Vec<u8>, u32), ApiError> {
    let kind = kind_to_meta(v.kind);
    let hmac_fields = [
        ("header", &v.header),
        ("prefix", &v.prefix),
        ("algorithm", &v.algorithm),
        ("encoding", &v.encoding),
        ("timestamp_header", &v.timestamp_header),
        ("template", &v.template),
    ];
    let config = if kind == VerifierKind::GenericHmac {
        let mut m = serde_json::Map::new();
        for (k, val) in hmac_fields {
            if let Some(s) = val {
                m.insert(k.to_owned(), serde_json::Value::String(s.clone()));
            }
        }
        if !m.contains_key("header") {
            return Err(invalid_verifier("hmac needs verifier.header"));
        }
        let raw = serde_json::to_vec(&serde_json::Value::Object(m))
            .map_err(|e| ApiError::internal(format!("verifier config: {e}")))?;
        GenericHmacSpec::parse_config(&raw, v.tolerance_s.unwrap_or(0))
            .map_err(|_| {
                invalid_verifier(
                    "hmac options (algorithm sha256|legacy_sha1, encoding hex|base64, template \
                     {body} without / {ts}.{body} with timestamp_header, tolerance needs a \
                     timestamp_header)",
                )
            })?
            .to_config()
    } else {
        if let Some((k, _)) = hmac_fields.iter().find(|(_, val)| val.is_some()) {
            return Err(invalid_verifier(&format!(
                "verifier.{k} applies to kind hmac only"
            )));
        }
        Vec::new()
    };
    let tol = v.tolerance_s.unwrap_or(0);
    VerifierSpec::from_meta(kind.as_u8(), &config, tol).map_err(|_| {
        invalid_verifier("tolerance_s must be unset for github and none, and every option valid")
    })?;
    Ok((kind.as_u8(), config, tol))
}

fn verifier_from_meta(w: &Webhook) -> Result<VerifierDto, ApiError> {
    let kind = VerifierKind::from_u8(w.verifier)
        .ok_or_else(|| ApiError::internal(format!("stored webhook has verifier {}", w.verifier)))?;
    let mut dto = VerifierDto {
        kind: kind_from_meta(kind),
        secret_ref: w.secret_ref.clone(),
        tolerance_s: (w.tolerance_s > 0).then_some(w.tolerance_s),
        ..VerifierDto::default()
    };
    if kind == VerifierKind::GenericHmac {
        let v: serde_json::Value = serde_json::from_slice(&w.verifier_config)
            .map_err(|_| ApiError::internal("stored webhook has an invalid verifier_config"))?;
        let get = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_owned);
        dto.header = get("header");
        dto.prefix = get("prefix");
        dto.algorithm = get("algorithm");
        dto.encoding = get("encoding");
        dto.timestamp_header = get("timestamp_header");
        dto.template = get("template");
    }
    Ok(dto)
}

/// Meta record -> DTO (never any secret material: `secret_ref` is a name).
pub fn webhook_to_dto(w: &Webhook, version: u64) -> Result<WebhookDto, ApiError> {
    let action = match w.action {
        WEBHOOK_ACTION_START => WebhookActionDto::Start {
            workflow: w.workflow.clone(),
            version: w.version,
            input: if w.input_ref.is_empty() {
                "body".to_owned()
            } else {
                w.input_ref.clone()
            },
        },
        WEBHOOK_ACTION_SIGNAL => WebhookActionDto::Signal {
            workflow: w.workflow.clone(),
            run_by: match w.run_ref.strip_prefix(RUN_REF_IDEMPOTENCY) {
                Some(p) => RunByDto::IdempotencyPointer(p.to_owned()),
                None => RunByDto::RunIdPointer(w.run_ref.clone()),
            },
            name: w.signal_name.clone(),
        },
        WEBHOOK_ACTION_COMPLETE_TOKEN => WebhookActionDto::CompleteToken {
            token_pointer: w.run_ref.clone(),
        },
        other => {
            return Err(ApiError::internal(format!(
                "stored webhook has action {other}"
            )));
        }
    };
    Ok(WebhookDto {
        name: w.name.clone(),
        action,
        verifier: verifier_from_meta(w)?,
        idempotency: (!w.idempotency.is_empty()).then(|| w.idempotency.clone()),
        version_tag: Some(version),
    })
}

/// DTO -> meta record (syntax only; existence checks are the controller's).
pub fn webhook_from_dto(d: &WebhookDto) -> Result<Webhook, ApiError> {
    names::validate_name("name", &d.name)?;
    let mut w = Webhook {
        name: d.name.clone(),
        ..Webhook::default()
    };
    match &d.action {
        WebhookActionDto::Start {
            workflow,
            version,
            input,
        } => {
            w.action = WEBHOOK_ACTION_START;
            w.workflow = workflow.clone();
            w.version = *version;
            w.input_ref = if input == "body" {
                String::new()
            } else {
                input.clone()
            };
            InputSelector::from_meta(&w.input_ref).map_err(|_| {
                ApiError::invalid_argument("action.input: \"body\" or a JSON pointer")
            })?;
        }
        WebhookActionDto::Signal {
            workflow,
            run_by,
            name,
        } => {
            w.action = WEBHOOK_ACTION_SIGNAL;
            w.workflow = workflow.clone();
            w.signal_name = name.clone();
            if name.len() > ostro_types::command::MAX_SIGNAL_NAME_LEN {
                return Err(ApiError::invalid_argument("action.name is too long"));
            }
            w.run_ref = match run_by {
                RunByDto::RunIdPointer(p) => p.clone(),
                RunByDto::IdempotencyPointer(p) => format!("{RUN_REF_IDEMPOTENCY}{p}"),
            };
        }
        WebhookActionDto::CompleteToken { token_pointer } => {
            w.action = WEBHOOK_ACTION_COMPLETE_TOKEN;
            w.run_ref = token_pointer.clone();
        }
    }
    let (verifier, config, tol) = verifier_to_meta(&d.verifier)?;
    w.verifier = verifier;
    w.verifier_config = config;
    w.tolerance_s = tol;
    w.secret_ref = if verifier == VerifierKind::None.as_u8() {
        String::new()
    } else {
        d.verifier.secret_ref.clone()
    };
    if let Some(i) = &d.idempotency {
        IdempotencyExtractor::parse(i)
            .map_err(|e| ApiError::invalid_argument(format!("idempotency {i:?}: {e}")))?;
        w.idempotency = i.clone();
    }
    Ok(w)
}

// ---------------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------------

/// Error of [`WebhookController::open_secret`]; never carries secret bytes.
#[derive(Debug)]
enum SecretError {
    Missing,
    Unopenable,
}

impl WebhookController {
    /// Over the shared controller context.
    pub fn new(ctx: ControlCtx) -> Self {
        Self {
            ctx,
            ring: OnceLock::new(),
        }
    }

    /// The shared controller context.
    pub fn ctx(&self) -> &ControlCtx {
        &self.ctx
    }

    /// Phase 13 (13-02): the routed control plane (deliveries start / signal / complete on
    /// the partition's leader).
    fn cp(&self) -> &dyn ControlPlane {
        self.ctx.routed.as_ref()
    }

    fn stored(&self, name: &str) -> Option<(u64, Webhook)> {
        self.ctx
            .meta
            .view()
            .read(|s| s.webhook(name).map(|v| (v.version, v.value.clone())))
    }

    /// The sealed record a `secret_ref` names: exact, else `_/<ref>` for a bare name.
    fn sealed(&self, secret_ref: &str) -> Option<SealedSecret> {
        if secret_ref.is_empty() {
            return None;
        }
        self.ctx.secrets.sealed(secret_ref).or_else(|| {
            if secret_ref.contains('/') {
                None
            } else {
                self.ctx
                    .secrets
                    .sealed(&format!("{DEFAULT_TENANT_SECRET_PREFIX}{secret_ref}"))
            }
        })
    }

    /// The node sealing key ring: the effects bridge's (06-14), else (no key at start)
    /// `OSTRO_SEALING_KEY` / `<data_dir>/sealing.key`, discovered lazily.
    fn ring(&self) -> Option<Arc<SealingKeyRing>> {
        if let Some(s) = self.ctx.partitions.bridge().sealed_store() {
            return Some(Arc::clone(s.ring()));
        }
        if let Some(r) = self.ring.get() {
            return Some(Arc::clone(r));
        }
        let ring = load_ring(&self.ctx.data_dir)?;
        Some(Arc::clone(self.ring.get_or_init(|| Arc::new(ring))))
    }

    fn open_secret(&self, secret_ref: &str) -> Result<SecretValue, SecretError> {
        let rec = self.sealed(secret_ref).ok_or(SecretError::Missing)?;
        if rec.key_id == TOKEN_KEY_PLAINTEXT_ID {
            return Ok(SecretValue::from_slice(&rec.ciphertext));
        }
        let ring = self.ring().ok_or(SecretError::Unopenable)?;
        SealedStore::new(ring)
            .open(&rec)
            .map_err(|_| SecretError::Unopenable)
    }

    /// Create/update checks beyond syntax: the secret exists, the workflow exists.
    fn check_refs(&self, w: &Webhook) -> Result<(), ApiError> {
        if w.verifier != VerifierKind::None.as_u8() && self.sealed(&w.secret_ref).is_none() {
            return Err(ApiError::invalid_argument(format!(
                "verifier.secret_ref {:?} names no sealed secret",
                w.secret_ref
            )));
        }
        if w.action != WEBHOOK_ACTION_COMPLETE_TOKEN {
            let key = policy::policy_key("workflow", &w.workflow)
                .map_err(|e| ApiError::invalid_argument(e.message))?;
            let exists = self
                .ctx
                .meta
                .view()
                .read(|s| s.workflows().any(|v| v.value.name == key));
            if !exists {
                return Err(ApiError::invalid_argument(format!(
                    "action.workflow {:?} does not exist",
                    w.workflow
                )));
            }
        }
        Ok(())
    }

    async fn put(&self, w: Webhook, expected: Option<u64>) -> Result<WebhookDto, ApiError> {
        let name = w.name.clone();
        let res = self
            .ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Webhook(name.clone()),
                expected_version: expected,
                record: MetaRecord::Webhook(w.clone()),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { version } => webhook_to_dto(&w, version),
            MetaResult::Conflict { .. } => {
                Err(ApiError::conflict(format!("webhook {name} already exists")))
            }
            MetaResult::StaleVersion { current } => {
                Err(version_mismatch("webhook", &name, current))
            }
            MetaResult::NotFound => Err(ApiError::not_found(format!("webhook {name} not found"))),
            MetaResult::Invalid(m) => Err(ApiError::invalid_argument(format!(
                "metadata rejected the webhook: {m}"
            ))),
        }
    }

    // -----------------------------------------------------------------------------
    // Delivery
    // -----------------------------------------------------------------------------

    async fn deliver_inner(&self, req: &HookRequest) -> (Outcome, Result<HookResponse, ApiError>) {
        // 1. Lookup.
        let Some((_, w)) = self.stored(&req.name) else {
            return (
                Outcome::NotFound,
                Err(ApiError::not_found(format!(
                    "webhook {} not found",
                    req.name
                ))),
            );
        };
        let error = |e: ApiError| (Outcome::Error, Err(e));
        let tolerance_s = effective_tolerance_s(
            w.verifier,
            w.tolerance_s,
            self.ctx.config.triggers.webhook_tolerance.get(),
        );
        let spec = match VerifierSpec::from_meta(w.verifier, &w.verifier_config, tolerance_s) {
            Ok(s) => s,
            Err(_) => {
                tracing::warn!(hook = %w.name, "webhook verifier configuration is invalid");
                return error(ApiError::internal("webhook verifier is misconfigured"));
            }
        };
        let headers: Vec<(String, Vec<u8>)> = req
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_vec()))
            .collect();
        // 2. Secret, 3. verify (raw bytes, injected clock).
        let now_unix = i64::try_from(self.ctx.now_ms() / 1000).unwrap_or(i64::MAX);
        let proof = if matches!(spec, VerifierSpec::None) {
            verify(&spec, &[], &headers, &req.body, now_unix)
        } else {
            let secret = match self.open_secret(&w.secret_ref) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(hook = %w.name, err = ?e, "webhook secret unavailable");
                    return error(ApiError::internal("webhook secret is unavailable"));
                }
            };
            let r = verify(&spec, secret.expose(), &headers, &req.body, now_unix);
            drop(secret);
            r
        };
        let proof = match proof {
            Ok(p) => p,
            Err(HookError::Config) => {
                return error(ApiError::internal("webhook verifier is misconfigured"));
            }
            Err(e) => return (Outcome::Unauthorized, Ok(unauthorized(e))),
        };
        // 4. Parse (only now).
        if spec.kind() == VerifierKind::Slack
            && let Some(challenge) = slack_challenge(&proof, &req.body)
        {
            return (Outcome::Ok, Ok(HookResponse::text(200, challenge)));
        }
        let key = match delivery_key(&proof, &w, spec.kind(), &headers, &req.body) {
            Ok(k) => k,
            Err(e) => return error(e),
        };
        // 5. Act, answer after apply.
        let r = match w.action {
            WEBHOOK_ACTION_START => self.act_start(&proof, &w, &key, req).await,
            WEBHOOK_ACTION_SIGNAL => self.act_signal(&proof, &w, &key, &req.body).await,
            WEBHOOK_ACTION_COMPLETE_TOKEN => self.act_complete(&proof, &w, &req.body).await,
            other => Err(ApiError::internal(format!(
                "stored webhook has action {other}"
            ))),
        };
        match r {
            Ok((dup, body)) => (
                if dup { Outcome::Duplicate } else { Outcome::Ok },
                Ok(HookResponse::json(200, &body)),
            ),
            Err(e) if e.code == ErrorCode::NotFound => (Outcome::NotFound, Err(e)),
            Err(e) => error(e),
        }
    }

    async fn act_start(
        &self,
        proof: &VerifiedDelivery,
        w: &Webhook,
        key: &str,
        req: &HookRequest,
    ) -> Result<(bool, serde_json::Value), ApiError> {
        let sel = InputSelector::from_meta(&w.input_ref)
            .map_err(|_| ApiError::internal("stored webhook has an invalid input_ref"))?;
        let input = sel
            .select(proof, &req.body)
            .map_err(|e| ApiError::invalid_argument(format!("webhook input: {e}")))?;
        // Providers may send non-traceparent junk: an invalid header is ignored.
        let traceparent = req
            .headers
            .get(ostro_api::phase8::TRACEPARENT_HEADER)
            .and_then(|v| v.to_str().ok())
            .filter(|t| validate_traceparent(t).is_ok())
            .map(str::to_owned);
        let r = self
            .cp()
            .start_run(StartRunRequest {
                workflow: w.workflow.clone(),
                input_json: input,
                version: w.version,
                queue: None,
                delay_ms: None,
                priority: None,
                tags: Vec::new(),
                traceparent,
                tenant: None,
                idempotency_key: Some(hook_idempotency_key(&w.name, key)),
                conflict_policy: ConflictPolicy::ReturnExisting,
                concurrency_key: None,
                schedule_to_start_ms: None,
                retention_ms: None,
                dry_run: false,
            })
            .await?;
        Ok((
            !r.created,
            serde_json::json!({
                "run_id": r.run_id,
                "created": r.created,
                "duplicate": !r.created,
            }),
        ))
    }

    /// The run a signal hook targets.
    async fn signal_target(
        &self,
        proof: &VerifiedDelivery,
        w: &Webhook,
        body: &[u8],
    ) -> Result<String, ApiError> {
        let not_found = || ApiError::not_found("no run matches this delivery");
        let pointer_value = |p: &str| -> Result<String, ApiError> {
            json_pointer_string(proof, body, p)
                .map_err(|e| ApiError::invalid_argument(format!("webhook run reference: {e}")))?
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    ApiError::invalid_argument(format!("webhook body has no value at {p}"))
                })
        };
        let want = policy::policy_key("workflow", &w.workflow).ok();
        if let Some(p) = w.run_ref.strip_prefix(RUN_REF_IDEMPOTENCY) {
            let v = pointer_value(p)?;
            let mut keys: Vec<String> = self.ctx.meta.view().read(|s| {
                s.webhooks()
                    .filter(|h| {
                        h.value.action == WEBHOOK_ACTION_START && h.value.workflow == w.workflow
                    })
                    .map(|h| hook_idempotency_key(&h.value.name, &v))
                    .collect()
            });
            keys.push(v);
            for k in keys {
                let r = self
                    .cp()
                    .start_run(StartRunRequest {
                        workflow: w.workflow.clone(),
                        input_json: b"null".to_vec(),
                        version: None,
                        queue: None,
                        delay_ms: None,
                        priority: None,
                        tags: Vec::new(),
                        traceparent: None,
                        tenant: None,
                        idempotency_key: Some(k),
                        conflict_policy: ConflictPolicy::ReturnExisting,
                        concurrency_key: None,
                        schedule_to_start_ms: None,
                        retention_ms: None,
                        dry_run: true,
                    })
                    .await;
                match r {
                    Ok(r) if !r.run_id.is_empty() => return Ok(r.run_id),
                    Ok(_) => {}
                    Err(e) if e.code == ErrorCode::NotFound => return Err(not_found()),
                    Err(e) => return Err(e),
                }
            }
            return Err(not_found());
        }
        let run_id = pointer_value(&w.run_ref)?;
        let info = match self.cp().get_run(run_id.clone()).await {
            Ok(i) => i,
            Err(e) if matches!(e.code, ErrorCode::NotFound | ErrorCode::InvalidArgument) => {
                return Err(not_found());
            }
            Err(e) => return Err(e),
        };
        let got = policy::policy_key("workflow", &info.workflow).ok();
        if want.is_none() || want != got {
            return Err(not_found());
        }
        Ok(run_id)
    }

    async fn act_signal(
        &self,
        proof: &VerifiedDelivery,
        w: &Webhook,
        key: &str,
        body: &[u8],
    ) -> Result<(bool, serde_json::Value), ApiError> {
        let payload = body_json(proof, body)?;
        let run_id = self.signal_target(proof, w, body).await?;
        let request_id = signal_request_id(key);
        let r = self
            .cp()
            .signal_run(SignalRequest {
                run_id: run_id.clone(),
                name: w.signal_name.clone(),
                payload,
                request_id: Some(request_id.clone()),
                dry_run: false,
            })
            .await;
        let r = match r {
            Ok(r) => r,
            Err(e) if e.code == ErrorCode::NotFound => {
                return Err(ApiError::not_found("no run matches this delivery"));
            }
            // A redelivery after the run finished: if this delivery's signal applied before
            // the run ended (its SignalSeen row exists), it is a duplicate; otherwise the
            // signal can never apply and the 409 `run_terminal` stands.
            Err(e) if e.code == ErrorCode::Conflict && is_run_terminal(&e) => {
                if self.signal_seen(&run_id, &request_id).await? {
                    return Ok((
                        true,
                        serde_json::json!({
                            "run_id": run_id,
                            "accepted": true,
                            "duplicate": true,
                        }),
                    ));
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        Ok((
            r.duplicate,
            serde_json::json!({
                "run_id": run_id,
                "accepted": r.accepted,
                "duplicate": r.duplicate,
            }),
        ))
    }

    /// Whether signal `request_id` was applied to run `run_id` (its D13 SignalSeen row in
    /// keyspace 15 exists). The row lives as long as the run record (retention deletes both),
    /// and a terminal run takes no more signals, so for a terminal run the answer is final.
    async fn signal_seen(&self, run_id: &str, request_id: &str) -> Result<bool, ApiError> {
        let cp = &self.ctx.partitions;
        let run = super::runs::parse_run_id(cp, run_id)?;
        let reader = cp.partition(run.partition())?.reader();
        let key = signal_seen_key(&run, request_id.as_bytes());
        let row = tokio::task::spawn_blocking(move || reader.get(Table::SignalSeen, &key))
            .await
            .map_err(super::runs::task_error)?
            .map_err(super::runs::store_error)?;
        Ok(row.is_some())
    }

    async fn act_complete(
        &self,
        proof: &VerifiedDelivery,
        w: &Webhook,
        body: &[u8],
    ) -> Result<(bool, serde_json::Value), ApiError> {
        let payload = body_json(proof, body)?;
        let token = json_pointer_string(proof, body, &w.run_ref)
            .map_err(|e| ApiError::invalid_argument(format!("webhook token: {e}")))?
            .ok_or_else(|| {
                ApiError::invalid_argument(format!("webhook body has no token at {}", w.run_ref))
            })?;
        let r = self
            .cp()
            .complete_token(CompleteTokenRequest {
                token,
                payload,
                dry_run: false,
            })
            .await;
        match r {
            Ok(r) => Ok((
                false,
                serde_json::json!({
                    "run_id": r.run_id,
                    "accepted": r.accepted,
                    "duplicate": false,
                }),
            )),
            // A redelivery of a completion that already applied (the token is spent, or
            // its run has finished since): nothing more can happen, so the provider is
            // told to stop retrying.
            Err(e) if e.code == ErrorCode::Conflict && is_spent(&e) => Ok((
                true,
                serde_json::json!({ "accepted": false, "duplicate": true }),
            )),
            Err(e) => Err(e),
        }
    }
}

/// A conflict whose `details.reason` is `run_terminal`.
fn is_run_terminal(e: &ApiError) -> bool {
    e.details
        .as_ref()
        .and_then(|d| d.get("reason"))
        .and_then(|r| r.as_str())
        == Some(ostro_api::error::reason::RUN_TERMINAL)
}

/// A completion conflict meaning "already done": `token_already_completed` or
/// `run_terminal`.
fn is_spent(e: &ApiError) -> bool {
    let reason = e
        .details
        .as_ref()
        .and_then(|d| d.get("reason"))
        .and_then(|r| r.as_str());
    matches!(
        reason,
        Some(ostro_api::error::reason::TOKEN_ALREADY_COMPLETED)
            | Some(ostro_api::error::reason::RUN_TERMINAL)
    )
}

/// The (verified) body as a JSON value: 400 when it is not JSON.
fn body_json(_proof: &VerifiedDelivery, body: &[u8]) -> Result<serde_json::Value, ApiError> {
    serde_json::from_slice(body)
        .map_err(|_| ApiError::invalid_argument("webhook body is not valid JSON"))
}

/// The delivery key: the configured extractor (else the vendor default); the body hash
/// when the extractor is `none` or finds nothing.
fn delivery_key(
    proof: &VerifiedDelivery,
    w: &Webhook,
    kind: VerifierKind,
    headers: &[(String, Vec<u8>)],
    body: &[u8],
) -> Result<String, ApiError> {
    let ex = if w.idempotency.is_empty() {
        default_for(kind)
    } else {
        IdempotencyExtractor::parse(&w.idempotency).map_err(|_| {
            ApiError::internal("stored webhook has an invalid idempotency extractor")
        })?
    };
    let key = ex
        .extract(proof, headers, body)
        .map_err(|e| ApiError::invalid_argument(format!("webhook idempotency key: {e}")))?;
    Ok(key.unwrap_or_else(|| body_hash_key(body)))
}

/// The node sealing key ring: `OSTRO_SEALING_KEY`, else `<data_dir>/sealing.key` if it
/// exists (never created here). `None` when neither is available or the key is invalid.
fn load_ring(data_dir: &Path) -> Option<SealingKeyRing> {
    let dev_key = data_dir.join(ostro_bridge::secrets::sealed::DEV_KEY_FILE);
    let cfg = SecretsConfig {
        sealing_key_file: dev_key.is_file().then_some(dev_key),
        ..SecretsConfig::default()
    };
    match SealingKeyRing::load(&cfg, data_dir, false) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(err = %e, "webhook secrets: sealing key unavailable");
            None
        }
    }
}

#[async_trait::async_trait]
impl WebhookApi for WebhookController {
    async fn list_webhooks(&self) -> Result<Vec<WebhookDto>, ApiError> {
        let all: Vec<(u64, Webhook)> = self
            .ctx
            .meta
            .view()
            .read(|s| s.webhooks().map(|v| (v.version, v.value.clone())).collect());
        all.iter().map(|(v, w)| webhook_to_dto(w, *v)).collect()
    }

    async fn get_webhook(&self, name: String) -> Result<WebhookDto, ApiError> {
        let (v, w) = self
            .stored(&name)
            .ok_or_else(|| ApiError::not_found(format!("webhook {name} not found")))?;
        webhook_to_dto(&w, v)
    }

    async fn create_webhook(&self, d: WebhookDto) -> Result<WebhookDto, ApiError> {
        let w = webhook_from_dto(&d)?;
        self.check_refs(&w)?;
        self.put(w, None).await
    }

    async fn update_webhook(&self, d: WebhookDto) -> Result<WebhookDto, ApiError> {
        let w = webhook_from_dto(&d)?;
        let (current, _) = self
            .stored(&w.name)
            .ok_or_else(|| ApiError::not_found(format!("webhook {} not found", w.name)))?;
        if let Some(t) = d.version_tag
            && t != current
        {
            return Err(version_mismatch("webhook", &w.name, current));
        }
        self.check_refs(&w)?;
        self.put(w, Some(d.version_tag.unwrap_or(current))).await
    }

    async fn delete_webhook(&self, name: String, version_tag: Option<u64>) -> Result<(), ApiError> {
        let (current, _) = self
            .stored(&name)
            .ok_or_else(|| ApiError::not_found(format!("webhook {name} not found")))?;
        if let Some(t) = version_tag
            && t != current
        {
            return Err(version_mismatch("webhook", &name, current));
        }
        let res = self
            .ctx
            .meta
            .propose(MetaCommand::Delete {
                key: MetaKey::Webhook(name.clone()),
                expected_version: version_tag.unwrap_or(current),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => Ok(()),
            MetaResult::NotFound => Err(ApiError::not_found(format!("webhook {name} not found"))),
            MetaResult::StaleVersion { current } | MetaResult::Conflict { current } => {
                Err(version_mismatch("webhook", &name, current))
            }
            MetaResult::Invalid(m) => Err(ApiError::invalid_argument(m)),
        }
    }
}

#[async_trait::async_trait]
impl HookIngress for WebhookController {
    async fn deliver(&self, req: HookRequest) -> Result<HookResponse, ApiError> {
        let (outcome, r) = self.deliver_inner(&req).await;
        let label = if outcome == Outcome::NotFound && self.stored(&req.name).is_none() {
            ""
        } else {
            req.name.as_str()
        };
        record_outcome(label, outcome);
        r
    }
}

/// The replay window used at delivery: the webhook's own `tolerance_s`, or, when that is 0,
/// `[triggers] webhook_tolerance` for the kinds whose signature always carries a timestamp
/// (Stripe, Slack, Standard Webhooks). Generic HMAC (0 = no window; its template may not
/// sign a timestamp), GitHub and None (no timestamp) keep their stored value.
fn effective_tolerance_s(verifier: u8, tolerance_s: u32, fallback: std::time::Duration) -> u32 {
    let timestamped = matches!(
        VerifierKind::from_u8(verifier),
        Some(VerifierKind::Stripe | VerifierKind::Slack | VerifierKind::StandardWebhooks)
    );
    if tolerance_s > 0 || !timestamped {
        return tolerance_s;
    }
    u32::try_from(fallback.as_secs()).unwrap_or(u32::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_meta::records::{
        VERIFIER_GENERIC_HMAC, VERIFIER_GITHUB, VERIFIER_NONE, VERIFIER_SLACK,
        VERIFIER_STANDARD_WEBHOOKS, VERIFIER_STRIPE,
    };

    fn dto(action: WebhookActionDto, verifier: VerifierDto) -> WebhookDto {
        WebhookDto {
            name: "h1".into(),
            action,
            verifier,
            idempotency: None,
            version_tag: None,
        }
    }

    fn start() -> WebhookActionDto {
        WebhookActionDto::Start {
            workflow: "wf".into(),
            version: Some(2),
            input: "/data".into(),
        }
    }

    fn v(kind: VerifierKindDto) -> VerifierDto {
        VerifierDto {
            kind,
            secret_ref: "s".into(),
            ..VerifierDto::default()
        }
    }

    #[test]
    fn webhook_tolerance_is_the_fallback_for_timestamped_kinds() {
        let cfg = std::time::Duration::from_secs(45);
        for k in [VERIFIER_STRIPE, VERIFIER_SLACK, VERIFIER_STANDARD_WEBHOOKS] {
            // Own value wins; 0 falls back to [triggers] webhook_tolerance.
            assert_eq!(effective_tolerance_s(k, 120, cfg), 120);
            assert_eq!(effective_tolerance_s(k, 0, cfg), 45);
        }
        assert_eq!(
            VerifierSpec::from_meta(
                VERIFIER_STRIPE,
                b"",
                effective_tolerance_s(VERIFIER_STRIPE, 0, cfg)
            ),
            Ok(VerifierSpec::Stripe { tolerance_s: 45 })
        );
        // Generic HMAC (0 = no window), GitHub and None (no timestamp) are unchanged.
        for k in [VERIFIER_GENERIC_HMAC, VERIFIER_GITHUB, VERIFIER_NONE] {
            assert_eq!(effective_tolerance_s(k, 0, cfg), 0);
        }
        assert_eq!(effective_tolerance_s(VERIFIER_GENERIC_HMAC, 60, cfg), 60);
        // A sub-second setting still yields a window (never 0 = "verifier default").
        assert_eq!(
            effective_tolerance_s(VERIFIER_SLACK, 0, std::time::Duration::from_millis(10)),
            1
        );
    }

    #[test]
    fn verifier_kinds_match_meta_ordinals() {
        for (k, ord) in [
            (VerifierKindDto::None, VERIFIER_NONE),
            (VerifierKindDto::Stripe, VERIFIER_STRIPE),
            (VerifierKindDto::Github, VERIFIER_GITHUB),
            (VerifierKindDto::Slack, VERIFIER_SLACK),
            (VerifierKindDto::Hmac, VERIFIER_GENERIC_HMAC),
            (VerifierKindDto::Standard, VERIFIER_STANDARD_WEBHOOKS),
        ] {
            assert_eq!(kind_to_meta(k).as_u8(), ord);
            assert_eq!(kind_from_meta(kind_to_meta(k)), k);
        }
    }

    #[test]
    fn dto_meta_round_trip_every_action() {
        let mut hmac = v(VerifierKindDto::Hmac);
        hmac.header = Some("x-sig".into());
        hmac.prefix = Some("sha256=".into());
        hmac.algorithm = Some("sha256".into());
        hmac.encoding = Some("base64".into());
        hmac.timestamp_header = Some("x-ts".into());
        hmac.template = Some("{ts}.{body}".into());
        hmac.tolerance_s = Some(60);
        let mut stripe = v(VerifierKindDto::Stripe);
        stripe.tolerance_s = Some(120);
        for d in [
            dto(start(), stripe),
            dto(
                WebhookActionDto::Signal {
                    workflow: "wf".into(),
                    run_by: RunByDto::IdempotencyPointer("/data/object/id".into()),
                    name: "paid".into(),
                },
                v(VerifierKindDto::Github),
            ),
            dto(
                WebhookActionDto::Signal {
                    workflow: "wf".into(),
                    run_by: RunByDto::RunIdPointer("/run".into()),
                    name: "x".into(),
                },
                v(VerifierKindDto::Slack),
            ),
            dto(
                WebhookActionDto::CompleteToken {
                    token_pointer: "/token".into(),
                },
                hmac,
            ),
            WebhookDto {
                idempotency: Some("header:x-id".into()),
                ..dto(
                    WebhookActionDto::Start {
                        workflow: "wf".into(),
                        version: None,
                        input: "body".into(),
                    },
                    v(VerifierKindDto::Standard),
                )
            },
        ] {
            let w = webhook_from_dto(&d).expect("valid");
            let back = webhook_to_dto(&w, 7).expect("decodes");
            assert_eq!(
                back,
                WebhookDto {
                    version_tag: Some(7),
                    ..d.clone()
                },
                "{d:?}"
            );
        }
    }

    #[test]
    fn invalid_verifiers_rejected() {
        let mut gh = v(VerifierKindDto::Github);
        gh.tolerance_s = Some(10);
        let mut stray = v(VerifierKindDto::Stripe);
        stray.header = Some("x".into());
        let mut no_header = v(VerifierKindDto::Hmac);
        no_header.encoding = Some("hex".into());
        let mut bad_alg = v(VerifierKindDto::Hmac);
        bad_alg.header = Some("x-sig".into());
        bad_alg.algorithm = Some("sha1".into());
        let mut unsigned_ts = v(VerifierKindDto::Hmac);
        unsigned_ts.header = Some("x-sig".into());
        unsigned_ts.timestamp_header = Some("x-ts".into());
        for bad in [gh, stray, no_header, bad_alg, unsigned_ts] {
            let e = webhook_from_dto(&dto(start(), bad.clone())).expect_err("invalid");
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{bad:?}");
        }
        let mut name = dto(start(), v(VerifierKindDto::Stripe));
        name.name = "__x".into();
        assert!(webhook_from_dto(&name).is_err());
    }

    #[test]
    fn none_verifier_drops_secret_ref() {
        let w = webhook_from_dto(&dto(start(), v(VerifierKindDto::None))).expect("valid");
        assert!(w.secret_ref.is_empty());
    }

    #[test]
    fn key_formats() {
        assert_eq!(hook_idempotency_key("h", "evt_1"), "__hook/h/evt_1");
        assert_eq!(signal_request_id("evt_1"), "evt_1");
        let long = "k".repeat(65);
        let id = signal_request_id(&long);
        assert_eq!(id.len(), 64);
        assert_ne!(id, long);
        let k = body_hash_key(b"{}");
        assert!(k.starts_with("sha256:"));
        assert_eq!(k.len(), 7 + 64);
    }
}
