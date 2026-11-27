//! Phase 14 (14-21, D-P14-31): the embedded UI behind the API's authentication, scopes,
//! tenant isolation and audit.
//!
//! # Sessions
//!
//! `POST /ui/login` exchanges an API bearer for the `ostro_session` cookie
//! ([`TokenUiAuth::issue`]): the bearer goes through the same authenticator as the API
//! (`authn::authenticator`, so a failure is audited by the 14-18 decorator) and the cookie
//! value is
//!
//! ```text
//! base64url("v1.<token_id>.<exp_ms>") "." base64url(HMAC-SHA256(key, "v1.<token_id>.<exp_ms>"))
//! ```
//!
//! with `exp_ms = min(now + [auth] ui_session_ttl, token.expires_at_ms)` on the injected
//! clock, and `key` = the 32 bytes of the meta secret `__ostro/ui-session-key` (one key for
//! the whole cluster, so a cookie issued by one node is accepted by every node). The MAC is
//! checked in constant time (`aws_lc_rs::hmac::verify`) before the payload is parsed;
//! unknown versions, malformed payloads and `exp_ms <= now` are refused.
//!
//! **Every request re-validates the token behind the session** against the local meta
//! view ([`TokenUiAuth::resolve`]): a revoked (deleted) or rotated-away token, or one that
//! expired, ends the session at once. The token's scopes become the UI scopes (`read` when
//! the token allows `read`, `operate` when it allows `operate`; `admin` allows both).
//!
//! # Key creation (B3)
//!
//! In token mode, once this node is the meta leader, an absent key is created as
//! `plaintext-v0` (32 random bytes, CAS create-only), whether or not a sealing key exists;
//! `reseal::run` (14-19) converts it to aes256gcm with an operator-supplied key. A sign-in
//! that finds no key creates it the same way (any node; the proposal is forwarded to the
//! leader). Readers accept both key ids ([`super::reseal::open_record`]); the decoded key
//! is cached per record version.
//!
//! # Principal hand-off
//!
//! [`UiScopeLayer`] is the outermost layer of the `/ui` router. In `[auth] mode = "none"`
//! every handler runs inside `PRINCIPAL.scope(Principal::dev(), …)` (14-01b, N2, unchanged).
//! In token mode it resolves the session once, runs the request inside
//! `PRINCIPAL.scope(<the token's principal>, …)` (no session: `Anonymous`, which the guard
//! denies), and leaves the result in the request extensions, where
//! [`TokenUiAuth::principal`] (the `ostro_ui::auth::UiAuth` the UI's extractors call) picks
//! it up instead of verifying again. The guarded `ControlPlane` behind the UI therefore
//! applies the same scope checks, tenant filtering (tenant principals see stripped names;
//! the UI renders them as they come) and audit as REST.

use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use aws_lc_rs::hmac;
use axum::extract::{Request, State};
use axum::http;
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ostro_api::auth::{Authenticator, Principal, PrincipalKind, Scope, ScopeSet};
use ostro_bridge::secrets::SealedStore;
use ostro_meta::ext::{SecretTimes, get_api_token_meta, put_secret_times};
use ostro_meta::records::{SealedSecret, TOKEN_KEY_PLAINTEXT_ID};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_types::compat::Feature;
use ostro_ui::auth::{LoginFuture, SESSION_COOKIE, UiAuth, UiPrincipal, UiReject, UiSession};
use tracing::{info, warn};

use super::P14Ctx;
use super::reseal::UI_SESSION_KEY_SECRET;
use crate::config::{ApiTlsMode, AuthMode};

/// Session format version.
pub const SESSION_VERSION: &str = "v1";
/// Longest `ostro_session` value considered (anything longer is refused unparsed).
pub const MAX_SESSION_LEN: usize = 512;
/// Key length (bytes).
pub const KEY_LEN: usize = 32;
/// CAS attempts when creating the key.
const CREATE_RETRIES: usize = 8;
/// How often a non-leader re-checks for leadership (key creation task).
const LEADER_POLL: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------------
// Session codec (pure)
// ---------------------------------------------------------------------------------

/// A verified session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    /// The API token behind the session.
    pub token_id: String,
    /// Expiry (clock ms).
    pub exp_ms: u64,
}

/// Why a session was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    /// No cookie, not `<payload>.<mac>` in base64url, or too long.
    #[error("malformed session")]
    Malformed,
    /// The MAC does not verify under the key (tampered, forged or another key).
    #[error("bad session signature")]
    BadMac,
    /// The MAC verifies but the payload version is not `v1`.
    #[error("unknown session version")]
    Version,
    /// `exp_ms <= now`.
    #[error("session expired")]
    Expired,
    /// The token behind the session no longer exists (revoked or rotated away).
    #[error("token revoked")]
    Revoked,
    /// The token behind the session expired.
    #[error("token expired")]
    TokenExpired,
    /// The token's scopes are not API scopes.
    #[error("not an API token")]
    NotApiToken,
    /// No session key (not created yet, or sealed with a key this node lacks).
    #[error("session key unavailable")]
    NoKey,
}

fn mac_key(key: &[u8; KEY_LEN]) -> hmac::Key {
    hmac::Key::new(hmac::HMAC_SHA256, key)
}

/// The cookie value of a session for `token_id` expiring at `exp_ms`.
pub fn encode(key: &[u8; KEY_LEN], token_id: &str, exp_ms: u64) -> String {
    let payload = format!("{SESSION_VERSION}.{token_id}.{exp_ms}");
    let tag = hmac::sign(&mac_key(key), payload.as_bytes());
    format!("{}.{}", B64.encode(payload), B64.encode(tag.as_ref()))
}

/// Verify a cookie value at `now_ms`: the MAC first (constant time), then the payload.
pub fn decode(key: &[u8; KEY_LEN], value: &str, now_ms: u64) -> Result<Session, SessionError> {
    if value.len() > MAX_SESSION_LEN {
        return Err(SessionError::Malformed);
    }
    let (p, m) = value.split_once('.').ok_or(SessionError::Malformed)?;
    let payload = B64.decode(p).map_err(|_| SessionError::Malformed)?;
    let mac = B64.decode(m).map_err(|_| SessionError::Malformed)?;
    hmac::verify(&mac_key(key), &payload, &mac).map_err(|_| SessionError::BadMac)?;
    let payload = std::str::from_utf8(&payload).map_err(|_| SessionError::Malformed)?;
    let mut it = payload.split('.');
    let (Some(v), Some(id), Some(exp), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(SessionError::Malformed);
    };
    if v != SESSION_VERSION {
        return Err(SessionError::Version);
    }
    if !super::tokens::is_api_token_id(id) {
        return Err(SessionError::Malformed);
    }
    let exp_ms: u64 = exp.parse().map_err(|_| SessionError::Malformed)?;
    if exp_ms <= now_ms {
        return Err(SessionError::Expired);
    }
    Ok(Session {
        token_id: id.to_owned(),
        exp_ms,
    })
}

/// The `ostro_session` cookie of a request, if any (the first one).
pub fn session_cookie(headers: &http::HeaderMap) -> Option<&str> {
    headers
        .get_all(http::header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == SESSION_COOKIE)
        .map(|(_, v)| v.trim())
}

/// The UI scopes of an API scope set.
pub fn ui_scopes(scopes: ScopeSet) -> Vec<String> {
    let mut out = Vec::new();
    if scopes.allows(Scope::Read) {
        out.push("read".to_owned());
    }
    if scopes.allows(Scope::Operate) {
        out.push("operate".to_owned());
    }
    out
}

// ---------------------------------------------------------------------------------
// TokenUiAuth
// ---------------------------------------------------------------------------------

/// The token-mode [`UiAuth`]: signed, expiring, revocable, cluster-wide sessions.
pub struct TokenUiAuth {
    ctx: Weak<P14Ctx>,
    authn: Arc<dyn Authenticator>,
    ttl_ms: u64,
    secure: bool,
    key: Mutex<Option<(u64, [u8; KEY_LEN])>>,
}

impl std::fmt::Debug for TokenUiAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenUiAuth")
            .field("ttl_ms", &self.ttl_ms)
            .field("secure", &self.secure)
            .finish_non_exhaustive()
    }
}

/// The request extension [`UiScopeLayer`] leaves for [`TokenUiAuth::principal`].
#[derive(Clone)]
struct SessionResolved(Result<UiPrincipal, SessionError>);

/// Whether the client listener serves TLS (`Secure` session cookies).
fn listener_tls(ctx: &P14Ctx) -> bool {
    match ctx.cfg.api.tls.mode {
        ApiTlsMode::Off => false,
        ApiTlsMode::Auto => !ctx.cfg.api.listen.ip().is_loopback(),
        ApiTlsMode::SelfSigned | ApiTlsMode::Files | ApiTlsMode::Acme => true,
    }
}

fn unavailable() -> UiReject {
    UiReject {
        status: http::StatusCode::SERVICE_UNAVAILABLE,
        message: "sign-in is unavailable".to_owned(),
    }
}

impl TokenUiAuth {
    /// The UI auth of `ctx` (token mode).
    pub fn new(ctx: &Arc<P14Ctx>) -> Self {
        Self {
            ctx: Arc::downgrade(ctx),
            authn: super::authn::authenticator(ctx),
            ttl_ms: u64::try_from(ctx.cfg.auth.ui_session_ttl.get().as_millis())
                .unwrap_or(u64::MAX)
                .max(1),
            secure: listener_tls(ctx),
            key: Mutex::new(None),
        }
    }

    /// The session key from the meta view (cached per record version); `None` while absent
    /// or unreadable on this node.
    fn session_key(&self, ctx: &P14Ctx) -> Option<[u8; KEY_LEN]> {
        let (version, record) = ctx.meta.view().read(|s| {
            s.get(&MetaKey::Secret(UI_SESSION_KEY_SECRET.to_owned()))
                .map(|v| (v.version, v.value.clone()))
        })?;
        let MetaRecord::Secret(rec) = record else {
            warn!("{UI_SESSION_KEY_SECRET} is not a secret record");
            return None;
        };
        let mut cache = self.key.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((v, k)) = *cache
            && v == version
        {
            return Some(k);
        }
        let store = if rec.key_id == TOKEN_KEY_PLAINTEXT_ID {
            None
        } else {
            super::reseal::load_ring(ctx).map(|r| SealedStore::new(Arc::new(r)))
        };
        let key = match super::reseal::open_record(UI_SESSION_KEY_SECRET, &rec, store.as_ref()) {
            Ok(v) => match <[u8; KEY_LEN]>::try_from(v.expose()) {
                Ok(k) => k,
                Err(_) => {
                    warn!(
                        len = v.expose().len(),
                        "{UI_SESSION_KEY_SECRET} is not {KEY_LEN} bytes"
                    );
                    return None;
                }
            },
            Err(e) => {
                warn!(error = %e, "UI sessions unavailable on this node");
                return None;
            }
        };
        *cache = Some((version, key));
        Some(key)
    }

    /// Verify the request's session and re-validate its token: the API principal for the
    /// `PRINCIPAL` scope and the UI principal.
    pub fn resolve(
        &self,
        headers: &http::HeaderMap,
    ) -> Result<(Principal, UiPrincipal), SessionError> {
        let value = session_cookie(headers).ok_or(SessionError::Malformed)?;
        let ctx = self.ctx.upgrade().ok_or(SessionError::NoKey)?;
        let key = self.session_key(&ctx).ok_or(SessionError::NoKey)?;
        let now = ctx.clock.now_ms();
        let s = decode(&key, value, now)?;
        let token = ctx
            .meta
            .view()
            .read(|st| st.token(&s.token_id).map(|v| v.value.clone()))
            .ok_or(SessionError::Revoked)?;
        if token.id != s.token_id {
            return Err(SessionError::Revoked);
        }
        if token.expires_at_ms.is_some_and(|e| e <= now) {
            return Err(SessionError::TokenExpired);
        }
        let scopes = ScopeSet::parse(&token.scopes).map_err(|_| SessionError::NotApiToken)?;
        let name = get_api_token_meta(&token.ext)
            .ok()
            .flatten()
            .map(|m| m.name)
            .filter(|n| !n.is_empty());
        let ui = UiPrincipal {
            scopes: ui_scopes(scopes),
            token_id: Some(token.id.clone()),
            name,
            tenant: token.tenant.clone(),
        };
        Ok((Principal::token(token.id, token.tenant, scopes), ui))
    }

    /// Exchange a bearer for a session (`POST /ui/login`). Failures are audited by the
    /// authenticator (14-18) and answered generically.
    pub async fn issue(&self, bearer: &str) -> Result<UiSession, UiReject> {
        let denied = || UiReject::unauthorized("sign-in failed");
        let ctx = self.ctx.upgrade().ok_or_else(unavailable)?;
        let p = self
            .authn
            .authenticate(Some(bearer))
            .await
            .map_err(|_| denied())?;
        if p.kind != PrincipalKind::Token {
            return Err(denied());
        }
        let token = ctx
            .meta
            .view()
            .read(|st| st.token(&p.token_id).map(|v| v.value.clone()))
            .ok_or_else(denied)?;
        let now = ctx.clock.now_ms();
        let mut exp = now.saturating_add(self.ttl_ms);
        if let Some(e) = token.expires_at_ms {
            exp = exp.min(e);
        }
        if exp <= now {
            return Err(denied());
        }
        let key = match self.session_key(&ctx) {
            Some(k) => k,
            None => {
                ensure_key(&ctx).await.map_err(|e| {
                    warn!(error = %e, "UI session key unavailable");
                    unavailable()
                })?;
                self.session_key(&ctx).ok_or_else(unavailable)?
            }
        };
        Ok(UiSession {
            value: encode(&key, &p.token_id, exp),
            max_age_secs: (exp - now).div_ceil(1000),
        })
    }
}

impl UiAuth for TokenUiAuth {
    fn principal(&self, parts: &http::request::Parts) -> Result<UiPrincipal, UiReject> {
        let res = match parts.extensions.get::<SessionResolved>() {
            Some(SessionResolved(r)) => r.clone(),
            None => self.resolve(&parts.headers).map(|(_, ui)| ui),
        };
        res.map_err(|_| UiReject::signed_out(parts, "sign in to continue"))
    }

    fn sessions(&self) -> bool {
        true
    }

    fn login<'a>(&'a self, bearer: &'a str) -> LoginFuture<'a> {
        Box::pin(self.issue(bearer))
    }

    fn secure_cookies(&self, uri: &http::Uri) -> bool {
        self.secure || uri.scheme_str() == Some("https")
    }
}

/// Create `__ostro/ui-session-key` as `plaintext-v0` (32 random bytes) unless it exists
/// (CAS create-only). `Ok(true)` when this call created it.
pub async fn ensure_key(ctx: &P14Ctx) -> Result<bool, String> {
    let key = MetaKey::Secret(UI_SESSION_KEY_SECRET.to_owned());
    let times_ok =
        ostro_cluster::compat::require(ctx.meta.view(), Feature::SecretTimesExt, ctx.level_max)
            .is_ok();
    for _ in 0..CREATE_RETRIES {
        if ctx.meta.view().read(|s| s.get(&key).is_some()) {
            return Ok(false);
        }
        let mut v = vec![0u8; KEY_LEN];
        getrandom::fill(&mut v).map_err(|e| format!("OS random source: {e}"))?;
        let ext = if times_ok {
            put_secret_times(
                &[],
                &SecretTimes {
                    created_at_ms: ctx.clock.now_ms(),
                    rotated_at_ms: None,
                },
            )
            .map_err(|e| e.to_string())?
        } else {
            Vec::new()
        };
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: key.clone(),
                expected_version: None,
                record: MetaRecord::Secret(SealedSecret {
                    name: UI_SESSION_KEY_SECRET.to_owned(),
                    ciphertext: v,
                    key_id: TOKEN_KEY_PLAINTEXT_ID.to_owned(),
                    ext,
                }),
            })
            .await
            .map_err(|e| format!("metadata unavailable: {e}"))?;
        match res {
            MetaResult::Ok { .. } => {
                info!("{UI_SESSION_KEY_SECRET} created (plaintext-v0; reseal seals it)");
                return Ok(true);
            }
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
            other => return Err(format!("metadata rejected {UI_SESSION_KEY_SECRET}: {other:?}")),
        }
    }
    Err(format!(
        "{UI_SESSION_KEY_SECRET}: lost the CAS {CREATE_RETRIES} times"
    ))
}

/// Token mode: once this node is the meta leader, create the key if absent.
fn spawn_key_creation(ctx: &Arc<P14Ctx>) {
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let weak = Arc::downgrade(ctx);
    let stop = ctx.shutdown.clone();
    rt.spawn(async move {
        loop {
            let Some(ctx) = weak.upgrade() else { break };
            if ctx.meta.is_leader() {
                if let Err(e) = ensure_key(&ctx).await {
                    warn!(error = %e, "{UI_SESSION_KEY_SECRET} not created at start");
                }
                break;
            }
            drop(ctx);
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(LEADER_POLL) => {}
            }
        }
    });
}

// ---------------------------------------------------------------------------------
// Construction hooks
// ---------------------------------------------------------------------------------

/// The UI's [`UiAuth`]: `AllowAll` in `[auth] mode = "none"`; [`TokenUiAuth`] in token
/// mode (which also starts the key creation on the meta leader).
pub fn ui_auth(ctx: &Arc<P14Ctx>) -> Arc<dyn UiAuth> {
    match ctx.cfg.auth.mode {
        AuthMode::None => Arc::new(ostro_ui::auth::AllowAll),
        AuthMode::Token => {
            spawn_key_creation(ctx);
            Arc::new(TokenUiAuth::new(ctx))
        }
    }
}

/// The `/ui` principal scope (built by [`super::ui_scope_layer`]); [`UiScopeLayer::apply`]
/// installs it on the UI router.
#[derive(Clone)]
pub struct UiScopeLayer {
    inner: Arc<Inner>,
}

struct Inner {
    mode: AuthMode,
    /// Token mode: the session resolver (`None` without a context: every request is
    /// anonymous).
    token: Option<Arc<TokenUiAuth>>,
}

impl std::fmt::Debug for UiScopeLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UiScopeLayer")
            .field("mode", &self.inner.mode)
            .finish_non_exhaustive()
    }
}

/// Build the layer for `ctx`'s `[auth] mode`. `_auth` is the UI's own instance; the layer
/// resolves sessions with its own [`TokenUiAuth`] and hands the result to it through the
/// request extensions.
pub fn scope_layer(ctx: &Arc<P14Ctx>, _auth: Arc<dyn UiAuth>) -> UiScopeLayer {
    let mode = ctx.cfg.auth.mode;
    UiScopeLayer {
        inner: Arc::new(Inner {
            mode,
            token: (mode == AuthMode::Token).then(|| Arc::new(TokenUiAuth::new(ctx))),
        }),
    }
}

impl UiScopeLayer {
    /// The layer for an explicit mode without a context (tests): token mode then runs every
    /// request as `Anonymous`.
    pub fn for_mode(mode: AuthMode, _auth: Arc<dyn UiAuth>) -> Self {
        Self {
            inner: Arc::new(Inner { mode, token: None }),
        }
    }

    /// Run every handler of `router` inside the request's principal scope.
    pub fn apply(self, router: axum::Router) -> axum::Router {
        router.layer(axum::middleware::from_fn_with_state(self, scope_ui))
    }

    /// The `[auth] mode` this layer serves.
    pub fn mode(&self) -> AuthMode {
        self.inner.mode
    }

    /// Token mode: the principal a request with `headers` runs as (`Anonymous` without a
    /// valid session).
    pub fn principal_of(&self, headers: &http::HeaderMap) -> Principal {
        match self.inner.mode {
            AuthMode::None => Principal::dev(),
            AuthMode::Token => self
                .inner
                .token
                .as_ref()
                .and_then(|t| t.resolve(headers).ok())
                .map_or_else(Principal::anonymous, |(p, _)| p),
        }
    }
}

async fn scope_ui(State(layer): State<UiScopeLayer>, req: Request, next: Next) -> Response {
    match layer.inner.mode {
        AuthMode::None => ostro_api::auth::scope(Principal::dev(), next.run(req)).await,
        AuthMode::Token => {
            let (mut parts, body) = req.into_parts();
            let res = match &layer.inner.token {
                Some(t) => t.resolve(&parts.headers),
                None => Err(SessionError::NoKey),
            };
            let (principal, ui) = match res {
                Ok((p, ui)) => (p, Ok(ui)),
                Err(e) => (Principal::anonymous(), Err(e)),
            };
            parts.extensions.insert(SessionResolved(ui));
            let req = Request::from_parts(parts, body);
            ostro_api::auth::scope(principal, next.run(req)).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use ostro_meta::{ApiToken, MetaHandle};
    use ostro_partition::Clock;
    use ostro_partition::clock::ManualClock;

    const K1: [u8; KEY_LEN] = [1; KEY_LEN];
    const K2: [u8; KEY_LEN] = [2; KEY_LEN];
    const ID: &str = "otk_aaaaaaaaaaaaaaaa";

    #[test]
    fn round_trip() {
        let v = encode(&K1, ID, 5_000);
        assert!(!v.contains(ID), "payload is encoded");
        assert_eq!(
            decode(&K1, &v, 4_999),
            Ok(Session {
                token_id: ID.into(),
                exp_ms: 5_000
            })
        );
    }

    #[test]
    fn tampered_payload_or_mac_is_refused() {
        let v = encode(&K1, ID, 5_000);
        let (p, m) = v.split_once('.').expect("two parts");
        // Payload swapped for another token / a later expiry, MAC kept.
        for forged in [
            B64.encode("v1.otk_bbbbbbbbbbbbbbbb.5000"),
            B64.encode("v1.otk_aaaaaaaaaaaaaaaa.9999999999999"),
        ] {
            assert_eq!(
                decode(&K1, &format!("{forged}.{m}"), 0),
                Err(SessionError::BadMac)
            );
        }
        // MAC bit flip, truncated MAC.
        let mut mac = B64.decode(m).expect("b64");
        mac[0] ^= 1;
        assert_eq!(
            decode(&K1, &format!("{p}.{}", B64.encode(&mac)), 0),
            Err(SessionError::BadMac)
        );
        assert_eq!(
            decode(&K1, &format!("{p}.{}", B64.encode(&mac[..16])), 0),
            Err(SessionError::BadMac)
        );
        // Garbage, too long.
        for junk in ["", ".", "abc", "a.b.c", "!!.??"] {
            assert!(decode(&K1, junk, 0).is_err(), "{junk:?}");
        }
        assert_eq!(
            decode(&K1, &"a".repeat(MAX_SESSION_LEN + 1), 0),
            Err(SessionError::Malformed)
        );
    }

    #[test]
    fn unknown_version_and_bad_payloads_are_refused_even_with_a_valid_mac() {
        let signed = |payload: &str| {
            let tag = hmac::sign(&mac_key(&K1), payload.as_bytes());
            format!("{}.{}", B64.encode(payload), B64.encode(tag.as_ref()))
        };
        assert_eq!(
            decode(&K1, &signed(&format!("v2.{ID}.5000")), 0),
            Err(SessionError::Version)
        );
        for bad in [
            format!("v1.{ID}"),
            format!("v1.{ID}.5000.x"),
            format!("v1.{ID}.soon"),
            "v1.wkr_aaaaaaaaaaaaaaaa.5000".to_owned(),
        ] {
            assert_eq!(
                decode(&K1, &signed(&bad), 0),
                Err(SessionError::Malformed),
                "{bad}"
            );
        }
    }

    #[test]
    fn expiry_follows_the_injected_clock() {
        let clock = ManualClock::new(1_000);
        let v = encode(&K1, ID, 2_000);
        assert!(decode(&K1, &v, clock.now_ms()).is_ok());
        clock.set(1_999);
        assert!(decode(&K1, &v, clock.now_ms()).is_ok());
        clock.set(2_000);
        assert_eq!(decode(&K1, &v, clock.now_ms()), Err(SessionError::Expired));
    }

    #[test]
    fn another_key_rejects_old_cookies() {
        let v = encode(&K1, ID, 5_000);
        assert_eq!(decode(&K2, &v, 0), Err(SessionError::BadMac));
        assert!(decode(&K2, &encode(&K2, ID, 5_000), 0).is_ok());
    }

    #[test]
    fn scopes_map_to_ui_scopes() {
        assert_eq!(ui_scopes(ScopeSet::admin()), ["read", "operate"]);
        assert_eq!(ui_scopes(ScopeSet::of(&[Scope::Read])), ["read"]);
        assert_eq!(
            ui_scopes(ScopeSet::of(&[Scope::Read, Scope::Operate])),
            ["read", "operate"]
        );
        assert!(ui_scopes(ScopeSet::of(&[Scope::Trigger, Scope::Deploy])).is_empty());
    }

    // -----------------------------------------------------------------------------
    // TokenUiAuth over a real meta handle (token mode)
    // -----------------------------------------------------------------------------

    struct Fx {
        ctx: Arc<P14Ctx>,
        clock: Arc<ManualClock>,
        _dir: tempfile::TempDir,
    }

    async fn fixture(sealing_key: Option<[u8; 32]>) -> Fx {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = Arc::new(MetaHandle::start(dir.path(), 1).await.expect("meta"));
        let clock = Arc::new(ManualClock::new(1_000_000));
        let mut cfg = crate::config::ServerConfig::server_defaults();
        cfg.auth.mode = AuthMode::Token;
        cfg.auth.ui_session_ttl = crate::config::FriendlyDuration::from_secs(3600);
        cfg.node.data_dir = dir.path().to_path_buf();
        if let Some(k) = sealing_key {
            let p = dir.path().join("operator-sealing.key");
            std::fs::write(&p, STANDARD.encode(k)).expect("key file");
            cfg.secrets.sealing_key_file = Some(p);
        }
        let ctx = super::super::build_ctx(super::super::P14Init {
            cfg: Arc::new(cfg),
            meta,
            clock: Arc::clone(&clock) as Arc<dyn Clock>,
            node_id: 1,
            data_dir: dir.path().to_path_buf(),
            level_max: ostro_types::compat::level_max(),
        })
        .expect("build_ctx");
        Fx {
            ctx,
            clock,
            _dir: dir,
        }
    }

    async fn put(meta: &MetaHandle, key: MetaKey, record: MetaRecord) {
        let cur = meta.view().read(|s| s.get(&key).map(|v| v.version));
        let r = meta
            .propose(MetaCommand::Put {
                key,
                expected_version: cur,
                record,
            })
            .await
            .expect("propose");
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    }

    /// A token of `tenant` with `scopes`; returns `(id, bearer)`.
    async fn token(
        meta: &MetaHandle,
        tenant: Option<&str>,
        scopes: &[&str],
        expires_at_ms: Option<u64>,
    ) -> (String, String) {
        let (id, secret, bearer) = super::super::tokens::generate_os().expect("rng");
        let rec = ApiToken {
            id: id.as_str().to_owned(),
            hash: super::super::tokens::hash(&secret),
            scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
            tenant: tenant.map(str::to_owned),
            created_at_ms: 1,
            expires_at_ms,
            ext: Vec::new(),
        };
        put(meta, MetaKey::Token(rec.id.clone()), MetaRecord::Token(rec)).await;
        (id.as_str().to_owned(), bearer.expose().to_owned())
    }

    fn cookie_headers(value: &str) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        h.insert(
            http::header::COOKIE,
            format!("ostro_csrf=x; {SESSION_COOKIE}={value}")
                .parse()
                .expect("hv"),
        );
        h
    }

    fn parts(headers: http::HeaderMap, method: &str, htmx: bool) -> http::request::Parts {
        let mut b = http::Request::builder().method(method).uri("/ui/runs");
        for (k, v) in &headers {
            b = b.header(k, v);
        }
        if htmx {
            b = b.header("hx-request", "true");
        }
        b.body(()).expect("request").into_parts().0
    }

    #[tokio::test]
    async fn login_creates_the_key_and_sessions_follow_the_token() {
        let fx = fixture(None).await;
        let meta = &fx.ctx.meta;
        let auth = TokenUiAuth::new(&fx.ctx);
        let (id, bearer) = token(meta, Some("acme"), &["read"], None).await;

        // Bad bearers: generic 401, no key created.
        for b in ["", "ostro_otk_aaaaaaaaaaaaaaaa_x", "nope"] {
            let e = auth.issue(b).await.expect_err("refused");
            assert_eq!(e.status, http::StatusCode::UNAUTHORIZED);
            assert_eq!(e.message, "sign-in failed");
        }
        let has_key = || {
            meta.view().read(|s| {
                s.get(&MetaKey::Secret(UI_SESSION_KEY_SECRET.into()))
                    .is_some()
            })
        };
        assert!(!has_key());

        // The first sign-in creates the key as plaintext-v0.
        let s = auth.issue(&bearer).await.expect("session");
        assert_eq!(s.max_age_secs, 3600);
        let rec = meta
            .view()
            .read(|st| st.get(&MetaKey::Secret(UI_SESSION_KEY_SECRET.into())))
            .expect("key created");
        let MetaRecord::Secret(rec) = rec.value else {
            panic!("not a secret")
        };
        assert_eq!(rec.key_id, TOKEN_KEY_PLAINTEXT_ID);
        assert_eq!(rec.ciphertext.len(), KEY_LEN);
        assert!(!ensure_key(&fx.ctx).await.expect("ensure"), "create-only");

        // The session resolves to the token's principal.
        let (p, ui) = auth.resolve(&cookie_headers(&s.value)).expect("valid");
        assert_eq!(
            p,
            Principal::token(&id, Some("acme".into()), ScopeSet::of(&[Scope::Read]))
        );
        assert_eq!(ui.scopes, ["read"]);
        assert_eq!(ui.token_id.as_deref(), Some(id.as_str()));
        assert_eq!(ui.tenant.as_deref(), Some("acme"));
        // Another instance (another node: the key is shared through meta) accepts it.
        let other = TokenUiAuth::new(&fx.ctx);
        assert!(other.resolve(&cookie_headers(&s.value)).is_ok());
        // UiAuth::principal: ok with the cookie; without one, 303 for a page GET and 401
        // for htmx / POST.
        let ok = other.principal(&parts(cookie_headers(&s.value), "GET", false));
        assert_eq!(ok.expect("principal").scopes, ["read"]);
        for (method, htmx, want) in [
            ("GET", false, http::StatusCode::SEE_OTHER),
            ("GET", true, http::StatusCode::UNAUTHORIZED),
            ("POST", false, http::StatusCode::UNAUTHORIZED),
        ] {
            let e = other
                .principal(&parts(http::HeaderMap::new(), method, htmx))
                .expect_err("signed out");
            assert_eq!(e.status, want, "{method} htmx={htmx}");
        }

        // Session expiry on the injected clock.
        fx.clock.advance(3_600_000);
        assert_eq!(
            auth.resolve(&cookie_headers(&s.value)).map(|_| ()),
            Err(SessionError::Expired)
        );
    }

    #[tokio::test]
    async fn revoked_or_expired_tokens_end_the_session() {
        let fx = fixture(None).await;
        let meta = &fx.ctx.meta;
        let auth = TokenUiAuth::new(&fx.ctx);
        let now = fx.clock.now_ms();
        // A token expiring in 10 s: the session is capped at the token's expiry.
        let (_, short) = token(meta, None, &["read"], Some(now + 10_000)).await;
        let s = auth.issue(&short).await.expect("session");
        assert_eq!(s.max_age_secs, 10);
        assert!(auth.resolve(&cookie_headers(&s.value)).is_ok());

        // Revocation (the record is deleted): refused on the next request.
        let (id, bearer) = token(meta, Some("acme"), &["admin"], None).await;
        let s2 = auth.issue(&bearer).await.expect("session");
        assert!(auth.resolve(&cookie_headers(&s2.value)).is_ok());
        let version = meta
            .view()
            .read(|st| st.token(&id).map(|v| v.version))
            .expect("token");
        let r = meta
            .propose(MetaCommand::Delete {
                key: MetaKey::Token(id.clone()),
                expected_version: version,
            })
            .await
            .expect("delete");
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
        assert_eq!(
            auth.resolve(&cookie_headers(&s2.value)).map(|_| ()),
            Err(SessionError::Revoked)
        );
        // An expired token cannot sign in.
        fx.clock.advance(10_000);
        assert!(auth.issue(&short).await.is_err());
        // ... and its session (capped at the token's expiry) is over.
        assert!(auth.resolve(&cookie_headers(&s.value)).is_err());
    }

    #[tokio::test]
    async fn a_new_key_rejects_old_cookies_and_a_forged_cookie_is_refused() {
        let fx = fixture(None).await;
        let meta = &fx.ctx.meta;
        let auth = TokenUiAuth::new(&fx.ctx);
        let (id, bearer) = token(meta, Some("acme"), &["read", "operate"], None).await;
        let s = auth.issue(&bearer).await.expect("session");
        assert!(auth.resolve(&cookie_headers(&s.value)).is_ok());
        // A cookie minted with a guessed key is forged.
        let forged = encode(&K1, &id, fx.clock.now_ms() + 60_000);
        assert_eq!(
            auth.resolve(&cookie_headers(&forged)).map(|_| ()),
            Err(SessionError::BadMac)
        );
        // Rotate the key in meta: old cookies are refused, new ones work.
        put(
            meta,
            MetaKey::Secret(UI_SESSION_KEY_SECRET.into()),
            MetaRecord::Secret(SealedSecret {
                name: UI_SESSION_KEY_SECRET.into(),
                ciphertext: K2.to_vec(),
                key_id: TOKEN_KEY_PLAINTEXT_ID.into(),
                ext: Vec::new(),
            }),
        )
        .await;
        assert_eq!(
            auth.resolve(&cookie_headers(&s.value)).map(|_| ()),
            Err(SessionError::BadMac)
        );
        let s2 = auth.issue(&bearer).await.expect("session");
        assert!(auth.resolve(&cookie_headers(&s2.value)).is_ok());
        assert_eq!(
            decode(&K2, &s2.value, fx.clock.now_ms()).map(|s| s.token_id),
            Ok(id)
        );
    }

    #[tokio::test]
    async fn a_sealed_key_is_read_with_the_operator_key() {
        let fx = fixture(Some([9u8; 32])).await;
        let ring = super::super::reseal::load_ring(&fx.ctx).expect("operator key resolves");
        let store = SealedStore::new(Arc::new(ring));
        let mut rec = store.seal(UI_SESSION_KEY_SECRET, &K2).expect("seal");
        rec.name = UI_SESSION_KEY_SECRET.into();
        assert_ne!(rec.key_id, TOKEN_KEY_PLAINTEXT_ID);
        put(
            &fx.ctx.meta,
            MetaKey::Secret(UI_SESSION_KEY_SECRET.into()),
            MetaRecord::Secret(rec),
        )
        .await;
        let auth = TokenUiAuth::new(&fx.ctx);
        assert_eq!(auth.session_key(&fx.ctx), Some(K2));
        let (_, bearer) = token(&fx.ctx.meta, None, &["read"], None).await;
        let s = auth.issue(&bearer).await.expect("session");
        assert!(decode(&K2, &s.value, fx.clock.now_ms()).is_ok());
    }

    #[tokio::test]
    async fn the_scope_layer_resolves_the_session_principal() {
        let fx = fixture(None).await;
        let (id, bearer) = token(&fx.ctx.meta, Some("acme"), &["operate", "read"], None).await;
        let s = TokenUiAuth::new(&fx.ctx)
            .issue(&bearer)
            .await
            .expect("session");
        let layer = scope_layer(&fx.ctx, Arc::new(ostro_ui::auth::AllowAll));
        let p = layer.principal_of(&cookie_headers(&s.value));
        assert_eq!(p.token_id, id);
        assert_eq!(p.tenant.as_deref(), Some("acme"));
        assert_eq!(p.kind, PrincipalKind::Token);
        assert!(layer.principal_of(&http::HeaderMap::new()).is_anonymous());
        assert!(
            layer
                .principal_of(&cookie_headers("forged.value"))
                .is_anonymous()
        );
        let none = UiScopeLayer::for_mode(AuthMode::None, Arc::new(ostro_ui::auth::AllowAll));
        assert_eq!(none.principal_of(&http::HeaderMap::new()), Principal::dev());
    }
}
