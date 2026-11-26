//! Phase 14 (14-03): the meta-backed [`Authenticator`] (D-P14-4/5/6).
//!
//! [`MetaAuthenticator`] resolves a bearer against the **local** meta view: no network, no
//! meta write, ever (D-P14-4: no `last_used` in meta). Order of checks:
//!
//! 1. [`tokens::parse`] (malformed or non-`otk_` bearers fail before any lookup);
//! 2. the `ApiToken` record by id (missing → `Invalid`);
//! 3. the constant-time hash comparison (mismatch → `Invalid`);
//! 4. the stored scopes: any non-API scope (`worker:join`, `node:join`) or unknown string →
//!    `NotApiToken`;
//! 5. expiry against the injected clock (`expires_at_ms <= now` → `Expired`). Expiry is
//!    checked after the secret, so only the secret's holder learns that a token expired.
//!
//! Clients only ever see 401 `unauthenticated` with `details.reason` = [`reason`]
//! (`invalid_token` or `expired_token`; `missing_token` without a bearer). A bounded
//! in-memory last-seen map ([`LAST_SEEN_CAP`] entries, LRU) and the
//! `ostro_auth_requests_total{result}` counter are the only side effects. Failpoint
//! `auth::lookup` (failpoints builds) injects a store error (503).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use ostro_api::auth::{AllowDev, AuthError, Authenticator, Principal, ScopeSet};
use ostro_meta::sm::MetaView;
use ostro_partition::Clock;

use super::P14Ctx;
use super::tokens::{self, ParseError};
use crate::config::AuthMode;

/// Counter of authentication attempts by `result` (`ok`, `missing`, `malformed`, `invalid`,
/// `expired`, `not_api_token`, `unavailable`).
pub const AUTH_REQUESTS_METRIC: &str = "ostro_auth_requests_total";

/// Capacity of the last-seen map (least recently seen ids are evicted first).
pub const LAST_SEEN_CAP: usize = 10_000;

/// The API authenticator: `[auth] mode = "none"` → [`AllowDev`] (every request is the dev
/// principal); `token` → [`MetaAuthenticator`] over this node's meta view and clock.
pub fn authenticator(ctx: &Arc<P14Ctx>) -> Arc<dyn Authenticator> {
    match ctx.cfg.auth.mode {
        AuthMode::None => Arc::new(AllowDev),
        AuthMode::Token => Arc::new(MetaAuthenticator::new(
            ctx.meta.view().clone(),
            Arc::clone(&ctx.clock),
            AuthMode::Token,
        )),
    }
}

/// The `details.reason` a transport puts into its 401 body for `e` (no further distinction
/// is ever leaked): `expired_token` for [`AuthError::Expired`], `missing_token` for
/// [`AuthError::Missing`], `invalid_token` for every other rejection. `Unavailable` is a 503,
/// not a 401 (`auth_unavailable`).
pub fn reason(e: &AuthError) -> &'static str {
    match e {
        AuthError::Expired => "expired_token",
        AuthError::Missing => "missing_token",
        AuthError::Unavailable(_) => "auth_unavailable",
        AuthError::Malformed | AuthError::Invalid | AuthError::NotApiToken => "invalid_token",
    }
}

/// Bearer → principal against the local meta view (see the module docs).
pub struct MetaAuthenticator {
    view: MetaView,
    clock: Arc<dyn Clock>,
    mode: AuthMode,
    seen: Mutex<LastSeen>,
}

impl std::fmt::Debug for MetaAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaAuthenticator")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl MetaAuthenticator {
    /// An authenticator over `view`, with expiry read from `clock`. `mode = None` answers
    /// [`Principal::dev`] for every request (bearer or not), like [`AllowDev`].
    pub fn new(view: MetaView, clock: Arc<dyn Clock>, mode: AuthMode) -> Self {
        Self {
            view,
            clock,
            mode,
            seen: Mutex::new(LastSeen::new(LAST_SEEN_CAP)),
        }
    }

    /// When `token_id` last authenticated successfully on this node (clock ms), if it is
    /// still in the bounded map.
    pub fn last_seen(&self, token_id: &str) -> Option<u64> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(token_id)
    }

    /// Number of ids in the last-seen map (≤ [`LAST_SEEN_CAP`]).
    pub fn last_seen_len(&self) -> usize {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// The synchronous check behind [`Authenticator::authenticate`].
    pub fn check(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        if self.mode == AuthMode::None {
            return Ok(Principal::dev());
        }
        let bearer = bearer.ok_or(AuthError::Missing)?;
        let (id, secret) = tokens::parse(bearer).map_err(|e| match e {
            ParseError::NotApiToken => AuthError::NotApiToken,
            _ => AuthError::Malformed,
        })?;
        #[cfg(feature = "failpoints")]
        fail::fail_point!("auth::lookup", |arg| Err(AuthError::Unavailable(
            arg.unwrap_or_else(|| "injected lookup failure (auth::lookup)".into())
        )));
        let token = self
            .view
            .read(|s| s.token(id.as_str()).map(|v| v.value.clone()))
            .ok_or(AuthError::Invalid)?;
        if token.id != id.as_str() || !tokens::verify(&token.hash, &secret) {
            return Err(AuthError::Invalid);
        }
        let scopes = ScopeSet::parse(&token.scopes).map_err(|_| AuthError::NotApiToken)?;
        let now = self.clock.now_ms();
        if token.expires_at_ms.is_some_and(|e| e <= now) {
            return Err(AuthError::Expired);
        }
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .touch(id.as_str(), now);
        Ok(Principal::token(id.into_string(), token.tenant, scopes))
    }
}

#[async_trait::async_trait]
impl Authenticator for MetaAuthenticator {
    async fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        let res = self.check(bearer);
        let result = match &res {
            Ok(_) => "ok",
            Err(e) => e.as_str(),
        };
        metrics::counter!(AUTH_REQUESTS_METRIC, "result" => result).increment(1);
        res
    }
}

/// A bounded least-recently-seen map: id → last success (clock ms).
#[derive(Debug)]
struct LastSeen {
    cap: usize,
    seq: u64,
    by_id: HashMap<String, (u64, u64)>,
    order: BTreeMap<u64, String>,
}

impl LastSeen {
    fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            seq: 0,
            by_id: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    fn get(&self, id: &str) -> Option<u64> {
        self.by_id.get(id).map(|&(_, ms)| ms)
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }

    fn touch(&mut self, id: &str, now_ms: u64) {
        if let Some((seq, _)) = self.by_id.remove(id) {
            self.order.remove(&seq);
        }
        self.seq += 1;
        self.order.insert(self.seq, id.to_owned());
        self.by_id.insert(id.to_owned(), (self.seq, now_ms));
        while self.by_id.len() > self.cap {
            match self.order.pop_first() {
                Some((_, old)) => {
                    self.by_id.remove(&old);
                }
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_seen_is_bounded_lru() {
        let mut m = LastSeen::new(3);
        m.touch("a", 1);
        m.touch("b", 2);
        m.touch("c", 3);
        m.touch("a", 4); // a is now the most recent
        m.touch("d", 5); // evicts b
        assert_eq!(m.len(), 3);
        assert_eq!(m.get("a"), Some(4));
        assert_eq!(m.get("b"), None);
        assert_eq!(m.get("c"), Some(3));
        assert_eq!(m.get("d"), Some(5));
    }

    #[test]
    fn reasons_leak_only_invalid_or_expired() {
        assert_eq!(reason(&AuthError::Expired), "expired_token");
        for e in [
            AuthError::Malformed,
            AuthError::Invalid,
            AuthError::NotApiToken,
        ] {
            assert_eq!(reason(&e), "invalid_token");
        }
    }
}
