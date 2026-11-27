---
phase: 14-multi-tenancy-security-1-0-release
plan: 21
subsystem: UI authentication (ostro-server p14/ui_auth.rs; ostro-ui auth seam, login/logout, layout)
tags: [sec-02, ui, sessions, hmac, csrf, scopes, tenants, audit]
requires:
  - phase: 14-01b
    provides: "p14::ui_auth / p14::ui_scope_layer call sites on the /ui router (p11::wire), UiScopeLayer stub (mode none -> Principal::dev())"
  - phase: 14-02
    provides: "/ui and /ui/* exempt from the bearer layer"
  - phase: 14-03
    provides: "MetaAuthenticator, tokens::{parse, is_api_token_id}"
  - phase: 14-15
    provides: "guarded ControlPlane scope checks (read / operate)"
  - phase: 14-16
    provides: "tenant filtering + name stripping for tenant principals"
  - phase: 14-17
    provides: "token revoke = record delete (DELETE /v1/auth/tokens/{id}), ApiTokenMeta name"
  - phase: 14-18
    provides: "auditing authenticator decorator (auth.failure), run.cancel audit capture through the guard"
  - phase: 14-19
    provides: "reseal::{UI_SESSION_KEY_SECRET, load_ring, open_record}; reseal of a plaintext-v0 UI key"
provides:
  - "p14::ui_auth::TokenUiAuth (impl ostro_ui::auth::UiAuth): signed, expiring, revocable, cluster-wide ostro_session cookies; encode/decode codec; ensure_key (plaintext-v0 CAS create-only)"
  - "UiScopeLayer token branch: handlers run inside PRINCIPAL.scope(<session principal>), Anonymous without a session; UiScopeLayer::principal_of"
  - "ostro-ui: UiPrincipal {scopes, token_id, name, tenant} (Default), UiReject::{login, unauthorized, signed_out}, UiAuth::{sessions, login, secure_cookies} (defaults), UiSession, LoginFuture, SESSION_COOKIE, LOGIN_PATH; routes GET/POST /ui/login, POST /ui/logout; viewer middleware"
  - "tests: lib p14::ui_auth (11), ostro-ui tests/p14_ui_auth.rs (6), ostro-server tests/p14_ui_e2e.rs (3)"
affects: [14-24, 14-25, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "HMAC-SHA256 via aws_lc_rs::hmac (sign / constant-time verify); base64url no-pad"
    - "resolve once per request in the outermost layer, hand the result to inner extractors through request extensions (no second HMAC/meta read)"
    - "askama path calls (crate::auth::viewer_*()) read a tokio task-local, so the layout shows the session without touching every page struct"
key-files:
  created:
    - crates/ostro-ui/src/login.rs
    - crates/ostro-ui/templates/ui/login.html
    - crates/ostro-ui/tests/p14_ui_auth.rs
    - crates/ostro-server/tests/p14_ui_e2e.rs
  modified:
    - crates/ostro-server/src/p14/ui_auth.rs
    - crates/ostro-server/src/p14/reseal.rs
    - crates/ostro-ui/src/auth.rs
    - crates/ostro-ui/src/lib.rs
    - crates/ostro-ui/src/pages.rs
    - crates/ostro-ui/templates/ui/layout.html
    - crates/ostro-ui/assets/app.css
    - crates/ostro-ui/tests/actions.rs
    - crates/ostro-ui/tests/inventory.rs
    - crates/ostro-server/tests/p14_reseal.rs
    - crates/ostro-server/tests/p14_skeleton.rs
key-decisions:
  - "The cookie carries only the token id + expiry under the cluster key; the bearer secret never reaches the browser. Every request re-reads the token record in the local meta view, so revoke/rotate/expiry end the session on the next request (no session store)"
  - "__ostro/ui-session-key is cluster-wide, so reseal now seals it only with an operator key (OSTRO_SEALING_KEY / [secrets] sealing_key_file), never the per-node dev key (orchestrator note: dev-key sealing broke cross-node reads); without one it stays plaintext-v0"
  - "Pages without a session: 303 /ui/login; htmx and mutating requests: 401 + HX-Redirect: /ui/login"
  - "Read-only principals: body.readonly hides every <form method=post> except .ui-keep (sign-out) via app.css (CSP forbids inline style); the server still enforces through the Operator extractor and the guard"
  - "Login failures answer one generic message; the reason only reaches the audit (14-18 decorator, auth.failure)"
requirements-completed: [SEC-02]
duration: ~3h
completed: 2026-11-27
---

# Phase 14 Plan 21: UI authentication Summary

**In token mode the embedded UI now uses the same credentials, scopes, tenant isolation and audit as the API. `POST /ui/login` exchanges an API bearer for an HMAC-signed, expiring `ostro_session` cookie that every node of the cluster accepts. Every `/ui` request re-validates the token behind the session, then runs inside `PRINCIPAL.scope` of that token, so the guarded `ControlPlane` applies the same rules to the UI as to REST. `[auth] mode = "none"` keeps the open Phase 11 UI.**

## Performance

- Duration: about 3 h
- Completed: 2026-11-27
- Tasks: 2 of 2 (+ 1 test commit)
- Files: 15 (4 created, 11 modified)

## Session format

```text
ostro_session = base64url("v1.<token_id>.<exp_ms>") "." base64url(HMAC-SHA256(K, "v1.<token_id>.<exp_ms>"))
K             = 32 bytes of meta secret __ostro/ui-session-key (plaintext-v0 or aes256gcm)
exp_ms        = min(now + [auth] ui_session_ttl (default 8 h), token.expires_at_ms)   (injected clock)
Set-Cookie    : ostro_session=<v>; Path=/ui; HttpOnly; SameSite=Strict; Max-Age=<ceil((exp-now)/1000)>[; Secure]
```

- Verification order: length cap (512), split, base64url, then **MAC (constant time, `aws_lc_rs::hmac::verify`)**. Only after the MAC passes are the version (`v1`), the `otk_` id format and `exp_ms > now` checked.
- After that, each request re-validates the token record in the **local meta view**: it must exist (revoke deletes it, and rotation with `revoke_old` deletes the old one), its expiry must not have passed, and its scopes must parse. No network call and no meta write.
- `Secure` is set when the client listener serves TLS: `[api.tls] mode` is files/acme/self-signed, or auto on a non-loopback listener. It is also set when the request URI is https.
- Key creation (B3): in token mode the meta leader runs `ensure_key` at start. A sign-in that finds no key also runs it, from any node, since proposals are forwarded. It creates 32 random bytes as `plaintext-v0` with CAS create-only, plus ext 117 created_at once the level allows it. 14-19's reseal turns this into aes256gcm when an operator key is configured. Readers use `reseal::open_record`, which accepts both formats, and cache the key per record version.

## Principal hand-off

1. `UiScopeLayer` (`p14::ui_scope_layer`) is the outermost layer of the `/ui` router. In mode none it uses `Principal::dev()`, unchanged from 14-01b. In token mode `TokenUiAuth::resolve(headers)` runs once and gives either `(Principal::token(id, tenant, scopes), UiPrincipal)` or `Principal::anonymous()`. The request then runs inside `ostro_api::auth::scope(principal, …)`, and the result is stored as a request extension.
2. The ostro-ui `viewer` middleware sits inside the Phase 11 Host guard and calls `UiAuth::principal`. `TokenUiAuth::principal` reads the extension, so verification is not repeated. The middleware stores the result for the `Reader` (read) / `Operator` (operate) extractors and sets a task-local for the layout: the session line (`name (otk_…) · tenant a` / `· system`), the sign-out form, and `class="readonly"` when the principal lacks operate.
3. Handlers call the guarded `ControlPlane` and slots, the instances 14-01b wired for the UI. Scope checks (14-15), tenant filtering with stripped names (14-16; the UI shows the names it receives and never re-qualifies them) and audit (14-18) apply exactly as they do for REST.

UI scopes come from the token's API scopes: `read` when `allows(Read)`, `operate` when `allows(Operate)`. `admin` allows both. A trigger- or deploy-only token gets neither, so its pages are 403.

## e2e results (local, debug build, slot 5)

`tests/p14_ui_e2e.rs` passes 3/3 on two runs at 68f668b and once at e498e46. One run takes 4-27 s, depending on how warm the build is.

- **Token mode, tenants a/b** (`token_mode_ui_enforces_sessions_scopes_tenants_and_audit`):
  - Signed out: `/ui/`, `/ui/runs`, `/ui/dlq` and `/ui/workflows` answer 303 to `/ui/login`, and a cancel POST answers 401.
  - Login: without the CSRF token it is 403 and no cookie is set. A bad bearer gets 401 "Sign-in failed" with no cookie. A good bearer gets 303 `/ui/` and a cookie with Path/HttpOnly/SameSite=Strict/Max-Age and no `Secure` (plain loopback).
  - a's `read` token: `/ui/runs` lists a's `signals` run and not b's. Names are unqualified (no `a/signals`). The page shows `· tenant a` and the readonly marker. a's run page is 200; b's run page is 404.
  - Cancel with the read token: 403. The run stays `Scheduled` and `cancel_reason` stays empty.
  - a's `read+operate` token: the cancel goes through, the run reaches `Cancelled`, and the audit journal holds `run.cancel` with `actor = <that token id>`, `actor_kind = token`, and the run as target. The operator's cancel of b's run is 404 and leaves b's run untouched.
  - `DELETE /v1/auth/tokens/<id>` (a's admin): the operator's next `/ui/runs` is a 303 to the login page.
  - Logout: without CSRF it is 403; with CSRF it is 303 and `ostro_session=; Max-Age=0`.
  - After shutdown the journal holds the `auth.failure` record (`denied`) from the bad-bearer sign-in.
- **Two-node static cluster, token mode** (`a_session_from_node_1_is_accepted_by_node_2`): a cookie from signing in on node 1 is accepted by node 2 (200, `· system`) without signing in there. A tampered cookie gets 303 on node 2.
- **Mode none** (`mode_none_keeps_the_open_phase_11_ui`): `/ui/runs` is 200 with no session line or readonly marker. `/ui/login` is a 303 to `/ui/`. No session key is created.

Unit tests (`p14::ui_auth`, 11):
- Codec: round trip; tampered payload or MAC, truncated MAC, garbage and an over-long value are refused; an unknown version and bad payloads are refused even with a valid MAC; expiry follows the injected ManualClock; another key rejects old cookies; scope mapping.
- Over a real `MetaHandle` + `build_ctx`: the first sign-in creates a 32-byte `plaintext-v0` key and `ensure_key` is create-only; bad bearers get a generic 401; a second `TokenUiAuth` instance (another node) accepts the cookie; `principal()` returns 303 for a page GET and 401 for htmx or POST.
- Session expiry; a session is capped at the token's expiry (`max_age = 10` for a token with 10 s left); revocation (record delete) gives `Revoked`; an expired token cannot sign in.
- A forged cookie (guessed key) gets `BadMac`; rotating the meta key refuses old cookies while new ones work.
- An aes256gcm key sealed with the operator key is read through the ring.
- `UiScopeLayer::principal_of` returns token, Anonymous or dev as expected.

ostro-ui `tests/p14_ui_auth.rs` (6, over a fake session UiAuth):
- Redirect vs 401 (+ `HX-Redirect`) on 7 pages and 3 mutations; the control plane is never reached and assets stay public.
- The login page carries the CSP and security headers plus `no-store`, mints the CSRF cookie, and its template has no inline script, style or handler.
- CSRF is required on login and logout; cross-site is refused; the login is never attempted.
- Cookie attributes with and without `Secure`; one generic failure message (the reason is not leaked); logout clears the cookie.
- Escaped session line; readonly marker; a reader's cancel is 403 with the run unchanged.
- AllowAll redirects the login routes to `/ui/`.

## Task Commits

1. **Task 1: token-backed UiAuth**: `8422930` (feat; includes the ostro-ui seam extensions it compiles against, and the reseal hunk)
2. **Task 2: UI login, logout and scope gating**: `68f668b` (feat)
3. **Test: failed sign-in audited**: `e498e46` (test)

## Files Created/Modified

- `crates/ostro-server/src/p14/ui_auth.rs`: codec, `TokenUiAuth`, `ensure_key`, leader key-creation task, token-mode `UiScopeLayer`, 11 unit tests.
- `crates/ostro-server/src/p14/reseal.rs`: the UI session key is sealed only with an operator key (see Deviations).
- `crates/ostro-ui/src/auth.rs`: the seam extensions listed under `provides`, plus the viewer middleware and task-local.
- `crates/ostro-ui/src/login.rs`, `templates/ui/login.html`: sign-in and sign-out.
- `crates/ostro-ui/src/lib.rs`: routes and `ROUTES` entries; the viewer layer sits inside the Host guard.
- `crates/ostro-ui/src/pages.rs`: a 303 rejection redirects to `/ui/login`; a 401 adds `HX-Redirect`.
- `crates/ostro-ui/templates/ui/layout.html`: nav hidden on the login page, session line + sign-out, readonly marker. Rendering is byte-identical for AllowAll, so the existing insta snapshots are unchanged.
- `crates/ostro-ui/assets/app.css`: login and session styles, plus the readonly rule.
- Tests: `crates/ostro-ui/tests/p14_ui_auth.rs`, `crates/ostro-server/tests/p14_ui_e2e.rs`, and the struct-literal updates in `crates/ostro-ui/tests/{actions,inventory}.rs`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] The UI session key was sealed with the per-node dev key**
- **Found during:** Task 1, from the orchestrator note after p14-fixups-2.
- **Issue:** `reseal::run` sealed `__ostro/ui-session-key` with `load_ring`, which includes the dev auto key that each node generates for itself. In a multi-node dev/test cluster the other nodes could not open it, so a cookie from one node could not verify on another. That breaks the must-have "accepted by every node".
- **Fix:** `run` now uses `cluster_store` (operator key only) for every `RESEAL_NOW` secret. Without an operator key the UI key stays `plaintext-v0`, like token-key and cluster-ca. Readers still open dev-key-sealed records from earlier runs through `load_ring`.
- **Files modified:** crates/ostro-server/src/p14/reseal.rs (outside files_modified; 14-19's file)
- **Commit:** 8422930

**2. [Rule 3 - Blocking] Two tests asserted the pre-14-21 stub behaviour**
- `p14_reseal::no_key_in_token_mode_warns_and_changes_nothing` asserted that the UI key is absent in token mode without a sealing key. B3 creates it as `plaintext-v0`, so the test now waits for it and asserts `plaintext-v0` with 32 bytes. The reseal assertions are unchanged.
- `p14_skeleton::ui_scope_layer_runs_handlers_as_dev_in_both_modes_until_14_21` became `…_as_dev_or_the_session_principal`: mode none gives Dev, and token mode without a session or context gives Anonymous.
- **Commit:** 68f668b. Neither is an oracle or crash checker.

**3. Files outside files_modified (minimal hunks)**
- `crates/ostro-ui/src/{auth,pages}.rs`: the seam itself (the plan lists `lib.rs` and `login.rs`). Rejection handling has to live where `UiError` is rendered.
- `crates/ostro-ui/assets/app.css`: CSP `style-src 'self'` rules out inline styles, so hiding the forms needs a stylesheet rule.
- `crates/ostro-ui/tests/{actions,inventory}.rs`: `UiPrincipal { scopes }` literals gain `..UiPrincipal::default()`.
- Templates live under `templates/ui/` (real path). The plan's verify greps `templates/login.html`; I ran the same grep on `templates/ui/login.html` (clean).

**Plan wording vs behaviour (no code change):**
- The plan's `UiReject::Login` / `UiReject::Unauthorized` are constructors on the existing struct (`UiReject::login`, `UiReject::unauthorized`, `UiReject::signed_out(parts, …)`), not enum variants. Changing the struct to an enum would have broken the public Phase 11 type.
- `ui_auth(ctx)` returns the UI's `TokenUiAuth`. `scope_layer` builds its own instance and passes its result over through request extensions. `P14Ctx` and `mod.rs` are frozen (14-01b), so no shared slot exists.
- `$(git rev-parse --git-dir)/p14-base` did not exist; I wrote it at worktree creation (fda88ac). The clock gate over `crates/ostro-server/src` since that base had no hits.

## Known gaps / for the orchestrator

- **Scope denials in the UI are not audited.** The `Operator` / `Reader` extractors (Phase 11 design) refuse a read-only principal before the guarded `ControlPlane` runs, so no `authz.denied` record is written for a UI 403. REST denials are audited. Fixing this means either auditing in ostro-ui's rejection path (needs an audit hook in `UiAuth`) or letting the call reach the guard. Candidate for 14-26.
- **Stale docs in frozen files:** `p14/mod.rs` still says "Stub (until 14-21): AllowAll" on `ui_auth` / `ui_scope_layer`. `p11.rs::wire` still warns "the embedded UI (/ui) and the REST API are unauthenticated until Phase 14" on a non-loopback listener, even in token mode. Both go to the fixup queue (14-01b-only files).
- **Pre-existing failure, not 14-21:** `ostro-server --test p14_authn server_bootstraps_once_across_restarts` fails on main. Line 476 asserts that a fresh cluster's bootstrap token has no ext 112 name (level 1), but c9875f9 (p14-fixups, user decision) makes fresh clusters start at the newest level. The other 8 tests in that binary pass. Fixup: update the assertion to `Some("bootstrap-admin")`.
- Error pages render with an empty CSRF token, so the sign-out button on an error page answers 403. This is cosmetic: signing out from any normal page works.
- No sign-in rate limit beyond the 14-18 coalesced `auth.failure` audit. This is the same exposure as bearer guessing on the API.
- `Secure` is derived from `[api.tls] mode`. A TLS-terminating proxy in front of a plain listener should set `[api.tls]` accordingly. This should be documented (14-25).

## Verification

- Task 1 verify: `cargo test -p ostro-server --locked --lib -- p14::ui_auth`: 11 passed (at 8422930's tree and again at 68f668b).
- Task 2 verify: `cargo test -p ostro-ui --locked --test p14_ui_auth`: 6 passed. `cargo test -p ostro-server --locked --test p14_ui_e2e -- --test-threads=1`: 3 passed. The template grep is clean on `templates/ui/login.html`. Clock gate: no hits.
- `cargo clippy -p ostro-ui -p ostro-server --locked --lib --tests -- -D warnings`: clean. `cargo fmt -p ostro-ui -p ostro-server -- --check`: clean.
- Regressions (local): ostro-ui all suites (lib 17, actions 17, hardening 7, inventory 13, p14_ui_auth 6, pages 13, security 10, vendored 2; snapshots unchanged). ostro-server: auth_scopes 2, p11_contract 10, p14_guard 8, p14_skeleton 7, p14_reseal 4, p14_system_sealing 2, p14_admin_api 7, audit_log 3, tenant_isolation 5. p14_authn: 8 of 9 pass; the failing one is the pre-existing failure above.
- **Plan-level gate: gate incomplete — cluster withdrawn; consolidated gate pending on dell-4.** Unverified at e498e46:
  - the lint group (workspace-wide clippy/doc/deny and the p12 grep gates beyond the two crates above);
  - the cargo-test shards beyond the suites listed above, including the ostro-server binaries not run here and the `ostro` crate;
  - the failpoint suites;
  - the musl build and the binary size step (no new dependencies; ostro-ui gains about 1 KB of templates);
  - polyglot/obuild;
  - Phase 12/13 tier C;
  - Phase 11 UI suites in both auth modes under CI. Mode none is covered by p14_guard, p11_contract and p14_ui_e2e; token mode by p14_ui_e2e.
- **human_needed (by design, no Node/browser here):** a real-browser pass in token mode:
  - the sign-in form submits and the cookie is stored (SameSite=Strict on same-origin navigation);
  - htmx follows `HX-Redirect` on a 401 fragment request (session expired mid-page);
  - the readonly stylesheet hides the action forms;
  - the sign-out button works;
  - nothing in the CSP report.

## For Later Plans

- 14-24 (auth matrix): `/ui/login` and `/ui/logout` are bearer-unauthenticated like the rest of `/ui/*`. Their credentials are the bearer in the form and the session cookie, both behind the Phase 11 CSRF rules. Every other `/ui` route requires a session in token mode.
- 14-25 (docs): the session format and lifetime (`[auth] ui_session_ttl`, capped at token expiry), "revoke ends UI sessions immediately", the operator-key rule for `__ostro/ui-session-key`, and `Secure` vs TLS mode / proxies.
- 14-26: audit UI scope denials (above). The fixup items above (mod.rs/p11.rs docs, p14_authn assertion).
- 14-27: no new ci-local step needed. `p14_ui_e2e` runs in the ostro-server cargo test shard and `p14_ui_auth` in ostro-ui's.

## Self-Check: PASSED

- Files exist: crates/ostro-server/src/p14/ui_auth.rs, crates/ostro-ui/src/login.rs, crates/ostro-ui/templates/ui/login.html, crates/ostro-ui/tests/p14_ui_auth.rs, crates/ostro-server/tests/p14_ui_e2e.rs.
- Commits on main: 8422930, 68f668b, e498e46.
