---
phase: 11-run-operations-embedded-ui
plan: 03
subsystem: ui / build
tags: [cargo, askama, htmx, ostro-ui, contract]
requires:
  - 11-01 PHASE-11 GATE passed
provides:
  - askama =0.16.1 workspace pin (D-P11-14 decision: askama, not maud)
  - ostro-ui workspace member (UiConfig, UiDeps, router(), UiAuth/AllowAll/UiPrincipal/UiReject)
  - ostro-server -> ostro-ui dependency (anchored until p11.rs merges the router)
  - ostro dev-deps ostro-api (feature `fake`) + axum for 11-14
  - vendored htmx 2.0.11 with its sha256 in VENDORED.md and tests/vendored.rs
affects: [11-04, 11-14, 11-15, 11-16, 11-17, Phase 14 (UiAuth seam)]
tech-stack:
  added: [askama 0.16.1 (askama_macros, askama_derive, askama_parser 0.16.1), htmx 2.0.11 (vendored JS)]
  patterns: [dependency anchors (`use x as _;`) for deps declared before their consumer lands]
key-files:
  created:
    - crates/ostro-ui/Cargo.toml
    - crates/ostro-ui/src/lib.rs
    - crates/ostro-ui/src/auth.rs
    - crates/ostro-ui/src/pages.rs
    - crates/ostro-ui/src/{security,assets,timeline,actions,inventory}.rs
    - crates/ostro-ui/templates/ui/layout.html
    - crates/ostro-ui/assets/app.css
    - crates/ostro-ui/assets/app.js
    - crates/ostro-ui/assets/vendor/htmx-2.0.11.min.js
    - crates/ostro-ui/tests/vendored.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-server/Cargo.toml
    - crates/ostro-server/src/lib.rs
    - crates/ostro/Cargo.toml
    - VENDORED.md
decisions:
  - "D-P11-14 resolved: askama =0.16.1 (features derive, std, urlencode; no config). maud fallback not needed: deny green, no new duplicate (syn 3.0.10 already in the lock), builds and lints on Rust 1.100.0"
  - "The fake feature is `fake` (ostro-api), which exposes ostro_api::fake::FakeControlPlane"
  - "ostro-ui reuses the Phase 12 `tower` pin (0.5.3, util) for ServiceExt in tests; `http = \"1\"` added as a workspace pin (already in the lock)"
metrics:
  duration: "~45m"
  completed: 2026-11-22
---

# Phase 11 Plan 03: Contract b (Cargo, ostro-ui skeleton, htmx) Summary

askama `=0.16.1` is pinned (the maud fallback was not needed). The new `ostro-ui` crate is a workspace member with the `UiAuth` seam and an empty `router()`, and `ostro-server` depends on it. htmx 2.0.11 is vendored byte-exact, with its npm sha512 checked and its sha256 recorded and enforced by a test. The `ostro` binary crate can now serve the API fakes in its tests.

## Template engine decision (D-P11-14): askama

Probe: added `askama = { version = "=0.16.1", default-features = false, features = ["derive", "std", "urlencode"] }`. `cargo info askama@0.16.1` gives features `default = [config, derive, std, urlencode]`, `alloc`, `serde_json`, `code-in-doc`, `full`, `nightly-spans`. `config` is off (no basic-toml/serde in the macro; the default `templates/` dir is used). `urlencode` is on for the 11-15..17 link filters and reuses `percent-encoding`, which `url` already brings in. Resolved askama features: `alloc, derive, std, urlencode`.

- **Lock delta:** 4 packages added (`askama`, `askama_macros`, `askama_derive`, `askama_parser`, all 0.16.1), +66 lines in Cargo.lock, no version changes. Their dependencies (itoa, percent-encoding, memchr, proc-macro2, quote, rustc-hash, unicode-ident, winnow, syn 3.0.10) were already locked.
- **Duplicates:** `cargo tree -i syn@3.0.10` shows that syn 3 was already the dominant syn (async-trait, clap_derive, serde_derive, thiserror-impl, tokio-macros, utoipa-gen and others). askama_derive adds no new duplicate.
- **cargo deny check:** `advisories ok, bans ok, licenses ok, sources ok`, both before and after the rebase onto 12-01's Cargo changes. No deny.toml entry was needed, so deny.toml is unchanged.
- **MSRV:** askama declares rust-version 1.88. The build, clippy and tests all pass on the pinned toolchain, rustc 1.100.0.
- **Banned crates:** the `cargo tree -p ostro-ui -e normal` grep finds none of chrono, bincode, rust-embed, openssl, native-tls or governor. `scripts/forbidden-crates.sh` passes.
- ostro-ui normal dependency tree: 145 unique crates.

Later plans use askama as written.

## Crate skeleton

- `lib.rs`: `UiConfig { enabled, allowed_hosts, bound_addr, #[doc(hidden)] test_probe }` (Default + Clone), `UiDeps { cp, ext, cfg, auth }` (Clone), and `router(deps) -> axum::Router`. The router is empty, and it returns `Router::new()` early when `cfg.enabled` is false.
- `auth.rs`: `UiAuth` trait, `AllowAll` (grants `SCOPES = ["read", "operate"]`), `UiPrincipal { scopes }` with `require(scope)` (403 `UiReject` when the scope is missing), and `UiReject { status, message }`.
- Stubs with owner docs: `security`, `assets`, `timeline` (11-15), `actions` (11-16), `inventory` (11-17). `pages.rs` (11-15) holds one `#[derive(Template)]` probe over `templates/ui/layout.html`, so the engine and its auto-escaping are exercised now.
- `layout.html` follows the D-P11-15 shell: the htmx-config meta, `hx-history="false"` on body, external scripts and styles only, no inline script or style.
- Dependency anchors: `ostro-ui` anchors `sha2` and `getrandom` until 11-15 uses them, and `ostro-server/src/lib.rs` gained `mod phase11_dep_anchors { use ostro_ui as _; }`. Without these, cargo's unused-dependency lint warns and ci-local's warning-free build fails.

## Vendored htmx 2.0.11

- Tarball: `https://registry.npmjs.org/htmx.org/-/htmx.org-2.0.11.tgz`, sha512 `Thx/WtpeOQqSrqBCw/A1cwGJGg4UrVa3+sW0GmrM3p4gJgO89ecH4qtbnyzDDWFvBTqjnIMCgELTNt636dtamA==` (MATCH)
- `package/dist/htmx.min.js` → `crates/ostro-ui/assets/vendor/htmx-2.0.11.min.js`, 52182 bytes, sha256 `d6fdc75f204e6bdefa99b69bf1e6d4ac69b8a364f77929f45c13476b4000f717`. Licence 0BSD (package.json `"license": "0BSD"`).
- VENDORED.md has a new section, `## crates/ostro-ui/assets/vendor`. The Chronik rows are untouched.
- `tests/vendored.rs` parses the sha256 cell from the VENDORED.md row and compares it with the file. It also checks that the file starts with `var htmx`, is under 100 KB and contains `version:"2.0.11"`.

## Verification

- Task 1 verify (metadata --locked, deny, banned-crate grep, manifest greps): VERIFY1-OK
- `cargo test -p ostro-ui --locked`: lib 4 passed (disabled router 404s on every path, AllowAll grants read and operate, require rejects a missing scope, the template renders and escapes), vendored 2 passed
- `cargo build --workspace --locked`: no warnings (re-run after the rebase onto 6f75fb3)
- `cargo clippy -p ostro-ui -p ostro-server -p ostro --all-targets --locked -- -D warnings`: clean
- `cargo fmt --all --check`, `scripts/check-vendored.sh`, `scripts/forbidden-crates.sh`: pass
- Not run: the full `bash scripts/ci-local.sh` (shared machine, 5 concurrent agents). The steps this plan affects (fmt, clippy on the touched crates, warning-free workspace build, deny, vendored, forbidden crates) were run individually.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Dependency anchors for declared-but-unused dependencies**
- **Found during:** Task 2 (`cargo build --workspace`)
- **Issue:** cargo's unused-dependency lint warned on `ostro-server -> ostro-ui` and on `ostro-ui -> sha2/getrandom`, and ci-local's check_build fails on any warning.
- **Fix:** Anchor modules (`use x as _;`), following the existing 06-02/07-02/08-01 pattern. This adds one edit outside `files_modified`: `crates/ostro-server/src/lib.rs`, +6 lines (append-only `phase11_dep_anchors`).
- **Commit:** d8fdf73

**2. [Rule 3 - Blocking] Rebase onto 12-01's Cargo changes**
- 12-01 (6f75fb3) landed while this plan was building and added the same `tower = 0.5.3 (util)` pin, plus `ostro-cluster` in ostro-server. I kept their lines and dropped my duplicate `tower` pin (ostro-ui uses theirs). Cargo.lock merged cleanly, and `cargo metadata --locked`, deny, the workspace build and clippy were all re-run on the rebased tree.

**Workflow note:** I followed the coordinator brief rather than the plan's B1 paths: the scratchpad worktree `wt-11-03`, target slot-5, and a CAS update of main. Tasks 1 and 2 are one commit, as the plan allows, because the lockfile needs the ostro-ui skeleton to resolve. The tests were written alongside the skeleton rather than in a separate RED commit, because the skeleton and its tests are trivially coupled.

## Commits

- d8fdf73 build(11-03): ostro-ui crate, template engine pin and vendored htmx 2.0.11

## Self-Check: PASSED
- All created files exist in d8fdf73 (`git show --stat`, 20 files, +440).
- d8fdf73 is on main.
