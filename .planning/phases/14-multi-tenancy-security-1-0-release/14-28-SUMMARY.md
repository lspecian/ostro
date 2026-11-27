---
phase: 14-multi-tenancy-security-1-0-release
plan: 28
subsystem: ostro-cli dev watcher (watch.rs + watch/{inotify,poll,debounce}.rs); ostro-server module upload (control/modules.rs); manifests/deny.toml; ostro-triggers webhook vectors
tags: [cli-08, cli-01, trig-01, inotify, file-watcher, deny, licences, dev-mode, write-once-versions, standard-webhooks]
requires:
  - phase: 14-01b
    provides: "p14::upload_gate / register_existing / activation_gate call sites in control/modules.rs"
  - phase: 10-16
    provides: "ostro dev --watch loop (WatchOptions, check_project, relevant, run_watch, messages, dirty-flag follow-up build)"
  - phase: 13-15
    provides: "module retirement + two-scan module GC (POST /v1/modules/gc)"
provides:
  - "ostro_cli::watch::debounce::{Debouncer, Event, Batch, spawn, MAX_LATENCY_MS}: quiet window + max latency, de-duplicated batches, never empty"
  - "ostro_cli::watch::inotify (Linux, libc): per-directory watches, subtree adds on create/move-in, moved-away subtrees unwatched, IN_Q_OVERFLOW rescan + Event::Rescan, IN_IGNORED drops the wd mapping, bounds-checked event parser"
  - "ostro_cli::watch::poll: 300 ms (mtime, size, inode) snapshots on a thread; default off Linux, fallback on Linux for EMFILE/ENOSPC"
  - "control/modules.rs dev_ephemeral() + rebind_dev(): ephemeral-dev in-place module replacement (D-P14-48)"
  - "tests: ostro-cli watch unit tests (9 new), dev_watch dir-rename case, ostro-server tests/dev_replace.rs (5)"
affects: [14-25, 14-26, 14-27]
tech-stack:
  added: ["libc (workspace pin, ostro-cli Linux-only)"]
  removed: ["notify 8.2.2", "notify-debouncer-full 0.7.0 (lock also drops notify-types, file-id, inotify, inotify-sys, kqueue, kqueue-sys, objc2-core-services and one windows-sys/windows-targets version family)"]
  patterns:
    - "watcher backend -> unbounded channel of debounce::Event -> debouncer task -> Batch channel -> unchanged build loop"
    - "unsafe confined to inotify.rs, one // SAFETY: per block; event-buffer parsing is safe code (from_ne_bytes over checked slices)"
    - "tokio AsyncFd::register (AsyncFd::new is deprecated in tokio 1.54) over a State that owns the OwnedFd"
key-files:
  created:
    - crates/ostro-cli/src/watch/debounce.rs
    - crates/ostro-cli/src/watch/inotify.rs
    - crates/ostro-cli/src/watch/poll.rs
    - crates/ostro-server/tests/dev_replace.rs
  modified:
    - crates/ostro-cli/src/watch.rs
    - crates/ostro-cli/src/lib.rs
    - crates/ostro-cli/Cargo.toml
    - Cargo.toml
    - Cargo.lock
    - deny.toml
    - crates/ostro/tests/dev_watch.rs
    - crates/ostro-server/src/control/modules.rs
    - crates/ostro-triggers/tests/webhook_verify.rs
    - docs/getting-started.md
key-decisions:
  - "Ephemeral-dev marker: TlsProfile::of(cp.config()) == Dev (secrets.auto_sealing_key, the derivation server.rs uses when no explicit profile is passed) AND config.node.ephemeral"
  - "Replacement response: UploadModuleResult lives in ostro-api (14-02-owned) and has no details/message field on success, so no `replaced`/`previous_module` fields were added; the response is the normal one (created = true for a new module, false for a stored one) and the replacement is visible in GET /v1/workflows (versions[].module_sha256) plus an info log line with previous_module. Adding the two fields needs an ostro-api change (orchestrator decision)"
  - "The rebinding is written after the Module record (the upload's commit point), so an interrupted upload never leaves a version bound to a sha without a Module record"
  - "Rebinding takes default_queue and run_timeout_ms from the new module, keeps created_at_ms and ext; the deployed module name is replaced by the upload's explicit name when it has one, else kept. Same sha + different explicit name still answers module_name_conflict everywhere (not a changed module)"
  - "An active version being rebound passes p14::activation_gate for the new sha first (signing policy), and its Active record is re-put with CAS (same content) so activation CAS tokens move"
  - "The Stripe test secret in webhook_verify.rs (whsec_test_...) was not split (the plan scopes D-P14-49 to the Standard Webhooks vector); the Stripe base64 test now reuses SW_SECRET instead of a second copy of that literal"
requirements-completed: []
duration: ~3h
completed: 2026-11-27
---

# Phase 14 Plan 28: Follow-ups (own watcher, dev-ephemeral replacement, test-key split) Summary

**`ostro dev --watch` runs on our own watcher (inotify over libc on Linux, polling elsewhere, one debouncer), so notify, notify-debouncer-full and the deny.toml CC0 exception are gone. `ostro dev --ephemeral` replaces a changed module under the same (name, version) for new runs only. The Standard Webhooks test key is now a split literal.**

## Performance

- Duration: ~3 h
- Completed: 2026-11-27
- Tasks: 2/2
- Files: 4 created, 10 modified

## Accomplishments

### Task 1: own file watcher (D-P14-47)

Watcher design as landed:

- **Pipeline:** backend -> `debounce::Event` (`Changed(path)` | `Rescan`) -> debouncer task (`quiet` = `--watch` debounce, default 250 ms; `max_latency` 2 s) -> `Batch { paths: BTreeSet, rescan }` -> the Phase 10 build loop. A non-empty batch sets `dirty`. Batches that arrive during a build collapse into one follow-up build. Public items (`DEBOUNCE_MS`, `WatchOptions`, `check_project`, `relevant`, `run_watch`) and every `ostro dev:` message are unchanged.
- **inotify (Linux):** `inotify_init1(IN_NONBLOCK | IN_CLOEXEC)`, read through `AsyncFd`. Watch mask `IN_CLOSE_WRITE | IN_CREATE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE_SELF | IN_MOVE_SELF | IN_ATTRIB`, plus `IN_ONLYDIR | IN_DONT_FOLLOW` on every add. The root and every directory `relevant()` accepts get one watch each. Ignored directories (`target/`, `.git/`, ...) are never watched, at any depth; symlinked dirs are not followed.
  - A directory created or moved in (`IN_CREATE`/`IN_MOVED_TO` + `IN_ISDIR`) gets watches for its whole subtree, and its files are emitted as changed. This closes the create-then-write race.
  - On `IN_MOVED_FROM` of a directory, the watches of its subtree are removed. A rename inside the tree re-adds them under the new path.
  - `IN_ATTRIB | IN_ISDIR` is dropped (the Phase 10 "directory-only metadata" filter). `IN_IGNORED` drops the wd mapping. Self events on the watched dir are skipped (the parent reports the entry).
  - `IN_Q_OVERFLOW`: rescan the tree (watches any missing directory) and emit one `Event::Rescan`.
  - The parser reads 16-byte headers plus `len` name bytes through checked `get()` slices. A truncated record is dropped and never read past.
  - Dropping the `Watcher` aborts its task, and the `OwnedFd` closes.
- **Fallback:** if `inotify_init1` or the initial walk fails with `EMFILE` or `ENOSPC`, the watcher prints `ostro dev: inotify unavailable (<err>); polling` and switches to polling. Any other setup error fails `run_watch` as before. A watch that fails at runtime on a new directory prints `ostro dev: cannot watch ...` and the loop goes on.
- **poll (all platforms; default off Linux):** a `(mtime, size, inode)` snapshot of the accepted tree every 300 ms on a named thread, which stops on drop. Directories count only when they appear or disappear.
- **Tests:** the debouncer core uses synthetic instants: burst -> 1 batch, continuous stream -> flush every `max_latency`, rescan carried, never empty. A tokio driver test covers the async loop. ostro-cli has no tokio `test-util`, and adding it would be a manifest change outside the exception, so there are no paused-clock tests. Other unit tests:
  - inotify parser bounds (every truncation of a 3-record buffer, and an oversized `len`);
  - overflow rescan through a synthetic `IN_Q_OVERFLOW` buffer, `IN_IGNORED`, and the directory-attrib filter;
  - poll snapshot/diff.
  
  `watch_inotify_backend_scenarios` and `watch_poll_backend_scenarios` (polling forced through `Kind::Poll`) each run the same scenarios on tempdirs: modify, create, delete, rename (one batch each); noise under `target/`, `.git/`, `*.swp`, `4913` (no batch); a new subdirectory with a file written immediately (the file is reported, then later edits are seen); a directory rename (the moved tree is still watched, with no stale paths); 200 rapid writes (1–2 batches). The `dev_watch.rs` suite is unchanged, plus the new case `watch_survives_a_directory_rename_inside_the_project`.

Manifest and deny diff:

- `Cargo.toml`: removed the `notify = "=8.2.2"` and `notify-debouncer-full = "=0.7.0"` pins with their comments, plus `[profile.release.package.notify]` and `[profile.release.package.notify-debouncer-full]`.
- `crates/ostro-cli/Cargo.toml`: removed `notify.workspace` and `notify-debouncer-full.workspace`; added `[target.'cfg(target_os = "linux")'.dependencies] libc.workspace = true`.
- `deny.toml`: removed the `{ allow = ["CC0-1.0"], crate = "notify" }` exception, so `exceptions = []`. The only CC0 left is the commented allow-list hint.
- `Cargo.lock`: 10 insertions, 186 deletions (notify, its debouncer and their transitive deps).
- `crates/ostro-cli/src/lib.rs`: dropped the `use notify as _; use notify_debouncer_full as _;` anchors (see Deviations).

### Task 2: dev-ephemeral module replacement (D-P14-48) + test key (D-P14-49)

- **Marker:** `dev_ephemeral(cp)` = `TlsProfile::of(cp.config()) == TlsProfile::Dev && cp.config().node.ephemeral`.
- **modules.rs:**
  - In the write-once check, a version bound to another sha goes to `rebind` when `dev_ephemeral`. Otherwise it is the unchanged `binding_conflict` 409.
  - After the Module record is stored, `rebind_dev()` CAS-puts each such WorkflowVersion: new sha, queue and timeout from the new module, `created_at_ms` and ext kept, the name replaced only by an explicit upload name.
  - An active version passes `activation_gate` first, and its `Active` record is re-put with CAS.
  - A lost CAS is accepted if the winner bound the same sha. Otherwise the answer is 503 "being replaced concurrently; retry".
  - The 14-01b hook lines are untouched, and the `upload_gate` < `s.module(&sha)` ordering check passes. There are no clock reads.
- **Replacement response shape:** the unchanged `UploadModuleResult { module, created }`. See key decisions: `replaced`/`previous_module` were not added because the type is in ostro-api. Observable signals are `GET /v1/workflows` and the server info log `ephemeral dev: replaced the module of an existing workflow version` (fields `workflow`, `version`, `previous_module`, `module`).
- **tests/dev_replace.rs (5 tests).** The ephemeral constructor is `ServerConfig::dev_defaults()` + `node.ephemeral = true` (what `ostro dev --ephemeral` loads), with the API on 127.0.0.1:0, a 50 ms hot wait and a 300 ms gc_grace.
  - `ephemeral_dev_replaced_module_serves_new_runs_old_runs_keep_theirs` (conf-rust A / conf-rust-v2 B, fresh-compile path):
    - after B, every shared workflow is bound to B and active stays 1;
    - parked `rs_conf_signals` r1 stays pinned to A and completes on A after two signals (each wake-up replays on A);
    - new `rs_conf_patch` r2 runs B (`["a2","b"]`);
    - re-uploading B is a no-op (`created = false`);
    - A re-uploaded (stored-module path) flips back; `rs_conf_ndet` r3 starts on A, B replaces A while r3 sleeps, and r3 completes `["a","b"]` pinned to A with no non-determinism (a replay on B would fail);
    - r4 runs B (`["b","a"]`).
  - `ephemeral_dev_replaced_module_is_kept_by_gc_while_a_run_needs_it` (patch-v1 / patch-v2): B is staged in another tenant so the replacement fits in the run's 2 s sleep. After the replacement, A has no version left, so it is retired (200). The GC round lists A as candidate + referenced, not deleted, while r is live. r completes `["A","B"]` on A. A later round deletes A, `patch-demo` stays bound to B, and a new run gives `["A","C","B"]`.
  - `persistent_dev_…`, `server_profile_…`, `server_profile_with_node_ephemeral_…`: 409 `conflict` with `existing_module_sha256 = A` and the "is already registered by module" message (the CLI turns it into the bump hint). B is not stored (404), every binding stays A, a new run runs A, and the parked run completes on A.
- **webhook_verify.rs:** `const SW_SECRET: &[u8] = concat!("whsec_", "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").as_bytes();` with the required comment. All 33 vectors/tests pass.
- **docs/getting-started.md "Changing a workflow":** a real server and a persistent dev keep write-once versions (bump the version). `ostro dev --ephemeral --watch` replaces in place for new runs, and started runs keep their module.

## Task Commits

1. Task 1: `644dc47` refactor(14-28): own file watcher replaces notify; drop the CC0 exception
2. Task 2: `613b1d1` feat(14-28): dev-ephemeral module replacement; split the Standard Webhooks test key

## Verification (local, target/slot-1, nice 10, 4 jobs)

- `cargo test -p ostro-cli --lib -- watch`: 12 passed (3 runs in a row).
- `cargo test -p ostro --test dev_watch -- --test-threads=1`: 6 passed, including the new dir-rename case (53.8 s).
- `cargo tree -p ostro -i notify`: package not found (not in the graph). No `notify` in Cargo.toml / ostro-cli Cargo.toml / deny.toml exceptions.
- `cargo deny check`: advisories ok, bans ok, licenses ok, sources ok.
- `cargo clippy -p ostro-cli --all-targets -D warnings` and `cargo clippy -p ostro-server -p ostro-triggers --all-targets -D warnings`: clean.
- `cargo test -p ostro-server --test dev_replace -- --test-threads=1`: 5 passed (3 runs).
- `cargo test -p ostro-triggers --test webhook_verify`: 33 passed. The `"whsec_"` and "public test vector" greps pass.
- 14-01b ordering check (`p14::upload_gate` before `s.module(&sha)`): ok. CLOCK_GATE_SCOPED on the server diff: no hits.
- Regression: `modules_api`, `p13_module_retire`, `p13_upload_gate` and `signed_modules` all pass; `--lib control::modules` passes.
- `cargo fmt --check` is clean for my files. It still reports `crates/ostro-server/src/p14/ui_auth.rs` (14-21's file, not touched here).
- Final-HEAD re-check after the CAS onto main (with 14-21's commits): `cargo check -p ostro-server -p ostro-cli -p ostro --tests` is clean.

**Plan-level gate: gate incomplete — consolidated gate pending on dell-4.** The cluster runner is withdrawn, so `ostro-k8s-ci --parallel` was not run. Unverified until the dell-4 consolidated gate:
- the full `scripts/ci-local.sh` (every step except the targeted tests above), including the workspace-wide `cargo test`, clippy/fmt over the whole workspace and the polyglot/obuild steps;
- the musl release build and the "musl binary size limit" step. **The musl size before/after was not measured**: notify was opt-level z and small, so a slight shrink is expected. Record the size from the dell-4 run;
- GitHub-CI-only legs (macOS: the polling backend is the default there and is only unit-tested on Linux through `Kind::Poll`; aarch64 musl).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] ostro-cli lib.rs unused-dep anchors**
- **Found during:** Task 1
- **Issue:** `crates/ostro-cli/src/lib.rs` still had `use notify as _; use notify_debouncer_full as _;` (10-02 anchors), so removing the dependencies broke the build. The file is not in files_modified.
- **Fix:** deleted the two anchor lines and their comment mention (as the p14 orchestrator notes direct for anchors).
- **Commit:** 644dc47

**2. [Rule 3 - Blocking] tokio 1.54 deprecates AsyncFd::new**
- **Fix:** `unsafe { AsyncFd::register(state) }` with a SAFETY comment (the State owns its OwnedFd and never replaces it).
- **Commit:** 644dc47

**3. [Plan shape] the modules.rs change is three small hunks, not one**
- The binding-check branch decides (`rebind` instead of `binding_conflict` in ephemeral dev). One call after the Module record runs the rebinding. The helper `dev_ephemeral` + `rebind_dev` is placed after `upload_module`, and there is a doc paragraph. The rebinding must follow the commit point, so it cannot sit inside the check closure. 14-01b's hook lines are untouched.

**4. [Rule 2 - Security] activation gate on rebinding an active version**
- Rebinding an active version makes a new sha serve the active version without going through activation. So `p14::activation_gate` (signing policy) is applied to the new sha first.

**5. [Plan detail] guest choice and response fields**
- conf-rust-v2 does not export `rs_conf_sig_api`, so the parked run uses `rs_conf_signals` (identical in both modules). The GC scenario moved to patch-v1/v2 because conf-rust A keeps other active workflows (B lacks them) and so cannot be retired.
- `replaced`/`previous_module` were not added (ostro-api type, no success details field), as recorded above.

## Issues Encountered

- None blocking. `cargo fmt --check` reports `crates/ostro-server/src/p14/ui_auth.rs` (14-21), which this plan did not touch.

## Next Phase Readiness

- 14-27: `cargo deny check` no longer needs the CC0 exception. The NFR/licence text can drop the notify note. Record the musl size from the dell-4 gate.
- Orchestrator decision: add `replaced: bool` + `previous_module: Option<String>` to `UploadModuleResult` (ostro-api, REST/gRPC/OpenAPI snapshot) if the plan's response shape is wanted. `rebind_dev` already knows both values.
- 14-25/14-26 docs: ephemeral-dev replacement exists only under `ostro dev --ephemeral`. A dev config that sets `[secrets] auto_sealing_key = false` is not detected as the dev profile, because the marker follows `TlsProfile::of`.

## Self-Check: PASSED

- FOUND: crates/ostro-cli/src/watch/{debounce,inotify,poll}.rs, crates/ostro-server/tests/dev_replace.rs
- FOUND: 644dc47, 613b1d1 on main
