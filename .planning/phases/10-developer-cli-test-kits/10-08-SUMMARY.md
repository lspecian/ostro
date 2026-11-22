---
phase: 10-developer-cli-test-kits
plan: 08
subsystem: build-scaffold
tags: [ostro-build, sdk-bundle, ostro-toml, templates, ostro-init, cli]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-02 ostro-build skeleton (sdk_bundle::SDK_BUNDLE, build.rs stub, build-deps toml/sha2/tar); 10-03 init verb flags + local_json envelope; 10-09 ostro-client crate (bundled)"
provides:
  - "sdk_bundle: BUNDLE (+ SDK_BUNDLE alias), bundle_id() (16 hex), bundle_sha256(), entries(), read_entry(), materialize(dest) (atomic replace, .bundle-id no-op)"
  - "project: Manifest/ProjectSection/BuildSection/DeploySection (deny_unknown_fields), Lang/ToolchainMode serde (lowercase) + as_str/FromStr/default_entry, Project, load(dir), validate_name, output_dir, ProjectError (is_usage)"
  - "templates: files(lang), output_path, substitute, render(lang, name, dest, force)"
  - "ostro init: commands::init::{run, execute}"
affects: [10-12, 10-15, 10-16, 10-17, 10-19]
tech-stack:
  added: []
  patterns: ["deterministic tar from build.rs (sorted, mtime 0, uid/gid 0, empty owner names, 0644/0755, GNU)", "materialise with extraction-time mtimes so cargo sees SDK changes", "template project = own cargo [workspace] with exclude = [\"target\"]"]
key-files:
  created:
    - crates/ostro-build/tests/sdk_bundle.rs
    - crates/ostro-build/tests/project.rs
    - crates/ostro-build/templates/{rust,python,ts}/**
    - crates/ostro-cli/tests/init.rs
    - crates/ostro-cli/tests/snapshots/init__tree_{rust,python,ts}.snap
  modified:
    - crates/ostro-build/build.rs
    - crates/ostro-build/src/{sdk_bundle,project,templates}.rs
    - crates/ostro-build/tests/scaffold.rs
    - crates/ostro-cli/src/commands/{init,mod}.rs
    - crates/ostro/tests/p10_scaffold.rs
key-decisions:
  - "Bundled ostro-client manifest drops [lints] AND [dev-dependencies] (dev-deps are never used by dependents; keeps offline resolution small) and sets publish = false; build fails on any remaining workspace = true"
  - "materialize extracts with preserve_mtime(false): files with mtime 0 would make cargo keep a stale build of the path-dependency SDK after a bundle change"
  - "Rust template carries `[workspace] exclude = [\"target\"]` so it is its own workspace root (works inside a parent cargo workspace) and the SDK copies are path deps, not members"
  - "Rust entry default = \"Cargo.toml\" (plan said \"crate\"); Python \"app\", TS \"src/index.ts\""
  - "project::load examines only the given dir (no ancestor walk); ostro.toml wins over detection; detected names are validated too"
  - "init: --lang required (exit 2 without it); Rust names may not start with a digit (cargo rule); default name = directory name, error hints --name"
  - "Client kits (clients/python/ostro_client, clients/ts/{package.json,src}) bundled only when present (10-04 not landed); a missing path cannot be rerun-if-changed without rerunning every build, so tests/sdk_bundle.rs fails on a stale incremental bundle (touch build.rs)"
duration: ~50m
completed: 2026-11-22
---

# Phase 10 Plan 08: SDK bundle, ostro.toml, templates, ostro init Summary

**`ostro init --lang {rust,python,ts}` turns an empty directory into a project with a `hello` workflow (step, durable 3-day sleep, `approve` signal), a kit-based test and the SDKs materialised offline from a 642 KB byte-reproducible tar embedded by build.rs; `ostro.toml` has a strict typed schema with language detection.**

## Accomplishments

### SDK bundle
- Size and id at `fe0f829`: **642,048 bytes, 56 entries, sha256 `d0b277f8c63448235a71f56d6f19a85d13979defde1fd9b5917abff2900bdd43`, id `d0b277f8c6344823`**. The id changes whenever a bundled source changes. Before 10-09's client move the bundle was 584,704 bytes.
- The bundle is reproducible: after touching the mtimes of every `sdks/ts/src` file and build.rs, the rebuilt tar had the same sha. Entries are sorted, and every header has mtime 0, uid/gid 0, empty owner names and mode 0644 (0755 if the source file is executable).
- File list (tar path ← repo path):
  - `rust/ostro-sdk/{Cargo.toml, src/{bindings,children,ctx,effects,error,lib,ops,registry,saga,step}.rs, wit/runtime.wit}` ← sdks/rust/ostro-sdk
  - `rust/ostro-sdk-macros/{Cargo.toml, src/lib.rs}`
  - `rust/ostro-client/{Cargo.toml (rewritten), src/**}` (5 entries at fe0f829)
  - `python/ostro/**` (15 entries: package modules, py.typed, wit/runtime.wit)
  - `ts/{package.json, rolldown.ostro.mjs, src/*.ts (16), wit/runtime.wit}`
  - `toolchain/{package.json, package-lock.json, rolldown.ostro.mjs}`
  - `clients/python/ostro_client/**` and `clients/ts/{package.json,src/**}` are added automatically once 10-04 creates them. There are none yet.
  - Excluded: target/, node_modules/, `__pycache__/`, tests/, *.pyc and hidden files.
- Tests check that the three bundled WIT copies equal `wit/ostro-runtime-0.1.0/runtime.wit` byte for byte, and that `toolchain/package-lock.json` equals `sdks/scripts/toolchain/package-lock.json`.

### Template trees (snapshots `crates/ostro-cli/tests/snapshots/init__tree_*.snap`)
- rust: `.gitignore, Cargo.toml, README.md, ostro.toml, rust-toolchain.toml, src/lib.rs, tests/workflow.rs, target/ostro/sdk/`
- python: `.gitignore, README.md, app.py, ostro.toml, pyproject.toml, tests/conftest.py, tests/test_workflow.py, target/ostro/sdk/`
- ts: `.gitignore, README.md, ostro.toml, package.json, src/index.ts, test/workflow.test.js, tsconfig.json, target/ostro/sdk/`

### SDK API spellings used
- **Rust:**
  - `use ostro_sdk::prelude::*`;
  - a `#[step] fn greet(name: &str) -> Result<String, StepError>` called as `greet(&input.name)?`;
  - `#[workflow(name = "hello", version = 1)] pub fn hello(ctx: &Ctx, input) -> Result<_, WorkflowError>`;
  - `ctx.sleep(Duration::from_secs(3*24*60*60)).wait()?`;
  - `ctx.wait_signal::<Approval>("approve", None).wait()?`;
  - `ostro_sdk::export!(HelloWorkflow)`.
- **Python:**
  - `@ostro.step def greet(name)` / `await greet(...)`;
  - `@ostro.workflow(name="hello") async def hello(ctx, inp)`;
  - `await ctx.sleep(3*24*60*60)`;
  - `await ctx.wait_signal("approve")`;
  - `ostro.export()` as the last line.
- **TS:**
  - `workflow({ name: "hello" }, async (ctx: Ctx, input) => ...)`;
  - `await step("greet", () => ...)`;
  - `await ctx.sleep(THREE_DAYS_MS)`;
  - `await ctx.waitSignal<Approval>("approve")`;
  - `export { workflows } from "ostro"`.
- **Kit calls in the template tests** follow D-P10-9 and the research example. They are not compiled or run until 10-17 owns them:
  - Rust: `TestEnv::start/build_and_deploy/start_run/signal/result(&run,"30s")/now_ms`, `run.accepted_at_ms`;
  - Python: the `ostro_env`/`ostro_project` fixtures;
  - TS: `TestEnv.start/buildAndDeploy/startRun/signal/result/nowMs/close`, imported from `../target/ostro/sdk/clients/ts/src/testing.js`.

### SDK README gaps (reported, sdks/ untouched)
- The plan's interface list says `ctx.step` for Rust and Python, but neither SDK has it. Rust has `run_step(name, &opts, body)` or the `#[step]` macro. Python has the `@ostro.step` decorator. Only TS has `ctx.step`, which aliases `step`. The Rust README's minimal example uses `run_step` and never shows `#[step]`/`#[workflow]` in code: the macro syntax is documented only in the ostro-sdk-macros crate docs and trybuild tests.
- The Rust README's dependency snippet uses `serde = "1"` and omits serde_json, while the example uses `json!` and `serde_json::Value`.

## Task Commits
1. `849af8f` feat(10-08): deterministic embedded SDK bundle and ostro.toml project model
2. `fe0f829` feat(10-08): rust/python/ts templates and ostro init

## Verification
- `cargo test -p ostro-build -p ostro-cli --locked`: all green:
  - ostro-build: lib 13, project 6, scaffold 6, sdk_bundle 6, toolchain_cache 13;
  - ostro-cli: lib 68, init 10, phase8_verbs 6.
- `cargo test -p ostro --test p10_scaffold`: 7 passed.
- `init.rs::rust_template_checks_offline`: `cargo check --offline` of the generated Rust project against the bundled SDK passes. It takes about 10 s and needed no `#[ignore]`. The dev-dependency ostro-client is resolved but not compiled.
- The `ostro` binary was smoke-tested by hand:
  - `ostro init demo --lang ts` exits 0 and prints the next steps;
  - a second run exits 2 (not empty);
  - `--name a/b` exits 2.
- `cargo clippy -p ostro-build -p ostro-cli -p ostro --all-targets -D warnings` is clean, `cargo fmt --all --check` passes, and `cargo check --workspace --all-targets` passes after the rebase onto 10-09.
- **Not run: the full `bash scripts/ci-local.sh --no-musl`** (about 3 h). The next ci-local gate (10-20 at the latest) covers it.

## Deviations from Plan

### Auto-fixed Issues
**1. [Rule 3 - Blocking] tests/scaffold.rs (10-02) asserted the 1024-byte empty stub tar.** That file is not in files_modified. The assertion now checks `> 1024`.

**2. [Rule 2 - Correctness] materialize uses extraction-time mtimes**, not the archive's 0. Otherwise cargo would keep a stale build of the path-dependency SDK after the bundle changed.

**3. [Rule 2] Bundled ostro-client also loses `[dev-dependencies]`.** The plan only mentioned `[lints]`.

**4. [Rule 2] The Rust template has `[workspace] exclude = ["target"]`.** This makes it its own workspace root and keeps the SDK copies as path deps. Rust names starting with a digit are rejected, because cargo cannot build them.

**5. API additions** beyond the interface: the `SDK_BUNDLE` alias (kept for 10-02's test), `BUNDLE_ID_FILE`, `bundle_sha256`, `entries`, `read_entry`, `Manifest::parse`, `ProjectSection::entry()`, `Lang::{ALL, as_str, default_entry}`, `templates::{output_path, substitute}` and `init::execute` (the testable core of `run`; ostro-cli has no binary).

## Open Issues
- 10-04 (client kits) has not landed. Once it does, an incremental build made earlier keeps a bundle without `clients/` until build.rs reruns. `bundle_has_client_kits_when_present` catches this; `touch crates/ostro-build/build.rs` fixes it. Clean builds and CI are unaffected.
- The template test files call the kit API as specified by D-P10-9. 10-17 owns adapting them to the final kit signatures.
- CLI-03 stays Pending in REQUIREMENTS: scaffolding is done, and the build half is 10-12/10-15.

## Self-Check: PASSED
- Commits 849af8f and fe0f829 are on main.
- build.rs, sdk_bundle.rs, project.rs, templates.rs, init.rs, the three snapshots and 21 template files exist.
