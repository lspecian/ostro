---
phase: 10
slug: developer-cli-test-kits
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 10: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 10-RESEARCH.md § Validation Architecture (commit a6197c1), mapped onto plans 10-01..10-17. The adopted decisions are in 10-DECISIONS.md (D-P10-1..D-P10-21), including the Phase 9 fold-in D-P9-28/29/30.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust test harness (`cargo test`) + insta snapshots; an axum fixture HTTP server for toolchain downloads (no network in the default suite); the spawned-binary harness (`crates/ostro/tests/common/p10.rs`, ready-line regex D-P10-9); pytest 8.4.2 via `uv run --no-project --python 3.12 --with pytest==8.4.2` (uv 0.7.15, D-P9-28); `node --test` on Node 24 (the 09-17 pin); `go test` (Go ≥ 1.23); a recording docker shim via `OSTRO_DOCKER` (Docker is absent locally). |
| **Toolchains under test** | uv 0.7.15 + uv-managed CPython 3.12 + componentize-py 0.25.1 **wheel** (D-P10-2, D-P9-30); Node 24 + `npm ci --ignore-scripts` of the bundled `sdks/scripts/toolchain` lock + jco 1.42.0 QuickJS flags (D-P10-3); rustup-init + Rust 1.100.0 wasm32-wasip2, zig 0.17.1 `cc` fallback (D-P10-4). All are pinned in `crates/ostro-build/toolchains.toml` and cached under `OSTRO_TOOLCHAIN_DIR` (tests: `target/ostro-toolchains-test`). |
| **Config** | 10-02 adds ci-local steps "clients python", "clients ts", "clients go" (`--no-go` skips visibly) and "ostro-build polyglot" (`--no-polyglot` skips visibly). 10-16 adds "musl binary size < 60 MB". Cargo feature `polyglot` on `ostro-build` and on the `ostro` binary crate gates every real toolchain download and every Python/TS build. `FAILPOINT_CRATES` is unchanged and must equal the ci.yml "Failpoint suites" step. ci.yml gains SHA-pinned `actions/setup-go` (polyglot job) and a `clean-machine` job. |
| **External services** | None locally besides network access to the upstream artefact hosts (GitHub releases, PyPI files, nodejs.org, static.rust-lang.org, ziglang.org, registry.npmjs.org) for polyglot/ignored tests. Docker is absent locally, so the container runs are a CI job and human_needed. **The microk8s cluster reachable via kubectl on this machine is never used.** |
| **Quick run command** | `cargo test -p ostro-build -p ostro-client -p ostro-cli --locked -- --test-threads=4` · `cargo test -p <touched crate> --locked` · `cd clients/python && uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q` · `node --test clients/ts/test/` · `(cd clients/go && go test ./...)` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (includes the client steps and the ostro-build polyglot step) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl + size gate).<br>2. `cargo deny check` (only the notify CC0-1.0 exception added) + absence of chrono/bincode/webpki-roots/dirs-sys/option-ext/openssl-sys.<br>3. `cargo test -p ostro-engine --test replay_corpus --locked` + `cargo test -p ostro-witcheck --locked` + `git diff <10-01 baseline> HEAD -- wit/ tests/replay-corpus` empty.<br>4. `cargo test -p ostro-build --locked --test build_rust_hermetic -- --ignored --nocapture` (network).<br>5. `cargo test -p ostro-server --locked --test time_skipping_auto -- --ignored race_gate_1000 --test-threads=1`.<br>6. `cargo test -p ostro --locked --test clean_machine -- --ignored local_all --nocapture --test-threads=1` (network; SC1 local mode).<br>7. `cargo test -p ostro --features polyglot --locked --test replay_cli --test template_tests -- --test-threads=1` (SC3, SC4).<br>8. `cargo test -p ostro-client --locked --test contract -- --nocapture` (four clients, none pending).<br>9. The Phase 8 clock grep gate, with `devclock.rs` allowed. |
| **Estimated runtime** | Quick: < 60 s per command (first real toolchain download 1–5 min per language, then cached). Full ci-local: about 20–30 min debug, including polyglot and clients. Clean-machine local run: cold up to 10 min per language (measured in 10-16), warm < 2 min. Race gate 1000: about 2–5 min. |

---

## Sampling Rate

- **After every task commit:**
  - the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for the touched Rust crates (`--all-features` for ostro-build/ostro-client).
  - Server time-skipping tasks (10-07, 10-10) also run `cargo test -p ostro-partition --locked` and the clock grep gate.
  - Client tasks also run the touched client's full suite.
- **After every plan wave / plan end:**
  - `bash scripts/ci-local.sh --no-musl`, recorded as the plan-end `<verification>`, not in task verifies;
  - from wave 3 on, also `cargo test -p ostro --locked --test cli_verbs -- --test-threads=1`: the CLI-06 snapshots must stay byte-identical.
- **Before `/gsd:verify-work`:** the 10-17 gate.
- **Max feedback latency:** 60 s for task-level checks with a warm toolchain cache. Hermetic downloads, the race gate and the clean-machine runs are gate level.
- **Verify-command rules:**
  - piped verify commands use `set -o pipefail`;
  - cargo test filters go after `--`;
  - never `|| true` (except inside grep-gate hit collection);
  - filtered cargo runs must report ≥ 1 passed (`test result: ok\. [1-9][0-9]* passed`);
  - pytest must report `N passed` with no failed/error;
  - node must report `# pass N≥1` and `# fail 0`;
  - go must print `ok`.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 10-01 gate (Phase 9 complete, Phase 8 clock seam) + names + decisions | RESERVATIONS.md, STATE.md |
| 2 | 10-02 deps/skeletons/stubs/scaffolds/ci; 10-03 API contract | 10-02: **Cargo files + deny.toml (only owner)**, ostro-build/ostro-client skeletons, `ostro-cli/src/{commands/mod.rs, lib.rs, output.rs, watch.rs}` + verb stubs, `crates/ostro/tests/common/p10.rs`, clients scaffolds, **ci-local.sh + ci.yml**. 10-03: **every ostro-api file + OpenAPI snapshot + parity.rs** (vs Phase 11), **server control/mod.rs** (2 mod lines + delegation), control/dev.rs + control/replay.rs stubs, `ostro-cli/src/client.rs` (3 methods). |
| 3 | 10-04 toolchain cache; 10-05 SDK bundle + templates + init; 10-06 ostro-client extraction + contract; 10-07 DevClock + manual skip; 10-08 replay | 10-04: ostro-build cache/fetch/extract/toolchain + toolchains.toml, commands/toolchain.rs. 10-05: ostro-build build.rs/sdk_bundle/project/templates + templates/**, commands/init.rs. 10-06: ostro-client src, **ostro-cli/src/client.rs → shim (vs Phase 11)**. 10-07: **server.rs, config.rs, lib.rs, control/mod.rs (dev field), partition clock.rs + dispatcher.rs, main.rs (`--time-skipping`)**, control/dev.rs, devclock.rs, guests/p10-time. 10-08: **engine lib.rs** (1 line), engine replay_check.rs + replay_corpus.rs, control/replay.rs, commands/replay.rs, guests/p10-replay-v1..3, crates/ostro/tests/fixtures/replay, docs/replay.md. |
| 4 | 10-09 build pipeline + Rust builder; 10-10 auto-skip idle detector; 10-11 Python/TS/Go clients | 10-09: ostro-build pipeline/stage/manifest/validate/lang/rust, commands/build.rs. 10-10: devtime.rs, **server.rs, lib.rs**, control/dev.rs, control/watch.rs, **dispatcher.rs, timers.rs**. 10-11: clients/** (client modules, route tables, e2e). |
| 5 | 10-12 Python/TS builders + --docker + parity; 10-13 deploy build-if-needed + dev --watch | 10-12: ostro-build lang/{python,ts,docker}, deploy/build-image/Dockerfile, ostro-conformance polyglot.rs (branch), docs/toolchains.md. 10-13: commands/deploy.rs, ostro-cli watch.rs, **main.rs (`--watch`)**. |
| 6 | 10-14 test kits; 10-15 completions + JSON contract | 10-14: ostro-client testing.rs (+1 lib.rs line), clients kits (pytest plugin, TS testing, Go ostrotest), template test files, ready_line.rs, template_tests.rs, docs/testing.md. 10-15: **main.rs (`completions`)**, ostro-cli output.rs (EXIT_CODES), completions/json_contract tests + snapshots, docs/cli-json.md. |
| 7 | 10-16 clean-machine e2e + CI job + size gate | scripts/clean-machine.sh, clean_machine.rs, **ci-local.sh, ci.yml**, docs/getting-started.md |
| 8 | 10-17 phase gate | RESERVATIONS flip, REQUIREMENTS ticks, VALIDATION, DECISIONS fold-in, STATE |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 10-01-01 | 01 | 1 | all | Phase 9 + Phase 8 seam gate, baseline, toolchains, reservations check + Phase 10 rows | `grep / doc checks` (Phase 9 SUMMARYs, VERIFICATION status, WIT cmp, seam grep, §11 rows) | ⬜ pending |
| 10-01-02 | 01 | 1 | all | Decisions, review/human todos, coordination in STATE.md | `grep / doc checks` | ⬜ pending |
| 10-02-01 | 02 | 2 | all | Deps, deny exception, ostro-build/ostro-client skeletons, fixture server | `cargo build --workspace`; `cargo deny check`; `cargo test -p ostro-build --test scaffold` | ⬜ pending |
| 10-02-02 | 02 | 2 | all | CLI verb stubs, exit 4, local_json, p10 helpers | `cargo test -p ostro-cli`; `cargo test -p ostro --test p10_scaffold`; `cargo test -p ostro --test cli_verbs` | ⬜ pending |
| 10-02-03 | 02 | 2 | SDK-06, SDK-07 | Client scaffolds + ci client steps | `pytest clients/python/tests`; `node --test clients/ts/test/`; `go test ./...`; `bash -n scripts/ci-local.sh` | ⬜ pending |
| 10-03-01 | 03 | 2 | SDK-06, SDK-07, CLI-07 | Domain types, trait defaults, REST, OpenAPI, fake, parity exclusion | `cargo test -p ostro-api --test dev_replay_surface`; `cargo test -p ostro-api` | ⬜ pending |
| 10-03-02 | 03 | 2 | SDK-06, SDK-07, CLI-07 | Server delegation stubs + client methods | `cargo test -p ostro-server --test dev_surface`; `cargo test -p ostro-cli` | ⬜ pending |
| 10-04-01 | 04 | 3 | CLI-04 | Cache, verified fetch, safe extract, lock, offline/mirror | `cargo test -p ostro-build --test toolchain_cache` | ⬜ pending |
| 10-04-02 | 04 | 3 | CLI-04 | toolchains.toml pins, toolchain/doctor verbs, network smoke | `cargo test -p ostro-build --lib -- toolchain`; `cargo test -p ostro-cli -- toolchain`; `cargo test -p ostro-build --features polyglot --test toolchain_network` | ⬜ pending |
| 10-05-01 | 05 | 3 | CLI-03 | Deterministic SDK bundle, project model | `cargo test -p ostro-build --test sdk_bundle --test project` | ⬜ pending |
| 10-05-02 | 05 | 3 | CLI-03 | Templates + `ostro init` snapshots | `cargo test -p ostro-cli --test init` | ⬜ pending |
| 10-06-01 | 06 | 3 | SDK-07 | Move the client into ostro-client, CLI shim | `cargo test -p ostro-client --lib`; `cargo test -p ostro --test cli_verbs`; snapshots unchanged | ⬜ pending |
| 10-06-02 | 06 | 3 | SDK-07 | Typed surface + four-client OpenAPI contract | `cargo test -p ostro-client --test contract --test typed` | ⬜ pending |
| 10-07-01 | 07 | 3 | SDK-06 | DevClock, subscribe_changes, dispatcher wake | `cargo test -p ostro-server --lib -- devclock`; `cargo test -p ostro-partition` | ⬜ pending |
| 10-07-02 | 07 | 3 | SDK-06 | Seam wiring, [dev] config, manual skip, `--time-skipping` | `cargo test -p ostro --test time_skipping_cli`; `cargo test -p ostro-server --test config` | ⬜ pending |
| 10-07-03 | 07 | 3 | SDK-06 | p10-time guest + manual time-skipping suite | `cargo test -p ostro-server --test time_skipping`; clock grep gate | ⬜ pending |
| 10-08-01 | 08 | 3 | CLI-07 | replay_check library; corpus test via it | `cargo test -p ostro-engine --test replay_corpus --test replay_check` | ⬜ pending |
| 10-08-02 | 08 | 3 | CLI-07 | Server replay endpoint + `ostro replay` (exit 4, --corpus) | `cargo test -p ostro-server --test replay_endpoint`; `cargo test -p ostro-cli -- replay` | ⬜ pending |
| 10-08-03 | 08 | 3 | CLI-07 | SC3 gate Rust/Python/TS + docs/replay.md | `cargo test -p ostro --test replay_cli`; `cargo test -p ostro --features polyglot --test replay_cli` | ⬜ pending |
| 10-09-01 | 09 | 4 | CLI-04 | Pipeline, input hash, stage, env scrub, validate, manifest | `cargo test -p ostro-build --test pipeline` | ⬜ pending |
| 10-09-02 | 09 | 4 | CLI-04 | Rust builder + `ostro build` | `cargo test -p ostro-build --test build_rust`; `cargo test -p ostro --test build_cli` | ⬜ pending |
| 10-10-01 | 10 | 4 | SDK-06 | Idle accessors, waiter count, DevTime, lease-safe manual skip | `cargo test -p ostro-partition`; `cargo test -p ostro-server --test time_skipping`; `cargo test -p ostro-server --lib -- devtime` | ⬜ pending |
| 10-10-02 | 10 | 4 | SDK-06 | Auto-skip wiring + race suite | `cargo test -p ostro-server --test time_skipping_auto`; clock grep gate | ⬜ pending |
| 10-11-01 | 11 | 4 | SDK-07 | Python client + e2e + uv build dry run | `pytest clients/python/tests`; `uv build` | ⬜ pending |
| 10-11-02 | 11 | 4 | SDK-07 | TS + Go clients + e2e + dry runs + contract | `node --test clients/ts/test/`; `npm pack --dry-run`; `go test ./...`; `cargo test -p ostro-client --test contract` | ⬜ pending |
| 10-12-01 | 12 | 5 | CLI-04 | Python builder (uv + wheel), pure deps | `cargo test -p ostro-build --features polyglot --test build_polyglot -- python` | ⬜ pending |
| 10-12-02 | 12 | 5 | CLI-04 | TS builder, parity, OSTRO_POLYGLOT_BUILDER=ostro | `cargo test -p ostro-build --features polyglot --test build_polyglot --test parity`; `OSTRO_POLYGLOT_BUILDER=ostro cargo test -p ostro-conformance --features polyglot --test polyglot_python --test polyglot_ts` | ⬜ pending |
| 10-12-03 | 12 | 5 | CLI-04 | --docker runner, template builds, docs/toolchains.md | `cargo test -p ostro-build --test docker_shim`; `cargo test -p ostro-build --features polyglot --test templates_build` | ⬜ pending |
| 10-13-01 | 13 | 5 | CLI-05 | Deploy build-if-needed, idempotent activation | `cargo test -p ostro --test deploy_build`; `cargo test -p ostro --test cli_verbs` | ⬜ pending |
| 10-13-02 | 13 | 5 | CLI-08 | `ostro dev --watch` | `cargo test -p ostro --test dev_watch` | ⬜ pending |
| 10-14-01 | 14 | 6 | SDK-06 | Ready-line contract + Rust kit | `cargo test -p ostro-client --features testing --test kit` | ⬜ pending |
| 10-14-02 | 14 | 6 | SDK-06, SDK-07 | Python kit + pytest plugin, TS kit | `pytest clients/python/tests`; `node --test clients/ts/test/` | ⬜ pending |
| 10-14-03 | 14 | 6 | SDK-06 | Go ostrotest, template tests, docs/testing.md | `go test ./...`; `cargo test -p ostro --test ready_line`; `cargo test -p ostro --features polyglot --test template_tests` | ⬜ pending |
| 10-15-01 | 15 | 6 | CLI-03..08 | `ostro completions` | `cargo test -p ostro --test completions` | ⬜ pending |
| 10-15-02 | 15 | 6 | CLI-03..08 | JSON envelope snapshots, EXIT_CODES, docs/cli-json.md | `cargo test -p ostro --test json_contract`; `cargo test -p ostro --test cli_verbs` | ⬜ pending |
| 10-16-01 | 16 | 7 | CLI-03, CLI-04, CLI-05 | clean-machine script + driver; `--local` run | `cargo test -p ostro --test clean_machine`; `bash scripts/clean-machine.sh all --local --no-cc` | ⬜ pending |
| 10-16-02 | 16 | 7 | CLI-03, CLI-04, CLI-05 | ci.yml clean-machine job, size gate, getting-started | `bash scripts/ci-local.sh` (with musl) | ⬜ pending |
| 10-17-01 | 17 | 8 | all | Full gate + gate-only suites + SC evidence | gate commands 1–9 | ⬜ pending |
| 10-17-02 | 17 | 8 | all | RESERVATIONS flip, REQUIREMENTS ticks, VALIDATION, DECISIONS, STATE | `grep / doc checks` | ⬜ pending |

---

## Requirement → Test Map

| Req ID | Behavior | Test | Plan |
|--------|----------|------|------|
| CLI-03 | init writes the expected tree per language; refuses non-empty; name validation | `cargo test -p ostro-cli --test init` | 10-05 |
| CLI-03 | generated projects build (all three) and their tests pass | `cargo test -p ostro-build --features polyglot --test templates_build`; `cargo test -p ostro --features polyglot --test template_tests` | 10-12, 10-14 |
| CLI-04 | toolchain fetch: sha mismatch fails, traversal/symlink rejected, atomic + lock, offline/mirror | `cargo test -p ostro-build --test toolchain_cache` | 10-04 |
| CLI-04 | pipeline: input-hash cache, validate rejects a forbidden import, manifest fields | `cargo test -p ostro-build --test pipeline --test build_rust` | 10-09 |
| CLI-04 | Python/TS hermetic builds; parity with build-guest.sh; conformance on ostro-built artefacts | `cargo test -p ostro-build --features polyglot --test build_polyglot --test parity`; `OSTRO_POLYGLOT_BUILDER=ostro cargo test -p ostro-conformance --features polyglot --test polyglot_python --test polyglot_ts` | 10-12 |
| CLI-04 | `--docker` wiring | `cargo test -p ostro-build --test docker_shim` (shim); ci.yml clean-machine (real) | 10-12, 10-16 |
| CLI-05 | deploy builds if stale, no-op on re-run, `.wasm` path unchanged, activation idempotent | `cargo test -p ostro --test deploy_build --test cli_verbs` | 10-13 |
| CLI-07 | replay_check verdicts; corpus via the library | `cargo test -p ostro-engine --test replay_corpus --test replay_check` | 10-08 |
| CLI-07 | `ostro replay` exits 4 on v2 and 0 on patched v3, in Rust/Python/TS | `cargo test -p ostro --features polyglot --test replay_cli` | 10-08 |
| CLI-08 | `dev --watch`: edit → new version; a broken edit keeps the old one; coalescing | `cargo test -p ostro --test dev_watch -- --test-threads=1` | 10-13 |
| SDK-06 | DevClock manual skips: sleep, backoff, run timeout, consistency, rejections | `cargo test -p ostro-server --test time_skipping`; `cargo test -p ostro --test time_skipping_cli` | 10-07 |
| SDK-06 | auto skip: 3-day sleep < 5 s, no waiter = no skip, signal-before-await, start-then-await (100 / 1000 iterations) | `cargo test -p ostro-server --test time_skipping_auto` (+ `--ignored race_gate_1000`) | 10-10 |
| SDK-06 | per-language kits: a multi-day sleep in seconds | `cargo test -p ostro-client --features testing --test kit`; pytest/node/go kit tests | 10-14 |
| SDK-07 | start/query/signal/result in TS/Python/Go/Rust; route coverage vs `ostro openapi` | `cargo test -p ostro-client --test contract --test typed`; client e2e suites | 10-06, 10-11 |
| SC1 | `init → deploy → run` with no pre-installed toolchain < 10 min per language | `cargo test -p ostro --test clean_machine -- --ignored local_all` (local); ci.yml `clean-machine` (containers) | 10-16 |
| NFR-2.1 | musl binary < 60 MB | ci-local "musl binary size < 60 MB" | 10-16 |
| — | ready-line contract; completions for 5 shells; local `--json` envelopes | `cargo test -p ostro --test ready_line --test completions --test json_contract` | 10-14, 10-15 |

---

## Wave 0 Requirements

- [ ] `crates/ostro-build/tests/common/fixture_server.rs`: an axum server serving generated archives (good, bad sha, traversal, absolute, symlink escape, internal symlink) (10-02)
- [ ] `crates/ostro/tests/common/p10.rs`: spawn helper for `dev` with extra args and the ready-line regex; `run_cli_in` with cwd/env (10-02)
- [ ] `clients/python/{pyproject.toml, tests/}`, `clients/ts/{package.json, test/}`, `clients/go/{go.mod, ostro/*_test.go}` scaffolds with green placeholder tests (10-02)
- [ ] ci-local + ci.yml: client steps, `--no-go`, SHA-pinned `actions/setup-go`, the ostro-build polyglot step (10-02)
- [ ] API contract (routes, fake, server stubs, client methods) before any implementation plan (10-03)
- [ ] Guests `guests/p10-time` (10-07) and `guests/p10-replay-v{1,2,3}` (10-08) for time-skipping and replay tests
- [ ] Cold-cache measurement: 10-12 (local warm/cold per language) and 10-16 (`clean-machine.sh --local`; containers in CI)

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SC1 in fresh containers (ubuntu:24.04, debian:12), hermetic and `--docker` | CLI-03/04/05 | Docker is absent on this machine; no git remote for CI (FOUND-05 pattern) | On a Docker host: `cargo zigbuild --release --target x86_64-unknown-linux-musl -p ostro`, then `bash scripts/clean-machine.sh all --image ubuntu:24.04 --ostro <musl bin>` and the same with `debian:12`; or push and confirm the ci.yml `clean-machine` job |
| macOS clean run (zig as host `cc`, darwin uv/node artefacts) | CLI-04 | No macOS machine/runner | On a fresh macOS without Xcode CLT: `ostro init hello --lang rust && cd hello && ostro deploy` (with `ostro dev` running); repeat for python/ts; record the timings |
| Publish `ostro-client` to PyPI, npm, crates.io; tag `clients/go/v0.1.0`; publish the `ostro-build` image and the Rust SDK crates | SDK-07, CLI-03 | No credentials, no remotes; publishing is irreversible (D-P10-21) | Review the 10-11 dry-run artefacts; `uv publish`, `npm publish`, `cargo publish` (after setting publish = true), `git tag clients/go/v0.1.0 && git push --tags`; `docker build -f deploy/build-image/Dockerfile` + push with the NOTICE |
| Remote CI run of the polyglot (incl. clients) and clean-machine jobs | all | No git remote yet | Push, confirm the jobs are green |
| User review of D-P10-11 (notify CC0-1.0 crate-scoped exception) and D-P10-6 (auto-skip trigger) | CLI-08, SDK-06 | Adopted defaults; the user may override | Read 10-DECISIONS.md, docs/testing.md and deny.toml |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags in verify commands (`dev --watch` tests drive the process and exit)
- [ ] Feedback latency < 60 s at task level (warm toolchain cache)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
