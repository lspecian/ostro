---
phase: 10-developer-cli-test-kits
plan: 19
subsystem: cli-e2e
tags: [sc1, clean-machine, docker, musl, nfr-2.1, getting-started, ci]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-15 hermetic Python/TS builders + --docker runner (ostro-build:<version>, OSTRO_LINUX_BIN); 10-16 ostro deploy (build if needed); 10-17 frozen ready line + auto time skipping on result waits; 10-12 hermetic Rust + zig cc fallback (OSTRO_NO_CC, OSTRO_ZIG_SHIM_LOG)"
provides:
  - "scripts/clean-machine.sh <rust|python|ts|all> (--image IMG | --local) [--docker] [--no-cc] [--cache DIR] [--ostro PATH] [--timeout 600] [--print-plan]"
  - "crates/ostro/tests/clean_machine.rs: print_plan (default suite); ignored local_all, container_ubuntu_2404, container_debian_12"
  - "ci.yml job clean-machine (needs test): static musl ostro, clean-machine.sh all in ubuntu:24.04 and debian:12"
  - "ci-local step 'musl binary size < 60 MB' (check_musl_size) + gate-only command list"
  - "docs/getting-started.md"
affects: [10-20]
tech-stack:
  added: []
  patterns:
    - "one script, two layers: the outer layer prepares the environment (docker run / env -i), then re-invokes itself with --inner inside it, so the container and local runs execute identical steps"
    - "JSON checks with grep/sed only: the containers have no jq or python"
key-files:
  created:
    - scripts/clean-machine.sh
    - crates/ostro/tests/clean_machine.rs
    - docs/getting-started.md
  modified:
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
key-decisions:
  - "Containers run with --shm-size 1g: `ostro dev --ephemeral` stores its data under /dev/shm and Docker's 64 MB default made the Python deploy fail with ENOSPC (HTTP 503). Documented in getting-started; see Open Issues"
  - "`--image … --docker` mounts the host Docker socket and the (static) docker CLI, and puts the work dir, toolchain cache and ostro (OSTRO_LINUX_BIN) at identical host/container paths, because the daemon resolves bind mounts on the host; the work dir is chowned back afterwards"
  - "`--local --docker` is the macOS-style path (host with Docker, no toolchain): filtered PATH plus the docker CLI, OSTRO_LINUX_BIN = the given ostro"
  - "`--no-cc` = failing cc/gcc/clang/c++/g++/clang++ stubs first on PATH + OSTRO_NO_CC=1; the zig shim log proves the fallback ran"
  - "Size gate uses decimal MB (10^6 bytes), fails at >= 60,000,000 bytes"
  - "`signal` takes the payload as --payload (the plan's positional form does not exist)"
requirements-completed: [CLI-03, CLI-04, CLI-05]
duration: ~1h20m
completed: 2026-11-23
---

# Phase 10 Plan 19: SC1 clean-machine e2e, CI job, musl size gate, getting-started Summary

**`scripts/clean-machine.sh` runs `ostro init → ostro dev --ephemeral --time-skipping → ostro deploy → ostro run + approve → ostro result` with no language toolchain present. It passes for Rust, Python and TS locally (`--local --no-cc`) and, because Docker is available here, in fresh `ubuntu:24.04` and `debian:12` containers with only ca-certificates installed. Every language takes 6–75 s, against the 600 s budget. The static musl binary is 56.2 MB, under the 60 MB gate.**

## Commits
- 46aa05f test(10-19): clean-machine SC1 script (container and local modes) and driver
- 0e95072 test(10-19): clean-machine --shm-size, --cache, side-effect-free --print-plan
- 617b6d1 ci(10-19): clean-machine job, musl size gate, docs/getting-started.md

## CLEAN lines

All runs are on linux-x86_64 (WSL2), with other agents' builds running at the same time (load 12–16).

**`--local --no-cc`, cold** (fresh HOME and empty OSTRO_TOOLCHAIN_DIR, debug ostro). This is the Task 1 verify run:
```
CLEAN rust local OK total=57.8 doctor=0.0 init=0.0 dev=0.2 deploy=57.2 run=0.1
CLEAN python local OK total=16.5 doctor=0.0 init=0.0 dev=0.2 deploy=15.7 run=0.2
CLEAN ts local OK total=23.0 doctor=0.0 init=0.0 dev=0.2 deploy=22.3 run=0.1
```
The other cold runs (`local_all` and the `--cache` cold pass) gave Rust 58.5–64.5 s, Python 17.9–19.9 s and TS 23.5–24.6 s.

**`--local --no-cc --cache DIR`, warm** (the second run against the same cache):
```
CLEAN rust local OK total=24.9 doctor=0.0 init=0.0 dev=0.3 deploy=23.9 run=0.2
CLEAN python local OK total=16.4 doctor=0.0 init=0.0 dev=0.3 deploy=15.4 run=0.2
CLEAN ts local OK total=5.9 doctor=0.0 init=0.0 dev=0.3 deploy=5.0 run=0.2
```
Warm Rust still compiles the crates of a fresh project. Warm Python is dominated by componentize-py itself.

**Containers, cold** (`cargo test --test clean_machine -- --ignored container_`, static musl ostro, 2 passed in 263 s):
```
CLEAN rust ubuntu:24.04 OK total=46.7 doctor=0.0 init=0.0 dev=0.1 deploy=46.2 run=0.0
CLEAN python ubuntu:24.04 OK total=13.8 doctor=0.0 init=0.0 dev=0.1 deploy=13.3 run=0.0
CLEAN ts ubuntu:24.04 OK total=16.0 doctor=0.0 init=0.0 dev=0.2 deploy=15.5 run=0.0
CLEAN rust debian:12 OK total=74.4 doctor=0.0 init=0.0 dev=0.2 deploy=73.9 run=0.1
CLEAN python debian:12 OK total=13.3 doctor=0.0 init=0.0 dev=0.2 deploy=12.7 run=0.0
CLEAN ts debian:12 OK total=15.6 doctor=0.0 init=0.0 dev=0.1 deploy=15.1 run=0.1
```
No cc, cargo, rustup, node, npm, uv or go was present in either image (the script reports any it finds).

**`--docker` paths (TS):**
- `--local --docker`: `CLEAN ts local OK total=22.4`, including the first build of the `ostro-build:0.1.0` image.
- `--image debian:12 --docker` (socket mount): `CLEAN ts debian:12 OK total=17.3`, and manifest.json records the tool `docker (ostro-build:0.1.0)`.

## Zig path
Yes, it was exercised. Every Rust run logged `zig cc fallback exercised: 17 shim calls`:
- locally with `--no-cc` (`/usr/bin/cc` exists on this machine but was masked);
- in both containers naturally (no cc; cc-rs printed `failed to find tool "cc"` and the zig shim was used).

`local_all` asserts this.

## musl size (NFR-2.1)
`target/slot-2/x86_64-unknown-linux-musl/release/ostro` (cargo zigbuild --release, static, mimalloc):
```
size=56.2 MB (56234416 bytes) delta_vs_P4=+33.5 MB (baseline 22.7 MB)
```
This is under 60 MB, but with only 3.8 MB of headroom (see Open Issues).

## Action pins (all reused from the existing ci.yml jobs, none new)
| Action | SHA | Tag |
|---|---|---|
| actions/checkout | 3d3c42e5aac5ba805825da76410c181273ba90b1 | v7.0.1 |
| Swatinem/rust-cache | d1527c8f67ef9af357290475d4af4b0fab8f12e0 | v2.9.3 |
| mlugg/setup-zig | d1434d08867e3ee9daa34448df10607b98908d29 | v2.2.1 |
| taiki-e/install-action | 75a7c0f87bd666af3cdb75c5b9e9d03ab6044e6f | v2.90.6 |

actionlint (rhysd/actionlint:latest via Docker) passes on the new ci.yml. `FAILPOINT_CRATES` is unchanged, and ci-local's own `check_failpoint_sync` passes.

## Tests run
| Command | Result |
|---|---|
| `cargo test -p ostro --locked --test clean_machine -- --test-threads=1` | 1 passed (print_plan), 3 ignored |
| Task 1 verify (bash -n, build, print_plan, `clean-machine.sh all --local --no-cc`, 3 CLEAN lines) | exit 0 |
| `cargo test … --test clean_machine -- --ignored local_all` | the run itself was green, but the test failed on its first attempt because the summary repeated the CLEAN lines (6 counted). Fixed (indented summary + dedupe); the identical script run then passed as the Task 1 verify |
| `cargo test … --test clean_machine -- --ignored container_` | 2 passed (263 s) |
| `cargo clippy -p ostro --locked --test clean_machine -- -D warnings`; `cargo fmt --all --check` | clean |
| Task 2 verify | every check passes except the plan's FAILPOINT grep loop (see Deviations). The corrected loop (crate name + `--test <target>`) and `check_failpoint_sync` pass |
| `check_musl_size` (standalone) | size line above, rc 0 |
| `check_workflow` (actionlint) | ok |

**Not run:** the full `scripts/ci-local.sh` with musl (plan-level verification). It is the 10-20 gate, and the disk and load budget rules out a full workspace run per plan. The new step was exercised standalone against the same musl binary.

## Human-needed
1. **CI `clean-machine` job on GitHub.** No remote or push from here. After pushing, check the job's step summary for 6 CLEAN lines. The equivalent local commands, already run green here:
   ```sh
   cargo zigbuild --release --locked -p ostro --target x86_64-unknown-linux-musl
   bash scripts/clean-machine.sh all --image ubuntu:24.04 --ostro target/x86_64-unknown-linux-musl/release/ostro
   bash scripts/clean-machine.sh all --image debian:12   --ostro target/x86_64-unknown-linux-musl/release/ostro
   ```
2. **macOS run** (no Mac here), using a darwin `ostro` built on the Mac:
   ```sh
   bash scripts/clean-machine.sh all --local                 # native darwin toolchains
   OSTRO_LINUX_BIN=<linux-musl ostro> bash scripts/clean-machine.sh all --local --docker   # Docker Desktop
   ```
   - `--local` refuses to start if cargo/rustup/node/npm/uv/go are in /usr/bin:/bin. The macOS stock `/usr/bin/python3` shim is allowed.
   - For `--local --docker`, pass `--ostro <darwin ostro>`; `OSTRO_LINUX_BIN` goes into the build container.
   - `mktemp` and `date +%s%N` are GNU-specific: macOS bash may need coreutils, and this is untested.
3. **Publishing** the binary or images: none was done (the image is built locally and never pushed).

## Deviations from Plan
1. **[Rule 1 - Bug] `ostro signal <run> approve --payload '{"ok":true}'`.** The plan's positional payload is not part of the CLI contract (`--payload` flag).
2. **[Rule 3 - Blocking] `--shm-size 1g` on the containers.** Without it, the Python deploy in a default container failed: `storing the module failed: io: No space left on device (HTTP 503)`, because ephemeral data lives in /dev/shm (64 MB in Docker). This is an environment flag, not a product fix. See Open Issues.
3. **Docker was available, so the container modes were run for real**, not only through `--print-plan`, as the coordinator note says (the plan assumed no Docker). The script still checks `docker info` and fails with the human_needed message when Docker is missing. The container tests fail loudly, not silently.
4. **Additional script options:**
   - `--cache DIR` (local) for the warm timings;
   - `--image … --docker` implemented with socket + same-path mounts + chown;
   - the summary lines are indented so `grep -c '^CLEAN .* OK'` counts each run once.
5. **Plan verify defect:** the Task 2 loop `for c in $FAILPOINT_CRATES; grep -q "$c" ci.yml` cannot match `crate:target` entries (ci.yml spells them `-p crate … --test target`). This is also the case on main before this plan. I verified with ci-local's `check_failpoint_sync` and a corrected loop instead.
6. **Worktree, target dir and commits** follow the coordinator brief (scratchpad worktree, `target/slot-2`, temp index + CAS with ancestry assert) instead of `/tmp/ostro-wt-10-19`.
7. **The step timer** reports `doctor` and `dev` as their own fields, in addition to `init`, `deploy` and `run`.

## Open Issues
- **`ostro dev --ephemeral` uses /dev/shm unconditionally.** In any default Docker container (and in the kits' CI containers), a Python module of about 19 MB overflows the 64 MB shm. Options:
  - fall back to `$TMPDIR` when /dev/shm is small;
  - check free space at startup and print a clear message.

  Documented in getting-started ("Inside a container"). This needs a server-side owner (not in this plan's files).
- **Binary size headroom:** 56.2 MB against the 60 MB limit, +33.5 MB since Phase 4 (embedded SDK bundle, UI, Phase 7–12 features). The next large addition will trip the gate. Worth a size breakdown (cargo-bloat) before 10-20/11.
- **Workflow version bumps** (user decision pending, 10-16): the SC1 path covers only the first deploy, which works. Re-deploying an edited workflow still needs a source version bump (documented in getting-started "Changing a workflow").
- **macOS portability** of clean-machine.sh (`date +%s%N`, `mktemp` template, `readlink -f`, `stat -c` in ci-local) is untested.
- **Root-owned files:** on Linux hosts, `--image … --docker` writes as root into the host work dir. The script chowns it back. If the container is killed, run `docker run --rm -v DIR:/x debian:12 chown -R $(id -u):$(id -g) /x`.

## Self-Check: PASSED
- scripts/clean-machine.sh, crates/ostro/tests/clean_machine.rs and docs/getting-started.md (194 lines) are present on main.
- The ci.yml clean-machine job and the ci-local `check_musl_size` step are present on main.
- Commits 46aa05f, 0e95072 and 617b6d1 are on main.
