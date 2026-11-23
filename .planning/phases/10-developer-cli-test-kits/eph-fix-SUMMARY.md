---
plan: eph-fix
phase: 10-developer-cli-test-kits
type: bug fix (follow-up to the 10-19 open issue)
files:
  - crates/ostro-server/src/ephemeral.rs (new)
  - crates/ostro-server/src/server.rs
  - crates/ostro-server/src/lib.rs
  - crates/ostro-server/tests/ephemeral_base.rs (new)
  - crates/ostro-server/tests/bootstrap.rs
  - crates/ostro/tests/dev_startup.rs
  - crates/ostro/src/main.rs (--ephemeral help text)
  - docs/getting-started.md
  - scripts/clean-machine.sh (comment only)
completed: 2026-11-23
---

# eph-fix: ephemeral data dir picks /dev/shm only when it has room

## Bug
`ostro dev --ephemeral` (and every test kit that starts it) always created its data dir
on `/dev/shm`. Docker gives a container a 64 MB `/dev/shm` by default. A Python module
(~19 MB) plus its prepared/AOT artifact overflowed it: `storing the module failed: io: No
space left on device (HTTP 503)` (10-19, deviation 2).

## Design
`ostro_server::ephemeral`:
- `ShmProbe { exists, writable, free_bytes }` and a pure `choose(probe, min_free)`. The
  result is `Shm` or `TempDir(Fallback::{Missing, NotWritable, FreeUnknown, TooSmall})`.
- `probe(dir)` checks `is_dir`, creates and removes a probe tempfile, and reads
  `statvfs` (`f_bavail * f_frsize`, the space available to an unprivileged process).
- `create_dir()` creates the `ostro-*` TempDir on the chosen base (`std::env::temp_dir()`,
  i.e. `TMPDIR`, on fallback). It logs exactly one info line, for example
  `ephemeral data dir in /tmp (not /dev/shm: only 64 MiB free, 512 MiB needed)`. If creating
  the dir on shm fails after the probe passed, it logs a warn and falls back.
- `server.rs::ephemeral_dir` calls it. The ready line is unchanged: only DIR changes.
- There is no override knob: RESERVATIONS §11 reserves no name for one. `TMPDIR` moves the
  fallback.
- Test hook `#[doc(hidden)] set_shm_candidate_for_tests(Option<(PathBuf, u64)>)` is
  process-wide, used only by its own test binary. `would_use_shm()` lets tests assert
  the same rule.

## Threshold: 512 MiB
The default `[api] max_module_bytes` is 64 MiB. One module of that size plus its AOT
artifact (2–3x the wasm) is about 256 MiB. 512 MiB leaves the same again for the WAL, the
partition state stores, meta and a second module version. Normal Linux hosts pass, since
tmpfs `/dev/shm` defaults to half of RAM (this machine: 21 GiB free). Docker's 64 MB default,
or a `--shm-size` below 512m, falls back to disk. That is slower to start but correct. The
check runs once at startup: a tmpfs that fills up later can still return ENOSPC.

## Tests
- Unit tests (`ephemeral::tests`, 3):
  - the selection table over injected (exists, writable, free): boundary at exactly the minimum, Docker's 64 MB, unknown free space, and precedence missing > not writable > free;
  - the reason strings;
  - a probe of a real dir, which leaves no probe file behind.
- `ostro-server --test ephemeral_base`: a real writable candidate with threshold `u64::MAX`
  (forced small). It checks:
  - the data dir is under `temp_dir()`, not under the candidate;
  - the candidate is left empty;
  - a 62 MiB padded sdk-basic module (65.2 MB, larger than Docker's shm) deploys with 201 and is prepared;
  - the dir is removed on shutdown.
- `bootstrap` (5) and `ostro --test dev_startup` (4 + 1 ignored) are green. Both now assert
  `/dev/shm` only when `would_use_shm()`. The CLI-01 1 s budget is still enforced whenever
  shm is used, and the relaxed budget applies when it is not (e.g. macOS, small shm).
- `cargo clippy -p ostro-server -p ostro --all-targets -D warnings` is clean.
- Manual Docker check (ubuntu:24.04, default 64 MB shm, host certs mounted): the log line read
  `ephemeral data dir in /tmp (not /dev/shm: only 64 MiB free, 512 MiB needed)` and PUT of
  the 65 MB module returned 201 (data dir 73 MB). The same deploy overflowed the shm before.

## Docs / scripts
- getting-started "Inside a container": describes the rule, and says `--shm-size 1g` is
  optional (it gives a faster start on tmpfs).
- clean-machine.sh keeps `--shm-size 1g` so container timings stay comparable with host
  runs. Only the comment changed.

## Open issues
- The fsync-free ephemeral mode (for the macOS / disk-fallback startup budget) is a
  separate follow-up and out of scope here.
- `scripts/clean-machine.sh` was not re-run without `--shm-size`. The manual Docker deploy
  above covers the failing step.
