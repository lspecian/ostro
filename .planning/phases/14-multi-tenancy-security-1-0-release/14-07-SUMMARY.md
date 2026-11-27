---
phase: 14-multi-tenancy-security-1-0-release
plan: 07
subsystem: fuzzing (ostro-fuzz-targets, fuzz/ workspace, scripts, scheduled workflow)
tags: [fuzz, libfuzzer, cargo-fuzz, corpus, regression, wal, sec-05]
requires:
  - phase: 14-01b
    provides: "15 fuzz bins, fuzz workspace on nightly-2026-11-25, cargo-fuzz 0.13.2 pin in scripts/fetch-tools.sh"
  - phase: 14-03
    provides: "p14::tokens::parse / base32 / hash / verify (bearer_token target)"
  - phase: 14-05
    provides: "p14::signing::parse_signature / parse_trusted_key / verify_module (signature_parse target)"
provides:
  - "13 non-Wasm fuzz target bodies with target-specific invariants"
  - "237 seed files under fuzz/corpus/<13 targets>/; first regression under fuzz/regressions/wal_recovery_scan/"
  - "stable replay: cargo test -p ostro-fuzz-targets --test corpus_replay (one test per target, 15 + total check)"
  - "scripts/fuzz-smoke.sh (time-boxed fuzzing, summary table, --merge); fuzz/Cargo.lock"
  - ".github/workflows/fuzz.yml on branch wip/14-07-workflows (orchestrator applies)"
  - "crash fix: vendored WalRecord::from_bytes bounds every claimed length before allocating"
affects: [14-08, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "multi-mode targets: first input byte selects raw bytes vs a CRC-correct / arbitrary-structured encoding, so checks behind CRCs and decoding are reached"
    - "store/WAL scratch dirs on /dev/shm when present (fsync cost dominated: replay 38 s -> 2.4 s, wal_recovery_scan 6 -> 1051 exec/s)"
key-files:
  created:
    - crates/ostro-fuzz-targets/tests/corpus_replay.rs
    - scripts/fuzz-smoke.sh
    - fuzz/Cargo.lock
    - fuzz/corpus/<13 targets>/seed-*
    - fuzz/regressions/wal_recovery_scan/huge-canonical-data-len-c23aab099a35
  modified:
    - crates/ostro-fuzz-targets/src/{apply,codec,meta,ext,snapshot,wal,parsers}.rs
    - vendor/ostro-wal/src/record.rs (crash fix, 17 lines + regression test)
    - VENDORED.md (record.rs row)
key-decisions:
  - "fuzz/Cargo.lock is the workspace Cargo.lock resolved by nightly-2026-11-25 (`cargo fetch` in fuzz/), not a fresh generate-lockfile: same versions as the main lock plus libfuzzer-sys"
  - "fuzz-smoke writes corpus growth to fuzz/artifacts/corpus/<t> (gitignored) and reads the committed fuzz/corpus/<t> read-only; growth is committed only through --merge (coverage-adding inputs, 2 MiB cap)"
  - "apply_command runs the determinism re-run (second fresh store) on half the inputs (selector bit 1) and the digest every 8 steps; MAX_CMDS 24"
  - "TenantPatch is fuzzed parse-only: its Serialize writes absent keys as null (= clear), so it is not the inverse of Deserialize (reported, no request path serialises it)"
requirements-completed: []
duration: ~3h40m
completed: 2026-11-27
---

# Phase 14 Plan 07: Fuzzing infrastructure + codec/apply/parser targets Summary

**13 libFuzzer targets over the partition apply, the frozen journal/meta/ext/OSNP/WAL codecs and the REST/webhook/cron/egress/template/bearer/signature parsers, 237 seed inputs, a stable per-target corpus replay, a time-boxed `scripts/fuzz-smoke.sh` and a nightly fuzz workflow; the smoke found one real crash (a corrupt WAL record length made recovery allocate 3 GiB, or panic in the V1 path), fixed in the vendored WAL reader with a committed regression.**

SEC-05 is not ticked here (14-27 ticks Phase 14 requirements, D-P14-35).

**Gate incomplete — consolidated gate pending on dell-4.** All checks below ran in lane `14-07` on dell-4 (the user withdrew local builds mid-plan); the plan-level `ci-local` gate did not run. Unverified, see "Unverified" below.

## Commits

| Commit | Task | Content |
|---|---|---|
| ed98015 | 1 | feat: 13 target bodies, seed corpora, `tests/corpus_replay.rs`, WAL crash fix + regression |
| 0642342 | 2 | ci: `scripts/fuzz-smoke.sh`, `fuzz/Cargo.lock` |
| 5a065f5 | 2 | **branch `wip/14-07-workflows` only**: `.github/workflows/fuzz.yml` (not on main) |
| (this) | — | docs: SUMMARY, ROADMAP tick, STATE line |

## Targets and invariants

| Target | Input | Invariant (besides "never panics") |
|---|---|---|
| apply_command | selector; raw `env ‖ len u16 ‖ Command` frames, or `arbitrary`-built well-formed commands (start, grant, append + 9 Derived intents, timers, signals, cancel, terminate, retry, tokens, effects, pause/resume, leader changes, raw bytes) over 3 run ids | fresh store (partition 5/16) seeded with LeaderAnnounced + one run; undecodable bytes are `Rejected(Invalid(Decode))`; the only `Err` is the designed `NewerFormat` fail-stop; every step commits, cursor +1, digest computable; second fresh store gives the same digest + rolling hash (selector bit 1) |
| journal_event_codec | kind code or 0xfe/0xff (step/op recorded result) ‖ bytes | unknown kinds rejected; decoded body has the input kind; decode → encode → decode stable (byte-compared, NaN-safe); summary helpers |
| meta_decode | selector % 4: command / snapshot / result / command sequence | decoded values re-encode byte-identically and compare equal; `MetaState::apply` on empty and initialised states; snapshot round trip after every apply |
| ext_tlv | selector ‖ ext | well-formed ext re-encodes canonically, put/remove keep other entries byte for byte, put-then-remove = remove; malformed ext rejected by every mutator; 14 typed getters round-trip |
| snapshot_frames | raw OSNP stream, or valid header + CRC-correct frames (`RawFrame::encode`, lz4 flag) | frame reader and `verify_snapshot` agree on frame errors; caps before allocation (rss limit) |
| wal_recovery_scan | raw segment(s) (1 or 2, floor 0..15), or CRC-correct V2 records (gaps/overlaps/epoch steps) with an optional byte flip | report or error; recovery is idempotent (same bounds, epochs, records; nothing left to cut); epochs increasing |
| rest_json | JSON body | deserialised as 32 request DTOs: serialise → parse equals, re-serialise identical; 7 more parse-only (no Serialize, or TenantPatch) |
| webhook_verify | kind, tolerance, now, config, headers, body | meta mapping round-trips; a delivery verified with the kind's secret never verifies with another |
| cron_expr | expr ‖ `\n` ‖ tz | next > probe, latest ≤ probe, `fires_in` ≤ cap, strictly increasing, inside the window |
| egress_pattern | rule lines ‖ URL | admitted URL is http(s), no placeholder, origin = origin of the normalised URL, matching rule is a policy rule with the URL's scheme/port |
| secret_template | template string | placeholders ordered, non-overlapping, spell `{{secret:<valid name>}}`; expanded length exact; no values / unparsable → identity |
| bearer_token | bearer string | base32 canonical; parsed bearer re-assembles exactly, id valid, secret verifies against its hash, 32 bytes |
| signature_parse | signature bytes / key text | minisig normalisation stable; key survives the ext 115/116 meta round trip; verify against the fixture key gives evidence only for a verifying signature |

## Smoke (60 s per target, dell-4 lane, nightly-2026-11-25, cargo-fuzz 0.13.2, ASan + debug assertions)

First full run (before the fixes):

```
target                        execs   exec/s     cov    new  result
apply_command                    41        0    7640      0  TIMEOUT (seed-structured-3, 12 s: target too slow, not a product bug)
journal_event_codec          810745    13290    3370   4351  ok
meta_decode                  351554     5763    6419   4292  ok
ext_tlv                      479313     7857    1727    755  ok
snapshot_frames              225350     3694    1367   1698  ok
wal_recovery_scan               370        0     812     75  OOM malloc(3238068184)  -> real crash, fixed
rest_json                    136549     2238    6338   3520  ok
webhook_verify               465415     7629    1705   5307  ok
cron_expr                     49254      807    2880    741  ok
egress_pattern               312150     5117    4049   4818  ok
secret_template              384062     6296     569   1170  ok
bearer_token                 769372    12612     263   1059  ok
signature_parse              465009     7623    2086   4584  ok
evil_guest                        -        -       -      -  SKIP (stub)
smith_component                   -        -       -      -  SKIP (stub)
```

After the WAL fix and the apply_command cost reduction (rerun of the two affected targets, then the plan's Task 2 verify set at 20 s):

```
target                        execs   exec/s     cov    new  result
apply_command                   599        9   10234    160  ok
wal_recovery_scan             64113     1051     904    606  ok

apply_command                   210       10   10234      2  ok
meta_decode                   91146     4340    5828   2399  ok
bearer_token                 264233    12582     263    898  ok
signature_parse              156639     7459    1947   2414  ok
```

apply_command stays slow (~10 exec/s with ASan: one or two fresh fjall stores per input); it is meant for long runs.

## Crashes found

| Target | Input | Root cause | Fix |
|---|---|---|---|
| wal_recovery_scan | `fuzz/regressions/wal_recovery_scan/huge-canonical-data-len-c23aab099a35` (24 bytes, hand-minimised from the 411-byte libFuzzer artifact `oom-e5239e60…`) | `vendor/ostro-wal/src/record.rs` `WalRecord::from_bytes` allocated `vec![0; data_len]` from the record's own untrusted `data_len` (here 0xFFFFFFFF) before reading; the V1 path also did `with_capacity(headers_count as usize)` and `vec![0; value_len as usize]` from signed lengths (capacity-overflow panic). A corrupt WAL segment could abort node recovery instead of being classified `Unparseable`. | ed98015: `read_vec` fails exactly like `read_exact` (UnexpectedEof) when the claimed length exceeds the remaining bytes, before allocating; V1 headers no longer pre-allocate. Accepted inputs decode identically (frozen WAL format unchanged; recovery maps every from_bytes error to `Unparseable` as before). 17 changed lines; unit test `record::tests::corrupt_lengths_fail_without_allocating` (the V1 case panicked before the fix); VENDORED.md row updated (**P5**). |
| apply_command | (timeout on my own 500-byte structured seed) | not a product bug: two tempdir stores per input with fsync and a digest after every step under ASan | target only: tmpfs scratch dirs, digest every 8 steps, determinism re-run on half the inputs, 24 commands max |

Other findings (no crash, reported only):
- **TenantPatch `Serialize` is not the inverse of `Deserialize`**: absent fields serialise as `null`, which the server reads as "clear the quota". No request path serialises it today (ostro-client builds the JSON by hand), but anything that does would clear every unspecified quota. Fix candidate: `#[serde(skip_serializing_if = "Option::is_none")]` on the six fields of `ostro_api::phase14::TenantPatch` (14-01 contract file; fixup queue).
- **croner backward search from the far future is unbounded**: `CronSpec::latest_at_or_before(i64::MAX / 2)` did not return within minutes (`next_after` returns None at once). Production only passes clock readings (bounded by `end_at_ms`), so it is not reachable; the target probes only realistic instants. Worth a guard if bounds ever come from users unclamped.
- Nightly (only) build of the fuzz bins prints one rustc warning from ostro-server: `overflow evaluating the requirement AdminController: Sync` (the Phase 10 type cycle noted in p14-notes). cargo-fuzz does not build with `-D warnings`, so it is harmless here; not handled further.

## Corpus sizes (committed seeds)

| Target | Files | Bytes | Sources |
|---|---:|---:|---|
| apply_command | 44 | 17182 | every line of `ostro-types/tests/golden/commands.txt` after a LeaderAnnounced, all in order, a stale-epoch sequence, 6 structured |
| journal_event_codec | 41 | 5859 | `journal_v1.txt` events (27) + recorded results (14) |
| meta_decode | 32 | 6984 | `meta_commands.txt`, meta_v2 commands/snapshots, records wrapped in `Put`, a command sequence |
| ext_tlv | 10 | 4761 | meta_v2 P14 ext values as entries, all together, test tag, empty, non-increasing |
| snapshot_frames | 6 | 11482 | OSNP export (plain + lz4) of a store with the golden commands, framed-mode versions, truncated |
| wal_recovery_scan | 13 | 43785 | format corpus v0.1 segments (p0 valid, p1 misplaced), torn, two-segment, floor, framed |
| rest_json | 30 | 6361 | 15 DTO defaults (serde field names) + 15 hand-written bodies (deep nesting, lone surrogate, u64 overflow…) |
| webhook_verify | 8 | 5521 | GitHub, Slack, Standard Webhooks vendor vectors; Stripe and generic HMAC (hex, ts+base64, legacy SHA-1 RFC 2202) computed with Python hmac |
| cron_expr | 17 | 4387 | expressions from `ostro-triggers/tests/cron.rs` incl. DST zones, L/W/#, never-firing, path-traversal tz |
| egress_pattern | 11 | 4636 | host, wildcard, CIDR, IPv6, connection, userinfo, placeholder, trailing dot, IDN, encoded placeholder |
| secret_template | 10 | 4432 | valid, repeated, unterminated, bad/reserved/too-long/empty names, non-ASCII |
| bearer_token | 7 | 4508 | valid (fixed bytes), wrong prefix, worker token, short/non-canonical secret, base32, empty |
| signature_parse | 8 | 5674 | 14-05 fixtures (prehashed + legacy minisig, minisign.pub, key line), throwaway cosign P-256 PEM + DER sig + bundle (key discarded), keyless bundle |

Total 237 files (≥ 65), 72 KiB of content (byte column = `du -sb`, incl. directory entries). `evil_guest` / `smith_component` corpora are 14-08's.

## Verification (dell-4 lane 14-07)

- Task 1 verify: `cargo test -p ostro-fuzz-targets --locked --test corpus_replay` (16 passed, 1 ignored seed printer), corpus ≥ 65 files, `cargo clippy -p ostro-fuzz-targets --all-targets --locked -- -D warnings` clean. Lib unit tests 11 passed.
- Task 2 verify: `OSTRO_FUZZ_SECS=20 bash scripts/fuzz-smoke.sh apply_command meta_decode bearer_token signature_parse` exit 0; workflow YAML parses; every `uses:` pinned to a 40-hex SHA; `schedule` present; replay green. shellcheck (fetch-tools 0.11.0) clean on fuzz-smoke.sh.
- `cargo test -p ostro-wal` (record tests incl. the regression) and `ostro-log --test corruption` green.
- Final run at main 0642342 (lane 14-07, 968 s): Task 1 verify PASS; lib 11 passed; `cargo test -p ostro-wal` all green (74 lib incl. the regression + 4 test binaries); `scripts/check-vendored.sh` 5/5 PASS; `cargo fmt --all --check` clean; `cargo clippy --workspace --all-targets --locked -- -D warnings` clean; fuzz bins `cargo +nightly-2026-11-25 build --locked --release --bins` OK (only the known ostro-server nightly warning), `fuzz/Cargo.lock` unchanged by the build.

## Unverified (consolidated gate pending on dell-4)

- Full `scripts/ci-local.sh` at the final commit (workspace tests, all lint steps, musl, gates).
- Full 60 s smoke of all 13 targets at the final commit: the 11 unaffected targets ran at the pre-fix tree, apply_command and wal_recovery_scan were rerun after the fix.
- `.github/workflows/fuzz.yml` on GitHub (human_needed until pushed: schedule trigger, cache, artifact upload, 15-job matrix).
- Long fuzz runs (commands below).

## Long runs for the orchestrator (dell-4)

```sh
# 30 min per target, all 13 non-Wasm targets (~7 h incl. builds); add evil_guest smith_component after 14-08
/home/lspecian/.local/bin/ostro-dell4-ci --lane 14-07 --cmd 'OSTRO_FUZZ_SECS=1800 bash scripts/fuzz-smoke.sh apply_command journal_event_codec meta_decode ext_tlv snapshot_frames wal_recovery_scan rest_json webhook_verify cron_expr egress_pattern secret_template bearer_token signature_parse; echo EXIT=$?; for f in fuzz/artifacts/*/crash-* fuzz/artifacts/*/timeout-* fuzz/artifacts/*/oom-*; do [ -f "$f" ] && echo "ARTIFACT $f $(xxd -p "$f" | tr -d "\n" | head -c 20000)"; done' <worktree-at-main>
# Parallel variant (one lane per target, e.g. --lane fuzz-<t>): OSTRO_FUZZ_SECS=1800 bash scripts/fuzz-smoke.sh <t>
# Fold coverage-adding inputs into the committed corpus afterwards (same lane):
#   bash scripts/fuzz-smoke.sh --merge <targets...>   (2 MiB cap per target; review before committing)
# Reproduce / minimise an artifact:
#   cargo +nightly-2026-11-25 fuzz run --fuzz-dir fuzz <t> <artifact>
#   cargo +nightly-2026-11-25 fuzz tmin --fuzz-dir fuzz <t> <artifact>   -> fuzz/regressions/<t>/<desc>-<sha256 prefix>
```

## Workflow hunks

`.github/workflows/fuzz.yml` (new file; commit 5a065f5 on `wip/14-07-workflows`). `actions/cache` v6.1.0 and `actions/upload-artifact` v7.0.2 SHAs resolved with `git ls-remote` on 2026-11-27 (lightweight tags); checkout and rust-cache SHAs reused from ci.yml. No ci.yml change is needed: the replay test runs inside the existing `cargo test` steps.

```yaml
# Scheduled fuzzing (Phase 14, 14-07, SEC-05; D-P14-17): every fuzz target for 30 minutes per
# night on the dated nightly of fuzz/rust-toolchain.toml. The grown corpus is cached per
# target; crash / timeout / OOM inputs are uploaded as artifacts. Crashes are minimised
# (`cargo fuzz tmin`), committed under fuzz/regressions/<target>/ and fixed; the stable corpus
# replay (`cargo test -p ostro-fuzz-targets --test corpus_replay`) then guards them in CI.
# Third-party actions are pinned to full commit SHAs (resolved with git ls-remote).
name: fuzz

on:
  schedule:
    - cron: "17 2 * * *"
  workflow_dispatch:
    inputs:
      seconds:
        description: "Seconds per target"
        required: false
        default: "1800"

permissions:
  contents: read

concurrency:
  group: fuzz-${{ github.ref }}
  cancel-in-progress: false

env:
  CARGO_TERM_COLOR: always
  RUST_BACKTRACE: 1

jobs:
  fuzz:
    name: fuzz ${{ matrix.target }}
    runs-on: ubuntu-24.04
    timeout-minutes: 75
    strategy:
      fail-fast: false
      matrix:
        target:
          - apply_command
          - journal_event_codec
          - meta_decode
          - ext_tlv
          - snapshot_frames
          - wal_recovery_scan
          - rest_json
          - webhook_verify
          - cron_expr
          - egress_pattern
          - secret_template
          - bearer_token
          - signature_parse
          - evil_guest
          - smith_component
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - name: Install the fuzz nightly (fuzz/rust-toolchain.toml)
        run: |
          NIGHTLY=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' fuzz/rust-toolchain.toml)
          rustup toolchain install "$NIGHTLY" --profile minimal
          rustup run "$NIGHTLY" rustc --version
      - uses: Swatinem/rust-cache@d1527c8f67ef9af357290475d4af4b0fab8f12e0 # v2.9.3
        with:
          key: fuzz
          workspaces: fuzz -> target
      - name: Restore the grown corpus
        uses: actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0
        with:
          path: fuzz/artifacts/corpus/${{ matrix.target }}
          key: fuzz-corpus-${{ matrix.target }}-${{ github.run_id }}
          restore-keys: |
            fuzz-corpus-${{ matrix.target }}-
      - name: Fuzz (cargo-fuzz 0.13.2 via scripts/fetch-tools.sh)
        env:
          OSTRO_FUZZ_SECS: ${{ github.event.inputs.seconds || '1800' }}
        run: bash scripts/fuzz-smoke.sh "${{ matrix.target }}"
      - name: Upload crash inputs
        if: failure()
        uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: fuzz-artifacts-${{ matrix.target }}
          path: |
            fuzz/artifacts/${{ matrix.target }}/
            fuzz/artifacts/${{ matrix.target }}.log
          if-no-files-found: ignore
          retention-days: 30
```

## Deviations from Plan

1. **[Rule 1 - Bug] WAL reader allocation from untrusted lengths** (Task 2 smoke) — fixed in `vendor/ostro-wal/src/record.rs` (owning file, not owned by a wave-4 plan; 17 lines ≤ 20) + VENDORED.md row (provenance, not in files_modified). Commit ed98015.
2. **[Rule 3] Builds moved to dell-4 mid-plan** (orchestrator instruction): early unit tests ran locally in target/slot-3 (then deleted); everything else ran in dell-4 lane 14-07. The plan's `ostro-k8s-ci --parallel` gate is withdrawn; consolidated gate pending.
3. **fuzz/Cargo.lock** seeded from the workspace lock and resolved with `cargo +nightly-2026-11-25 fetch` instead of `generate-lockfile` (keeps the main lock's versions; `--locked` nightly build passes).
4. **Seeds written by a scratch Python script** (data only) plus the ignored test `print_generated_seeds` in corpus_replay.rs (OSNP exports, DTO defaults); file names `seed-<name>` (libFuzzer names its own additions by SHA-1).
5. **apply_command cost** (target only): tmpfs, digest every 8 steps, determinism re-run on selector bit 1, 24 commands.
6. **cron_expr probes** exclude far-future instants (see findings).
7. Commit ed98015 replaced my own just-landed 2941eb4 (message said 264 files instead of 237) by CAS while main still pointed at it; no other commit was affected.

## Self-Check

PASSED — files present: 7 target modules, tests/corpus_replay.rs, scripts/fuzz-smoke.sh (mode 100755), fuzz/Cargo.lock, 237 corpus files, 1 regression file; commits ed98015 and 0642342 are ancestors of main; 5a065f5 is the tip of `wip/14-07-workflows` and not on main.
