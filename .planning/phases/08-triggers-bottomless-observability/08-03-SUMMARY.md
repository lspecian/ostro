---
phase: 08-triggers-bottomless-observability
plan: 03
subsystem: storage
tags: [object-store, s3, rusty-s3, reqwest, localfs, conditional-put, multipart, s3s-fs, minio, cargo-deny]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-01 ostro-objstore skeleton (40047c3) and the rusty-s3/reqwest/jiff/futures pins
provides:
  - frozen object-safe ObjectStore trait (put, put_if_absent, put_if_match, get, get_range, head, list, delete, delete_many, put_multipart(BoxStream) with default impl, supports_conditional_put)
  - ObjectMeta, PutOutcome, Conditional, StoreOptions, Credentials, UrlStyle, TlsRoots, Error/Result
  - LocalFs (durable tmp+fsync+rename+dir fsync, atomic emulated conditional puts) and S3Store (rusty-s3 + reqwest/rustls, retries, multipart)
  - open_url(file:///abs | s3://bucket/prefix)
  - backend-agnostic conformance suite (tests/conformance) run on LocalFs, in-process s3s-fs and env-gated MinIO
  - CI MinIO docker-run step + OSTRO_TEST_S3_* env on linux-x86_64-gnu (shared Phase 6/8 spec)
affects: [08-13 uploader/fencing, 08-18 restore, 08-19 remote GC, 06-12 object connector (put_with/presign), 06 FX-05]

tech-stack:
  added: [s3s 0.17.0 (dev), s3s-fs 0.17.0 (dev), hyper-util server-auto (dev, existing 0.1.21)]
  patterns:
    - "Presign every rusty-s3 action per attempt (60 s), conditional headers inserted via headers_mut() before signing"
    - "Buffered Resp {status, headers, body} so body-read failures are retried like transport errors"
    - "One conformance module included by each backend's test binary (tests/conformance/mod.rs)"

key-files:
  created:
    - crates/ostro-objstore/src/error.rs
    - crates/ostro-objstore/src/local.rs
    - crates/ostro-objstore/src/s3.rs
    - crates/ostro-objstore/src/url.rs
    - crates/ostro-objstore/tests/conformance/mod.rs
    - crates/ostro-objstore/tests/localfs.rs
    - crates/ostro-objstore/tests/s3.rs
  modified:
    - crates/ostro-objstore/Cargo.toml
    - crates/ostro-objstore/src/lib.rs
    - Cargo.lock
    - .github/workflows/ci.yml

key-decisions:
  - "s3s-fs kept as the in-process S3 test server: cargo deny (exclude-dev = true) is green, chrono stays out of the normal graph, hyper/http stay single-version"
  - "tls_roots = webpki is a configuration error (bundled roots are CDLA, D-FX2); platform and file:<pem> work"
  - "LocalFs list returns e_tag None (hashing every listed object is too costly); get/head report the sha256 etag"
  - "409 on a conditional PUT maps to PreconditionFailed (AWS ConditionalRequestConflict)"
  - "Multipart: threshold and part size are StoreOptions (64 MiB / 16 MiB defaults, part >= 5 MiB)"
  - "Phase 8 (08-03) added the shared CI MinIO step first; Phase 6 reuses it unchanged"

requirements-completed: []  # BACK-01 transport only; BACK-01 completes with 08-13

duration: ~45 min
completed: 2026-11-20
---

# Phase 8 Plan 03: ostro-objstore Summary

**The frozen object-safe `ObjectStore` trait is implemented by two backends: a durable `LocalFs` and an S3 backend built on rusty-s3 0.12, reqwest and rustls. One conformance suite passes on LocalFs and on an in-process s3s-fs server, and an env-gated MinIO leg is wired into CI. cargo deny stays green with no exception.**

## Performance

- **Duration:** about 45 min
- **Completed:** 2026-11-20
- **Tasks:** 2 of 2, in one feature commit (see Deviations)
- **Files:** 11

## Spike results

### Spike 1: s3s-fs under cargo-deny (Open Question 1). Outcome: kept.

- `s3s = "0.17"` and `s3s-fs = "0.17"` are `[dev-dependencies]` of ostro-objstore. Both resolved to 0.17.0, and s3s-fs pulls chrono 0.4.45 into the dev graph only.
- Output of `cargo deny check` (stock deny.toml, `exclude-dev = true`; deny.toml unchanged):
  ```
  advisories ok, bans ok, licenses ok, sources ok
  ```
- `cargo tree --workspace -e normal --locked --prefix none` contains no `^chrono v` line. This was checked under `set -euo pipefail`, with the output captured first. `scripts/forbidden-crates.sh` also passes.
- hyper (1.12.0) and http (1.5.0) are still single-version in Cargo.lock, so s3s brings in no second major.
- There is no chrono exception and no new licence allowance. The lock gained about 40 dev-only packages (s3s-*, quick-xml, nom, phf, ...).
- The smoke test `s3s_fs_smoke_put_get` passes. It starts s3s-fs with `SimpleAuth` on 127.0.0.1:0 over hyper-util, then does a SigV4 PUT and GET through `S3Store`, using rusty-s3 signing and reqwest.
- s3s-fs **enforces `If-None-Match: *` and `If-Match`** on PutObject. It also **verifies SigV4**, and a wrong secret returns 403. So the in-process leg already proves that the signed conditional headers reach the server.

### Spike 2: conditional PUT on MinIO (Open Question 2). Outcome: pending the first CI run.

- WSL has no Docker. Locally, `conformance_minio` and `minio_rejects_mismatched_if_match` print `SKIP (human_needed): OSTRO_TEST_S3_URL unset ...` and pass.
- The CI step runs them on linux-x86_64-gnu. They cover the stale-etag and missing-object `If-Match` cases, `If-None-Match` twice, a race of 16 concurrent `put_if_absent` calls, and multipart.
- R2 stays human_needed.
- Whether rusty-s3 signs headers added via `headers_mut()` is **answered: yes**. The unit test `conditional_header_is_signed` asserts `X-Amz-SignedHeaders=host%3Bif-match`, and s3s-fs accepts the signed request and enforces the condition.

## Crate ownership

- **Created by Phase 8:** 08-01 (40047c3) made the skeleton. 08-03 now owns the trait and both backends.
- **No Phase 6 code existed** in the crate when this plan ran. At main 52f8536, `git log -- crates/ostro-objstore` showed only 40047c3. 06-02 (1203863) left the crate untouched and recorded "owned by Phase 8".
- So no Phase 6 items (presign, PutOpts, PresignOpts) were present to preserve. Nothing was deleted or renamed.
- **Files Phase 6 will also touch (06-12):** `crates/ostro-objstore/src/{lib,local,s3}.rs` and `tests/{localfs,s3}.rs`, plus its new `tests/presign.rs`. 06-12 adds `put_with`/`PutOpts` and `presign`, each with a default impl. It may put a LocalFs `<key>.ostro-meta.json` content-type sidecar in place; if so, it must also hide that sidecar from `LocalFs::list_blocking`.
- **Divergence check against the 06-12 text:** none in the trait. 06-12 names the S3 type `S3Store`, which matches. `ObjectMeta` already has `e_tag`, `version` and `last_modified`, so 06-12's "add if missing" step is a no-op.
- **Phase 6 hooks:** `S3Store::new(bucket, prefix, &StoreOptions)` and `LocalFs::new(root)` are public, so an object connection config can build a store directly without going through a URL.

## CI MinIO step

- **Phase 8 (08-03) added it first**, to the spec in 06-DECISIONS § "Shared MinIO / S3 test spec". It runs on the linux-x86_64-gnu leg, before `cargo test`, so the workspace test run also sees MinIO.
- It is a `docker run` of `quay.io/minio/minio server /data` with the user/password `ostro`/`ostrominio`.
- The health wait polls `curl -sf .../minio/health/ready`, retrying for up to 60 s.
- It creates the bucket `ostro-test` with a one-off `minio/mc` container. `MC_HOST_m=http://ostro:ostrominio@127.0.0.1:9000 ... mb -p m/ostro-test` is mc's env form of `mc alias set m ... && mc mb -p m/ostro-test`, so the container needs no shell.
- It exports the four `OSTRO_TEST_S3_*` variables through `$GITHUB_ENV`, which scopes them to that leg. The musl and macOS legs have no Docker MinIO and must not get them.
- A later step runs `cargo test -p ostro-objstore --test s3 --locked -- --nocapture`. It first runs `test -n "$OSTRO_TEST_S3_URL"`, so a missing env fails instead of skipping.
- **Phase 6 should reuse this unchanged.**
- actionlint is not installed locally and Docker is unavailable, so only a YAML parse (`yaml.safe_load`) was checked. ci-local's workflow step uses the same fallback.

## Task Commits

1. **Tasks 1 and 2 (spike, trait, backends, conformance, CI):** `7709fc6`

## Verification

- `cargo deny check`: `advisories ok, bans ok, licenses ok, sources ok`.
- No chrono in the normal tree (pipefail-safe check).
- `cargo build -p ostro-objstore -p ostro-bridge --all-targets --locked` is warning-free.
- `cargo clippy -p ostro-objstore --all-targets --locked -- -D warnings` is clean, and `cargo fmt --check` is clean.
- `cargo test -p ostro-objstore --locked`:
  - lib: **ok, 6 passed**. Covers the key grammar, credential redaction, the signed if-match header, backoff bounds, the http/webpki config errors and prefix normalisation.
  - `--test localfs`: **ok, 6 passed**. Covers the full conformance suite plus the 16-way put_if_absent race, a 70 MiB streamed multipart, path mapping with the hidden `.ostro-tmp`, the sha256 etag, directory pruning, quoted-etag matching, `open_url` dispatch and errors, and rejection of a relative root.
  - `--test s3`: **ok, 10 passed**. Covers the s3s-fs smoke test and full conformance (1005 objects across list pages and DeleteObjects batches, and a 12 MiB multipart in three parts), prefix prepend/strip, a mismatched If-Match / If-None-Match returning 412, a bad signature returning 403, and retries. The scripted retry server checks 503/429/500→200 = 4 requests, a cap of 1 + 5, 412/400/404 not retried, and a connect failure surfacing as Transport. The MinIO tests print SKIP.

## Deviations from Plan

**1. [Process] The two tasks share one commit.**
- The Task 1 smoke test is written against the Task 2 S3 backend. A dev-dependency-only commit would also have tripped cargo's unused-dependency lint.
- The TDD RED commit was folded in for the same reason. The suite was written alongside the implementation and run before committing.

**2. [Rule 2] `tls_roots = webpki` returns `Error::Config`.**
- reqwest 0.13 has no bundled-webpki feature, and the webpki root bundles are CDLA-Permissive-2.0, which D-FX2 excludes.
- `platform` (the default) and `file:<pem>` (`tls_certs_only`) work. The error message points at `file:`.
- 08-13 maps `BackupConfig.tls_roots` onto this.

**3. [Rule 1] The open-ended range is sent as `bytes=N-`.**
- s3s rejected `bytes=90-18446744073709551614` with 400 InvalidArgument.
- Ends above i64::MAX now use the open-ended form.

**4. [Rule 2] Hardening beyond the sketch.**
- `open_url` rejects userinfo credentials, queries and fragments.
- `LocalFs` reserves `.ostro-tmp` and prunes empty directories on delete.
- On a 200 response, `CompleteMultipartUpload` parses the body to detect the `<Error>` S3 can return with that status.
- The LocalFs write lock is per root and process-wide, which is stronger than a per-instance lock.

**5. [Note] Clock rule.**
- There are no new time reads for logic. rusty-s3 signs with its own `Timestamp::now()`, which is protocol and not logic.
- The retry backoff is a `tokio::time::sleep` marked `// clock-exempt: transport retry backoff`. 08-22's grep gate targets `now()` reads, not sleeps.
- The test-only `SystemTime::now()` used for the MinIO namespace is marked `clock-exempt`.

## Self-Check: PASSED

- All 7 created files and 4 modified files are present at 7709fc6.
- Commit 7709fc6 is on main, and `git show --stat main` shows 11 files: ci.yml, Cargo.lock and the objstore crate only.
