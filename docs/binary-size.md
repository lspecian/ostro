# Binary size (static musl `ostro`)

NFR-2.1 caps the static `x86_64-unknown-linux-musl` release binary at **60 MB**
(60,000,000 bytes, `scripts/ci-local.sh` step "musl binary size < 60 MB"). This page records
where the bytes go and what could be cut. It was measured on 2026-11-23 at `3b5b785` (after
Phase 10, Phase 11 and most of Phase 12). Option 1 below was applied by size1 the same day;
see "Applied: option 1" for the result. The breakdown tables are from before that change.

## Measurement

| Build | Size |
|---|---|
| Phase 4 baseline (04-20) | 22.7 MB |
| Phase 10 gate (10-19 / 10-20) | 56.2 MB |
| 2026-11-23, `3b5b785` | 59.09 MB (59,092,784 bytes): 0.9 MB of headroom |
| 2026-11-23, `73e53a7` (size1 baseline) | 59.09 MB (59,093,232 bytes) |
| 2026-11-23, size1 (option 1 applied) | 56.02 MB (56,023,344 bytes): 3.98 MB of headroom |
| 2026-11-25, `a735563` (before the Phase 13 wave below) | 57.05 MB (57,046,192 bytes) |
| 2026-11-25, `54364b8` (13-04/09/11/12/14/15 landed) | 60.16 MB (60,163,760 bytes): **gate failed** |
| 2026-11-25, `b2d5848` + musl-size code fix only | 59.32 MB (59,316,336 bytes) |
| 2026-11-25, `b2d5848` + musl-size (code + profile) | 57.87 MB (57,865,264 bytes): 2.13 MB of headroom |
| 2026-11-25, `7f60ead` (musl-size landed; + 13-16, p13-fixes) | 58.02 MB (58,017,968 bytes) |
| **2026-11-25, `7f60ead` + every cold crate at "z"** | **57.48 MB (57,482,608 bytes): 2.52 MB of headroom** |

The binary grew by 2.9 MB since the Phase 10 gate. The new code since then is Phase 11 and
Phase 12 (bulk/DLQ/UI routes, replication); no per-commit bisection was done. **The next feature of a few hundred kB may trip the gate.**

How the numbers were produced (no new tools; `cargo-bloat` is not installed and not needed):

```sh
# the gated binary, exactly as ci-local builds it
cargo zigbuild --release --locked -p ostro --target x86_64-unknown-linux-musl
size -A target/x86_64-unknown-linux-musl/release/ostro           # sections
# the same build with the symbol table kept, for attribution (relinks only)
CARGO_PROFILE_RELEASE_STRIP=none cargo zigbuild --release --locked -p ostro \
  --target x86_64-unknown-linux-musl
nm -S --size-sort -C --defined-only target/x86_64-unknown-linux-musl/release/ostro
```

The symbols were grouped by the first non-`core`/`alloc`/`std` crate path in their demangled
name. A generic instantiated for an Ostro type counts toward that Ostro crate. The release
profile already uses `lto = "fat"`, `codegen-units = 1` and `strip = "debuginfo"`, and the
shipped binary has no symbol table. So stripping, LTO and codegen units are already maxed out.

## Sections (gated binary)

| Section | MB | Notes |
|---|---:|---|
| `.text` | 48.23 | machine code |
| `.rodata` | 3.97 | constants, string tables, the embedded SDK bundle (0.76 MB tar), UI assets (72 kB) |
| `.eh_frame` + `.eh_frame_hdr` | 3.77 | unwind tables (`panic = "unwind"`) |
| `.gcc_except_table` | 2.03 | landing pads (`panic = "unwind"`) |
| `.data.rel.ro` | 1.05 | vtables, relocated constants |
| rest | 0.05 | |

About **5.8 MB (10%) is unwinding metadata**.

## Top contributors (`.text` + symbol-attributed `.rodata`)

| Group | MB | What |
|---|---:|---|
| Ostro crates (all `ostro_*`) | 12.9 | `ostro_server` 2.53, `ostro_api` 2.44, `ostro_partition` 1.19, `ostro_cli` 1.15, `ostro_bridge` 0.65, `ostro_backup` 0.63, `ostro_engine` 0.59, `ostro_proto` 0.57, `ostro_ui` 0.48, `ostro_cluster` 0.43, `ostro_build` 0.37, `ostro_wal` 0.33, `ostro_worker` 0.27 |
| Wasmtime + Cranelift | 9.3 | `cranelift_codegen` 2.59, `cranelift_assembler_x64` 1.87, `wasmtime_internal_cranelift` 0.90, `wasmtime` 0.87, `wasmparser` 0.82, `wasmtime_environ` 0.76, `regalloc2` 0.45. Of this, **~1.45 MB is the Pulley backend/interpreter**, and DWARF transform/gimli is 0.56 MB |
| `core`/`alloc`/`std` (non-generic) + drop glue + sort | 7.9 | drop glue alone is 2.77 MB, `slice::sort` instantiations 0.85 MB |
| Async/HTTP/gRPC stack | 5.4 | `tokio` 1.68, `h2` 1.54, `hyper` + `hyper_util` 1.17, `tonic` 0.42, `axum` 0.31, `http_body_util` 0.34 |
| serde (all instantiations) | 3.0 | spread across every crate |
| TLS/crypto | 2.1 | `aws_lc` C code 1.51 (incl. **0.70 MB of AVX-512 AES-GCM**), `rustls` 0.58 |
| clap | 1.3 | `ClientCommand::augment_subcommands` alone is 216 kB |
| kube client | 1.06 | `ostro-bridge` (Kubernetes secrets) |
| fjall / lsm-tree | 0.87 | storage engine |
| sqlx (Postgres/MySQL) | 0.87 | exactly-once SQL effects |
| utoipa (OpenAPI) | 0.91 | |
| regex | 0.69 | |
| opentelemetry | 0.38 | |

Largest single symbols: `ostro_api::rest::api_routes` 360 kB,
`cranelift ... constructor_simplify` 346 kB, the two `aws_lc ... aes_gcm_*_avx512` at 340 kB
each, `ostro_api::phase11::api_routes` 280 kB, `ostro_api::phase8::api_routes` 231 kB,
`ClientCommand::augment_subcommands` 216 kB, `ostro_api::rest::phase7_routes` 206 kB. The
`utoipa-axum` route builders (`*_routes`) total **1.25 MB**. Each one is a single function into
which every handler's `Handler` impl and schema registration is inlined.

## Applied: option 1 (size1, 2026-11-23)

The root `Cargo.toml` sets `[profile.release.package.<crate>] opt-level = "s"` for these
cold crates: `ostro-api`, `ostro-cli`, `ostro-client` (the CLI's REST client), `ostro-ui`,
`ostro-build`, `clap`, `clap_builder`, `utoipa`, `utoipa-axum`, `kube`, `kube-client`,
`kube-core`, `k8s-openapi` and `serde-saphyr` (kube's YAML). The proc-macro crates
(`clap_derive`, `utoipa-gen`) are not in the binary and are left alone. The hot path keeps
opt-level 3: wasmtime/cranelift, `ostro-engine`, `ostro-partition`, `ostro-log`, `ostro-wal`,
`ostro-server`, fjall/lsm-tree, tokio, hyper/h2, tonic, prost, rustls/aws-lc.

| Section | before (bytes) | after (bytes) | delta |
|---|---:|---:|---:|
| `.text` | 48,225,798 | 45,194,118 | -3.03 MB |
| `.gcc_except_table` | 2,029,396 | 1,953,188 | -0.08 MB |
| `.rodata` | 3,968,880 | 3,960,688 | -0.01 MB |
| `.eh_frame` | 3,360,408 | 3,396,448 | +0.04 MB |
| **file** | **59,093,232** | **56,023,344** | **-3.07 MB** |

Generic code from these crates that is instantiated in another crate (for example serde or
clap derive output expanded inside `ostro-server`) is compiled with that crate's opt-level, so
some cold bytes remain at opt-level 3.

Checks with the musl release binary (`OSTRO_BIN`):

- `dev_startup` including the ignored `cold_first_run_measurement`: cold first run had the
  same best time before and after, measured back to back under the same machine load (1559 ms
  vs 1554 ms; median 1583 vs 1585 ms). `--ephemeral` took 91 ms before and 94 ms after
  (budget 1 s).
- `e2e_dev`: 3/3 passed.
- `cargo test --release -p ostro-api -p ostro-cli -p ostro-ui`: all passed. This includes the
  OpenAPI snapshot, so every route is still registered.

Option 2 was not needed for the 3 MB headroom target and is still open.

## Applied: musl-size (2026-11-25)

Between `a735563` and `54364b8` the binary grew by 3.12 MB (57.05 -> 60.16 MB) without a single
new dependency or feature. Attribution (same method as above, old vs new symbol tables):
`ostro_server` +0.92 MB (13-12 routing, 13-09 join/joiner, 13-14 pre-warm, 13-15 module GC),
`ostro_api` +0.86 MB, drop glue +0.21 MB, `ostro_cluster` +0.13 MB, tonic/http-body generics
+0.2 MB. Two Phase 13 patterns were the bulk of it:

- **`GrpcControlPlane` (13-03), linked for the first time by 13-12's `PeerApi::grpc`.** Each of
  its 91 methods called a generated tonic client method, i.e. one
  `tonic::client::Grpc::unary::<Req, Resp, ProstCodec<..>>` instantiation (ready + encode +
  transport + decode futures) per method: ~10 kB each, ~0.9 MB.
- **13-12's `RouteCore::partition` / `meta_leader` / `unkeyed_start` / fan-out**, generic over the
  local and forwarding closures and the result type, monomorphised and inlined into every
  routed `ControlPlane` / extension method (~170 call sites, 10-20 kB each).

Fix, in order of size:

1. **Code (-1.04 MB at `54364b8`, 60.16 -> 59.13 MB).** `GrpcControlPlane` now encodes the
   request with prost, sends it through one non-generic `unary_raw` (a pass-through `Bytes`
   codec, the same path and `GrpcMethod` as the generated client, so the bytes on the wire are
   unchanged) and decodes the reply. The router's core functions are thin generic shims that
   box the two branches and erase the result type (`Box<dyn Any + Send>`) around one
   non-generic body each (`partition_dyn`, `meta_leader_dyn`, `unkeyed_start_dyn`,
   `fanout_partitions_dyn`, `all_nodes_dyn`, `first_hit_dyn`). Cost per routed call: two small
   boxes and a downcast, on a path that already boxes every `async_trait` future.
   `grpc_client_parity` (every client method against the in-process server) and the tier-B
   routing / handoff / version-gate / module-GC / membership scenarios pass unchanged.
2. **Profile, more cold dependencies (-0.45 MB).** opt-level "s" for kube's YAML / JSONPath
   stack (`granit-parser`, `annotate-snippets`, `jsonpath-rust`, `pest`), configuration loading
   (`config`, `toml`, `toml_parser`), the CLI's `clap_complete` and file watcher (`notify`,
   `notify-debouncer-full`), certificate issuance (`instant-acme`, `rcgen`, `yasna`), wasmtime's
   symbol demangling and wasm printer (`cpp_demangle`, `rustc-demangle`, `wasmprinter`: trap
   backtraces and debug output only), the Vault client (`vaultrs`, `rustify`) and the bridge's
   DNS resolver (`hickory-*`, I/O bound).
3. **Profile, opt-level "z" for `ostro-api` and `ostro-cli` (-0.72 MB, 58.58 -> 57.87 MB).**
   Both were already "s" (size1). REST latency A/B with the musl binary, see below.
4. **Profile, every other cold crate "s" -> "z" (-0.54 MB at `7f60ead`, 58.02 -> 57.48 MB):**
   `ostro-client`, `ostro-ui`, `ostro-build`, clap, utoipa, kube and the crates of step 2.
   None runs per request except utoipa's route / schema registration, which runs once when
   the routers are built.

The hot path (wasmtime/cranelift, `ostro-engine`, `ostro-partition`, `ostro-log`, `ostro-wal`,
`ostro-server`, `ostro-cluster`, fjall, tokio, hyper/h2, tonic, prost, rustls/aws-lc) keeps
opt-level 3.

A/B (`ostro dev --ephemeral`, one keep-alive client, 4 alternating rounds x 3000 requests per
endpoint, same pod, `ostro-api`/`ostro-cli` "s" vs "z"):

| Endpoint | "s" median / p90 / p99 (us) | "z" median / p90 / p99 (us) |
|---|---:|---:|
| `GET /v1/workflows` (200) | 153 / 197 / 247 | 155 / 185 / 235 |
| `GET /v1/runs?limit=20` (200) | 377 / 521 / 618 | 428 / 537 / 658 |
| `POST /v1/runs` (405 path) | 177 / 439 / 646 | 186 / 480 / 676 |

The listing call's median is ~50 us slower (13%); tails are within 3-6%, about the
round-to-round noise. These are admin calls well under a millisecond; a start or signal
commits through Raft and `fsync`, which `ostro-api` does not touch. With the musl "z" binary,
`dev_startup` including the ignored `cold_first_run_measurement` passes (cold first run
median 252 ms, `--ephemeral` 116 ms, warm restart 74 ms) and `e2e_dev` passes 3/3. An A/A run of two identical binaries
in the same harness (later, same node) differed by 2-8% per percentile, so most of the gap is
noise. With the all-"z" binary of step 4, `dev_startup` (5/5) and `e2e_dev` (3/3) pass again.
If a future API endpoint turns out CPU-bound, move `ostro-api` back to "s" (+0.6 MB) first.

## Reduction options

1. **(Applied, see above.) Size-optimise the cold crates (low risk, measure first).** Add
   `[profile.release.package.<crate>] opt-level = "s"` for code that is not on the hot path:
   `ostro-api` (route builders, OpenAPI), `ostro-cli`, `clap_builder`, `utoipa`, `kube-client`,
   `k8s-openapi`, `serde_saphyr`, `ostro-ui`, `ostro-build`. Fat LTO merges modules, but LLVM
   keeps the per-function `optsize` attribute, so most of the saving survives. Expected 1–3 MB.
   This is a profile change only: it does not affect the storage format or WIT. Validate with
   the perf suite (the API path is I/O-bound).
2. **Break up the route builders (low risk, ~0.5–1 MB).** Mark the per-handler registration
   helpers `#[inline(never)]`, or split `api_routes` / `phase11::api_routes` / `phase8::api_routes`
   / `phase7_routes` into several smaller functions. Each handler is then compiled once instead
   of being duplicated across the inlined builder bodies. This is mechanical, but every route
   must still be registered: the OpenAPI snapshot test catches a missing one.
3. **Feature-gate optional integrations (medium risk, ~1–2 MB).** Put the Kubernetes secrets
   provider (`kube`, 1.06 MB) and possibly the SQL effect drivers (`sqlx`, 0.87 MB) behind
   default-on cargo features, so a slim build can drop them. The default binary does not shrink
   unless a default is flipped, which is a product decision. This option only gives a lever.

Not recommended without a decision:

- **`panic = "abort"`** would remove most of the 5.8 MB of unwind tables. But
  `ostro-partition/src/machine.rs` relies on `catch_unwind` (it turns an apply-thread panic into
  a contained error), and the root `Cargo.toml` defers the panic strategy to a design decision.
  This is a behaviour change, not a size tweak.
- **Pulley (~1.45 MB)** cannot be turned off: `wasmtime` 50 enables
  `wasmtime-internal-cranelift/pulley` unconditionally whenever `cranelift` is on. Removing it
  needs an upstream change or a patched crate, and a vendored fork is against the pin policy.
- **Replacing aws-lc with ring** would save about 1 MB (the AVX-512 kernels alone are 0.7 MB).
  But it changes the TLS crypto provider for rustls, OTLP and kube, and loses the
  post-quantum key exchange. This is a security decision.
- **Dropping the wasmtime `addr2line` / `coredump` / `demangle` features** saves an estimated
  0.5–1 MB (gimli, DWARF transform), at the cost of file:line guest backtraces and coredumps in
  failure reports.
