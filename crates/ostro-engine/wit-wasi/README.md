# wit-wasi: WASI 0.2.12 WIT for Ostro's deterministic shim

`deps/{io,clocks,random,cli}.wit` are copied from the `wasip2` crate, version
`1.0.4+wasi-0.2.12` (`wit/deps/<file>` in the published crate; upstream is the
WebAssembly WASI project, https://github.com/WebAssembly/WASI). Licence:
`Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` (see NOTICE). Each file's
status and every local modification is recorded in the repository-root
`VENDORED.md`, section "Third-party WIT (not under vendor/)".

Only one modification is made: `deps/cli.wit` drops its trailing `world imports`
and `world command` blocks, because they import `wasi:filesystem/*` and
`wasi:sockets/*`, which Ostro deliberately does not vendor (guests get no
filesystem or sockets). All interfaces are byte-identical to upstream.

`shim.wit` (Ostro-authored) is the world the host implements in
`src/host/wasi/`: the 18 interfaces listed in `ALLOWED_WASI` (`src/validate.rs`).

## Re-vendoring on a Rust toolchain bump

A new Rust toolchain may make `wasm32-wasip2` std emit a newer `wasi:*@0.2.N`
version, possibly with new functions. The host linker matches 0.2.x versions
semver-compatibly but per *function*, so a new function fails `instantiate_pre`
for every new guest (Research Pitfall 5).

1. Run `cargo test -p ostro-engine --test validate validate_wasi_probe_imports`.
   If the import snapshot changed, the toolchain changed the WASI surface.
2. Copy the four files from the matching `wasip2` crate
   (`~/.cargo/registry/src/*/wasip2-<ver>/wit/deps/`), re-apply the `cli.wit`
   world deletion, and bump the `@0.2.N` versions in `shim.wit`.
3. Implement any new functions in `src/host/wasi/`, update `VENDORED.md`, and
   accept the new snapshot (`cargo insta review`).
