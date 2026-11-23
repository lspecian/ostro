//! Rust build step: `cargo build --release --target wasm32-wasip2` with the system rustup
//! or a hermetic rustup-init (D-P10-4), and a zig `cc` shim as the host linker when there
//! is no host C compiler.
//!
//! - The build runs in the project dir (the cargo path dependencies point at
//!   `target/ostro/sdk/rust/…`), with `CARGO_TARGET_DIR=<root>/target/ostro/cargo`, so the
//!   cargo build dir stays inside ostro's output dir. `--locked` is passed only when
//!   `Cargo.lock` exists.
//! - **system**: `rustup` + `cargo` from the host `PATH` (the rustup proxy honours
//!   `rust-toolchain.toml`; `RUSTUP_HOME` / `CARGO_HOME` pass through when set).
//! - **hermetic**: the pinned rustup-init installs rustup into the cache
//!   (`state/rustup`, `state/cargo`) once, the project's toolchain channel (default
//!   [`RUST_CHANNEL`]) and targets are installed under the cache lock, and the build uses
//!   that cargo. `OSTRO_TOOLCHAIN_MIRROR` also sets `RUSTUP_DIST_SERVER=<mirror>/rustup` and
//!   `RUSTUP_UPDATE_ROOT=<mirror>/rustup/rustup` (W6).
//! - The environment is the pipeline's scrubbed one (`env_clear`), so `RUSTFLAGS`,
//!   `CARGO_ENCODED_RUSTFLAGS`, `RUSTC_WRAPPER` and `CARGO_BUILD_*` are never inherited.
//!   Host pass-through: `CARGO_NET_OFFLINE` and the proxy variables.
//!
//! Owner: 10-12.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::pipeline::{BuildError, HostEnv, LangStep, StepCtx, block_on, cc_on_path, run_streamed};
use crate::project::{self, ToolchainMode};
use crate::toolchain::{self, Platform, Tool};

/// The Rust release the templates pin (`rust-toolchain.toml`); the hermetic default when a
/// project has none.
pub const RUST_CHANNEL: &str = "1.100.0";
/// The component target.
pub const WASM_TARGET: &str = "wasm32-wasip2";

/// Host variables passed through to cargo (network configuration only).
const PASS_THROUGH: &[&str] = &[
    "CARGO_NET_OFFLINE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "all_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];

/// The Rust step; `mode` is resolved (`System` or `Hermetic`).
#[derive(Debug, Clone, Copy)]
pub struct RustStep {
    /// The resolved toolchain mode.
    pub mode: ToolchainMode,
}

/// No usable host C compiler: none of `cc`/`gcc`/`clang` on the host `PATH`, or
/// `OSTRO_NO_CC=1` (forces the zig fallback; used by the clean-machine tests, whose
/// masked `cc` stubs are still executables).
pub fn needs_zig(host: HostEnv) -> bool {
    host("OSTRO_NO_CC").is_some_and(|v| crate::fetch::is_truthy(&v)) || !cc_on_path(host)
}

impl LangStep for RustStep {
    fn tool_ids(&self, ctx: &StepCtx) -> Vec<String> {
        let mut v = Vec::new();
        match self.mode {
            ToolchainMode::Hermetic => {
                let ver = ctx
                    .set
                    .pin(Tool::RustupInit)
                    .map_or("unpinned", |p| p.version.as_str());
                v.push(format!("rustup-init@{ver}"));
                v.push(format!("rust@{}", channel_of(ctx.project.root.as_path())));
            }
            _ => v.push("system-rustup".to_owned()),
        }
        if needs_zig(ctx.host) {
            let ver = ctx
                .set
                .pin(Tool::Zig)
                .map_or("unpinned", |p| p.version.as_str());
            v.push(format!("zig@{ver}"));
        }
        v
    }

    fn run(&self, ctx: &StepCtx) -> Result<(), BuildError> {
        let root = ctx.project.root.as_path();
        let out = project::output_dir(ctx.project);
        let target_dir = out.join("cargo");
        let mut path_dirs: Vec<PathBuf> = Vec::new();
        let mut cmd_env: Vec<(String, String)> = Vec::new();

        let cargo = match self.mode {
            ToolchainMode::Hermetic => {
                let h = hermetic_rustup(ctx)?;
                cmd_env.extend(h.env.iter().cloned());
                path_dirs.push(h.cargo_home.join("bin"));
                h.cargo_home.join("bin").join("cargo")
            }
            _ => {
                let cargo = system_cargo(ctx.host).ok_or_else(|| BuildError::Step {
                    step: "rust toolchain".to_owned(),
                    message: "no cargo on PATH (install rustup from https://rustup.rs, or use \
                              --toolchain hermetic)"
                        .to_owned(),
                    stderr_tail: String::new(),
                })?;
                for k in ["RUSTUP_HOME", "CARGO_HOME"] {
                    if let Some(v) = (ctx.host)(k).filter(|v| !v.is_empty()) {
                        cmd_env.push((k.to_owned(), v));
                    }
                }
                if let Some(d) = cargo.parent() {
                    path_dirs.push(d.to_path_buf());
                }
                cargo
            }
        };

        if needs_zig(ctx.host) {
            let shim = zig_shim(ctx)?;
            eprintln!(
                "ostro: no C compiler on PATH: using zig as the host linker ({})",
                shim.cc.display()
            );
            let triple = host_triple()?;
            let upper = triple.to_uppercase().replace('-', "_");
            cmd_env.push(("CC".to_owned(), shim.cc.display().to_string()));
            cmd_env.push(("AR".to_owned(), shim.ar.display().to_string()));
            cmd_env.push((
                format!("CARGO_TARGET_{upper}_LINKER"),
                shim.cc.display().to_string(),
            ));
            if let Some(v) = (ctx.host)("OSTRO_ZIG_SHIM_LOG") {
                cmd_env.push(("OSTRO_ZIG_SHIM_LOG".to_owned(), v));
            }
            path_dirs.insert(0, shim.dir);
        }

        let mut cmd = Command::new(&cargo);
        cmd.current_dir(root)
            .env_clear()
            .envs(ctx.env)
            .args(["build", "--release", "--target", WASM_TARGET])
            .env("CARGO_TARGET_DIR", &target_dir)
            .env("PATH", join_path(&path_dirs, &ctx.env["PATH"]));
        if root.join("Cargo.lock").is_file() {
            cmd.arg("--locked");
        } else {
            eprintln!(
                "ostro: warning: no Cargo.lock: generated; commit it for reproducible builds"
            );
        }
        for k in PASS_THROUGH {
            if let Some(v) = (ctx.host)(k) {
                cmd.env(k, v);
            }
        }
        if ctx.fetch.offline {
            cmd.env("CARGO_NET_OFFLINE", "true");
        }
        cmd.envs(cmd_env);
        run_streamed(&mut cmd, "cargo build")?;

        let lib = lib_name(root)?;
        let built = target_dir
            .join(WASM_TARGET)
            .join("release")
            .join(format!("{lib}.wasm"));
        std::fs::copy(&built, ctx.out_wasm).map_err(|e| {
            BuildError::io(
                format!("copying {} (is the crate a `cdylib`?)", built.display()),
                e,
            )
        })?;
        Ok(())
    }
}

/// `a:b:…:base`.
fn join_path(dirs: &[PathBuf], base: &str) -> String {
    let mut parts: Vec<String> = dirs.iter().map(|d| d.display().to_string()).collect();
    parts.push(base.to_owned());
    parts.join(":")
}

/// The system cargo: the rustup proxy next to `rustup` (it honours `rust-toolchain.toml`),
/// else the first `cargo` on the host `PATH`.
pub fn system_cargo(host: HostEnv) -> Option<PathBuf> {
    if let Some(rustup) = crate::pipeline::which_in(host, "rustup") {
        let proxy = rustup.with_file_name("cargo");
        if proxy.is_file() {
            return Some(proxy);
        }
    }
    crate::pipeline::which_in(host, "cargo")
}

/// The library file stem: `[lib] name`, else `[package] name`, with `-` → `_`.
pub fn lib_name(root: &Path) -> Result<String, BuildError> {
    let path = root.join("Cargo.toml");
    let text = std::fs::read_to_string(&path)
        .map_err(|e| BuildError::io(format!("reading {}", path.display()), e))?;
    let v: toml::Table = toml::from_str(&text).map_err(|e| {
        BuildError::io(
            format!("parsing {}", path.display()),
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.message().to_owned()),
        )
    })?;
    let name = v
        .get("lib")
        .and_then(|l| l.get("name"))
        .or_else(|| v.get("package").and_then(|p| p.get("name")))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| {
            BuildError::io(
                format!("parsing {}", path.display()),
                std::io::Error::new(std::io::ErrorKind::InvalidData, "no [package] name"),
            )
        })?;
    Ok(name.replace('-', "_"))
}

/// `[toolchain] channel` and `targets` of the project's `rust-toolchain.toml` (or the
/// legacy `rust-toolchain` file); defaults [`RUST_CHANNEL`] and `[wasm32-wasip2]`.
fn toolchain_file(root: &Path) -> (String, Vec<String>) {
    let mut channel = RUST_CHANNEL.to_owned();
    let mut targets = vec![WASM_TARGET.to_owned()];
    for f in ["rust-toolchain.toml", "rust-toolchain"] {
        let Ok(text) = std::fs::read_to_string(root.join(f)) else {
            continue;
        };
        if let Ok(v) = toml::from_str::<toml::Table>(&text) {
            if let Some(t) = v.get("toolchain") {
                if let Some(c) = t.get("channel").and_then(toml::Value::as_str) {
                    channel = c.to_owned();
                }
                if let Some(list) = t.get("targets").and_then(toml::Value::as_array) {
                    for s in list.iter().filter_map(toml::Value::as_str) {
                        if !targets.iter().any(|x| x == s) {
                            targets.push(s.to_owned());
                        }
                    }
                }
            }
        } else if f == "rust-toolchain" && !text.trim().is_empty() {
            channel = text.trim().to_owned();
        }
        break;
    }
    (channel, targets)
}

fn channel_of(root: &Path) -> String {
    toolchain_file(root).0
}

/// The host's Rust target triple (`x86_64-unknown-linux-gnu`, `aarch64-apple-darwin`, ...).
pub fn host_triple() -> Result<String, BuildError> {
    let p = Platform::detect()?;
    Ok(match p.os {
        "macos" => format!("{}-apple-darwin", p.arch),
        _ if toolchain::musl_host() => format!("{}-unknown-linux-musl", p.arch),
        _ => format!("{}-unknown-linux-gnu", p.arch),
    })
}

/// zig's spelling of the host triple (`x86_64-linux-gnu`, `aarch64-macos`, ...).
pub fn host_zig_triple() -> Result<String, BuildError> {
    let p = Platform::detect()?;
    Ok(match p.os {
        "macos" => format!("{}-macos", p.arch),
        _ if toolchain::musl_host() => format!("{}-linux-musl", p.arch),
        _ => format!("{}-linux-gnu", p.arch),
    })
}

struct Hermetic {
    cargo_home: PathBuf,
    env: Vec<(String, String)>,
}

fn step_err(step: &str, message: impl Into<String>) -> BuildError {
    BuildError::Step {
        step: step.to_owned(),
        message: message.into(),
        stderr_tail: String::new(),
    }
}

/// Install (once) rustup and the project's toolchain into the cache; returns the homes.
fn hermetic_rustup(ctx: &StepCtx) -> Result<Hermetic, BuildError> {
    let rustup_home = ctx.cache.state_dir("rustup");
    let cargo_home = ctx.cache.state_dir("cargo");
    let mut env = vec![
        ("RUSTUP_HOME".to_owned(), rustup_home.display().to_string()),
        ("CARGO_HOME".to_owned(), cargo_home.display().to_string()),
    ];
    if let Some(m) = &ctx.fetch.mirror {
        let m = m.trim_end_matches('/');
        env.push(("RUSTUP_DIST_SERVER".to_owned(), format!("{m}/rustup")));
        env.push((
            "RUSTUP_UPDATE_ROOT".to_owned(),
            format!("{m}/rustup/rustup"),
        ));
    }
    let (channel, targets) = toolchain_file(&ctx.project.root);
    let triple = host_triple()?;
    let rustup = cargo_home.join("bin").join("rustup");
    let installed = |rustup_home: &Path| {
        let tc = rustup_home
            .join("toolchains")
            .join(format!("{channel}-{triple}"));
        tc.join("bin").join("cargo").is_file()
            && targets
                .iter()
                .all(|t| tc.join("lib").join("rustlib").join(t).is_dir())
    };
    if rustup.is_file() && installed(&rustup_home) {
        env.push(("RUSTUP_TOOLCHAIN".to_owned(), channel));
        return Ok(Hermetic { cargo_home, env });
    }

    let pin = ctx
        .set
        .pin(Tool::RustupInit)
        .ok_or_else(|| step_err("rustup-init", "no rustup-init pin for this platform"))?
        .clone();
    let entry = block_on(|| toolchain::ensure(ctx.cache, &pin, ctx.fetch))??;
    if ctx.fetch.offline {
        return Err(step_err(
            "rust toolchain",
            format!(
                "Rust {channel} ({}) is not installed in the toolchain cache and the build is \
                 offline (run `ostro build --toolchain hermetic` once with network access)",
                targets.join(", ")
            ),
        ));
    }
    let base_env = |cmd: &mut Command| {
        cmd.env_clear().envs(ctx.env).envs(env.iter().cloned());
        for k in PASS_THROUGH {
            if let Some(v) = (ctx.host)(k) {
                cmd.env(k, v);
            }
        }
    };
    // Serialise rustup mutations across processes (the cache lock).
    let lock = block_on(|| ctx.cache.lock())??;
    std::fs::create_dir_all(&rustup_home)
        .and_then(|()| std::fs::create_dir_all(&cargo_home))
        .map_err(|e| BuildError::io("creating the hermetic rustup homes", e))?;
    if !rustup.is_file() {
        eprintln!(
            "ostro: installing rustup {} into {}",
            pin.version,
            cargo_home.display()
        );
        let mut cmd = Command::new(entry.join("rustup-init"));
        base_env(&mut cmd);
        cmd.args([
            "-y",
            "--no-modify-path",
            "--profile",
            "minimal",
            "--default-toolchain",
            "none",
        ]);
        run_streamed(&mut cmd, "rustup-init")?;
    }
    if !installed(&rustup_home) {
        eprintln!("ostro: installing Rust {channel} ({})", targets.join(", "));
        let mut cmd = Command::new(&rustup);
        base_env(&mut cmd);
        cmd.args(["toolchain", "install", &channel, "--profile", "minimal"]);
        for t in &targets {
            cmd.args(["--target", t]);
        }
        run_streamed(&mut cmd, "rustup toolchain install")?;
    }
    drop(lock);
    env.push(("RUSTUP_TOOLCHAIN".to_owned(), channel));
    Ok(Hermetic { cargo_home, env })
}

struct Shim {
    dir: PathBuf,
    cc: PathBuf,
    ar: PathBuf,
}

/// Header + argument filter of the `cc` shim. `@ZIG@` and `@TARGET@` are substituted.
///
/// The filtering rules are ported from cargo-zigbuild (<https://github.com/rust-cross/cargo-zigbuild>,
/// `src/zig.rs`, MIT OR Apache-2.0); every rule is listed in the script header.
const CC_SHIM: &str = r#"#!/bin/sh
# ostro zig cc shim: the host linker / C compiler of `ostro build` when the host has no
# cc, gcc or clang (generated; do not edit). Argument filtering ported from cargo-zigbuild
# (https://github.com/rust-cross/cargo-zigbuild, src/zig.rs; MIT OR Apache-2.0):
#   -lgcc_s                         -> -lunwind (zig ships libunwind, not libgcc_s)
#   -lgcc_eh                        dropped (zig links its own unwinder / compiler-rt)
#   --target=<rust triple>          mapped to zig's spelling (x86_64-unknown-linux-gnu ->
#   -target <rust triple>             x86_64-linux-gnu, *-apple-darwin -> <arch>-macos,
#                                     *-linux-musl* -> <arch>-linux-musl); the shim always
#                                     passes one explicit -target (default: the host)
#   -fuse-ld=*                      dropped (zig always links with its own lld)
#   -B<dir>                         dropped (rustc's self-contained gcc-ld dir)
#   -Wl,--no-undefined-version      dropped (rejected by zig's lld)
#   -Wl,-znostart-stop-gc           dropped (not understood by zig's lld)
#   -Wl,--disable-auto-image-base   dropped (PE-only)
#   -Wl,--dynamicbase               dropped (PE-only)
#   -Wl,-exported_symbols_list,F    (macOS) split into -Wl,-exported_symbols_list -Wl,F
#   -Wl,-dylib                      (macOS) dropped (-shared already selects a dylib)
# OSTRO_ZIG_SHIM_LOG, when set, gets one line per invocation with the original argv.
ZIG='@ZIG@'
TARGET='@TARGET@'
map_triple() {
    case "$1" in
        *-apple-darwin*) echo "${1%%-*}-macos" ;;
        *-linux-musl*) echo "${1%%-*}-linux-musl" ;;
        *-linux-gnu*) echo "${1%%-*}-linux-gnu" ;;
        *) echo "$TARGET" ;;
    esac
}
if [ -n "${OSTRO_ZIG_SHIM_LOG:-}" ]; then
    printf 'cc %s\n' "$*" >> "$OSTRO_ZIG_SHIM_LOG"
fi
next_is_target=0
first=1
for arg in "$@"; do
    if [ "$first" = 1 ]; then
        set --
        first=0
    fi
    if [ "$next_is_target" = 1 ]; then
        TARGET="$(map_triple "$arg")"
        next_is_target=0
        continue
    fi
    case "$arg" in
        -lgcc_s) set -- "$@" -lunwind ;;
        -lgcc_eh) ;;
        --target=*) TARGET="$(map_triple "${arg#--target=}")" ;;
        -target) next_is_target=1 ;;
        -fuse-ld=*) ;;
        -B*) ;;
        -Wl,--no-undefined-version) ;;
        -Wl,-znostart-stop-gc) ;;
        -Wl,--disable-auto-image-base) ;;
        -Wl,--dynamicbase) ;;
        -Wl,-exported_symbols_list,*)
            set -- "$@" -Wl,-exported_symbols_list "-Wl,${arg#-Wl,-exported_symbols_list,}" ;;
        -Wl,-dylib) ;;
        *) set -- "$@" "$arg" ;;
    esac
done
exec "$ZIG" cc -target "$TARGET" "$@"
"#;

const AR_SHIM: &str = r#"#!/bin/sh
# ostro zig ar shim (generated; do not edit): `zig ar`.
if [ -n "${OSTRO_ZIG_SHIM_LOG:-}" ]; then
    printf 'ar %s\n' "$*" >> "$OSTRO_ZIG_SHIM_LOG"
fi
exec '@ZIG@' ar "$@"
"#;

/// Install zig (pinned) and write the `cc` / `ar` shims into `state/zig-shim`.
fn zig_shim(ctx: &StepCtx) -> Result<Shim, BuildError> {
    let pin = ctx
        .set
        .pin(Tool::Zig)
        .ok_or_else(|| step_err("zig", "no zig pin for this platform"))?
        .clone();
    let entry = block_on(|| toolchain::ensure(ctx.cache, &pin, ctx.fetch))??;
    let zig = entry.join("zig");
    let dir = ctx.cache.state_dir("zig-shim");
    std::fs::create_dir_all(&dir).map_err(|e| BuildError::io("creating the zig shim dir", e))?;
    let zig_s = zig.display().to_string().replace('\'', "'\\''");
    let target = host_zig_triple()?;
    let cc = dir.join("cc");
    let ar = dir.join("ar");
    write_script(
        &cc,
        &CC_SHIM
            .replace("@ZIG@", &zig_s)
            .replace("@TARGET@", &target),
    )?;
    write_script(&ar, &AR_SHIM.replace("@ZIG@", &zig_s))?;
    Ok(Shim { dir, cc, ar })
}

/// Write an executable script atomically (temp file + rename), mode 0755.
fn write_script(path: &Path, text: &str) -> Result<(), BuildError> {
    if std::fs::read_to_string(path).is_ok_and(|t| t == text) {
        return Ok(());
    }
    let tmp = path.with_extension(format!("tmp-{}", crate::pipeline::tmp_suffix()));
    let res = (|| {
        std::fs::write(&tmp, text)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
        }
        std::fs::rename(&tmp, path)
    })();
    res.map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        BuildError::io(format!("writing {}", path.display()), e)
    })
}
