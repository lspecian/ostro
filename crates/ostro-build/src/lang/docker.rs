//! `--docker` mode (D-P10-15): the whole build runs in the `ostro-build:<version>` image,
//! built on first use from the embedded `deploy/build-image/Dockerfile`.
//!
//! - The docker binary is `OSTRO_DOCKER`, else `docker` on the host `PATH`.
//! - `docker image inspect ostro-build:<version>`; when it fails,
//!   `docker build -t ostro-build:<version> -` with the Dockerfile on stdin (no context).
//! - The ostro binary mounted into the container is `OSTRO_LINUX_BIN`, else (Linux) the
//!   running executable, which should be a static musl build: a binary with an ELF
//!   `PT_INTERP` (dynamically linked) gets a warning. macOS needs `OSTRO_LINUX_BIN`.
//! - `docker run --rm --user <uid>:<gid> -v <ostro>:/usr/local/bin/ostro:ro
//!   -v <project>:/src -v <cache>:/cache -e HOME=/tmp -e SOURCE_DATE_EPOCH=<sde>
//!   [-e OSTRO_OFFLINE=1] [-e OSTRO_TOOLCHAIN_MIRROR=<m>] <image> ostro build /src
//!   --in-container --json`: the container builds hermetically into the project's
//!   `target/ostro`; its JSON result names the wasm, which is copied to the pipeline's
//!   output and validated again on the host.
//!
//! The docker CLI runs with the host process environment (it needs `DOCKER_HOST`, `HOME`
//! for its config, credential helpers on `PATH`); the container gets only the variables
//! above.
//!
//! Owner: 10-15.

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::lang::python::step_err;
use crate::pipeline::{BuildError, HostEnv, LangStep, StepCtx, which_in};

/// The image repository.
pub const IMAGE: &str = "ostro-build";

/// The build image recipe (embedded; built locally on first use).
pub const DOCKERFILE: &str = include_str!("../../../../deploy/build-image/Dockerfile");

/// Where the project, the cache and the binary are mounted.
pub const SRC_MOUNT: &str = "/src";
/// The toolchain cache mount (`OSTRO_TOOLCHAIN_DIR` in the image).
pub const CACHE_MOUNT: &str = "/cache";
/// The ostro binary mount.
pub const BIN_MOUNT: &str = "/usr/local/bin/ostro";

/// `ostro-build:<ostro version>`.
pub fn image_tag() -> String {
    format!("{IMAGE}:{}", env!("CARGO_PKG_VERSION"))
}

/// The Docker step.
#[derive(Debug, Clone, Copy, Default)]
pub struct DockerStep;

/// The docker CLI: `OSTRO_DOCKER` (a path that must exist, or a name looked up on the host
/// `PATH`), else `docker` on the host `PATH`.
pub fn docker_bin(host: HostEnv) -> Result<PathBuf, BuildError> {
    if let Some(v) = host("OSTRO_DOCKER").filter(|v| !v.is_empty()) {
        // A bare name is looked up on PATH (as `ostro doctor` does).
        let found = if v.contains('/') {
            Some(PathBuf::from(&v)).filter(|p| p.is_file())
        } else {
            which_in(host, &v)
        };
        if let Some(p) = found {
            return Ok(p);
        }
        let p = PathBuf::from(v);
        return Err(step_err(
            "docker",
            format!(
                "--docker needs docker (OSTRO_DOCKER={} does not exist)",
                p.display()
            ),
        ));
    }
    which_in(host, "docker").ok_or_else(|| {
        step_err(
            "docker",
            "--docker needs docker (not found on PATH; install Docker or set OSTRO_DOCKER)",
        )
    })
}

/// The ELF interpreter (`PT_INTERP`) of `bytes`, `None` for a static binary or a non-ELF
/// file.
pub fn elf_interp(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 0x40 || &bytes[..4] != b"\x7fELF" {
        return None;
    }
    let is64 = bytes[4] == 2;
    let le = bytes[5] == 1;
    let u16_at = |o: usize| -> Option<u64> {
        let b: [u8; 2] = bytes.get(o..o + 2)?.try_into().ok()?;
        Some(u64::from(if le {
            u16::from_le_bytes(b)
        } else {
            u16::from_be_bytes(b)
        }))
    };
    let u32_at = |o: usize| -> Option<u64> {
        let b: [u8; 4] = bytes.get(o..o + 4)?.try_into().ok()?;
        Some(u64::from(if le {
            u32::from_le_bytes(b)
        } else {
            u32::from_be_bytes(b)
        }))
    };
    let u64_at = |o: usize| -> Option<u64> {
        let b: [u8; 8] = bytes.get(o..o + 8)?.try_into().ok()?;
        Some(if le {
            u64::from_le_bytes(b)
        } else {
            u64::from_be_bytes(b)
        })
    };
    let (phoff, phentsize, phnum) = if is64 {
        (u64_at(0x20)?, u16_at(0x36)?, u16_at(0x38)?)
    } else {
        (u32_at(0x1c)?, u16_at(0x2a)?, u16_at(0x2c)?)
    };
    for i in 0..phnum {
        let ph = usize::try_from(phoff + i * phentsize).ok()?;
        if u32_at(ph)? != 3 {
            continue;
        }
        let (off, size) = if is64 {
            (u64_at(ph + 8)?, u64_at(ph + 32)?)
        } else {
            (u32_at(ph + 4)?, u32_at(ph + 16)?)
        };
        let (off, size) = (usize::try_from(off).ok()?, usize::try_from(size).ok()?);
        let raw = bytes.get(off..off.checked_add(size)?)?;
        let s = String::from_utf8_lossy(raw);
        return Some(s.trim_end_matches('\0').to_owned());
    }
    None
}

/// The ostro binary for the container: `OSTRO_LINUX_BIN`, else the running executable on
/// Linux. Warns when it is dynamically linked.
pub fn linux_bin(host: HostEnv) -> Result<PathBuf, BuildError> {
    let bin = match host("OSTRO_LINUX_BIN").filter(|v| !v.is_empty()) {
        Some(p) => PathBuf::from(p),
        None if std::env::consts::OS == "linux" => std::env::current_exe()
            .map_err(|e| BuildError::io("locating the running ostro binary", e))?,
        None => {
            return Err(step_err(
                "docker",
                "--docker on this OS needs OSTRO_LINUX_BIN: the path of a static Linux musl \
                 ostro binary for the container's architecture",
            ));
        }
    };
    let bytes =
        std::fs::read(&bin).map_err(|e| BuildError::io(format!("reading {}", bin.display()), e))?;
    if let Some(interp) = elf_interp(&bytes) {
        eprintln!(
            "ostro: warning: the host ostro ({}) is dynamically linked ({interp}); set \
             OSTRO_LINUX_BIN to a static musl build",
            bin.display()
        );
    }
    Ok(bin)
}

/// `id -u`:`id -g` of this process.
fn user_spec() -> Result<String, BuildError> {
    let id = |flag: &str| -> Result<String, BuildError> {
        let out = Command::new("id")
            .arg(flag)
            .stdin(Stdio::null())
            .output()
            .map_err(|e| BuildError::io("running id", e))?;
        let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        if !out.status.success() || s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(step_err("docker", format!("`id {flag}` failed")));
        }
        Ok(s)
    };
    Ok(format!("{}:{}", id("-u")?, id("-g")?))
}

/// Build the image unless `docker image inspect` finds it.
pub fn ensure_image(docker: &Path, tag: &str) -> Result<(), BuildError> {
    let inspect = Command::new(docker)
        .args(["image", "inspect", tag])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| step_err("docker image inspect", format!("cannot run docker: {e}")))?;
    if inspect.success() {
        return Ok(());
    }
    eprintln!("ostro: building the {tag} image (first use of --docker)");
    let mut child = Command::new(docker)
        .args(["build", "-t", tag, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| step_err("docker build", format!("cannot run docker: {e}")))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(DOCKERFILE.as_bytes())
            .map_err(|e| BuildError::io("writing the Dockerfile to docker build", e))?;
    }
    let (status, _out, tail) = wait_streaming(child, "docker build")?;
    if !status.success() {
        return Err(BuildError::Step {
            step: "docker build".to_owned(),
            message: format!("exited with {status}"),
            stderr_tail: tail,
        });
    }
    Ok(())
}

/// Wait for `child`: stdout is collected, stderr (and stdout) lines are echoed to this
/// process's stderr; returns the status, stdout and the stderr tail.
fn wait_streaming(
    mut child: std::process::Child,
    step: &str,
) -> Result<(std::process::ExitStatus, String, String), BuildError> {
    let err = child.stderr.take().map(|e| {
        std::thread::spawn(move || {
            let mut tail = std::collections::VecDeque::new();
            for line in BufReader::new(e).lines() {
                let Ok(line) = line else { break };
                eprintln!("{line}");
                if tail.len() == 60 {
                    tail.pop_front();
                }
                tail.push_back(line);
            }
            tail.into_iter().collect::<Vec<_>>().join("\n")
        })
    });
    let mut out = String::new();
    if let Some(mut o) = child.stdout.take() {
        let _ = o.read_to_string(&mut out);
    }
    let status = child.wait().map_err(|e| BuildError::Step {
        step: step.to_owned(),
        message: format!("waiting for the process: {e}"),
        stderr_tail: String::new(),
    })?;
    let tail = err.and_then(|h| h.join().ok()).unwrap_or_default();
    Ok((status, out, tail))
}

/// The `docker run` arguments (after the docker binary).
pub fn run_args(
    user: &str,
    bin: &Path,
    project: &Path,
    cache: &Path,
    env: &[(String, String)],
    tag: &str,
    offline: bool,
) -> Vec<String> {
    let mut a: Vec<String> = vec!["run".into(), "--rm".into(), "--user".into(), user.into()];
    for (from, to, ro) in [
        (bin, BIN_MOUNT, true),
        (project, SRC_MOUNT, false),
        (cache, CACHE_MOUNT, false),
    ] {
        a.push("-v".into());
        a.push(format!(
            "{}:{to}{}",
            from.display(),
            if ro { ":ro" } else { "" }
        ));
    }
    for (k, v) in env {
        a.push("-e".into());
        a.push(format!("{k}={v}"));
    }
    a.push(tag.into());
    a.extend(
        ["ostro", "build", SRC_MOUNT, "--in-container", "--json"]
            .iter()
            .map(|s| (*s).to_owned()),
    );
    if offline {
        a.push("--offline".into());
    }
    a
}

impl LangStep for DockerStep {
    fn tool_ids(&self, _ctx: &StepCtx) -> Vec<String> {
        vec![format!("docker ({})", image_tag())]
    }

    fn run(&self, ctx: &StepCtx) -> Result<(), BuildError> {
        let docker = docker_bin(ctx.host)?;
        let tag = image_tag();
        ensure_image(&docker, &tag)?;
        let bin = linux_bin(ctx.host)?;
        let user = user_spec()?;
        std::fs::create_dir_all(&ctx.cache.root)
            .map_err(|e| BuildError::io(format!("creating {}", ctx.cache.root.display()), e))?;
        let cache = std::path::absolute(&ctx.cache.root)
            .map_err(|e| BuildError::io("resolving the toolchain cache dir", e))?;
        let mut env = vec![("HOME".to_owned(), "/tmp".to_owned())];
        if let Some(sde) = ctx.env.get("SOURCE_DATE_EPOCH") {
            env.push(("SOURCE_DATE_EPOCH".to_owned(), sde.clone()));
        }
        if ctx.fetch.offline {
            env.push(("OSTRO_OFFLINE".to_owned(), "1".to_owned()));
        }
        if let Some(m) = &ctx.fetch.mirror {
            env.push(("OSTRO_TOOLCHAIN_MIRROR".to_owned(), m.clone()));
        }
        let root = ctx.project.root.as_path();
        let args = run_args(&user, &bin, root, &cache, &env, &tag, ctx.fetch.offline);
        let child = Command::new(&docker)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| step_err("docker run", format!("cannot run docker: {e}")))?;
        let (status, out, tail) = wait_streaming(child, "docker run")?;
        if !status.success() {
            return Err(BuildError::Step {
                step: "docker run (ostro build --in-container)".to_owned(),
                message: format!("exited with {status}"),
                stderr_tail: tail,
            });
        }
        let v: serde_json::Value = serde_json::from_str(out.trim()).map_err(|e| {
            step_err(
                "docker run (ostro build --in-container)",
                format!("unparsable JSON result ({e}): {}", out.trim()),
            )
        })?;
        let wasm = v
            .get("wasm")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                step_err(
                    "docker run (ostro build --in-container)",
                    format!("the JSON result has no `wasm`: {}", out.trim()),
                )
            })?;
        let rel = Path::new(wasm).strip_prefix(SRC_MOUNT).map_err(|_| {
            step_err(
                "docker run (ostro build --in-container)",
                format!("the wasm {wasm} is outside {SRC_MOUNT}"),
            )
        })?;
        let host_wasm = root.join(rel);
        std::fs::copy(&host_wasm, ctx.out_wasm)
            .map_err(|e| BuildError::io(format!("copying {}", host_wasm.display()), e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dockerfile_is_pinned() {
        assert!(DOCKERFILE.contains("@sha256:"));
        assert!(DOCKERFILE.contains("ca-certificates"));
        assert!(image_tag().starts_with("ostro-build:"));
    }

    #[test]
    fn elf_interp_of_a_dynamic_and_a_non_elf_file() {
        if let Ok(b) = std::fs::read("/bin/sh")
            && b.starts_with(b"\x7fELF")
        {
            assert!(elf_interp(&b).is_some_and(|i| i.contains("ld")));
        }
        assert_eq!(elf_interp(b"#!/bin/sh\n"), None);
    }

    #[test]
    fn run_args_have_the_mounts() {
        let a = run_args(
            "1000:1000",
            Path::new("/b/ostro"),
            Path::new("/p"),
            Path::new("/c"),
            &[("HOME".into(), "/tmp".into())],
            "ostro-build:0",
            true,
        );
        let s = a.join(" ");
        assert!(s.contains("--user 1000:1000"), "{s}");
        assert!(s.contains("-v /b/ostro:/usr/local/bin/ostro:ro"), "{s}");
        assert!(s.contains("-v /p:/src "), "{s}");
        assert!(s.contains("-v /c:/cache "), "{s}");
        assert!(s.ends_with("ostro-build:0 ostro build /src --in-container --json --offline"));
    }
}
