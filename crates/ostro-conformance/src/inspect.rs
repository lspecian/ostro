//! Component inspection: top-level imports/exports and core instance/module counts.
//!
//! Used by the polyglot smoke test (09-02), the engine pool sizing (09-04/09-07: core
//! instances per component instance) and the `ostro-conf-inspect` bin.

use serde::Serialize;
use wasmparser::{Encoding, Parser, Payload};

/// What a component imports, exports and instantiates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ComponentInfo {
    /// Top-level component import names (full names incl. version), in section order.
    pub imports: Vec<String>,
    /// Top-level component export names, in section order.
    pub exports: Vec<String>,
    /// Core instances created, counted over the outer component and every nested component.
    pub core_instances: u32,
    /// Core modules embedded anywhere in the component.
    pub core_modules: u32,
}

/// Inspects a component binary. Fails on a core module or malformed input.
pub fn inspect(bytes: &[u8]) -> Result<ComponentInfo, String> {
    let mut info = ComponentInfo {
        imports: Vec::new(),
        exports: Vec::new(),
        core_instances: 0,
        core_modules: 0,
    };
    // One entry per open module/component; `true` = component.
    let mut stack: Vec<bool> = Vec::new();
    for payload in Parser::new(0).parse_all(bytes) {
        let payload = payload.map_err(|e| format!("parse: {e}"))?;
        match payload {
            Payload::Version { encoding, .. } => match encoding {
                Encoding::Component => stack.push(true),
                Encoding::Module => {
                    if stack.is_empty() {
                        return Err("not a component (core module)".into());
                    }
                    info.core_modules += 1;
                    stack.push(false);
                }
            },
            Payload::End(_) => {
                stack.pop();
            }
            // Core instance section of a component (inside a core module this payload
            // does not occur).
            Payload::InstanceSection(reader) if stack.last() == Some(&true) => {
                info.core_instances += reader.count();
            }
            Payload::ComponentImportSection(reader) if stack.len() == 1 => {
                for imp in reader {
                    let imp = imp.map_err(|e| format!("import: {e}"))?;
                    info.imports.push(imp.name.full_name().into_owned());
                }
            }
            Payload::ComponentExportSection(reader) if stack.len() == 1 => {
                for exp in reader {
                    let exp = exp.map_err(|e| format!("export: {e}"))?;
                    info.exports.push(exp.name.full_name().into_owned());
                }
            }
            _ => {}
        }
    }
    if !stack.is_empty() {
        return Err("truncated component".into());
    }
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    /// Builds `guests/smoke` (wasm32-wasip2, release) into `<repo>/target/guests`, like
    /// `crates/ostro-engine/tests/common::guest_wasm`.
    fn smoke_guest() -> Vec<u8> {
        let root = crate::repo_root();
        let target_dir = root.join("target/guests");
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "-p",
                "smoke",
                "--manifest-path",
            ])
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("spawn cargo for guests/smoke");
        assert!(
            out.status.success(),
            "guests/smoke build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let path = target_dir.join("wasm32-wasip2/release/smoke.wasm");
        std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
    }

    #[test]
    fn rust_guest_shape() {
        let info = inspect(&smoke_guest()).expect("inspect");
        eprintln!("smoke: {info:?}");
        assert!(
            info.imports.iter().any(|i| i.starts_with("ostro:runtime/")),
            "{:?}",
            info.imports
        );
        assert!(
            info.exports
                .iter()
                .any(|e| e == "ostro:runtime/workflows@0.1.0"),
            "{:?}",
            info.exports
        );
        assert!(info.core_instances >= 1);
        assert!(info.core_modules >= 1);
    }

    #[test]
    fn rejects_core_module() {
        let err = inspect(b"\0asm\x01\0\0\0").unwrap_err();
        assert!(err.contains("core module"), "{err}");
    }

    #[test]
    fn rejects_garbage() {
        assert!(inspect(b"not wasm").is_err());
    }
}
