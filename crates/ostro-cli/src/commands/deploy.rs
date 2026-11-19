//! `ostro deploy <file.wasm> [--activate] [--name NAME] [--tenant T]` (D12: upload only,
//! no build; CLI-05 build-if-needed is Phase 10).

use std::path::{Path, PathBuf};

use clap::Args;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s};

/// `ostro deploy`.
#[derive(Debug, Clone, Args)]
pub struct DeployArgs {
    /// The Wasm component to upload.
    pub file: PathBuf,
    /// Activate every workflow version the module describes.
    #[arg(long)]
    pub activate: bool,
    /// Deployed module name (default: the file stem).
    #[arg(long)]
    pub name: Option<String>,
    /// Uploading tenant (default: the default tenant).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

impl DeployArgs {
    /// `--name`, else the file stem (`build/orders.wasm` -> `orders`).
    pub fn module_name(&self) -> Option<String> {
        self.name
            .clone()
            .or_else(|| stem(&self.file))
            .filter(|n| !n.is_empty())
    }
}

fn stem(p: &Path) -> Option<String> {
    p.file_stem().map(|s| s.to_string_lossy().into_owned())
}

/// Lowercase hex sha256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The activation name of a described workflow: tenant-qualified when `--tenant` is set
/// (sent as one `%2F`-encoded path segment).
pub fn activation_name(tenant: Option<&str>, workflow: &str) -> String {
    match tenant.filter(|t| !t.is_empty()) {
        Some(t) => format!("{t}/{workflow}"),
        None => workflow.to_owned(),
    }
}

/// `ostro deploy`.
pub async fn deploy(c: &Client, out: Out, a: &DeployArgs) -> Result<Outcome, CallError> {
    let wasm = std::fs::read(&a.file)
        .map_err(|e| CallError::Usage(format!("cannot read {}: {e}", a.file.display())))?;
    let sha = sha256_hex(&wasm);
    let size = wasm.len();
    let name = a.module_name();
    let up = c
        .upload_module(&sha, wasm, name.as_deref(), a.tenant.as_deref())
        .await?;
    let described: Vec<(String, u64)> = up.body["module"]["workflows"]
        .as_array()
        .map(|ws| {
            ws.iter()
                .filter_map(|w| Some((w["name"].as_str()?.to_owned(), w["version"].as_u64()?)))
                .collect()
        })
        .unwrap_or_default();

    if !out.json {
        let state = if up.status == 201 {
            "created"
        } else {
            "unchanged"
        };
        println!(
            "uploaded {} ({state}, {size} bytes){}",
            &sha[..12],
            name.as_deref()
                .map(|n| format!(" as module {n}"))
                .unwrap_or_default()
        );
        let list: Vec<String> = described.iter().map(|(n, v)| format!("{n}@{v}")).collect();
        println!(
            "workflows: {}",
            if list.is_empty() {
                "-".to_owned()
            } else {
                list.join(", ")
            }
        );
    }
    if !a.activate {
        if out.json {
            out.json(&up.body);
        }
        return Ok(Outcome::Ok);
    }

    let mut activated = Vec::new();
    for (wf, version) in &described {
        let qn = activation_name(a.tenant.as_deref(), wf);
        let r = c.activate(&qn, *version).await?;
        if !out.json {
            println!(
                "activated {qn}@{version} (active version {})",
                s(&r.body["active_version"])
            );
        }
        activated.push(r.body);
    }
    if out.json {
        out.json(&json!({ "upload": up.body, "activated": Value::Array(activated) }));
    }
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    #[test]
    fn name_defaults_to_the_file_stem() {
        let ClientCommand::Deploy(a) = parse(&["deploy", "build/orders.wasm"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.module_name().as_deref(), Some("orders"));
        assert!(!a.activate);
        assert_eq!(a.tenant, None);
        let ClientCommand::Deploy(a) = parse(&[
            "deploy",
            "x.wasm",
            "--activate",
            "--name",
            "billing",
            "--tenant",
            "acme",
        ])
        .unwrap() else {
            panic!()
        };
        assert_eq!(a.module_name().as_deref(), Some("billing"));
        assert!(a.activate);
        assert_eq!(a.tenant.as_deref(), Some("acme"));
        assert_eq!(parse(&["deploy"]).unwrap_err().exit_code(), 2);
    }

    #[test]
    fn sha_and_activation_names() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(activation_name(None, "w"), "w");
        assert_eq!(activation_name(Some(""), "w"), "w");
        assert_eq!(activation_name(Some("acme"), "w"), "acme/w");
    }
}
