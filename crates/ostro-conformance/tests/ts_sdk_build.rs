//! TypeScript SDK guest build (09-10 Task 3, `--features polyglot`): the SDK example
//! `sdks/ts/examples/hello` builds with `build-guest.sh ts` (QuickJS, default backend) and
//! `build-guest.sh ts-sm` (StarlingMonkey). QuickJS imports only `ostro:runtime/*@0.1.0`
//! (zero `wasi:`, SDK-03) and exports only `ostro:runtime/workflows@0.1.0`; StarlingMonkey
//! imports only `ostro:runtime/*`. A lowering check bundles the example the way S9 Task 2
//! did (Rolldown from the pinned toolchain with `sdks/ts/rolldown.ostro.mjs` merged) and
//! asserts no native async code survives in SDK or example code.
//! Needs Node 24 and the pinned npm toolchain; a missing toolchain fails loudly. The first
//! real-engine runs of SDK-built TS guests are 09-12 Task 3.
#![cfg(feature = "polyglot")]

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use ostro_conformance::inspect::inspect;
use ostro_conformance::polyglot::{Mode, build};
use ostro_conformance::repo_root;

const OSTRO_PREFIX: &str = "ostro:runtime/";
const WORKFLOWS: &str = "ostro:runtime/workflows@0.1.0";
const NEEDED_IMPORTS: &[&str] = &[
    "ostro:runtime/context@0.1.0",
    "ostro:runtime/journal@0.1.0",
    "ostro:runtime/effects@0.1.0",
    "ostro:runtime/log@0.1.0",
];

fn hello_dir() -> std::path::PathBuf {
    repo_root().join("sdks/ts/examples/hello")
}

fn build_and_inspect(mode: Mode) -> ostro_conformance::inspect::ComponentInfo {
    let path = build(mode, &hello_dir()).unwrap_or_else(|e| panic!("{e}"));
    let bytes = std::fs::read(&path).expect("read component");
    let info = inspect(&bytes).unwrap_or_else(|e| panic!("inspect {}: {e}", path.display()));
    println!(
        "ts sdk hello ({}): bytes={} core_instances={} core_modules={} imports={:?} exports={:?}",
        mode.arg(),
        bytes.len(),
        info.core_instances,
        info.core_modules,
        info.imports,
        info.exports
    );
    for imp in &info.imports {
        assert!(
            !imp.starts_with("wasi:"),
            "{}: wasi import {imp} (all: {:?})",
            mode.arg(),
            info.imports
        );
        assert!(
            imp.starts_with(OSTRO_PREFIX),
            "{}: unexpected import {imp} (all: {:?})",
            mode.arg(),
            info.imports
        );
    }
    assert!(info.core_instances >= 1);
    info
}

#[test]
fn quickjs_hello_imports_only_ostro_runtime_and_exports_only_workflows() {
    let info = build_and_inspect(Mode::Ts);
    for imp in &info.imports {
        assert!(
            imp.ends_with("@0.1.0"),
            "unversioned/foreign import {imp} (all: {:?})",
            info.imports
        );
    }
    for needed in NEEDED_IMPORTS {
        assert!(
            info.imports.iter().any(|i| i == needed),
            "missing import {needed} (all: {:?})",
            info.imports
        );
    }
    let exports: BTreeSet<&str> = info.exports.iter().map(String::as_str).collect();
    assert_eq!(exports, BTreeSet::from([WORKFLOWS]));
}

#[test]
fn starlingmonkey_hello_builds_with_ostro_only_imports() {
    let info = build_and_inspect(Mode::TsSm);
    assert!(
        info.exports.iter().any(|e| e == WORKFLOWS),
        "missing export {WORKFLOWS} (all: {:?})",
        info.exports
    );
}

/// Bundles with Rolldown (pinned toolchain, `sdks/ts/rolldown.ostro.mjs` merged, `ostro:*`
/// external like jco), parses the result with the toolchain's acorn and counts async
/// functions, `await` expressions and `for await` loops. Prints one JSON line.
const BUNDLE_SCRIPT: &str = r#"
import { pathToFileURL } from "node:url";
import { join } from "node:path";
import { readFileSync } from "node:fs";
const [toolchain, config, entry, out] = process.argv.slice(1);
const mod = (p) => import(pathToFileURL(join(toolchain, "node_modules", p)).href);
const { rolldown } = await mod("rolldown/dist/index.mjs");
const acorn = await mod("acorn/dist/acorn.mjs");
const cfg = (await import(pathToFileURL(config).href)).default;
const diagnostics = [];
const bundle = await rolldown({
  ...cfg,
  input: entry,
  platform: "neutral",
  external: [/^ostro:/],
  onLog(level, log) { diagnostics.push(`${level}: ${log.code ?? ""}`); },
});
await bundle.write({ file: out, format: "esm" });
await bundle.close();
const code = readFileSync(out, "utf8");
const ast = acorn.parse(code, { ecmaVersion: "latest", sourceType: "module" });
let asyncFns = 0, awaits = 0, forAwait = 0;
const walk = (n) => {
  if (Array.isArray(n)) { n.forEach(walk); return; }
  if (!n || typeof n.type !== "string") return;
  if (n.async === true) asyncFns++;
  if (n.type === "AwaitExpression") awaits++;
  if (n.type === "ForOfStatement" && n.await) forAwait++;
  for (const k of Object.keys(n)) if (k !== "type") { const v = n[k]; if (v && typeof v === "object") walk(v); }
};
walk(ast);
const tokens = (code.match(/async function|async \(/g) ?? []).length;
console.log(JSON.stringify({ bytes: code.length, asyncFns, awaits, forAwait, tokens, diagnostics: [...new Set(diagnostics)] }));
"#;

fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap_or_else(|e| panic!("mkdir {}: {e}", dst.display()));
    for entry in std::fs::read_dir(src).unwrap_or_else(|e| panic!("read {}: {e}", src.display())) {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        if name == "node_modules" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if entry.file_type().expect("file type").is_dir() {
            copy_tree(&from, &to);
        } else {
            std::fs::copy(&from, &to)
                .unwrap_or_else(|e| panic!("copy {} -> {}: {e}", from.display(), to.display()));
        }
    }
}

#[test]
fn bundle_has_no_unlowered_async_code() {
    let root = repo_root();
    // Installs the pinned toolchain if needed (build-guest.sh runs `npm ci`).
    build(Mode::Ts, &hello_dir()).unwrap_or_else(|e| panic!("{e}"));
    let toolchain = root.join("sdks/scripts/toolchain");
    assert!(
        toolchain
            .join("node_modules/rolldown/dist/index.mjs")
            .is_file(),
        "rolldown missing from {}",
        toolchain.display()
    );

    // Same staging as build-guest.sh: the guest dir, plus sdks/ts as node_modules/ostro.
    let check = root.join("target/ts-bundle-check");
    if check.exists() {
        std::fs::remove_dir_all(&check).expect("clean target/ts-bundle-check");
    }
    let stage = check.join("src");
    copy_tree(&hello_dir(), &stage);
    copy_tree(&root.join("sdks/ts"), &stage.join("node_modules/ostro"));
    let out_js = check.join("bundle.js");

    let res = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(BUNDLE_SCRIPT)
        .arg(&toolchain)
        .arg(root.join("sdks/ts/rolldown.ostro.mjs"))
        .arg(stage.join("index.ts"))
        .arg(&out_js)
        .output()
        .expect("spawn node (Node 24 required)");
    let stdout = String::from_utf8_lossy(&res.stdout);
    assert!(
        res.status.success(),
        "bundle check failed ({}):\n--- stdout\n{stdout}\n--- stderr\n{}",
        res.status,
        String::from_utf8_lossy(&res.stderr)
    );
    let line = stdout.lines().last().expect("bundle check output");
    println!("ts bundle check: {line} ({})", out_js.display());
    let v: serde_json::Value = serde_json::from_str(line).expect("bundle check JSON");
    for key in ["asyncFns", "awaits", "forAwait", "tokens"] {
        assert_eq!(
            v[key], 0,
            "unlowered async code in the bundle ({key}): {line}"
        );
    }
    assert!(v["bytes"].as_u64().unwrap_or(0) > 0, "empty bundle: {line}");
}
