//! Export a stopped server's run as a QA-02 replay-corpus entry (plan 05-12).
//!
//! ```text
//! cargo run -p ostro-server --example export_run -- --data-dir D --run R --entry NAME \
//!     [--node-id N] [--corpus DIR]
//! ```
//!
//! The data dir must belong to a **stopped** dev/test node. It is never written: the tool
//! copies it to a temp dir and opens the copy (meta in its own Raft dir, partitions in
//! [`StartMode::Inspect`]). From the copy it reads:
//! - the run record (partition from the run id; the run must be terminal: completed, failed,
//!   cancelled or continued-as-new);
//! - the journal through [`journal_io::read_journal`] (blob payloads fetched and inlined), and
//!   writes it as `journal.bin` (`u32 LE length ‖ JournalEvent protobuf` per event, the same
//!   bytes as the `Events` rows for inline payloads; checked row by row);
//! - the module component from the node blob store by the record's `module_sha`;
//! - the cluster's await-token key from meta (`__ostro/token-key`), so replayed token minting
//!   reproduces byte-identically. Export refuses when the key cannot be read.
//!
//! **The exported `token_key_hex` is the data dir's token key.** Export only from throwaway
//! dev/test data dirs (`ostro dev`, test servers): their key is ephemeral and not a secret.
//! Never export from a production data dir; a production key must never be committed.
//!
//! Before writing, the entry is replayed on the engine (production linker, zero appends,
//! same terminal outcome); the entry directory must not exist (entries are never
//! re-recorded, see tests/replay-corpus/README.md).

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::replay_journal;
use ostro_meta::MetaHandle;
use ostro_partition::store::keys::event_key;
use ostro_partition::store::values::RunStatus;
use ostro_partition::{Node, NodeConfig, StartMode, Table, query};
use ostro_server::journal_io;
use ostro_types::journal::{Event, EventBody, Failure, encode_event, encode_result};
use ostro_types::payload::payload_ref;
use ostro_types::{PartitionCount, RunId};
use sha2::{Digest, Sha256};

struct Args {
    data_dir: PathBuf,
    run: RunId,
    entry: String,
    node_id: u64,
    corpus: PathBuf,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let (mut data_dir, mut run, mut entry) = (None, None, None);
    let mut node_id = 1u64;
    let mut corpus = repo_root().join("tests/replay-corpus");
    while let Some(arg) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(val()?)),
            "--run" => {
                let v = val()?;
                run = Some(v.parse::<RunId>().map_err(|e| format!("--run {v}: {e}"))?);
            }
            "--entry" => entry = Some(val()?),
            "--node-id" => node_id = val()?.parse().map_err(|e| format!("--node-id: {e}"))?,
            "--corpus" => corpus = PathBuf::from(val()?),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let entry = entry.ok_or("--entry is required")?;
    if entry.is_empty()
        || !entry
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(format!("entry name {entry:?} must match [a-z0-9-]+"));
    }
    Ok(Args {
        data_dir: data_dir.ok_or("--data-dir is required")?,
        run: run.ok_or("--run is required")?,
        entry,
        node_id,
        corpus,
    })
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn copy_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let to = dst.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &to)?;
        } else {
            std::fs::copy(e.path(), &to)?;
        }
    }
    Ok(())
}

/// `partitions = N` from `node.toml`.
fn partition_count(data_dir: &Path) -> Result<PartitionCount, String> {
    let text = std::fs::read_to_string(data_dir.join("node.toml"))
        .map_err(|e| format!("node.toml: {e} (is this an ostro data dir?)"))?;
    let n: u16 = text
        .lines()
        .find_map(|l| l.trim().strip_prefix("partitions = "))
        .ok_or("node.toml has no `partitions = N`")?
        .trim()
        .parse()
        .map_err(|e| format!("node.toml partitions: {e}"))?;
    PartitionCount::new(n).ok_or_else(|| "node.toml partitions = 0".to_string())
}

// ------------------------------------------------------------------------- encoding

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Protobuf bytes of `JournalEvent{seq, kind, payload: PayloadRef{inline: bytes}}` exactly as
/// prost encodes them (fields in tag order, proto3 zero scalars omitted, the message and
/// oneof fields always present). ostro-server has no direct prost dependency; the encoder is
/// checked against the stored `Events` rows of every inline event (see `export`).
fn journal_event_bytes(seq: u64, kind: u32, payload: &[u8]) -> Vec<u8> {
    let mut pr = vec![0x0a];
    varint(&mut pr, payload.len() as u64);
    pr.extend_from_slice(payload);
    let mut out = Vec::with_capacity(pr.len() + 16);
    if seq != 0 {
        out.push(0x08);
        varint(&mut out, seq);
    }
    if kind != 0 {
        out.push(0x10);
        varint(&mut out, u64::from(kind));
    }
    out.push(0x1a);
    varint(&mut out, pr.len() as u64);
    out.extend_from_slice(&pr);
    out
}

/// TOML basic string.
fn toml_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\u{:04X}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `(expected, terminal payload bytes)` from the journal's last event (same definition as
/// crates/ostro-engine/tests/replay_corpus.rs).
fn terminal(journal: &[Event]) -> Result<(&'static str, Vec<u8>, ActivationOutcome), String> {
    match journal.last().map(|e| &e.body) {
        Some(EventBody::RunCompleted(c)) => Ok((
            "completed",
            c.output.data.clone(),
            ActivationOutcome::Completed {
                output: c.output.clone(),
            },
        )),
        Some(EventBody::RunFailed(f)) => Ok((
            if f.failure == Failure::Cancelled {
                "cancelled"
            } else {
                "failed"
            },
            encode_result(&f.failure).bytes,
            ActivationOutcome::Failed {
                failure: f.failure.clone(),
            },
        )),
        Some(EventBody::RunContinuedAsNew(c)) => Ok((
            "continued_as_new",
            encode_result(&c.continuation).bytes,
            ActivationOutcome::ContinuedAsNew {
                continuation: c.continuation.clone(),
            },
        )),
        other => Err(format!(
            "the journal does not end in RunCompleted/RunFailed/RunContinuedAsNew \
             (last: {:?}); terminated / timed-out runs are not corpus entries",
            other.map(EventBody::kind)
        )),
    }
}

fn git_describe() -> String {
    std::process::Command::new("git")
        .args(["describe", "--always", "--dirty", "--abbrev=12"])
        .current_dir(repo_root())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn wasmtime_pin() -> String {
    let text = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap_or_default();
    text.lines()
        .find(|l| l.starts_with("wasmtime = "))
        .and_then(|l| l.split("version = \"=").nth(1))
        .and_then(|r| r.split('"').next())
        .unwrap_or("unknown")
        .to_string()
}

fn target_triple() -> String {
    let arch = std::env::consts::ARCH;
    match std::env::consts::OS {
        "linux" => {
            let env = if cfg!(target_env = "musl") {
                "musl"
            } else {
                "gnu"
            };
            format!("{arch}-unknown-linux-{env}")
        }
        "macos" => format!("{arch}-apple-darwin"),
        os => format!("{arch}-unknown-{os}"),
    }
}

fn today() -> String {
    jiff::Timestamp::now().strftime("%Y-%m-%d").to_string()
}

// --------------------------------------------------------------------------- export

/// What the stopped data dir yields for one run.
struct Exported {
    workflow: String,
    input: Vec<u8>,
    partition: u16,
    run_id: String,
    token_key: [u8; 32],
    module: Vec<u8>,
    journal: Vec<Event>,
    bin: Vec<u8>,
}

async fn read_token_key(copy: &Path, node_id: u64) -> Result<[u8; 32], String> {
    let meta = MetaHandle::start(&copy.join("meta"), node_id)
        .await
        .map_err(|e| format!("cannot open meta to read the token key: {e}"))?;
    let key = meta.view().read(|s| s.token_key());
    meta.stop().await;
    key.ok_or_else(|| "the token key is not readable from meta: refusing to export".to_string())
}

async fn read_run(copy: &Path, run: RunId) -> Result<Exported, String> {
    let count = partition_count(copy)?;
    let mut cfg = NodeConfig::new(copy.to_path_buf(), count);
    cfg.mode = StartMode::Inspect;
    let node = Node::open(cfg)
        .await
        .map_err(|e| format!("open node: {e}"))?;
    let res = read_run_on(&node, run).await;
    let _ = node.shutdown().await;
    res
}

async fn read_run_on(node: &Node, run: RunId) -> Result<Exported, String> {
    let p = run.partition();
    let h = node
        .handle(p)
        .ok_or_else(|| format!("partition {} is not ready", p.0))?;
    let rec = h
        .read_run(&run)
        .map_err(|e| format!("read run: {e}"))?
        .ok_or_else(|| format!("run {run} not found on partition {}", p.0))?;
    if !matches!(
        rec.status,
        RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled | RunStatus::ContinuedAsNew
    ) {
        return Err(format!(
            "run {run} is {:?}: only completed, failed, cancelled or continued-as-new runs are \
             corpus entries",
            rec.status
        ));
    }
    let journal = journal_io::read_journal(h, &run)
        .await
        .map_err(|e| e.to_string())?;
    let (workflow, input) = match journal.first().map(|e| &e.body) {
        Some(EventBody::RunStarted(rs)) => (rs.workflow.clone(), rs.input.data.clone()),
        other => return Err(format!("seq 0 is not RunStarted: {other:?}")),
    };

    // journal.bin, checked against the stored rows: every inline row's payload must be the
    // re-encoded event, and its stored protobuf bytes must equal ours.
    let rows = query::journal_rows(&h.reader(), &run, 0, usize::MAX)
        .map_err(|e| format!("journal rows: {e}"))?;
    if rows.len() != journal.len() {
        return Err(format!("{} rows vs {} events", rows.len(), journal.len()));
    }
    let mut bin = Vec::new();
    let mut checked = 0usize;
    for (ev, (seq, kind, payload)) in journal.iter().zip(&rows) {
        let (k, bytes) = encode_event(ev);
        if (ev.seq, k) != (*seq, *kind) {
            return Err(format!("seq {seq}: row/event mismatch"));
        }
        let msg = journal_event_bytes(ev.seq, k, &bytes);
        if let Some(payload_ref::Kind::Inline(stored)) = &payload.kind {
            if stored.as_ref() != bytes.as_slice() {
                return Err(format!(
                    "seq {seq}: stored event bytes do not re-encode identically"
                ));
            }
            let raw = h
                .reader()
                .get(Table::Events, &event_key(&run, *seq))
                .map_err(|e| format!("events row {seq}: {e}"))?
                .ok_or_else(|| format!("events row {seq} missing"))?;
            if raw != msg {
                return Err(format!(
                    "seq {seq}: journal.bin record != stored Events row"
                ));
            }
            checked += 1;
        }
        bin.extend_from_slice(&(msg.len() as u32).to_le_bytes());
        bin.extend_from_slice(&msg);
    }
    eprintln!(
        "export_run: {} events ({checked} inline rows byte-checked)",
        journal.len()
    );

    let sha = rec
        .module_sha
        .ok_or_else(|| format!("run {run} has no module_sha"))?;
    let path = h.blobs().path_of(&ostro_blob::BlobRef {
        sha256: sha,
        len: 0,
    });
    let module = std::fs::read(&path).map_err(|e| format!("module {}: {e}", path.display()))?;
    if <[u8; 32]>::from(Sha256::digest(&module)) != sha {
        return Err(format!("module blob {} fails its sha256", path.display()));
    }
    Ok(Exported {
        workflow,
        input,
        partition: p.0,
        run_id: run.to_string(),
        token_key: [0; 32],
        module,
        journal,
        bin,
    })
}

async fn export(a: &Args) -> Result<PathBuf, String> {
    let dir = a.corpus.join(&a.entry);
    if dir.exists() {
        return Err(format!(
            "{} exists: entries are never re-recorded (add a new entry instead)",
            dir.display()
        ));
    }
    let tmp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let copy = tmp.path().join("data");
    copy_dir(&a.data_dir, &copy).map_err(|e| format!("copy {}: {e}", a.data_dir.display()))?;
    let token_key = read_token_key(&copy, a.node_id).await?;
    let mut x = read_run(&copy, a.run).await?;
    x.token_key = token_key;
    drop(tmp);

    let (expected, terminal_bytes, journaled) = terminal(&x.journal)?;

    // The entry must replay clean before it is written.
    let rt = Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            token_key: x.token_key,
            ..RuntimeOptions::default()
        },
    )
    .map_err(|e| format!("runtime: {e}"))?;
    let p = rt
        .prepare(&x.module)
        .map_err(|e| format!("prepare (production linker): {e}"))?;
    let run = RunRef {
        partition: x.partition,
        run_id: x.run_id.clone(),
    };
    let (outcome, appended) = replay_journal(&rt, &p, run, x.journal.clone()).await;
    if outcome != journaled || appended != 0 {
        return Err(format!(
            "the exported journal does not replay clean (outcome {outcome:?}, {appended} \
             appended): this is a determinism bug, report it; nothing was written"
        ));
    }

    let input_json = serde_json::from_slice::<serde_json::Value>(&x.input)
        .ok()
        .and_then(|_| String::from_utf8(x.input.clone()).ok());
    let mut toml = String::from(
        "# QA-02 replay-corpus entry. Never edit or re-record; see ../README.md.\n\
         # Exported from a dev/test data dir: token_key_hex is that dir's ephemeral key.\n",
    );
    toml.push_str(&format!("name = {}\n", toml_str(&a.entry)));
    toml.push_str(&format!("workflow = {}\n", toml_str(&x.workflow)));
    if let Some(i) = &input_json {
        toml.push_str(&format!("input_json = {}\n", toml_str(i)));
    }
    toml.push_str(&format!("expected = {}\n", toml_str(expected)));
    toml.push_str(&format!("partition = {}\n", x.partition));
    toml.push_str(&format!("run_id = {}\n", toml_str(&x.run_id)));
    toml.push_str(&format!("token_key_hex = \"{}\"\n", hex(&x.token_key)));
    toml.push_str(&format!(
        "terminal_payload_sha256 = \"{}\"\n",
        hex(&Sha256::digest(&terminal_bytes))
    ));
    toml.push_str(&format!("journal_events = {}\n", x.journal.len()));
    toml.push_str(&format!(
        "module_sha256 = \"{}\"\n",
        hex(&Sha256::digest(&x.module))
    ));
    toml.push_str(&format!(
        "journal_sha256 = \"{}\"\n",
        hex(&Sha256::digest(&x.bin))
    ));
    toml.push_str("\n[recorded_with]\n");
    toml.push_str(&format!("ostro = {}\n", toml_str(&git_describe())));
    toml.push_str(&format!("wasmtime = {}\n", toml_str(&wasmtime_pin())));
    toml.push_str(&format!("target = {}\n", toml_str(&target_triple())));
    toml.push_str(&format!("date = {}\n", toml_str(&today())));
    toml.push_str("tool = \"export_run\"\n");
    toml.push_str(&format!(
        "command = {}\n",
        toml_str(&format!("export_run --run {} --entry {}", a.run, a.entry))
    ));

    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    std::fs::create_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    std::fs::write(dir.join("module.wasm"), &x.module).map_err(|e| format!("module: {e}"))?;
    std::fs::write(dir.join("journal.bin"), &x.bin).map_err(|e| format!("journal: {e}"))?;
    std::fs::write(dir.join("entry.toml"), toml).map_err(|e| format!("entry.toml: {e}"))?;
    println!(
        "exported {} ({expected}, {} events)",
        dir.display(),
        x.journal.len()
    );
    Ok(dir)
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("export_run: {e}");
            eprintln!(
                "usage: export_run --data-dir D --run R --entry NAME [--node-id N] [--corpus DIR]"
            );
            return ExitCode::from(2);
        }
    };
    match export(&args).await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("export_run: {e}");
            ExitCode::FAILURE
        }
    }
}
