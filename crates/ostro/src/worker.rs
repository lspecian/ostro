//! `ostro worker` (07-15, WORK-01): the remote worker process, and the
//! `ostro worker token create|list|revoke` client verbs.
//!
//! Configuration, lowest to highest precedence: [`WorkerConfig::default`], the `[worker]`
//! table of `--config FILE`, `OSTRO_WORKER_<KEY>` environment variables
//! ([`WorkerConfig::load`]), then the flags below. The join token comes from
//! `OSTRO_WORKER_TOKEN` (preferred), `--token-file` or `--token`; it is a [`JoinToken`],
//! so it never reaches a log line, an error or `--print-config`.
//!
//! The process runs `ostro_worker::run` semantics through [`Worker::new`] +
//! [`Worker::run`] (the same two calls `ostro_worker::run` makes) so it can observe the
//! session: once the first session is welcomed it prints one ready line on stdout,
//! `ostro worker ready: id=<worker_id> server=<endpoint> slots=<n>`.
//!
//! Shutdown: the first SIGINT / SIGTERM cancels the worker, which drains (sends `Drain`,
//! evicts in-flight activations, waits up to [`ostro_worker::drain::DRAIN_BOUND`] for them
//! and closes the session) and exits 0. A second signal exits at once with 130. A drain
//! that overruns `DRAIN_BOUND` + [`DRAIN_GRACE`] exits 1.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use ostro_cli::commands::worker_tokens::{self, WorkerTokenCommand};
use ostro_server::config::LogSection;
use ostro_server::logging::init_tracing;
use ostro_worker::config::parse_labels;
use ostro_worker::{JoinToken, Worker, WorkerConfig, WorkerError, WorkerEvent};
use serde_json::Value;

use crate::{Signals, fail};

/// Extra time on top of the worker's own drain bound before the process gives up.
pub const DRAIN_GRACE: Duration = Duration::from_secs(5);

/// `ostro worker [flags]` or `ostro worker token ...`.
#[derive(Debug, Args)]
pub struct WorkerCli {
    #[command(subcommand)]
    pub command: Option<WorkerSub>,
    #[command(flatten)]
    pub run: WorkerArgs,
}

/// `ostro worker` subcommands.
#[derive(Debug, Subcommand)]
pub enum WorkerSub {
    /// Manage worker join tokens (create, list, revoke).
    Token {
        #[command(subcommand)]
        command: WorkerTokenCommand,
    },
}

/// The worker process flags (each overrides the file and `OSTRO_WORKER_*`).
#[derive(Debug, Args)]
pub struct WorkerArgs {
    /// Join URL of a server's client listener (https://host:7700; http:// only for
    /// loopback).
    #[arg(long, value_name = "URL")]
    pub join: Option<String>,
    /// Join token wkr_<id>.<secret>. Prefer OSTRO_WORKER_TOKEN or --token-file: argv is
    /// visible to other local users.
    #[arg(long, value_name = "TOKEN", value_parser = parse_token, conflicts_with = "token_file")]
    pub token: Option<JoinToken>,
    /// Read the join token from this file (surrounding whitespace ignored).
    #[arg(long, value_name = "FILE")]
    pub token_file: Option<PathBuf>,
    /// sha256:<hex> of the cluster CA certificate, pinned during the join.
    #[arg(long, value_name = "sha256:HEX")]
    pub ca_hash: Option<String>,
    /// Labels matched by queue selectors: k=v,k2=v2.
    #[arg(long, value_name = "K=V,..", value_parser = parse_labels)]
    pub labels: Option<BTreeMap<String, String>>,
    /// Concurrent activation slots [default: available parallelism].
    #[arg(long, value_name = "N")]
    pub slots: Option<u32>,
    /// Module / AOT cache directory [default: ./.ostro-worker/cache].
    #[arg(long, value_name = "DIR")]
    pub cache_dir: Option<PathBuf>,
    /// Engine memory budget: bytes, or with a unit (512MiB, 2GiB, 1G) [default: 1GiB].
    #[arg(long, value_name = "BYTES", value_parser = parse_bytes)]
    pub memory: Option<u64>,
    /// External client certificate (PEM; with --tls-key and --tls-ca replaces the join).
    #[arg(long, value_name = "FILE")]
    pub tls_cert: Option<PathBuf>,
    /// External client key (PEM).
    #[arg(long, value_name = "FILE")]
    pub tls_key: Option<PathBuf>,
    /// CA bundle that issued the cluster listener's certificate (PEM).
    #[arg(long, value_name = "FILE")]
    pub tls_ca: Option<PathBuf>,
    /// Cluster listener endpoints host:port (repeatable or comma-separated; required with
    /// the external certificate, learned from the join otherwise).
    #[arg(long = "endpoint", value_name = "HOST:PORT", value_delimiter = ',')]
    pub endpoints: Vec<String>,
    /// Stable worker id ([A-Za-z0-9._-]{1,64}) [default: generated].
    #[arg(long, value_name = "ID")]
    pub worker_id: Option<String>,
    /// TOML file with a [worker] table (must exist when given).
    #[arg(long, short, value_name = "FILE")]
    pub config: Option<PathBuf>,
    /// Print the effective configuration as TOML (token redacted) and exit.
    #[arg(long)]
    pub print_config: bool,
}

fn parse_token(s: &str) -> Result<JoinToken, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty token".into());
    }
    Ok(JoinToken::new(t))
}

/// `1048576`, `512MiB`, `2GiB`, `1G`, `64K` (binary multiples for every unit).
pub fn parse_bytes(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid size {s:?} (expected e.g. 1073741824, 512MiB, 2GiB)"))?;
    let mul: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        "t" | "tb" | "tib" => 1 << 40,
        other => return Err(format!("invalid size {s:?}: unknown unit {other:?}")),
    };
    n.checked_mul(mul)
        .ok_or_else(|| format!("invalid size {s:?}: too large"))
}

/// Defaults < file < env < flags.
pub fn build_config(
    a: &WorkerArgs,
    env: &HashMap<String, String>,
) -> Result<WorkerConfig, WorkerError> {
    let mut cfg = WorkerConfig::load(a.config.as_deref(), env)?;
    if let Some(v) = &a.join {
        cfg.join = Some(v.clone());
    }
    if let Some(t) = &a.token {
        cfg.token = Some(t.clone());
    }
    if let Some(p) = &a.token_file {
        let text = std::fs::read_to_string(p).map_err(|e| {
            WorkerError::Config(format!("cannot read --token-file {}: {e}", p.display()))
        })?;
        let t = text.trim();
        if t.is_empty() {
            return Err(WorkerError::Config(format!(
                "--token-file {} is empty",
                p.display()
            )));
        }
        cfg.token = Some(JoinToken::new(t));
    }
    if let Some(v) = &a.ca_hash {
        cfg.ca_hash = Some(v.clone());
    }
    if let Some(v) = &a.labels {
        cfg.labels = v.clone();
    }
    if let Some(v) = a.slots {
        cfg.slots = v;
    }
    if let Some(v) = &a.cache_dir {
        cfg.cache_dir = v.clone();
    }
    if let Some(v) = a.memory {
        cfg.memory_bytes = v;
    }
    if let Some(v) = &a.tls_cert {
        cfg.tls_cert = Some(v.clone());
    }
    if let Some(v) = &a.tls_key {
        cfg.tls_key = Some(v.clone());
    }
    if let Some(v) = &a.tls_ca {
        cfg.tls_ca = Some(v.clone());
    }
    if !a.endpoints.is_empty() {
        cfg.endpoints = a
            .endpoints
            .iter()
            .map(|e| e.trim().to_owned())
            .filter(|e| !e.is_empty())
            .collect();
    }
    if let Some(v) = &a.worker_id {
        cfg.worker_id = Some(v.clone());
    }
    Ok(cfg)
}

/// A TOML literal for a JSON value (strings, numbers, booleans, arrays, one level of
/// tables as inline tables).
fn toml_value(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let parts: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("{} = {}", Value::String(k.clone()), toml_value(v)))
                .collect();
            if parts.is_empty() {
                "{}".to_owned()
            } else {
                format!("{{ {} }}", parts.join(", "))
            }
        }
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(toml_value).collect::<Vec<_>>().join(", ")
        ),
        other => other.to_string(),
    }
}

/// `[worker]` as TOML (the token serialises as `<redacted>`).
pub fn render_config(cfg: &WorkerConfig) -> String {
    let mut out = String::from("[worker]\n");
    if let Ok(Value::Object(m)) = serde_json::to_value(cfg) {
        for (k, v) in m.iter().filter(|(_, v)| !v.is_null()) {
            out.push_str(&format!("{k} = {}\n", toml_value(v)));
        }
    }
    out
}

/// Token pair of the worker's shutdown signal: the token type is the one
/// [`Worker::run`] takes (inferred), one end for the worker and one for the signal path.
fn shutdown_pair<T: Default + Clone>() -> (T, T) {
    let t = T::default();
    (t.clone(), t)
}

/// `ostro worker ...`.
pub fn main(cli: WorkerCli, started: Instant) -> ExitCode {
    if let Some(WorkerSub::Token { command }) = &cli.command {
        return worker_tokens::run(command).into();
    }
    let env: HashMap<String, String> = std::env::vars().collect();
    let cfg = match build_config(&cli.run, &env) {
        Ok(c) => c,
        Err(e) => return usage(e),
    };
    if cli.run.print_config {
        print!("{}", render_config(&cfg));
        return ExitCode::SUCCESS;
    }
    let worker = match Worker::new(cfg.clone()) {
        Ok(w) => w,
        Err(e) => return usage(e),
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ostro-worker")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(format!("cannot start the async runtime: {e}")),
    };
    init_tracing(&LogSection::default());
    rt.block_on(run(worker, cfg.slots, started))
}

/// Configuration errors: one line on stderr, exit 2 (the CLI usage code).
fn usage(e: WorkerError) -> ExitCode {
    eprintln!("ostro: error: {e}");
    ExitCode::from(2)
}

async fn run(worker: Worker, slots: u32, started: Instant) -> ExitCode {
    let mut signals = match Signals::new() {
        Ok(s) => s,
        Err(e) => return fail(format!("cannot install signal handlers: {e}")),
    };
    let mut events = worker.subscribe();
    let (shutdown, trigger) = shutdown_pair();
    let id = worker.worker_id().to_owned();
    let main = worker.run(shutdown);
    tokio::pin!(main);
    let mut ready_printed = false;
    let mut events_open = true;
    let res = loop {
        tokio::select! {
            r = &mut main => break r,
            sig = signals.recv() => {
                tracing::info!(signal = sig, "draining");
                trigger.cancel();
                break drain(&mut main, &mut signals).await;
            }
            ev = events.recv(), if events_open => match ev {
                Ok(WorkerEvent::Connected { endpoint, .. }) if !ready_printed => {
                    ready_printed = true;
                    print_ready(&id, &endpoint, slots, started);
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => events_open = false,
            },
        }
    };
    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(e),
    }
}

/// After the first signal: wait for the drain (bounded); a second signal exits at once.
async fn drain<F>(
    main: &mut std::pin::Pin<&mut F>,
    signals: &mut Signals,
) -> Result<(), WorkerError>
where
    F: std::future::Future<Output = Result<(), WorkerError>>,
{
    let bound = ostro_worker::drain::DRAIN_BOUND + DRAIN_GRACE;
    tokio::select! {
        r = main.as_mut() => r,
        sig = signals.recv() => {
            eprintln!("ostro: {sig} again: exiting without waiting for the drain");
            std::process::exit(130);
        }
        () = tokio::time::sleep(bound) => {
            eprintln!("ostro: error: the drain did not finish within {bound:?}");
            std::process::exit(1);
        }
    }
}

/// `ostro worker ready: id=<id> server=<endpoint> slots=<n>` (the 07-16 harness parses
/// it), with the time since process start in the log.
fn print_ready(id: &str, endpoint: &str, slots: u32, started: Instant) {
    use std::io::Write as _;
    let mut out = std::io::stdout().lock();
    let _ = writeln!(
        out,
        "ostro worker ready: id={id} server={endpoint} slots={slots}"
    );
    let _ = out.flush();
    tracing::info!(
        ready_ms = started.elapsed().as_millis() as u64,
        "worker ready"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes() {
        assert_eq!(parse_bytes("1048576").unwrap(), 1 << 20);
        assert_eq!(parse_bytes("512MiB").unwrap(), 512 << 20);
        assert_eq!(parse_bytes("2GiB").unwrap(), 2 << 30);
        assert_eq!(parse_bytes("1G").unwrap(), 1 << 30);
        assert_eq!(parse_bytes("64k").unwrap(), 64 << 10);
        assert!(parse_bytes("lots").is_err());
        assert!(parse_bytes("5XB").is_err());
        assert!(parse_bytes("99999999999T").is_err());
    }

    #[test]
    fn rendered_config_redacts_the_token() {
        let cfg = WorkerConfig {
            join: Some("https://h:7700".into()),
            token: Some(JoinToken::new("wkr_1.SECRET")),
            labels: [("zone".to_owned(), "a".to_owned())].into(),
            ..WorkerConfig::default()
        };
        let t = render_config(&cfg);
        assert!(t.starts_with("[worker]\n"), "{t}");
        assert!(t.contains("token = \"<redacted>\""), "{t}");
        assert!(t.contains("labels = { \"zone\" = \"a\" }"), "{t}");
        assert!(!t.contains("SECRET"), "{t}");
    }
}
