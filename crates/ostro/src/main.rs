//! The `ostro` binary: `ostro dev`, `ostro server`, `ostro openapi`, `ostro build-info`,
//! and the CLI-06 client verbs (`run`, `ls`, `inspect`, `history`, `signal`, `cancel`,
//! `terminate`, `retry`, `result`, `token complete`, `deploy`) from [`ostro_cli`], which
//! talk to a server over REST (`--server` / `OSTRO_SERVER`, default
//! `http://127.0.0.1:7700`).
//!
//! `ostro backup` / `ostro restore` (08-18): the online backup downloads
//! `GET /v1/admin/backup`; the offline backup (`--data-dir`) and restore run here on
//! [`ostro_backup`] (see docs/bottomless.md, "Restore runbook").
//!
//! `dev` and `server` load the layered configuration (base profile < optional TOML file
//! < `OSTRO_<SECTION>__<KEY>` env < flags), start [`ostro_server::Server`], print one
//! ready line on stdout, then wait for SIGINT / SIGTERM (or the API server stopping) and
//! shut down gracefully (an `--ephemeral` data directory is removed on the way out).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::{Parser, Subcommand};
use ostro_server::config::{EnvSource, Overrides, ServerConfig, load_layered};
use ostro_server::logging::init_tracing;
use ostro_server::{Server, ServerHandle};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(name = "ostro", version, about = "Ostro durable Wasm workflow engine")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Server + embedded executor + UI shell, zero config.
    Dev {
        /// Keep all state in a throwaway temp directory (tmpfs) removed on exit.
        #[arg(long)]
        ephemeral: bool,
        /// Listen address for HTTP and gRPC [default: 127.0.0.1:7700].
        #[arg(long, env = "OSTRO_LISTEN")]
        listen: Option<SocketAddr>,
        /// Data directory [default: ./.ostro]; ignored with --ephemeral.
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Single-node server; TOML file and OSTRO_<SECTION>__<KEY> env overrides.
    Server {
        /// TOML configuration file (must exist when given).
        #[arg(long, short, env = "OSTRO_CONFIG")]
        config: Option<PathBuf>,
        /// Print the effective configuration as TOML and exit.
        #[arg(long)]
        print_config: bool,
        /// Listen address for HTTP and gRPC (overrides file and env).
        #[arg(long)]
        listen: Option<SocketAddr>,
        /// Data directory (overrides file and env).
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Print the OpenAPI document served at /openapi.json.
    Openapi,
    /// Print build information (version, target triple, allocator).
    BuildInfo,
    /// Write a backup file (tar): online from a running server, or offline (--data-dir).
    Backup(ostro_cli::commands::backup::BackupArgs),
    /// Rebuild a data directory from a bucket (s3://, file://) or a backup file, verified.
    Restore(Box<ostro_cli::commands::backup::RestoreArgs>),
    #[command(flatten)]
    Client(Box<ostro_cli::ClientCommand>),
}

/// `build-info` suffix of a failpoint-enabled build (the crash campaign checks it).
#[cfg(feature = "failpoints")]
const FAILPOINTS_INFO: &str = " failpoints=on";
#[cfg(not(feature = "failpoints"))]
const FAILPOINTS_INFO: &str = "";

fn main() -> ExitCode {
    // Plan 05-18: a `--features failpoints` build reads the FAILPOINTS env var
    // (`name=action;...`, e.g. `executor::before_append=return(enospc)`) and keeps the
    // scenario for the whole process. Without the feature no failpoint code is compiled.
    #[cfg(feature = "failpoints")]
    let _failpoints = ::fail::FailScenario::setup();
    let started = Instant::now();
    let cli = Cli::parse();
    match cli.command {
        Some(Command::BuildInfo) => {
            println!(
                "ostro {} target={} allocator=mimalloc{FAILPOINTS_INFO}",
                env!("CARGO_PKG_VERSION"),
                env!("OSTRO_TARGET")
            );
            ExitCode::SUCCESS
        }
        Some(Command::Openapi) => match serde_json::to_string_pretty(&ostro_api::openapi()) {
            Ok(doc) => {
                println!("{doc}");
                ExitCode::SUCCESS
            }
            Err(e) => fail(format!("cannot render the OpenAPI document: {e}")),
        },
        Some(Command::Dev {
            ephemeral,
            listen,
            data_dir,
        }) => {
            let overrides = Overrides { listen, data_dir };
            match load_layered(
                ServerConfig::dev_defaults(),
                None,
                false,
                &EnvSource::Process,
                &overrides,
            ) {
                Ok(mut cfg) => {
                    if ephemeral {
                        cfg.node.ephemeral = true;
                    }
                    run_server("ostro dev", cfg, started)
                }
                Err(e) => fail(e),
            }
        }
        Some(Command::Server {
            config,
            print_config,
            listen,
            data_dir,
        }) => {
            let overrides = Overrides { listen, data_dir };
            let cfg = match load_layered(
                ServerConfig::server_defaults(),
                config.as_deref(),
                config.is_some(),
                &EnvSource::Process,
                &overrides,
            ) {
                Ok(cfg) => cfg,
                Err(e) => return fail(e),
            };
            if print_config {
                print!("{}", cfg.to_toml_string());
                return ExitCode::SUCCESS;
            }
            run_server("ostro server", cfg, started)
        }
        Some(Command::Backup(a)) => run_backup(&a),
        Some(Command::Restore(a)) => run_restore(&a),
        Some(Command::Client(cmd)) => ostro_cli::run(*cmd).into(),
        None => {
            use clap::CommandFactory;
            Cli::command().print_help().expect("print help");
            println!();
            ExitCode::SUCCESS
        }
    }
}

/// One-line error on stderr, exit code 1.
fn fail(msg: impl std::fmt::Display) -> ExitCode {
    eprintln!("ostro: error: {msg}");
    ExitCode::FAILURE
}

fn runtime() -> Result<tokio::runtime::Runtime, ExitCode> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ostro")
        .build()
        .map_err(|e| fail(format!("cannot start the async runtime: {e}")))
}

/// `ostro backup`: online (REST download) or offline (`--data-dir`, Inspect mode).
fn run_backup(a: &ostro_cli::commands::backup::BackupArgs) -> ExitCode {
    use ostro_cli::commands::backup as cli;
    let rt = match runtime() {
        Ok(rt) => rt,
        Err(code) => return code,
    };
    let Some(dir) = &a.data_dir else {
        return rt.block_on(cli::run_online(a)).into();
    };
    // The server's default clock (the one injectable clock, system time here).
    let clock = ostro_server::ServerOptions::default().clock;
    match rt.block_on(ostro_backup::archive::backup_offline(
        dir, a.node_id, clock, &a.to,
    )) {
        Ok(rep) => cli::print_backup(
            &serde_json::json!({
                "file": a.to.display().to_string(),
                "mode": "offline",
                "bytes": rep.bytes,
                "entries": rep.entries,
                "generation": rep.manifest.generation,
            }),
            a.json,
        )
        .into(),
        Err(e) => fail(format!("offline backup: {e}")),
    }
}

/// `ostro restore`.
fn run_restore(a: &ostro_cli::commands::backup::RestoreArgs) -> ExitCode {
    use ostro_backup::restore::{At, RestoreOptions, RestoreSource, restore};
    use ostro_cli::commands::backup as cli;
    let env = |k: &str| std::env::var(k).ok();
    let source = match cli::classify_from(&a.from) {
        cli::FromArg::Url(url) => {
            let (access_key_id, secret_access_key, session_token) = cli::env_credentials(env);
            let (endpoint, region) = cli::endpoint_and_region(a, env);
            let cfg = ostro_backup::BackupConfig {
                url: Some(url.clone()),
                endpoint,
                region,
                url_style: match a.url_style {
                    cli::UrlStyleArg::Path => ostro_backup::UrlStyle::Path,
                    cli::UrlStyleArg::Virtual => ostro_backup::UrlStyle::Virtual,
                },
                access_key_id,
                secret_access_key,
                session_token,
                allow_http: a.allow_http,
                tls_roots: a.tls_roots_file.clone().map_or(
                    ostro_backup::TlsRoots::Platform,
                    ostro_backup::TlsRoots::File,
                ),
                ..ostro_backup::BackupConfig::default()
            };
            RestoreSource::Url {
                url,
                options: ostro_backup::fence::store_options(&cfg),
            }
        }
        cli::FromArg::File(p) => {
            if !p.is_file() {
                return fail(format!(
                    "--from {}: not a file (use s3://bucket/prefix, file:///abs/path or a .tar backup file)",
                    p.display()
                ));
            }
            RestoreSource::Tar(p)
        }
    };
    let mut opts = RestoreOptions::new(&a.data_dir);
    opts.at = a.at.map(|at| match at {
        cli::AtArg::Generation(g) => At::Generation(g),
        cli::AtArg::TimeMs(t) => At::TimeMs(t),
    });
    opts.takeover = a.takeover;
    opts.parallelism = a.parallelism.max(1);
    opts.node_id = a.node_id;
    let rt = match runtime() {
        Ok(rt) => rt,
        Err(code) => return code,
    };
    match rt.block_on(restore(source, opts)) {
        Ok(report) => match serde_json::to_value(&report) {
            Ok(v) => cli::print_restore_report(&v, a.json).into(),
            Err(e) => fail(format!("restore report: {e}")),
        },
        Err(e) => fail(format!("restore failed: {e}")),
    }
}

/// Start the server, print the ready line, wait for a signal, shut down.
fn run_server(name: &str, cfg: ServerConfig, started: Instant) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ostro")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(format!("cannot start the async runtime: {e}")),
    };
    init_tracing(&cfg.effective_log_section());
    rt.block_on(async move {
        // Register the handlers before startup so a signal during startup is not lost
        // (it is delivered as soon as startup finishes) and does not kill the process
        // with a half-created ephemeral directory.
        let mut signals = match Signals::new() {
            Ok(s) => s,
            Err(e) => return fail(format!("cannot install signal handlers: {e}")),
        };
        let handle = match Server::start(cfg).await {
            Ok(h) => h,
            Err(e) => return fail(e),
        };
        print_ready(name, &handle, started);
        tokio::select! {
            sig = signals.recv() => {
                tracing::info!(signal = sig, "shutting down");
            }
            () = handle.wait() => {
                tracing::warn!("API server stopped; shutting down");
            }
        }
        match handle.shutdown().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => fail(format!("shutdown: {e}")),
        }
    })
}

/// `NAME listening on http://ADDR (data: DIR[, ephemeral]) ready in N ms`, where N is
/// the time since the process started (the spawned-binary tests parse this line).
fn print_ready(name: &str, handle: &ServerHandle, started: Instant) {
    use std::io::Write as _;

    let kind = if handle.is_ephemeral() {
        ", ephemeral"
    } else {
        ""
    };
    let mut out = std::io::stdout().lock();
    let _ = writeln!(
        out,
        "{name} listening on http://{} (data: {}{kind}) ready in {} ms",
        handle.local_addr(),
        handle.data_dir().display(),
        started.elapsed().as_millis(),
    );
    let _ = out.flush();
    tracing::info!(
        server_start_ms = handle.timings().total.as_millis() as u64,
        "ready"
    );
}

/// SIGINT + SIGTERM (unix) or Ctrl-C (elsewhere).
#[cfg(unix)]
struct Signals {
    int: tokio::signal::unix::Signal,
    term: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn new() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            int: signal(SignalKind::interrupt())?,
            term: signal(SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.int.recv() => "SIGINT",
            _ = self.term.recv() => "SIGTERM",
        }
    }
}

#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn new() -> std::io::Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> &'static str {
        let _ = tokio::signal::ctrl_c().await;
        "Ctrl-C"
    }
}
