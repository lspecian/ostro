//! `ostro backup` / `ostro restore` (plan 08-18, P8-D6/D7): argument definitions, the online
//! backup download and the restore report printer.
//!
//! The `ostro` binary embeds [`BackupArgs`] and [`RestoreArgs`] as its own subcommands and
//! runs the parts that need the storage crates (offline backup, restore) itself; this
//! module stays a pure REST client:
//!
//! - `ostro backup --to f.tar [--server URL]`: downloads `GET /v1/admin/backup` (the
//!   running server packs one bottomless round into a tar) to `f.tar.tmp`, fsyncs, checks
//!   that it is a tar holding `manifest/current`, then renames it to `f.tar`.
//! - `ostro backup --to f.tar --data-dir DIR`: offline backup of a stopped data directory
//!   (Inspect mode), run by the binary.
//! - `ostro restore --from <s3://…|file://…|path.tar> --data-dir <empty> [--at <gen|RFC3339>]
//!   [--takeover] [--json]`: run by the binary; S3 credentials come only from the
//!   environment (`OSTRO_BACKUP_*`, else the standard `AWS_*`) and are never printed.

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, ValueEnum};
use serde_json::Value;

use crate::client::{CallError, DEFAULT_SERVER, parse_server};
use crate::commands::parse_duration;
use crate::output::Outcome;

/// `ostro backup`.
#[derive(Debug, Clone, Args)]
pub struct BackupArgs {
    /// Backup file to write (a tar of the bottomless layout).
    #[arg(long, value_name = "FILE")]
    pub to: PathBuf,
    /// Offline backup of this stopped data directory (no server needed); without it the
    /// running server at --server makes the backup.
    #[arg(long, value_name = "DIR")]
    pub data_dir: Option<PathBuf>,
    /// Metadata voter id of the stopped node (`[node] node_id`), offline only.
    #[arg(long, default_value_t = 1)]
    pub node_id: u64,
    /// Server URL (online backup).
    #[arg(long, env = "OSTRO_SERVER", default_value = DEFAULT_SERVER)]
    pub server: String,
    /// Print the result as JSON.
    #[arg(long)]
    pub json: bool,
    /// Download timeout (friendly duration).
    #[arg(long, default_value = "1h", value_parser = parse_duration)]
    pub timeout: Duration,
}

/// S3 addressing style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum UrlStyleArg {
    /// `https://endpoint/bucket/key`.
    Path,
    /// `https://bucket.endpoint/key`.
    Virtual,
}

/// `ostro restore`.
#[derive(Debug, Clone, Args)]
pub struct RestoreArgs {
    /// Source: `s3://bucket/prefix`, `file:///abs/path` or a backup file (`.tar`).
    #[arg(long, value_name = "SOURCE")]
    pub from: String,
    /// Data directory to create; must be absent or empty.
    #[arg(long, value_name = "DIR")]
    pub data_dir: PathBuf,
    /// Restore point: a manifest generation (`42`) or a time (RFC 3339, the newest
    /// manifest cut at or before it). Default: the current manifest.
    #[arg(long, value_name = "GEN|RFC3339", value_parser = parse_at)]
    pub at: Option<AtArg>,
    /// The restored node will take over the source prefix: start it with `[backup]
    /// takeover = true` and the same url (otherwise configure a new prefix).
    #[arg(long)]
    pub takeover: bool,
    /// Metadata voter id (`[node] node_id` of the server that will run on the directory);
    /// default: the source node's id.
    #[arg(long)]
    pub node_id: Option<u64>,
    /// Partitions restored concurrently.
    #[arg(long, default_value_t = 4)]
    pub parallelism: usize,
    /// S3 endpoint (SeaweedFS, R2, …); env OSTRO_BACKUP_ENDPOINT, AWS_ENDPOINT_URL.
    #[arg(long)]
    pub endpoint: Option<String>,
    /// S3 region; env OSTRO_BACKUP_REGION, AWS_REGION.
    #[arg(long)]
    pub region: Option<String>,
    /// S3 addressing style.
    #[arg(long, value_enum, default_value_t = UrlStyleArg::Path)]
    pub url_style: UrlStyleArg,
    /// Allow a plain http:// endpoint.
    #[arg(long)]
    pub allow_http: bool,
    /// PEM bundle of TLS roots for S3 (default: the platform store; scratch images without
    /// /etc/ssl/certs need one).
    #[arg(long, value_name = "PEM")]
    pub tls_roots_file: Option<PathBuf>,
    /// Print the restore report as JSON.
    #[arg(long)]
    pub json: bool,
}

/// A parsed `--at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtArg {
    /// A manifest generation.
    Generation(u64),
    /// Milliseconds since the Unix epoch.
    TimeMs(u64),
}

/// `--at`: all digits = a generation, else an RFC 3339 timestamp.
pub fn parse_at(s: &str) -> Result<AtArg, String> {
    let t = s.trim();
    if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) {
        return t
            .parse()
            .map(AtArg::Generation)
            .map_err(|e| format!("generation {t:?}: {e}"));
    }
    let ts: jiff::Timestamp = t.parse().map_err(|e| {
        format!("--at {s:?}: expected a generation number or an RFC 3339 time ({e})")
    })?;
    u64::try_from(ts.as_millisecond())
        .map(AtArg::TimeMs)
        .map_err(|_| format!("--at {s:?}: before 1970"))
}

/// Where `ostro restore --from` points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromArg {
    /// `s3://…` or `file://…`.
    Url(String),
    /// A backup file.
    File(PathBuf),
}

/// Classify `--from`.
pub fn classify_from(s: &str) -> FromArg {
    if s.starts_with("s3://") || s.starts_with("file://") {
        FromArg::Url(s.to_owned())
    } else {
        FromArg::File(PathBuf::from(s))
    }
}

/// S3 credentials from the environment: `OSTRO_BACKUP_<NAME>`, else `AWS_<NAME>`.
/// Returns `(access_key_id, secret_access_key, session_token)`.
pub fn env_credentials(
    get: impl Fn(&str) -> Option<String>,
) -> (Option<String>, Option<String>, Option<String>) {
    let pick = |a: &str, b: &str| get(a).or_else(|| get(b)).filter(|v| !v.is_empty());
    (
        pick("OSTRO_BACKUP_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
        pick("OSTRO_BACKUP_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"),
        pick("OSTRO_BACKUP_SESSION_TOKEN", "AWS_SESSION_TOKEN"),
    )
}

/// `--endpoint` / `--region` with their env fallbacks.
pub fn endpoint_and_region(
    a: &RestoreArgs,
    get: impl Fn(&str) -> Option<String>,
) -> (Option<String>, Option<String>) {
    let pick = |flag: &Option<String>, x: &str, y: &str| {
        flag.clone()
            .or_else(|| get(x))
            .or_else(|| get(y))
            .filter(|v| !v.is_empty())
    };
    (
        pick(&a.endpoint, "OSTRO_BACKUP_ENDPOINT", "AWS_ENDPOINT_URL"),
        pick(&a.region, "OSTRO_BACKUP_REGION", "AWS_REGION"),
    )
}

fn tmp_of(p: &Path) -> PathBuf {
    let mut n = p.file_name().unwrap_or_default().to_os_string();
    n.push(".tmp");
    p.with_file_name(n)
}

/// Entries of a tar file; requires `manifest/current`.
pub fn check_backup_file(path: &Path) -> Result<u64, String> {
    let f = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut a = tar::Archive::new(std::io::BufReader::new(f));
    let mut n = 0u64;
    let mut manifest = false;
    for e in a
        .entries()
        .map_err(|e| format!("{}: not a tar file: {e}", path.display()))?
    {
        let e = e.map_err(|e| format!("{}: corrupt tar: {e}", path.display()))?;
        let p = e
            .path()
            .map_err(|e| format!("{}: corrupt tar: {e}", path.display()))?;
        if p == Path::new("manifest/current") {
            manifest = true;
        }
        n += 1;
    }
    if !manifest {
        return Err(format!(
            "{}: not an Ostro backup (no manifest/current)",
            path.display()
        ));
    }
    Ok(n)
}

/// Online backup: stream `GET /v1/admin/backup` into `args.to`.
pub async fn download(args: &BackupArgs) -> Result<Value, CallError> {
    let base = parse_server(&args.server).map_err(CallError::Usage)?;
    let url = base
        .join("v1/admin/backup")
        .map_err(|e| CallError::Usage(format!("server URL: {e}")))?;
    let http = reqwest::Client::builder()
        .no_proxy()
        .user_agent(concat!("ostro-cli/", env!("CARGO_PKG_VERSION")))
        .timeout(args.timeout)
        .build()
        .map_err(|e| CallError::Transport(format!("cannot build the HTTP client: {e}")))?;
    let mut resp = http
        .get(url.clone())
        .send()
        .await
        .map_err(|e| CallError::Transport(format!("GET {url}: {e}")))?;
    let status = resp.status().as_u16();
    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        let body = serde_json::from_str(&text).unwrap_or_else(
            |_| serde_json::json!({"error": {"code": format!("http_{status}"), "message": text}}),
        );
        return Err(CallError::Api { status, body });
    }
    let tmp = tmp_of(&args.to);
    let io = |e: std::io::Error| CallError::Transport(format!("{}: {e}", tmp.display()));
    let mut f = File::create(&tmp).map_err(io)?;
    let mut bytes = 0u64;
    loop {
        match resp.chunk().await {
            Ok(Some(c)) => {
                f.write_all(&c).map_err(io)?;
                bytes += c.len() as u64;
            }
            Ok(None) => break,
            Err(e) => {
                drop(f);
                let _ = std::fs::remove_file(&tmp);
                return Err(CallError::Transport(format!("download interrupted: {e}")));
            }
        }
    }
    f.sync_all().map_err(io)?;
    drop(f);
    let entries = match check_backup_file(&tmp) {
        Ok(n) => n,
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            return Err(CallError::Transport(e));
        }
    };
    std::fs::rename(&tmp, &args.to).map_err(io)?;
    Ok(serde_json::json!({
        "file": args.to.display().to_string(),
        "mode": "online",
        "bytes": bytes,
        "entries": entries,
    }))
}

/// Print a backup result (`{file, mode, bytes, entries, generation?}`).
pub fn print_backup(v: &Value, json: bool) -> Outcome {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
        );
    } else {
        let gen_part = v["generation"]
            .as_u64()
            .map(|g| format!(", manifest generation {g}"))
            .unwrap_or_default();
        println!(
            "backup written to {} ({} backup, {} bytes, {} entries{gen_part})",
            v["file"].as_str().unwrap_or("?"),
            v["mode"].as_str().unwrap_or("?"),
            v["bytes"],
            v["entries"],
        );
    }
    Outcome::Ok
}

/// The `ostro backup` (online) verb.
pub async fn run_online(args: &BackupArgs) -> Outcome {
    match download(args).await {
        Ok(v) => print_backup(&v, args.json),
        Err(e) => crate::output::Out { json: args.json }.error(&e),
    }
}

fn short(h: &str) -> &str {
    h.get(..16).unwrap_or(h)
}

/// Print a restore report (the serialized `ostro_backup::restore::RestoreReport`).
pub fn print_restore_report(v: &Value, json: bool) -> Outcome {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
        );
        return Outcome::Ok;
    }
    let d = &v["durations"];
    println!(
        "restored {} from {} (generation {}, cut at {} ms, cluster {}, {} blobs, meta index {})",
        v["data_dir"].as_str().unwrap_or("?"),
        v["source"].as_str().unwrap_or("?"),
        v["generation"],
        v["cut_at_ms"],
        v["cluster_id"].as_str().unwrap_or("?"),
        v["blobs"],
        v["meta_index"],
    );
    println!(
        "{:>4}  {:>12}  {:>12}  {:>12}  {:>7}  {:<16}  {:<16}",
        "P", "SNAPSHOT", "VERIFY", "CUT", "CHUNKS", "STATE_HASH", "DIGEST"
    );
    for p in v["partitions"].as_array().into_iter().flatten() {
        let snap = p["snapshot_offset"]
            .as_u64()
            .map_or_else(|| "-".to_owned(), |o| o.to_string());
        println!(
            "{:>4}  {:>12}  {:>12}  {:>12}  {:>7}  {:<16}  {:<16}",
            p["p"],
            snap,
            p["verify_offset"],
            p["cut_offset"],
            p["chunks"],
            short(p["verify_state_hash"].as_str().unwrap_or("")),
            short(p["digest"].as_str().unwrap_or("")),
        );
    }
    println!(
        "verified: every partition's state hash matched at its verify offset; new incarnation {}",
        v["incarnation"].as_str().unwrap_or("?")
    );
    println!(
        "took {} ms (manifest {}, meta {}, blobs {}, partitions {}, replay {})",
        d["total_ms"],
        d["manifest_ms"],
        d["meta_ms"],
        d["blobs_ms"],
        d["partitions_ms"],
        d["replay_ms"]
    );
    if v["takeover"].as_bool() == Some(true) {
        println!(
            "takeover: start the server with [backup] url = <the same url> and takeover = true"
        );
    } else {
        println!(
            "to keep backing up, configure a new [backup] url (or use --takeover for the same prefix)"
        );
    }
    Outcome::Ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::tests::parse;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct R {
        #[command(flatten)]
        a: RestoreArgs,
    }

    #[derive(Debug, Parser)]
    struct B {
        #[command(flatten)]
        a: BackupArgs,
    }

    #[test]
    fn at_parses_generations_and_times() {
        assert_eq!(parse_at("42").unwrap(), AtArg::Generation(42));
        assert_eq!(
            parse_at("2026-01-02T03:04:05Z").unwrap(),
            AtArg::TimeMs(1_767_323_045_000)
        );
        assert!(parse_at("yesterday").unwrap_err().contains("RFC 3339"));
    }

    #[test]
    fn from_is_url_or_file() {
        assert_eq!(classify_from("s3://b/p"), FromArg::Url("s3://b/p".into()));
        assert_eq!(classify_from("file:///x"), FromArg::Url("file:///x".into()));
        assert_eq!(classify_from("f.tar"), FromArg::File("f.tar".into()));
    }

    #[test]
    fn credentials_prefer_ostro_names_and_never_come_from_flags() {
        let env = |k: &str| match k {
            "OSTRO_BACKUP_ACCESS_KEY_ID" => Some("ostro-id".to_owned()),
            "AWS_ACCESS_KEY_ID" => Some("aws-id".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("aws-secret".to_owned()),
            _ => None,
        };
        let (id, secret, token) = env_credentials(env);
        assert_eq!(id.as_deref(), Some("ostro-id"));
        assert_eq!(secret.as_deref(), Some("aws-secret"));
        assert!(token.is_none());
        let a = R::try_parse_from(["r", "--from", "s3://b/p", "--data-dir", "d"]).unwrap();
        assert!(a.a.endpoint.is_none() && !a.a.takeover && a.a.at.is_none());
        assert!(
            R::try_parse_from([
                "r",
                "--from",
                "x",
                "--data-dir",
                "d",
                "--secret-access-key",
                "s"
            ])
            .is_err(),
            "no flag takes secret material"
        );
        let (ep, region) = endpoint_and_region(&a.a, |k| {
            (k == "AWS_ENDPOINT_URL").then(|| "http://127.0.0.1:8333".to_owned())
        });
        assert_eq!(ep.as_deref(), Some("http://127.0.0.1:8333"));
        assert!(region.is_none());
    }

    #[test]
    fn backup_args() {
        let b = B::try_parse_from(["b", "--to", "f.tar"]).unwrap();
        assert!(b.a.data_dir.is_none());
        assert_eq!(b.a.timeout, Duration::from_secs(3600));
        assert!(B::try_parse_from(["b"]).is_err(), "--to is required");
        // The client verbs are untouched.
        assert!(parse(&["backup"]).is_err());
    }

    #[test]
    fn check_backup_file_wants_a_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.tar");
        {
            let mut b = tar::Builder::new(File::create(&p).unwrap());
            let mut h = tar::Header::new_gnu();
            h.set_size(1);
            h.set_mode(0o644);
            b.append_data(&mut h, "manifest/current", &b"m"[..])
                .unwrap();
            b.finish().unwrap();
        }
        assert_eq!(check_backup_file(&p).unwrap(), 1);
        std::fs::write(&p, b"not a tar at all, just some bytes").unwrap();
        assert!(check_backup_file(&p).is_err());
    }
}
