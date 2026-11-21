//! `[worker]` configuration: the contract between `ostro worker` (07-15) and the worker
//! runtime (07-07).
//!
//! Precedence, lowest to highest: [`WorkerConfig::default`], the `[worker]` table of an
//! optional TOML file, then `OSTRO_WORKER_<KEY>` environment variables (one flat level:
//! `OSTRO_WORKER_JOIN`, `OSTRO_WORKER_TOKEN`, `OSTRO_WORKER_SLOTS`, ...;
//! `OSTRO_WORKER_LABELS` is `k=v,k2=v2`). CLI flags are applied by the caller on top.
//!
//! The join token is a [`JoinToken`]: `Debug`, `Display` and serialisation print
//! `<redacted>`, so it never reaches a log line or `--print-config`.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::{Path, PathBuf},
};

use config::{Config, File, FileFormat};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::WorkerError;

/// Environment variable prefix of the worker keys (followed by the upper-case key).
pub const ENV_PREFIX: &str = "OSTRO_WORKER_";

/// Default cap of the reconnect backoff, in ms.
pub const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;
/// Default number of consecutive session failures before the worker re-joins (B5).
pub const DEFAULT_REJOIN_AFTER_FAILURES: u32 = 5;
/// Default memory budget of the worker's engine (1 GiB, the engine's node default).
pub const DEFAULT_MEMORY_BYTES: u64 = 1024 * 1024 * 1024;

/// The join token `wkr_<id>.<secret>`; never printed.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct JoinToken(String);

impl JoinToken {
    /// What `Debug`, `Display` and serialisation show instead of the value.
    pub const REDACTED: &'static str = "<redacted>";

    pub fn new(v: impl Into<String>) -> Self {
        Self(v.into())
    }

    /// The token value (send it only in the join request body).
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// The public token id (`wkr_<id>`, the part before the first `.`), if well formed.
    pub fn id(&self) -> Option<&str> {
        let (id, secret) = self.0.split_once('.')?;
        (id.starts_with("wkr_") && id.len() > 4 && !secret.is_empty()).then_some(id)
    }
}

impl fmt::Debug for JoinToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::REDACTED)
    }
}

impl fmt::Display for JoinToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::REDACTED)
    }
}

impl Serialize for JoinToken {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(Self::REDACTED)
    }
}

impl<'de> Deserialize<'de> for JoinToken {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(Self)
    }
}

/// `[worker]`: everything the worker process needs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerConfig {
    /// Join URL of a server's client listener (`https://host:7700`); plain `http://` only
    /// for loopback hosts (D-W20).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join: Option<String>,
    /// Join token `wkr_<id>.<secret>` (redacted everywhere).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<JoinToken>,
    /// `sha256:<hex>` of the cluster CA certificate (DER), pinned during the join.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_hash: Option<String>,
    /// Labels matched by workflow/queue selectors.
    pub labels: BTreeMap<String, String>,
    /// Concurrent activation slots (default `available_parallelism()`).
    pub slots: u32,
    /// Module / AOT cache directory.
    pub cache_dir: PathBuf,
    /// Engine memory budget in bytes (default 1 GiB).
    pub memory_bytes: u64,
    /// External client certificate (cert-manager path, D-W25); with `tls_key` and `tls_ca`
    /// it replaces the join.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_key: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_ca: Option<PathBuf>,
    /// Cluster listener endpoints (`host:port`) for the external-certificate path, which has
    /// no join response to learn them from (07-07). Ignored with a join (the join response
    /// lists the endpoints).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<String>,
    /// Stable worker id (`[A-Za-z0-9._-]{1,64}`); unset = generated at start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Reconnect backoff cap in ms (default 30,000).
    pub reconnect_max_ms: u64,
    /// Consecutive session failures before the worker re-joins (default 5, B5).
    pub rejoin_after_failures: u32,
}

/// `available_parallelism()` as slots (at least 1).
pub fn default_slots() -> u32 {
    std::thread::available_parallelism()
        .map(|n| u32::try_from(n.get()).unwrap_or(u32::MAX))
        .unwrap_or(1)
        .max(1)
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            join: None,
            token: None,
            ca_hash: None,
            labels: BTreeMap::new(),
            slots: default_slots(),
            cache_dir: PathBuf::from("./.ostro-worker/cache"),
            memory_bytes: DEFAULT_MEMORY_BYTES,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            endpoints: Vec::new(),
            worker_id: None,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            rejoin_after_failures: DEFAULT_REJOIN_AFTER_FAILURES,
        }
    }
}

/// The TOML file shape: only a `[worker]` table.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileShape {
    #[serde(default)]
    worker: Option<WorkerConfig>,
}

fn invalid(msg: impl Into<String>) -> WorkerError {
    WorkerError::Config(msg.into())
}

impl WorkerConfig {
    /// Load defaults < `file` (`[worker]` table) < `env` (`OSTRO_WORKER_*`).
    ///
    /// `env` is passed explicitly (tests never touch the process environment); the binary
    /// passes `std::env::vars().collect()`.
    pub fn load(file: Option<&Path>, env: &HashMap<String, String>) -> Result<Self, WorkerError> {
        let mut cfg = match file {
            None => Self::default(),
            Some(path) => {
                if !path.is_file() {
                    return Err(invalid(format!(
                        "config file not found: {}",
                        path.display()
                    )));
                }
                let shape: FileShape = Config::builder()
                    .add_source(File::from(path).format(FileFormat::Toml))
                    .build()
                    .and_then(Config::try_deserialize)
                    .map_err(|e| invalid(e.to_string()))?;
                shape.worker.unwrap_or_default()
            }
        };
        cfg.apply_env(env)?;
        Ok(cfg)
    }

    /// Apply `OSTRO_WORKER_<KEY>` overrides. Unknown `OSTRO_WORKER_*` keys are rejected
    /// (typo protection).
    pub fn apply_env(&mut self, env: &HashMap<String, String>) -> Result<(), WorkerError> {
        let mut keys: Vec<(&String, &String)> = env
            .iter()
            .filter(|(k, _)| k.starts_with(ENV_PREFIX))
            .collect();
        keys.sort();
        for (k, v) in keys {
            let key = k[ENV_PREFIX.len()..].to_ascii_lowercase();
            let num = |what: &str| -> Result<u64, WorkerError> {
                v.trim()
                    .parse::<u64>()
                    .map_err(|_| invalid(format!("{k}: expected {what}, got {v:?}")))
            };
            match key.as_str() {
                "join" => self.join = Some(v.clone()),
                "token" => self.token = Some(JoinToken::new(v.clone())),
                "ca_hash" => self.ca_hash = Some(v.clone()),
                "labels" => {
                    self.labels = parse_labels(v).map_err(|e| invalid(format!("{k}: {e}")))?
                }
                "slots" => {
                    self.slots = u32::try_from(num("an integer")?)
                        .map_err(|_| invalid(format!("{k}: out of range")))?;
                }
                "cache_dir" => self.cache_dir = PathBuf::from(v),
                "memory_bytes" => self.memory_bytes = num("a byte count")?,
                "tls_cert" => self.tls_cert = Some(PathBuf::from(v)),
                "tls_key" => self.tls_key = Some(PathBuf::from(v)),
                "tls_ca" => self.tls_ca = Some(PathBuf::from(v)),
                "endpoints" => {
                    self.endpoints = v
                        .split(',')
                        .map(str::trim)
                        .filter(|e| !e.is_empty())
                        .map(str::to_owned)
                        .collect();
                }
                "worker_id" => self.worker_id = Some(v.clone()),
                "reconnect_max_ms" => self.reconnect_max_ms = num("milliseconds")?,
                "rejoin_after_failures" => {
                    self.rejoin_after_failures = u32::try_from(num("an integer")?)
                        .map_err(|_| invalid(format!("{k}: out of range")))?;
                }
                _ => return Err(invalid(format!("unknown worker setting {k}"))),
            }
        }
        Ok(())
    }

    /// Whether the external-certificate path (`tls_cert` + `tls_key` + `tls_ca`) is used.
    pub fn uses_external_cert(&self) -> bool {
        self.tls_cert.is_some() || self.tls_key.is_some() || self.tls_ca.is_some()
    }

    /// Structural validation: exactly one of join (`join` + `token` + `ca_hash`) or the
    /// external certificate triple; `slots >= 1`; a well-formed `worker_id`.
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.slots == 0 {
            return Err(invalid("worker.slots must be at least 1"));
        }
        if let Some(id) = &self.worker_id
            && !valid_worker_id(id)
        {
            return Err(invalid(format!(
                "worker.worker_id {id:?} must match [A-Za-z0-9._-]{{1,64}}"
            )));
        }
        if self.uses_external_cert() {
            if self.tls_cert.is_none() || self.tls_key.is_none() || self.tls_ca.is_none() {
                return Err(invalid(
                    "worker.tls_cert, worker.tls_key and worker.tls_ca must be set together",
                ));
            }
            if self.token.is_some() {
                return Err(invalid(
                    "worker.token and the external certificate (tls_cert/tls_key/tls_ca) are mutually exclusive",
                ));
            }
            if self.endpoints.is_empty() {
                return Err(invalid(
                    "worker.endpoints (cluster host:port list) is required with tls_cert/tls_key/tls_ca",
                ));
            }
            return Ok(());
        }
        let Some(join) = &self.join else {
            return Err(invalid(
                "worker.join is required (or tls_cert/tls_key/tls_ca)",
            ));
        };
        if !(join.starts_with("https://") || join.starts_with("http://")) {
            return Err(invalid(format!(
                "worker.join must be an http(s):// URL (got {join:?})"
            )));
        }
        match &self.token {
            None => return Err(invalid("worker.token is required with worker.join")),
            Some(t) if t.id().is_none() => {
                return Err(invalid("worker.token must look like wkr_<id>.<secret>"));
            }
            Some(_) => {}
        }
        if let Some(h) = &self.ca_hash
            && !valid_ca_hash(h)
        {
            return Err(invalid(
                "worker.ca_hash must be sha256:<64 lowercase hex digits>",
            ));
        }
        Ok(())
    }
}

/// `[A-Za-z0-9._-]{1,64}` (D-W25).
pub fn valid_worker_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn valid_ca_hash(h: &str) -> bool {
    h.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// `k=v,k2=v2` (empty string = no labels).
pub fn parse_labels(s: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| format!("label {part:?} is not k=v"))?;
        let k = k.trim();
        if k.is_empty() {
            return Err(format!("label {part:?} has an empty key"));
        }
        out.insert(k.to_owned(), v.trim().to_owned());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    const HASH: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn defaults() {
        let c = WorkerConfig::default();
        assert_eq!(c.reconnect_max_ms, 30_000);
        assert_eq!(c.rejoin_after_failures, 5);
        assert!(c.slots >= 1);
        assert!(c.labels.is_empty());
        assert!(c.validate().is_err(), "no join and no certs");
    }

    #[test]
    fn token_is_redacted_everywhere() {
        let c = WorkerConfig {
            token: Some(JoinToken::new("wkr_abc.s3cr3t-value")),
            ..WorkerConfig::default()
        };
        let dbg = format!("{c:?}");
        let json = serde_json::to_string(&c).unwrap();
        assert!(!dbg.contains("s3cr3t"), "{dbg}");
        assert!(!json.contains("s3cr3t"), "{json}");
        assert_eq!(c.token.as_ref().unwrap().to_string(), "<redacted>");
        assert_eq!(c.token.as_ref().unwrap().id(), Some("wkr_abc"));
    }

    #[test]
    fn file_then_env() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker.toml");
        std::fs::write(
            &path,
            format!(
                "[worker]\njoin = \"https://h:7700\"\ntoken = \"wkr_a.b\"\nca_hash = \"{HASH}\"\nslots = 3\n[worker.labels]\ngpu = \"no\"\n"
            ),
        )
        .unwrap();
        let c = WorkerConfig::load(
            Some(&path),
            &env(&[
                ("OSTRO_WORKER_SLOTS", "7"),
                ("OSTRO_WORKER_LABELS", "zone=a, tier=b"),
                ("OSTRO_DET_ITERS", "ignored"),
            ]),
        )
        .unwrap();
        assert_eq!(c.slots, 7);
        assert_eq!(c.join.as_deref(), Some("https://h:7700"));
        assert_eq!(c.labels.get("zone").map(String::as_str), Some("a"));
        assert_eq!(c.labels.get("tier").map(String::as_str), Some("b"));
        assert!(!c.labels.contains_key("gpu"), "env replaces the label map");
        c.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_keys_and_bad_values() {
        assert!(WorkerConfig::load(None, &env(&[("OSTRO_WORKER_SLOTZ", "1")])).is_err());
        assert!(WorkerConfig::load(None, &env(&[("OSTRO_WORKER_SLOTS", "x")])).is_err());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.toml");
        std::fs::write(&path, "[worker]\nslotz = 1\n").unwrap();
        assert!(WorkerConfig::load(Some(&path), &HashMap::new()).is_err());
    }

    #[test]
    fn validate_paths() {
        let mut c = WorkerConfig {
            join: Some("https://h:7700".into()),
            token: Some(JoinToken::new("wkr_a.b")),
            ca_hash: Some(HASH.into()),
            ..WorkerConfig::default()
        };
        c.validate().unwrap();
        c.token = Some(JoinToken::new("nope"));
        assert!(c.validate().is_err());
        c.token = Some(JoinToken::new("wkr_a.b"));
        c.ca_hash = Some("sha256:XYZ".into());
        assert!(c.validate().is_err());
        c.ca_hash = None;
        c.worker_id = Some("bad id!".into());
        assert!(c.validate().is_err());

        let ext = WorkerConfig {
            tls_cert: Some("c.pem".into()),
            tls_key: Some("k.pem".into()),
            tls_ca: Some("ca.pem".into()),
            ..WorkerConfig::default()
        };
        assert!(ext.validate().is_err(), "external certs need endpoints");
        let ext = WorkerConfig {
            endpoints: vec!["h:7701".into()],
            ..ext
        };
        ext.validate().unwrap();
        let env_eps =
            WorkerConfig::load(None, &env(&[("OSTRO_WORKER_ENDPOINTS", "a:1, b:2,")])).unwrap();
        assert_eq!(env_eps.endpoints, vec!["a:1".to_owned(), "b:2".to_owned()]);
        let half = WorkerConfig {
            tls_cert: Some("c.pem".into()),
            ..WorkerConfig::default()
        };
        assert!(half.validate().is_err());
    }
}
