//! Verified downloads: streamed sha256 checked before any extraction, size cap, connect /
//! read-idle / total timeouts, offline mode and the mirror rewrite.
//!
//! Owner: 10-07.

use std::path::Path;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::toolchain::{ToolchainError, hex};

/// Default connect timeout (D-P10-1).
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Default read-idle timeout: the longest wait for the response head or the next chunk.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Default total timeout per artefact.
pub const TOTAL_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// How downloads are made.
#[derive(Debug, Clone)]
pub struct FetchOptions {
    /// Never make a request (`OSTRO_OFFLINE=1`): only cached tools succeed.
    pub offline: bool,
    /// `OSTRO_TOOLCHAIN_MIRROR`: replaces scheme and host of every pinned URL.
    pub mirror: Option<String>,
    /// The HTTP client (build it with [`FetchOptions::client_for`] so the connect timeout
    /// applies).
    pub client: reqwest::Client,
    /// Connect timeout (applied by the client).
    pub connect_timeout: Duration,
    /// Read-idle timeout (response head and every chunk).
    pub idle_timeout: Duration,
    /// Total timeout for one artefact.
    pub total_timeout: Duration,
}

impl FetchOptions {
    /// Default timeouts, online, no mirror.
    pub fn new() -> Result<FetchOptions, ToolchainError> {
        Ok(FetchOptions {
            offline: false,
            mirror: None,
            client: Self::client_for(CONNECT_TIMEOUT)?,
            connect_timeout: CONNECT_TIMEOUT,
            idle_timeout: IDLE_TIMEOUT,
            total_timeout: TOTAL_TIMEOUT,
        })
    }

    /// [`FetchOptions::new`] plus `OSTRO_OFFLINE` / `OSTRO_TOOLCHAIN_MIRROR` from `env`.
    pub fn from_env_with(
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<FetchOptions, ToolchainError> {
        let mut o = Self::new()?;
        o.offline = env("OSTRO_OFFLINE").is_some_and(|v| is_truthy(&v));
        o.mirror = env("OSTRO_TOOLCHAIN_MIRROR").filter(|m| !m.trim().is_empty());
        Ok(o)
    }

    /// [`FetchOptions::from_env_with`] over the process environment.
    pub fn from_env() -> Result<FetchOptions, ToolchainError> {
        Self::from_env_with(|k| std::env::var(k).ok())
    }

    /// A client with `connect_timeout` (rustls, platform verifier, system proxies).
    pub fn client_for(connect_timeout: Duration) -> Result<reqwest::Client, ToolchainError> {
        reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .user_agent(concat!("ostro/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| ToolchainError::Config(format!("cannot build the HTTP client: {e}")))
    }

    /// The URL actually fetched for `url` (the mirror rewrite).
    pub fn effective_url(&self, url: &str) -> Result<String, ToolchainError> {
        match &self.mirror {
            None => Ok(url.to_string()),
            Some(m) => mirror_url(m, url),
        }
    }
}

/// `1`, `true`, `yes`, `on` (case-insensitive).
pub fn is_truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// `mirror.trim_end_matches('/') + <path and query of url>`.
pub fn mirror_url(mirror: &str, url: &str) -> Result<String, ToolchainError> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| ToolchainError::Config(format!("invalid pinned url {url}: {e}")))?;
    let mut out = format!("{}{}", mirror.trim_end_matches('/'), parsed.path());
    if let Some(q) = parsed.query() {
        out.push('?');
        out.push_str(q);
    }
    Ok(out)
}

/// Download `url` into `dest` (created/truncated), streaming the sha256, enforcing the size
/// cap and the timeouts. On any failure, including a sha mismatch, `dest` is removed.
pub async fn download_verified(
    opts: &FetchOptions,
    url: &str,
    expected_sha: &str,
    max_bytes: u64,
    dest: &Path,
) -> Result<(), ToolchainError> {
    if opts.offline {
        return Err(ToolchainError::Config(format!(
            "offline: refusing to download {url}"
        )));
    }
    let res = match tokio::time::timeout(
        opts.total_timeout,
        download_inner(opts, url, expected_sha, max_bytes, dest),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err(ToolchainError::Timeout {
            url: url.to_string(),
            what: "total",
            secs: opts.total_timeout.as_secs_f64(),
        }),
    };
    if res.is_err() {
        let _ = tokio::fs::remove_file(dest).await;
    }
    res
}

async fn download_inner(
    opts: &FetchOptions,
    url: &str,
    expected_sha: &str,
    max_bytes: u64,
    dest: &Path,
) -> Result<(), ToolchainError> {
    let idle = |what: &'static str| ToolchainError::Timeout {
        url: url.to_string(),
        what,
        secs: opts.idle_timeout.as_secs_f64(),
    };
    let transport = |e: reqwest::Error| {
        if e.is_connect() && e.is_timeout() {
            ToolchainError::Timeout {
                url: url.to_string(),
                what: "connect",
                secs: opts.connect_timeout.as_secs_f64(),
            }
        } else {
            ToolchainError::Http {
                url: url.to_string(),
                detail: e.to_string(),
            }
        }
    };
    let mut resp = tokio::time::timeout(opts.idle_timeout, opts.client.get(url).send())
        .await
        .map_err(|_| idle("read-idle"))?
        .map_err(transport)?;
    if !resp.status().is_success() {
        return Err(ToolchainError::Http {
            url: url.to_string(),
            detail: format!("HTTP {}", resp.status()),
        });
    }
    let too_large = || ToolchainError::TooLarge {
        url: url.to_string(),
        limit: max_bytes,
    };
    if resp.content_length().is_some_and(|n| n > max_bytes) {
        return Err(too_large());
    }
    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| ToolchainError::io(format!("create {}", dest.display()), e))?;
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    loop {
        let chunk = tokio::time::timeout(opts.idle_timeout, resp.chunk())
            .await
            .map_err(|_| idle("read-idle"))?
            .map_err(transport)?;
        let Some(chunk) = chunk else { break };
        total += chunk.len() as u64;
        if total > max_bytes {
            return Err(too_large());
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| ToolchainError::io(format!("write {}", dest.display()), e))?;
    }
    file.flush()
        .await
        .map_err(|e| ToolchainError::io(format!("write {}", dest.display()), e))?;
    file.sync_all()
        .await
        .map_err(|e| ToolchainError::io(format!("sync {}", dest.display()), e))?;
    drop(file);
    let actual = hex(&hasher.finalize());
    if actual != expected_sha {
        return Err(ToolchainError::ShaMismatch {
            url: url.to_string(),
            expected: expected_sha.to_string(),
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirror_rewrites_scheme_and_host_only() {
        assert_eq!(
            mirror_url(
                "http://m.local/base/",
                "https://nodejs.org/dist/v1/node.tar.xz"
            )
            .unwrap(),
            "http://m.local/base/dist/v1/node.tar.xz"
        );
        assert_eq!(
            mirror_url("http://m", "https://h/a?x=1").unwrap(),
            "http://m/a?x=1"
        );
    }

    #[test]
    fn env_parsing() {
        let o = FetchOptions::from_env_with(|k| match k {
            "OSTRO_OFFLINE" => Some("1".into()),
            "OSTRO_TOOLCHAIN_MIRROR" => Some("http://m".into()),
            _ => None,
        })
        .unwrap();
        assert!(o.offline);
        assert_eq!(o.mirror.as_deref(), Some("http://m"));
        let o =
            FetchOptions::from_env_with(|k| (k == "OSTRO_OFFLINE").then(|| "0".into())).unwrap();
        assert!(!o.offline);
        assert!(o.mirror.is_none());
    }
}
