//! Pinned toolchains (`toolchains.toml`, D-P10-1..D-P10-4): the tool list, the platform
//! detection, the pinned set per platform and `ensure` (download, verify, extract into the
//! cache).
//!
//! Owner: 10-07.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::cache::Cache;
use crate::fetch::FetchOptions;
use crate::project::Lang;

/// The embedded pin file.
pub const TOOLCHAINS_TOML: &str = include_str!("../toolchains.toml");

/// Default per-artefact size cap (512 MiB) when a pin has no `max_bytes`.
pub const DEFAULT_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// The four supported platforms, as written in `toolchains.toml`.
pub const PLATFORMS: [&str; 4] = [
    "linux-x86_64",
    "linux-aarch64",
    "macos-aarch64",
    "macos-x86_64",
];

/// A tool Ostro can download into its cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tool {
    /// uv (Python package manager).
    Uv,
    /// CPython (managed by uv).
    Python,
    /// componentize-py (released wheel, D-P9-30).
    ComponentizePy,
    /// Node.js 24 (official tarball).
    Node,
    /// The pinned npm toolchain (jco, rolldown) from `sdks/scripts/toolchain`.
    JcoToolchain,
    /// rustup-init (hermetic Rust).
    RustupInit,
    /// zig (`cc` shim when the host has no C compiler).
    Zig,
}

impl Tool {
    /// Every tool that has a pin in `toolchains.toml` (Python is installed by uv, the jco
    /// toolchain by `npm ci`).
    pub const PINNED: [Tool; 5] = [
        Tool::Uv,
        Tool::ComponentizePy,
        Tool::Node,
        Tool::RustupInit,
        Tool::Zig,
    ];

    /// The tool's name (`toolchains.toml` `name`, the cache entry prefix).
    pub fn name(self) -> &'static str {
        match self {
            Tool::Uv => "uv",
            Tool::Python => "python",
            Tool::ComponentizePy => "componentize-py",
            Tool::Node => "node",
            Tool::JcoToolchain => "jco-toolchain",
            Tool::RustupInit => "rustup-init",
            Tool::Zig => "zig",
        }
    }

    /// Inverse of [`Tool::name`].
    pub fn from_name(s: &str) -> Option<Tool> {
        [
            Tool::Uv,
            Tool::Python,
            Tool::ComponentizePy,
            Tool::Node,
            Tool::JcoToolchain,
            Tool::RustupInit,
            Tool::Zig,
        ]
        .into_iter()
        .find(|t| t.name() == s)
    }
}

impl std::fmt::Display for Tool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// How a downloaded artefact is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveKind {
    /// gzip-compressed tar.
    TarGz,
    /// xz-compressed tar.
    TarXz,
    /// A single executable (copied, mode 0755).
    Binary,
    /// A Python wheel: verified and stored as is, never extracted.
    Wheel,
}

impl ArchiveKind {
    fn parse(s: &str) -> Option<ArchiveKind> {
        Some(match s {
            "tar.gz" => ArchiveKind::TarGz,
            "tar.xz" => ArchiveKind::TarXz,
            "binary" => ArchiveKind::Binary,
            "wheel" => ArchiveKind::Wheel,
            _ => return None,
        })
    }
}

/// One pinned artefact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pin {
    /// The tool.
    pub tool: Tool,
    /// Exact version.
    pub version: String,
    /// https URL (rewritten by a mirror; the sha stays authoritative).
    pub url: String,
    /// Lower-case hex sha256 of the downloaded bytes.
    pub sha256: String,
    /// How to install it.
    pub kind: ArchiveKind,
    /// Leading directory dropped on extraction.
    pub strip: Option<String>,
    /// Size cap; `None` means [`DEFAULT_MAX_BYTES`].
    pub max_bytes: Option<u64>,
}

impl Pin {
    /// The effective size cap.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes.unwrap_or(DEFAULT_MAX_BYTES)
    }

    /// The last path segment of the URL (the stored file name for binaries and wheels).
    pub fn file_name(&self) -> &str {
        let no_query = self.url.split(['?', '#']).next().unwrap_or_default();
        no_query.rsplit('/').next().unwrap_or_default()
    }
}

/// The pinned toolchain set for one platform.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolchainSet {
    /// sha256 of the `toolchains.toml` bytes, first 16 hex.
    pub id: String,
    /// The pins of this platform.
    pub pins: Vec<Pin>,
}

impl ToolchainSet {
    /// The pin of `tool`, if this set has one.
    pub fn pin(&self, tool: Tool) -> Option<&Pin> {
        self.pins.iter().find(|p| p.tool == tool)
    }
}

/// Toolchain errors.
#[derive(Debug, thiserror::Error)]
pub enum ToolchainError {
    /// The host is not one of the four supported platforms.
    #[error(
        "unsupported platform {os}-{arch} (hermetic toolchains exist for linux/macos x86_64/aarch64; use --docker)"
    )]
    Unsupported {
        /// `std::env::consts::OS`.
        os: String,
        /// `std::env::consts::ARCH`.
        arch: String,
    },
    /// `OSTRO_OFFLINE=1` and the tool is not cached.
    #[error(
        "{tool} is offline and not cached (unset OSTRO_OFFLINE or run `ostro toolchain install` with network access)"
    )]
    Offline {
        /// The tool.
        tool: String,
    },
    /// The downloaded bytes do not match the pin.
    #[error("sha256 mismatch for {url}: expected {expected}, got {actual}")]
    ShaMismatch {
        /// The URL actually fetched.
        url: String,
        /// The pinned sha.
        expected: String,
        /// The sha of the downloaded bytes.
        actual: String,
    },
    /// An archive entry would escape the install dir (or is not a supported entry type).
    #[error("unsafe archive entry {entry:?}: {reason}")]
    UnsafeArchive {
        /// The entry path (as written in the archive).
        entry: String,
        /// Why it was rejected.
        reason: String,
    },
    /// The download exceeded the size cap.
    #[error("{url} exceeds the size cap of {limit} bytes")]
    TooLarge {
        /// The URL.
        url: String,
        /// The cap.
        limit: u64,
    },
    /// A connect, read-idle or total timeout fired.
    #[error("{url}: {what} timeout after {secs:.1} s")]
    Timeout {
        /// The URL.
        url: String,
        /// `connect`, `read-idle` or `total`.
        what: &'static str,
        /// The configured timeout.
        secs: f64,
    },
    /// A non-2xx response or a transport error.
    #[error("download of {url} failed: {detail}")]
    Http {
        /// The URL.
        url: String,
        /// Status or transport error.
        detail: String,
    },
    /// `toolchains.toml` is invalid (a build-time bug) or the cache root cannot be resolved.
    #[error("{0}")]
    Config(String),
    /// A filesystem error.
    #[error("{context}: {source}")]
    Io {
        /// What was being done.
        context: String,
        /// The error.
        #[source]
        source: std::io::Error,
    },
}

impl ToolchainError {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        ToolchainError::Io {
            context: context.into(),
            source,
        }
    }
}

/// The host platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Platform {
    /// `linux` or `macos`.
    pub os: &'static str,
    /// `x86_64` or `aarch64`.
    pub arch: &'static str,
}

impl Platform {
    /// The current host.
    pub fn detect() -> Result<Platform, ToolchainError> {
        Self::from_parts(std::env::consts::OS, std::env::consts::ARCH)
    }

    /// `os`/`arch` as in `std::env::consts`.
    pub fn from_parts(os: &str, arch: &str) -> Result<Platform, ToolchainError> {
        let os = match os {
            "linux" => "linux",
            "macos" => "macos",
            _ => return Err(unsupported(os, arch)),
        };
        let arch = match arch {
            "x86_64" => "x86_64",
            "aarch64" => "aarch64",
            _ => return Err(unsupported(os, arch)),
        };
        Ok(Platform { os, arch })
    }

    /// `<os>-<arch>` (the `platform` key in `toolchains.toml`).
    pub fn key(&self) -> String {
        format!("{}-{}", self.os, self.arch)
    }
}

fn unsupported(os: &str, arch: &str) -> ToolchainError {
    ToolchainError::Unsupported {
        os: os.to_string(),
        arch: arch.to_string(),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PinFile {
    tool: Vec<RawPin>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPin {
    name: String,
    version: String,
    platform: String,
    kind: String,
    url: String,
    sha256: String,
    strip: Option<String>,
    max_bytes: Option<u64>,
}

/// The id of the embedded pin file (sha256 of its bytes, first 16 hex).
pub fn set_id() -> String {
    hex(&Sha256::digest(TOOLCHAINS_TOML.as_bytes()))[..16].to_string()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Every pin of every platform, as `(platform, pin)`, validated.
pub fn all_pins() -> Result<Vec<(String, Pin)>, ToolchainError> {
    parse_pins(TOOLCHAINS_TOML)
}

fn parse_pins(text: &str) -> Result<Vec<(String, Pin)>, ToolchainError> {
    let file: PinFile = toml::from_str(text)
        .map_err(|e| ToolchainError::Config(format!("toolchains.toml: {e}")))?;
    let bad = |what: String| ToolchainError::Config(format!("toolchains.toml: {what}"));
    let mut out = Vec::new();
    for raw in file.tool {
        let tool = Tool::from_name(&raw.name)
            .ok_or_else(|| bad(format!("unknown tool {:?}", raw.name)))?;
        let kind = ArchiveKind::parse(&raw.kind)
            .ok_or_else(|| bad(format!("unknown kind {:?} for {}", raw.kind, raw.name)))?;
        if !PLATFORMS.contains(&raw.platform.as_str()) {
            return Err(bad(format!("unknown platform {:?}", raw.platform)));
        }
        if !is_sha256_hex(&raw.sha256) {
            return Err(bad(format!(
                "{} {}: sha256 is not 64 lower-case hex",
                raw.name, raw.platform
            )));
        }
        if !raw.url.starts_with("https://") {
            return Err(bad(format!(
                "{} {}: url is not https",
                raw.name, raw.platform
            )));
        }
        out.push((
            raw.platform,
            Pin {
                tool,
                version: raw.version,
                url: raw.url,
                sha256: raw.sha256,
                kind,
                strip: raw.strip,
                max_bytes: raw.max_bytes,
            },
        ));
    }
    Ok(out)
}

/// `s` is 64 lower-case hex digits.
pub fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The pinned set of `platform`.
pub fn toolchain_set(platform: &Platform) -> Result<ToolchainSet, ToolchainError> {
    let key = platform.key();
    let pins = all_pins()?
        .into_iter()
        .filter(|(p, _)| *p == key)
        .map(|(_, pin)| pin)
        .collect::<Vec<_>>();
    if pins.is_empty() {
        return Err(unsupported(platform.os, platform.arch));
    }
    Ok(ToolchainSet { id: set_id(), pins })
}

/// The tools a `lang` build needs (Rust: zig only when no `cc` is on PATH).
pub fn tools_for(lang: Lang) -> Vec<Tool> {
    tools_for_with(lang, cc_available())
}

/// [`tools_for`] with an explicit `cc` presence.
pub fn tools_for_with(lang: Lang, cc_present: bool) -> Vec<Tool> {
    match lang {
        Lang::Rust if cc_present => vec![Tool::RustupInit],
        Lang::Rust => vec![Tool::RustupInit, Tool::Zig],
        Lang::Python => vec![Tool::Uv, Tool::ComponentizePy],
        Lang::Ts => vec![Tool::Node],
    }
}

/// Download, verify and install `pin` into `cache` (a cache hit makes no request); returns
/// the installed entry dir. Builders (10-12, 10-15) obtain every tool through this.
pub async fn ensure(
    cache: &Cache,
    pin: &Pin,
    opts: &FetchOptions,
) -> Result<PathBuf, ToolchainError> {
    crate::cache::install(cache, pin, opts).await
}

/// The first executable called `name` on `PATH`.
pub fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join(name))
        .find(|p| is_executable(p))
}

fn is_executable(p: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(p)
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        p.is_file()
    }
}

/// A host C compiler (`cc`, else `gcc`, else `clang`) is on PATH.
pub fn cc_available() -> bool {
    ["cc", "gcc", "clang"].iter().any(|c| which(c).is_some())
}

/// The host glibc version from `ldd --version` (Linux only; `None` on musl or elsewhere).
pub fn glibc_version() -> Option<String> {
    if std::env::consts::OS != "linux" {
        return None;
    }
    let out = std::process::Command::new("ldd")
        .arg("--version")
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    parse_glibc_version(&text)
}

/// The version in the first line of `ldd --version` when it is glibc's.
pub fn parse_glibc_version(text: &str) -> Option<String> {
    let first = text.lines().next()?;
    let lower = first.to_ascii_lowercase();
    if !(lower.contains("glibc") || lower.contains("gnu libc")) {
        return None;
    }
    let last = first.split_whitespace().last()?;
    let ok = last.split('.').count() >= 2
        && last
            .split('.')
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    ok.then(|| last.to_string())
}

/// `major.minor` of a glibc version is at least 2.28 (manylinux_2_28, official Node builds).
pub fn glibc_at_least_2_28(version: &str) -> bool {
    let mut it = version.split('.').map(|p| p.parse::<u32>().unwrap_or(0));
    let (major, minor) = (it.next().unwrap_or(0), it.next().unwrap_or(0));
    (major, minor) >= (2, 28)
}

/// The host is a musl Linux (official Node and manylinux wheels do not run there).
pub fn musl_host() -> bool {
    if std::env::consts::OS != "linux" {
        return false;
    }
    if let Ok(rd) = std::fs::read_dir("/lib") {
        for e in rd.flatten() {
            if e.file_name().to_string_lossy().starts_with("ld-musl-") {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toolchain_every_platform_has_every_tool() {
        for key in PLATFORMS {
            let (os, arch) = key.split_once('-').unwrap();
            let set = toolchain_set(&Platform::from_parts(os, arch).unwrap()).unwrap();
            for tool in Tool::PINNED {
                let n = set.pins.iter().filter(|p| p.tool == tool).count();
                assert_eq!(n, 1, "{key}: {tool} pinned {n} times");
            }
            assert_eq!(set.pins.len(), Tool::PINNED.len(), "{key}");
        }
    }

    #[test]
    fn toolchain_versions_match_decisions() {
        for (platform, pin) in all_pins().unwrap() {
            let want = match pin.tool {
                Tool::Uv => "0.7.15",
                Tool::ComponentizePy => "0.25.1",
                Tool::Node => "24.13.0",
                Tool::RustupInit => "1.29.3",
                Tool::Zig => "0.17.1",
                other => panic!("unexpected pinned tool {other}"),
            };
            assert_eq!(pin.version, want, "{platform} {}", pin.tool);
            assert!(
                pin.url.contains(want),
                "{platform} {}: url lacks version",
                pin.tool
            );
        }
    }

    #[test]
    fn toolchain_shas_and_urls_are_well_formed() {
        let pins = all_pins().unwrap();
        assert_eq!(pins.len(), PLATFORMS.len() * Tool::PINNED.len());
        for (platform, pin) in &pins {
            assert!(is_sha256_hex(&pin.sha256), "{platform} {}", pin.tool);
            assert!(pin.url.starts_with("https://"), "{platform} {}", pin.tool);
            match pin.tool {
                Tool::ComponentizePy => {
                    assert_eq!(pin.kind, ArchiveKind::Wheel);
                    assert!(pin.file_name().ends_with(".whl"));
                    assert!(!pin.url.contains("github.com"), "never the GitHub binary");
                }
                Tool::RustupInit => assert_eq!(pin.kind, ArchiveKind::Binary),
                Tool::Node => {
                    let strip = pin.strip.as_deref().unwrap();
                    assert!(
                        pin.file_name().starts_with(strip),
                        "{platform}: strip {strip}"
                    );
                }
                Tool::Uv | Tool::Zig => assert!(pin.strip.is_some()),
                _ => {}
            }
        }
        let mut shas: Vec<_> = pins.iter().map(|(_, p)| &p.sha256).collect();
        shas.sort();
        shas.dedup();
        assert_eq!(shas.len(), pins.len(), "duplicate sha256");
        let manylinux: Vec<_> = pins
            .iter()
            .filter(|(p, pin)| p.starts_with("linux") && pin.tool == Tool::ComponentizePy)
            .map(|(_, pin)| pin.file_name().to_string())
            .collect();
        assert!(
            manylinux.iter().all(|f| f.contains("manylinux_2_28")),
            "{manylinux:?}"
        );
    }

    #[test]
    fn toolchain_set_id_is_stable() {
        let id = set_id();
        assert_eq!(id.len(), 16);
        assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
        let p = Platform::from_parts("linux", "x86_64").unwrap();
        assert_eq!(toolchain_set(&p).unwrap().id, id);
        assert_eq!(set_id(), id);
    }

    #[test]
    fn toolchain_platform_detection() {
        assert_eq!(
            Platform::from_parts("macos", "aarch64").unwrap().key(),
            "macos-aarch64"
        );
        assert!(matches!(
            Platform::from_parts("windows", "x86_64"),
            Err(ToolchainError::Unsupported { .. })
        ));
        assert!(Platform::from_parts("linux", "riscv64").is_err());
    }

    #[test]
    fn toolchain_tools_for_lang() {
        assert_eq!(tools_for_with(Lang::Rust, true), vec![Tool::RustupInit]);
        assert_eq!(
            tools_for_with(Lang::Rust, false),
            vec![Tool::RustupInit, Tool::Zig]
        );
        assert_eq!(
            tools_for_with(Lang::Python, true),
            vec![Tool::Uv, Tool::ComponentizePy]
        );
        assert_eq!(tools_for_with(Lang::Ts, false), vec![Tool::Node]);
        for t in Tool::PINNED {
            assert_eq!(Tool::from_name(t.name()), Some(t));
        }
    }

    #[test]
    fn toolchain_parse_rejects_bad_pins() {
        let base = |sha: &str, url: &str| {
            format!(
                "[[tool]]\nname=\"uv\"\nversion=\"1\"\nplatform=\"linux-x86_64\"\nkind=\"binary\"\nurl=\"{url}\"\nsha256=\"{sha}\"\n"
            )
        };
        let good = "a".repeat(64);
        assert!(parse_pins(&base(&good, "https://x/y")).is_ok());
        assert!(parse_pins(&base(&"A".repeat(64), "https://x/y")).is_err());
        assert!(parse_pins(&base(&"a".repeat(63), "https://x/y")).is_err());
        assert!(parse_pins(&base(&good, "http://x/y")).is_err());
        assert!(parse_pins(&format!("{}extra=1\n", base(&good, "https://x/y"))).is_err());
    }

    #[test]
    fn toolchain_glibc_parse() {
        assert_eq!(
            parse_glibc_version("ldd (Ubuntu GLIBC 2.39-0ubuntu8.4) 2.39\nCopyright\n").as_deref(),
            Some("2.39")
        );
        assert_eq!(
            parse_glibc_version("ldd (GNU libc) 2.28\n").as_deref(),
            Some("2.28")
        );
        assert_eq!(
            parse_glibc_version("musl libc (x86_64)\nVersion 1.2.4\n"),
            None
        );
        assert!(glibc_at_least_2_28("2.28"));
        assert!(glibc_at_least_2_28("2.39"));
        assert!(!glibc_at_least_2_28("2.17"));
        assert!(glibc_at_least_2_28("3.0"));
    }
}
