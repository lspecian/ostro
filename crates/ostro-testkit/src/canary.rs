//! Canary leak harness (plan 06-19, feature `canary`; P-12, D-FX6).
//!
//! A [`Canary`] is a random printable value (32 base62 characters, fresh per call) that a
//! test stores as a secret and then hunts for everywhere it must never appear. The harness
//! has its **own** encoder implementation and never depends on ostro-bridge, so it
//! cross-checks the bridge's scrubber instead of sharing its bugs.
//!
//! - [`Canary::needles`]: every form searched for: raw; standard and URL-safe base64, padded
//!   and unpadded, of the value alone and, at byte alignments 0/1/2, the stable middle that
//!   any larger base64 blob containing the value must carry; lower/upper hex;
//!   percent-encoding of every byte (upper/lower) and the RFC 3986 unreserved-preserving
//!   form; JSON string escaping (serde form and ASCII-only form).
//! - [`Canary::scan_bytes`] / [`Canary::scan_tree`]: memmem over the needles; the tree walk
//!   reads every regular file fully and follows no symlinks.
//! - [`CaptureLayer`]: a `tracing_subscriber::Layer` that records every span's and event's
//!   fields, at TRACE, into one process-wide buffer ([`CaptureLayer::install_global`],
//!   [`CaptureLayer::captured`], capped at [`MAX_CAPTURE_BYTES`]). `log` records are bridged
//!   too. [`CaptureLayer::watch`] scans every line for a canary as it is recorded, so the
//!   cap never hides a leak. Only the wasm compiler's targets are left out
//!   ([`EXCLUDED_TARGET_PREFIXES`]: IR dumps of guest code, which holds no secret value).
//! - [`register_sink`] / [`scan_sinks`]: extra byte sources scanned by every report. This
//!   is the **D-FX6 hook**: Phase 6 covers tracing output through [`CaptureLayer`]; Phase 8
//!   registers an OTLP mock collector here (`register_sink("otlp", ..)`) so the same canary
//!   scan covers the OTLP export at the Phase 8 gate.
//! - [`Report`]: aggregates hits per scanned location; [`Report::assert_clean`] panics
//!   naming location, encoding and offset only.
//! - [`spawn_quiet`]: runs a mock server on its own OS thread and current-thread tokio
//!   runtime with a thread-local `NoSubscriber`, so a mock that legitimately sees the canary
//!   never feeds [`CaptureLayer`] (W6).
//!
//! The harness never prints the canary: `Debug` is redacted, and hits carry only where and
//! how the value was found.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Length of a generated canary (base62 characters).
pub const CANARY_LEN: usize = 32;

const BASE62: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Name prefix of the threads [`spawn_quiet`] starts ([`CaptureLayer`] drops anything
/// recorded on them, a second guard behind the thread-local `NoSubscriber`).
pub const QUIET_THREAD_PREFIX: &str = "ostro-testkit-quiet-";

/// Target prefix [`CaptureLayer`] drops (the testkit's own mock servers).
pub const MOCK_TARGET_PREFIX: &str = "ostro_testkit::mock";

// ---------------------------------------------------------------------------------
// Canary and needles
// ---------------------------------------------------------------------------------

/// One searched form of the canary. `Debug` never shows the bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct Needle {
    /// Encoding label, e.g. `base64-url-nopad@1`.
    pub encoding: String,
    bytes: Vec<u8>,
}

impl Needle {
    /// The searched bytes (for tests that embed a needle on purpose).
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for Needle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Needle")
            .field("encoding", &self.encoding)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// A canary secret value. `Debug` prints `Canary(<redacted>)`.
#[derive(Clone)]
pub struct Canary {
    id: u64,
    value: Vec<u8>,
    needles: Vec<Needle>,
}

impl fmt::Debug for Canary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Canary(<redacted>)")
    }
}

impl Default for Canary {
    fn default() -> Self {
        Self::new()
    }
}

fn random_u64() -> u64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    // RandomState is keyed from the OS RNG once per thread and stepped per instance.
    let mut h = RandomState::new().build_hasher();
    h.write_u64(SEQ.fetch_add(1, Ordering::Relaxed));
    h.write_u32(std::process::id());
    if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        h.write_u128(d.as_nanos());
    }
    h.finish()
}

impl Canary {
    /// A fresh random canary: [`CANARY_LEN`] printable base62 characters.
    pub fn new() -> Self {
        let value: Vec<u8> = (0..CANARY_LEN)
            .map(|_| BASE62[(random_u64() % 62) as usize])
            .collect();
        Self::from_value(value)
    }

    /// A canary with a given value (unit tests use non-alphanumeric values so every
    /// encoding is distinct). Must be at least 8 bytes.
    pub fn from_value(value: impl Into<Vec<u8>>) -> Self {
        let value = value.into();
        assert!(value.len() >= 8, "a canary needs at least 8 bytes");
        let needles = build_needles(&value);
        Self {
            id: random_u64(),
            value,
            needles,
        }
    }

    /// The raw value, for configuring the secret under test. Never print it.
    pub fn expose(&self) -> &[u8] {
        &self.value
    }

    /// The raw value as text (generated canaries are ASCII). Never print it.
    pub fn expose_str(&self) -> &str {
        std::str::from_utf8(&self.value).expect("canary is not UTF-8")
    }

    /// Every searched form, deduplicated by bytes (first label wins).
    pub fn needles(&self) -> &[Needle] {
        &self.needles
    }

    /// Every needle occurrence in `bytes`, labelled `label`.
    pub fn scan_bytes(&self, label: &str, bytes: &[u8]) -> Vec<Hit> {
        let mut hits = Vec::new();
        for n in &self.needles {
            let mut from = 0;
            while let Some(at) = find(&bytes[from..], &n.bytes) {
                hits.push(Hit {
                    label: label.to_owned(),
                    encoding: n.encoding.clone(),
                    offset: from + at,
                });
                from += at + 1;
            }
        }
        hits
    }

    /// Every needle occurrence in every regular file under `dir` (symlinks are not
    /// followed; files that vanish during the walk are skipped). The label is the path.
    pub fn scan_tree(&self, dir: &Path) -> Vec<Hit> {
        self.scan_tree_counted(dir).0
    }

    /// [`scan_tree`](Self::scan_tree) plus `(files, bytes)` read.
    pub fn scan_tree_counted(&self, dir: &Path) -> (Vec<Hit>, usize, usize) {
        let mut hits = Vec::new();
        let (mut files, mut bytes) = (0, 0);
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            let entries = match std::fs::read_dir(&d) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => panic!("scan_tree: read_dir {}: {e}", d.display()),
            };
            for entry in entries {
                let entry = entry.unwrap_or_else(|e| panic!("scan_tree: {}: {e}", d.display()));
                let path = entry.path();
                let meta = match std::fs::symlink_metadata(&path) {
                    Ok(m) => m,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => panic!("scan_tree: stat {}: {e}", path.display()),
                };
                let ft = meta.file_type();
                if ft.is_dir() {
                    stack.push(path);
                } else if ft.is_file() {
                    let data = match std::fs::read(&path) {
                        Ok(b) => b,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(e) => panic!("scan_tree: read {}: {e}", path.display()),
                    };
                    files += 1;
                    bytes += data.len();
                    hits.extend(self.scan_bytes(&path.display().to_string(), &data));
                }
            }
        }
        (hits, files, bytes)
    }
}

/// First occurrence of `needle` in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    let first = needle[0];
    let last = hay.len() - needle.len();
    let mut i = 0;
    while i <= last {
        i += hay[i..=last].iter().position(|&b| b == first)?;
        if &hay[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";
const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

fn hex(v: &[u8], table: &[u8; 16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 2);
    for &b in v {
        out.push(table[usize::from(b >> 4)]);
        out.push(table[usize::from(b & 15)]);
    }
    out
}

fn percent(v: &[u8], table: &[u8; 16], keep_unreserved: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 3);
    for &b in v {
        if keep_unreserved && (b.is_ascii_alphanumeric() || b"-._~".contains(&b)) {
            out.push(b);
        } else {
            out.push(b'%');
            out.push(table[usize::from(b >> 4)]);
            out.push(table[usize::from(b & 15)]);
        }
    }
    out
}

/// JSON string body (no quotes): `"` and `\` escaped, the short escapes for
/// `\b \f \n \r \t`, other controls as `\u00xx` (`upper` picks the hex case); non-ASCII
/// kept as UTF-8 unless `ascii_only` (then `\uXXXX`, surrogate pairs above the BMP).
fn json_escape(s: &str, upper: bool, ascii_only: bool) -> Vec<u8> {
    let table = if upper { HEX_UPPER } else { HEX_LOWER };
    let unit = |out: &mut Vec<u8>, u: u16| {
        out.extend_from_slice(b"\\u");
        for shift in [12u16, 8, 4, 0] {
            out.push(table[usize::from((u >> shift) & 15)]);
        }
    };
    let mut out = Vec::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{8}' => out.extend_from_slice(b"\\b"),
            '\u{c}' => out.extend_from_slice(b"\\f"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => unit(&mut out, c as u16),
            c if ascii_only && !c.is_ascii() => {
                let mut buf = [0u16; 2];
                for u in c.encode_utf16(&mut buf) {
                    unit(&mut out, *u);
                }
            }
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out
}

/// The base64 characters of `prefix_k || v` that depend only on `v`'s bytes: whole
/// 4-character groups that lie inside `v`. Any base64 blob that contains `v` at a byte
/// offset `≡ k (mod 3)` contains this substring.
fn b64_stable(engine: &impl base64::Engine, v: &[u8], k: usize) -> Vec<u8> {
    let mut buf = vec![0xA5u8; k];
    buf.extend_from_slice(v);
    let enc = engine.encode(&buf).into_bytes();
    let start = 4 * k.div_ceil(3);
    let end = 4 * (buf.len() / 3);
    enc[start..end].to_vec()
}

fn build_needles(v: &[u8]) -> Vec<Needle> {
    let mut forms: Vec<(String, Vec<u8>)> = vec![("raw".into(), v.to_vec())];
    let engines: [(&str, &base64::engine::GeneralPurpose); 4] = [
        ("base64-std-pad", &STANDARD),
        ("base64-std-nopad", &STANDARD_NO_PAD),
        ("base64-url-pad", &URL_SAFE),
        ("base64-url-nopad", &URL_SAFE_NO_PAD),
    ];
    for (name, e) in engines {
        forms.push((name.into(), e.encode(v).into_bytes()));
        for k in 0..3 {
            forms.push((format!("{name}@{k}"), b64_stable(e, v, k)));
        }
    }
    forms.push(("hex-lower".into(), hex(v, HEX_LOWER)));
    forms.push(("hex-upper".into(), hex(v, HEX_UPPER)));
    forms.push(("percent-all-upper".into(), percent(v, HEX_UPPER, false)));
    forms.push(("percent-all-lower".into(), percent(v, HEX_LOWER, false)));
    forms.push(("percent-unreserved".into(), percent(v, HEX_UPPER, true)));
    forms.push((
        "percent-unreserved-lower".into(),
        percent(v, HEX_LOWER, true),
    ));
    if let Ok(s) = std::str::from_utf8(v) {
        forms.push(("json".into(), json_escape(s, false, false)));
        forms.push(("json-upper".into(), json_escape(s, true, false)));
        forms.push(("json-ascii".into(), json_escape(s, false, true)));
        forms.push(("json-ascii-upper".into(), json_escape(s, true, true)));
    }
    let mut out: Vec<Needle> = Vec::new();
    for (encoding, bytes) in forms {
        if bytes.len() >= 8 && !out.iter().any(|n| n.bytes == bytes) {
            out.push(Needle { encoding, bytes });
        }
    }
    out
}

// ---------------------------------------------------------------------------------
// Hits and reports
// ---------------------------------------------------------------------------------

/// One needle occurrence: where and in which encoding (never the value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hit {
    /// Location (a path, `journal`, `tracing`, `sink:otlp`, ...).
    pub label: String,
    /// The needle's encoding label.
    pub encoding: String,
    /// Byte offset within the location.
    pub offset: usize,
}

/// One scanned location of a [`Report`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location {
    /// Location label.
    pub label: String,
    /// Files read (1 for a byte source).
    pub files: usize,
    /// Bytes scanned.
    pub bytes: usize,
    /// Hits found there.
    pub hits: usize,
}

/// Aggregated scan results for one canary.
pub struct Report<'a> {
    canary: &'a Canary,
    hits: Vec<Hit>,
    locations: Vec<Location>,
}

impl fmt::Debug for Report<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Report")
            .field("hits", &self.hits)
            .field("locations", &self.locations)
            .finish()
    }
}

impl<'a> Report<'a> {
    /// An empty report for `canary`.
    pub fn new(canary: &'a Canary) -> Self {
        Self {
            canary,
            hits: Vec::new(),
            locations: Vec::new(),
        }
    }

    /// Scan a byte source.
    pub fn bytes(&mut self, label: &str, bytes: &[u8]) -> &mut Self {
        let hits = self.canary.scan_bytes(label, bytes);
        self.locations.push(Location {
            label: label.to_owned(),
            files: 1,
            bytes: bytes.len(),
            hits: hits.len(),
        });
        self.hits.extend(hits);
        self
    }

    /// Scan every regular file under `dir`.
    pub fn tree(&mut self, label: &str, dir: &Path) -> &mut Self {
        let (hits, files, bytes) = self.canary.scan_tree_counted(dir);
        self.locations.push(Location {
            label: label.to_owned(),
            files,
            bytes,
            hits: hits.len(),
        });
        self.hits.extend(hits);
        self
    }

    /// Scan [`CaptureLayer::captured`] plus the hits streamed for this canary since
    /// [`CaptureLayer::watch`]. Panics when the buffer was truncated and the canary was
    /// never watched (the scan would be incomplete).
    pub fn tracing(&mut self) -> &mut Self {
        let buf = CaptureLayer::captured();
        self.bytes("tracing (buffer)", &buf);
        let stats = CaptureLayer::stats();
        match CaptureLayer::streamed_hits(self.canary) {
            Some(hits) => {
                self.locations.push(Location {
                    label: "tracing (streamed)".into(),
                    files: 1,
                    bytes: stats.bytes,
                    hits: hits.len(),
                });
                self.hits.extend(hits);
            }
            None => assert!(
                !stats.truncated,
                "the capture buffer was truncated and this canary was not watched"
            ),
        }
        self
    }

    /// Scan every registered sink ([`register_sink`]).
    pub fn sinks(&mut self) -> &mut Self {
        for (name, bytes) in sink_contents() {
            self.bytes(&format!("sink:{name}"), &bytes);
        }
        self
    }

    /// Every hit so far.
    pub fn hits(&self) -> &[Hit] {
        &self.hits
    }

    /// Every scanned location so far.
    pub fn locations(&self) -> &[Location] {
        &self.locations
    }

    /// One line per location: label, files, bytes, hits.
    pub fn summary(&self) -> String {
        let mut s = String::new();
        for l in &self.locations {
            s.push_str(&format!(
                "{:<40} files={:<5} bytes={:<10} hits={}\n",
                l.label, l.files, l.bytes, l.hits
            ));
        }
        s
    }

    /// Panics when any hit was found, naming location, encoding and offset only.
    pub fn assert_clean(&self) {
        if self.hits.is_empty() {
            return;
        }
        let mut msg = format!("canary leaked ({} hits):\n", self.hits.len());
        for h in self.hits.iter().take(50) {
            msg.push_str(&format!(
                "  {} [{}] at byte {}\n",
                h.label, h.encoding, h.offset
            ));
        }
        panic!("{msg}");
    }
}

// ---------------------------------------------------------------------------------
// Sinks (D-FX6 hook)
// ---------------------------------------------------------------------------------

/// A byte source scanned by [`scan_sinks`] and [`Report::sinks`].
pub type SinkFn = Box<dyn Fn() -> Vec<u8> + Send + Sync>;

fn sinks() -> &'static Mutex<Vec<(String, SinkFn)>> {
    static SINKS: OnceLock<Mutex<Vec<(String, SinkFn)>>> = OnceLock::new();
    SINKS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Register (or replace) the sink `name`. D-FX6: Phase 8 registers its OTLP mock
/// collector's received bytes here, so every canary report also scans the OTLP export.
pub fn register_sink(name: &str, f: SinkFn) {
    let mut s = sinks().lock().unwrap_or_else(PoisonError::into_inner);
    s.retain(|(n, _)| n != name);
    s.push((name.to_owned(), f));
}

/// Names of the registered sinks.
pub fn sink_names() -> Vec<String> {
    let s = sinks().lock().unwrap_or_else(PoisonError::into_inner);
    s.iter().map(|(n, _)| n.clone()).collect()
}

fn sink_contents() -> Vec<(String, Vec<u8>)> {
    let s = sinks().lock().unwrap_or_else(PoisonError::into_inner);
    s.iter().map(|(n, f)| (n.clone(), f())).collect()
}

/// Every needle occurrence in every registered sink (label `sink:<name>`).
pub fn scan_sinks(canary: &Canary) -> Vec<Hit> {
    sink_contents()
        .into_iter()
        .flat_map(|(n, b)| canary.scan_bytes(&format!("sink:{n}"), &b))
        .collect()
}

// ---------------------------------------------------------------------------------
// CaptureLayer
// ---------------------------------------------------------------------------------

/// Largest buffer [`CaptureLayer::captured`] keeps (256 MiB). Lines past it are still
/// scanned for every watched canary ([`CaptureLayer::watch`]), just not stored.
pub const MAX_CAPTURE_BYTES: usize = 256 << 20;

/// Targets the capture leaves disabled: the wasm compiler (cranelift / regalloc2 and
/// wasmtime's cranelift glue). At TRACE they dump the IR of every compiled guest function
/// (gigabytes per module in a debug build), and their only input is guest wasm code, which
/// never holds a secret value: secrets are resolved by the bridge at send time, long after
/// compilation. Everything else, down to TRACE, is captured.
pub const EXCLUDED_TARGET_PREFIXES: &[&str] = &[
    "cranelift",
    "regalloc2",
    "wasmtime_cranelift",
    "wasmtime_internal_cranelift",
    "wasmtime_environ::compile",
];

#[derive(Default)]
struct Capture {
    buf: Vec<u8>,
    /// Every byte formatted (stored or not).
    total: usize,
    lines: usize,
    truncated: bool,
    /// (canary id, needles, streamed hits).
    watched: Vec<(u64, Vec<Needle>, Vec<Hit>)>,
    /// target -> (lines, bytes).
    per_target: std::collections::BTreeMap<String, (usize, usize)>,
}

fn capture() -> &'static Mutex<Capture> {
    static CAP: OnceLock<Mutex<Capture>> = OnceLock::new();
    CAP.get_or_init(|| Mutex::new(Capture::default()))
}

/// A layer recording every span's and event's fields (Debug and Display forms) into
/// one process-wide buffer, down to TRACE (`RUST_LOG` ignored). Lines are also scanned as
/// they are recorded for every watched canary, so the buffer cap never hides a leak. See
/// [`EXCLUDED_TARGET_PREFIXES`] for the only targets left out.
#[derive(Clone, Copy, Debug, Default)]
pub struct CaptureLayer;

struct Writer(String);

impl Visit for Writer {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.push_str(&format!(" {}={:?}", field.name(), value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push_str(&format!(
            " {}={} {}?={:?}",
            field.name(),
            value,
            field.name(),
            value
        ));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.0.push_str(&format!(
            " {}={} {}?={:?}",
            field.name(),
            value,
            field.name(),
            value
        ));
    }
}

fn excluded(target: &str) -> bool {
    EXCLUDED_TARGET_PREFIXES
        .iter()
        .any(|p| target.starts_with(p))
}

fn skipped(meta: &Metadata<'_>) -> bool {
    meta.target().starts_with(MOCK_TARGET_PREFIX)
        || std::thread::current()
            .name()
            .is_some_and(|n| n.starts_with(QUIET_THREAD_PREFIX))
}

fn push(target: &str, line: String) {
    let mut c = capture().lock().unwrap_or_else(PoisonError::into_inner);
    let bytes = line.as_bytes();
    let base = c.total;
    for (_, needles, hits) in c.watched.iter_mut() {
        for n in needles.iter() {
            let mut from = 0;
            while let Some(at) = find(&bytes[from..], &n.bytes) {
                hits.push(Hit {
                    label: "tracing (streamed)".into(),
                    encoding: n.encoding.clone(),
                    offset: base + from + at,
                });
                from += at + 1;
            }
        }
    }
    c.total += bytes.len() + 1;
    c.lines += 1;
    match c.per_target.get_mut(target) {
        Some(e) => {
            e.0 += 1;
            e.1 += bytes.len() + 1;
        }
        None => {
            c.per_target.insert(target.to_owned(), (1, bytes.len() + 1));
        }
    }
    if c.buf.len() + bytes.len() < MAX_CAPTURE_BYTES {
        c.buf.extend_from_slice(bytes);
        c.buf.push(b'\n');
    } else {
        c.truncated = true;
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, meta: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        !excluded(meta.target())
    }

    fn register_callsite(&self, meta: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        if excluded(meta.target()) {
            tracing::subscriber::Interest::never()
        } else {
            tracing::subscriber::Interest::always()
        }
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        let meta = attrs.metadata();
        if skipped(meta) {
            return;
        }
        let mut w = Writer(format!(
            "span {} {} {}",
            meta.level(),
            meta.target(),
            meta.name()
        ));
        attrs.record(&mut w);
        push(meta.target(), w.0);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let target = match ctx.metadata(id) {
            Some(meta) if skipped(meta) => return,
            Some(meta) => meta.target(),
            None => "?",
        };
        let mut w = Writer(String::from("record"));
        values.record(&mut w);
        push(target, w.0);
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if skipped(meta) {
            return;
        }
        let mut w = Writer(format!("event {} {}", meta.level(), meta.target()));
        event.record(&mut w);
        push(meta.target(), w.0);
    }
}

/// Capture statistics ([`CaptureLayer::stats`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CaptureStats {
    /// Lines recorded.
    pub lines: usize,
    /// Bytes formatted (stored or not).
    pub bytes: usize,
    /// Whether the buffer hit [`MAX_CAPTURE_BYTES`].
    pub truncated: bool,
    /// The ten busiest targets: (target, lines, bytes).
    pub top_targets: Vec<(String, usize, usize)>,
}

impl CaptureLayer {
    /// Install `registry().with(CaptureLayer)` as the global subscriber (plus the `log`
    /// bridge), once per process. `RUST_LOG` is ignored. Panics when another global
    /// subscriber was installed first: the capture would then be empty and every scan of
    /// it vacuous.
    pub fn install_global() {
        static ONCE: OnceLock<()> = OnceLock::new();
        ONCE.get_or_init(|| {
            use tracing_subscriber::layer::SubscriberExt as _;
            use tracing_subscriber::util::SubscriberInitExt as _;
            tracing_subscriber::registry()
                .with(CaptureLayer)
                .try_init()
                .expect("CaptureLayer::install_global: a global subscriber already exists");
        });
    }

    /// Scan every line recorded from now on for `canary` as it is recorded (independent of
    /// the buffer cap). [`Report::tracing`] includes these hits.
    pub fn watch(canary: &Canary) {
        let mut c = capture().lock().unwrap_or_else(PoisonError::into_inner);
        if !c.watched.iter().any(|(id, _, _)| *id == canary.id) {
            c.watched
                .push((canary.id, canary.needles.clone(), Vec::new()));
        }
    }

    fn streamed_hits(canary: &Canary) -> Option<Vec<Hit>> {
        let c = capture().lock().unwrap_or_else(PoisonError::into_inner);
        c.watched
            .iter()
            .find(|(id, _, _)| *id == canary.id)
            .map(|(_, _, h)| h.clone())
    }

    /// A copy of the stored buffer (at most [`MAX_CAPTURE_BYTES`]).
    pub fn captured() -> Vec<u8> {
        capture()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .buf
            .clone()
    }

    /// Lines, bytes, truncation and the busiest targets so far.
    pub fn stats() -> CaptureStats {
        let c = capture().lock().unwrap_or_else(PoisonError::into_inner);
        let mut top: Vec<(String, usize, usize)> = c
            .per_target
            .iter()
            .map(|(t, (l, b))| (t.clone(), *l, *b))
            .collect();
        top.sort_by_key(|t| std::cmp::Reverse(t.2));
        top.truncate(10);
        CaptureStats {
            lines: c.lines,
            bytes: c.total,
            truncated: c.truncated,
            top_targets: top,
        }
    }
}

// ---------------------------------------------------------------------------------
// spawn_quiet
// ---------------------------------------------------------------------------------

/// The output of a future run by [`spawn_quiet`]; derefs to it. Dropping it drops the
/// value, then stops the thread's runtime and joins the thread.
pub struct Quiet<T> {
    value: Option<T>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl<T> std::ops::Deref for Quiet<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value.as_ref().expect("quiet value")
    }
}

impl<T> Drop for Quiet<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Run `fut` on a new OS thread with its own current-thread tokio runtime and a
/// thread-local `NoSubscriber` default, and return its output once ready. Tasks it spawns
/// keep running on that thread until the returned [`Quiet`] is dropped. Nothing logged
/// there (tracing or `log`) reaches the global subscriber, so mocks that see the canary
/// cannot pollute [`CaptureLayer`] (W6).
pub fn spawn_quiet<Fut>(fut: Fut) -> Quiet<Fut::Output>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    static N: AtomicU64 = AtomicU64::new(0);
    let (tx, rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let name = format!("{QUIET_THREAD_PREFIX}{}", N.fetch_add(1, Ordering::Relaxed));
    let thread = std::thread::Builder::new()
        .name(name)
        .spawn(move || {
            let _quiet =
                tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("quiet runtime");
            rt.block_on(async move {
                let v = fut.await;
                if tx.send(v).is_err() {
                    return;
                }
                let _ = stop_rx.await;
            });
        })
        .expect("spawn quiet thread");
    let value = rx.recv().expect("the quiet future panicked");
    Quiet {
        value: Some(value),
        stop: Some(stop_tx),
        thread: Some(thread),
    }
}

// ---------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A value with quotes, a backslash, controls, reserved URL bytes and non-ASCII, so
    /// every encoding differs from the raw form.
    fn odd() -> Canary {
        Canary::from_value(
            "k\"e\\y/+ \n\t\u{1}é€😀-tail-0123456789"
                .as_bytes()
                .to_vec(),
        )
    }

    fn noise(n: usize, seed: u8) -> Vec<u8> {
        (0..n)
            .map(|i| b"#~!*()[]{}"[(i * 7 + usize::from(seed)) % 10])
            .collect()
    }

    #[test]
    fn generated_canary_shape_and_redacted_debug() {
        let a = Canary::new();
        let b = Canary::new();
        assert_eq!(a.expose().len(), CANARY_LEN);
        assert!(a.expose().iter().all(u8::is_ascii_alphanumeric));
        assert_ne!(a.expose(), b.expose(), "fresh per call");
        assert_eq!(format!("{a:?}"), "Canary(<redacted>)");
        let dbg = format!("{:?}", a.needles());
        assert!(!dbg.contains(a.expose_str()));
    }

    #[test]
    fn every_needle_form_is_detected_in_noise() {
        for c in [odd(), Canary::new()] {
            assert!(c.needles().len() >= 5);
            for n in c.needles() {
                let mut hay = noise(97, 3);
                hay.extend_from_slice(n.bytes());
                hay.extend_from_slice(&noise(41, 5));
                let hits = c.scan_bytes("hay", &hay);
                assert!(
                    hits.iter().any(|h| h.offset == 97),
                    "{} not detected",
                    n.encoding
                );
            }
        }
    }

    #[test]
    fn expected_encodings_exist_for_an_odd_value() {
        let c = odd();
        let labels: Vec<&str> = c.needles().iter().map(|n| n.encoding.as_str()).collect();
        for l in [
            "raw",
            "base64-std-pad",
            "base64-std-pad@1",
            "base64-url-pad@2",
            "hex-lower",
            "hex-upper",
            "percent-all-upper",
            "percent-all-lower",
            "percent-unreserved",
            "json",
            "json-ascii",
        ] {
            assert!(labels.contains(&l), "missing {l}: {labels:?}");
        }
        // Forms that coincide byte for byte are kept once (first label wins).
        for e in [&STANDARD, &STANDARD_NO_PAD, &URL_SAFE, &URL_SAFE_NO_PAD] {
            let enc = e.encode(c.expose()).into_bytes();
            assert!(c.needles().iter().any(|n| n.bytes() == enc.as_slice()));
        }
        let json = c.needles().iter().find(|n| n.encoding == "json").unwrap();
        assert!(json.bytes().starts_with(b"k\\\"e\\\\y/+ \\n\\t\\u0001"));
        let ascii = c
            .needles()
            .iter()
            .find(|n| n.encoding == "json-ascii")
            .unwrap();
        assert!(
            ascii
                .bytes()
                .ends_with(b"\\u00e9\\u20ac\\ud83d\\ude00-tail-0123456789")
        );
    }

    #[test]
    fn value_inside_a_larger_base64_blob_is_found_at_every_alignment() {
        let c = odd();
        for engine in [&STANDARD, &STANDARD_NO_PAD, &URL_SAFE, &URL_SAFE_NO_PAD] {
            for k in 0..6 {
                let mut blob = noise(k, 1);
                blob.extend_from_slice(c.expose());
                blob.extend_from_slice(&noise(5, 2));
                let enc = engine.encode(&blob);
                assert!(
                    !c.scan_bytes("b64", enc.as_bytes()).is_empty(),
                    "alignment {k} missed"
                );
            }
        }
    }

    #[test]
    fn a_one_byte_different_value_is_not_detected() {
        for c in [odd(), Canary::new()] {
            let mut other = c.expose().to_vec();
            let mid = other.len() / 2;
            other[mid] ^= 0x01;
            let near = Canary::from_value(other);
            for n in near.needles() {
                let mut hay = noise(33, 4);
                hay.extend_from_slice(n.bytes());
                hay.extend_from_slice(&noise(33, 6));
                assert!(
                    c.scan_bytes("near", &hay).is_empty(),
                    "{} of a different value matched",
                    n.encoding
                );
            }
        }
    }

    #[test]
    fn scan_tree_reads_files_and_skips_symlinks() {
        let c = Canary::new();
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("a/b")).unwrap();
        std::fs::write(dir.path().join("a/b/clean"), b"nothing here").unwrap();
        let mut leak = b"prefix ".to_vec();
        leak.extend_from_slice(&hex(c.expose(), HEX_UPPER));
        std::fs::write(dir.path().join("a/leak"), &leak).unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), c.expose()).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("link")).unwrap();
        let (hits, files, _) = c.scan_tree_counted(dir.path());
        assert_eq!(files, 2);
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert!(hits[0].label.ends_with("a/leak"));
        assert_eq!(hits[0].encoding, "hex-upper");
        assert_eq!(hits[0].offset, 7);
    }

    #[test]
    fn report_aggregates_and_panics_without_the_value() {
        let c = Canary::new();
        let mut r = Report::new(&c);
        r.bytes("clean", b"no secret");
        assert!(r.hits().is_empty());
        r.assert_clean();
        let mut doc = b"{\"v\":\"".to_vec();
        doc.extend_from_slice(STANDARD.encode(c.expose()).as_bytes());
        r.bytes("api", &doc);
        assert_eq!(r.locations().len(), 2);
        let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| r.assert_clean()))
            .expect_err("must panic");
        let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(msg.contains("api ["), "{msg}");
        assert!(!msg.contains(c.expose_str()));
        assert!(!r.summary().contains(c.expose_str()));
    }

    #[test]
    fn sinks_are_scanned() {
        let c = Canary::new();
        let v = c.expose().to_vec();
        register_sink("unit-test", Box::new(move || v.clone()));
        assert!(sink_names().contains(&"unit-test".to_owned()));
        let hits = scan_sinks(&c);
        assert!(hits.iter().any(|h| h.label == "sink:unit-test"));
        register_sink("unit-test", Box::new(Vec::new));
        assert!(scan_sinks(&c).is_empty());
    }

    #[test]
    fn capture_layer_records_and_quiet_threads_do_not() {
        CaptureLayer::install_global();
        let marker = Canary::new();
        let quiet_marker = Canary::new();
        CaptureLayer::watch(&marker);
        CaptureLayer::watch(&quiet_marker);
        tracing::trace!(target: "cranelift_codegen::context", value = %quiet_marker.expose_str(), "ir");
        let m = marker.expose_str().to_owned();
        tracing::trace!(field = %m, "trace event");
        let span = tracing::trace_span!("s", f = tracing::field::Empty);
        span.record(
            "f",
            format!(
                "rec-{}",
                String::from_utf8(hex(marker.expose(), HEX_LOWER)).unwrap()
            ),
        );
        let q = quiet_marker.expose_str().to_owned();
        let quiet = spawn_quiet(async move {
            tracing::error!(value = %q, "from a quiet thread");
            tokio::spawn(async {}).await.ok();
            7u8
        });
        assert_eq!(*quiet, 7);
        drop(quiet);
        tracing::info!(target: "ostro_testkit::mock_provider", value = %quiet_marker.expose_str(), "mock");
        let buf = CaptureLayer::captured();
        let hits = marker.scan_bytes("tracing", &buf);
        assert!(hits.iter().any(|h| h.encoding == "raw"), "event captured");
        assert!(
            hits.iter().any(|h| h.encoding == "hex-lower"),
            "span record captured"
        );
        assert!(
            quiet_marker.scan_bytes("tracing", &buf).is_empty(),
            "quiet thread, mock target and compiler target are not captured"
        );
        let mut r = Report::new(&marker);
        r.tracing();
        assert!(r.hits().iter().any(|h| h.label == "tracing (streamed)"));
        let mut r = Report::new(&quiet_marker);
        r.tracing();
        r.assert_clean();
        assert!(CaptureLayer::stats().lines > 0);
    }
}
