//! QA-02 replay-corpus entries as a library (plan 10-11, CLI-07): load and verify the
//! 05-12 entry directories (`tests/replay-corpus/<entry>/`) so `ostro replay --corpus`
//! and `tests/replay_corpus.rs` share one loader. The verdict logic is
//! [`crate::replay_check`] (11-10), used unchanged.
//!
//! An entry is a directory with exactly three files:
//! - `module.wasm`: the component bytes the journal was recorded with;
//! - `journal.bin`: repeated `u32 LE length ‖ JournalEvent protobuf` (canonical prost
//!   encoding, payloads always inline, seqs contiguous from 0);
//! - `entry.toml`: metadata (`tests/replay-corpus/README.md`).
//!
//! [`load_corpus_entry`] checks the file set, the entry name, both sha256s, the event
//! count, the token key, the journal codec (every record canonical, every event
//! re-encoding to the same bytes) and that seq 0 is the `RunStarted` of the entry's
//! workflow (with `input_json` when given). The error strings are the corpus test's.
//!
//! The engine has no protobuf or TOML dependency, so both formats are read here by hand:
//! `JournalEvent{seq = 1 (varint), kind = 2 (varint), payload = 3 (PayloadRef{inline = 1}),
//! attempt = 4 (fixed64)}` is the frozen wire shape (RESERVATIONS), and `entry.toml` is
//! flat `key = value` lines (basic / literal strings and integers) before the
//! `[recorded_with]` table, which is informational and not read.

use std::path::{Path, PathBuf};

use ostro_types::journal::{Event, EventBody, decode_event, encode_event};
use sha2::{Digest, Sha256};

use crate::sink::RunRef;

/// The three files of an entry; anything else in an entry directory is an error.
pub const ENTRY_FILES: [&str; 3] = ["entry.toml", "journal.bin", "module.wasm"];

/// A loaded and verified corpus entry.
#[derive(Debug, Clone)]
pub struct CorpusEntry {
    /// Entry name (= its directory name).
    pub name: String,
    /// `RunStarted.workflow`.
    pub workflow: String,
    /// `completed` | `failed` | `continued_as_new` | `cancelled`.
    pub expected: String,
    /// Await-token key the run was recorded with (`token_key_hex`).
    pub token_key: [u8; 32],
    /// The decoded journal.
    pub journal: Vec<Event>,
    /// `module.wasm` bytes.
    pub module: Vec<u8>,
    /// The run the journal was recorded as (`partition`, `run_id`).
    pub run: RunRef,
}

/// Lowercase hex.
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn sha_hex(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

/// 64 hex characters as 32 bytes.
pub fn unhex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = s
            .get(2 * i..2 * i + 2)
            .and_then(|h| u8::from_str_radix(h, 16).ok())
            .ok_or_else(|| format!("hex: invalid digit at {}", 2 * i))?;
    }
    Ok(out)
}

// ------------------------------------------------------------------------- journal.bin

fn read_varint(b: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*pos).ok_or("truncated varint")?;
        *pos += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err("varint longer than 10 bytes".to_owned())
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn read_len<'a>(b: &'a [u8], pos: &mut usize) -> Result<&'a [u8], String> {
    let len = usize::try_from(read_varint(b, pos)?).map_err(|_| "length overflow")?;
    let s = b
        .get(*pos..pos.saturating_add(len))
        .ok_or("truncated length-delimited field")?;
    *pos += len;
    Ok(s)
}

/// One `JournalEvent` record: `(seq, kind, inline payload, attempt)`.
#[derive(Debug)]
struct Record<'a> {
    seq: u64,
    kind: u32,
    payload: &'a [u8],
    attempt: u64,
}

/// `PayloadRef` bytes → the inline payload (`Blob` and empty refs are errors).
fn inline_payload(b: &[u8]) -> Result<&[u8], String> {
    let mut pos = 0;
    let mut inline = None;
    while pos < b.len() {
        match read_varint(b, &mut pos)? {
            0x0a => inline = Some(read_len(b, &mut pos)?),
            0x12 => return Err("payload must be inline, got a blob reference".to_owned()),
            k => return Err(format!("payload: unexpected field key {k}")),
        }
    }
    inline.ok_or_else(|| "payload must be inline, got none".to_owned())
}

fn decode_record(msg: &[u8]) -> Result<Record<'_>, String> {
    let mut pos = 0;
    let mut r = Record {
        seq: 0,
        kind: 0,
        payload: &[],
        attempt: 0,
    };
    let mut payload = None;
    while pos < msg.len() {
        match read_varint(msg, &mut pos)? {
            0x08 => r.seq = read_varint(msg, &mut pos)?,
            0x10 => {
                r.kind = u32::try_from(read_varint(msg, &mut pos)?)
                    .map_err(|_| "kind out of range".to_owned())?;
            }
            0x1a => payload = Some(inline_payload(read_len(msg, &mut pos)?)?),
            0x21 => {
                let b: [u8; 8] = msg
                    .get(pos..pos + 8)
                    .and_then(|s| s.try_into().ok())
                    .ok_or("truncated fixed64")?;
                pos += 8;
                r.attempt = u64::from_le_bytes(b);
            }
            k => return Err(format!("unexpected field key {k}")),
        }
    }
    r.payload = payload.ok_or("payload must be inline, got none")?;
    Ok(r)
}

/// The canonical (prost) encoding of a record: fields in tag order, defaults omitted,
/// the payload always present.
fn encode_record(r: &Record<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(r.payload.len() + 24);
    if r.seq != 0 {
        out.push(0x08);
        put_varint(&mut out, r.seq);
    }
    if r.kind != 0 {
        out.push(0x10);
        put_varint(&mut out, u64::from(r.kind));
    }
    let mut p = vec![0x0a];
    put_varint(&mut p, r.payload.len() as u64);
    p.extend_from_slice(r.payload);
    out.push(0x1a);
    put_varint(&mut out, p.len() as u64);
    out.extend_from_slice(&p);
    if r.attempt != 0 {
        out.push(0x21);
        out.extend_from_slice(&r.attempt.to_le_bytes());
    }
    out
}

/// Decode `journal.bin` into events (contiguous seqs from 0, inline payloads, canonical
/// protobuf bytes, every event re-encoding identically).
pub fn decode_journal_bin(bin: &[u8]) -> Result<Vec<Event>, String> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < bin.len() {
        let len_bytes: [u8; 4] = bin
            .get(pos..pos + 4)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| format!("truncated length prefix at byte {pos}"))?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        pos += 4;
        let msg = bin
            .get(pos..pos + len)
            .ok_or_else(|| format!("truncated record at byte {pos} (len {len})"))?;
        pos += len;
        let seq = out.len() as u64;
        let r = decode_record(msg).map_err(|e| format!("record {seq}: {e}"))?;
        if encode_record(&r) != msg {
            return Err(format!("record {seq}: not canonical protobuf"));
        }
        if r.seq != seq {
            return Err(format!("record {seq}: seq {} (want {seq})", r.seq));
        }
        let ev = decode_event(seq, r.kind, r.payload).map_err(|e| format!("seq {seq}: {e}"))?;
        if encode_event(&ev) != (r.kind, r.payload.to_vec()) {
            return Err(format!(
                "seq {seq}: event bytes do not re-encode identically"
            ));
        }
        out.push(ev);
    }
    Ok(out)
}

/// Encode events as `journal.bin` (inline payloads, `attempt` 0), the inverse of
/// [`decode_journal_bin`].
pub fn encode_journal_bin(journal: &[Event]) -> Vec<u8> {
    let mut out = Vec::new();
    for ev in journal {
        let (kind, bytes) = encode_event(ev);
        let msg = encode_record(&Record {
            seq: ev.seq,
            kind,
            payload: &bytes,
            attempt: 0,
        });
        out.extend_from_slice(&(msg.len() as u32).to_le_bytes());
        out.extend_from_slice(&msg);
    }
    out
}

// -------------------------------------------------------------------------- entry.toml

/// A top-level `entry.toml` value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TomlValue {
    Str(String),
    Int(i64),
}

/// A basic string body after the opening quote; returns the value and the rest.
fn basic_string(s: &str) -> Result<(String, &str), String> {
    let mut out = String::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Ok((out, &s[i + 1..])),
            '\\' => {
                let (_, e) = chars.next().ok_or("unterminated escape")?;
                match e {
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    'u' | 'U' => {
                        let n = if e == 'u' { 4 } else { 8 };
                        let mut code = String::new();
                        for _ in 0..n {
                            code.push(chars.next().ok_or("short unicode escape")?.1);
                        }
                        let v = u32::from_str_radix(&code, 16)
                            .ok()
                            .and_then(char::from_u32)
                            .ok_or_else(|| format!("bad unicode escape \\{e}{code}"))?;
                        out.push(v);
                    }
                    other => return Err(format!("unknown escape \\{other}")),
                }
            }
            c => out.push(c),
        }
    }
    Err("unterminated string".to_owned())
}

/// The top-level `key = value` pairs of `entry.toml` (comments and blank lines skipped,
/// reading stops at the first table header).
fn parse_toml_top(text: &str) -> Result<Vec<(String, TomlValue)>, String> {
    let mut out = Vec::new();
    for (n, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') {
            break;
        }
        let err = |m: &str| format!("line {}: {m}", n + 1);
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| err("expected key = value"))?;
        let key = k.trim();
        if key.is_empty()
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(err("unsupported key"));
        }
        let v = v.trim_start();
        let (value, rest) = if v.starts_with("\"\"\"") || v.starts_with("'''") {
            return Err(err("multi-line strings are not supported at the top level"));
        } else if let Some(body) = v.strip_prefix('"') {
            let (s, rest) = basic_string(body).map_err(|e| err(&e))?;
            (TomlValue::Str(s), rest)
        } else if let Some(body) = v.strip_prefix('\'') {
            let end = body.find('\'').ok_or_else(|| err("unterminated string"))?;
            (TomlValue::Str(body[..end].to_owned()), &body[end + 1..])
        } else {
            let end = v.find('#').unwrap_or(v.len());
            let num = v[..end].trim().replace('_', "");
            let i = num
                .parse::<i64>()
                .map_err(|_| err(&format!("unsupported value {:?}", v[..end].trim())))?;
            (TomlValue::Int(i), "")
        };
        let rest = rest.trim();
        if !(rest.is_empty() || rest.starts_with('#')) {
            return Err(err("trailing characters after the value"));
        }
        if out.iter().any(|(k, _)| k == key) {
            return Err(err(&format!("duplicate key {key}")));
        }
        out.push((key.to_owned(), value));
    }
    Ok(out)
}

/// The metadata the loader reads from `entry.toml`.
struct Meta {
    name: String,
    workflow: String,
    input_json: Option<String>,
    expected: String,
    partition: u16,
    run_id: String,
    token_key_hex: String,
    journal_events: u64,
    module_sha256: String,
    journal_sha256: String,
}

fn parse_meta(text: &str) -> Result<Meta, String> {
    let kv = parse_toml_top(text)?;
    let get = |k: &str| kv.iter().find(|(key, _)| key == k).map(|(_, v)| v);
    let string = |k: &str| match get(k) {
        Some(TomlValue::Str(s)) => Ok(s.clone()),
        Some(_) => Err(format!("{k}: expected a string")),
        None => Err(format!("missing field `{k}`")),
    };
    let int = |k: &str| match get(k) {
        Some(TomlValue::Int(i)) => Ok(*i),
        Some(_) => Err(format!("{k}: expected an integer")),
        None => Err(format!("missing field `{k}`")),
    };
    Ok(Meta {
        name: string("name")?,
        workflow: string("workflow")?,
        input_json: match get("input_json") {
            None => None,
            Some(_) => Some(string("input_json")?),
        },
        expected: string("expected")?,
        partition: u16::try_from(int("partition")?).map_err(|_| "partition: out of range")?,
        run_id: string("run_id")?,
        token_key_hex: string("token_key_hex")?,
        journal_events: u64::try_from(int("journal_events")?)
            .map_err(|_| "journal_events: negative")?,
        module_sha256: string("module_sha256")?,
        journal_sha256: string("journal_sha256")?,
    })
}

// ------------------------------------------------------------------------------ loader

/// Read and verify one entry directory (see the module docs).
pub fn load_corpus_entry(dir: &Path) -> Result<CorpusEntry, String> {
    let mut files: Vec<String> = std::fs::read_dir(dir)
        .map_err(|e| format!("read dir: {e}"))?
        .map(|e| {
            e.map(|e| e.file_name().to_string_lossy().into_owned())
                .map_err(|e| format!("read dir: {e}"))
        })
        .collect::<Result<_, _>>()?;
    files.sort();
    if files != ENTRY_FILES {
        return Err(format!("entry files are {files:?}, want {ENTRY_FILES:?}"));
    }
    let toml_text =
        std::fs::read_to_string(dir.join("entry.toml")).map_err(|e| format!("entry.toml: {e}"))?;
    let m = parse_meta(&toml_text).map_err(|e| format!("entry.toml: {e}"))?;
    let dir_name = dir.file_name().map(|n| n.to_string_lossy().into_owned());
    if dir_name.as_deref() != Some(m.name.as_str()) {
        return Err(format!(
            "entry.toml name {:?} != directory {dir_name:?}",
            m.name
        ));
    }
    let module = std::fs::read(dir.join("module.wasm")).map_err(|e| format!("module: {e}"))?;
    let got = sha_hex(&module);
    if got != m.module_sha256 {
        return Err(format!(
            "module_sha256 mismatch: file {got}, entry.toml {}",
            m.module_sha256
        ));
    }
    let bin = std::fs::read(dir.join("journal.bin")).map_err(|e| format!("journal: {e}"))?;
    let got = sha_hex(&bin);
    if got != m.journal_sha256 {
        return Err(format!(
            "journal_sha256 mismatch: file {got}, entry.toml {}",
            m.journal_sha256
        ));
    }
    let journal = decode_journal_bin(&bin)?;
    if journal.len() as u64 != m.journal_events {
        return Err(format!(
            "journal_events mismatch: file {}, entry.toml {}",
            journal.len(),
            m.journal_events
        ));
    }
    let token_key = unhex32(&m.token_key_hex).map_err(|e| format!("token_key_hex: {e}"))?;
    match journal.first().map(|e| &e.body) {
        Some(EventBody::RunStarted(rs)) if rs.workflow == m.workflow => {
            if let Some(input) = &m.input_json
                && input.as_bytes() != rs.input.data.as_slice()
            {
                return Err(format!(
                    "input_json {input:?} != RunStarted input {:?}",
                    String::from_utf8_lossy(&rs.input.data)
                ));
            }
        }
        other => {
            return Err(format!(
                "seq 0 must be RunStarted of workflow {:?}, got {other:?}",
                m.workflow
            ));
        }
    }
    Ok(CorpusEntry {
        name: m.name,
        workflow: m.workflow,
        expected: m.expected,
        token_key,
        journal,
        module,
        run: RunRef {
            partition: m.partition,
            run_id: m.run_id,
        },
    })
}

/// The entry directories under `root`, sorted by name (regular files such as the README
/// are skipped). A missing `root` is an error.
pub fn corpus_entry_dirs(root: &Path) -> Result<Vec<PathBuf>, String> {
    let rd = std::fs::read_dir(root).map_err(|e| format!("{}: {e}", root.display()))?;
    let mut dirs = Vec::new();
    for e in rd {
        let p = e.map_err(|e| format!("{}: {e}", root.display()))?.path();
        if p.is_dir() {
            dirs.push(p);
        }
    }
    dirs.sort();
    Ok(dirs)
}

/// Every entry under `root`, sorted by name; the first bad entry is an error naming it.
pub fn load_corpus(root: &Path) -> Result<Vec<CorpusEntry>, String> {
    corpus_entry_dirs(root)?
        .iter()
        .map(|d| {
            load_corpus_entry(d).map_err(|e| {
                let name = d.file_name().map(|n| n.to_string_lossy().into_owned());
                format!("{}: {e}", name.unwrap_or_default())
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip() {
        for v in [0u64, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            let mut pos = 0;
            assert_eq!(read_varint(&b, &mut pos).unwrap(), v);
            assert_eq!(pos, b.len());
        }
        assert!(read_varint(&[0x80], &mut 0).is_err());
    }

    #[test]
    fn records_round_trip_and_reject_blobs() {
        let r = Record {
            seq: 5,
            kind: 3,
            payload: b"abc",
            attempt: 7 << 32 | 1,
        };
        let b = encode_record(&r);
        let d = decode_record(&b).unwrap();
        assert_eq!(
            (d.seq, d.kind, d.payload, d.attempt),
            (5, 3, &b"abc"[..], r.attempt)
        );
        // seq 0, kind 0, attempt 0 are omitted; the payload is always present.
        let z = encode_record(&Record {
            seq: 0,
            kind: 0,
            payload: b"",
            attempt: 0,
        });
        assert_eq!(z, [0x1a, 0x02, 0x0a, 0x00]);
        // A blob reference is refused.
        let blob = [0x1a, 0x02, 0x12, 0x00];
        assert!(decode_record(&blob).unwrap_err().contains("blob"));
        assert!(decode_record(&[0x28, 0x01]).is_err());
    }

    #[test]
    fn toml_top_level() {
        let t = "# c\nname = \"x\"\nn = 7 # seven\nlit = '{\"a\":1}'\nesc = \"q\\\"\\u0041\"\n\n[t]\nk = \"\"\"multi\"\"\"\n";
        let kv = parse_toml_top(t).unwrap();
        assert_eq!(
            kv,
            vec![
                ("name".into(), TomlValue::Str("x".into())),
                ("n".into(), TomlValue::Int(7)),
                ("lit".into(), TomlValue::Str("{\"a\":1}".into())),
                ("esc".into(), TomlValue::Str("q\"A".into())),
            ]
        );
        assert!(parse_toml_top("a = \"x\" y").is_err());
        assert!(parse_toml_top("a = 1\na = 2").is_err());
        assert!(parse_toml_top("a = \"\"\"x\"\"\"").is_err());
        assert!(parse_toml_top("a = true").is_err());
    }

    #[test]
    fn unhex() {
        assert_eq!(unhex32(&"09".repeat(32)).unwrap(), [9; 32]);
        assert!(unhex32(&"zz".repeat(32)).is_err());
        assert!(unhex32("00").is_err());
    }
}
