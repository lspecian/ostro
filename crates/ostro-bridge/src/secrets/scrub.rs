//! The response scrubber (FX-07): replaces every resolved secret value, in every common
//! encoding, with `[REDACTED]` before an outcome is encoded.
//!
//! Patterns per value:
//!
//! - raw bytes;
//! - lowercase and uppercase hex;
//! - standard and URL-safe base64, padded and unpadded, of the value on its own, plus the
//!   **stable middle** of the value's encoding at each of the three byte alignments: only the
//!   characters determined solely by the value's bytes, so a value embedded anywhere in a
//!   larger base64 blob is still caught (at most the two boundary characters, i.e. a few
//!   bits, remain);
//! - percent-encoding of every byte (upper and lower hex) and the RFC 3986 form that keeps
//!   unreserved characters;
//! - JSON string escaping (when the value is UTF-8): `serde_json`'s form without the quotes,
//!   every control character as `\u00XX` (lower and upper hex), and the ASCII-only form with
//!   every non-ASCII character as `\uXXXX`.
//!
//! Matching uses aho-corasick over all patterns. Every match is found (overlapping search),
//! overlapping or adjacent matches are merged, and each merged range becomes one
//! `[REDACTED]`, so two secrets that overlap in the text are both removed entirely.
//!
//! The automaton holds copies of the patterns that are not zeroized (aho-corasick owns
//! them); the build-time pattern buffers are.

use std::fmt;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use zeroize::{Zeroize, Zeroizing};

use super::value::SecretValue;

/// The replacement text.
pub const REDACTED: &[u8] = b"[REDACTED]";

/// Scrubs secret values (and their encodings) from bytes and strings.
pub struct Scrubber {
    ac: Option<AhoCorasick>,
    patterns: usize,
}

impl fmt::Debug for Scrubber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scrubber")
            .field("patterns", &self.patterns)
            .finish()
    }
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";
const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

fn hex(v: &[u8], table: &[u8; 16]) -> Zeroizing<Vec<u8>> {
    let mut out = Zeroizing::new(Vec::with_capacity(v.len() * 2));
    for &b in v {
        out.push(table[usize::from(b >> 4)]);
        out.push(table[usize::from(b & 0xf)]);
    }
    out
}

fn unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
}

fn percent(v: &[u8], table: &[u8; 16], keep_unreserved: bool) -> Zeroizing<Vec<u8>> {
    let mut out = Zeroizing::new(Vec::with_capacity(v.len() * 3));
    for &b in v {
        if keep_unreserved && unreserved(b) {
            out.push(b);
        } else {
            out.push(b'%');
            out.push(table[usize::from(b >> 4)]);
            out.push(table[usize::from(b & 0xf)]);
        }
    }
    out
}

fn b64(engine: &impl Engine, v: &[u8]) -> Zeroizing<Vec<u8>> {
    let mut s = engine.encode(v);
    let out = Zeroizing::new(s.as_bytes().to_vec());
    s.zeroize();
    out
}

/// The base64 characters of `v` that do not depend on surrounding bytes when `v` starts at
/// byte offset `align` (0..3) of the encoded stream.
fn b64_stable_middle(engine: &impl Engine, v: &[u8], align: usize) -> Zeroizing<Vec<u8>> {
    let mut buf = Zeroizing::new(vec![0u8; align]);
    buf.extend_from_slice(v);
    let enc = b64(engine, &buf);
    // Character k covers stream bits [6k, 6k + 6); it depends only on `v` when
    // 8 * align <= 6k and 6k + 6 <= 8 * (align + len).
    let first = (8 * align).div_ceil(6);
    let end = (8 * (align + v.len())) / 6;
    if end <= first {
        return Zeroizing::new(Vec::new());
    }
    Zeroizing::new(enc[first..end].to_vec())
}

fn json_forms(s: &str) -> Vec<Zeroizing<Vec<u8>>> {
    let mut out = Vec::new();
    let mut quoted = serde_json::to_string(s).expect("a str always serialises");
    out.push(Zeroizing::new(
        quoted.as_bytes()[1..quoted.len() - 1].to_vec(),
    ));
    quoted.zeroize();
    // All control characters as \u00XX, lower and upper hex; and the ASCII-only form.
    for (table, ascii_only) in [(HEX_LOWER, false), (HEX_UPPER, false), (HEX_LOWER, true)] {
        let mut o = Zeroizing::new(Vec::with_capacity(s.len() * 2));
        let mut units = [0u16; 2];
        for c in s.chars() {
            match c {
                '"' => o.extend_from_slice(b"\\\""),
                '\\' => o.extend_from_slice(b"\\\\"),
                c if (c as u32) < 0x20 || (ascii_only && !c.is_ascii()) => {
                    for &u in c.encode_utf16(&mut units).iter() {
                        o.extend_from_slice(b"\\u");
                        for shift in [12u16, 8, 4, 0] {
                            o.push(table[usize::from((u >> shift) & 0xf)]);
                        }
                    }
                }
                c => {
                    let mut tmp = [0u8; 4];
                    o.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
                    tmp.zeroize();
                }
            }
        }
        units.zeroize();
        out.push(o);
    }
    out
}

fn patterns_for(v: &[u8]) -> Vec<Zeroizing<Vec<u8>>> {
    let mut p = vec![
        Zeroizing::new(v.to_vec()),
        hex(v, HEX_LOWER),
        hex(v, HEX_UPPER),
        percent(v, HEX_UPPER, false),
        percent(v, HEX_LOWER, false),
        percent(v, HEX_UPPER, true),
        percent(v, HEX_LOWER, true),
        b64(&STANDARD, v),
        b64(&STANDARD_NO_PAD, v),
        b64(&URL_SAFE, v),
        b64(&URL_SAFE_NO_PAD, v),
    ];
    for align in 0..3 {
        p.push(b64_stable_middle(&STANDARD_NO_PAD, v, align));
        p.push(b64_stable_middle(&URL_SAFE_NO_PAD, v, align));
    }
    if let Ok(s) = std::str::from_utf8(v) {
        p.extend(json_forms(s));
    }
    p
}

impl Scrubber {
    /// Builds the automaton for `values` (empty values are ignored). An empty list gives a
    /// no-op scrubber.
    pub fn new(values: &[&SecretValue]) -> Self {
        let mut set: Vec<Zeroizing<Vec<u8>>> = Vec::new();
        for v in values.iter().filter(|v| !v.is_empty()) {
            set.extend(
                patterns_for(v.expose())
                    .into_iter()
                    .filter(|p| !p.is_empty()),
            );
        }
        set.sort_unstable_by(|a, b| a.as_slice().cmp(b.as_slice()));
        set.dedup_by(|a, b| a.as_slice() == b.as_slice());
        if set.is_empty() {
            return Self {
                ac: None,
                patterns: 0,
            };
        }
        let pats: Vec<&[u8]> = set.iter().map(|p| p.as_slice()).collect();
        let ac = AhoCorasickBuilder::new()
            .match_kind(MatchKind::Standard)
            .build(&pats)
            .expect("bounded literal patterns always build");
        Self {
            ac: Some(ac),
            patterns: set.len(),
        }
    }

    /// Whether the scrubber has no patterns (scrubbing is a no-op).
    pub fn is_empty(&self) -> bool {
        self.ac.is_none()
    }

    /// Merged byte ranges of every match in `h`.
    fn ranges(&self, h: &[u8]) -> Vec<(usize, usize)> {
        let Some(ac) = &self.ac else {
            return Vec::new();
        };
        let mut spans: Vec<(usize, usize)> = ac
            .find_overlapping_iter(h)
            .map(|m| (m.start(), m.end()))
            .collect();
        spans.sort_unstable();
        let mut merged: Vec<(usize, usize)> = Vec::with_capacity(spans.len());
        for (s, e) in spans {
            match merged.last_mut() {
                Some(last) if s <= last.1 => last.1 = last.1.max(e),
                _ => merged.push((s, e)),
            }
        }
        merged
    }

    fn apply(h: &[u8], ranges: &[(usize, usize)]) -> Vec<u8> {
        let mut out = Vec::with_capacity(h.len());
        let mut last = 0;
        for &(s, e) in ranges {
            out.extend_from_slice(&h[last..s]);
            out.extend_from_slice(REDACTED);
            last = e;
        }
        out.extend_from_slice(&h[last..]);
        out
    }

    /// Scrubs `b` in place; returns the number of `[REDACTED]` replacements. The original
    /// buffer is zeroized before it is released.
    pub fn scrub_bytes(&self, b: &mut Vec<u8>) -> usize {
        let ranges = self.ranges(b);
        if ranges.is_empty() {
            return 0;
        }
        let mut out = Self::apply(b, &ranges);
        std::mem::swap(b, &mut out);
        out.zeroize();
        ranges.len()
    }

    /// A scrubbed copy of `s`. A replacement that splits a multi-byte character is repaired
    /// with U+FFFD.
    pub fn scrub_str(&self, s: &str) -> String {
        let ranges = self.ranges(s.as_bytes());
        if ranges.is_empty() {
            return s.to_owned();
        }
        let mut out = Self::apply(s.as_bytes(), &ranges);
        let r = String::from_utf8_lossy(&out).into_owned();
        out.zeroize();
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lc_rs::rand::fill;

    /// 24-byte canary (non-ASCII, a control char and a quote included, so every encoding
    /// differs from the raw bytes).
    fn canary() -> SecretValue {
        SecretValue::new("sk_live\"é\n/+?=&%Zx9·QW".as_bytes().to_vec())
    }

    fn random_bytes(n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n];
        fill(&mut v).unwrap();
        v
    }

    fn contains(h: &[u8], needle: &[u8]) -> bool {
        h.windows(needle.len()).any(|w| w == needle)
    }

    /// ~1 KiB of JSON-ish text with `enc` embedded at offset `pad` inside a string.
    fn json_doc(enc: &[u8], pad: usize) -> Vec<u8> {
        let mut doc = br#"{"id":"evt_123","data":{"object":{"note":""#.to_vec();
        doc.extend(std::iter::repeat_n(b'q', pad));
        doc.extend_from_slice(enc);
        doc.extend_from_slice(br#"","filler":""#);
        doc.extend(
            STANDARD
                .encode(random_bytes(700))
                .bytes()
                .filter(|b| b.is_ascii_alphanumeric()),
        );
        doc.extend_from_slice(br#""}}}"#);
        doc
    }

    fn encodings(v: &[u8]) -> Vec<(String, Vec<u8>)> {
        let s = std::str::from_utf8(v).unwrap();
        let q = serde_json::to_string(s).unwrap();
        vec![
            ("raw".into(), v.to_vec()),
            ("hex".into(), hex(v, HEX_LOWER).to_vec()),
            ("HEX".into(), hex(v, HEX_UPPER).to_vec()),
            ("pct-all".into(), percent(v, HEX_UPPER, false).to_vec()),
            (
                "pct-all-lower".into(),
                percent(v, HEX_LOWER, false).to_vec(),
            ),
            ("pct-3986".into(), percent(v, HEX_UPPER, true).to_vec()),
            ("json".into(), q.as_bytes()[1..q.len() - 1].to_vec()),
            ("b64".into(), STANDARD.encode(v).into_bytes()),
            ("b64-nopad".into(), STANDARD_NO_PAD.encode(v).into_bytes()),
            ("b64url".into(), URL_SAFE.encode(v).into_bytes()),
            (
                "b64url-nopad".into(),
                URL_SAFE_NO_PAD.encode(v).into_bytes(),
            ),
        ]
    }

    #[test]
    fn every_encoding_at_every_offset_is_scrubbed() {
        let v = canary();
        assert_eq!(v.len(), 24);
        let s = Scrubber::new(&[&v]);
        for (name, enc) in encodings(v.expose()) {
            for pad in 0..3 {
                let mut doc = json_doc(&enc, pad);
                let n = s.scrub_bytes(&mut doc);
                assert!(n >= 1, "{name} at {pad}: nothing replaced");
                assert!(!contains(&doc, &enc), "{name} at {pad} survived");
                assert!(contains(&doc, REDACTED));
                assert!(!contains(&doc, v.expose()));
            }
        }
    }

    #[test]
    fn value_inside_a_larger_base64_blob_is_scrubbed_at_all_alignments() {
        let v = canary();
        let s = Scrubber::new(&[&v]);
        for engine_name in ["std", "url"] {
            for align in 0..3 {
                for tail in 0..3 {
                    let mut blob = random_bytes(300 + align);
                    blob.extend_from_slice(v.expose());
                    blob.extend(random_bytes(200 + tail));
                    let enc = if engine_name == "std" {
                        STANDARD.encode(&blob)
                    } else {
                        URL_SAFE_NO_PAD.encode(&blob)
                    };
                    let mut doc = json_doc(enc.as_bytes(), 0);
                    assert!(s.scrub_bytes(&mut doc) >= 1, "{engine_name} align {align}");
                    // Decode what is left: the canary bytes must be gone.
                    let mid = b64_stable_middle(&STANDARD_NO_PAD, v.expose(), align);
                    let mid_url = b64_stable_middle(&URL_SAFE_NO_PAD, v.expose(), align);
                    assert!(!contains(&doc, &mid) && !contains(&doc, &mid_url));
                }
            }
        }
    }

    #[test]
    fn json_control_and_ascii_forms() {
        let v = canary();
        let s = Scrubber::new(&[&v]);
        let forms = json_forms(std::str::from_utf8(v.expose()).unwrap());
        assert!(
            forms
                .iter()
                .any(|f| contains(f, b"\\u000a") || contains(f, b"\\u000A"))
        );
        assert!(forms.iter().any(|f| contains(f, b"\\u00e9")));
        for f in &forms {
            let mut doc = json_doc(f, 1);
            assert!(s.scrub_bytes(&mut doc) >= 1);
            assert!(!contains(&doc, f));
        }
    }

    #[test]
    fn empty_scrubber_is_a_no_op() {
        let s = Scrubber::new(&[]);
        assert!(s.is_empty());
        let mut b = b"anything".to_vec();
        assert_eq!(s.scrub_bytes(&mut b), 0);
        assert_eq!(b, b"anything");
        assert_eq!(s.scrub_str("x"), "x");
        let e = SecretValue::new(Vec::new());
        assert!(Scrubber::new(&[&e]).is_empty());
    }

    #[test]
    fn overlapping_secrets_are_both_removed() {
        let a = SecretValue::new(b"AAAAxxxxSHARED".to_vec());
        let b = SecretValue::new(b"SHAREDyyyyBBBB".to_vec());
        let s = Scrubber::new(&[&a, &b]);
        let mut doc = b"pre AAAAxxxxSHAREDyyyyBBBB post".to_vec();
        assert_eq!(s.scrub_bytes(&mut doc), 1);
        assert_eq!(doc, b"pre [REDACTED] post");
        // One secret containing the other.
        let inner = SecretValue::new(b"middle-part".to_vec());
        let outer = SecretValue::new(b"outer-middle-part-outer".to_vec());
        let s = Scrubber::new(&[&inner, &outer]);
        assert_eq!(
            s.scrub_str("x outer-middle-part-outer y middle-part z"),
            "x [REDACTED] y [REDACTED] z"
        );
    }

    #[test]
    fn scrub_str_and_debug() {
        let v = canary();
        let s = Scrubber::new(&[&v]);
        let text = format!("token={}", STANDARD.encode(v.expose()));
        assert_eq!(s.scrub_str(&text), "token=[REDACTED]");
        let d = format!("{s:?}");
        assert!(d.starts_with("Scrubber { patterns: "));
        assert!(!d.contains("sk_live"));
        assert!(!s.is_empty());
    }
}
