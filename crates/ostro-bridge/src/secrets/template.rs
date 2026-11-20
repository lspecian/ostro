//! `{{secret:NAME}}` placeholders (FX-06). This module parses and expands templates; it never
//! resolves a secret.
//!
//! Grammar (case-sensitive): `{{secret:` NAME `}}`, with NAME matching
//! `[a-z0-9][a-z0-9_.\-/]{0,127}`. Further rules on NAME:
//!
//! - it must not start with the reserved prefix `__ostro/`;
//! - it must not contain `..`, an empty path segment (`a//b`, a trailing `/`) or a `.`
//!   segment, so the file provider can join it under its directory safely.
//!
//! There is no escaping: every literal `{{secret:` starts a placeholder, and a malformed one
//! is an error. Errors carry byte offsets and never echo the text after `{{secret:` (it
//! could be secret material pasted by mistake), except for the reserved-prefix error, whose
//! name is a well-formed reserved name.

use std::collections::BTreeMap;
use std::ops::Range;

use zeroize::Zeroizing;

use super::value::SecretValue;

/// Opening marker of a placeholder.
pub const OPEN: &str = "{{secret:";
/// Closing marker of a placeholder.
pub const CLOSE: &str = "}}";
/// Reserved name prefix (RESERVATIONS §11): guests and the admin API can never reference it.
pub const RESERVED_PREFIX: &str = "__ostro/";
/// Longest allowed name.
pub const MAX_NAME_LEN: usize = 128;

/// One placeholder found in a template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placeholder {
    /// The secret name.
    pub name: String,
    /// Byte range of the whole `{{secret:NAME}}` in the template.
    pub range: Range<usize>,
}

/// A template or a secret name is invalid.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    /// `{{secret:` at byte `at` has no closing `}}`.
    #[error("unterminated secret placeholder at byte {at}")]
    Unterminated {
        /// Byte offset of `{{secret:`.
        at: usize,
    },
    /// The name of the placeholder at byte `at` does not match the grammar.
    #[error("invalid secret name in placeholder at byte {at}")]
    InvalidName {
        /// Byte offset of `{{secret:` (0 for a bare name).
        at: usize,
    },
    /// The name uses the reserved `__ostro/` prefix.
    #[error("secret name {name:?} uses the reserved prefix `__ostro/`")]
    Reserved {
        /// The reserved name.
        name: String,
    },
    /// The name contains `..`, an empty segment or a `.` segment.
    #[error("invalid path segment in secret name at byte {at}")]
    BadPath {
        /// Byte offset of `{{secret:` (0 for a bare name).
        at: usize,
    },
}

fn name_byte(b: u8) -> bool {
    matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'/')
}

fn check_name(n: &str, at: usize) -> Result<(), TemplateError> {
    if n.starts_with(RESERVED_PREFIX) {
        return Err(TemplateError::Reserved { name: n.to_owned() });
    }
    let b = n.as_bytes();
    if b.is_empty()
        || b.len() > MAX_NAME_LEN
        || !matches!(b[0], b'a'..=b'z' | b'0'..=b'9')
        || !b.iter().all(|&c| name_byte(c))
    {
        return Err(TemplateError::InvalidName { at });
    }
    if n.contains("..") || n.split('/').any(|seg| seg.is_empty() || seg == ".") {
        return Err(TemplateError::BadPath { at });
    }
    Ok(())
}

/// Checks a bare secret name against the grammar and the path rules.
pub fn validate_name(n: &str) -> Result<(), TemplateError> {
    check_name(n, 0)
}

/// Whether `s` contains a placeholder marker (well-formed or not). Used to refuse
/// placeholders in the URL, the method and header names (`egress_denied`).
pub fn contains(s: &str) -> bool {
    s.contains(OPEN)
}

/// Every placeholder of `s`, in order. Fails on the first malformed one.
pub fn parse(s: &str) -> Result<Vec<Placeholder>, TemplateError> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = s[from..].find(OPEN) {
        let at = from + rel;
        let name_start = at + OPEN.len();
        let rest = &s.as_bytes()[name_start..];
        let name_len = rest.iter().take_while(|&&c| name_byte(c)).count();
        let after = &s[name_start + name_len..];
        if !after.starts_with(CLOSE) {
            return Err(if after.contains(CLOSE) {
                TemplateError::InvalidName { at }
            } else {
                TemplateError::Unterminated { at }
            });
        }
        let name = &s[name_start..name_start + name_len];
        // A reserved name has `_` first, which the byte scan accepts; check_name reports it.
        check_name(name, at)?;
        let end = name_start + name_len + CLOSE.len();
        out.push(Placeholder {
            name: name.to_owned(),
            range: at..end,
        });
        from = end;
    }
    Ok(out)
}

/// Replaces every placeholder of `s` with its value from `values`.
///
/// Callers `parse` first and resolve every name; `expand` itself never fails. A template that
/// does not parse is returned unchanged (it holds no secret bytes), and a placeholder whose
/// name is missing from `values` is copied literally.
///
/// The output buffer is sized exactly before any secret byte is written, so it never
/// reallocates (a reallocation would leave an unzeroized copy behind), and no intermediate
/// `String` holds secret bytes.
pub fn expand(s: &str, values: &BTreeMap<String, SecretValue>) -> Zeroizing<Vec<u8>> {
    let Ok(placeholders) = parse(s) else {
        return Zeroizing::new(s.as_bytes().to_vec());
    };
    let mut len = s.len();
    for p in &placeholders {
        if let Some(v) = values.get(&p.name) {
            len = len - p.range.len() + v.len();
        }
    }
    let mut out = Zeroizing::new(Vec::with_capacity(len));
    let mut last = 0;
    for p in &placeholders {
        if let Some(v) = values.get(&p.name) {
            out.extend_from_slice(&s.as_bytes()[last..p.range.start]);
            out.extend_from_slice(v.expose());
            last = p.range.end;
        }
    }
    out.extend_from_slice(&s.as_bytes()[last..]);
    debug_assert_eq!(out.len(), len);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn names(s: &str) -> Vec<String> {
        parse(s).unwrap().into_iter().map(|p| p.name).collect()
    }

    #[test]
    fn parses_placeholders_with_spans() {
        let s = "Bearer {{secret:stripe/live-key.v2}} and {{secret:a}}!";
        let p = parse(s).unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].name, "stripe/live-key.v2");
        assert_eq!(&s[p[0].range.clone()], "{{secret:stripe/live-key.v2}}");
        assert_eq!(p[1].name, "a");
        assert_eq!(&s[p[1].range.clone()], "{{secret:a}}");
        assert!(
            parse("no placeholders {{ secret }} {secret:x}")
                .unwrap()
                .is_empty()
        );
        assert_eq!(names("{{secret:x}}{{secret:y}}"), ["x", "y"]);
        assert_eq!(names("{{{secret:x}}}"), ["x"]);
    }

    #[test]
    fn contains_detects_any_marker() {
        assert!(contains("https://x/{{secret:k}}"));
        assert!(contains("{{secret:"));
        assert!(!contains("{{Secret:k}}"));
        assert!(!contains("plain"));
    }

    #[test]
    fn rejects_malformed() {
        assert_eq!(
            parse("x {{secret:abc").unwrap_err(),
            TemplateError::Unterminated { at: 2 }
        );
        assert_eq!(
            parse("{{secret:abc}").unwrap_err(),
            TemplateError::Unterminated { at: 0 }
        );
        for bad in [
            "{{secret:}}",
            "{{secret:ABC}}",
            "{{secret:a b}}",
            "{{secret:-a}}",
            "{{secret:.a}}",
            "{{secret:/a}}",
            "{{secret:a!}}",
            "{{secret:k} }}",
        ] {
            assert!(
                matches!(parse(bad), Err(TemplateError::InvalidName { at: 0 })),
                "{bad}"
            );
        }
        let long = format!("{{{{secret:{}}}}}", "a".repeat(129));
        assert!(matches!(
            parse(&long),
            Err(TemplateError::InvalidName { .. })
        ));
        let max = format!("{{{{secret:{}}}}}", "a".repeat(128));
        assert_eq!(parse(&max).unwrap().len(), 1);
        // A valid placeholder before a malformed one does not hide the error.
        assert!(parse("{{secret:ok}} {{secret:BAD}}").is_err());
    }

    #[test]
    fn rejects_reserved_and_bad_paths() {
        assert_eq!(
            parse("{{secret:__ostro/token-key}}").unwrap_err(),
            TemplateError::Reserved {
                name: "__ostro/token-key".into()
            }
        );
        assert!(matches!(
            validate_name("__ostro/presign/x"),
            Err(TemplateError::Reserved { .. })
        ));
        for bad in ["a/../b", "a..b", "a//b", "a/", "a/./b", "a/.", "x/.."] {
            assert!(
                matches!(validate_name(bad), Err(TemplateError::BadPath { .. })),
                "{bad}"
            );
        }
        for ok in ["a", "0", "stripe/live-key.v2", "t_1/a.b/c-d", "a.b"] {
            validate_name(ok).unwrap();
        }
    }

    #[test]
    fn errors_never_echo_text() {
        let e = parse("{{secret:Sup3rS3cretValue}}").unwrap_err();
        assert!(!e.to_string().contains("Sup3r"));
        let e = parse("{{secret:sup3rs3cretvalue").unwrap_err();
        assert!(!e.to_string().contains("sup3r"));
    }

    #[test]
    fn expand_substitutes_values() {
        let mut v = BTreeMap::new();
        v.insert("a".to_owned(), SecretValue::new(b"AAAAAAAA".to_vec()));
        v.insert("b/c".to_owned(), SecretValue::new(b"\x00\xffbin".to_vec()));
        let out = expand("x{{secret:a}}-{{secret:b/c}}{{secret:a}}y", &v);
        assert_eq!(&out[..], b"xAAAAAAAA-\x00\xffbinAAAAAAAAy");
        assert_eq!(out.capacity(), out.len());
        // Missing names stay literal; unparsable templates are returned unchanged.
        assert_eq!(&expand("{{secret:zz}}", &v)[..], b"{{secret:zz}}");
        assert_eq!(&expand("{{secret:a", &v)[..], b"{{secret:a");
        assert_eq!(&expand("plain", &v)[..], b"plain");
    }

    fn name_strategy() -> impl Strategy<Value = String> {
        "[a-z0-9][a-z0-9_-]{0,10}(/[a-z0-9][a-z0-9_-]{0,10}){0,2}"
    }

    proptest! {
        #[test]
        fn parse_expand_round_trip(
            parts in proptest::collection::vec(("[ -z|~]{0,8}", name_strategy()), 0..6),
            tail in "[ -z|~]{0,8}",
        ) {
            // Literal text uses printable ASCII without `{`, so it never forms a marker.
            let mut s = String::new();
            let mut spans = Vec::new();
            for (lit, name) in &parts {
                s.push_str(lit);
                let start = s.len();
                s.push_str(&format!("{{{{secret:{name}}}}}"));
                spans.push((name.clone(), start..s.len()));
            }
            s.push_str(&tail);
            let parsed = parse(&s).unwrap();
            prop_assert_eq!(parsed.len(), spans.len());
            for (p, (n, r)) in parsed.iter().zip(&spans) {
                prop_assert_eq!(&p.name, n);
                prop_assert_eq!(&p.range, r);
            }
            // Expanding with every name mapped to its own placeholder text is the identity.
            let values: BTreeMap<String, SecretValue> = spans
                .iter()
                .map(|(n, _)| (n.clone(), SecretValue::new(format!("{{{{secret:{n}}}}}").into_bytes())))
                .collect();
            prop_assert_eq!(&expand(&s, &values)[..], s.as_bytes());
        }

        #[test]
        fn random_input_never_panics(s in ".{0,64}", t in "(\\{\\{secret:|[a-z/._}-]){0,24}") {
            let _ = parse(&s);
            let _ = parse(&t);
            let _ = validate_name(&s);
            let _ = expand(&t, &BTreeMap::new());
            let _ = contains(&s);
        }
    }
}
