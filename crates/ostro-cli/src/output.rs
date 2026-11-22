//! Output and exit codes.
//!
//! - `--json`: stdout gets exactly the REST response body, pretty-printed (two bodies
//!   are combined into one object only where a verb makes two calls: `run --wait`
//!   prints `{"start", "result"}`, `deploy --activate` prints `{"upload", "activated"}`).
//!   An API error prints the server's error body on stdout too.
//! - Human: short lines and tables on stdout; errors as one `ostro: error: ...` line on
//!   stderr.

use std::process::ExitCode;

use serde_json::Value;

use crate::client::CallError;

/// How a verb ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Success.
    Ok,
    /// Server/API or transport error.
    Error,
    /// Bad usage (clap reports its own usage errors with the same code).
    Usage,
    /// `run --wait` / `result --wait` ended in a status other than `completed`.
    NotCompleted,
    /// Phase 10 (D-P10-7): `ostro replay` found a diverged or skipped run.
    Diverged,
}

/// The single exit-code mapping: 0 ok, 1 API/server error, 2 usage, 3 not completed,
/// 4 replay divergence (Phase 10, D-P10-10).
pub fn exit_code(o: Outcome) -> u8 {
    match o {
        Outcome::Ok => 0,
        Outcome::Error => 1,
        Outcome::Usage => 2,
        Outcome::NotCompleted => 3,
        Outcome::Diverged => 4,
    }
}

impl From<Outcome> for ExitCode {
    fn from(o: Outcome) -> Self {
        ExitCode::from(exit_code(o))
    }
}

/// Printer for one invocation.
#[derive(Debug, Clone, Copy)]
pub struct Out {
    /// `--json` was given.
    pub json: bool,
}

impl Out {
    /// Print a body as pretty JSON on stdout.
    pub fn json(&self, v: &Value) {
        println!(
            "{}",
            serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
        );
    }

    /// Report an error and return its outcome.
    pub fn error(&self, e: &CallError) -> Outcome {
        match e {
            CallError::Usage(m) => {
                eprintln!("ostro: error: {m}");
                Outcome::Usage
            }
            CallError::Transport(m) => {
                eprintln!("ostro: error: {m}");
                Outcome::Error
            }
            CallError::Api { body, .. } => {
                if self.json {
                    self.json(body);
                }
                eprintln!("ostro: error: {}", e.message());
                Outcome::Error
            }
        }
    }
}

/// Compact one-line JSON for human output.
pub fn compact(v: &Value) -> String {
    v.to_string()
}

/// A string field or `-`.
pub fn s(v: &Value) -> String {
    match v {
        Value::Null => "-".to_owned(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Render rows as a left-aligned table with a header (two spaces between columns).
pub fn table(header: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for r in rows {
        for (i, c) in r.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(c.chars().count());
            }
        }
    }
    let line = |cells: Vec<&str>| {
        let n = cells.len();
        let mut out = String::new();
        for (i, c) in cells.into_iter().enumerate() {
            out.push_str(c);
            if i + 1 < n {
                let pad = widths[i].saturating_sub(c.chars().count()) + 2;
                out.push_str(&" ".repeat(pad));
            }
        }
        out
    };
    let mut out = line(header.to_vec());
    for r in rows {
        out.push('\n');
        out.push_str(&line(r.iter().map(String::as_str).collect()));
    }
    out
}

/// A token shown in human output: only a short prefix, never the whole secret.
pub fn token_prefix(token: &str) -> String {
    let prefix: String = token.chars().take(8).collect();
    format!("{prefix}...")
}

/// The `--json` envelope of a **local** verb (D-P10-10: init, build, toolchain, doctor,
/// replay, the build half of deploy), pretty-printed: an object whose first two keys are
/// `"kind": "ostro.<kind>.result"` and `"version": 1`, followed by the fields of `body`
/// in their order. A `kind` / `version` field inside `body` is dropped (the envelope
/// owns them); a non-object `body` is placed under `"result"`.
///
/// serde_json's `preserve_order` feature is not enabled in this workspace (its map
/// sorts keys), so the object is written by hand to keep the key order.
pub fn local_json(kind: &str, body: &Value) -> String {
    let kind = Value::String(format!("ostro.{kind}.result"));
    let mut fields: Vec<(String, &Value)> = Vec::new();
    match body {
        Value::Object(m) => {
            for (k, v) in m {
                if k != "kind" && k != "version" {
                    fields.push((k.clone(), v));
                }
            }
        }
        other => fields.push(("result".to_owned(), other)),
    }
    let mut out = format!("{{\n  \"kind\": {kind},\n  \"version\": 1");
    for (k, v) in fields {
        let pretty = serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string());
        // JSON strings never contain a raw newline, so indenting by line is safe.
        let indented = pretty.replace('\n', "\n  ");
        out.push_str(&format!(",\n  {}: {indented}", Value::String(k)));
    }
    out.push_str("\n}");
    out
}

/// The exit-code table (10-18): every code a verb can exit with and its meaning, in
/// order. docs/cli-json.md copies it (a test in `crates/ostro/tests/json_contract.rs`
/// compares the two).
pub const EXIT_CODES: &[(i32, &str)] = &[
    (0, "ok"),
    (1, "API, server or build error"),
    (2, "usage error"),
    (3, "--wait ended in a status other than completed"),
    (4, "replay divergence (ostro replay)"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes() {
        assert_eq!(exit_code(Outcome::Ok), 0);
        assert_eq!(exit_code(Outcome::Error), 1);
        assert_eq!(exit_code(Outcome::Usage), 2);
        assert_eq!(exit_code(Outcome::NotCompleted), 3);
        assert_eq!(exit_code(Outcome::Diverged), 4);
    }

    #[test]
    fn exit_codes_match_table() {
        let all = [
            Outcome::Ok,
            Outcome::Error,
            Outcome::Usage,
            Outcome::NotCompleted,
            Outcome::Diverged,
        ];
        assert_eq!(all.len(), EXIT_CODES.len());
        for (o, (code, meaning)) in all.into_iter().zip(EXIT_CODES) {
            assert_eq!(i32::from(exit_code(o)), *code, "{o:?}");
            assert!(!meaning.is_empty());
        }
    }

    #[test]
    fn local_json_kind_and_version_first() {
        let body = serde_json::json!({
            "artifacts": [{"path": "a.wasm", "n": 1}],
            "kind": "ignored",
            "version": 99,
            "a": "x\ny",
        });
        let s = local_json("build", &body);
        assert!(
            s.starts_with("{\n  \"kind\": \"ostro.build.result\",\n  \"version\": 1,\n"),
            "{s}"
        );
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["kind"], "ostro.build.result");
        assert_eq!(v["version"], 1);
        assert_eq!(v["artifacts"][0]["path"], "a.wasm");
        assert_eq!(v["a"], "x\ny");
        assert_eq!(v.as_object().unwrap().len(), 4);
        // body order preserved after the envelope keys (BTreeMap order: a, artifacts)
        let ia = s.find("\"a\":").unwrap();
        let ib = s.find("\"artifacts\":").unwrap();
        assert!(s.find("\"version\"").unwrap() < ia && ia < ib, "{s}");

        let empty = local_json("doctor", &serde_json::json!({}));
        assert_eq!(
            empty,
            "{\n  \"kind\": \"ostro.doctor.result\",\n  \"version\": 1\n}"
        );
        let scalar: Value =
            serde_json::from_str(&local_json("init", &serde_json::json!(3))).unwrap();
        assert_eq!(
            scalar,
            serde_json::json!({"kind": "ostro.init.result", "version": 1, "result": 3})
        );
    }

    #[test]
    fn error_outcomes() {
        let out = Out { json: false };
        assert_eq!(out.error(&CallError::Usage("x".into())), Outcome::Usage);
        assert_eq!(out.error(&CallError::Transport("x".into())), Outcome::Error);
        let api = CallError::Api {
            status: 404,
            body: serde_json::json!({"error": {"code": "not_found", "message": "m"}}),
        };
        assert_eq!(out.error(&api), Outcome::Error);
    }

    #[test]
    fn tables_align() {
        let t = table(
            &["A", "BB"],
            &[
                vec!["xyz".into(), "1".into()],
                vec!["q".into(), "22".into()],
            ],
        );
        assert_eq!(t, "A    BB\nxyz  1\nq    22");
    }

    #[test]
    fn token_is_truncated() {
        assert_eq!(token_prefix("ot1_abcdefghijkl"), "ot1_abcd...");
        assert_eq!(token_prefix("ab"), "ab...");
    }
}
