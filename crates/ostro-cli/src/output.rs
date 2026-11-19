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
}

/// The single exit-code mapping: 0 ok, 1 API/server error, 2 usage, 3 not completed.
pub fn exit_code(o: Outcome) -> u8 {
    match o {
        Outcome::Ok => 0,
        Outcome::Error => 1,
        Outcome::Usage => 2,
        Outcome::NotCompleted => 3,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes() {
        assert_eq!(exit_code(Outcome::Ok), 0);
        assert_eq!(exit_code(Outcome::Error), 1);
        assert_eq!(exit_code(Outcome::Usage), 2);
        assert_eq!(exit_code(Outcome::NotCompleted), 3);
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
