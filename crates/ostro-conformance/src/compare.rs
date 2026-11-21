//! Comparators (D-P9-21).
//!
//! - [`facts`]: an expected fact list (a spec's `expect.projection`) against an actual one.
//! - [`projections`]: two languages' projections (facts in order, values in order unless
//!   [`Opts::values`] is off, inputs as a multiset).
//! - [`outcomes`]: canonical-JSON outcome equality.
//! - [`journals_identical`]: the per-language strict check "same seed and run id twice →
//!   byte-identical encoded journals".
//!
//! Every mismatch is reported as a [`Diff`] whose `Display` is a readable side-by-side.

use std::fmt;

use ostro_types::journal::{Event, encode_event};
use serde_json::Value;

use crate::canon;
use crate::projection::Projection;

/// A readable mismatch report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff {
    pub what: String,
    pub lines: Vec<String>,
}

impl fmt::Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{} differs:", self.what)?;
        for l in &self.lines {
            writeln!(f, "  {l}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Diff {}

/// Cross-language comparison options.
#[derive(Debug, Clone, Copy)]
pub struct Opts {
    /// Compare recorded values (off for per-language value probes such as C-NATIVE-01).
    pub values: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self { values: true }
    }
}

/// Side-by-side diff of two ordered lists; `Ok` when equal.
pub fn lists(what: &str, expected: &[String], actual: &[String]) -> Result<(), Diff> {
    if expected == actual {
        return Ok(());
    }
    let first = expected
        .iter()
        .zip(actual)
        .position(|(a, b)| a != b)
        .unwrap_or(expected.len().min(actual.len()));
    let mut lines = vec![format!(
        "first difference at index {first} (expected {} entries, actual {})",
        expected.len(),
        actual.len()
    )];
    let n = expected.len().max(actual.len());
    for i in 0..n {
        let e = expected.get(i).map_or("<none>", String::as_str);
        let a = actual.get(i).map_or("<none>", String::as_str);
        let mark = if e == a { "  " } else { "!=" };
        lines.push(format!("{i:>3} {mark} expected {e:<40} actual {a}"));
    }
    Err(Diff {
        what: what.to_string(),
        lines,
    })
}

/// Expected fact list (spec) against the actual projection facts.
pub fn facts(expected: &[String], actual: &[String]) -> Result<(), Diff> {
    lists("projection", expected, actual)
}

/// Canonical-JSON outcome equality.
pub fn outcomes(what: &str, expected: &Value, actual: &Value) -> Result<(), Diff> {
    let (e, a) = (canon::canonical(expected), canon::canonical(actual));
    if e == a {
        Ok(())
    } else {
        Err(Diff {
            what: what.to_string(),
            lines: vec![format!("expected {e}"), format!("actual   {a}")],
        })
    }
}

/// Cross-language projection comparison (reference first).
pub fn projections(reference: &Projection, other: &Projection, opts: Opts) -> Result<(), Diff> {
    lists("projection facts", &reference.facts, &other.facts)?;
    if opts.values {
        lists("projection values", &reference.values, &other.values)?;
    }
    if reference.inputs != other.inputs {
        let mut lines = Vec::new();
        let keys: std::collections::BTreeSet<&String> =
            reference.inputs.keys().chain(other.inputs.keys()).collect();
        for k in keys {
            let (r, o) = (
                reference.inputs.get(k).copied().unwrap_or(0),
                other.inputs.get(k).copied().unwrap_or(0),
            );
            if r != o {
                lines.push(format!("{k}: reference x{r}, other x{o}"));
            }
        }
        return Err(Diff {
            what: "projection inputs (multiset)".to_string(),
            lines,
        });
    }
    Ok(())
}

/// Encoded `(kind, bytes)` of every event, the journal's storage form.
pub fn encoded(journal: &[Event]) -> Vec<(u64, u32, Vec<u8>)> {
    journal
        .iter()
        .map(|e| {
            let (k, b) = encode_event(e);
            (e.seq, k, b)
        })
        .collect()
}

/// Strict per-language check: two journals are byte-identical in their encoded form.
pub fn journals_identical(a: &[Event], b: &[Event]) -> Result<(), Diff> {
    let (ea, eb) = (encoded(a), encoded(b));
    if ea == eb {
        return Ok(());
    }
    let mut lines = vec![format!("lengths {} vs {}", ea.len(), eb.len())];
    for (i, (x, y)) in ea.iter().zip(&eb).enumerate() {
        if x != y {
            lines.push(format!(
                "first differing event at index {i}: {} vs {}",
                a[i].body.summary(),
                b[i].body.summary()
            ));
            break;
        }
    }
    Err(Diff {
        what: "journal bytes (same seed and run id)".to_string(),
        lines,
    })
}
