//! Canonical JSON (D-P9-21): the form in which outcomes are compared across languages.
//!
//! - Object keys are sorted (byte order), whatever map order the producer used.
//! - Integral numbers are written as integers and compared exactly (`u64`/`i64`); a float
//!   with no fractional part inside the `i64` range counts as integral (`1.0 == 1`).
//! - Non-integral numbers are written with Rust's shortest round-trip `f64` form, so two
//!   values compare equal exactly when their `f64` values are equal
//!   (`0.1 + 0.2 == 0.30000000000000004`).
//! - NaN and infinities cannot be represented: they become `null`.
//! - Strings are JSON-escaped by serde_json (one canonical escaping).

use serde_json::Value;

/// Canonical JSON text of `v`.
pub fn canonical(v: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, v);
    out
}

/// Canonical JSON text of raw JSON bytes; undecodable bytes are rendered as
/// `<bytes:N:hex-prefix>` so a comparison still fails readably.
pub fn canonical_bytes(data: &[u8]) -> String {
    if data.is_empty() {
        return "null".to_string();
    }
    match serde_json::from_slice::<Value>(data) {
        Ok(v) => canonical(&v),
        Err(_) => {
            let hex: String = data.iter().take(16).map(|b| format!("{b:02x}")).collect();
            format!("<bytes:{}:{hex}>", data.len())
        }
    }
}

/// Canonical form as a `Value` (re-parsed canonical text), for storing in specs.
pub fn normalise(v: &Value) -> Value {
    serde_json::from_str(&canonical(v)).unwrap_or(Value::Null)
}

/// True if `a` and `b` are equal under the canonical-JSON rules.
pub fn equal(a: &Value, b: &Value) -> bool {
    canonical(a) == canonical(b)
}

fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => write_number(out, n),
        Value::String(s) => out.push_str(&Value::String(s.clone()).to_string()),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                write_value(out, &map[k.as_str()]);
            }
            out.push('}');
        }
    }
}

/// `2^63` as f64: integral floats strictly inside `(-2^63, 2^63)` are written as `i64`.
const I64_BOUND: f64 = 9_223_372_036_854_775_808.0;

fn write_number(out: &mut String, n: &serde_json::Number) {
    if let Some(u) = n.as_u64() {
        out.push_str(&u.to_string());
    } else if let Some(i) = n.as_i64() {
        out.push_str(&i.to_string());
    } else if let Some(f) = n.as_f64() {
        write_f64(out, f);
    } else {
        out.push_str("null");
    }
}

#[allow(clippy::cast_possible_truncation)]
fn write_f64(out: &mut String, f: f64) {
    if !f.is_finite() {
        out.push_str("null");
    } else if f.fract() == 0.0 && f.abs() < I64_BOUND {
        // Exact: |f| < 2^63 and integral.
        out.push_str(&(f as i64).to_string());
    } else {
        // Shortest representation that round-trips to the same f64.
        out.push_str(&format!("{f:?}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn integral_floats_are_integers() {
        assert_eq!(canonical(&json!(1.0)), "1");
        assert_eq!(canonical(&json!(-3.0)), "-3");
        assert_eq!(canonical(&json!(u64::MAX)), u64::MAX.to_string());
        assert_eq!(canonical(&json!(0.5)), "0.5");
        assert_eq!(canonical(&json!(1e300)), "1e300");
    }

    #[test]
    fn keys_sorted_nested() {
        assert_eq!(
            canonical(&json!({"z": {"b": 1, "a": [true, null]}, "a": "x"})),
            r#"{"a":"x","z":{"a":[true,null],"b":1}}"#
        );
    }
}
