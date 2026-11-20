//! Dynamic value mapping between WIT `sql-value` ([`SqlValue`]) and sqlx (06-RESEARCH value
//! table).
//!
//! **Parameters** are converted to [`Param`] before anything runs, so a malformed parameter
//! never opens a transaction:
//!
//! | `SqlValue` | PostgreSQL bind | MySQL bind |
//! |---|---|---|
//! | `Null` | untyped NULL (oid 0: the server infers the type) | NULL |
//! | `Boolean` | `bool` | `bool` |
//! | `Int8` | `int8` | `BIGINT` |
//! | `Float8` | `float8` | `DOUBLE` |
//! | `Numeric(s)` | `numeric` (`BigDecimal` parsed from `s`) | `DECIMAL` |
//! | `Text` | `text` | `VARCHAR` |
//! | `Bytea` | `bytea` | `BLOB` |
//! | `Json(s)` | `jsonb` (raw JSON text, validated) | `JSON` |
//! | `Timestamptz` | `timestamptz` (`time::OffsetDateTime`) | `TIMESTAMP` (UTC) |
//!
//! **Columns** decode by `type_info().name()` ([`pg_class`], [`mysql_class`]):
//! - bool → `Boolean`; every integer → `Int8`; MySQL `BIGINT UNSIGNED` above `i64::MAX` →
//!   `Numeric`;
//! - float4/8, FLOAT/DOUBLE → `Float8`; numeric/DECIMAL → `Numeric` holding the server's text
//!   exactly: plain notation at the server's scale (`numeric(30,4)` 1.5 → `"1.5000"`, unconstrained
//!   `numeric` keeps the stored scale; sqlx alone drops the PostgreSQL `dscale`);
//! - text/varchar/char/name/citext/enum, MySQL CHAR/VARCHAR/*TEXT/ENUM → `Text`; uuid → `Text`;
//! - bytea, MySQL BINARY/VARBINARY/*BLOB → `Bytea`; json/jsonb/JSON → `Json` (server text);
//! - timestamptz and MySQL TIMESTAMP → `Timestamptz`. PostgreSQL `timestamp` (without time zone)
//!   and MySQL `DATETIME` carry no zone and are **read as UTC**. (sqlx sets the MySQL session
//!   `time_zone` to `+00:00`, so TIMESTAMP values arrive in UTC.) Instants before 1970 cannot
//!   be represented (`seconds` is unsigned) and are `sql_unsupported_type`;
//! - anything else (date, time, interval, arrays, geometry, ...) →
//!   `NonRetryable{sql_unsupported_type, details: {column, type}}`. Cast in SQL instead
//!   (`col::text`, `CAST(col AS CHAR)`).

use std::str::FromStr;

use ostro_types::journal::{Failure, SqlValue};
use sqlx::mysql::{MySql, MySqlArguments, MySqlRow};
use sqlx::postgres::types::Oid;
use sqlx::postgres::{
    PgArgumentBuffer, PgArguments, PgRow, PgTypeInfo, PgTypeKind, PgValueFormat, Postgres,
};
use sqlx::query::Query;
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime};
use sqlx::types::{BigDecimal, JsonRawValue, Uuid};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use crate::kinds::{SQL_ERROR, SQL_UNSUPPORTED_TYPE};
use crate::outcome::non_retryable;

/// A validated, owned statement parameter.
#[derive(Clone, Debug)]
pub enum Param {
    /// NULL.
    Null,
    /// Boolean.
    Bool(bool),
    /// 64-bit integer.
    I64(i64),
    /// Double.
    F64(f64),
    /// Arbitrary-precision decimal.
    Numeric(BigDecimal),
    /// Text.
    Text(String),
    /// Bytes.
    Bytes(Vec<u8>),
    /// JSON text (validated).
    Json(Box<JsonRawValue>),
    /// Instant (UTC).
    Tstz(OffsetDateTime),
}

fn bad_param(index: usize, what: &str) -> Failure {
    non_retryable(
        SQL_ERROR,
        format!("parameter {}: {what}", index + 1),
        Some(serde_json::json!({ "param": index + 1 })),
    )
}

/// Converts the guest's parameters (1-based in messages).
pub fn to_params(values: &[SqlValue]) -> Result<Vec<Param>, Failure> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            Ok(match v {
                SqlValue::Null => Param::Null,
                SqlValue::Boolean(b) => Param::Bool(*b),
                SqlValue::Int8(n) => Param::I64(*n),
                SqlValue::Float8(f) => Param::F64(*f),
                SqlValue::Numeric(s) => Param::Numeric(
                    BigDecimal::from_str(s.trim())
                        .map_err(|_| bad_param(i, "invalid numeric value"))?,
                ),
                SqlValue::Text(s) => Param::Text(s.clone()),
                SqlValue::Bytea(b) => Param::Bytes(b.clone()),
                SqlValue::Json(s) => Param::Json(
                    JsonRawValue::from_string(s.clone())
                        .map_err(|_| bad_param(i, "invalid JSON value"))?,
                ),
                SqlValue::Timestamptz {
                    seconds,
                    nanoseconds,
                } => Param::Tstz(
                    tstz_from_parts(*seconds, *nanoseconds)
                        .ok_or_else(|| bad_param(i, "timestamp out of range"))?,
                ),
            })
        })
        .collect()
}

/// `{seconds, nanoseconds}` since the Unix epoch → `OffsetDateTime` (UTC).
pub fn tstz_from_parts(seconds: u64, nanoseconds: u32) -> Option<OffsetDateTime> {
    if nanoseconds >= 1_000_000_000 {
        return None;
    }
    let n = i128::from(seconds) * 1_000_000_000 + i128::from(nanoseconds);
    OffsetDateTime::from_unix_timestamp_nanos(n).ok()
}

/// `OffsetDateTime` → `Timestamptz{seconds, nanoseconds}`; `None` before 1970.
pub fn tstz_to_value(t: OffsetDateTime) -> Option<SqlValue> {
    let n = t.unix_timestamp_nanos();
    if n < 0 {
        return None;
    }
    Some(SqlValue::Timestamptz {
        seconds: u64::try_from(n / 1_000_000_000).ok()?,
        nanoseconds: u32::try_from(n % 1_000_000_000).ok()?,
    })
}

/// A MySQL unsigned integer: `Int8` when it fits, else `Numeric`.
pub fn unsigned_to_value(n: u64) -> SqlValue {
    match i64::try_from(n) {
        Ok(i) => SqlValue::Int8(i),
        Err(_) => SqlValue::Numeric(n.to_string()),
    }
}

/// A PostgreSQL NULL whose type the server infers (parameter oid 0), so NULL fits any
/// column type.
struct UntypedNull;

impl sqlx::Type<Postgres> for UntypedNull {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(0))
    }
}

impl sqlx::Encode<'_, Postgres> for UntypedNull {
    fn encode_by_ref(
        &self,
        _buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        Ok(sqlx::encode::IsNull::Yes)
    }
}

/// Binds `p` to a PostgreSQL query.
pub fn bind_pg<'q>(
    q: Query<'q, Postgres, PgArguments>,
    p: &Param,
) -> Query<'q, Postgres, PgArguments> {
    match p {
        Param::Null => q.bind(UntypedNull),
        Param::Bool(b) => q.bind(*b),
        Param::I64(n) => q.bind(*n),
        Param::F64(f) => q.bind(*f),
        Param::Numeric(d) => q.bind(d.clone()),
        Param::Text(s) => q.bind(s.clone()),
        Param::Bytes(b) => q.bind(b.clone()),
        Param::Json(j) => q.bind(j.clone()),
        Param::Tstz(t) => q.bind(*t),
    }
}

/// Binds `p` to a MySQL query.
pub fn bind_mysql<'q>(
    q: Query<'q, MySql, MySqlArguments>,
    p: &Param,
) -> Query<'q, MySql, MySqlArguments> {
    match p {
        Param::Null => q.bind(Option::<String>::None),
        Param::Bool(b) => q.bind(*b),
        Param::I64(n) => q.bind(*n),
        Param::F64(f) => q.bind(*f),
        Param::Numeric(d) => q.bind(d.clone()),
        Param::Text(s) => q.bind(s.clone()),
        Param::Bytes(b) => q.bind(b.clone()),
        Param::Json(j) => q.bind(j.clone()),
        Param::Tstz(t) => q.bind(*t),
    }
}

/// How a column decodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Always NULL (PostgreSQL `void`, MySQL `NULL`).
    Null,
    /// Boolean.
    Bool,
    /// 16-bit integer (PostgreSQL int2).
    I16,
    /// 32-bit integer (PostgreSQL int4).
    I32,
    /// Signed integer read as i64.
    I64,
    /// Unsigned integer read as u64 (MySQL).
    U64,
    /// float4 / FLOAT.
    F32,
    /// float8 / DOUBLE.
    F64,
    /// numeric / DECIMAL.
    Numeric,
    /// Text.
    Text,
    /// Bytes.
    Bytes,
    /// JSON.
    Json,
    /// Instant with a zone.
    Tstz,
    /// Date-time without a zone, read as UTC.
    TsUtc,
    /// UUID, returned as text.
    Uuid,
    /// No mapping (`sql_unsupported_type`).
    Unsupported,
}

/// The decode class of a PostgreSQL column type name (`is_enum` for enum kinds).
pub fn pg_class(name: &str, is_enum: bool) -> Class {
    if is_enum {
        return Class::Text;
    }
    match name {
        "VOID" => Class::Null,
        "BOOL" => Class::Bool,
        "INT2" => Class::I16,
        "INT4" => Class::I32,
        "INT8" => Class::I64,
        "FLOAT4" => Class::F32,
        "FLOAT8" => Class::F64,
        "NUMERIC" => Class::Numeric,
        "TEXT" | "VARCHAR" | "CHAR" | "NAME" | "citext" | "CITEXT" => Class::Text,
        "BYTEA" => Class::Bytes,
        "JSON" | "JSONB" => Class::Json,
        "TIMESTAMPTZ" => Class::Tstz,
        "TIMESTAMP" => Class::TsUtc,
        "UUID" => Class::Uuid,
        _ => Class::Unsupported,
    }
}

/// The decode class of a MySQL column type name.
pub fn mysql_class(name: &str) -> Class {
    match name {
        "NULL" => Class::Null,
        "BOOLEAN" => Class::Bool,
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" | "YEAR" => Class::I64,
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" => Class::U64,
        "FLOAT" => Class::F32,
        "DOUBLE" => Class::F64,
        "DECIMAL" => Class::Numeric,
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" => {
            Class::Text
        }
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => Class::Bytes,
        "JSON" => Class::Json,
        "TIMESTAMP" => Class::Tstz,
        "DATETIME" => Class::TsUtc,
        _ => Class::Unsupported,
    }
}

/// Approximate encoded size of a value (for the byte cap).
pub fn value_size(v: &SqlValue) -> u64 {
    let n = match v {
        SqlValue::Null | SqlValue::Boolean(_) => 1,
        SqlValue::Int8(_) | SqlValue::Float8(_) => 8,
        SqlValue::Timestamptz { .. } => 12,
        SqlValue::Numeric(s) | SqlValue::Text(s) | SqlValue::Json(s) => s.len() + 4,
        SqlValue::Bytea(b) => b.len() + 4,
    };
    n as u64
}

/// The result does not fit the caps.
#[derive(Debug, PartialEq, Eq)]
pub struct TooLarge;

/// Collects decoded rows up to the row and byte caps.
#[derive(Debug)]
pub struct Collector {
    max_rows: u64,
    max_bytes: u64,
    bytes: u64,
    /// Decoded rows so far.
    pub rows: Vec<Vec<SqlValue>>,
}

impl Collector {
    /// An empty collector with the given caps.
    pub fn new(max_rows: u64, max_bytes: u64, columns: &[String]) -> Self {
        Self {
            max_rows,
            max_bytes,
            bytes: columns.iter().map(|c| c.len() as u64 + 4).sum(),
            rows: Vec::new(),
        }
    }

    /// Adds a row; `Err(TooLarge)` once a cap is exceeded.
    pub fn push(&mut self, row: Vec<SqlValue>) -> Result<(), TooLarge> {
        self.bytes += row.iter().map(value_size).sum::<u64>();
        if self.rows.len() as u64 >= self.max_rows || self.bytes > self.max_bytes {
            return Err(TooLarge);
        }
        self.rows.push(row);
        Ok(())
    }

    /// Whether the column names alone exceed the byte cap.
    pub fn over(&self) -> bool {
        self.bytes > self.max_bytes
    }
}

fn unsupported(column: &str, ty: &str) -> Failure {
    non_retryable(
        SQL_UNSUPPORTED_TYPE,
        format!(
            "column {column:?} has type {ty}, which has no sql-value mapping; cast it in SQL (e.g. ::text)"
        ),
        Some(serde_json::json!({ "column": column, "type": ty })),
    )
}

fn decode_error(column: &str, e: sqlx::Error) -> Failure {
    non_retryable(
        SQL_ERROR,
        format!("column {column:?} could not be decoded: {e}"),
        Some(serde_json::json!({ "column": column })),
    )
}

fn tstz(column: &str, ty: &str, t: OffsetDateTime) -> Result<SqlValue, Failure> {
    tstz_to_value(t).ok_or_else(|| unsupported(column, &format!("{ty} before 1970")))
}

/// Column names of a row.
pub fn column_names<R: Row>(row: &R) -> Vec<String> {
    row.columns().iter().map(|c| c.name().to_owned()).collect()
}

/// The display scale (`dscale`) of a binary PostgreSQL `numeric`: the header is
/// `ndigits i16, weight i16, sign u16, dscale u16` (big-endian). `None` when too short.
fn pg_numeric_dscale(bytes: &[u8]) -> Option<u16> {
    let b = bytes.get(6..8)?;
    Some(u16::from_be_bytes([b[0], b[1]]))
}

/// The server's text of a decimal: plain notation (never `1E-9`), at the server's scale.
///
/// sqlx decodes a binary PostgreSQL `numeric` from its base-10000 digit groups and drops the
/// `dscale` header field, so `12345678901234567890.125` would come back as `...1250` and
/// `10000` as `1E+4`. Re-applying `dscale` is exact: the digits past `dscale` are the zero
/// padding of the last group. The result equals the server's `numeric::text`.
fn numeric_text(d: BigDecimal, dscale: Option<u16>) -> String {
    match dscale {
        Some(s) => d.with_scale(i64::from(s)).to_plain_string(),
        None => d.to_plain_string(),
    }
}

/// Decodes one PostgreSQL row.
pub fn decode_pg_row(row: &PgRow) -> Result<Vec<SqlValue>, Failure> {
    let mut out = Vec::with_capacity(row.len());
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name();
        let ti = col.type_info();
        let ty = ti.name();
        let raw = row.try_get_raw(i).map_err(|e| decode_error(name, e))?;
        if raw.is_null() {
            out.push(SqlValue::Null);
            continue;
        }
        let is_enum = matches!(ti.kind(), PgTypeKind::Enum(_));
        let e = |e| decode_error(name, e);
        let v = match pg_class(ty, is_enum) {
            Class::Null => SqlValue::Null,
            Class::Bool => SqlValue::Boolean(row.try_get(i).map_err(e)?),
            Class::I16 => SqlValue::Int8(i64::from(row.try_get::<i16, _>(i).map_err(e)?)),
            Class::I32 => SqlValue::Int8(i64::from(row.try_get::<i32, _>(i).map_err(e)?)),
            Class::I64 => SqlValue::Int8(row.try_get(i).map_err(e)?),
            Class::F32 => SqlValue::Float8(f64::from(row.try_get::<f32, _>(i).map_err(e)?)),
            Class::F64 => SqlValue::Float8(row.try_get(i).map_err(e)?),
            Class::Numeric => {
                let d = row.try_get::<BigDecimal, _>(i).map_err(e)?;
                let dscale = match raw.format() {
                    PgValueFormat::Binary => raw.as_bytes().ok().and_then(pg_numeric_dscale),
                    PgValueFormat::Text => None,
                };
                SqlValue::Numeric(numeric_text(d, dscale))
            }
            Class::Text => SqlValue::Text(row.try_get_unchecked::<String, _>(i).map_err(e)?),
            Class::Bytes => SqlValue::Bytea(row.try_get(i).map_err(e)?),
            Class::Json => SqlValue::Json(
                row.try_get::<Box<JsonRawValue>, _>(i)
                    .map_err(e)?
                    .get()
                    .to_owned(),
            ),
            Class::Tstz => tstz(name, ty, row.try_get(i).map_err(e)?)?,
            Class::TsUtc => tstz(
                name,
                ty,
                row.try_get::<PrimitiveDateTime, _>(i)
                    .map_err(e)?
                    .assume_utc(),
            )?,
            Class::Uuid => SqlValue::Text(row.try_get::<Uuid, _>(i).map_err(e)?.to_string()),
            Class::U64 | Class::Unsupported => return Err(unsupported(name, ty)),
        };
        out.push(v);
    }
    Ok(out)
}

/// Decodes one MySQL row.
pub fn decode_mysql_row(row: &MySqlRow) -> Result<Vec<SqlValue>, Failure> {
    let mut out = Vec::with_capacity(row.len());
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name();
        let ty = col.type_info().name();
        let raw = row.try_get_raw(i).map_err(|e| decode_error(name, e))?;
        if raw.is_null() {
            out.push(SqlValue::Null);
            continue;
        }
        let e = |e| decode_error(name, e);
        let v = match mysql_class(ty) {
            Class::Null => SqlValue::Null,
            Class::Bool => SqlValue::Boolean(row.try_get(i).map_err(e)?),
            Class::I16 | Class::I32 | Class::I64 => {
                SqlValue::Int8(row.try_get_unchecked::<i64, _>(i).map_err(e)?)
            }
            Class::U64 => unsigned_to_value(row.try_get_unchecked::<u64, _>(i).map_err(e)?),
            Class::F32 => SqlValue::Float8(f64::from(row.try_get::<f32, _>(i).map_err(e)?)),
            Class::F64 => SqlValue::Float8(row.try_get(i).map_err(e)?),
            // MySQL sends DECIMAL as text, so the parsed value already carries the server's
            // scale.
            Class::Numeric => SqlValue::Numeric(numeric_text(
                row.try_get::<BigDecimal, _>(i).map_err(e)?,
                None,
            )),
            Class::Text => SqlValue::Text(row.try_get_unchecked::<String, _>(i).map_err(e)?),
            Class::Bytes => SqlValue::Bytea(row.try_get_unchecked::<Vec<u8>, _>(i).map_err(e)?),
            Class::Json => SqlValue::Json(
                row.try_get::<Box<JsonRawValue>, _>(i)
                    .map_err(e)?
                    .get()
                    .to_owned(),
            ),
            // sqlx reads TIMESTAMP and DATETIME as UTC (session time_zone +00:00).
            Class::Tstz | Class::TsUtc => {
                tstz(name, ty, row.try_get::<OffsetDateTime, _>(i).map_err(e)?)?
            }
            Class::Uuid | Class::Unsupported => return Err(unsupported(name, ty)),
        };
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonretryable_kind(f: &Failure) -> &str {
        match f {
            Failure::NonRetryable(e) => &e.kind,
            other => panic!("non-retryable expected, got {other:?}"),
        }
    }

    #[test]
    fn params_convert_every_arm() {
        let p = to_params(&[
            SqlValue::Null,
            SqlValue::Boolean(true),
            SqlValue::Int8(-7),
            SqlValue::Float8(1.5),
            SqlValue::Numeric("12345678901234567890.0001".into()),
            SqlValue::Text("hé".into()),
            SqlValue::Bytea(vec![0, 255]),
            SqlValue::Json(r#"{"b":1,"a":[true]}"#.into()),
            SqlValue::Timestamptz {
                seconds: 1_700_000_000,
                nanoseconds: 123_456_789,
            },
        ])
        .unwrap();
        assert!(matches!(p[0], Param::Null));
        assert!(matches!(p[1], Param::Bool(true)));
        assert!(matches!(p[2], Param::I64(-7)));
        assert!(matches!(p[3], Param::F64(f) if f == 1.5));
        let Param::Numeric(d) = &p[4] else { panic!() };
        assert_eq!(d.to_string(), "12345678901234567890.0001");
        assert!(matches!(&p[5], Param::Text(s) if s == "hé"));
        assert!(matches!(&p[6], Param::Bytes(b) if b == &[0, 255]));
        // JSON text is kept verbatim (key order included).
        assert!(matches!(&p[7], Param::Json(j) if j.get() == r#"{"b":1,"a":[true]}"#));
        let Param::Tstz(t) = p[8] else { panic!() };
        assert_eq!(t.unix_timestamp(), 1_700_000_000);
        assert_eq!(t.nanosecond(), 123_456_789);
    }

    #[test]
    fn bad_params_fail_before_running() {
        for (v, what) in [
            (SqlValue::Numeric("12a".into()), "invalid numeric value"),
            (SqlValue::Json("{".into()), "invalid JSON value"),
            (
                SqlValue::Timestamptz {
                    seconds: 0,
                    nanoseconds: 1_000_000_000,
                },
                "timestamp out of range",
            ),
            (
                SqlValue::Timestamptz {
                    seconds: u64::MAX,
                    nanoseconds: 0,
                },
                "timestamp out of range",
            ),
        ] {
            let f = to_params(&[SqlValue::Int8(1), v]).unwrap_err();
            assert_eq!(nonretryable_kind(&f), SQL_ERROR);
            let Failure::NonRetryable(e) = f else {
                panic!()
            };
            assert_eq!(e.message, format!("parameter 2: {what}"));
        }
    }

    #[test]
    fn timestamps_round_trip() {
        for (s, n) in [(0, 0), (1, 999_999_999), (1_700_000_000, 1)] {
            let t = tstz_from_parts(s, n).unwrap();
            assert_eq!(
                tstz_to_value(t),
                Some(SqlValue::Timestamptz {
                    seconds: s,
                    nanoseconds: n
                })
            );
        }
        let before_epoch = OffsetDateTime::from_unix_timestamp(-1).unwrap();
        assert_eq!(tstz_to_value(before_epoch), None);
        assert_eq!(
            tstz("c", "TIMESTAMP", before_epoch).map_err(|f| nonretryable_kind(&f).to_owned()),
            Err(SQL_UNSUPPORTED_TYPE.to_owned())
        );
    }

    #[test]
    fn numeric_text_is_the_server_text() {
        let d = |s: &str| BigDecimal::from_str(s).unwrap();
        // sqlx's binary decode of `...125` (dscale 3) yields scale 4; dscale restores it.
        assert_eq!(
            numeric_text(d("12345678901234567890.1250"), Some(3)),
            "12345678901234567890.125"
        );
        assert_eq!(
            numeric_text(d("12345678901234567890.125"), Some(4)),
            "12345678901234567890.1250"
        );
        // 10000 decodes with a negative scale (Display would print 1E+4).
        assert_eq!(
            numeric_text(BigDecimal::new(1.into(), -4), Some(0)),
            "10000"
        );
        assert_eq!(numeric_text(d("100"), Some(2)), "100.00");
        assert_eq!(numeric_text(d("-0.0000000010"), Some(9)), "-0.000000001");
        // Text-format and MySQL values keep their parsed scale, in plain notation.
        assert_eq!(numeric_text(d("0.0000000001"), None), "0.0000000001");
        assert_eq!(numeric_text(d("1.10"), None), "1.10");
        assert_eq!(pg_numeric_dscale(&[0, 1, 0, 0, 0, 0, 0, 3, 0, 1]), Some(3));
        assert_eq!(pg_numeric_dscale(&[0, 0, 0]), None);
    }

    #[test]
    fn unsigned_overflow_becomes_numeric() {
        assert_eq!(unsigned_to_value(5), SqlValue::Int8(5));
        assert_eq!(unsigned_to_value(i64::MAX as u64), SqlValue::Int8(i64::MAX));
        assert_eq!(
            unsigned_to_value(u64::MAX),
            SqlValue::Numeric("18446744073709551615".into())
        );
    }

    #[test]
    fn pg_type_classes() {
        for (name, want) in [
            ("VOID", Class::Null),
            ("BOOL", Class::Bool),
            ("INT2", Class::I16),
            ("INT4", Class::I32),
            ("INT8", Class::I64),
            ("FLOAT4", Class::F32),
            ("FLOAT8", Class::F64),
            ("NUMERIC", Class::Numeric),
            ("TEXT", Class::Text),
            ("VARCHAR", Class::Text),
            ("CHAR", Class::Text),
            ("NAME", Class::Text),
            ("citext", Class::Text),
            ("BYTEA", Class::Bytes),
            ("JSON", Class::Json),
            ("JSONB", Class::Json),
            ("TIMESTAMPTZ", Class::Tstz),
            ("TIMESTAMP", Class::TsUtc),
            ("UUID", Class::Uuid),
            ("DATE", Class::Unsupported),
            ("TIME", Class::Unsupported),
            ("INTERVAL", Class::Unsupported),
            ("INT4[]", Class::Unsupported),
            ("POINT", Class::Unsupported),
            ("my_enum", Class::Unsupported),
        ] {
            assert_eq!(pg_class(name, false), want, "{name}");
        }
        assert_eq!(pg_class("my_enum", true), Class::Text);
    }

    #[test]
    fn mysql_type_classes() {
        for (name, want) in [
            ("NULL", Class::Null),
            ("BOOLEAN", Class::Bool),
            ("TINYINT", Class::I64),
            ("SMALLINT", Class::I64),
            ("MEDIUMINT", Class::I64),
            ("INT", Class::I64),
            ("BIGINT", Class::I64),
            ("YEAR", Class::I64),
            ("TINYINT UNSIGNED", Class::U64),
            ("INT UNSIGNED", Class::U64),
            ("BIGINT UNSIGNED", Class::U64),
            ("FLOAT", Class::F32),
            ("DOUBLE", Class::F64),
            ("DECIMAL", Class::Numeric),
            ("CHAR", Class::Text),
            ("VARCHAR", Class::Text),
            ("TEXT", Class::Text),
            ("LONGTEXT", Class::Text),
            ("ENUM", Class::Text),
            ("BINARY", Class::Bytes),
            ("VARBINARY", Class::Bytes),
            ("BLOB", Class::Bytes),
            ("LONGBLOB", Class::Bytes),
            ("JSON", Class::Json),
            ("TIMESTAMP", Class::Tstz),
            ("DATETIME", Class::TsUtc),
            ("DATE", Class::Unsupported),
            ("TIME", Class::Unsupported),
            ("BIT", Class::Unsupported),
            ("SET", Class::Unsupported),
            ("GEOMETRY", Class::Unsupported),
        ] {
            assert_eq!(mysql_class(name), want, "{name}");
        }
    }

    #[test]
    fn unsupported_type_failure_names_column_and_type() {
        let f = unsupported("d", "DATE");
        let Failure::NonRetryable(e) = f else {
            panic!()
        };
        assert_eq!(e.kind, SQL_UNSUPPORTED_TYPE);
        let d: serde_json::Value = serde_json::from_slice(&e.details.unwrap().data).unwrap();
        assert_eq!(d, serde_json::json!({"column": "d", "type": "DATE"}));
    }

    #[test]
    fn collector_caps() {
        let cols = vec!["a".to_owned()];
        let mut c = Collector::new(2, 1_000, &cols);
        c.push(vec![SqlValue::Int8(1)]).unwrap();
        c.push(vec![SqlValue::Int8(2)]).unwrap();
        assert_eq!(c.push(vec![SqlValue::Int8(3)]), Err(TooLarge));

        let mut c = Collector::new(100, 20, &cols);
        assert!(!c.over());
        c.push(vec![SqlValue::Text("0123456789".into())]).unwrap();
        assert_eq!(c.push(vec![SqlValue::Bytea(vec![0; 16])]), Err(TooLarge));
        assert!(Collector::new(1, 2, &cols).over());
        assert_eq!(value_size(&SqlValue::Null), 1);
        assert_eq!(value_size(&SqlValue::Json("{}".into())), 6);
    }
}
