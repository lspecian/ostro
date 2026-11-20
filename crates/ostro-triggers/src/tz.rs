//! IANA time-zone loading for the cron engine (P8-D9, plan 08-05).
//!
//! croner speaks jiff 0.2 while the rest of Ostro speaks jiff 1.0. The two majors each carry
//! their own `TimeZone` type, and two independently loaded databases could disagree on a
//! rule (RESEARCH Pitfall 9). So both zones are parsed from **the same TZif bytes**:
//!
//! 1. `/usr/share/zoneinfo/<name>` when it exists and is a regular file (the name is checked
//!    first: no absolute paths, no `.`/`..` components, IANA characters only);
//! 2. otherwise the bundled tzdb bytes from the `jiff-tzdb` crate, the very crate (and
//!    version) behind jiff 1.0's `tzdb-bundle-always`, so `FROM scratch` images work.
//!
//! jiff 1.0 has no public accessor for a zone's TZif bytes, so the bundle is read through
//! `jiff_tzdb::get` directly rather than through `jiff::tz::db()`.
//!
//! `""` and `"UTC"` are UTC without a lookup. Nothing here reads a clock; file reads happen only
//! when a schedule is parsed (`CronSpec::parse`), never per evaluation.

use std::borrow::Cow;
use std::path::Path;

/// The system zoneinfo directory consulted before the bundled tzdb.
pub const SYSTEM_ZONEINFO: &str = "/usr/share/zoneinfo";

/// Longest zone name accepted (the meta record's `SCHEDULE_TIMEZONE_MAX`).
pub const TZ_NAME_MAX: usize = 64;

/// Where a zone's TZif bytes came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TzSource {
    /// `/usr/share/zoneinfo/<name>`.
    System,
    /// The `jiff-tzdb` bundle compiled into the binary.
    Bundled,
}

/// A time-zone lookup failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TzError {
    /// The name is not a well-formed IANA zone name (empty segment, `..`, bad characters,
    /// too long).
    #[error("malformed time zone name {0:?}")]
    BadName(String),
    /// Neither the system zoneinfo nor the bundled tzdb has this zone.
    #[error("unknown time zone {0:?}")]
    Unknown(String),
    /// The TZif bytes did not parse.
    #[error("invalid TZif data for {name:?}: {reason}")]
    Invalid {
        /// Zone name.
        name: String,
        /// Parser message.
        reason: String,
    },
}

/// Whether `name` is the UTC shorthand (`""` or `"UTC"`), which needs no lookup.
pub fn is_utc(name: &str) -> bool {
    name.is_empty() || name == "UTC"
}

/// Checks that `name` is a plausible IANA zone name that is safe to join onto a directory:
/// relative, `/`-separated segments of `[A-Za-z0-9_+-]` (plus `.` inside a segment), no
/// `.` or `..` segment.
pub fn validate_name(name: &str) -> Result<(), TzError> {
    let bad = || TzError::BadName(name.to_owned());
    if name.is_empty() || name.len() > TZ_NAME_MAX {
        return Err(bad());
    }
    for seg in name.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." || seg.starts_with('.') {
            return Err(bad());
        }
        if !seg
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'+' | b'-' | b'.'))
        {
            return Err(bad());
        }
    }
    Ok(())
}

/// The TZif bytes of zone `name`: the system zoneinfo file when present (and a regular file),
/// else the bundled tzdb. `name` must not be the UTC shorthand `""` (use [`is_utc`]).
pub fn tz_bytes(name: &str) -> Result<(Cow<'static, [u8]>, TzSource), TzError> {
    validate_name(name)?;
    let path = Path::new(SYSTEM_ZONEINFO).join(name);
    if let Ok(meta) = std::fs::metadata(&path)
        && meta.is_file()
        && let Ok(bytes) = std::fs::read(&path)
        && bytes.starts_with(b"TZif")
    {
        return Ok((Cow::Owned(bytes), TzSource::System));
    }
    bundled_bytes(name)
        .map(|b| (Cow::Borrowed(b), TzSource::Bundled))
        .ok_or_else(|| TzError::Unknown(name.to_owned()))
}

/// The bundled tzdb bytes of `name` (case-insensitive, as jiff's own lookup), ignoring the
/// system zoneinfo.
pub fn bundled_bytes(name: &str) -> Option<&'static [u8]> {
    jiff_tzdb::get(name).map(|(_, bytes)| bytes)
}

/// Both majors' zones for one name, parsed from the same bytes.
#[derive(Clone, Debug)]
pub(crate) struct DualZone {
    pub(crate) v1: jiff::tz::TimeZone,
    pub(crate) v02: jiff02::tz::TimeZone,
    pub(crate) source: Option<TzSource>,
}

impl DualZone {
    /// Loads `name` (`""`/`"UTC"` = UTC) from the system zoneinfo or the bundle.
    pub(crate) fn load(name: &str) -> Result<Self, TzError> {
        if is_utc(name) {
            return Ok(Self {
                v1: jiff::tz::TimeZone::UTC,
                v02: jiff02::tz::TimeZone::UTC,
                source: None,
            });
        }
        let (bytes, source) = tz_bytes(name)?;
        Self::from_tzif(name, &bytes).map(|mut z| {
            z.source = Some(source);
            z
        })
    }

    /// Parses `bytes` once per major.
    pub(crate) fn from_tzif(name: &str, bytes: &[u8]) -> Result<Self, TzError> {
        let invalid = |reason: String| TzError::Invalid {
            name: name.to_owned(),
            reason,
        };
        let v1 = jiff::tz::TimeZone::tzif(name, bytes).map_err(|e| invalid(e.to_string()))?;
        let v02 = jiff02::tz::TimeZone::tzif(name, bytes).map_err(|e| invalid(e.to_string()))?;
        Ok(Self {
            v1,
            v02,
            source: None,
        })
    }
}

/// Loads zone `name` as a jiff 1.0 `TimeZone` from the same bytes the cron engine uses
/// (`""`/`"UTC"` = UTC). Returns where the bytes came from (`None` for UTC).
pub fn load(name: &str) -> Result<(jiff::tz::TimeZone, Option<TzSource>), TzError> {
    DualZone::load(name).map(|z| (z.v1, z.source))
}
