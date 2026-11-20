//! Owned by 06-07: per-node, per-target rate limiting (FX-09, D-FX7) over governor; 06-02 only
//! defines [`QuotaSpec`], which [`crate::connector::Target`] carries.

/// A rate quota: `per_second` sustained, with `burst` cells. 06-07 extends it.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaSpec {
    /// Sustained rate, in requests per second (at least 1).
    pub per_second: u32,
    /// Burst size (at least 1).
    pub burst: u32,
}

/// A quota string that is not `<n>/s` or `<n>/m` with `n >= 1`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid rate {0:?}: expected <n>/s or <n>/m with n >= 1")]
pub struct QuotaParseError(pub String);

impl QuotaSpec {
    /// Parses `"100/s"` or `"6000/m"`. A per-minute rate is converted to per second (rounded
    /// up, at least 1); the burst equals the per-second rate for `/s` and the per-minute count
    /// for `/m` (one minute's worth).
    pub fn parse(s: &str) -> Result<Self, QuotaParseError> {
        let err = || QuotaParseError(s.to_owned());
        let (n, unit) = s.trim().split_once('/').ok_or_else(err)?;
        let n: u32 = n.trim().parse().map_err(|_| err())?;
        if n == 0 {
            return Err(err());
        }
        match unit.trim() {
            "s" => Ok(Self {
                per_second: n,
                burst: n,
            }),
            "m" => Ok(Self {
                per_second: n.div_ceil(60),
                burst: n,
            }),
            _ => Err(err()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_per_second_and_per_minute() {
        assert_eq!(
            QuotaSpec::parse("100/s"),
            Ok(QuotaSpec {
                per_second: 100,
                burst: 100
            })
        );
        assert_eq!(
            QuotaSpec::parse("6000/m"),
            Ok(QuotaSpec {
                per_second: 100,
                burst: 6000
            })
        );
        assert_eq!(QuotaSpec::parse("30/m").unwrap().per_second, 1);
        for bad in ["", "100", "0/s", "-1/s", "10/h", "x/s", "10/"] {
            assert!(QuotaSpec::parse(bad).is_err(), "{bad}");
        }
    }
}
