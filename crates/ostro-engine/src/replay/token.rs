//! Await tokens (stub; implemented in the GREEN step).

/// Decoded token fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenParts {
    pub partition: u16,
    pub run_id: String,
    pub op: u64,
}

#[allow(unused_variables)]
pub fn mint(partition: u16, run_id: &str, op: u64, key: &[u8; 32]) -> String {
    todo!()
}

#[allow(unused_variables)]
pub fn parse(token: &str) -> Option<TokenParts> {
    todo!()
}

#[allow(unused_variables)]
pub fn verify(token: &str, key: &[u8; 32]) -> bool {
    todo!()
}

#[allow(unused_variables)]
fn base32_encode(bytes: &[u8]) -> String {
    todo!()
}

#[allow(unused_variables)]
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [9; 32];

    #[test]
    fn base32_rfc4648_vectors() {
        let v = [
            ("", ""),
            ("f", "my"),
            ("fo", "mzxq"),
            ("foo", "mzxw6"),
            ("foob", "mzxw6yq"),
            ("fooba", "mzxw6ytb"),
            ("foobar", "mzxw6ytboi"),
        ];
        for (plain, enc) in v {
            assert_eq!(base32_encode(plain.as_bytes()), enc);
            assert_eq!(base32_decode(enc).as_deref(), Some(plain.as_bytes()));
        }
    }

    #[test]
    fn base32_rejects_noncanonical() {
        // Invalid lengths, non-alphabet chars, uppercase and non-zero trailing bits.
        for bad in ["m", "mzx", "mzxw6y", "MZXW6", "mzxw1", "mz", "mzxr"] {
            assert_eq!(base32_decode(bad), None, "{bad}");
        }
    }

    /// Golden token computed independently (Python hmac/hashlib/base64).
    #[test]
    fn await_token_golden() {
        assert_eq!(
            mint(3, "run-01", 42, &KEY),
            "ot1_aabqm4tvnywtamiaaaaaaaaaaavdc46cczcvk4rr32fq"
        );
    }

    #[test]
    fn await_token_round_trip() {
        let t = mint(65_535, "01JABCDEF", u64::MAX - 1, &KEY);
        assert!(t.starts_with("ot1_"));
        assert_eq!(
            parse(&t),
            Some(TokenParts {
                partition: 65_535,
                run_id: "01JABCDEF".into(),
                op: u64::MAX - 1,
            })
        );
        assert!(verify(&t, &KEY));
        assert!(!verify(&t, &[8; 32]));
    }

    #[test]
    fn await_token_tamper_detected() {
        let t = mint(3, "run-01", 42, &KEY);
        let alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        for i in 4..t.len() {
            let c = t.as_bytes()[i] as char;
            for r in alphabet.chars().filter(|r| *r != c) {
                let mut s = t.clone();
                s.replace_range(i..=i, &r.to_string());
                assert!(
                    parse(&s).is_none() || !verify(&s, &KEY),
                    "tampered token accepted: {s}"
                );
            }
        }
        assert_eq!(parse("ot2_aabqm4tvnywtamia"), None);
        assert_eq!(parse("ot1_"), None);
        assert!(!verify("garbage", &KEY));
    }
}
