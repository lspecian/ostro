//! SASL authentication (14-31, D-P14-42): SaslHandshake v1 then SaslAuthenticate, with
//! `PLAIN` (RFC 4616) and `SCRAM-SHA-256` / `SCRAM-SHA-512` (RFC 5802 / RFC 7677).
//!
//! SCRAM: random 24-byte client nonce, PBKDF2-HMAC via aws-lc-rs, client proof, and the
//! server signature in server-final is verified (constant time); a mismatch, a server nonce
//! that does not extend the client's, fewer than 4096 iterations or a server error is
//! [`KafkaError::Sasl`]. Every credential-derived buffer is wiped on drop (`Zeroizing`).
//!
//! No credential appears in `Debug` output, logs or errors: errors name the mechanism (and
//! the user name at most), and broker error messages are scrubbed of the password before they
//! are surfaced.

use aws_lc_rs::{constant_time, digest, hmac, pbkdf2};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use kafka_protocol::messages::{SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::StrBytes;
use zeroize::Zeroizing;

use crate::conn::Conn;
use crate::{KafkaError, SaslConfig, SaslMechanism};

/// Kafka error code `UNSUPPORTED_SASL_MECHANISM`.
const UNSUPPORTED_SASL_MECHANISM: i16 = 33;

/// Minimum SCRAM iteration count accepted from a server (RFC 7677 recommendation; Kafka's
/// minimum).
pub const SCRAM_MIN_ITERATIONS: u32 = 4096;
/// Maximum SCRAM iteration count accepted (bounds the work a hostile server can demand).
pub const SCRAM_MAX_ITERATIONS: u32 = 1_000_000;

fn sasl_err(mech: SaslMechanism, msg: impl std::fmt::Display) -> KafkaError {
    KafkaError::Sasl(format!("{}: {msg}", mech.as_str()))
}

/// Authenticate `conn` (already negotiated) with `cfg`.
pub(crate) async fn authenticate(conn: &Conn, cfg: &SaslConfig) -> Result<(), KafkaError> {
    let mech = cfg.mechanism;
    let hs = conn
        .request(
            &SaslHandshakeRequest::default()
                .with_mechanism(StrBytes::from_static_str(mech.as_str())),
        )
        .await?;
    match hs.error_code {
        0 => {}
        UNSUPPORTED_SASL_MECHANISM => {
            let offered: Vec<&str> = hs.mechanisms.iter().map(|m| m.as_str()).collect();
            return Err(sasl_err(
                mech,
                format_args!("mechanism not enabled on the broker (offered: {offered:?})"),
            ));
        }
        code => return Err(sasl_err(mech, format_args!("SaslHandshake error {code}"))),
    }
    match mech {
        SaslMechanism::Plain => {
            let token = plain_token(&cfg.username, &cfg.password);
            round(conn, cfg, Bytes::copy_from_slice(&token)).await?;
        }
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
            let mut scram = ScramClient::new(mech, &cfg.username, &cfg.password)?;
            let first = scram.client_first();
            let server_first = round(conn, cfg, Bytes::from(first.into_bytes())).await?;
            let server_first = std::str::from_utf8(&server_first)
                .map_err(|_| sasl_err(mech, "server-first-message is not UTF-8"))?;
            let client_final = scram.client_final(server_first)?;
            let server_final =
                round(conn, cfg, Bytes::copy_from_slice(client_final.as_bytes())).await?;
            let server_final = std::str::from_utf8(&server_final)
                .map_err(|_| sasl_err(mech, "server-final-message is not UTF-8"))?;
            scram.verify_server_final(server_final)?;
        }
    }
    tracing::debug!(addr = conn.addr(), mechanism = mech.as_str(), user = %cfg.username, "kafka: SASL authenticated");
    Ok(())
}

/// One SaslAuthenticate round trip; returns the server's bytes.
async fn round(conn: &Conn, cfg: &SaslConfig, token: Bytes) -> Result<Bytes, KafkaError> {
    let r = conn
        .request(&SaslAuthenticateRequest::default().with_auth_bytes(token))
        .await?;
    if r.error_code != 0 {
        let msg = r
            .error_message
            .as_ref()
            .map(|m| scrub(m.as_str(), &cfg.password))
            .unwrap_or_default();
        return Err(sasl_err(
            cfg.mechanism,
            format_args!(
                "authentication failed for user {:?} (broker error {}: {msg})",
                cfg.username, r.error_code
            ),
        ));
    }
    Ok(r.auth_bytes)
}

/// Remove any occurrence of `secret` from a broker-supplied message.
fn scrub(msg: &str, secret: &str) -> String {
    if secret.is_empty() {
        msg.to_owned()
    } else {
        msg.replace(secret, "<redacted>")
    }
}

/// The PLAIN token: `authzid NUL authcid NUL passwd` with an empty authzid.
pub fn plain_token(user: &str, password: &str) -> Zeroizing<Vec<u8>> {
    let mut t = Zeroizing::new(Vec::with_capacity(user.len() + password.len() + 2));
    t.push(0);
    t.extend_from_slice(user.as_bytes());
    t.push(0);
    t.extend_from_slice(password.as_bytes());
    t
}

/// `saslname` escaping of RFC 5802 (`=` → `=3D`, `,` → `=2C`).
fn sasl_name(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

#[derive(Clone, Copy)]
struct Algo {
    hmac: hmac::Algorithm,
    digest: &'static digest::Algorithm,
    pbkdf2: pbkdf2::Algorithm,
    len: usize,
}

fn algo(mech: SaslMechanism) -> Option<Algo> {
    match mech {
        SaslMechanism::ScramSha256 => Some(Algo {
            hmac: hmac::HMAC_SHA256,
            digest: &digest::SHA256,
            pbkdf2: pbkdf2::PBKDF2_HMAC_SHA256,
            len: 32,
        }),
        SaslMechanism::ScramSha512 => Some(Algo {
            hmac: hmac::HMAC_SHA512,
            digest: &digest::SHA512,
            pbkdf2: pbkdf2::PBKDF2_HMAC_SHA512,
            len: 64,
        }),
        SaslMechanism::Plain => None,
    }
}

fn hmac_sign(a: Algo, key: &[u8], msg: &[u8]) -> Zeroizing<Vec<u8>> {
    let k = hmac::Key::new(a.hmac, key);
    Zeroizing::new(hmac::sign(&k, msg).as_ref().to_vec())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScramStep {
    Start,
    SentFirst,
    SentFinal,
    Done,
}

/// The client side of one SCRAM exchange (RFC 5802), without channel binding (`n,,`).
pub struct ScramClient {
    mech: SaslMechanism,
    algo: Algo,
    password: Zeroizing<String>,
    client_first_bare: String,
    cnonce: String,
    expected_server_sig: Zeroizing<Vec<u8>>,
    step: ScramStep,
}

impl std::fmt::Debug for ScramClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramClient")
            .field("mechanism", &self.mech)
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl ScramClient {
    /// A client with a fresh random nonce.
    pub fn new(mech: SaslMechanism, user: &str, password: &str) -> Result<Self, KafkaError> {
        let mut raw = [0u8; 24];
        aws_lc_rs::rand::fill(&mut raw).map_err(|_| sasl_err(mech, "system RNG failed"))?;
        Self::with_nonce(mech, user, password, &B64.encode(raw))
    }

    /// A client with a fixed nonce (RFC test vectors).
    pub fn with_nonce(
        mech: SaslMechanism,
        user: &str,
        password: &str,
        nonce: &str,
    ) -> Result<Self, KafkaError> {
        let algo = algo(mech).ok_or_else(|| sasl_err(mech, "not a SCRAM mechanism"))?;
        if nonce.is_empty() || nonce.contains(',') {
            return Err(sasl_err(mech, "invalid client nonce"));
        }
        Ok(ScramClient {
            mech,
            algo,
            password: Zeroizing::new(password.to_owned()),
            client_first_bare: format!("n={},r={nonce}", sasl_name(user)),
            cnonce: nonce.to_owned(),
            expected_server_sig: Zeroizing::new(Vec::new()),
            step: ScramStep::Start,
        })
    }

    /// `client-first-message` (`n,,n=<user>,r=<nonce>`).
    pub fn client_first(&mut self) -> String {
        self.step = ScramStep::SentFirst;
        format!("n,,{}", self.client_first_bare)
    }

    /// Process `server-first-message`; returns `client-final-message` (with the proof).
    pub fn client_final(&mut self, server_first: &str) -> Result<Zeroizing<String>, KafkaError> {
        let mech = self.mech;
        if self.step != ScramStep::SentFirst {
            return Err(sasl_err(mech, "server-first-message out of order"));
        }
        let mut nonce = None;
        let mut salt = None;
        let mut iters = None;
        for (i, attr) in server_first.split(',').enumerate() {
            match attr.split_once('=') {
                Some(("m", _)) if i == 0 => {
                    return Err(sasl_err(mech, "server requires an unsupported extension"));
                }
                Some(("r", v)) => nonce = Some(v),
                Some(("s", v)) => salt = Some(v),
                Some(("i", v)) => iters = Some(v),
                Some(("e", v)) => {
                    return Err(sasl_err(mech, format_args!("server error: {v}")));
                }
                _ => {}
            }
        }
        let (Some(nonce), Some(salt), Some(iters)) = (nonce, salt, iters) else {
            return Err(sasl_err(mech, "malformed server-first-message"));
        };
        if !nonce.starts_with(&self.cnonce) || nonce.len() <= self.cnonce.len() {
            return Err(sasl_err(
                mech,
                "server nonce does not extend the client nonce",
            ));
        }
        let salt = B64
            .decode(salt)
            .map_err(|_| sasl_err(mech, "server salt is not base64"))?;
        let iters: u32 = iters
            .parse()
            .map_err(|_| sasl_err(mech, "server iteration count is not a number"))?;
        if !(SCRAM_MIN_ITERATIONS..=SCRAM_MAX_ITERATIONS).contains(&iters) {
            return Err(sasl_err(
                mech,
                format_args!(
                    "server iteration count {iters} outside {SCRAM_MIN_ITERATIONS}..={SCRAM_MAX_ITERATIONS}"
                ),
            ));
        }
        let a = self.algo;
        let mut salted = Zeroizing::new(vec![0u8; a.len]);
        let n =
            std::num::NonZeroU32::new(iters).ok_or_else(|| sasl_err(mech, "zero iterations"))?;
        pbkdf2::derive(a.pbkdf2, n, &salt, self.password.as_bytes(), &mut salted);
        let client_key = hmac_sign(a, &salted, b"Client Key");
        let stored_key = digest::digest(a.digest, &client_key);
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = Zeroizing::new(format!(
            "{},{server_first},{without_proof}",
            self.client_first_bare
        ));
        let client_sig = hmac_sign(a, stored_key.as_ref(), auth_message.as_bytes());
        let proof: Zeroizing<Vec<u8>> = Zeroizing::new(
            client_key
                .iter()
                .zip(client_sig.iter())
                .map(|(k, s)| k ^ s)
                .collect(),
        );
        let server_key = hmac_sign(a, &salted, b"Server Key");
        self.expected_server_sig = hmac_sign(a, &server_key, auth_message.as_bytes());
        self.step = ScramStep::SentFinal;
        Ok(Zeroizing::new(format!(
            "{without_proof},p={}",
            B64.encode(&*proof)
        )))
    }

    /// Verify `server-final-message` (`v=<server signature>`).
    pub fn verify_server_final(&mut self, server_final: &str) -> Result<(), KafkaError> {
        let mech = self.mech;
        if self.step != ScramStep::SentFinal {
            return Err(sasl_err(mech, "server-final-message out of order"));
        }
        let first = server_final.split(',').next().unwrap_or_default();
        if let Some(e) = first.strip_prefix("e=") {
            return Err(sasl_err(mech, format_args!("server error: {e}")));
        }
        let sig = first
            .strip_prefix("v=")
            .ok_or_else(|| sasl_err(mech, "malformed server-final-message"))?;
        let sig = B64
            .decode(sig)
            .map_err(|_| sasl_err(mech, "server signature is not base64"))?;
        constant_time::verify_slices_are_equal(&sig, &self.expected_server_sig)
            .map_err(|_| sasl_err(mech, "server signature mismatch (not the expected server)"))?;
        self.step = ScramStep::Done;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 section 3 test vector (SCRAM-SHA-256).
    #[test]
    fn rfc7677_scram_sha256_vector() {
        let mut c = ScramClient::with_nonce(
            SaslMechanism::ScramSha256,
            "user",
            "pencil",
            "rOprNGfwEbeRWgbNEkqO",
        )
        .unwrap();
        assert_eq!(c.client_first(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let fin = c.client_final(server_first).unwrap();
        assert_eq!(
            fin.as_str(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        c.verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
    }

    #[test]
    fn rfc7677_wrong_server_signature_is_rejected() {
        let mut c = ScramClient::with_nonce(
            SaslMechanism::ScramSha256,
            "user",
            "pencil",
            "rOprNGfwEbeRWgbNEkqO",
        )
        .unwrap();
        c.client_first();
        c.client_final(
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
        )
        .unwrap();
        let e = c
            .verify_server_final("v=AAAATRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap_err();
        assert!(matches!(e, KafkaError::Sasl(_)), "{e:?}");
        assert!(!e.to_string().contains("pencil"));
    }

    /// The SHA-512 proof matches an independent server-side computation (RFC 5802 formulas).
    #[test]
    fn scram_sha512_round_trip_against_server_formulas() {
        let (user, pw, salt, iters) = ("ostro", "s3cret,=pw", b"0123456789abcdef", 4096u32);
        let mut c = ScramClient::new(SaslMechanism::ScramSha512, user, pw).unwrap();
        let first = c.client_first();
        let bare = first.strip_prefix("n,,").unwrap();
        let cnonce = bare.split_once(",r=").unwrap().1;
        let server_first = format!("r={cnonce}SERVER,s={},i={iters}", B64.encode(salt));
        let fin = c.client_final(&server_first).unwrap();
        // server side
        let mut salted = [0u8; 64];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA512,
            std::num::NonZeroU32::new(iters).unwrap(),
            salt,
            pw.as_bytes(),
            &mut salted,
        );
        let ck = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA512, &salted), b"Client Key");
        let stored = digest::digest(&digest::SHA512, ck.as_ref());
        let (without, proof) = fin.rsplit_once(",p=").unwrap();
        let auth = format!("{bare},{server_first},{without}");
        let sig = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA512, stored.as_ref()),
            auth.as_bytes(),
        );
        let proof = B64.decode(proof).unwrap();
        let recovered: Vec<u8> = proof.iter().zip(sig.as_ref()).map(|(p, s)| p ^ s).collect();
        assert_eq!(
            digest::digest(&digest::SHA512, &recovered).as_ref(),
            stored.as_ref()
        );
        let sk = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA512, &salted), b"Server Key");
        let ssig = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA512, sk.as_ref()),
            auth.as_bytes(),
        );
        c.verify_server_final(&format!("v={}", B64.encode(ssig.as_ref())))
            .unwrap();
    }

    #[test]
    fn hostile_server_first_messages_are_rejected() {
        let mk = || {
            let mut c =
                ScramClient::with_nonce(SaslMechanism::ScramSha512, "u", "p", "abc").unwrap();
            c.client_first();
            c
        };
        for bad in [
            "r=xyzdef,s=AAAA,i=4096",      // nonce not extending ours
            "r=abc,s=AAAA,i=4096",         // nonce not longer
            "r=abcd,s=!!!,i=4096",         // salt not base64
            "r=abcd,s=AAAA,i=1",           // too few iterations
            "r=abcd,s=AAAA,i=99999999999", // too many / overflow
            "r=abcd,s=AAAA",               // missing i
            "m=ext,r=abcd,s=AAAA,i=4096",  // mandatory extension
            "e=other-error",
            "",
        ] {
            assert!(
                matches!(mk().client_final(bad), Err(KafkaError::Sasl(_))),
                "{bad}"
            );
        }
        let mut c = mk();
        c.client_final("r=abcd,s=AAAA,i=4096").unwrap();
        assert!(c.verify_server_final("e=invalid-proof").is_err());
        assert!(c.verify_server_final("x=1").is_err());
        assert!(c.verify_server_final("v=***").is_err());
        // out of order
        let mut c = ScramClient::with_nonce(SaslMechanism::ScramSha256, "u", "p", "abc").unwrap();
        assert!(c.client_final("r=abcd,s=AAAA,i=4096").is_err());
        assert!(ScramClient::with_nonce(SaslMechanism::Plain, "u", "p", "abc").is_err());
        assert!(ScramClient::with_nonce(SaslMechanism::ScramSha256, "u", "p", "a,b").is_err());
    }

    #[test]
    fn plain_token_and_names() {
        assert_eq!(&plain_token("u", "p")[..], b"\0u\0p");
        assert_eq!(sasl_name("a=b,c"), "a=3Db=2Cc");
        let mut c =
            ScramClient::with_nonce(SaslMechanism::ScramSha256, "a=b,c", "hunter2", "n").unwrap();
        assert_eq!(c.client_first(), "n,,n=a=3Db=2Cc,r=n");
        assert!(!format!("{c:?}").contains("hunter2"));
    }

    #[test]
    fn broker_messages_are_scrubbed() {
        assert_eq!(scrub("bad pw hunter2!", "hunter2"), "bad pw <redacted>!");
        assert_eq!(scrub("msg", ""), "msg");
    }
}
