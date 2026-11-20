//! The sealed store (FX-10): AES-256-GCM envelopes in the existing `SealedSecret` record,
//! by convention (06-DECISIONS, no format change):
//!
//! ```text
//! SealedSecret{ name: "<tenant|_>/<name>",
//!               ciphertext: [ver = 1][nonce: 12][ciphertext + tag: 16],
//!               key_id: "aes256gcm:<kid>" | "plaintext-v0" }
//! AAD = b"ostro/secret/v1" || name || key_id        kid = first 8 hex chars of sha256(key)
//! ```
//!
//! Crypto is aws-lc-rs only: [`RandomizedNonceKey`] with [`AES_256_GCM`] draws a fresh random
//! nonce for every seal, so nonce reuse is impossible by construction. The AAD binds the
//! record name and key id: moving a ciphertext to another name, or relabelling its key id,
//! fails authentication.
//!
//! `plaintext-v0` (the Phase 4 await-token key, `__ostro/token-key`) is opened as raw bytes;
//! `seal` never produces it. After [`SealingKeyRing::rotate`] new seals use the new key and
//! ciphertexts under old kids still open.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_lc_rs::aead::{AES_256_GCM, Aad, NONCE_LEN, Nonce, RandomizedNonceKey};
use aws_lc_rs::digest::{SHA256, digest};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ostro_meta::records::{SealedSecret, TOKEN_KEY_PLAINTEXT_ID};
use zeroize::{Zeroize, Zeroizing};

use super::config::SecretsConfig;
use super::value::SecretValue;

/// Envelope version byte.
pub const SEAL_VERSION: u8 = 1;
/// AAD prefix.
pub const AAD_PREFIX: &[u8] = b"ostro/secret/v1";
/// `key_id` prefix of AES-256-GCM envelopes.
pub const KEY_ID_PREFIX: &str = "aes256gcm:";
/// Environment variable holding the base64 sealing key (RESERVATIONS §11).
pub const SEALING_KEY_ENV: &str = "OSTRO_SEALING_KEY";
/// File name of the dev-profile auto-generated key under the data dir.
pub const DEV_KEY_FILE: &str = "sealing.key";
/// Key length.
pub const KEY_LEN: usize = 32;
const TAG_LEN: usize = 16;

/// A sealing or opening failure. Never carries key material or plaintext.
#[derive(Debug, thiserror::Error)]
pub enum SealError {
    /// The key is not 32 bytes.
    #[error("sealing key from {source_name} must be {KEY_LEN} bytes, got {len}")]
    KeyLength {
        /// Where the key came from (`OSTRO_SEALING_KEY`, a path).
        source_name: String,
        /// Decoded length.
        len: usize,
    },
    /// The key is not valid base64.
    #[error("sealing key from {0} is not valid base64")]
    KeyFormat(String),
    /// Reading or writing a key file failed.
    #[error("sealing key file {path}: {err}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The I/O error.
        err: std::io::Error,
    },
    /// The record's `key_id` names no key in the ring (or an unknown scheme).
    #[error("unknown sealing key id {0:?}")]
    UnknownKeyId(String),
    /// The ciphertext is not a valid version-1 envelope.
    #[error("malformed sealed secret envelope")]
    Malformed,
    /// Authentication failed: wrong key, tampered ciphertext, or a name/key-id swap.
    #[error("sealed secret failed authentication")]
    Decrypt,
    /// The AEAD or the random generator failed.
    #[error("sealing failed")]
    Crypto,
}

/// The kid of `key`: the first 8 hex characters of sha256(key).
fn kid_of(key: &[u8]) -> String {
    let d = digest(&SHA256, key);
    d.as_ref()[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// The sealing keys: the current key plus every older key (read-only).
pub struct SealingKeyRing {
    current: String,
    keys: BTreeMap<String, RandomizedNonceKey>,
}

impl fmt::Debug for SealingKeyRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealingKeyRing")
            .field("current", &self.current)
            .field("kids", &self.keys.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn decode_key(text: &[u8], source_name: &str) -> Result<Zeroizing<[u8; KEY_LEN]>, SealError> {
    let trimmed = text.trim_ascii();
    let mut raw = Zeroizing::new(
        STANDARD
            .decode(trimmed)
            .map_err(|_| SealError::KeyFormat(source_name.to_owned()))?,
    );
    to_key(&mut raw, source_name)
}

fn to_key(raw: &mut [u8], source_name: &str) -> Result<Zeroizing<[u8; KEY_LEN]>, SealError> {
    if raw.len() != KEY_LEN {
        let len = raw.len();
        raw.zeroize();
        return Err(SealError::KeyLength {
            source_name: source_name.to_owned(),
            len,
        });
    }
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    key.copy_from_slice(raw);
    raw.zeroize();
    Ok(key)
}

/// A key file holds base64 text, or exactly 32 raw bytes.
fn read_key_file(path: &Path) -> Result<Zeroizing<[u8; KEY_LEN]>, SealError> {
    let mut data = Zeroizing::new(std::fs::read(path).map_err(|err| SealError::Io {
        path: path.to_owned(),
        err,
    })?);
    let name = path.display().to_string();
    if data.len() == KEY_LEN {
        return to_key(&mut data, &name);
    }
    decode_key(&data, &name)
}

fn create_dev_key(path: &Path) -> Result<Zeroizing<[u8; KEY_LEN]>, SealError> {
    use std::io::Write;
    let io = |err| SealError::Io {
        path: path.to_owned(),
        err,
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(io)?;
    }
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    aws_lc_rs::rand::fill(&mut key[..]).map_err(|_| SealError::Crypto)?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path).map_err(io)?;
    // Sized up front so the push never reallocates (no stray unzeroized copy).
    let mut text = Zeroizing::new(String::with_capacity(48));
    STANDARD.encode_string(&key[..], &mut text);
    text.push('\n');
    f.write_all(text.as_bytes()).map_err(io)?;
    f.sync_all().map_err(io)?;
    tracing::warn!(
        path = %path.display(),
        "generated a development sealing key; set OSTRO_SEALING_KEY or [secrets] sealing_key_file in production"
    );
    Ok(key)
}

impl SealingKeyRing {
    /// A ring with `key` as the current (and only) key. `key` is zeroized.
    pub fn from_bytes(mut key: [u8; KEY_LEN]) -> Self {
        let mut ring = Self {
            current: String::new(),
            keys: BTreeMap::new(),
        };
        ring.rotate_inner(&key);
        key.zeroize();
        ring
    }

    fn rotate_inner(&mut self, key: &[u8; KEY_LEN]) {
        let kid = kid_of(key);
        let k = RandomizedNonceKey::new(&AES_256_GCM, key).expect("32-byte AES-256 key");
        self.keys.entry(kid.clone()).or_insert(k);
        self.current = kid;
    }

    /// Makes `key` the current key; older keys stay available for `open`. `key` is zeroized.
    pub fn rotate(&mut self, mut key: [u8; KEY_LEN]) {
        self.rotate_inner(&key);
        key.zeroize();
    }

    /// The current kid.
    pub fn current_kid(&self) -> &str {
        &self.current
    }

    /// The current `key_id` (`aes256gcm:<kid>`).
    pub fn current_key_id(&self) -> String {
        format!("{KEY_ID_PREFIX}{}", self.current)
    }

    /// Loads the sealing key, in order:
    ///
    /// 1. `OSTRO_SEALING_KEY` (base64 of 32 bytes);
    /// 2. `[secrets] sealing_key_file` (base64 text or 32 raw bytes);
    /// 3. `dev` only: `<data_dir>/sealing.key`, created with mode 0600 (and a warning naming
    ///    the path) if missing.
    ///
    /// `Ok(None)` = server mode without a key: the caller refuses to store secrets. A key of
    /// the wrong length or encoding is an error.
    pub fn load(
        cfg: &SecretsConfig,
        data_dir: &Path,
        dev: bool,
    ) -> Result<Option<Self>, SealError> {
        Self::load_with(cfg, data_dir, dev, &|k| std::env::var_os(k))
    }

    /// [`load`](Self::load) with an injected environment lookup (tests).
    pub fn load_with(
        cfg: &SecretsConfig,
        data_dir: &Path,
        dev: bool,
        env: &dyn Fn(&str) -> Option<OsString>,
    ) -> Result<Option<Self>, SealError> {
        let key = if let Some(v) = env(SEALING_KEY_ENV) {
            let v = Zeroizing::new(v.into_encoded_bytes());
            decode_key(&v, SEALING_KEY_ENV)?
        } else if let Some(p) = &cfg.sealing_key_file {
            read_key_file(p)?
        } else if dev {
            let p = data_dir.join(DEV_KEY_FILE);
            match read_key_file(&p) {
                Ok(k) => k,
                Err(SealError::Io { err, .. }) if err.kind() == std::io::ErrorKind::NotFound => {
                    create_dev_key(&p)?
                }
                Err(e) => return Err(e),
            }
        } else {
            return Ok(None);
        };
        Ok(Some(Self::from_bytes(*key)))
    }
}

/// Seals and opens `SealedSecret` records.
#[derive(Clone, Debug)]
pub struct SealedStore {
    ring: Arc<SealingKeyRing>,
}

fn aad(name: &str, key_id: &str) -> Vec<u8> {
    let mut a = Vec::with_capacity(AAD_PREFIX.len() + name.len() + key_id.len());
    a.extend_from_slice(AAD_PREFIX);
    a.extend_from_slice(name.as_bytes());
    a.extend_from_slice(key_id.as_bytes());
    a
}

impl SealedStore {
    /// A store over `ring`.
    pub fn new(ring: Arc<SealingKeyRing>) -> Self {
        Self { ring }
    }

    /// The key ring.
    pub fn ring(&self) -> &Arc<SealingKeyRing> {
        &self.ring
    }

    /// Seals `plaintext` under the current key as record `name` (`<tenant|_>/<name>`).
    pub fn seal(&self, name: &str, plaintext: &[u8]) -> Result<SealedSecret, SealError> {
        let key_id = self.ring.current_key_id();
        let key = self
            .ring
            .keys
            .get(&self.ring.current)
            .ok_or_else(|| SealError::UnknownKeyId(key_id.clone()))?;
        let mut buf = Zeroizing::new(Vec::with_capacity(plaintext.len() + TAG_LEN));
        buf.extend_from_slice(plaintext);
        let nonce = key
            .seal_in_place_append_tag(Aad::from(aad(name, &key_id)), &mut *buf)
            .map_err(|_| SealError::Crypto)?;
        let mut ciphertext = Vec::with_capacity(1 + NONCE_LEN + buf.len());
        ciphertext.push(SEAL_VERSION);
        ciphertext.extend_from_slice(nonce.as_ref());
        ciphertext.extend_from_slice(&buf);
        Ok(SealedSecret {
            name: name.to_owned(),
            ciphertext,
            key_id,
            ext: Vec::new(),
        })
    }

    /// Opens a record. `plaintext-v0` returns the raw bytes; `aes256gcm:<kid>` must name a
    /// key of the ring and authenticate under the record's name and key id.
    pub fn open(&self, s: &SealedSecret) -> Result<SecretValue, SealError> {
        open_record(Some(&self.ring), s)
    }
}

/// Opens `s` with `ring` (`None`: only `plaintext-v0` records open).
pub(crate) fn open_record(
    ring: Option<&SealingKeyRing>,
    s: &SealedSecret,
) -> Result<SecretValue, SealError> {
    if s.key_id == TOKEN_KEY_PLAINTEXT_ID {
        return Ok(SecretValue::from_slice(&s.ciphertext));
    }
    let kid = s
        .key_id
        .strip_prefix(KEY_ID_PREFIX)
        .ok_or_else(|| SealError::UnknownKeyId(s.key_id.clone()))?;
    let key = ring
        .and_then(|r| r.keys.get(kid))
        .ok_or_else(|| SealError::UnknownKeyId(s.key_id.clone()))?;
    let c = &s.ciphertext;
    if c.len() < 1 + NONCE_LEN + TAG_LEN || c[0] != SEAL_VERSION {
        return Err(SealError::Malformed);
    }
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&c[1..1 + NONCE_LEN]);
    let mut buf = Zeroizing::new(c[1 + NONCE_LEN..].to_vec());
    let plain = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad(&s.name, &s.key_id)),
            &mut buf,
        )
        .map_err(|_| SealError::Decrypt)?;
    Ok(SecretValue::from_slice(plain))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANARY: &[u8] = b"CANARY-sealed-9d2c-value";

    fn ring(seed: u8) -> SealingKeyRing {
        SealingKeyRing::from_bytes([seed; KEY_LEN])
    }

    fn store(seed: u8) -> SealedStore {
        SealedStore::new(Arc::new(ring(seed)))
    }

    #[test]
    fn round_trip_and_envelope_layout() {
        let st = store(7);
        let s = st.seal("_/stripe/key", CANARY).unwrap();
        assert_eq!(s.name, "_/stripe/key");
        assert_eq!(s.key_id, format!("aes256gcm:{}", kid_of(&[7; 32])));
        assert_eq!(s.key_id.len(), "aes256gcm:".len() + 8);
        assert_eq!(s.ciphertext[0], 1);
        assert_eq!(s.ciphertext.len(), 1 + 12 + CANARY.len() + 16);
        assert!(!s.ciphertext.windows(CANARY.len()).any(|w| w == CANARY));
        assert_eq!(st.open(&s).unwrap().expose(), CANARY);
        // Fresh random nonce per seal.
        let s2 = st.seal("_/stripe/key", CANARY).unwrap();
        assert_ne!(s.ciphertext, s2.ciphertext);
        assert_eq!(st.open(&s2).unwrap().expose(), CANARY);
        // Empty plaintext seals too.
        let e = st.seal("_/e", b"").unwrap();
        assert!(st.open(&e).unwrap().is_empty());
    }

    #[test]
    fn kid_is_sha256_prefix() {
        // sha256 of 32 zero bytes = 66687aad...
        assert_eq!(kid_of(&[0u8; 32]), "66687aad");
    }

    #[test]
    fn wrong_key_tamper_and_swap_fail() {
        let st = store(1);
        let s = st.seal("t1/k", CANARY).unwrap();

        // A different key with the same kid label cannot exist; a different ring has no kid.
        assert!(matches!(store(2).open(&s), Err(SealError::UnknownKeyId(_))));

        // Wrong key under the right kid label: forge a ring entry.
        let mut forged = ring(1);
        let kid = forged.current.clone();
        forged.keys.insert(
            kid,
            RandomizedNonceKey::new(&AES_256_GCM, &[3u8; 32]).unwrap(),
        );
        assert!(matches!(
            SealedStore::new(Arc::new(forged)).open(&s),
            Err(SealError::Decrypt)
        ));

        for i in 0..s.ciphertext.len() {
            let mut t = s.clone();
            t.ciphertext[i] ^= 0x01;
            assert!(st.open(&t).is_err(), "flip at {i} accepted");
        }

        let mut swapped = s.clone();
        swapped.name = "t2/k".into();
        assert!(matches!(st.open(&swapped), Err(SealError::Decrypt)));

        let mut relabelled = s.clone();
        relabelled.key_id = "aes256gcm:00000000".into();
        assert!(matches!(
            st.open(&relabelled),
            Err(SealError::UnknownKeyId(_))
        ));
        let mut scheme = s.clone();
        scheme.key_id = "chacha:abcd".into();
        assert!(matches!(st.open(&scheme), Err(SealError::UnknownKeyId(_))));

        let mut short = s.clone();
        short.ciphertext.truncate(20);
        assert!(matches!(st.open(&short), Err(SealError::Malformed)));
    }

    #[test]
    fn plaintext_v0_stays_readable() {
        let rec = SealedSecret {
            name: ostro_meta::records::TOKEN_KEY_SECRET.into(),
            ciphertext: CANARY.to_vec(),
            key_id: "plaintext-v0".into(),
            ext: vec![],
        };
        assert_eq!(store(1).open(&rec).unwrap().expose(), CANARY);
        assert_eq!(open_record(None, &rec).unwrap().expose(), CANARY);
        // seal never produces plaintext-v0.
        assert!(
            store(1)
                .seal("x", b"y")
                .unwrap()
                .key_id
                .starts_with("aes256gcm:")
        );
    }

    #[test]
    fn rotation_keeps_old_kids_readable() {
        let mut r = ring(1);
        let old_kid = r.current_kid().to_owned();
        let old = SealedStore::new(Arc::new(ring(1)))
            .seal("_/k", CANARY)
            .unwrap();
        r.rotate([9; KEY_LEN]);
        assert_ne!(r.current_kid(), old_kid);
        let st = SealedStore::new(Arc::new(r));
        let new = st.seal("_/k", CANARY).unwrap();
        assert_eq!(new.key_id, format!("aes256gcm:{}", kid_of(&[9; 32])));
        assert_eq!(st.open(&old).unwrap().expose(), CANARY);
        assert_eq!(st.open(&new).unwrap().expose(), CANARY);
    }

    fn no_env(_: &str) -> Option<OsString> {
        None
    }

    #[test]
    fn load_order_env_then_file_then_dev() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SecretsConfig::default();

        // Server mode, nothing configured: no key.
        assert!(
            SealingKeyRing::load_with(&cfg, dir.path(), false, &no_env)
                .unwrap()
                .is_none()
        );

        // Env wins over the file.
        let env_key = STANDARD.encode([5u8; 32]);
        let file = dir.path().join("k.b64");
        std::fs::write(&file, format!("{}\n", STANDARD.encode([6u8; 32]))).unwrap();
        let cfg_file = SecretsConfig {
            sealing_key_file: Some(file.clone()),
            ..SecretsConfig::default()
        };
        let env = |k: &str| (k == SEALING_KEY_ENV).then(|| OsString::from(env_key.clone()));
        let r = SealingKeyRing::load_with(&cfg_file, dir.path(), false, &env)
            .unwrap()
            .unwrap();
        assert_eq!(r.current_kid(), kid_of(&[5; 32]));

        // File (base64 text, trailing newline) when the env is unset.
        let r = SealingKeyRing::load_with(&cfg_file, dir.path(), false, &no_env)
            .unwrap()
            .unwrap();
        assert_eq!(r.current_kid(), kid_of(&[6; 32]));

        // File with 32 raw bytes.
        std::fs::write(&file, [8u8; 32]).unwrap();
        let r = SealingKeyRing::load_with(&cfg_file, dir.path(), false, &no_env)
            .unwrap()
            .unwrap();
        assert_eq!(r.current_kid(), kid_of(&[8; 32]));

        // Dev: auto-generated, 0600, stable across loads.
        let data = dir.path().join("data");
        let r1 = SealingKeyRing::load_with(&cfg, &data, true, &no_env)
            .unwrap()
            .unwrap();
        let p = data.join(DEV_KEY_FILE);
        assert!(p.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let r2 = SealingKeyRing::load_with(&cfg, &data, true, &no_env)
            .unwrap()
            .unwrap();
        assert_eq!(r1.current_kid(), r2.current_kid());
        // Server mode never reads the dev file.
        assert!(
            SealingKeyRing::load_with(&cfg, &data, false, &no_env)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn load_rejects_bad_keys() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SecretsConfig::default();
        let short = STANDARD.encode([1u8; 16]);
        let env = |k: &str| (k == SEALING_KEY_ENV).then(|| OsString::from(short.clone()));
        let e = SealingKeyRing::load_with(&cfg, dir.path(), false, &env).unwrap_err();
        assert!(matches!(e, SealError::KeyLength { len: 16, .. }));
        let env = |k: &str| (k == SEALING_KEY_ENV).then(|| OsString::from("not base64!!"));
        assert!(matches!(
            SealingKeyRing::load_with(&cfg, dir.path(), false, &env),
            Err(SealError::KeyFormat(_))
        ));
        let file = dir.path().join("bad");
        std::fs::write(&file, STANDARD.encode([1u8; 31])).unwrap();
        let cfg_file = SecretsConfig {
            sealing_key_file: Some(file),
            ..SecretsConfig::default()
        };
        assert!(matches!(
            SealingKeyRing::load_with(&cfg_file, dir.path(), true, &no_env),
            Err(SealError::KeyLength { len: 31, .. })
        ));
        let missing = SecretsConfig {
            sealing_key_file: Some(dir.path().join("missing")),
            ..SecretsConfig::default()
        };
        assert!(matches!(
            SealingKeyRing::load_with(&missing, dir.path(), true, &no_env),
            Err(SealError::Io { .. })
        ));
    }

    #[test]
    fn debug_never_shows_key_material() {
        let r = ring(0x41);
        let d = format!("{r:?}");
        assert!(d.contains(r.current_kid()));
        assert!(!d.contains("AAAA") && !d.contains("65, 65"));
        let st = SealedStore::new(Arc::new(r));
        assert!(!format!("{st:?}").contains("65, 65"));
        let v = st.open(&st.seal("_/x", CANARY).unwrap()).unwrap();
        assert!(!format!("{v:?}").contains("CANARY"));
    }
}
