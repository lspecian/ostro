//! Test PKI (12-03, D-P12-18): one in-memory ECDSA P-256 CA per test cluster plus node and
//! worker certificates, generated with rcgen.
//!
//! Node certificates carry exactly one URI SAN, `ostro://<cluster_id>/node/<id>` (the
//! Phase 7 node identity, parsed by `ostro_server::tls::NodeIdentity::from_der`), plus
//! `localhost`, `127.0.0.1` and any extra loopback IPs. Worker certificates carry the worker
//! identity URI instead, so tests can prove that the cluster-internal services reject
//! workers. The CA subject CN is `ostro cluster <cluster_id>`, the shape
//! `ClusterCa::import_pem` accepts, so the PEM pair can be written to `[cluster.tls]
//! ca_cert_file` / `ca_key_file`.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, SanType,
};

use super::HarnessError;

/// A certificate and its private key.
#[derive(Clone, Debug)]
pub struct IssuedCert {
    /// Leaf certificate, PEM.
    pub cert_pem: String,
    /// Leaf private key (PKCS#8), PEM.
    pub key_pem: String,
    /// Leaf certificate, DER.
    pub cert_der: Vec<u8>,
}

/// A test CA for one cluster.
pub struct TestPki {
    cluster_id: String,
    ca_cert_pem: String,
    ca_cert_der: Vec<u8>,
    ca_key_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl std::fmt::Debug for TestPki {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestPki")
            .field("cluster_id", &self.cluster_id)
            .finish_non_exhaustive()
    }
}

fn pki_err(what: &str, e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Other(format!("test pki: {what}: {e}"))
}

fn ia5(s: String) -> Result<SanType, HarnessError> {
    Ok(SanType::URI(
        s.try_into().map_err(|e| pki_err("uri san", e))?,
    ))
}

impl TestPki {
    /// A fresh CA (`CN = ostro cluster <cluster_id>`).
    pub fn new(cluster_id: &str) -> Result<Self, HarnessError> {
        let mut p = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, format!("ostro cluster {cluster_id}"));
        p.distinguished_name = dn;
        p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        p.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key =
            KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(|e| pki_err("ca key", e))?;
        let cert = p
            .self_signed(&key)
            .map_err(|e| pki_err("ca self-sign", e))?;
        let ca_key_pem = key.serialize_pem();
        Ok(Self {
            cluster_id: cluster_id.to_owned(),
            ca_cert_pem: cert.pem(),
            ca_cert_der: cert.der().to_vec(),
            ca_key_pem,
            issuer: Issuer::new(p, key),
        })
    }

    /// The cluster id the CA was made for.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// CA certificate, PEM.
    pub fn ca_cert_pem(&self) -> &str {
        &self.ca_cert_pem
    }

    /// CA certificate, DER.
    pub fn ca_cert_der(&self) -> &[u8] {
        &self.ca_cert_der
    }

    /// CA private key, PEM.
    pub fn ca_key_pem(&self) -> &str {
        &self.ca_key_pem
    }

    /// Write `ca.crt` / `ca.key` into `dir` (created); returns `(cert, key)` paths for
    /// `[cluster.tls] ca_cert_file` / `ca_key_file`.
    pub fn write_ca(&self, dir: &Path) -> Result<(PathBuf, PathBuf), HarnessError> {
        std::fs::create_dir_all(dir).map_err(|e| pki_err("mkdir", e))?;
        let crt = dir.join("ca.crt");
        let key = dir.join("ca.key");
        std::fs::write(&crt, &self.ca_cert_pem).map_err(|e| pki_err("write ca.crt", e))?;
        std::fs::write(&key, &self.ca_key_pem).map_err(|e| pki_err("write ca.key", e))?;
        Ok((crt, key))
    }

    fn leaf(&self, cn: &str, sans: Vec<SanType>) -> Result<IssuedCert, HarnessError> {
        let mut p = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, cn);
        p.distinguished_name = dn;
        p.subject_alt_names = sans;
        p.is_ca = IsCa::ExplicitNoCa;
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        p.use_authority_key_identifier_extension = true;
        let key =
            KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(|e| pki_err("leaf key", e))?;
        let cert = p
            .signed_by(&key, &self.issuer)
            .map_err(|e| pki_err("leaf sign", e))?;
        Ok(IssuedCert {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            cert_der: cert.der().to_vec(),
        })
    }

    /// The node certificate of `id`: exactly one URI SAN `ostro://<cluster_id>/node/<id>`,
    /// plus `localhost`, `127.0.0.1` and every IP of `extra_ips`.
    pub fn node_cert(&self, id: u64, extra_ips: &[IpAddr]) -> Result<IssuedCert, HarnessError> {
        let mut sans = vec![
            ia5(format!("ostro://{}/node/{id}", self.cluster_id))?,
            SanType::DnsName(
                "localhost"
                    .to_owned()
                    .try_into()
                    .map_err(|e| pki_err("dns san", e))?,
            ),
            SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
        ];
        for ip in extra_ips {
            if !sans.contains(&SanType::IpAddress(*ip)) {
                sans.push(SanType::IpAddress(*ip));
            }
        }
        self.leaf(&format!("ostro node {id}"), sans)
    }

    /// A worker-shaped certificate (`ostro://<cluster_id>/worker/<token_id>/<worker_id>`),
    /// signed by the same CA: the cluster services must reject it.
    pub fn worker_cert(&self, token_id: &str, worker_id: &str) -> Result<IssuedCert, HarnessError> {
        let sans = vec![
            ia5(format!(
                "ostro://{}/worker/{token_id}/{worker_id}",
                self.cluster_id
            ))?,
            SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
        ];
        self.leaf(&format!("ostro worker {worker_id}"), sans)
    }
}
