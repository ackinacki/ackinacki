use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use ed25519_dalek::Signer;
use ed25519_dalek::Verifier;
use rcgen::CertificateParams;
use rcgen::CustomExtension;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::client::danger::ServerCertVerified;
use rustls::client::danger::ServerCertVerifier;
use rustls::server::danger::ClientCertVerified;
use rustls::server::danger::ClientCertVerifier;
use rustls::server::WebPkiClientVerifier;
use rustls::version::TLS13;
use rustls::DigitallySignedStruct;
use rustls::DistinguishedName;
use rustls::Error;
use rustls::RootCertStore;
use rustls::SignatureScheme;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::ServerName;
use rustls_pki_types::UnixTime;
use serde::Deserialize;
use x509_parser::nom::AsBytes;

use crate::CertHash;
use crate::NetCredential;

#[derive(Debug, Default)]
struct NoCertVerification;

impl ServerCertVerifier for NoCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

impl ClientCertVerifier for NoCertVerification {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

fn root_cert_store(_cred: &NetCredential) -> RootCertStore {
    RootCertStore::empty()
}

pub fn client_tls_config(
    is_debug: bool,
    credential: &NetCredential,
    alpn_preferred: &[&str],
) -> Result<rustls::ClientConfig, anyhow::Error> {
    let mut tls_config = if is_debug {
        rustls::ClientConfig::builder_with_protocol_versions(&[&TLS13])
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerification))
            .with_client_auth_cert(credential.my_certs.clone(), credential.my_key.clone_key())?
    } else {
        rustls::ClientConfig::builder_with_protocol_versions(&[&TLS13])
            .with_root_certificates(root_cert_store(credential))
            .with_client_auth_cert(credential.my_certs.clone(), credential.my_key.clone_key())?
    };
    tls_config.alpn_protocols = alpn_preferred.iter().map(|s| s.as_bytes().to_vec()).collect();
    tls_config.key_log = Arc::new(rustls::KeyLogFile::new());
    Ok(tls_config)
}

pub fn server_tls_config(
    is_debug: bool,
    credential: &NetCredential,
    alpn_supported: &[&str],
) -> anyhow::Result<rustls::ServerConfig> {
    let mut tls_config = if is_debug {
        rustls::ServerConfig::builder_with_protocol_versions(&[&TLS13])
            .with_client_cert_verifier(Arc::new(NoCertVerification))
            .with_single_cert(credential.my_certs.clone(), credential.my_key.clone_key())?
    } else {
        rustls::ServerConfig::builder_with_protocol_versions(&[&TLS13])
            .with_client_cert_verifier(
                WebPkiClientVerifier::builder(Arc::new(root_cert_store(credential))).build()?,
            )
            .with_single_cert(credential.my_certs.clone(), credential.my_key.clone_key())?
    };
    tls_config.alpn_protocols = alpn_supported.iter().map(|s| s.as_bytes().to_vec()).collect();
    Ok(tls_config)
}

fn cert_subjects(subjects: Option<Vec<String>>) -> Vec<String> {
    subjects.unwrap_or_else(|| vec!["localhost".to_string()])
}

const ED_SIGNATURE_OID_PARTS: [u64; 6] = [1, 2, 3, 4, 5, 6];
const ED_SIGNATURE_OID: x509_parser::der_parser::Oid =
    x509_parser::der_parser::oid!(1.2.3 .4 .5 .6);

pub fn generate_self_signed_cert(
    subjects: Option<Vec<String>>,
    ed_signing_key: Option<ed25519_dalek::SigningKey>,
) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
    let key_pair = rcgen::KeyPair::generate()?;
    let key = PrivateKeyDer::<'static>::try_from(key_pair.serialize_der())
        .map_err(|err| anyhow::anyhow!("Failed to generate TLS key: {}", err.to_string()))?
        .clone_key();
    create_self_signed_cert_with_ed_signature(subjects, &key, ed_signing_key)
}

pub fn create_self_signed_cert_with_ed_signature(
    subjects: Option<Vec<String>>,
    tls_key: &PrivateKeyDer<'static>,
    ed_signing_key: Option<ed25519_dalek::SigningKey>,
) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
    let mut params = CertificateParams::new(cert_subjects(subjects))?;
    let key_pair = rcgen::KeyPair::try_from(tls_key)?;

    if let Some(ed_signing_key) = ed_signing_key {
        let signature = ed_signing_key.sign(&key_pair.public_key_der());

        let mut ed_signature_value = Vec::new();
        ed_signature_value.extend_from_slice(ed_signing_key.verifying_key().as_bytes()); // 32 bytes
        ed_signature_value.extend_from_slice(&signature.to_bytes()); // 64 bytes

        params.custom_extensions.push(CustomExtension::from_oid_content(
            &ED_SIGNATURE_OID_PARTS,
            ed_signature_value.clone(),
        ));
    }
    let cert_der = params.self_signed(&key_pair)?.der().clone();
    Ok((tls_key.clone_key(), cert_der))
}

pub fn get_ed_pubkey_from_cert_der(
    cert: &CertificateDer<'static>,
) -> anyhow::Result<Option<ed25519_dalek::VerifyingKey>> {
    let (_, x509) = x509_parser::parse_x509_certificate(cert.as_ref())?;
    get_ed_pubkey_from_cert(&x509)
}

pub fn get_ed_pubkey_from_cert(
    cert: &x509_parser::certificate::X509Certificate,
) -> anyhow::Result<Option<ed25519_dalek::VerifyingKey>> {
    let Some(ext) = cert.extensions().iter().find(|e| e.oid == ED_SIGNATURE_OID) else {
        return Ok(None);
    };
    // Split: first 32 bytes = ed25519 pubkey, rest = signature
    let (pubkey_bytes, sig_bytes) = ext
        .value
        .split_at_checked(32)
        .ok_or_else(|| anyhow::anyhow!("Invalid ED_SIGNATURE certificate extension"))?;
    let ed_pub = ed25519_dalek::VerifyingKey::from_bytes(pubkey_bytes.try_into()?)?;
    let ed_sig = ed25519_dalek::Signature::from_bytes(sig_bytes.try_into()?);

    let tls_pub_der = cert.public_key().raw;
    ed_pub.verify(tls_pub_der, &ed_sig)?;

    Ok(Some(ed_pub))
}

pub fn verify_cert_or_ed_pubkey_is_trusted(
    cert_hash: &CertHash,
    ed_pub_key: &Option<ed25519_dalek::VerifyingKey>,
    trusted_cert_hashes: &HashSet<CertHash>,
    trusted_ed_pubkeys: &HashSet<ed25519_dalek::VerifyingKey>,
) -> bool {
    let is_trusted_cert_hash = || trusted_cert_hashes.contains(cert_hash);
    let is_trusted_ed_pub_key = || ed_pub_key.is_some_and(|x| trusted_ed_pubkeys.contains(&x));
    match (!trusted_cert_hashes.is_empty(), !trusted_ed_pubkeys.is_empty()) {
        (false, false) => true,
        (false, true) => is_trusted_cert_hash(),
        (true, false) => is_trusted_ed_pub_key(),
        (true, true) => is_trusted_cert_hash() || is_trusted_ed_pub_key(),
    }
}

pub fn verify_is_valid_cert(
    cert: &CertificateDer<'static>,
    trusted_cert_hashes: &HashSet<CertHash>,
    trusted_ed_pubkeys: &HashSet<crate::VerifyingKey>,
) -> bool {
    let Some((_, x509)) = x509_parser::parse_x509_certificate(cert.as_ref()).ok() else {
        return false;
    };

    if x509.verify_signature(None).is_err() {
        tracing::warn!("Failed to verify signature of certificate");
        return false;
    }

    match (!trusted_cert_hashes.is_empty(), !trusted_ed_pubkeys.is_empty()) {
        (false, false) => true,
        (false, true) => is_valid_with_trusted_ed_pub_keys(&x509, trusted_ed_pubkeys),
        (true, false) => is_valid_with_cert_hashes(cert, trusted_cert_hashes),
        (true, true) => {
            is_valid_with_trusted_ed_pub_keys(&x509, trusted_ed_pubkeys)
                || is_valid_with_cert_hashes(cert, trusted_cert_hashes)
        }
    }
}

fn is_valid_with_cert_hashes(
    cert: &CertificateDer<'static>,
    cert_hashes: &HashSet<CertHash>,
) -> bool {
    if cert_hashes.contains(&CertHash::from(cert)) {
        true
    } else {
        tracing::warn!("TLS certificated is not trusted");
        false
    }
}

fn is_valid_with_trusted_ed_pub_keys(
    cert: &x509_parser::certificate::X509Certificate,
    trusted_ed_pubkeys: &HashSet<crate::VerifyingKey>,
) -> bool {
    match get_ed_pubkey_from_cert(cert) {
        Ok(Some(pubkey)) => {
            if trusted_ed_pubkeys.contains(&pubkey) {
                true
            } else {
                tracing::warn!("TLS certificate has an untrusted ED pubkey");
                false
            }
        }
        Ok(None) => {
            tracing::warn!("TLS certificate is not signed with ED key");
            false
        }
        Err(err) => {
            tracing::warn!("TLS certificate has invalid ED signature: {}", err);
            false
        }
    }
}

pub fn build_pkcs12(credential: &NetCredential) -> anyhow::Result<Vec<u8>> {
    let p12_der = crate::pkcs12::Pfx::new(
        credential.my_certs[0].as_ref(),
        credential.my_key.secret_der(),
        None,
        "",
        "",
    )
    .ok_or_else(|| anyhow::anyhow!("Failed to build PFX"))?
    .to_der();
    Ok(p12_der)
}

pub fn resolve_signing_key(
    key_secret: Option<String>,
    key_path: Option<String>,
) -> anyhow::Result<Option<crate::SigningKey>> {
    match (key_secret, key_path) {
        (Some(_), Some(_)) => Err(anyhow::anyhow!(
            "Both key secret and key file are specified. Only one of them is allowed."
        )),
        (None, None) => Ok(None),
        (Some(secret), None) => Ok(Some(parse_signing_key(&secret)?)),
        (None, Some(path)) => {
            #[derive(Deserialize)]
            struct Key {
                secret: String,
            }
            let key = serde_json::from_slice::<Key>(&std::fs::read(path)?)?;
            Ok(Some(parse_signing_key(&key.secret)?))
        }
    }
}

fn parse_signing_key(s: &str) -> anyhow::Result<crate::SigningKey> {
    let bytes = <[u8; 32]>::try_from(hex::decode(s)?.as_slice())?;
    Ok(crate::SigningKey::from_bytes(&bytes))
}

#[derive(Clone)]
pub struct TlsCertCache(Arc<std::sync::Mutex<TlsCertCacheInner>>);

struct TlsCertCacheInner {
    default_key: PrivateKeyDer<'static>,
    certs: HashMap<Vec<u8>, (PrivateKeyDer<'static>, CertificateDer<'static>)>,
}

impl TlsCertCache {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self(Arc::new(std::sync::Mutex::new(TlsCertCacheInner {
            default_key: PrivateKeyDer::try_from(rcgen::KeyPair::generate()?.serialize_der())
                .map_err(|_| anyhow::anyhow!("Failed to generate TLS key"))?,
            certs: HashMap::new(),
        }))))
    }

    pub fn get_key_cert(
        &self,
        subjects: Option<Vec<String>>,
        tls_key: Option<&PrivateKeyDer<'static>>,
        ed_signing_key: Option<ed25519_dalek::SigningKey>,
    ) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
        let mut inner = self.0.lock().map_err(|_| anyhow::anyhow!("Failed to lock mutex"))?;
        let tls_key = tls_key.unwrap_or(&inner.default_key);

        let subjects = cert_subjects(subjects);
        let mut cache_key = Vec::new();
        for subject in &subjects {
            cache_key.extend_from_slice(subject.as_bytes());
        }
        cache_key.extend_from_slice(tls_key.secret_der());
        if let Some(ed_key) = &ed_signing_key {
            cache_key.extend_from_slice(ed_key.verifying_key().as_ref());
        }

        if let Some((key, cert)) = inner.certs.get(&cache_key) {
            return Ok((key.clone_key(), cert.clone()));
        }

        let (key, cert) = create_self_signed_cert_with_ed_signature(None, tls_key, ed_signing_key)?;
        inner.certs.insert(cache_key, (key.clone_key(), cert.clone()));
        Ok((key, cert))
    }
}

pub mod hex_verifying_key {
    use ed25519_dalek::VerifyingKey;
    use hex::encode;
    use hex::FromHex;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S>(key: &VerifyingKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex = encode(key.to_bytes()); // [u8; 32] → hex string
        serializer.serialize_str(&hex)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<VerifyingKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_str = <&str>::deserialize(deserializer)?;
        let bytes = <[u8; 32]>::from_hex(hex_str).map_err(serde::de::Error::custom)?;
        VerifyingKey::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

pub mod hex_verifying_keys {
    use std::fmt;

    use ed25519_dalek::VerifyingKey;
    use hex::encode;
    use hex::FromHex;
    use serde::de::SeqAccess;
    use serde::de::Visitor;
    use serde::ser::SerializeSeq;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S>(keys: &Vec<VerifyingKey>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(keys.len()))?;
        for key in keys {
            seq.serialize_element(&encode(key.to_bytes()))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<VerifyingKey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct KeyVisitor;

        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Vec<VerifyingKey>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a list of 64-character hex-encoded ed25519 public keys")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut keys = Vec::new();
                while let Some(hex_str) = seq.next_element::<String>()? {
                    let bytes = <[u8; 32]>::from_hex(&hex_str).map_err(serde::de::Error::custom)?;
                    let key = VerifyingKey::from_bytes(&bytes).map_err(serde::de::Error::custom)?;
                    keys.push(key);
                }
                Ok(keys)
            }
        }

        deserializer.deserialize_seq(KeyVisitor)
    }
}

#[test]
fn test_cert_validation() {
    let ed_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
    let (_key, cert) = generate_self_signed_cert(None, Some(ed_key.clone())).unwrap();

    assert!(verify_is_valid_cert(
        &cert,
        &HashSet::new(),
        &HashSet::from_iter([ed_key.verifying_key()].into_iter())
    ));
}
