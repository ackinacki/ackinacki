use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

use ed25519_dalek::Signer;
use ed25519_dalek::SigningKey;
use ed25519_dalek::Verifier;
use ed25519_dalek::VerifyingKey;
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
use sha2::Digest;
use x509_parser::nom::AsBytes;

use crate::msquic::msquic_async::connection::StartError;

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
pub struct CertHash(pub [u8; 32]);

impl CertHash {
    pub fn prefix(&self) -> String {
        hex::encode(&self.0[0..3])
    }
}

impl From<&CertificateDer<'static>> for CertHash {
    fn from(cert: &CertificateDer) -> Self {
        Self(sha2::Sha256::digest(cert).into())
    }
}

impl Display for CertHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

#[derive(PartialEq)]
pub struct NetCredential {
    pub my_key: PrivateKeyDer<'static>,
    pub my_certs: Vec<CertificateDer<'static>>,
    pub trusted_cert_hashes: HashSet<CertHash>,
    pub trusted_pubkeys: HashSet<VerifyingKey>,
}

impl NetCredential {
    pub fn generate_self_signed(
        subjects: Option<Vec<String>>,
        ed_signing_keys: &[SigningKey],
    ) -> anyhow::Result<Self> {
        let (my_key, my_cert) = generate_self_signed_cert(subjects, ed_signing_keys)?;
        Ok(Self {
            my_key,
            my_certs: vec![my_cert],
            trusted_cert_hashes: HashSet::new(),
            trusted_pubkeys: HashSet::new(),
        })
    }

    pub fn identity(&self) -> String {
        if self.my_certs.is_empty() {
            String::new()
        } else {
            CertHash::from(&self.my_certs[0]).to_string()
        }
    }

    pub fn identity_prefix(&self) -> String {
        if self.my_certs.is_empty() {
            String::new()
        } else {
            CertHash::from(&self.my_certs[0]).to_string().chars().take(4).collect()
        }
    }

    pub fn verify_cert(&self, cert: &CertificateDer<'static>) -> Result<(), StartError> {
        verify_cert(cert, &self.trusted_cert_hashes, &self.trusted_pubkeys)
    }

    pub fn verify_cert_hash_and_pubkeys(
        &self,
        hash: &CertHash,
        pubkeys: &[VerifyingKey],
    ) -> Result<(), StartError> {
        verify_cert_hash_and_pubkeys(
            hash,
            pubkeys,
            &self.trusted_cert_hashes,
            &self.trusted_pubkeys,
        )
    }

    pub fn my_cert_pubkeys(&self) -> anyhow::Result<HashSet<VerifyingKey>> {
        let mut pubkeys = HashSet::new();
        for cert in &self.my_certs {
            pubkeys.extend(get_pubkeys_from_cert_der(cert)?);
        }
        Ok(pubkeys)
    }
}

impl Clone for NetCredential {
    fn clone(&self) -> Self {
        Self {
            my_key: self.my_key.clone_key(),
            my_certs: self.my_certs.clone(),
            trusted_cert_hashes: self.trusted_cert_hashes.clone(),
            trusted_pubkeys: self.trusted_pubkeys.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct NoCertVerification;

fn supported_schemes() -> Vec<SignatureScheme> {
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
        supported_schemes()
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
        supported_schemes()
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
    ed_signing_keys: &[SigningKey],
) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
    let key_pair = rcgen::KeyPair::generate()?;
    let key = PrivateKeyDer::<'static>::try_from(key_pair.serialize_der())
        .map_err(|err| anyhow::anyhow!("Failed to generate TLS key: {err}"))?
        .clone_key();
    create_self_signed_cert_with_ed_signatures(subjects, &key, ed_signing_keys)
}

pub fn create_self_signed_cert_with_ed_signatures(
    subjects: Option<Vec<String>>,
    tls_key: &PrivateKeyDer<'static>,
    ed_signing_keys: &[SigningKey],
) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
    let mut params = CertificateParams::new(cert_subjects(subjects))?;
    let key_pair = rcgen::KeyPair::try_from(tls_key)?;

    let mut ext_value = Vec::new();
    for ed_signing_key in ed_signing_keys {
        let signature = ed_signing_key.sign(&key_pair.public_key_der());
        ext_value.extend_from_slice(ed_signing_key.verifying_key().as_bytes()); // 32 bytes
        ext_value.extend_from_slice(&signature.to_bytes()); // 64 bytes
    }
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(&ED_SIGNATURE_OID_PARTS, ext_value));
    let cert_der = params.self_signed(&key_pair)?.der().clone();
    Ok((tls_key.clone_key(), cert_der))
}

pub fn get_pubkeys_from_cert_der(
    cert: &CertificateDer<'static>,
) -> Result<Vec<VerifyingKey>, StartError> {
    let (_, x509) = x509_parser::parse_x509_certificate(cert.as_ref()).map_err(|err| {
        StartError::BadCertificate(format!("TLS certificate has malformed X509 data: {err}"))
    })?;
    get_pubkeys_from_cert(&x509)
}

pub fn get_pubkeys_from_cert(
    cert: &x509_parser::certificate::X509Certificate,
) -> Result<Vec<VerifyingKey>, StartError> {
    let Some(ext) = cert.extensions().iter().find(|e| e.oid == ED_SIGNATURE_OID) else {
        return Ok(vec![]);
    };
    let tls_pub_der = cert.public_key().raw;
    let mut pubkeys = Vec::new();
    let mut offset = 0;
    while ext.value.len() - offset >= 32 + 64 {
        let pubkey_bytes = &ext.value[offset..(offset + 32)];
        offset += 32;
        let sig_bytes = &ext.value[offset..(offset + 64)];
        offset += 64;
        let pubkey = VerifyingKey::from_bytes(pubkey_bytes.try_into().map_err(|err| {
            StartError::BadCertificate(format!("TLS certificate has malformed ED pubkey: {err}"))
        })?)
        .map_err(|err| {
            StartError::BadCertificate(format!("TLS certificate has malformed ED pubkey: {err}"))
        })?;
        let signature =
            ed25519_dalek::Signature::from_bytes(sig_bytes.try_into().map_err(|err| {
                StartError::BadCertificate(format!(
                    "TLS certificate has malformed ED signature: {err}"
                ))
            })?);

        pubkey.verify(tls_pub_der, &signature).map_err(|err| {
            StartError::BadCertificate(format!("TLS certificate has invalid ED signature: {err}"))
        })?;
        pubkeys.push(pubkey);
    }

    Ok(pubkeys)
}

pub fn verify_cert(
    cert: &CertificateDer<'static>,
    trusted_hashes: &HashSet<CertHash>,
    trusted_pubkeys: &HashSet<VerifyingKey>,
) -> Result<(), StartError> {
    let (_, x509) = x509_parser::parse_x509_certificate(cert.as_ref())
        .map_err(|err| StartError::BadCertificate(err.to_string()))?;
    x509.verify_signature(None).map_err(|err| StartError::BadCertificate(err.to_string()))?;
    let hash = CertHash::from(cert);
    let pubkeys = get_pubkeys_from_cert(&x509)?;
    verify_cert_hash_and_pubkeys(&hash, &pubkeys, trusted_hashes, trusted_pubkeys)
}

pub fn verify_cert_hash_and_pubkeys(
    hash: &CertHash,
    pubkeys: &[VerifyingKey],
    trusted_hashes: &HashSet<CertHash>,
    trusted_pubkeys: &HashSet<VerifyingKey>,
) -> Result<(), StartError> {
    match (as_option(trusted_hashes), as_option(trusted_pubkeys)) {
        (None, None) => Ok(()),
        (None, Some(trusted_pubkeys)) => verify_cert_pubkeys(pubkeys, trusted_pubkeys),
        (Some(trusted_hashes), None) => verify_cert_hash(hash, trusted_hashes),
        (Some(trusted_hashes), Some(trusted_pubkeys)) => {
            verify_cert_pubkeys(pubkeys, trusted_pubkeys).or_else(|pubkey_err| {
                if verify_cert_hash(hash, trusted_hashes).is_ok() {
                    Ok(())
                } else {
                    Err(pubkey_err)
                }
            })
        }
    }
}

fn as_option<T>(set: &HashSet<T>) -> Option<&HashSet<T>> {
    (!set.is_empty()).then_some(set)
}

fn verify_cert_hash(hash: &CertHash, trusted: &HashSet<CertHash>) -> Result<(), StartError> {
    if trusted.contains(hash) {
        Ok(())
    } else {
        Err(StartError::BadCertificate("TLS certificate hash is not trusted".to_string()))
    }
}

fn verify_cert_pubkeys(
    pubkeys: &[VerifyingKey],
    trusted: &HashSet<VerifyingKey>,
) -> Result<(), StartError> {
    if contains_any_pubkey(pubkeys, trusted) {
        return Ok(());
    }
    Err(StartError::BadCertificate(if !pubkeys.is_empty() {
        format!(
            "TLS certificate signed by untrusted ED pubkeys [{}]. Trusted pubkeys are [{}].",
            pubkeys_info(pubkeys, 4),
            pubkeys_info(trusted, 4)
        )
    } else {
        "TLS certificate is not signed with ED key".to_string()
    }))
}

pub fn pubkeys_info<'a>(pubkeys: impl IntoIterator<Item = &'a VerifyingKey>, len: usize) -> String {
    let mut s = String::new();
    for pubkey in pubkeys {
        if !s.is_empty() {
            s.push(',');
        }
        s.push_str(&hex::encode(&pubkey.as_bytes()[0..len]));
    }
    s
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

pub fn resolve_signing_keys(
    key_secrets: &[String],
    key_paths: &[String],
) -> anyhow::Result<Vec<SigningKey>> {
    let mut keys = Vec::new();
    for secret in key_secrets {
        keys.push(parse_signing_key(secret.as_str())?);
    }
    for path in key_paths {
        #[derive(Deserialize)]
        struct Key {
            secret: String,
        }
        let key = serde_json::from_slice::<Key>(&std::fs::read(path)?)?;
        keys.push(parse_signing_key(&key.secret)?);
    }
    Ok(keys)
}

fn parse_signing_key(s: &str) -> anyhow::Result<SigningKey> {
    let bytes = <[u8; 32]>::try_from(hex::decode(s)?.as_slice())?;
    Ok(SigningKey::from_bytes(&bytes))
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
        ed_signing_keys: &[SigningKey],
    ) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
        let mut inner = self.0.lock().map_err(|_| anyhow::anyhow!("Failed to lock mutex"))?;
        let tls_key = tls_key.unwrap_or(&inner.default_key);

        let subjects = cert_subjects(subjects);
        let mut cache_key = Vec::new();
        for subject in &subjects {
            cache_key.extend_from_slice(subject.as_bytes());
        }
        cache_key.extend_from_slice(tls_key.secret_der());
        for ed_key in ed_signing_keys {
            cache_key.extend_from_slice(ed_key.verifying_key().as_ref());
        }

        if let Some((key, cert)) = inner.certs.get(&cache_key) {
            return Ok((key.clone_key(), cert.clone()));
        }

        let (key, cert) =
            create_self_signed_cert_with_ed_signatures(None, tls_key, ed_signing_keys)?;
        inner.certs.insert(cache_key, (key.clone_key(), cert.clone()));
        Ok((key, cert))
    }
}

pub fn contains_any_pubkey(pubkeys: &[VerifyingKey], contains_in: &HashSet<VerifyingKey>) -> bool {
    pubkeys.iter().any(|x| contains_in.contains(x))
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
#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::VerifyingKey;
    use rustls_pki_types::CertificateDer;

    use crate::generate_self_signed_cert;
    use crate::msquic::msquic_async::connection::StartError;
    use crate::verify_cert;

    fn verify<const N: usize>(
        cert: &CertificateDer<'static>,
        trusted_pubkeys: [VerifyingKey; N],
    ) -> Result<(), StartError> {
        verify_cert(cert, &[].into(), &trusted_pubkeys.into())
    }

    fn gen_key() -> (SigningKey, VerifyingKey) {
        let k = SigningKey::generate(&mut rand::thread_rng());
        let v = k.verifying_key();
        (k, v)
    }

    fn gen_cert<const N: usize>(keys: [SigningKey; N]) -> CertificateDer<'static> {
        generate_self_signed_cert(None, &keys).unwrap().1
    }

    #[test]
    fn test_cert_validation() {
        let ((k1, v1), (k2, v2), (_, v3)) = (gen_key(), gen_key(), gen_key());

        let cert = gen_cert([k1.clone()]);
        assert!(verify(&cert, [v1]).is_ok());

        let cert = gen_cert([k1, k2]);
        assert!(verify(&cert, [v1, v2, v3]).is_ok());
        assert!(verify(&cert, [v1, v2]).is_ok());
        assert!(verify(&cert, [v1, v3]).is_ok());
        assert!(verify(&cert, [v2, v3]).is_ok());
        assert!(verify(&cert, [v1]).is_ok());
        assert!(verify(&cert, [v2]).is_ok());
        assert!(verify(&cert, [v3]).is_err());
    }
}
