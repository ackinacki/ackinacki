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
use rustls_pki_types::PrivatePkcs8KeyDer;
use rustls_pki_types::ServerName;
use rustls_pki_types::UnixTime;
use x509_parser::nom::AsBytes;

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

fn root_cert_store(cred: &NetCredential) -> RootCertStore {
    let mut store = RootCertStore::empty();
    store.add_parsable_certificates(cred.cert_validation.root_certs.clone());
    store
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

pub fn generate_self_signed_cert() -> (PrivateKeyDer<'static>, CertificateDer<'static>) {
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    (
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key.key_pair.serialize_der())),
        key.cert.der().clone(),
    )
}

const ED_SIGNATURE_OID_PARTS: [u64; 6] = [1, 2, 3, 4, 5, 6];
const ED_SIGNATURE_OID: x509_parser::der_parser::Oid =
    x509_parser::der_parser::oid!(1.2.3 .4 .5 .6);

pub fn generate_self_signed_cert_with_ed_signature(
    ed_signing_key: &ed25519_dalek::SigningKey,
) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
    create_self_signed_cert_with_ed_signature(&rcgen::KeyPair::generate()?, ed_signing_key)
}

pub fn create_self_signed_cert_with_ed_signature(
    tls_key: &rcgen::KeyPair,
    ed_signing_key: &ed25519_dalek::SigningKey,
) -> anyhow::Result<(PrivateKeyDer<'static>, CertificateDer<'static>)> {
    let signature = ed_signing_key.sign(&tls_key.public_key_der());

    let mut ed_signature_value = Vec::new();
    ed_signature_value.extend_from_slice(ed_signing_key.verifying_key().as_bytes()); // 32 bytes
    ed_signature_value.extend_from_slice(&signature.to_bytes()); // 64 bytes

    let mut params = CertificateParams::new(vec!["localhost".to_string()])?;
    params.custom_extensions.push(CustomExtension::from_oid_content(
        &ED_SIGNATURE_OID_PARTS,
        ed_signature_value.clone(),
    ));

    let cert_der = params.self_signed(tls_key)?.der().clone();
    let key_der = PrivateKeyDer::<'static>::from(PrivatePkcs8KeyDer::from(tls_key.serialize_der()));
    Ok((key_der, cert_der))
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

pub fn verify_is_valid_cert(
    cert: &CertificateDer,
    root_certs: &[CertificateDer],
    trusted_ed_pubkeys: &[[u8; 32]],
) -> bool {
    let Some((_, x509)) = x509_parser::parse_x509_certificate(cert.as_ref()).ok() else {
        return false;
    };

    if x509.verify_signature(None).is_err() {
        return false;
    }

    is_valid_with_root_certs(cert, root_certs)
        | is_valid_with_trusted_ed_pub_keys(&x509, trusted_ed_pubkeys)
}

fn is_valid_with_root_certs(cert: &CertificateDer, root_certs: &[CertificateDer]) -> bool {
    if root_certs.is_empty() {
        return true;
    }
    let Ok(end_entity) = webpki::EndEntityCert::try_from(cert.as_ref()) else {
        return false;
    };
    let now = webpki::Time::from_seconds_since_unix_epoch(
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
    );
    let anchors: Vec<webpki::TrustAnchor<'_>> = root_certs
        .iter()
        .filter_map(|rc| webpki::TrustAnchor::try_from_cert_der(rc.as_ref()).ok())
        .collect();
    let trust_anchors = webpki::TlsServerTrustAnchors(&anchors);
    end_entity
        .verify_is_valid_tls_server_cert_ext(
            &[&webpki::ECDSA_P256_SHA256, &webpki::ED25519],
            &trust_anchors,
            &[],
            now,
        )
        .is_ok()
}

fn is_valid_with_trusted_ed_pub_keys(
    cert: &x509_parser::certificate::X509Certificate,
    trusted_ed_pubkeys: &[[u8; 32]],
) -> bool {
    if trusted_ed_pubkeys.is_empty() {
        return true;
    }
    match get_ed_pubkey_from_cert(cert) {
        Ok(Some(pubkey)) => trusted_ed_pubkeys.contains(&pubkey.to_bytes()),
        Ok(None) => false,
        Err(_) => false,
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

#[test]
fn test_cert_validation() {
    let ed_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
    let (_key, cert) = generate_self_signed_cert_with_ed_signature(&ed_key).unwrap();

    assert!(verify_is_valid_cert(&cert, &[], &[ed_key.verifying_key().to_bytes()]));
}
