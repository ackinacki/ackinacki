use std::sync::Arc;

use rustls_pki_types::CertificateDer;
use rustls_pki_types::ServerName;
use rustls_pki_types::UnixTime;
use wtransport::quinn::rustls;
use wtransport::quinn::rustls::client::danger::HandshakeSignatureValid;
use wtransport::quinn::rustls::client::danger::ServerCertVerified;
use wtransport::quinn::rustls::client::danger::ServerCertVerifier;
use wtransport::quinn::rustls::server::danger::ClientCertVerified;
use wtransport::quinn::rustls::server::danger::ClientCertVerifier;
use wtransport::quinn::rustls::server::WebPkiClientVerifier;
use wtransport::quinn::rustls::version::TLS13;
use wtransport::quinn::rustls::DigitallySignedStruct;
use wtransport::quinn::rustls::DistinguishedName;
use wtransport::quinn::rustls::Error;
use wtransport::quinn::rustls::SignatureScheme;
use wtransport::tls::rustls::RootCertStore;

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
    store.add_parsable_certificates(cred.root_certs.clone());
    store
}

pub fn create_client_config(
    is_debug: bool,
    credential: &NetCredential,
) -> Result<wtransport::ClientConfig, anyhow::Error> {
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
    tls_config.key_log = Arc::new(rustls::KeyLogFile::new());
    Ok(wtransport::ClientConfig::builder().with_bind_default().with_custom_tls(tls_config).build())
}

pub fn server_tls_config(
    is_debug: bool,
    credential: &NetCredential,
) -> anyhow::Result<rustls::ServerConfig> {
    let tls_config = if is_debug {
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

    Ok(tls_config)
}
