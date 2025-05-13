use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::HandshakeSignatureValid;
use rustls::client::danger::ServerCertVerified;
use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use rustls::pki_types::UnixTime;
use rustls::server::danger::ClientCertVerified;
use rustls::server::danger::ClientCertVerifier;
use rustls::server::WebPkiClientVerifier;
use rustls::version::TLS13;
use rustls::DigitallySignedStruct;
use rustls::DistinguishedName;
use rustls::Error;
use rustls::SignatureScheme;
use serde::Deserialize;
use serde::Serialize;
use wtransport::ServerConfig;

use crate::host_id_prefix;
use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::PrivateKeyFile;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TlsConfig {
    pub my_cert: CertFile,
    pub my_key: PrivateKeyFile,
    pub peer_certs: CertStore,
}

impl TlsConfig {
    pub fn is_debug(&self) -> bool {
        self.my_cert.is_debug()
    }

    pub fn my_host_id(&self) -> String {
        self.my_cert.resolve_host_id(&self.my_key)
    }

    pub fn my_host_id_prefix(&self) -> String {
        host_id_prefix(&self.my_cert.resolve_host_id(&self.my_key)).to_string()
    }
}

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

pub fn create_client_config(config: &TlsConfig) -> Result<wtransport::ClientConfig, anyhow::Error> {
    let root_certs = CertStore::build_root_cert_store(&config.peer_certs)?;
    let (my_certs, my_key) = config.my_cert.resolve(&config.my_key)?;
    let mut tls_config = if config.peer_certs.is_debug() {
        rustls::ClientConfig::builder_with_protocol_versions(&[&TLS13])
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerification))
            .with_client_auth_cert(my_certs, my_key)?
    } else {
        rustls::ClientConfig::builder_with_protocol_versions(&[&TLS13])
            .with_root_certificates(root_certs)
            .with_client_auth_cert(my_certs, my_key)?
    };
    tls_config.key_log = Arc::new(rustls::KeyLogFile::new());
    Ok(wtransport::ClientConfig::builder().with_bind_default().with_custom_tls(tls_config).build())
}

const PROXY_CONNECTION_ALIVE_TIMEOUT: Duration = Duration::from_millis(300);

pub fn server_tls_config(config: &TlsConfig) -> anyhow::Result<rustls::ServerConfig> {
    let (my_certs, my_key) = config.my_cert.resolve(&config.my_key)?;

    let root_store = Arc::new(config.peer_certs.build_root_cert_store()?);

    let tls_config = if config.is_debug() {
        rustls::ServerConfig::builder_with_protocol_versions(&[&TLS13])
            .with_client_cert_verifier(Arc::new(NoCertVerification))
            .with_single_cert(my_certs, my_key)?
    } else {
        rustls::ServerConfig::builder_with_protocol_versions(&[&TLS13])
            .with_client_cert_verifier(WebPkiClientVerifier::builder(root_store).build()?)
            .with_single_cert(my_certs, my_key)?
    };

    Ok(tls_config)
}

pub fn generate_server_config(
    bind: SocketAddr,
    config: &TlsConfig,
) -> anyhow::Result<ServerConfig> {
    Ok(ServerConfig::builder()
        .with_bind_address(bind)
        .with_custom_tls(server_tls_config(config)?)
        .keep_alive_interval(Some(PROXY_CONNECTION_ALIVE_TIMEOUT))
        .max_idle_timeout(Some(PROXY_CONNECTION_ALIVE_TIMEOUT * 2))?
        .build())
}
