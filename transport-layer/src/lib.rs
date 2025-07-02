// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use sha2::Digest;

use crate::tls::generate_self_signed_cert;
use crate::tls::generate_self_signed_cert_with_ed_signature;
use crate::tls::verify_is_valid_cert;

pub mod msquic;
mod pkcs12;
pub mod quinn;
pub mod server;
mod tls;
mod utils;
pub mod wtransport;

pub fn cert_hash(cert: &CertificateDer) -> [u8; 32] {
    sha2::Sha256::digest(cert).into()
}

#[derive(Default, Clone)]
pub struct CertValidation {
    pub root_certs: Vec<CertificateDer<'static>>,
    pub trusted_ed_pubkeys: Vec<[u8; 32]>,
}

impl CertValidation {
    pub fn is_empty(&self) -> bool {
        self.root_certs.is_empty() && self.trusted_ed_pubkeys.is_empty()
    }

    pub fn verify_is_valid_cert(&self, cert: &CertificateDer) -> bool {
        self.is_empty() | verify_is_valid_cert(cert, &self.root_certs, &self.trusted_ed_pubkeys)
    }
}

pub struct NetCredential {
    pub my_key: PrivateKeyDer<'static>,
    pub my_certs: Vec<CertificateDer<'static>>,
    pub cert_validation: CertValidation,
}

impl NetCredential {
    pub fn generate_self_signed() -> Self {
        let (my_key, my_cert) = generate_self_signed_cert();
        Self { my_key, my_certs: vec![my_cert], cert_validation: CertValidation::default() }
    }

    pub fn generate_self_signed_with_ed_signature(
        ed_signing_key: &ed25519_dalek::SigningKey,
    ) -> anyhow::Result<Self> {
        let (my_key, my_cert) = generate_self_signed_cert_with_ed_signature(ed_signing_key)?;
        Ok(Self { my_key, my_certs: vec![my_cert], cert_validation: CertValidation::default() })
    }

    pub fn identity(&self) -> String {
        if self.my_certs.is_empty() {
            String::new()
        } else {
            hex::encode(cert_hash(&self.my_certs[0]))
        }
    }
}

impl Clone for NetCredential {
    fn clone(&self) -> Self {
        Self {
            my_key: self.my_key.clone_key(),
            my_certs: self.my_certs.clone(),
            cert_validation: self.cert_validation.clone(),
        }
    }
}

#[async_trait]
pub trait NetTransport: Clone + Send + Sync {
    type Connection: NetConnection;
    type Listener: NetListener<Connection = Self::Connection>;
    async fn create_listener(
        &self,
        bind_addr: SocketAddr,
        alpn_supported: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Listener>;
    async fn connect(
        &self,
        addr: SocketAddr,
        alpn_preferred: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Connection>;
}

#[async_trait]
pub trait NetListener: Send + Sync {
    type Connection: NetConnection;
    type IncomingRequest: NetIncomingRequest<Connection = Self::Connection>;
    async fn accept(&self) -> anyhow::Result<Self::IncomingRequest>;
}

#[async_trait]
pub trait NetIncomingRequest: Send + Sync {
    type Connection: NetConnection;
    async fn accept(self) -> anyhow::Result<Self::Connection>;
}

#[async_trait]
pub trait NetConnection: Clone + Send + Sync {
    fn local_addr(&self) -> SocketAddr;
    fn remote_addr(&self) -> SocketAddr;
    // Returns unique endpoint identification (usually it is hash of the TLS certificate)
    fn local_identity(&self) -> String;
    // Returns unique remote endpoint identification (usually it is hash of the TLS certificate)
    fn remote_identity(&self) -> String;
    fn alpn_negotiated(&self) -> Option<String>;
    fn alpn_negotiated_is(&self, protocol: &str) -> bool {
        self.alpn_negotiated().as_ref().map(|x| x == protocol).unwrap_or_default()
    }
    async fn send(&self, data: &[u8]) -> anyhow::Result<()>;
    async fn recv(&self) -> anyhow::Result<(Vec<u8>, Duration)>;
    async fn close(&self, code: usize);
    async fn watch_close(&self);
}
