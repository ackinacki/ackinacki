// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
pub use ed25519_dalek::SecretKey;
pub use ed25519_dalek::Signature;
pub use ed25519_dalek::SigningKey;
pub use ed25519_dalek::VerifyingKey;
pub use rcgen::KeyPair;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use sha2::Digest;

pub use crate::tls::create_self_signed_cert_with_ed_signature;
pub use crate::tls::generate_self_signed_cert;
pub use crate::tls::hex_verifying_key;
pub use crate::tls::hex_verifying_keys;
pub use crate::tls::resolve_signing_key;
pub use crate::tls::verify_is_valid_cert;
pub use crate::tls::TlsCertCache;

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

pub struct NetCredential {
    pub my_key: PrivateKeyDer<'static>,
    pub my_certs: Vec<CertificateDer<'static>>,
    pub trusted_certs: Vec<CertificateDer<'static>>,
    pub trusted_ed_pubkeys: Vec<VerifyingKey>,
}

impl NetCredential {
    pub fn generate_self_signed(
        subjects: Option<Vec<String>>,
        ed_signing_key: Option<SigningKey>,
    ) -> anyhow::Result<Self> {
        let (my_key, my_cert) = generate_self_signed_cert(subjects, ed_signing_key)?;
        Ok(Self {
            my_key,
            my_certs: vec![my_cert],
            trusted_certs: vec![],
            trusted_ed_pubkeys: vec![],
        })
    }

    pub fn identity(&self) -> String {
        if self.my_certs.is_empty() {
            String::new()
        } else {
            hex::encode(cert_hash(&self.my_certs[0]))
        }
    }

    pub fn identity_prefix(&self) -> String {
        if self.my_certs.is_empty() {
            String::new()
        } else {
            hex::encode(cert_hash(&self.my_certs[0]).iter().take(4).cloned().collect::<Vec<_>>())
        }
    }

    pub fn any_cert_are_trusted(&self) -> bool {
        self.trusted_certs.is_empty() && self.trusted_ed_pubkeys.is_empty()
    }

    pub fn verify_is_valid_cert(&self, cert: &CertificateDer) -> bool {
        self.any_cert_are_trusted()
            | verify_is_valid_cert(cert, &self.trusted_certs, &self.trusted_ed_pubkeys)
    }
}

impl Clone for NetCredential {
    fn clone(&self) -> Self {
        Self {
            my_key: self.my_key.clone_key(),
            my_certs: self.my_certs.clone(),
            trusted_certs: self.trusted_certs.clone(),
            trusted_ed_pubkeys: self.trusted_ed_pubkeys.clone(),
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
