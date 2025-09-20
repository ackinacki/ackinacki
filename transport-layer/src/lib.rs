// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::fmt::Display;
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

use crate::msquic::msquic_async::connection::StartError;
pub use crate::tls::create_self_signed_cert_with_ed_signatures;
pub use crate::tls::generate_self_signed_cert;
pub use crate::tls::get_pubkeys_from_cert_der;
pub use crate::tls::hex_verifying_key;
pub use crate::tls::hex_verifying_keys;
pub use crate::tls::resolve_signing_keys;
pub use crate::tls::verify_cert;
use crate::tls::verify_cert_hash_and_pubkeys;
pub use crate::tls::TlsCertCache;

pub mod msquic;
mod pkcs12;
pub mod quinn;
pub mod server;
mod tls;
mod utils;
pub mod wtransport;

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
pub struct CertHash(pub [u8; 32]);

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
    fn remote_certificate(&self) -> Option<CertificateDer<'static>>;
    fn alpn_negotiated(&self) -> Option<String>;
    fn alpn_negotiated_is(&self, protocol: &str) -> bool {
        self.alpn_negotiated().as_ref().map(|x| x == protocol).unwrap_or_default()
    }
    async fn send(&self, data: &[u8]) -> anyhow::Result<()>;
    async fn recv(&self) -> anyhow::Result<(Vec<u8>, Duration)>;
    async fn close(&self, code: usize);
    async fn watch_close(&self);
}
