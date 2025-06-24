// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;

use async_trait::async_trait;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::PrivatePkcs8KeyDer;
use sha2::Digest;
pub mod msquic;
pub mod server;
mod utils;
pub mod wtransport;

pub fn cert_hash(cert: &CertificateDer) -> [u8; 32] {
    sha2::Sha256::digest(cert).into()
}

pub struct NetCredential {
    pub my_key: PrivateKeyDer<'static>,
    pub my_certs: Vec<CertificateDer<'static>>,
    pub root_certs: Vec<CertificateDer<'static>>,
}

impl NetCredential {
    pub fn generate_self_signed() -> Self {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        Self {
            my_key: PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key.key_pair.serialize_der())),
            my_certs: vec![key.cert.der().clone()],
            root_certs: vec![],
        }
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
            root_certs: self.root_certs.clone(),
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
    type RecvRequest: NetRecvRequest;

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
    async fn accept_recv(&self) -> anyhow::Result<Self::RecvRequest>;
    async fn close(&self, code: usize);
    async fn watch_close(&self);
}

#[async_trait]
pub trait NetRecvRequest: Send + Sync {
    async fn recv(self) -> anyhow::Result<Vec<u8>>;
}
