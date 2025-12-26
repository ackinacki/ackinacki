use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use rustls_pki_types::CertificateDer;

use crate::NetCredential;

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
    fn remote_addr(&self) -> anyhow::Result<SocketAddr>;
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
