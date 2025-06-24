mod tls;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use wtransport::endpoint::endpoint_side::Server;
use wtransport::endpoint::ConnectOptions;
use wtransport::endpoint::IncomingSession;
use wtransport::quinn::congestion::BbrConfig;
use wtransport::quinn::TransportConfig;
use wtransport::quinn::VarInt;
use wtransport::Connection;
use wtransport::Endpoint;
use wtransport::RecvStream;

use crate::wtransport::tls::create_client_config;
use crate::wtransport::tls::server_tls_config;
use crate::NetConnection;
use crate::NetCredential;
use crate::NetIncomingRequest;
use crate::NetListener;
use crate::NetRecvRequest;
use crate::NetTransport;

#[derive(Clone, Default)]
pub struct WTransport {
    debug_tls_mode: bool,
}

impl WTransport {
    pub fn with_debug_tls_mode(mut self, mode: bool) -> Self {
        self.debug_tls_mode = mode;
        self
    }
}

#[async_trait]
impl NetTransport for WTransport {
    type Connection = WTransportConnection;
    type Listener = WTransportListener;

    async fn create_listener(
        &self,
        bind_addr: SocketAddr,
        alpn_supported: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Listener> {
        let local_identity = credential.identity();

        let mut quic_transport_config = TransportConfig::default();
        quic_transport_config.max_concurrent_uni_streams(VarInt::from_u32(1_000));

        let tls_config = server_tls_config(self.debug_tls_mode, &credential)?;

        let mut server_config = wtransport::ServerConfig::builder()
            .with_bind_address(bind_addr)
            .with_custom_tls_and_transport(tls_config, quic_transport_config)
            .keep_alive_interval(Some(Duration::from_millis(1500)))
            .max_idle_timeout(Some(Duration::from_millis(3000)))?
            .build();

        server_config
            .quic_endpoint_config_mut()
            .max_udp_payload_size(65000)?
            .min_reset_interval(Duration::from_millis(2));

        let mut transport_config = TransportConfig::default();
        transport_config
            .congestion_controller_factory(Arc::new(BbrConfig::default()))
            .max_idle_timeout(Some(Duration::from_secs(10).try_into()?))
            .keep_alive_interval(Some(Duration::from_secs(2)))
            .send_window(2 * 1024 * 1024); // 2MB or tune lower
        server_config.quic_config_mut().transport_config(Arc::new(transport_config));

        let endpoint = wtransport::Endpoint::server(server_config)?;
        Ok(WTransportListener {
            endpoint,
            local_addr: bind_addr,
            local_identity,
            alpn_supported: alpn_supported.iter().map(|x| x.to_string()).collect(),
        })
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        alpn_preferred: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Connection> {
        let local_identity = credential.identity();
        let endpoint =
            wtransport::Endpoint::client(create_client_config(self.debug_tls_mode, &credential)?)?;
        let connection = endpoint
            .connect(
                ConnectOptions::builder(format!("https://{addr}"))
                    .add_header(ALPN_HEADER_NAME, alpn_preferred.join(",")),
            )
            .await?;
        Ok(WTransportConnection {
            connection,
            local_addr: endpoint.local_addr()?,
            local_identity,
            alpn_negotiated: None,
        })
    }
}

pub struct WTransportListener {
    endpoint: Endpoint<Server>,
    local_addr: SocketAddr,
    local_identity: String,
    alpn_supported: HashSet<String>,
}

#[async_trait]
impl NetListener for WTransportListener {
    type Connection = WTransportConnection;
    type IncomingRequest = WTransportIncomingRequest;

    async fn accept(&self) -> anyhow::Result<Self::IncomingRequest> {
        let request = self.endpoint.accept().await;
        Ok(WTransportIncomingRequest {
            request,
            local_addr: self.local_addr,
            local_identity: self.local_identity.clone(),
            alpn_supported: self.alpn_supported.clone(),
        })
    }
}

pub struct WTransportIncomingRequest {
    request: IncomingSession,
    local_addr: SocketAddr,
    local_identity: String,
    alpn_supported: HashSet<String>,
}

const ALPN_HEADER_NAME: &str = "Acki-Nacki-ALPN";

#[async_trait]
impl NetIncomingRequest for WTransportIncomingRequest {
    type Connection = WTransportConnection;

    async fn accept(self) -> anyhow::Result<Self::Connection> {
        let request = self.request.await?;
        let alpn_supported = self.alpn_supported;
        let alpn_negotiated = request.headers().get(ALPN_HEADER_NAME).and_then(|alpn_preferred| {
            alpn_preferred
                .split(',')
                .find(|x| !x.is_empty() && alpn_supported.contains(*x))
                .map(|x| x.to_string())
        });

        let connection = request.accept().await?;
        Ok(WTransportConnection {
            connection,
            local_addr: self.local_addr,
            local_identity: self.local_identity,
            alpn_negotiated,
        })
    }
}

pub struct WTransportRecvRequest {
    stream: RecvStream,
}

#[async_trait]
impl NetRecvRequest for WTransportRecvRequest {
    async fn recv(mut self) -> anyhow::Result<Vec<u8>> {
        let mut data = Vec::new();
        self.stream.read_to_end(&mut data).await?;
        Ok(data)
    }
}

#[derive(Clone)]
pub struct WTransportConnection {
    connection: Connection,
    local_addr: SocketAddr,
    local_identity: String,
    alpn_negotiated: Option<String>,
}

#[async_trait]
impl NetConnection for WTransportConnection {
    type RecvRequest = WTransportRecvRequest;

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn remote_addr(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    fn local_identity(&self) -> String {
        self.local_identity.clone()
    }

    fn remote_identity(&self) -> String {
        if let Some(identity) = self.connection.peer_identity() {
            if let Some(first_cert) = identity.as_slice().first() {
                return hex::encode(first_cert.hash().as_ref());
            }
        }
        self.remote_addr().to_string()
    }

    fn alpn_negotiated(&self) -> Option<String> {
        self.alpn_negotiated.clone()
    }

    async fn send(&self, data: &[u8]) -> anyhow::Result<()> {
        let mut stream = self.connection.open_uni().await?.await?;
        stream.write_all(data).await?;
        stream.finish().await?;
        Ok(())
    }

    async fn close(&self, code: usize) {
        self.connection.close((code as u32).into(), b"");
    }

    async fn accept_recv(&self) -> anyhow::Result<Self::RecvRequest> {
        let stream = self.connection.accept_uni().await?;
        Ok(WTransportRecvRequest { stream })
    }

    async fn watch_close(&self) {
        // todo!()
    }
}
