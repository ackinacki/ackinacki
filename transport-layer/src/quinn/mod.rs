use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use rustls_pki_types::CertificateDer;

use crate::tls::client_tls_config;
use crate::tls::server_tls_config;
use crate::NetConnection;
use crate::NetCredential;
use crate::NetIncomingRequest;
use crate::NetListener;
use crate::NetTransport;

const STREAM_OP_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Clone)]
pub struct QuinnTransport;

impl Default for QuinnTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl QuinnTransport {
    pub fn new() -> Self {
        Self {}
    }
}

fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport
        .initial_rtt(Duration::from_millis(2))
        .max_idle_timeout(None)
        .keep_alive_interval(Some(Duration::from_millis(500)))
        .send_window(2_147_483_648)
        .receive_window(268_435_456u32.into())
        .stream_receive_window(268_435_456u32.into());
    Arc::new(transport)
}

#[async_trait]
impl NetTransport for QuinnTransport {
    type Connection = QuinnConnection;
    type Listener = QuinnListener;

    async fn create_listener(
        &self,
        bind_addr: SocketAddr,
        alpn_supported: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Listener> {
        let tls_config = server_tls_config(true, &credential, alpn_supported)?;
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(transport_config());
        let endpoint = quinn::Endpoint::server(config, bind_addr)?;
        Ok(QuinnListener { endpoint })
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        alpn_preferred: &[&str],
        cred: NetCredential,
    ) -> anyhow::Result<Self::Connection> {
        let tls_config = client_tls_config(true, &cred, alpn_preferred)?;
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)?;
        let mut client_cfg = quinn::ClientConfig::new(Arc::new(crypto));
        client_cfg.transport_config(transport_config());
        let endpoint = quinn::Endpoint::client(([0, 0, 0, 0], 0).into())?;
        let conn = endpoint.connect_with(client_cfg, addr, "localhost")?.await?;
        Ok(QuinnConnection::from_connection(conn, endpoint.local_addr()?))
    }
}

pub struct QuinnListener {
    endpoint: quinn::Endpoint,
}

#[async_trait]
impl NetListener for QuinnListener {
    type Connection = QuinnConnection;
    type IncomingRequest = QuinnIncomingRequest;

    async fn accept(&self) -> anyhow::Result<Self::IncomingRequest> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| anyhow::anyhow!("quinn connection closed"))?;
        Ok(QuinnIncomingRequest { incoming, local_addr: self.endpoint.local_addr()? })
    }
}

pub struct QuinnIncomingRequest {
    local_addr: SocketAddr,
    incoming: quinn::Incoming,
}

#[async_trait]
impl NetIncomingRequest for QuinnIncomingRequest {
    type Connection = QuinnConnection;

    fn remote_addr(&self) -> anyhow::Result<SocketAddr> {
        Ok(self.incoming.remote_address())
    }

    async fn accept(self) -> anyhow::Result<Self::Connection> {
        let connecting = self.incoming.accept()?;
        let connection = connecting.await?;
        Ok(QuinnConnection::from_connection(connection, self.local_addr))
    }
}

#[derive(Clone)]
pub struct QuinnConnection {
    local_addr: SocketAddr,
    inner: quinn::Connection,
    stream_pool: Arc<StreamPool>,
}

impl QuinnConnection {
    pub fn from_connection(inner: quinn::Connection, local_addr: SocketAddr) -> Self {
        Self { inner, local_addr, stream_pool: Arc::new(StreamPool::new()) }
    }
}

#[async_trait]
impl NetConnection for QuinnConnection {
    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn remote_addr(&self) -> SocketAddr {
        self.inner.remote_address()
    }

    fn local_identity(&self) -> String {
        "local".into()
    }

    fn remote_identity(&self) -> String {
        "remote".into()
    }

    fn remote_certificate(&self) -> Option<CertificateDer<'static>> {
        None
    }

    fn alpn_negotiated(&self) -> Option<String> {
        self.inner
            .handshake_data()?
            .downcast_ref::<quinn::crypto::rustls::HandshakeData>()?
            .protocol
            .as_ref()
            .map(|p| String::from_utf8_lossy(p).into_owned())
    }

    async fn send(&self, data: &[u8]) -> anyhow::Result<()> {
        let mut stream = self.stream_pool.acquire_send(self).await?;
        let result = if let Some(stream) = stream.as_mut() {
            write_buffer_to_stream(data, stream).await
        } else {
            Err(anyhow::anyhow!("Unexpectedly missing send stream"))
        };
        if result.is_err() {
            *stream = None;
        }
        result
    }

    async fn recv(&self) -> anyhow::Result<(Vec<u8>, Duration)> {
        let mut stream = self.stream_pool.acquire_recv(self).await?;
        let recv_time = Instant::now();
        let result = if let Some(stream) = stream.as_mut() {
            read_message_from_stream(stream).await
        } else {
            Err(anyhow::anyhow!("Failed to acquire recv stream"))
        };
        if result.is_err() {
            *stream = None;
        };
        result.map(|x| (x, recv_time.elapsed()))
    }

    async fn close(&self, code: usize) {
        self.inner.close((code as u32).into(), b"");
    }

    async fn watch_close(&self) {
        self.inner.closed().await;
    }
}

struct StreamPool {
    send: tokio::sync::Mutex<Option<quinn::SendStream>>,
    recv: tokio::sync::Mutex<Option<quinn::RecvStream>>,
}

impl StreamPool {
    fn new() -> Self {
        Self { send: tokio::sync::Mutex::new(None), recv: tokio::sync::Mutex::new(None) }
    }

    async fn acquire_send(
        &self,
        connection: &QuinnConnection,
    ) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<quinn::SendStream>>> {
        let mut stream_lock = self.send.lock().await;
        if stream_lock.is_none() {
            let stream = match tokio::time::timeout(STREAM_OP_TIMEOUT, connection.inner.open_uni())
                .await
            {
                Ok(stream) => stream?,
                Err(_) => anyhow::bail!("Timeout opening stream: took more {STREAM_OP_TIMEOUT:?}"),
            };
            stream_lock.replace(stream);
        }
        Ok(stream_lock)
    }

    async fn acquire_recv(
        &self,
        connection: &QuinnConnection,
    ) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<quinn::RecvStream>>> {
        let mut stream_lock = self.recv.lock().await;
        if stream_lock.is_none() {
            let stream = match tokio::time::timeout(
                STREAM_OP_TIMEOUT,
                connection.inner.accept_uni(),
            )
            .await
            {
                Ok(stream) => stream?,
                Err(_) => {
                    anyhow::bail!("Timeout opening recv stream: took more {STREAM_OP_TIMEOUT:?}")
                }
            };
            stream_lock.replace(stream);
        }
        Ok(stream_lock)
    }
}

async fn write_buffer_to_stream(
    bytes: &[u8],
    stream: &mut quinn::SendStream,
) -> anyhow::Result<()> {
    let len = bytes.len() as u32;
    let mut encoded = Vec::with_capacity(4 + bytes.len());
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(bytes);
    match tokio::time::timeout(STREAM_OP_TIMEOUT, stream.write_all(&encoded)).await {
        Ok(result) => result?,
        Err(_) => anyhow::bail!(
            "Timeout writing stream: took more {STREAM_OP_TIMEOUT:?}, {}",
            stream.id()
        ),
    }
    Ok(())
}

pub async fn read_message_from_stream(stream: &mut quinn::RecvStream) -> anyhow::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}
