pub mod msquic_async;
mod pkcs12;
mod quic_settings;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;

use futures::AsyncWriteExt;
use msquic::BufferRef;
use msquic::Registration;
use msquic::RegistrationConfig;
use msquic_async::Connection;
use msquic_async::Listener;
use tokio::io::AsyncReadExt;

use crate::cert_hash;
use crate::msquic::msquic_async::stream::ReadStream;
use crate::msquic::msquic_async::stream::Stream;
use crate::msquic::quic_settings::ConfigFactory;
use crate::NetConnection;
use crate::NetCredential;
use crate::NetIncomingRequest;
use crate::NetListener;
use crate::NetRecvRequest;
use crate::NetTransport;

const STREAM_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(12);

#[derive(Clone)]
pub struct MsQuicTransport {
    registration: Arc<Registration>,
}

impl Default for MsQuicTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl MsQuicTransport {
    pub fn new() -> Self {
        let registration = Registration::new(&RegistrationConfig::default())
            .expect("Default registration is always possible");
        Self { registration: Arc::new(registration) }
    }
}

#[async_trait::async_trait]
impl NetTransport for MsQuicTransport {
    type Connection = MsQuicNetConnection;
    type Listener = MsQuicListener;

    async fn create_listener(
        &self,
        bind_addr: SocketAddr,
        alpn: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Listener> {
        let config = ConfigFactory::Server.build(&self.registration, alpn, &credential)?;
        let listener = Listener::new(&self.registration, config)?;
        let alpn: Vec<BufferRef> = alpn.iter().map(|s| BufferRef::from(*s)).collect();
        listener.start(&alpn, Some(bind_addr))?;
        Ok(MsQuicListener { instance: listener, local_identity: credential.identity() })
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        alpn: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Connection> {
        let config = ConfigFactory::Client.build(&self.registration, alpn, &credential)?;
        let conn = Connection::new(&self.registration)?;

        let host = addr.ip().to_string();
        let port = addr.port();
        conn.start(&config, &host, port).await?;
        let remote_cert = conn.receive_remote_certificate().await?;
        let remote_identity = hex::encode(cert_hash(&remote_cert));
        Ok(MsQuicNetConnection {
            inner: conn,
            local_identity: credential.identity(),
            remote_identity,
        })
    }
}

pub struct MsQuicListener {
    instance: Listener,
    local_identity: String,
}

#[async_trait::async_trait]
impl NetListener for MsQuicListener {
    type Connection = MsQuicNetConnection;
    type IncomingRequest = MsQuicNetIncomingRequest;

    async fn accept(&self) -> anyhow::Result<Self::IncomingRequest> {
        match self.instance.accept().await {
            Ok(connection) => Ok(MsQuicNetIncomingRequest {
                connection,
                local_identity: self.local_identity.clone(),
            }),
            Err(err) => anyhow::bail!("{:?}", err),
        }
    }
}

pub struct MsQuicNetIncomingRequest {
    connection: Connection,
    local_identity: String,
}

#[async_trait::async_trait]
impl NetIncomingRequest for MsQuicNetIncomingRequest {
    type Connection = MsQuicNetConnection;

    async fn accept(self) -> anyhow::Result<Self::Connection> {
        self.connection.accept().await?;
        let remote_cert = self.connection.receive_remote_certificate().await?;
        let remote_identity = hex::encode(cert_hash(&remote_cert));
        Ok(MsQuicNetConnection {
            inner: self.connection,
            local_identity: self.local_identity,
            remote_identity,
        })
    }
}

pub struct MsQuicNetRecvRequest {
    stream: ReadStream,
}

#[async_trait::async_trait]
impl NetRecvRequest for MsQuicNetRecvRequest {
    async fn recv(mut self) -> anyhow::Result<Vec<u8>> {
        let buf = read_message_from_stream(&mut self.stream).await?;
        Ok(buf)
    }
}

#[derive(Clone)]
pub struct MsQuicNetConnection {
    inner: Connection,
    local_identity: String,
    remote_identity: String,
}

#[async_trait::async_trait]
impl NetConnection for MsQuicNetConnection {
    type RecvRequest = MsQuicNetRecvRequest;

    fn local_addr(&self) -> SocketAddr {
        self.inner.get_local_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    fn remote_addr(&self) -> SocketAddr {
        self.inner.get_remote_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    fn local_identity(&self) -> String {
        self.local_identity.clone()
    }

    fn remote_identity(&self) -> String {
        self.remote_identity.clone()
    }

    fn alpn_negotiated(&self) -> Option<String> {
        self.inner.get_negotiated_alpn()
    }

    async fn send(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let mut stream = match tokio::time::timeout(
            STREAM_OP_TIMEOUT,
            self.inner.open_outbound_stream(msquic_async::StreamType::Unidirectional, false),
        )
        .await
        {
            Ok(stream) => stream?,
            Err(_) => anyhow::bail!("Timeout opening stream: took more {STREAM_OP_TIMEOUT:?}"),
        };
        write_buffer_to_stream(bytes, &mut stream).await?;
        match tokio::time::timeout(STREAM_OP_TIMEOUT, stream.flush()).await {
            Ok(result) => result?,
            Err(_) => anyhow::bail!(
                "Timeout flushing stream: took more {STREAM_OP_TIMEOUT:?}, {}",
                stream.debug_id(),
            ),
        }
        match tokio::time::timeout(STREAM_OP_TIMEOUT, stream.close()).await {
            Ok(result) => result?,
            Err(_) => anyhow::bail!(
                "Timeout closing stream: took more {STREAM_OP_TIMEOUT:?}, {}",
                stream.debug_id()
            ),
        }
        Ok(())
    }

    async fn accept_recv(&self) -> anyhow::Result<Self::RecvRequest> {
        let stream = self.inner.accept_inbound_uni_stream().await?;
        Ok(MsQuicNetRecvRequest { stream })
    }

    async fn close(&self, code: usize) {
        if let Err(err) = self.inner.shutdown(code as u64) {
            tracing::error!("Failed to shutdown connection: {}", err);
        }
    }

    async fn watch_close(&self) {
        self.inner.watch_shutdown().await;
    }
}

async fn write_buffer_to_stream(bytes: &[u8], stream: &mut Stream) -> anyhow::Result<()> {
    // Prepend data with its length in bytes
    let len = bytes.len() as u32;
    let mut encoded = Vec::with_capacity(4 + bytes.len());
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(bytes);
    match tokio::time::timeout(STREAM_OP_TIMEOUT, stream.write_all(&encoded)).await {
        Ok(result) => result?,
        Err(_) => anyhow::bail!(
            "Timeout writing stream: took more {STREAM_OP_TIMEOUT:?}, {}",
            stream.debug_id()
        ),
    }
    Ok(())
}

pub async fn read_message_from_stream<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> anyhow::Result<Vec<u8>> {
    let mut header = [0u8; 4];
    let mut header_len = 0;
    let mut decoded = Vec::new();
    let mut decoded_len = 0;
    loop {
        if header_len < 4 {
            header_len += stream.read(&mut header[header_len..]).await?;
        } else {
            if decoded.is_empty() {
                decoded.resize(u32::from_be_bytes(header) as usize, 0);
                if decoded.is_empty() {
                    return Ok(decoded);
                }
            }
            decoded_len += stream.read(&mut decoded[decoded_len..]).await?;
            if decoded_len == decoded.len() {
                // Drain the stream until EOF
                let mut drain = [0u8; 1024];
                while stream.read(&mut drain).await? != 0 {}
                return Ok(decoded);
            }
        }
    }
}
