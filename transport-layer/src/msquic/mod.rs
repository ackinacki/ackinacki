pub mod msquic_async;
mod quic_settings;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use msquic::BufferRef;
use msquic::Registration;
use msquic::RegistrationConfig;
use msquic_async::Connection;
use msquic_async::Listener;
use tokio::io::AsyncReadExt;

use crate::msquic::msquic_async::send_stream::SendStream;
use crate::msquic::msquic_async::stream::ReadStream;
use crate::msquic::quic_settings::ConfigFactory;
use crate::CertHash;
use crate::NetConnection;
use crate::NetCredential;
use crate::NetIncomingRequest;
use crate::NetListener;
use crate::NetTransport;

const STREAM_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(12);

#[derive(Clone)]
pub struct MsQuicTransport {
    registration: Arc<RegistrationWrapper>,
}

struct RegistrationWrapper(Registration);

impl Drop for RegistrationWrapper {
    fn drop(&mut self) {
        self.0.shutdown();
    }
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
        Self { registration: Arc::new(RegistrationWrapper(registration)) }
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
        let local_identity = credential.identity();
        let config = ConfigFactory::Server.build(&self.registration.0, alpn, &credential)?;
        let listener = Listener::new(&self.registration.0, config, credential)?;
        let alpn: Vec<BufferRef> = alpn.iter().map(|s| BufferRef::from(*s)).collect();
        listener.start(&alpn, Some(bind_addr))?;
        Ok(MsQuicListener { instance: listener, local_identity })
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        alpn: &[&str],
        credential: NetCredential,
    ) -> anyhow::Result<Self::Connection> {
        let local_identity = credential.identity();
        let config = ConfigFactory::Client.build(&self.registration.0, alpn, &credential)?;
        let conn = Connection::new(&self.registration.0, credential)?;

        let host = addr.ip().to_string();
        let port = addr.port();
        conn.start(&config, &host, port).await?;
        let remote_cert = conn.receive_remote_certificate().await?;
        let remote_identity = CertHash::from(&remote_cert).to_string();
        Ok(MsQuicNetConnection::new(conn, local_identity, remote_identity))
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
        let remote_identity = CertHash::from(&remote_cert).to_string();
        Ok(MsQuicNetConnection::new(self.connection, self.local_identity, remote_identity))
    }
}

#[derive(Clone)]
pub struct MsQuicNetConnection {
    inner: Connection,
    local_identity: String,
    remote_identity: String,
    stream_pool: Arc<StreamPool>,
}

struct StreamPool {
    send: tokio::sync::Mutex<Option<SendStream>>,
    recv: tokio::sync::Mutex<Option<ReadStream>>,
}

impl StreamPool {
    fn new() -> Self {
        Self { send: tokio::sync::Mutex::new(None), recv: tokio::sync::Mutex::new(None) }
    }

    async fn acquire_send(
        &self,
        connection: &MsQuicNetConnection,
    ) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<SendStream>>> {
        let mut stream_lock = self.send.lock().await;
        if stream_lock.is_none() {
            let stream =
                match tokio::time::timeout(STREAM_OP_TIMEOUT, connection.inner.open_send_stream())
                    .await
                {
                    Ok(stream) => stream?,
                    Err(_) => {
                        anyhow::bail!("Timeout opening stream: took more {STREAM_OP_TIMEOUT:?}")
                    }
                };
            stream_lock.replace(stream);
        }
        Ok(stream_lock)
    }

    async fn acquire_recv(
        &self,
        connection: &MsQuicNetConnection,
    ) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<ReadStream>>> {
        let mut stream_lock = self.recv.lock().await;
        if stream_lock.is_none() {
            let stream = connection.inner.accept_inbound_uni_stream().await?;
            stream_lock.replace(stream);
        }
        Ok(stream_lock)
    }
}

impl MsQuicNetConnection {
    fn new(inner: Connection, local_identity: String, remote_identity: String) -> Self {
        Self { inner, local_identity, remote_identity, stream_pool: Arc::new(StreamPool::new()) }
    }
}

#[async_trait::async_trait]
impl NetConnection for MsQuicNetConnection {
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
        let mut stream = self.stream_pool.acquire_send(self).await?;
        let result = if let Some(stream) = stream.as_mut() {
            write_buffer_to_stream(bytes, stream).await
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
        let result = if let Some(stream) = stream.as_mut() {
            read_message_from_stream(stream).await
        } else {
            Err(anyhow::anyhow!("Failed to acquire recv stream"))
        };
        if result.is_err() {
            *stream = None;
        };
        result
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

async fn write_buffer_to_stream(bytes: &[u8], stream: &mut SendStream) -> anyhow::Result<()> {
    let len = bytes.len() as u32;
    let mut encoded = Vec::with_capacity(4 + bytes.len());
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(bytes);
    match tokio::time::timeout(STREAM_OP_TIMEOUT, stream.send(encoded)).await {
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
) -> anyhow::Result<(Vec<u8>, Duration)> {
    let mut len_buf = [0u8; 4];
    read_exact(stream, &mut len_buf).await?;
    let recv_time = Instant::now();
    let len = u32::from_be_bytes(len_buf);
    let mut buf = vec![0u8; len as usize];
    read_exact(stream, &mut buf).await?;
    Ok((buf, recv_time.elapsed()))
}

pub async fn read_exact<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut [u8],
) -> anyhow::Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        let read_len = stream.read(&mut buf[offset..]).await?;
        offset += read_len;
    }
    Ok(())
}
