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
use msquic::StatusCode::QUIC_STATUS_BAD_CERTIFICATE;
use msquic_async::Connection;
use msquic_async::Listener;
use rustls_pki_types::CertificateDer;
use tokio::io::AsyncReadExt;

use crate::msquic::msquic_async::send_stream::SendStream;
use crate::msquic::msquic_async::stream::ReadStream;
use crate::msquic::quic_settings::ConfigFactory;
use crate::tls::pubkeys_info;
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
        let connect_result = conn.start(&config, &host, port).await;
        if let Err(msquic_async::connection::StartError::ConnectionLost(err)) = &connect_result {
            if let Some(cert_err) = conn.certificate_error() {
                return Err(anyhow::anyhow!(
                    "Peer ({}) certificate rejected: {}",
                    conn.get_remote_addr().map(|x| x.to_string()).unwrap_or_default(),
                    cert_err
                ));
            }
            return Err(resolve_bad_certificate(err, &conn));
        }
        connect_result?;
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
        let accept_result = self.connection.accept().await;
        if let Err(err) = &accept_result {
            if let Some(cert_err) = self.connection.certificate_error() {
                return Err(anyhow::anyhow!(
                    "Peer ({}) certificate rejected: {}",
                    self.connection.get_remote_addr().map(|x| x.to_string()).unwrap_or_default(),
                    cert_err
                ));
            }
            if let msquic_async::connection::StartError::ConnectionLost(err) = err {
                return Err(resolve_bad_certificate(err, &self.connection));
            }
        }
        accept_result?;
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
                    Ok(stream) => {
                        if let Err(msquic_async::stream::StartError::ConnectionLost(err)) = &stream
                        {
                            return Err(resolve_bad_certificate(err, &connection.inner));
                        }
                        stream?
                    }
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
            let result = connection.inner.accept_inbound_uni_stream().await;
            if let Err(msquic_async::stream::StartError::ConnectionLost(err)) = &result {
                return Err(resolve_bad_certificate(err, &connection.inner));
            }
            stream_lock.replace(result?);
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

    fn remote_certificate(&self) -> Option<CertificateDer<'static>> {
        self.inner.get_remote_certificate()
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
            write_buffer_to_stream(bytes, stream, self).await
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

async fn write_buffer_to_stream(
    bytes: &[u8],
    stream: &mut SendStream,
    connection: &MsQuicNetConnection,
) -> anyhow::Result<()> {
    let len = bytes.len() as u32;
    let mut encoded = Vec::with_capacity(4 + bytes.len());
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(bytes);
    match tokio::time::timeout(STREAM_OP_TIMEOUT, stream.send(encoded)).await {
        Ok(result) => {
            if let Err(msquic_async::send_stream::WriteError::ConnectionLost(err)) = &result {
                return Err(resolve_bad_certificate(err, &connection.inner));
            }
            result?
        }
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

fn resolve_bad_certificate(
    err: &msquic_async::connection::ConnectionError,
    connection: &Connection,
) -> anyhow::Error {
    if let msquic_async::connection::ConnectionError::ShutdownByTransport(status, _) = err {
        if status.0 == QUIC_STATUS_BAD_CERTIFICATE as u32 {
            let pubkeys = if let Some(cert) = connection.get_credential().my_certs.first() {
                crate::tls::get_pubkeys_from_cert_der(cert).unwrap_or_default()
            } else {
                vec![]
            };
            let peer_addr = connection.get_remote_addr().map(|x| x.to_string()).unwrap_or_default();
            return anyhow::anyhow!(
                "My certificate rejected by peer ({peer_addr}). My certificate signed by ED pubkeys [{}].",
                pubkeys_info(&pubkeys, 4)
            );
        }
    }
    err.clone().into()
}
