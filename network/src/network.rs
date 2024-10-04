// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::sleep;
use tracing::Instrument;

use crate::binchannel::BinReceiver;
use crate::binchannel::BinSender;
use crate::config::NetworkConfig;
use crate::config::NodeIdentifier;

pub static CERTIFICATE: &[u8] = include_bytes!("../certs/cert.der");
pub static PRIVATE_KEY: &[u8] = include_bytes!("../certs/key.der");

pub const BROADCAST_RETENTION_CAPACY: usize = 100;
const NODE_WAIT_FOR_ALIVE_MAX_ATTEMPTS: i32 = 20;
const NODE_WAIT_FOR_ALIVE_PAUSE_TIME_MILLIS: u64 = 50;

type BroadcastMessage = Arc<[u8]>;
type BroadcastReceiver = tokio::sync::broadcast::Receiver<BroadcastMessage>;
type BroadcastSender = tokio::sync::broadcast::Sender<BroadcastMessage>;

pub struct BasicNetwork {
    config: NetworkConfig,
}

impl BasicNetwork {
    pub fn from(config: NetworkConfig) -> Self {
        Self { config }
    }

    pub async fn start<T>(
        &self,
        listener_sender: Sender<T>,
        send_buffer_size: usize,
    ) -> anyhow::Result<(Sender<T>, Sender<(NodeIdentifier, T)>)>
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
    {
        tracing::info!("Starting network with configuration: {:?}", self.config);

        let (client_sender, client_receiver) = channel::<T>();
        let (one_node_sender, one_node_receiver) = channel::<(NodeIdentifier, T)>();

        // listen for incoming connections
        let bind = self.config.bind;
        tokio::spawn(async move {
            if let Err(e) = listener(bind, BinSender::from(listener_sender)).await {
                tracing::warn!("listener failed: {:?}", e);
            }
        });

        // listen for outgoing broadcasted messages
        let config = self.config.clone();
        tokio::spawn(async move {
            if let Err(e) =
                sender(BinReceiver::from(client_receiver), send_buffer_size, config).await
            {
                tracing::warn!("sender failed: {:?}", e);
            }
        });

        // listen for outgoing directed messages
        let config = self.config.clone();
        tokio::spawn(async move {
            if let Err(e) = single_sender(one_node_receiver, send_buffer_size, config).await {
                tracing::warn!("sender failed: {:?}", e);
            }
        });

        Ok((client_sender, one_node_sender))
    }
}

async fn listener<T>(bind: SocketAddr, server_sender: BinSender<T>) -> anyhow::Result<()>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send + Sync + 'static,
{
    let certs = vec![rustls::Certificate(CERTIFICATE.into())];
    let private_key = rustls::PrivateKey(PRIVATE_KEY.into());

    // NOTE: for the time of writing it was the most sane default
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)?;

    server_crypto.key_log = Arc::new(rustls::KeyLogFile::new());

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_crypto));

    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    // TODO: move to config
    transport_config.max_idle_timeout(Some(Duration::from_millis(100).try_into()?));
    // TODO: move to config
    transport_config.max_concurrent_uni_streams(0_u8.into());
    // TODO: move to config
    server_config.use_retry(true);

    // A QUIC endpoint.
    // An endpoint corresponds to a single UDP socket, may host many connections,
    // and may act as both client and server for different connections.
    let endpoint = quinn::Endpoint::server(server_config, bind)?;
    tracing::info!("listening on {}", endpoint.local_addr()?);

    while let Some(conn) = endpoint.accept().await {
        tracing::info!("connection incoming");
        let fut = handle_connection(conn, server_sender.clone());
        tokio::spawn(async move {
            if let Err(e) = fut.await {
                tracing::warn!("connection failed: {reason}", reason = e.to_string())
            }
        });
    }

    Ok(())
}

async fn handle_connection<T>(
    conn: quinn::Connecting,
    server_sender: BinSender<T>,
) -> anyhow::Result<()>
where
    T: for<'de> serde::Deserialize<'de> + Clone + Send + 'static,
{
    let connection = conn.await?;
    let span = tracing::info_span!(
        "connection",
        remote = %connection.remote_address(),
        protocol = %connection
            .handshake_data()
            .unwrap()
            .downcast::<quinn::crypto::rustls::HandshakeData>().unwrap()
            .protocol
            .map_or_else(|| "<none>".into(), |x| String::from_utf8_lossy(&x).into_owned())
    );
    async {
        tracing::info!("established");

        // Each stream initiated by the client constitutes a new request.
        loop {
            let stream = connection.accept_bi().await;
            let stream = match stream {
                Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                    tracing::info!("connection closed");
                    return Ok(());
                }
                Err(e) => {
                    return Err(e);
                }
                Ok(s) => s,
            };
            let (_, recv) = stream;
            let fut = handle_request(recv, server_sender.clone());
            tokio::spawn(
                async move {
                    if let Err(e) = fut.await {
                        tracing::warn!("failed: {reason}", reason = e.to_string());
                    }
                }
                .instrument(tracing::info_span!("request")),
            );
        }
    }
    .instrument(span)
    .await?;
    Ok(())
}

async fn handle_request<T>(
    mut recv: quinn::RecvStream,
    server_sender: BinSender<T>,
) -> anyhow::Result<()>
where
    T: for<'de> serde::Deserialize<'de>,
{
    let mut buf = Vec::with_capacity(2048);
    let _ = AsyncReadExt::read_to_end(&mut recv, &mut buf).await.inspect_err(|_| {
        tracing::info!("incomplete request");
    });
    tracing::info!("complete: msg. len: {:?}", &buf.len());
    server_sender.send(&buf)?;
    Ok(())
}

async fn sender<T>(
    client_receiver: BinReceiver<T>,
    send_buffer_size: usize,
    mut config: NetworkConfig,
) -> anyhow::Result<()>
where
    T: serde::Serialize + Send + 'static,
{
    assert!(send_buffer_size > 1);

    // tokio::task::spawn_blocking(move || {
    while let Ok(message) = client_receiver.recv() {
        let broadcast_sender = BroadcastSender::new(send_buffer_size / 2);

        let nodes = config
            .alive_nodes(true)
            .await
            .unwrap_or(Vec::from_iter(config.nodes.values().copied()));
        if !nodes.is_empty() {
            for node in nodes {
                let broadcast_receiver = broadcast_sender.subscribe();
                tokio::spawn(async move {
                    if let Err(e) = solo_sender(
                        node,
                        broadcast_receiver,
                        send_buffer_size - send_buffer_size / 2,
                    )
                    .await
                    {
                        tracing::warn!(
                            "failed to connect to {node}: {reason}",
                            node = node,
                            reason = e.to_string()
                        )
                    }
                });
            }

            tracing::trace!("network: send message to nodes");
            match broadcast_sender.send(message.into()) {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("send message to nodes failed: {e}");
                    return Ok(());
                }
            }
        }
    }
    // });
    Ok(())
}

async fn single_sender<T>(
    client_receiver: Receiver<(NodeIdentifier, T)>,
    send_buffer_size: usize,
    mut config: NetworkConfig,
) -> anyhow::Result<()>
where
    T: serde::Serialize + Send + 'static,
{
    assert!(send_buffer_size > 1);

    // tokio::task::spawn_blocking(move || {
    while let Ok((node_id, message)) = client_receiver.recv() {
        tracing::trace!("Send single message to node {}", node_id);
        let message = if let Ok(data) = bincode::serialize(&message) {
            data
        } else {
            tracing::warn!("Failed to serialize message");
            continue;
        };
        {
            let mut chitchat_guard = config.gossip.lock().await;
            tracing::trace!(
                "chitchat_guard {:?}",
                chitchat_guard.self_node_state().get("node_id").unwrap()
            );
            if node_id
                == NodeIdentifier::from_str(
                    chitchat_guard.self_node_state().get("node_id").unwrap(),
                )
                .expect("Failed to convert node id from str")
            {
                continue;
            }
        }

        let broadcast_sender = BroadcastSender::new(send_buffer_size / 2);

        // update alive nodes
        // node_id can be absent in the list of alive nodes. But if we want to send
        // message to it, we don't want to lose this message. So wait for this
        // node to appear in the list.
        let mut attempts_cnt = NODE_WAIT_FOR_ALIVE_MAX_ATTEMPTS;
        while attempts_cnt != 0 {
            attempts_cnt -= 1;
            let _alive = config.alive_nodes(true).await.unwrap_or_default();
            if let Some(node_address) = config.nodes.get(&node_id).copied() {
                let broadcast_receiver = broadcast_sender.subscribe();
                tokio::spawn(async move {
                    if let Err(e) = solo_sender(
                        node_address,
                        broadcast_receiver,
                        send_buffer_size - send_buffer_size / 2,
                    )
                    .await
                    {
                        tracing::warn!(
                            "failed to connect to {node}: {reason}",
                            node = node_address,
                            reason = e.to_string()
                        )
                    }
                });

                tracing::trace!("network: send message to node {} {}", node_id, node_address);
                match broadcast_sender.send(message.into()) {
                    Ok(_) => {
                        tracing::trace!(
                            "send message to node {} {} succeeded",
                            node_id,
                            node_address
                        );
                    }
                    Err(e) => {
                        tracing::warn!("send message to node failed: {e}");
                        return Ok(());
                    }
                }
                break;
            }
            sleep(Duration::from_millis(NODE_WAIT_FOR_ALIVE_PAUSE_TIME_MILLIS)).await;
        }
        assert_ne!(attempts_cnt, 0, "Node failed to send message to a particular node");
    }

    Ok(())
}

async fn solo_sender(
    addr: SocketAddr,
    mut receiver: BroadcastReceiver,
    send_buffer_size: usize,
) -> anyhow::Result<()> {
    tracing::trace!("node network mod received the message");
    let mut roots = rustls::RootCertStore::empty();
    roots.add(&rustls::Certificate(CERTIFICATE.into()))?;
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();

    client_crypto.key_log = Arc::new(rustls::KeyLogFile::new());

    let client_config = quinn::ClientConfig::new(Arc::new(client_crypto));
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;
    endpoint.set_default_client_config(client_config);

    tracing::info!("connecting to {addr}");
    let mut established_connection = None;

    let mut buffer = VecDeque::new();
    loop {
        if buffer.is_empty() {
            tracing::trace!("Start waiting for message");
            match receiver.recv().await {
                Ok(msg) => {
                    buffer.push_back(msg);
                }
                Err(RecvError::Closed) => {
                    tracing::trace!("Receiver was disconnected, stop execution");
                    return Ok(());
                }
                Err(RecvError::Lagged(skipped)) => {
                    tracing::trace!(
                        "network lagged err skipped={skipped} + buffer_len={}",
                        buffer.len()
                    );
                    buffer.clear();
                    continue;
                }
            }
            tracing::trace!("Stop waiting for message");
        }
        while buffer.len() < send_buffer_size {
            match receiver.try_recv() {
                Err(TryRecvError::Closed) => {
                    tracing::trace!("Receiver was disconnected");
                    // buffer can be not empty and we should send message
                    // return Ok(());
                    break;
                }
                Err(TryRecvError::Lagged(skipped)) => {
                    tracing::trace!(
                        "network lagged err skipped={skipped} + buffer_len={}",
                        buffer.len()
                    );
                    buffer.clear();
                    continue;
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
                Ok(msg) => {
                    buffer.push_back(msg);
                }
            };
        }
        if !buffer.is_empty() {
            tracing::trace!("Message buffer len: {}", buffer.len());
            if established_connection.is_none() {
                let connection = endpoint.connect(addr, "localhost"); // TODO: remove localhost {
                match connection {
                    Err(e) => {
                        tracing::warn!("network: Connection to {addr:?} refused: {e}");
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Ok(connection) => match connection.await {
                        Err(e) => {
                            tracing::warn!("network: Connection to {addr:?} failed: {e}");
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        Ok(connection) => {
                            tracing::trace!("network connection established to {addr:?}");
                            established_connection = Some(Arc::new(connection));
                        }
                    },
                }
            }
            let c = established_connection.clone().unwrap();
            match send_buffer(c, &mut buffer).await {
                Err(e) => {
                    tracing::warn!("network send buffer error: {e}");
                    established_connection = None;
                }
                Ok(()) => {
                    tracing::trace!("Buffer sent successfully");
                }
            };
        }
    }
}

async fn send_buffer(
    established_connection: Arc<quinn::Connection>,
    buffer: &mut VecDeque<BroadcastMessage>,
) -> anyhow::Result<()> {
    // for message in buffer.drain(..) {
    while !buffer.is_empty() {
        let message = &buffer[0];
        let connection = established_connection.clone();
        // tokio::spawn(async move {
        if let Err(e) = send_stream(connection, message).await {
            tracing::trace!("Failed to send data: {e}");
            return Err(e);
        } else {
            buffer.remove(0);
        }
        // });
    }

    Ok(())
}

async fn send_stream(
    established_connection: Arc<quinn::Connection>,
    data: &[u8],
) -> anyhow::Result<()> {
    tracing::trace!("Start sending data");
    let (mut send, _recv) = established_connection
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("failed to open stream: {}", e))?;

    // TODO: add this later to handle NAT
    //
    // if rebind {
    // let socket = std::net::UdpSocket::bind("[::]:0").unwrap();
    // let addr = socket.local_addr().unwrap();
    // eprintln!("rebinding to {addr}");
    // endpoint.rebind(socket).expect("rebind failed");
    // }

    send.write_all(data).await.map_err(|e| anyhow::anyhow!("failed to send request: {e}"))?;
    send.finish().await.map_err(|e| anyhow::anyhow!("failed to shutdown stream: {e}"))?;
    tracing::trace!("Finish sending data");
    Ok(())
}
