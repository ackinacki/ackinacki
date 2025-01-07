use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::time::Duration;

use anyhow::anyhow;
use anyhow::bail;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::version::TLS13;
use tokio::signal::unix::signal;
use tokio::signal::unix::SignalKind;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::info;
use tracing::warn;
use wtransport::endpoint::IncomingSession;
use wtransport::ClientConfig;
use wtransport::Connection;
use wtransport::Endpoint;

use super::cli::CliArgs;
use super::connection::ConnectionPool;
use super::connection::ConnectionWrapper;
use crate::config::serde::ConnectionConfig;
use crate::config::store::ConfigStore;
use crate::server::wtransport_server::quic_session_endpoint;

const DEFAULT_BROADCAST_CAPACITY: usize = 300;
const DEFAULT_OUTER_CONNECTION_TIMEOUT: Duration = Duration::from_millis(500);

pub async fn execute(args: CliArgs) -> anyhow::Result<()> {
    let config_store = ConfigStore::try_load(&args.config_path)?;
    let (config_pub, config_sub) = watch::channel(config_store);

    let (session_pub, session_sub) = channel();
    let (btx, _ /* we will subscribe() later */) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
    let (tx, rx) = channel::<Message>();

    // Spawn config reload handler
    let config_reload_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn({
        let config_path = args.config_path.clone();
        let config_pub = config_pub.clone();
        async move { config_reload_handler(config_pub, config_path).await }
    });

    info!("Starting Acki Nacki Proxy server");

    let receiver_handle = tokio::spawn({
        let bind = args.bind;
        let config_sub = config_sub.clone();
        async move { quic_session_endpoint(bind, session_pub, config_sub).await }
    });

    let session_handler_handle = tokio::spawn({
        let btx = btx.clone();
        let config_pub = config_pub.clone();
        async move { sessions_handler(session_sub, tx, btx, config_pub).await }
    });

    let multiplexer_handle = tokio::spawn({
        let btx = btx.clone();
        async move { message_multiplexor(rx, btx).await }
    });

    tokio::select! {
        v = receiver_handle => {
            if let Err(err) = v {
                tracing::error!("Receiver task stopped with error: {}", err);
            }
            anyhow::bail!("Receiver task stopped");
        }
        v = session_handler_handle => {
            tracing::error!("Session handler task stopped: {:?}", v);
            if let Err(err) = v {
                tracing::error!("Session handler task stopped with error: {}", err);
            }
            anyhow::bail!("Session handler task stopped");
        }
        v = multiplexer_handle => {
            if let Err(err) = v {
                tracing::error!("Multiplexer task stopped with error: {}", err);
            }
            anyhow::bail!("Multiplexer task stopped");
        }
        v = config_reload_handle => {
            if let Err(err) = v {
                tracing::error!("Config reload task stopped with error: {}", err);
            }
            anyhow::bail!("Config reload task stopped");
        }
    }
}

async fn config_reload_handler(
    config_pub: watch::Sender<ConfigStore>,
    config_path: PathBuf,
) -> anyhow::Result<()> {
    // 1. config error shouldn't be an error
    // 2. every other error should panic
    let mut sig_hup = signal(SignalKind::hangup())?;

    loop {
        sig_hup.recv().await;
        info!("Received SIGHUP, reloading configuration...");

        match ConfigStore::try_load(&config_path) {
            Ok(new_config_state) => {
                config_pub.send(new_config_state).expect("failed to send config")
            }
            Err(err) => {
                error!("Failed to load new configuration at {}: {:?}", config_path.display(), err);
            }
        };
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub connection_id: String,
    pub data: Vec<u8>,
}

async fn sessions_handler(
    session_recv: Receiver<IncomingSession>,
    incoming_msg_pub: Sender<Message>,
    btx: broadcast::Sender<Message>,
    config_pub: watch::Sender<ConfigStore>,
) -> anyhow::Result<()> {
    let mut config_sub = config_pub.subscribe();
    let config_state = config_sub.borrow_and_update().clone();

    let logger_handle: JoinHandle<anyhow::Result<()>> = {
        let btx = btx.clone();

        info!("Prepare Starting broadcaster logger");
        tokio::spawn(async move {
            info!("Starting broadcaster logger");
            let mut brx = btx.subscribe();
            loop {
                match brx.recv().await {
                    Ok(msg) => {
                        let msg_log =
                            if msg.data.len() > 10 { &msg.data[..10] } else { &msg.data[..] };
                        info!("Received message from broadcast: {:?}", msg_log);
                        info!("brx len {:?}", brx.len());
                    }
                    Err(err) => {
                        error!("Error receiving from broadcast: {}", err);
                        bail!(err);
                    }
                }
            }
        })
    };

    let mut connection_pool = ConnectionPool::new();

    let (connection_pool_pub, connection_pool_sub) = channel();

    let connection_pool_handler: JoinHandle<anyhow::Result<()>> = tokio::spawn({
        async move {
            loop {
                let (connection_wrapper, incoming_msg_pub, broadcast_sub) =
                    connection_pool_sub.recv().unwrap();
                connection_pool
                    .add_connection(connection_wrapper, incoming_msg_pub, broadcast_sub)
                    .await;
            }
        }
    });

    // connect to outers
    let outers_handerle: JoinHandle<anyhow::Result<()>> = tokio::spawn({
        let mut config_sub = config_pub.subscribe();
        let incoming_msg_pub = incoming_msg_pub.clone();
        let connection_pool_pub = connection_pool_pub.clone();
        let btx = btx.clone();
        async move {
            loop {
                info!("Starting outers handler");
                let config = config_sub.borrow_and_update().clone();
                let server_config = config.config.server.clone();

                for connection_config in config.out_connections() {
                    let Ok(connection) = connect_outer(&server_config, connection_config).await
                    else {
                        continue;
                    };
                    let Some(identity) = connection.peer_identity() else {
                        error!("Connection has no certificate chain: report it as a BUG");
                        continue;
                    };

                    let Some(first_cert) = identity.as_slice().first() else {
                        error!("Connection has no certificates: report it as a BUG");
                        continue;
                    };

                    let Some(name) = config.get_name_by_cert(first_cert) else {
                        error!("Connection has no name: report it as a BUG");
                        continue;
                    };
                    // try connect to outer
                    connection_pool_pub
                        .send((
                            ConnectionWrapper::new(name, connection_config.clone(), connection),
                            incoming_msg_pub.clone(),
                            btx.subscribe(),
                        ))
                        .unwrap();
                }
                tokio::time::sleep(DEFAULT_OUTER_CONNECTION_TIMEOUT).await;
            }
        }
    });

    let mut pool = FuturesUnordered::<JoinHandle<anyhow::Result<()>>>::new();
    let (_tx, mut rx) = tokio::sync::mpsc::channel::<JoinHandle<anyhow::Result<()>>>(200);

    pool.push(outers_handerle);
    pool.push(connection_pool_handler);
    pool.push(logger_handle);
    pool.push(tokio::spawn(async {
        // note: here we guarantie that pool won't stop if no errors accured
        loop {
            tokio::time::sleep(Duration::from_secs(100)).await;
        }
    }));

    let mut config_sub_clone = config_sub.clone();
    pool.push(tokio::spawn({
        let incoming_msg_pub = incoming_msg_pub.clone();
        let connection_pool_pub = connection_pool_pub.clone();
        let btx = btx.clone();
        async move {
            loop {
                let incoming_session = session_recv.recv()?;

                // TODO: make a function
                let Ok(session_request) = incoming_session.await else {
                    error!("Error incoming session");
                    continue;
                };

                let Ok(connection) = session_request.accept().await else {
                    error!("Error accepting connection");
                    continue;
                };

                let Some(identity) = connection.peer_identity() else {
                    error!("Connection has no certificate chain: report it as a BUG");
                    continue;
                };

                let Some(first_cert) = identity.as_slice().first() else {
                    error!("Connection has no certificates: report it as a BUG");
                    continue;
                };

                let Some(name) = config_state.get_name_by_cert(first_cert) else {
                    error!("Connection has no name: report it as a BUG");
                    continue;
                };

                let connection_state = config_sub_clone.borrow_and_update().clone();

                let Some(connection_config) = connection_state.config.connections.get(&name) else {
                    error!("name is not in config: report it as a BUG");
                    continue;
                };

                connection_pool_pub
                    .send((
                        ConnectionWrapper::new(name, connection_config.clone(), connection),
                        incoming_msg_pub.clone(),
                        btx.subscribe(),
                    ))
                    .unwrap();
            }
        }
    }));

    loop {
        tokio::select! {
            v = rx.recv() => pool.push(v.ok_or_else(|| anyhow!("channel was closed"))?),
            v = pool.select_next_some() => {
                match v {
                    Ok(v) => {
                        match v {
                            Err(err) => error!("Recoverable error: Session handler task stopped with error: {}", err),
                            Ok(_) => info!("Session handler task stopped"),
                        }
                    }
                    Err(err) => {
                        warn!("Recoverable error: Unable to join task with error: {}", err);
                    }
                }
            },
        }
        info!("pool len {}", pool.len());
    }
}

async fn connect_outer(
    server_config: &crate::config::serde::ServerConfig,
    config: &ConnectionConfig,
) -> anyhow::Result<Connection> {
    let endpoint = config
        .outer
        .clone()
        .ok_or_else(|| {
            anyhow!("expected outer config for outer prefiltered connection: suppose to be BUG")
        })?
        .url;

    // NOTE!!! reverse logic for outer connection:
    // client_cert is actually proxy_server public TLS certificate
    // and client_private_key is proxy_server private key
    let outer_cert: CertificateDer<'static> = config.cert.clone().into();
    let client_cert: CertificateDer<'static> = server_config.cert.clone().into();
    let private_key: PrivateKeyDer<'static> = server_config.key.clone().try_into()?;

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(outer_cert).unwrap();

    let tls_config = rustls::ClientConfig::builder_with_protocol_versions(&[&TLS13])
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_cert], private_key)
        .expect("tls config");

    let config = ClientConfig::builder() //
        .with_bind_default()
        .with_custom_tls(tls_config.clone())
        .build();

    let Ok(connection) = Endpoint::client(config) //
        .expect("endpoint client")
        .connect(endpoint)
        .await
    else {
        tracing::error!("connection error");
        return Err(anyhow!("connection error"));
    };
    Ok(connection)
}

async fn message_multiplexor(
    rx: Receiver<Message>,
    btx: broadcast::Sender<Message>,
) -> anyhow::Result<()> {
    info!("Message multiplexor started");
    loop {
        match btx.send(rx.recv()?) {
            Ok(number_subscribers) => {
                tracing::debug!(
                    "Internal broadcast: message received by {} subs",
                    number_subscribers
                );
            }
            Err(_err) => {
                // NOTE: this is not a real error: e.g. if there're no receivers
            }
        }
    }
}
