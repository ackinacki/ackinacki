// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use wtransport::endpoint::IncomingSession;
use wtransport::Connection;
use wtransport::Endpoint;
use wtransport::Identity;
use wtransport::ServerConfig;

const DEFAULT_BROADCAST_CAPACITY: usize = 10;

#[derive(Debug, Clone)]
pub struct LiteServer {
    pub bind: SocketAddr,
}

impl LiteServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }

    pub async fn start(self, raw_block_receiver: Receiver<Vec<u8>>) -> anyhow::Result<()> {
        let (tx, rx) = std::sync::mpsc::channel::<IncomingSession>();
        let (btx, _ /* we will subscribe() later */) =
            broadcast::channel(DEFAULT_BROADCAST_CAPACITY);

        let server_handler: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            self.server(tx).await?;
            Ok(())
        });

        let session_handler: JoinHandle<anyhow::Result<()>> = {
            let btx = btx.clone();
            tokio::spawn(async move {
                sessions_handler(rx, btx).await?;
                Ok(())
            })
        };

        let multiplexer_handler: JoinHandle<anyhow::Result<()>> = {
            let btx = btx.clone();
            tokio::spawn(async move {
                message_multiplexor(raw_block_receiver, btx).await?;
                Ok(())
            })
        };

        tokio::select! {
            v = server_handler => v??,
            v = multiplexer_handler => v??,
            v = session_handler => v??,
        }

        Ok(())
    }

    async fn server(&self, session_sender: Sender<IncomingSession>) -> anyhow::Result<()> {
        let identity = Identity::self_signed(["0.0.0.0", "localhost", "127.0.0.1", "::1"])
            .expect("Fail identity generation");

        let config = ServerConfig::builder() //
            .with_bind_address(self.bind)
            .with_identity(&identity)
            .keep_alive_interval(Some(Duration::from_secs(3)))
            .build();

        let server = Endpoint::server(config)?;
        tracing::info!("LiteServer started on port {}", self.bind.port());

        loop {
            let incoming_session: IncomingSession = server.accept().await;
            tracing::info!("New session incoming");
            session_sender.send(incoming_session)?;
        }
    }
}

async fn sessions_handler(
    session_recv: Receiver<IncomingSession>,
    btx: broadcast::Sender<Vec<u8>>,
) -> anyhow::Result<()> {
    let logger_handle: JoinHandle<anyhow::Result<()>> = {
        let btx = btx.clone();

        tracing::info!("Prepare Starting broadcaster logger");
        tokio::spawn(async move {
            tracing::info!("Starting broadcaster logger");
            let mut brx = btx.subscribe();
            loop {
                match brx.recv().await {
                    Ok(msg) => {
                        tracing::info!("Received message from broadcast: {:?}", &msg[..10]);
                        tracing::info!("brx len {:?}", brx.len());
                    }
                    Err(err) => {
                        tracing::error!("Error receiving from broadcast: {}", err);
                        anyhow::bail!(err);
                    }
                }
            }
        })
    };

    let mut pool = FuturesUnordered::<JoinHandle<anyhow::Result<()>>>::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<JoinHandle<anyhow::Result<()>>>(20);

    pool.push(logger_handle);
    pool.push(tokio::spawn(async {
        // note: here we guarantie that pool won't stop if no errors accured
        loop {
            tokio::time::sleep(Duration::from_secs(100)).await;
        }
    }));

    pool.push(tokio::spawn(async move {
        loop {
            let incoming_session = session_recv.recv()?;
            tracing::info!("New session incoming received");
            let btx = btx.clone();
            tx.send(tokio::spawn(async move {
                tracing::info!("Starting session handler for incoming session");
                let incoming_request = incoming_session.await?;
                tracing::info!("Incoming request received");
                let connection = incoming_request.accept().await?;
                tracing::info!(
                    "Connection accepted {:?} {:?}",
                    connection.stable_id(),
                    connection.session_id(),
                );

                let mut brx = btx.subscribe();

                loop {
                    let data = brx.recv().await.map_err(|err| {
                        tracing::error!("brx err: {}", err);
                        err
                    })?;
                    tracing::info!("Received data len {:?}", data.len());
                    handle(&connection, &data).await?;
                }
            }))
            .await?;
        }
    }));

    loop {
        tokio::select! {
            v = rx.recv() => pool.push(v.ok_or_else(|| anyhow::anyhow!("channel was closed"))?),
            v = pool.select_next_some() => v??,
        }
    }
}

async fn handle(connection: &Connection, data: &[u8]) -> anyhow::Result<()> {
    tracing::trace!("Handling data len {:?}", data.len());
    let mut stream = connection.open_uni().await?.await?;
    stream.write_all(data).await?;
    stream.finish().await?;
    Ok(())
}

async fn message_multiplexor(
    rx: Receiver<Vec<u8>>,
    btx: broadcast::Sender<Vec<u8>>,
) -> anyhow::Result<()> {
    tracing::info!("Message multiplexor started");
    loop {
        match btx.send(rx.recv()?) {
            Ok(number_subscribers) => {
                tracing::info!("Message received by {} subs", number_subscribers);
            }
            Err(_err) => {
                // NOTE: this is not a real error: e.g. if there're no
            }
        }
    }
}
