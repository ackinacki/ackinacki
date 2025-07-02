// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;

use anyhow::Context;
use database::sqlite::sqlite_helper;
use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::sqlite_helper::SqliteHelperConfig;
use node::bls::envelope::BLSSignedEnvelope;
use node::bls::envelope::Envelope;
use node::bls::GoshBLS;
use node::types::AckiNackiBlock;
use rusqlite::Connection;
use transport_layer::msquic::MsQuicTransport;
use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetTransport;
use tvm_block::ShardStateUnsplit;

use crate::events::Event;

pub struct BlockSubscriber {
    db_file: PathBuf,
    socket_addr: SocketAddr,
    event_pub: Sender<Event>,
    bp_data_tx: Sender<(String, Vec<String>)>,
    // archive: Arc<dyn DocumentsDb>,
    // TODO: more fields related to cache of blocks
}

impl BlockSubscriber {
    /// # Panics
    ///
    /// Panics if the database file cannot be opened or created.
    pub fn new(
        db_file: PathBuf,
        socket_addr: SocketAddr,
        event_pub: Sender<Event>,
        bp_data_tx: Sender<(String, Vec<String>)>,
        // archive: Arc<dyn DocumentsDb>,
    ) -> Self {
        Self { db_file, socket_addr, event_pub, bp_data_tx /* , archive */ }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let (stream_pub, stream_sub) = mpsc::channel();

        let listener_handle = listener(self.socket_addr, stream_pub);

        let db_file = self.db_file.clone();
        let events_pub = self.event_pub.clone();
        let bp_data_tx = self.bp_data_tx.clone();
        let block_sub_handle = tokio::spawn(async move {
            thread::scope(|s| {
                thread::Builder::new()
                    .name("block-subscriber".to_string())
                    .spawn_scoped(s, || worker(db_file, stream_sub, events_pub, bp_data_tx))
                    .expect("spawn block-subscriber worker");
            });
        });

        tokio::select! {
            listener_result = listener_handle => {
                anyhow::bail!("listener thread exited with error: {}", listener_result.unwrap_err());
            }
            block_sub_result = block_sub_handle => {
                anyhow::bail!("block-subscriber thread exited with error: {}", block_sub_result.unwrap_err())
            }
        }
    }
}

fn worker(
    _db_file: impl AsRef<Path>,
    rx: mpsc::Receiver<Vec<u8>>,
    event_pub: mpsc::Sender<Event>,
    bp_data_tx: mpsc::Sender<(String, Vec<String>)>,
) -> anyhow::Result<()> {
    let data_dir =
        std::env::var("SQLITE_PATH").unwrap_or(sqlite_helper::SQLITE_DATA_DIR.to_string());
    let sqlite_helper_config =
        SqliteHelperConfig::new(data_dir.into(), Some("bm-archive.db".into()));

    let (sqlite_helper, _writer_join_handle) = SqliteHelper::from_config(sqlite_helper_config)?;
    let sqlite_helper = Arc::new(sqlite_helper);

    let mut transaction_traces = HashMap::new();
    let shard_state = Arc::new(ShardStateUnsplit::default());

    tracing::debug!("worker() starting loop...");
    loop {
        match rx.recv() {
            Ok(v) => {
                tracing::debug!("Data received");
                let (node_addr, raw_block) = bincode::deserialize::<(Option<String>, Vec<u8>)>(&v)?;
                let envelope: Envelope<GoshBLS, AckiNackiBlock> = bincode::deserialize(&raw_block)?;
                let thread_id = envelope.data().get_common_section().thread_id;
                if let Some(node_addr) = node_addr {
                    if let Err(err) = bp_data_tx.send((thread_id.to_string(), vec![node_addr])) {
                        tracing::error!("Failed to send data to the BPresolver: {err}");
                    }
                }

                let result = node::database::serialize_block::reflect_block_in_db(
                    sqlite_helper.clone(),
                    envelope,
                    shard_state.clone(),
                    &mut transaction_traces,
                );

                match result {
                    Ok(_) => tracing::debug!("block stored"),
                    Err(e) => tracing::debug!("failed to store block: {e}"),
                }

                event_pub.send(Event::NewBlock).expect("even send should not fail");
            }
            Err(err) => tracing::error!("Error receiving data: {}", err),
        };
    }
}

async fn listener(socket_addr: SocketAddr, tx: mpsc::Sender<Vec<u8>>) -> anyhow::Result<()> {
    loop {
        let transport = MsQuicTransport::new();
        match transport.connect(socket_addr, &["ALPN"], NetCredential::generate_self_signed()).await
        {
            Ok(conn) => loop {
                tracing::info!("Wait for incoming stream...");
                match conn.recv().await {
                    Ok((message, duration)) => {
                        tracing::info!(
                            duration = duration.as_millis(),
                            "Received: {} bytes",
                            message.len()
                        );
                        tx.send(message).expect("Receiver always exists");
                    }
                    Err(error) => {
                        tracing::error!("Error receiving a message: {error}");
                        break;
                    }
                }
            },
            Err(error) => {
                tracing::error!("Can't connect to  {socket_addr}: {error}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Connect to the database and create the `raw_blocks` table if it doesn't exist.
///
/// # Panics
///
/// Panics if the database file cannot be opened or created.
pub fn connect(db_file: impl AsRef<Path>) -> anyhow::Result<Connection> {
    let conn = Connection::open(db_file).unwrap();
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = 0;
        PRAGMA cache_size = 1000000;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA temp_store = MEMORY;",
    )
    .context("set PRAGMA")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS raw_blocks (
            `id` text PRIMARY KEY,
            `data` blob
        );",
        [],
    )
    .context("CREATE TABLE")?;
    Ok(conn)
}
