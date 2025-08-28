// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
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
use parking_lot::Mutex;
use rusqlite::Connection;
use transport_layer::msquic::MsQuicTransport;
use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetTransport;
use tvm_block::ShardStateUnsplit;

use crate::events::Event;
use crate::metrics::Metrics;

pub enum WorkerCommand {
    Data(Vec<u8>),
    RotateDb,
    Shutdown,
}

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

    pub async fn run(
        &self,
        metrics: Option<Metrics>,
        cmd_tx: mpsc::Sender<WorkerCommand>,
        cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> anyhow::Result<()> {
        let listener_handle = listener(self.socket_addr, cmd_tx);

        let db_file = self.db_file.clone();
        let events_pub = self.event_pub.clone();
        let bp_data_tx = self.bp_data_tx.clone();
        let block_sub_handle = tokio::task::spawn_blocking(move || {
            match thread::Builder::new()
                .name("block-subscriber".to_string())
                .spawn(|| worker(db_file, cmd_rx, events_pub, bp_data_tx, metrics))
                .expect("spawn block-subscriber worker")
                .join()
            {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(anyhow::anyhow!("Subscriber worker failed: {e:?}")),
                Err(e) => Err(anyhow::anyhow!("Subscriber worker panic: {e:?}")),
            }
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
    rx: mpsc::Receiver<WorkerCommand>,
    event_pub: mpsc::Sender<Event>,
    bp_data_tx: mpsc::Sender<(String, Vec<String>)>,
    metrics: Option<Metrics>,
) -> anyhow::Result<()> {
    let data_dir =
        std::env::var("SQLITE_PATH").unwrap_or(sqlite_helper::SQLITE_DATA_DIR.to_string());
    let sqlite_helper_config =
        SqliteHelperConfig::new(data_dir.into(), Some("bm-archive.db".into()));

    let (sqlite_helper, _writer_join_handle) = SqliteHelper::from_config(sqlite_helper_config)?;
    let sqlite_helper = Arc::new(Mutex::new(sqlite_helper));

    let mut transaction_traces = HashMap::new();
    let shard_state = Arc::new(ShardStateUnsplit::default());

    tracing::debug!("worker() starting loop...");
    loop {
        match rx.recv() {
            Ok(WorkerCommand::Data(v)) => {
                tracing::debug!("Data received");
                let (node_addr, raw_block) = bincode::deserialize::<(Option<String>, Vec<u8>)>(&v)?;
                let envelope: Envelope<GoshBLS, AckiNackiBlock> = bincode::deserialize(&raw_block)?;
                let thread_id = envelope.data().get_common_section().thread_id;
                if let Some(node_addr) = node_addr {
                    if let Err(err) = bp_data_tx.send((thread_id.to_string(), vec![node_addr])) {
                        tracing::error!("Failed to send data to the BPresolver: {err}");
                    }
                }

                if let Some(metrics) = metrics.as_ref() {
                    match envelope.data().tvm_block().read_info() {
                        Ok(block_info) => metrics.bm.report_last_finalized_seqno(
                            block_info.seq_no(),
                            thread_id.to_string(),
                        ),
                        Err(err) => {
                            tracing::error!("Failed to record last_finalized_seqno: {err}");
                        }
                    }
                }

                let result = node::database::serialize_block::reflect_block_in_db(
                    sqlite_helper.clone(),
                    envelope,
                    Some(raw_block),
                    shard_state.clone(),
                    &mut transaction_traces,
                );

                match result {
                    Ok(_) => tracing::debug!("block stored"),
                    Err(e) => tracing::debug!("failed to store block: {e}"),
                }

                event_pub.send(Event::NewBlock).expect("even send should not fail");
            }
            Ok(WorkerCommand::RotateDb) => {
                tracing::info!("Rotating SQLite DB...");

                let mut guarded = sqlite_helper.lock();
                match guarded.rotate_db_file() {
                    Ok(_) => tracing::info!("Database rotated successfully."),
                    Err(e) => tracing::error!("Failed to rotate database: {e}"),
                }
            }
            Ok(WorkerCommand::Shutdown) => {
                tracing::info!("Shutdown by SIGTERM...");
                let mut guarded = sqlite_helper.lock();
                match guarded.shutdown() {
                    Ok(_) => tracing::info!("Database is ready to shutdown."),
                    Err(e) => tracing::error!("Failed to create checkpoint: {e}"),
                }
            }
            Err(err) => tracing::error!("Error receiving data: {}", err),
        };
    }
}

async fn listener(socket_addr: SocketAddr, tx: mpsc::Sender<WorkerCommand>) -> anyhow::Result<()> {
    loop {
        let transport = MsQuicTransport::new();
        match transport
            .connect(
                socket_addr,
                &["ALPN"],
                NetCredential::generate_self_signed(Some(vec![socket_addr.to_string()]), None)?,
            )
            .await
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
                        tx.send(WorkerCommand::Data(message)).expect("Receiver always exists");
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
