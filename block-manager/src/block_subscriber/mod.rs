// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;

use anyhow::Context;
use database::sqlite::sqlite_helper;
use database::sqlite::sqlite_helper::SqliteHelper;
use node::bls::envelope::Envelope;
use node::bls::GoshBLS;
use node::types::AckiNackiBlock;
use rusqlite::Connection;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tvm_block::ShardStateUnsplit;
use url::Url;
use wtransport::ClientConfig;
use wtransport::Endpoint;

use crate::events::Event;

pub struct BlockSubscriber {
    db_file: PathBuf,
    stream_src_url: Url,
    event_pub: Sender<Event>,
    // archive: Arc<dyn DocumentsDb>,
    // TODO: more fields related to cache of blocks
}

impl BlockSubscriber {
    /// # Panics
    ///
    /// Panics if the database file cannot be opened or created.
    pub fn new(
        db_file: PathBuf,
        stream_src_url: Url,
        event_pub: Sender<Event>,
        // archive: Arc<dyn DocumentsDb>,
    ) -> Self {
        Self { db_file, stream_src_url, event_pub /* , archive */ }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let (stream_pub, stream_sub) = mpsc::channel();

        let listener_handle = listener(self.stream_src_url.clone(), stream_pub);

        let db_file = self.db_file.clone();
        let events_pub = self.event_pub.clone();
        let block_sub_handle = tokio::spawn(async move {
            thread::scope(|s| {
                thread::Builder::new()
                    .name("block-subscriber".to_string())
                    .spawn_scoped(s, || worker(db_file, stream_sub, events_pub))
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
) -> anyhow::Result<()> {
    let sqlite_helper_config = json!({
        "data_dir": std::env::var("SQLITE_PATH").unwrap_or(sqlite_helper::SQLITE_DATA_DIR.into()),
        "db_file": "bm-archive.db".to_string(),
    })
    .to_string();
    let sqlite_helper_raw = SqliteHelper::from_config(&sqlite_helper_config)?;
    let sqlite_helper = Arc::new(sqlite_helper_raw.clone());

    let mut transaction_traces = HashMap::new();
    let shard_state = Arc::new(ShardStateUnsplit::default());

    loop {
        match rx.recv() {
            Ok(v) => {
                tracing::debug!("Data received");
                let envelope: Envelope<GoshBLS, AckiNackiBlock<GoshBLS>> =
                    bincode::deserialize(&v)?;

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

async fn listener(stream_src_url: Url, tx: mpsc::Sender<Vec<u8>>) -> anyhow::Result<()> {
    loop {
        let config = ClientConfig::builder() //
            .with_bind_default()
            .with_no_cert_validation()
            .build();

        tracing::info!("Connecting to {} {:?}", stream_src_url.as_str(), stream_src_url.port(),);

        let Ok(connection) = Endpoint::client(config) //
            .expect("endpoint client")
            .connect(stream_src_url.as_str())
            .await
        else {
            tracing::error!("connection error");
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            continue;
        };

        tracing::info!("Connection {:?} {:?}", connection.stable_id(), connection.session_id());

        loop {
            tracing::info!("Wait for incoming stream...");

            let Ok(mut stream) = connection.accept_uni().await else {
                tracing::warn!("Connection is closed");
                break;
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::with_capacity(1024);
                stream.read_to_end(&mut buf).await.expect("stream read_to_end successful");

                tracing::info!("Received: {} bytes", buf.len());
                tx_clone.send(buf).unwrap();
            })
            .await
            .ok();
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
