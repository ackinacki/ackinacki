// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use anyhow::bail;
use database::sqlite::sqlite_helper::SqliteHelper;
use node::bls::envelope::BLSSignedEnvelope;
use node::bls::envelope::Envelope;
use node::bls::GoshBLS;
use node::types::AckiNackiBlock;
use parking_lot::Mutex;
use transport_layer::HostPort;
use tvm_block::ShardStateUnsplit;

use crate::application::metrics::Metrics;
use crate::application::metrics::ERR_GRACEFULL_SHUTDOWN;
use crate::application::metrics::ERR_RECORD_SEQNO;
use crate::application::metrics::ERR_ROTATE_DB;
use crate::application::metrics::ERR_STORE_BLOCK;
use crate::application::metrics::ERR_UPDATE_BP_RESOLVER;
use crate::domain::models::AppState;
use crate::domain::models::UpdatableBPResolver;
use crate::domain::models::WorkerCommand;

pub fn run(
    bp_resolver: Arc<Mutex<dyn UpdatableBPResolver>>,
    db_writer: SqliteHelper,
    app_state: Arc<AppState>,
    metrics: Option<Metrics>,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    tokio::task::spawn_blocking(move || {
        match thread::Builder::new()
            .name("block-applier".to_string())
            .spawn(|| worker(db_writer, cmd_rx, bp_resolver, app_state, metrics))
            .expect("spawn block-subscriber worker")
            .join()
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(anyhow::anyhow!("Block applier worker failed: {e:?}")),
            Err(e) => Err(anyhow::anyhow!("Block applier worker panic: {e:?}")),
        }
    })
}

fn worker(
    db_writer: SqliteHelper,
    rx: mpsc::Receiver<WorkerCommand>,
    bp_resolver: Arc<Mutex<dyn UpdatableBPResolver>>,
    app_state: Arc<AppState>,
    metrics: Option<Metrics>,
) -> anyhow::Result<()> {
    let mut transaction_traces = HashMap::new();
    let shard_state = Arc::new(ShardStateUnsplit::default());

    tracing::debug!("worker() starting loop...");
    let db_helper = Arc::new(Mutex::new(db_writer));
    loop {
        match rx.recv() {
            Ok(WorkerCommand::Data(v)) => {
                tracing::debug!("Data received");
                let (node_addr, raw_block) =
                    bincode::deserialize::<(Option<HostPort>, Vec<u8>)>(&v)?;
                let envelope: Envelope<GoshBLS, AckiNackiBlock> = bincode::deserialize(&raw_block)?;
                let thread_id = envelope.data().get_common_section().thread_id;
                if let Some(node_addr) = node_addr {
                    if let Err(err) =
                        bp_resolver.lock().upsert(thread_id.to_string(), vec![node_addr])
                    {
                        // This error can happen if `node_addr` can't be parsed as a SocketAddress
                        tracing::error!("Failed to update bp_resolver state: {err}");
                        if let Some(m) = &metrics {
                            m.bm.report_errors(ERR_UPDATE_BP_RESOLVER);
                        }
                    }
                }

                if let Ok(time) = envelope.data().time() {
                    let current = app_state.last_block_gen_utime.load(Ordering::Relaxed);
                    if current < time {
                        app_state.last_block_gen_utime.store(time, Ordering::Relaxed);
                    }
                }

                if let Some(m) = &metrics {
                    match envelope.data().tvm_block().read_info() {
                        Ok(block_info) => m.bm.report_last_finalized_seqno(
                            block_info.seq_no(),
                            thread_id.to_string(),
                        ),
                        Err(err) => {
                            tracing::error!("Failed to record last_finalized_seqno: {err}");
                            m.bm.report_errors(ERR_RECORD_SEQNO);
                        }
                    }
                }

                if let Err(err) = node::database::serialize_block::reflect_block_in_db(
                    db_helper.clone(),
                    envelope,
                    Some(raw_block),
                    shard_state.clone(),
                    &mut transaction_traces,
                ) {
                    tracing::error!("failed to store block: {err}");
                    if let Some(m) = &metrics {
                        m.bm.report_errors(ERR_STORE_BLOCK);
                    }
                };
            }
            Ok(WorkerCommand::RotateDb) => {
                tracing::info!("Rotating SQLite DB...");

                let mut guarded = db_helper.lock();
                if let Err(err) = guarded.rotate_db_file() {
                    tracing::error!("Failed to rotate database: {err}");
                    if let Some(m) = &metrics {
                        m.bm.report_errors(ERR_ROTATE_DB);
                    }
                } else if let Some(m) = &metrics {
                    m.bm.report_rotation();
                }
            }
            Ok(WorkerCommand::Shutdown(tx)) => {
                tracing::info!("Shutdown by SIGTERM...");
                let mut guarded = db_helper.lock();
                loop {
                    if let Err(err) = guarded.shutdown() {
                        tracing::error!("Failed to create checkpoint: {err}");
                        if let Some(m) = &metrics {
                            m.bm.report_errors(ERR_GRACEFULL_SHUTDOWN);
                        }
                        // No sleep() here, fn shutdown() has sleep inside
                    } else {
                        // After this signal will be snt the entire application is allowed to close.
                        tx.send(()).expect("Receiver has been already dropped");
                        break;
                    }
                }
            }
            Err(err) => bail!("Error reading from channel: {err}"),
        };
    }
}
