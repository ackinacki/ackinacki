// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use database::documents_db::DocumentsDb;
use parking_lot::Mutex;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::database::serialize_block::reflect_block_in_db;
use crate::repository::accounts::NodeThreadAccountsRef;
use crate::repository::accounts::NodeThreadAccountsRepository;
use crate::types::AckiNackiBlock;

pub mod serialize_block;

pub fn write_to_db(
    archive: Arc<Mutex<dyn DocumentsDb>>,
    envelope: Envelope<AckiNackiBlock>,
    thread_accounts_repository: NodeThreadAccountsRepository,
    shard_state: NodeThreadAccountsRef,
) -> anyhow::Result<()> {
    let block = envelope.data().clone();
    let sqlite_clone = archive.clone();

    tracing::trace!("Write to archive: seq_no={:?}, id={:?}", block.seq_no(), block.identifier());

    let mut transaction_traces = HashMap::new();
    let _activity_summaries = reflect_block_in_db(
        sqlite_clone,
        envelope,
        None,
        &shard_state,
        &mut transaction_traces,
        &thread_accounts_repository,
    )
    .map_err(|e| anyhow::format_err!("Failed to archive block data: {e}"))
    .expect("Failed to archive block data");

    tracing::trace!("reflect_block_in_db finished");

    Ok(())
}
