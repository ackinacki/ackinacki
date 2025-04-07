// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use database::documents_db::DocumentsDb;
use tvm_block::Deserializable;
use tvm_block::ShardStateUnsplit;
use tvm_types::Cell;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::database::serialize_block::reflect_block_in_db;
use crate::types::AckiNackiBlock;

pub mod serialize_block;

pub fn write_to_db(
    archive: Arc<dyn DocumentsDb>,
    envelope: Envelope<GoshBLS, AckiNackiBlock>,
    shard_state: Option<Arc<ShardStateUnsplit>>,
    shard_state_cell: Option<Cell>,
) -> anyhow::Result<()> {
    let block = envelope.data().clone();
    let sqlite_clone = archive.clone();

    let shard_state = if let Some(shard_state) = shard_state {
        shard_state
    } else {
        assert!(shard_state_cell.is_some());
        let cell = shard_state_cell.unwrap();
        Arc::new(
            ShardStateUnsplit::construct_from_cell(cell)
                .expect("Failed to deserialize shard state"),
        )
    };

    tracing::trace!("Write to archive: seq_no={:?}, id={:?}", block.seq_no(), block.identifier());

    let mut transaction_traces = HashMap::new();
    reflect_block_in_db(sqlite_clone, envelope, shard_state, &mut transaction_traces)
        .map_err(|e| anyhow::format_err!("Failed to archive block data: {e}"))
        .expect("Failed to archive block data");

    tracing::trace!("reflect_block_in_db finished");

    Ok(())
}
