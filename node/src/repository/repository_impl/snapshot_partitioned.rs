//! Canonical SNAP2V2 primary payload. Not wired into snapshot publication yet.
use bincode::Options;
use serde::Serializer;

use super::snapshot_manifest::ThreadSnapshotManifest;
use super::snapshot_manifest::MAX_SNAPSHOT_ROOTS_PER_LIST;
use super::*;
use crate::multithreading::cross_thread_messaging::thread_references_state::ThreadReferencesState;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::types::BlockInfo;
use crate::types::ThreadsTable;

pub(crate) const MAGIC: &[u8; 7] = b"SNAP2V2";

/// Primary state deliberately has no account map, queues, cursors or pending
/// account operations. The legacy TVM state is retained with its existing hash.
#[derive(Serialize, Deserialize)]
pub(crate) struct PrimaryState {
    pub block_seq_no: BlockSeqNo,
    pub block_id: BlockIdentifier,
    pub shard_state: OptimisticShardState,
    pub block_info: BlockInfo,
    pub threads_table: ThreadsTable,
    pub thread_id: ThreadIdentifier,
    #[serde(serialize_with = "sorted_thread_refs")]
    pub thread_refs_state: ThreadReferencesState,
    pub cropped: Option<(ThreadIdentifier, ThreadsTable)>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PrimaryMetadata {
    pub state: PrimaryState,
    #[serde(serialize_with = "sorted_ancestors")]
    pub ancestor_blocks_data: Vec<AncestorBlockData>,
    pub finalized_block: Envelope<AckiNackiBlock>,
    #[serde(serialize_with = "sorted_bk_set")]
    pub bk_set: BlockKeeperSet,
    #[serde(serialize_with = "sorted_bk_set")]
    pub descendant_bk_set: BlockKeeperSet,
    #[serde(serialize_with = "sorted_bk_set")]
    pub future_bk_set: BlockKeeperSet,
    #[serde(serialize_with = "sorted_bk_set")]
    pub descendant_future_bk_set: BlockKeeperSet,
    pub finalized_block_stats: BlockStatistics,
    pub attestation_target: AttestationTargets,
    pub producer_selector: ProducerSelector,
    pub block_height: BlockHeight,
    pub prefinalization_proof: Envelope<AttestationData>,
    #[serde(serialize_with = "sorted_checkpoints")]
    pub ancestor_blocks_finalization_checkpoints: AncestorBlocksFinalizationCheckpoints,
    pub finalizes_blocks: BTreeSet<BlockIndex>,
    #[serde(serialize_with = "sorted_checkpoints")]
    pub parent_ancestor_blocks_finalization_checkpoints: AncestorBlocksFinalizationCheckpoints,
    pub block_protocol_version_state: BlockProtocolVersionState,
    pub history_data_snapshot: GlobalHistoryDataSnapshot,
    pub history_cursor: HistoryLayerData,
    pub finalization_chain: Vec<Envelope<AckiNackiBlock>>,
}

pub(crate) struct PrimaryThreadSnapshot {
    pub manifest: ThreadSnapshotManifest,
    pub metadata: PrimaryMetadata,
}

/// Input state is supplied separately: do not deserialize the old optimistic
/// payload (and all its queues) just to obtain primary metadata.
pub(crate) fn write_primary_snapshot_checked<W, C>(
    snapshot: &ThreadSnapshot,
    state: &OptimisticStateImpl,
    manifest: &ThreadSnapshotManifest,
    writer: &mut W,
    should_cancel: &C,
) -> anyhow::Result<()>
where
    W: Write + ?Sized,
    C: Fn() -> bool,
{
    anyhow::ensure!(!should_cancel(), "primary snapshot cancelled");
    manifest.validate_root_limits(MAX_SNAPSHOT_ROOTS_PER_LIST)?;
    let metadata = PrimaryMetadata {
        state: PrimaryState {
            block_seq_no: state.block_seq_no,
            block_id: state.block_id,
            shard_state: state.shard_state.clone(),
            block_info: state.block_info.clone(),
            threads_table: state.threads_table.clone(),
            thread_id: state.thread_id,
            thread_refs_state: state.thread_refs_state.clone(),
            cropped: state.cropped.clone(),
        },
        ancestor_blocks_data: snapshot.ancestor_blocks_data.clone(),
        finalized_block: snapshot.finalized_block.clone(),
        bk_set: snapshot.bk_set.clone(),
        descendant_bk_set: snapshot.descendant_bk_set.clone(),
        future_bk_set: snapshot.future_bk_set.clone(),
        descendant_future_bk_set: snapshot.descendant_future_bk_set.clone(),
        finalized_block_stats: snapshot.finalized_block_stats.clone(),
        attestation_target: snapshot.attestation_target,
        producer_selector: snapshot.producer_selector.clone(),
        block_height: snapshot.block_height,
        prefinalization_proof: snapshot.prefinalization_proof.clone(),
        ancestor_blocks_finalization_checkpoints: snapshot
            .ancestor_blocks_finalization_checkpoints
            .clone(),
        finalizes_blocks: snapshot.finalizes_blocks.clone(),
        parent_ancestor_blocks_finalization_checkpoints: snapshot
            .parent_ancestor_blocks_finalization_checkpoints
            .clone(),
        block_protocol_version_state: snapshot.block_protocol_version_state.clone(),
        history_data_snapshot: snapshot.history_data_snapshot.clone(),
        history_cursor: snapshot.history_cursor.clone(),
        finalization_chain: snapshot.finalization_chain.clone(),
    };
    validate_boundary(&metadata)?;
    writer.write_all(MAGIC)?;
    bincode::serialize_into(&mut *writer, manifest)?;
    bincode::serialize_into(&mut *writer, &metadata)?;
    anyhow::ensure!(!should_cancel(), "primary snapshot cancelled");
    Ok(())
}

/// Decode only a bounded primary payload. A successful decode does not verify
/// consensus proofs or authorize import: the caller still verifies the primary
/// proofs, all partitions and their complete-state roots before committing.
pub(crate) fn read_primary_snapshot_checked<R, C>(
    reader: &mut R,
    max_roots_per_list: u64,
    should_cancel: &C,
) -> anyhow::Result<PrimaryThreadSnapshot>
where
    R: Read + ?Sized,
    C: Fn() -> bool,
{
    let max_roots_per_list = max_roots_per_list.min(MAX_SNAPSHOT_ROOTS_PER_LIST);
    let mut bytes = Vec::new();
    let mut buffer = [0; 65536];
    loop {
        anyhow::ensure!(!should_cancel(), "primary snapshot cancelled");
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    anyhow::ensure!(bytes.starts_with(MAGIC), "invalid primary snapshot marker");
    let mut remaining = &bytes[MAGIC.len()..];
    let mut roots = || -> anyhow::Result<Vec<[u8; 32]>> {
        let mut count = [0; 8];
        remaining.read_exact(&mut count)?;
        let count = u64::from_le_bytes(count);
        anyhow::ensure!(
            count <= max_roots_per_list && count <= remaining.len() as u64 / 32,
            "primary manifest root budget exceeded"
        );
        let mut roots = Vec::new();
        for _ in 0..count {
            anyhow::ensure!(!should_cancel(), "primary snapshot cancelled");
            let mut root = [0; 32];
            remaining.read_exact(&mut root)?;
            roots.push(root);
        }
        Ok(roots)
    };
    let manifest = ThreadSnapshotManifest {
        account_partition_roots: roots()?,
        normal_message_partition_roots: roots()?,
        high_priority_message_partition_roots: roots()?,
    };
    let metadata: PrimaryMetadata = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(remaining.len() as u64)
        .reject_trailing_bytes()
        .deserialize(remaining)?;
    anyhow::ensure!(
        bincode::serialize(&metadata)? == remaining,
        "non-canonical primary snapshot metadata"
    );
    validate_boundary(&metadata)?;
    anyhow::ensure!(!should_cancel(), "primary snapshot cancelled");
    Ok(PrimaryThreadSnapshot { manifest, metadata })
}

fn validate_boundary(metadata: &PrimaryMetadata) -> anyhow::Result<()> {
    let state = &metadata.state;
    let block = metadata.finalized_block.data();
    anyhow::ensure!(state.block_id == block.identifier(), "primary snapshot block mismatch");
    anyhow::ensure!(
        state.thread_id == *block.common_section().thread_id(),
        "primary snapshot thread mismatch"
    );
    anyhow::ensure!(state.block_seq_no == block.seq_no(), "primary snapshot sequence mismatch");
    Ok(())
}

fn sorted_bk_set<S: Serializer>(value: &BlockKeeperSet, serializer: S) -> Result<S::Ok, S::Error> {
    value.iter_by_signer().collect::<BTreeMap<_, _>>().serialize(serializer)
}

fn sorted_thread_refs<S: Serializer>(
    value: &ThreadReferencesState,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value.all_thread_refs().iter().collect::<BTreeMap<_, _>>().serialize(serializer)
}

fn sorted_checkpoints<S: Serializer>(
    value: &AncestorBlocksFinalizationCheckpoints,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    (
        value.primary().iter().collect::<BTreeMap<_, _>>(),
        value.fallback().iter().collect::<BTreeMap<_, _>>(),
    )
        .serialize(serializer)
}

fn sorted_ancestors<S: Serializer>(
    values: &[AncestorBlockData],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut sequence = serializer.serialize_seq(Some(values.len()))?;
    for value in values {
        // Tuple order matches the existing struct's field order. Sequence order
        // is semantic and is preserved; only unordered maps are sorted.
        sequence.serialize_element(&(
            value.block_identifier,
            value.block_seq_no,
            value.thread_identifier,
            SortedRef(&value.cross_thread_ref_data),
            SortedBk(&value.bk_set),
            SortedBk(&value.future_bk_set),
            &value.envelope_hash,
            value.parent_block_identifier,
            &value.block_version_state,
        ))?;
    }
    sequence.end()
}

struct SortedBk<'a>(&'a BlockKeeperSet);
impl Serialize for SortedBk<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        sorted_bk_set(self.0, serializer)
    }
}

struct SortedRef<'a>(&'a CrossThreadRefData);
impl Serialize for SortedRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value = self.0;
        (
            value.block_identifier(),
            value.block_seq_no(),
            value.block_thread_identifier(),
            value.outbound_messages().iter().collect::<BTreeMap<_, _>>(),
            value.outbound_accounts().iter().collect::<BTreeMap<_, _>>(),
            value.threads_table(),
            value.parent_block_identifier(),
            value.block_refs(),
            value.is_tombstone(),
        )
            .serialize(serializer)
    }
}

#[cfg(test)]
#[path = "snapshot_partitioned_tests.rs"]
mod tests;
