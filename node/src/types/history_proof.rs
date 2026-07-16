use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use anyhow::ensure;
use derive_getters::Getters;
pub use history_proof::compute_block_leaf_hash;
pub use history_proof::compute_ext_message_leaf_hash;
pub use history_proof::compute_ext_out_messages_root;
pub use history_proof::compute_referenced_block_leaf_hash;
pub use history_proof::compute_referenced_blocks_root;
pub use history_proof::dense_leaf_hash;
pub use history_proof::dense_merkle_proof;
pub use history_proof::dense_merkle_root;
pub use history_proof::dense_merkle_tree;
pub use history_proof::dense_merkle_verify;
pub use history_proof::history_proofs_l0;
pub use history_proof::LayerNumber;
pub use history_proof::PoseidonHasher;
pub use history_proof::HISTORY_PROOF_WINDOW_SIZE;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::RwLock;
use serde::de::Error;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typed_builder::TypedBuilder;

use crate::types::BlockHeight;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, TypedBuilder, Getters)]
pub struct ProofLayerRootHash {
    layer: LayerNumber,        // Proof Layer
    root_hash: [u8; 32],       // Root hash
    block_height: BlockHeight, // Height of the last block that was used to calculate this hash
    block_id: BlockIdentifier, // ID of the block that contains this hash
}

impl Debug for ProofLayerRootHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProofLayerRootHash")
            .field("layer", &self.layer)
            .field("root_hash", &hex::encode(self.root_hash))
            .field("block_height", &self.block_height)
            .field("block_id", &self.block_id)
            .finish()
    }
}

pub type HistoryLayerData = BTreeMap<LayerNumber, HistoryBlockData>;
pub type GlobalHistoryData = Arc<RwLock<HistoryLayerData>>;
pub type GlobalHistoryDataSnapshot = HistoryLayerData;

pub fn history_proof_thread_id() -> ThreadIdentifier {
    ThreadIdentifier::default()
}

pub fn take_history_data_snapshot(data: GlobalHistoryData) -> GlobalHistoryDataSnapshot {
    normalize_history_layer_data(data.read().clone())
}

pub fn unpack_history_data_snapshot(snapshot: GlobalHistoryDataSnapshot) -> GlobalHistoryData {
    Arc::new(RwLock::new(normalize_history_layer_data(snapshot)))
}

fn normalize_history_layer_data(snapshot: GlobalHistoryDataSnapshot) -> GlobalHistoryDataSnapshot {
    let history_thread_id = history_proof_thread_id();
    let mut normalized = HistoryLayerData::new();

    for (layer, data) in snapshot {
        let mut normalized_data = HistoryBlockData::new();
        let data_len = data.data_len.min(HISTORY_PROOF_WINDOW_SIZE);
        for hash in data.data[..data_len].iter() {
            normalized_data.data[normalized_data.data_len] = *hash;
            normalized_data.data_len += 1;
        }
        normalized_data.last_processed_block_height = BlockHeight::builder()
            .height(*data.last_processed_block_height.height())
            .thread_identifier(history_thread_id)
            .build();
        normalized.insert(layer, normalized_data);
    }

    normalized
}

pub fn contains_history_proof_hash(history_data: &GlobalHistoryData, hash_bytes: [u8; 32]) -> bool {
    let history_read = history_data.read();
    for (_layer, layer_data) in history_read.iter() {
        for hash in layer_data.data().iter().take(*layer_data.data_len()) {
            if *hash == hash_bytes {
                return true;
            }
        }
    }

    false
}

pub fn make_check_history_proof_hash_callback(
    history_data: GlobalHistoryData,
) -> Arc<dyn Send + Sync + Fn(u8, [u8; 32]) -> bool> {
    Arc::new(move |_layer_number: u8, hash_bytes: [u8; 32]| -> bool {
        // layer_number is intentionally ignored: history proof storage is global
        // and CHKHISTPROOF checks the whole zero-thread proof set.
        contains_history_proof_hash(&history_data, hash_bytes)
    })
}

#[derive(Clone, Getters)]
pub struct HistoryBlockData {
    thread_id: ThreadIdentifier,
    last_processed_block_height: BlockHeight,
    data_len: usize,
    data: [[u8; 32]; HISTORY_PROOF_WINDOW_SIZE],
}

#[derive(Getters, Serialize, Deserialize)]
struct HistoryBlockDataSerDe {
    thread_id: ThreadIdentifier,
    last_processed_block_height: BlockHeight,
    data_len: usize,
    data: Vec<[u8; 32]>,
}

impl Serialize for HistoryBlockData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        HistoryBlockDataSerDe {
            thread_id: self.thread_id,
            last_processed_block_height: self.last_processed_block_height,
            data_len: self.data_len,
            data: self.data.to_vec(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HistoryBlockData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = HistoryBlockDataSerDe::deserialize(deserializer)?;
        Ok(Self {
            data: data.data.try_into().map_err(|_| D::Error::custom(""))?,
            thread_id: data.thread_id,
            data_len: data.data_len,
            last_processed_block_height: data.last_processed_block_height,
        })
    }
}

impl Default for HistoryBlockData {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoryBlockData {
    pub fn new() -> Self {
        let thread_id = history_proof_thread_id();
        let data = [[0u8; 32]; HISTORY_PROOF_WINDOW_SIZE];
        Self {
            thread_id,
            last_processed_block_height: BlockHeight::builder()
                .height(0)
                .thread_identifier(thread_id)
                .build(),
            data,
            data_len: 0,
        }
    }

    pub fn update_from_pure_data(
        &mut self,
        leaf_hash: [u8; 32],
        block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        tracing::trace!("HistoryBlockData: update: self.data_len={}, self.last_processed_block_height={}, new_height={}", self.data_len, self.last_processed_block_height.height(), block_height.height());
        tracing::trace!(
            "HistoryBlockData: update_from_pure_data leaf_hash={:?}",
            hex::encode(leaf_hash)
        );
        tracing::trace!(
            "HistoryBlockData: update_from_pure_data data_len={} data_tail={:?}",
            self.data_len,
            self.data.iter().next_back().map(hex::encode)
        );
        ensure!(
            block_height.thread_identifier() == &self.thread_id,
            "History proof data is stored only for the default thread"
        );
        ensure!(
            *self.last_processed_block_height.height() == 0
                || block_height.height() > self.last_processed_block_height.height()
        );
        if self.data_len == HISTORY_PROOF_WINDOW_SIZE {
            self.clear_data();
        }
        self.data[self.data_len] = leaf_hash;
        self.data_len += 1;
        self.last_processed_block_height = block_height;
        Ok(())
    }

    pub fn clear_data(&mut self) {
        self.data_len = 0;
    }

    pub fn is_full(&self) -> bool {
        self.data_len == HISTORY_PROOF_WINDOW_SIZE
    }

    pub fn calculate_root_hash(
        &self,
        last_layer_root_hash_of_the_same_layer: Option<[u8; 32]>,
        last_layer_root_hash_of_the_higher_layer: Option<[u8; 32]>,
    ) -> anyhow::Result<[u8; 32]> {
        tracing::trace!("HistoryBlockData: calculate_root_hash: self.data_len={}, self.last_processed_block_height={}, last_layer_root_hash_of_the_same_layer={:?}, last_layer_root_hash_of_the_higher_layer={:?}", self.data_len, self.last_processed_block_height.height(), last_layer_root_hash_of_the_same_layer.as_ref().map(hex::encode), last_layer_root_hash_of_the_higher_layer.as_ref().map(hex::encode));
        ensure!(self.data_len == HISTORY_PROOF_WINDOW_SIZE, "History block data length mismatch");

        let hasher = PoseidonHasher::new();

        let mut leaf_hashes: Vec<[u8; 32]> = Vec::with_capacity(2 + HISTORY_PROOF_WINDOW_SIZE);
        leaf_hashes.push(last_layer_root_hash_of_the_higher_layer.unwrap_or([0u8; 32]));
        leaf_hashes.push(last_layer_root_hash_of_the_same_layer.unwrap_or([0u8; 32]));
        for leaf in &self.data[..self.data_len] {
            leaf_hashes.push(*leaf);
        }

        let start = std::time::Instant::now();
        let root = dense_merkle_root(&hasher, &leaf_hashes);
        tracing::trace!("calculate_root_hash Poseidon insert: {} ms", start.elapsed().as_millis());

        tracing::trace!("HistoryBlockData: calculate_root_hash root={:?}", hex::encode(root));
        Ok(root)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;
    use node_types::ThreadIdentifier;
    use parking_lot::RwLock;

    use super::compute_block_leaf_hash;
    use super::compute_ext_out_messages_root;
    use super::contains_history_proof_hash;
    use super::make_check_history_proof_hash_callback;
    use super::unpack_history_data_snapshot;
    use super::GlobalHistoryData;
    use super::HistoryBlockData;
    use super::HistoryLayerData;
    use super::LayerNumber;
    use crate::types::BlockHeight;

    fn routing(dapp: u8, account: u8) -> node_types::AccountRouting {
        AccountIdentifier::new([account; 32]).routing(DAppIdentifier::new([dapp; 32]))
    }

    fn block_height(height: u64) -> BlockHeight {
        BlockHeight::builder().height(height).thread_identifier(ThreadIdentifier::default()).build()
    }

    fn history_data_with_layers(layers: HistoryLayerData) -> GlobalHistoryData {
        Arc::new(RwLock::new(layers))
    }

    #[test]
    fn check_history_proof_callback_searches_zero_thread() -> anyhow::Result<()> {
        let hash = [7u8; 32];
        let mut layer_data = HistoryBlockData::new();
        layer_data.update_from_pure_data(hash, block_height(1))?;
        let history_data = history_data_with_layers(BTreeMap::from_iter([(0, layer_data)]));

        let callback = make_check_history_proof_hash_callback(history_data);

        assert!(callback(99, hash));
        Ok(())
    }

    #[test]
    fn check_history_proof_callback_searches_all_layers() -> anyhow::Result<()> {
        let hash = [8u8; 32];
        let mut layer_data = HistoryBlockData::new();
        layer_data.update_from_pure_data(hash, block_height(1))?;
        let history_data = history_data_with_layers(BTreeMap::from_iter([(2, layer_data)]));

        let callback = make_check_history_proof_hash_callback(history_data);

        assert!(callback(0, hash));
        Ok(())
    }

    #[test]
    fn check_history_proof_does_not_match_unfilled_zero_entries() {
        let layer_data = HistoryBlockData::new();
        let history_data = history_data_with_layers(BTreeMap::from_iter([(0, layer_data)]));

        assert!(!contains_history_proof_hash(&history_data, [0u8; 32]));
    }

    #[test]
    fn check_history_proof_uses_direct_default_thread_storage() -> anyhow::Result<()> {
        let hash = [9u8; 32];
        let mut layer_data = HistoryBlockData::new();
        layer_data.update_from_pure_data(hash, block_height(1))?;
        let history_data = history_data_with_layers(BTreeMap::from_iter([(0, layer_data)]));

        assert!(contains_history_proof_hash(&history_data, hash));
        Ok(())
    }

    #[test]
    fn unpack_history_snapshot_uses_direct_layer_storage() -> anyhow::Result<()> {
        let hash = [11u8; 32];
        let mut layer_data = HistoryBlockData::new();
        layer_data.update_from_pure_data(hash, block_height(1))?;

        let history_data =
            unpack_history_data_snapshot(BTreeMap::from_iter([(0 as LayerNumber, layer_data)]));

        assert!(contains_history_proof_hash(&history_data, hash));
        Ok(())
    }

    #[test]
    fn block_leaf_hash_depends_on_block_id_envelope_hash_and_tracked_ext_out_messages_root() {
        let block_id = [1u8; 32];
        let envelope_hash = [2u8; 32];
        let tracked_ext_out_messages_root = [3u8; 32];
        let base =
            compute_block_leaf_hash(&block_id, &envelope_hash, &tracked_ext_out_messages_root);

        let changed_block_id =
            compute_block_leaf_hash(&[4u8; 32], &envelope_hash, &tracked_ext_out_messages_root);
        assert_ne!(base, changed_block_id);

        let changed_envelope_hash =
            compute_block_leaf_hash(&block_id, &[5u8; 32], &tracked_ext_out_messages_root);
        assert_ne!(base, changed_envelope_hash);

        let changed_tracked_ext_out_messages_root =
            compute_block_leaf_hash(&block_id, &envelope_hash, &[6u8; 32]);
        assert_ne!(base, changed_tracked_ext_out_messages_root);
    }

    #[test]
    fn ext_out_messages_root_is_stable_for_different_insertion_order() {
        let first = routing(1, 1);
        let second = routing(2, 2);

        let mut forward = BTreeMap::new();
        forward.insert(first, vec![[0x11; 32], [0x12; 32]]);
        forward.insert(second, vec![[0x21; 32]]);

        let mut reverse = BTreeMap::new();
        reverse.insert(second, vec![[0x21; 32]]);
        reverse.insert(first, vec![[0x11; 32], [0x12; 32]]);

        assert_eq!(
            compute_ext_out_messages_root(&forward),
            compute_ext_out_messages_root(&reverse)
        );
    }
}
