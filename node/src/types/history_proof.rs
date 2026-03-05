use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use anyhow::ensure;
use derive_getters::Getters;
use monotree::Hash;
use monotree::Hasher;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::RwLock;
use serde::de::Error;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_vm::executor::zk_stuff::bn254::poseidon::poseidon_bytes_flat;
use typed_builder::TypedBuilder;

use crate::types::envelope_hash::AckiNackiEnvelopeHash;
use crate::types::BlockHeight;

pub const HISTORY_PROOF_WINDOW_SIZE: usize = 128;

pub struct PoseidonHasher {}

impl Hasher for PoseidonHasher {
    fn new() -> Self {
        PoseidonHasher {}
    }

    fn digest(&self, bytes: &[u8]) -> Hash {
        poseidon_bytes_flat(bytes).expect("Should not fail")
    }
}

pub type LayerNumber = u8;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, TypedBuilder, Getters)]
pub struct ProofLayerRootHash {
    layer: LayerNumber,        // Proof Layer
    root_hash: monotree::Hash, // Root hash
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
pub type GlobalHistoryData = Arc<RwLock<HashMap<ThreadIdentifier, Arc<RwLock<HistoryLayerData>>>>>;
pub type GlobalHistoryDataSnapshot = HashMap<ThreadIdentifier, HistoryLayerData>;

pub fn take_history_data_snapshot(data: GlobalHistoryData) -> GlobalHistoryDataSnapshot {
    let data_lock = data.read();
    let mut snapshot = HashMap::new();
    for (thread_id, data) in data_lock.iter() {
        snapshot.insert(*thread_id, data.read().clone());
    }
    snapshot
}

pub fn unpack_history_data_snapshot(snapshot: GlobalHistoryDataSnapshot) -> GlobalHistoryData {
    let shared_data = snapshot.into_iter().map(|(k, v)| (k, Arc::new(RwLock::new(v)))).collect();
    Arc::new(RwLock::new(shared_data))
}

#[derive(Clone, Getters)]
pub struct HistoryBlockData {
    thread_id: ThreadIdentifier,
    last_processed_block_height: BlockHeight,
    data_len: usize,
    data: [(monotree::Hash, monotree::Hash); HISTORY_PROOF_WINDOW_SIZE],
}

#[derive(Getters, Serialize, Deserialize)]
struct HistoryBlockDataSerDe {
    thread_id: ThreadIdentifier,
    last_processed_block_height: BlockHeight,
    data_len: usize,
    data: Vec<(monotree::Hash, monotree::Hash)>,
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

impl From<&AckiNackiEnvelopeHash> for monotree::Hash {
    fn from(hash: &AckiNackiEnvelopeHash) -> Self {
        hash.0
    }
}

impl HistoryBlockData {
    pub fn new(thread_id: ThreadIdentifier) -> Self {
        let data =
            vec![(monotree::Hash::default(), monotree::Hash::default()); HISTORY_PROOF_WINDOW_SIZE]
                .try_into()
                .expect("should not fail");
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

    // // update_from_block fn is valid only for Layer 1
    // pub fn update_from_block(&mut self, envelope: &Envelope<GoshBLS, AckiNackiBlock>) -> anyhow::Result<()> {
    //     let block = envelope.data();
    //     ensure!(*block.common_section().thread_id() == self.thread_id, "Tried to update history with a block from different thread");
    //     let block_height = block.common_section().block_height().clone();
    //     ensure!(block_height.height() > self.last_processed_block_height.next(&self.thread_id).height(), "Tried to update history with a block height that is less or equal to the last processed height");
    //     self.last_processed_block_height = block_height;
    //     let index = (block_height.height().clone() as usize) % HISTORY_PROOF_WINDOW_SIZE;
    //     ensure!(self.data_len == index, "History block data length mismatch");
    //     self.data[index] = ((&block.identifier()).into(), (&envelope_hash(envelope)).into());
    //     self.data_len += 1;
    //     Ok(())
    // }

    pub fn update_from_pure_data(
        &mut self,
        key: monotree::Hash,
        leaf: monotree::Hash,
        block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        tracing::trace!("HistoryBlockData: update: self.data_len={}, self.last_processed_block_height={}, new_height={}", self.data_len, self.last_processed_block_height.height(), block_height.height());
        tracing::trace!(
            "HistoryBlockData: update_from_pure_data key={:?} leaf={:?}",
            hex::encode(key),
            hex::encode(leaf)
        );
        tracing::trace!(
            "HistoryBlockData: update_from_pure_data data={:?}",
            self.data
                .iter()
                .map(|(v, u)| format!("({},{})", hex::encode(v), hex::encode(u)))
                .collect::<Vec<_>>()
        );
        ensure!(
            *self.last_processed_block_height.height() == 0
                || block_height.height() > self.last_processed_block_height.height()
        );
        if self.data_len == HISTORY_PROOF_WINDOW_SIZE {
            self.clear_data();
        }
        self.data[self.data_len] = (key, leaf);
        self.data_len += 1;
        self.last_processed_block_height = block_height;
        Ok(())
    }

    pub fn clear_data(&mut self) {
        self.data_len = 0;
    }

    // pub fn from_iter(data: Vec<(BlockIdentifier, AckiNackiEnvelopeHash)>) -> anyhow::Result<Self> {
    //     let data = data.iter().ma.try_into().map_err(|_| anyhow!("Failed to convert history data vec to array"))?;
    //     Ok(Self{
    //         last_processed_block_height: BlockHeight::builder()
    //             .thread_identifier(ThreadIdentifier::default()).height(0).build(),
    //         thread_id: ThreadIdentifier::default(),
    //         data,
    //     })
    // }

    pub fn calculate_root_hash(
        &self,
        prev_layer_root_hash: Option<(monotree::Hash, monotree::Hash)>,
    ) -> anyhow::Result<monotree::Hash> {
        tracing::trace!("HistoryBlockData: calculate_root_hash: self.data_len={}, self.last_processed_block_height={}, prev_layer_root_hash={:?}", self.data_len, self.last_processed_block_height.height(), prev_layer_root_hash.as_ref().map(|(hash, _)| hex::encode(hash)));
        ensure!(self.data_len == HISTORY_PROOF_WINDOW_SIZE, "History block data length mismatch");
        let mut tree =
            monotree::Monotree::<monotree::database::MemoryDB, monotree::hasher::Blake3>::new(""); // dbpath is ignored for MemoryDb
        let mut keys = vec![prev_layer_root_hash.map(|t| t.0).unwrap_or_default()];
        keys.extend(self.data.iter().map(|(k, _)| *k).collect::<Vec<_>>());
        let mut leaves: Vec<_> = vec![prev_layer_root_hash.map(|t| t.1).unwrap_or_default()];
        leaves.extend(self.data.iter().map(|(_, h)| *h).collect::<Vec<_>>());
        tracing::trace!(
            "HistoryBlockData: calculate_root_hash keys={:?}, leaves={:?}",
            keys.iter().map(hex::encode).collect::<Vec<_>>(),
            leaves.iter().map(hex::encode).collect::<Vec<_>>()
        );
        let root = tree
            .inserts(None.as_ref(), &keys, &leaves)?
            .ok_or(anyhow::format_err!("Failed to compute merkle tree root"));
        tracing::trace!(
            "HistoryBlockData: calculate_root_hash root={:?}",
            root.as_ref().map(hex::encode)
        );
        root
    }
}
