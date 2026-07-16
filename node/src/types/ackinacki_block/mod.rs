// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use account_state::DurableThreadAccountsStateDiff;
use account_state::DurableThreadAccountsStateDiffSerDe;
use derive_getters::Getters;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::TemporaryBlockId;
use node_types::ThreadIdentifier;
use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_types::write_boc;
use tvm_types::Cell;
use versioned_struct::versioned;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::live_metrics::LiveAckiNackiBlockCounter;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::ackinacki_block::common_section::CommonSection;
use crate::types::ackinacki_block::common_section::CommonSectionOld;
use crate::types::ackinacki_block::common_section::CommonSectionVersioned;
use crate::types::ackinacki_block::common_section::Directives;
use crate::types::ackinacki_block::common_section::ParentBlockId;
use crate::types::ackinacki_block::hash::calculate_hash;
use crate::types::ackinacki_block::hash::debug_hash;
use crate::types::ackinacki_block::hash::Sha256Hash;
use crate::types::BlockHeight;
use crate::types::BlockRound;
use crate::types::BlockSeqNo;
use crate::types::ThreadsTable;
use crate::types::ThreadsTablePrefab;

pub mod as_signatures_map;
pub mod common_section;
pub mod envelope_hash;
pub mod hash;
pub mod merkle;
mod parse_block_accounts_and_messages;
mod serialize;

pub use hash::compare_hashes;

const BLOCK_SUFFIX_LEN: usize = 32;
pub const BLOCK_MERKLE_LEAF_COUNT: usize = 16;
pub const BLOCK_MERKLE_HISTORY_PROOFS_LEAF_INDEX: usize = 0;
pub const BLOCK_MERKLE_COMMON_SECTION_LEAF_INDEX: usize = 1;
pub const BLOCK_MERKLE_OLD_BK_SET_LEAF_INDEX: usize = 2;
pub const BLOCK_MERKLE_NEW_BK_SET_LEAF_INDEX: usize = 3;
pub const BLOCK_MERKLE_TVM_BLOCK_LEAF_INDEX: usize = 4;
pub const BLOCK_MERKLE_DURABLE_STATE_UPDATE_LEAF_INDEX: usize = 5;
pub const BLOCK_MERKLE_TX_COUNT_LEAF_INDEX: usize = 6;
pub const BLOCK_MERKLE_PROOF_BLOCK_REFS_LEAF_INDEX: usize = 7;
pub const BLOCK_MERKLE_TRACKED_EXT_OUT_MESSAGES_ROOT_LEAF_INDEX: usize = 8;
pub const BLOCK_MERKLE_PADDING_START_LEAF_INDEX: usize = 9;

#[versioned]
#[derive(Clone, PartialEq, Eq, Getters)]
pub struct AckiNackiBlock {
    #[future]
    common_section: CommonSection,
    #[legacy]
    common_section: CommonSectionOld,
    block: tvm_block::Block,
    tx_cnt: usize,
    hash: Sha256Hash,
    raw_data: Option<Vec<u8>>,
    block_cell: Option<Cell>,
    durable_state_update: DurableThreadAccountsStateDiff,
    /// Cached canonical block identifier.
    /// Set once after the final set_common_section(_, true) call or on deserialization.
    /// Not serialized — reconstructed from block data.
    cached_block_id: Option<BlockIdentifier>,
    /// Runtime-only marker for blocks that were received in the previous block-id format.
    /// Such blocks must keep using the old 4-leaf Merkle root as their canonical ID even if
    /// their common section is updated after deserialization.
    #[future]
    legacy_merkle_block_id: bool,
    /// Runtime-only live instance counter. It must never be serialized or deserialized.
    _live_counter: LiveAckiNackiBlockCounter,
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum AckiNackiBlockVersioned {
    New(AckiNackiBlock),
    Old(AckiNackiBlockOld),
}

impl Display for AckiNackiBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let parent = self.common_section.parent_block_id();
        let identifier = parent.block_id().map(|_| self.identifier());
        write!(
            f,
            "seq_no: {:?}, id: {:?}, tx_cnt: {}, hash: {}, time: {}, common_section: {:?}, parent: {:?}, durable_accounts_update_len: {}",
            self.seq_no(),
            identifier,
            self.tx_cnt,
            debug_hash(&self.hash),
            self.time().unwrap_or(0),
            self.common_section,
            parent,
            self.durable_state_update.accounts.len(),
        )
    }
}

impl Display for AckiNackiBlockOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seq_no: {:?}, id: {:?}, tx_cnt: {}, hash: {}, time: {}, common_section: {:?}, parent: {:?}",
            self.seq_no(),
            self.identifier(),
            self.tx_cnt,
            debug_hash(&self.hash),
            self.time().unwrap_or(0),
            self.common_section,
            self.parent(),
        )
    }
}

impl Debug for AckiNackiBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let identifier =
            self.common_section.parent_block_id().block_id().map(|_| self.identifier());
        write!(f, "seq_no: {:?}, id: {:?}", self.seq_no(), identifier,)
    }
}

impl AckiNackiBlock {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_block_id: BlockIdentifier,
        thread_id: ThreadIdentifier,
        block: tvm_block::Block,
        producer_id: NodeIdentifier,
        tx_cnt: usize,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTablePrefab>,
        // changed_dapp_ids: DAppIdTableChangeSet,
        round: BlockRound,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
        #[cfg(feature = "protocol_version_hash_in_block")]
        protocol_version_hash: ProtocolVersionHash,
        durable_state_update: DurableThreadAccountsStateDiff,
        tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,
        tracked_ext_out_messages_root: [u8; 32],
    ) -> Self {
        Self::new_with_parent_block_id(
            ParentBlockId::Block(parent_block_id),
            thread_id,
            block,
            producer_id,
            tx_cnt,
            block_keeper_set_changes,
            verify_complexity,
            refs,
            threads_table,
            round,
            block_height,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number_diff,
            #[cfg(feature = "protocol_version_hash_in_block")]
            protocol_version_hash,
            durable_state_update,
            tracked_ext_out_messages,
            tracked_ext_out_messages_root,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_unresolved_parent(
        parent_temp_id: TemporaryBlockId,
        thread_id: ThreadIdentifier,
        block: tvm_block::Block,
        producer_id: NodeIdentifier,
        tx_cnt: usize,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTablePrefab>,
        round: BlockRound,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
        #[cfg(feature = "protocol_version_hash_in_block")]
        protocol_version_hash: ProtocolVersionHash,
        durable_state_update: DurableThreadAccountsStateDiff,
        tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,
        tracked_ext_out_messages_root: [u8; 32],
    ) -> Self {
        Self::new_with_parent_block_id(
            ParentBlockId::Unresolved(parent_temp_id),
            thread_id,
            block,
            producer_id,
            tx_cnt,
            block_keeper_set_changes,
            verify_complexity,
            refs,
            threads_table,
            round,
            block_height,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number_diff,
            #[cfg(feature = "protocol_version_hash_in_block")]
            protocol_version_hash,
            durable_state_update,
            tracked_ext_out_messages,
            tracked_ext_out_messages_root,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_parent_block_id(
        parent_block_id: ParentBlockId,
        thread_id: ThreadIdentifier,
        block: tvm_block::Block,
        producer_id: NodeIdentifier,
        tx_cnt: usize,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTablePrefab>,
        round: BlockRound,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
        #[cfg(feature = "protocol_version_hash_in_block")]
        protocol_version_hash: ProtocolVersionHash,
        durable_state_update: DurableThreadAccountsStateDiff,
        tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,
        tracked_ext_out_messages_root: [u8; 32],
    ) -> Self {
        // Note: according to the node logic we will update common section of every
        // block so there is no need to calculate hash here
        // res.hash = res.calculate_hash().expect("Failed to calculate wrapped block
        // hash");
        Self {
            common_section: CommonSection::new_with_parent_block_id(
                parent_block_id,
                thread_id,
                round,
                producer_id,
                block_keeper_set_changes,
                verify_complexity,
                refs,
                threads_table,
                // changed_dapp_ids,
                block_height,
                #[cfg(feature = "monitor-accounts-number")]
                accounts_number_diff,
                #[cfg(feature = "protocol_version_hash_in_block")]
                protocol_version_hash,
                tracked_ext_out_messages,
                tracked_ext_out_messages_root,
            ),
            block,
            tx_cnt,
            hash: [0; 32],
            raw_data: None,
            block_cell: None,
            durable_state_update,
            cached_block_id: None,
            legacy_merkle_block_id: false,
            _live_counter: LiveAckiNackiBlockCounter::new(),
        }
    }

    pub fn raw_block_data(&self) -> anyhow::Result<(Vec<u8>, Cell)> {
        if let Some(raw_data) = &self.raw_data {
            let (common_section_len_data, rest) = raw_data.split_at(8);
            let common_section_len =
                usize::from_be_bytes(common_section_len_data.try_into().unwrap());
            let (_common_section_data, rest) = rest.split_at(common_section_len);

            let (block_len_data, rest) = rest.split_at(8);
            let block_len = usize::from_be_bytes(block_len_data.try_into().unwrap());
            let (block_data, _rest) = rest.split_at(block_len);
            if self.block_cell.is_some() {
                Ok((block_data.to_vec(), self.block_cell.clone().unwrap()))
            } else {
                let block_cell = self
                    .block
                    .serialize()
                    .map_err(|e| anyhow::format_err!("Failed to serialize block: {e}"))?;
                Ok((block_data.to_vec(), block_cell))
            }
        } else if self.block_cell.is_some() {
            let block_cell = self.block_cell.clone().unwrap();
            let data = write_boc(&block_cell)
                .map_err(|e| anyhow::format_err!("Failed to write block cell to bytes: {e}"))?;
            Ok((data, block_cell))
        } else {
            let block_cell = self
                .block
                .serialize()
                .map_err(|e| anyhow::format_err!("Failed to serialize block: {e}"))?;
            let data = write_boc(&block_cell)
                .map_err(|e| anyhow::format_err!("Failed to write block cell to bytes: {e}"))?;
            Ok((data, block_cell))
        }
    }

    pub fn parent(&self) -> BlockIdentifier {
        *self.common_section.parent_block_id().expect_block_id()
    }

    pub fn parent_seq_no(&self) -> BlockSeqNo {
        BlockSeqNo::from(
            self.block.info.read_struct().unwrap().read_prev_ref().unwrap().prev1().unwrap().seq_no,
        )
    }

    /// TVM block's internal parent reference (root_hash from prev_ref).
    /// Kept for internal TVM compatibility; `parent()` should be used for Block ID purposes.
    pub fn tvm_parent(&self) -> BlockIdentifier {
        BlockIdentifier::from(
            self.block
                .info
                .read_struct()
                .unwrap()
                .read_prev_ref()
                .unwrap()
                .prev1()
                .unwrap()
                .root_hash,
        )
    }

    pub fn identifier(&self) -> BlockIdentifier {
        if self.legacy_merkle_block_id {
            return BlockIdentifier::new(self.old_merkle_block_id());
        }
        if let Some(cached) = self.cached_block_id {
            return cached;
        }
        // Fallback: compute on-the-fly (happens before common_section is finalized)
        BlockIdentifier::new(self.merkle_block_id())
    }

    /// TVM block representation hash (the old Block ID, kept for compatibility).
    pub fn tvm_block_hash(&self) -> BlockIdentifier {
        self.block.hash().unwrap().into()
    }

    pub fn uses_legacy_merkle_block_id(&self) -> bool {
        self.legacy_merkle_block_id
    }

    /// Compute the previous block ID scheme used before the history-proof Merkle root.
    /// Leaves:
    /// 1. Hash of ALL old CommonSection fields
    /// 2. TVM Block representation hash
    /// 3. Hash of durable_state_update
    /// 4. tx_cnt as bytes
    fn old_merkle_block_id(&self) -> crate::types::ackinacki_block::merkle::MerkleHash {
        use crate::types::ackinacki_block::merkle::leaf_hash;
        use crate::types::ackinacki_block::merkle::merkle_root;

        let common_section_data = self.common_section.full_hash_data_legacy();
        let leaf_common = leaf_hash(&common_section_data);

        let tvm_hash_bytes =
            self.block.hash().expect("TVM block must have hash").as_slice().to_vec();
        let leaf_tvm: [u8; 32] = tvm_hash_bytes.try_into().expect("Hash must be 32 bytes");

        let durable: DurableThreadAccountsStateDiffSerDe = self.durable_state_update.clone().into();
        let durable_data = bincode::serialize(&durable).expect("Must serialize durable state");
        let leaf_durable = leaf_hash(&durable_data);

        let leaf_tx = leaf_hash(&self.tx_cnt.to_be_bytes());

        merkle_root(&[leaf_common, leaf_tvm, leaf_durable, leaf_tx])
    }

    /// Compute the Block ID as the root of a 16-leaf SHA-256 Merkle tree.
    ///
    /// L0: Poseidon(layer history proof preimage)
    /// L1: SHA-256(bincode(CommonSection))
    /// L2: Poseidon(old BK set commitment)
    /// L3: Poseidon(new BK set commitment)
    /// L4: TVM Block representation hash
    /// L5: SHA-256(bincode(durable_state_update))
    /// L6: SHA-256(tx_cnt.to_be_bytes())
    /// L7: Poseidon Merkle root of referenced blocks: parent, then refs
    /// L8: tracked_ext_out_messages_root
    /// L9..L15: zero padding
    pub fn block_merkle_leaves(
        &self,
    ) -> [crate::types::ackinacki_block::merkle::MerkleHash; BLOCK_MERKLE_LEAF_COUNT] {
        use crate::types::ackinacki_block::merkle::leaf_hash;
        use crate::types::history_proof::compute_referenced_blocks_root;
        use crate::types::history_proof::history_proofs_l0;

        let l0 = history_proofs_l0(
            self.common_section
                .history_proofs()
                .iter()
                .map(|(layer, proof_layer)| (*layer, proof_layer.root_hash())),
        );

        let l1 = leaf_hash(&self.common_section.full_hash_data());

        let (l2, l3) =
            if let Some(proof_data) = self.common_section.block_keeper_set_change_proof_data() {
                let transition_hashes = proof_data.transition_hashes();
                (*transition_hashes.old_bk_set_hash(), *transition_hashes.new_bk_set_hash())
            } else {
                ([0u8; 32], [0u8; 32])
            };

        let l4 = {
            let tvm_hash_bytes =
                self.block.hash().expect("TVM block must have hash").as_slice().to_vec();
            let hash: [u8; 32] = tvm_hash_bytes.try_into().expect("Hash must be 32 bytes");
            hash
        };

        let durable: DurableThreadAccountsStateDiffSerDe = self.durable_state_update.clone().into();
        let durable_data = bincode::serialize(&durable).expect("Must serialize durable state");
        let l5 = leaf_hash(&durable_data);

        let l6 = leaf_hash(&self.tx_cnt.to_be_bytes());
        let mut proof_block_refs = Vec::with_capacity(1 + self.common_section.refs().len());
        proof_block_refs.push(*self.parent().as_array());
        proof_block_refs
            .extend(self.common_section.refs().iter().map(|block_ref| *block_ref.as_array()));
        let l7 = compute_referenced_blocks_root(proof_block_refs.iter());
        let l8 = *self.common_section.tracked_ext_out_messages_root();

        let mut leaves = [[0u8; 32]; BLOCK_MERKLE_LEAF_COUNT];
        leaves[BLOCK_MERKLE_HISTORY_PROOFS_LEAF_INDEX] = l0;
        leaves[BLOCK_MERKLE_COMMON_SECTION_LEAF_INDEX] = l1;
        leaves[BLOCK_MERKLE_OLD_BK_SET_LEAF_INDEX] = l2;
        leaves[BLOCK_MERKLE_NEW_BK_SET_LEAF_INDEX] = l3;
        leaves[BLOCK_MERKLE_TVM_BLOCK_LEAF_INDEX] = l4;
        leaves[BLOCK_MERKLE_DURABLE_STATE_UPDATE_LEAF_INDEX] = l5;
        leaves[BLOCK_MERKLE_TX_COUNT_LEAF_INDEX] = l6;
        leaves[BLOCK_MERKLE_PROOF_BLOCK_REFS_LEAF_INDEX] = l7;
        leaves[BLOCK_MERKLE_TRACKED_EXT_OUT_MESSAGES_ROOT_LEAF_INDEX] = l8;
        leaves
    }

    fn merkle_block_id(&self) -> crate::types::ackinacki_block::merkle::MerkleHash {
        use crate::types::ackinacki_block::merkle::merkle_root;
        merkle_root(&self.block_merkle_leaves())
    }

    pub fn block_id_with_merkle_leaves(
        &self,
    ) -> (
        crate::types::ackinacki_block::merkle::MerkleHash,
        [crate::types::ackinacki_block::merkle::MerkleHash; BLOCK_MERKLE_LEAF_COUNT],
    ) {
        use crate::types::ackinacki_block::merkle::merkle_root;
        let leaves = self.block_merkle_leaves();
        (merkle_root(&leaves), leaves)
    }

    pub fn seq_no(&self) -> BlockSeqNo {
        BlockSeqNo::from(self.block.info.read_struct().unwrap().seq_no())
    }

    pub fn directives(&self) -> &Directives {
        self.common_section.directives()
    }

    pub fn check_hash(&self) -> anyhow::Result<bool> {
        tracing::trace!("Check hash for block {:?} {:?}", self.seq_no(), self.identifier());
        let real_hash = if let Some(raw_data) = &self.raw_data {
            assert!(raw_data.len() > BLOCK_SUFFIX_LEN);
            tracing::trace!("Use raw data to check hash");
            let (data_for_hash, _) = raw_data.split_at(raw_data.len() - BLOCK_SUFFIX_LEN);
            calculate_hash(data_for_hash)?
        } else {
            tracing::trace!("Serialize data to check hash");
            let raw_data = self.get_raw_data_without_hash()?;

            calculate_hash(&raw_data)?
        };
        tracing::trace!(
            "Calculated hash: {}, block hash: {}",
            debug_hash(&real_hash),
            debug_hash(&self.hash)
        );
        Ok(self.hash == real_hash)
    }

    pub fn set_common_section(
        &mut self,
        common_section: CommonSection,
        update_hash: bool,
    ) -> anyhow::Result<()> {
        self.common_section = common_section;
        // Invalidate block ID cache — common section changed
        self.cached_block_id = None;

        // To save resources and not serialize block several times, update hash only on the final change
        if update_hash {
            // Compute and cache the block identifier.
            self.cached_block_id = Some(if self.legacy_merkle_block_id {
                BlockIdentifier::new(self.old_merkle_block_id())
            } else {
                BlockIdentifier::new(self.merkle_block_id())
            });

            let mut raw_data = self.get_raw_data_without_hash()?;
            self.hash = calculate_hash(&raw_data)?;
            raw_data.extend_from_slice(&self.hash);
            self.raw_data = Some(raw_data);
            #[cfg(feature = "test_nack")]
            if self.seq_no() == BlockSeqNo::from(324)
                && self.common_section.producer_id == NodeIdentifier::some_id()
            {
                tracing::trace!(target: "node", "Skip common section to make fake block");
                self.hash = Sha256Hash::default();
                self.raw_data = None;
            }
        }
        Ok(())
    }

    pub fn tvm_block(&self) -> &tvm_block::Block {
        &self.block
    }

    pub fn time(&self) -> anyhow::Result<u64> {
        Ok(self
            .tvm_block()
            .info
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .gen_utime_ms())
    }

    /// Resolve the threads table prefab using the given block identifier.
    /// Returns None if the block has no threads table change.
    pub fn resolve_threads_table(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<ThreadsTable>> {
        match &self.common_section.threads_table() {
            Some(prefab) => Ok(Some(prefab.resolve(block_id)?)),
            None => Ok(None),
        }
    }

    /// Update the parent block identifier (e.g. after the parent block is finalized
    /// and its canonical ID changes). Invalidates the cached block ID.
    pub fn update_parent_block_id(&mut self, new_parent: BlockIdentifier) {
        self.common_section.set_parent_block_id(ParentBlockId::Block(new_parent));
        self.cached_block_id = None;
    }

    pub fn is_thread_splitting(&self) -> bool {
        self.common_section
            .threads_table()
            .as_ref()
            .map(|prefab| prefab.has_insert_instructions())
            .unwrap_or(false)
    }
}

impl AckiNackiBlockOld {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_block_id: BlockIdentifier,
        thread_id: ThreadIdentifier,
        block: tvm_block::Block,
        producer_id: NodeIdentifier,
        tx_cnt: usize,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTablePrefab>,
        round: BlockRound,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
        #[cfg(feature = "protocol_version_hash_in_block")]
        protocol_version_hash: ProtocolVersionHash,
        durable_state_update: DurableThreadAccountsStateDiff,
    ) -> Self {
        Self {
            common_section: CommonSectionOld::new(
                parent_block_id,
                thread_id,
                round,
                producer_id,
                block_keeper_set_changes,
                verify_complexity,
                refs,
                threads_table,
                block_height,
                #[cfg(feature = "monitor-accounts-number")]
                accounts_number_diff,
                #[cfg(feature = "protocol_version_hash_in_block")]
                protocol_version_hash,
            ),
            block,
            tx_cnt,
            hash: [0; 32],
            raw_data: None,
            block_cell: None,
            durable_state_update,
            cached_block_id: None,
            _live_counter: Default::default(),
        }
    }

    pub fn parent(&self) -> BlockIdentifier {
        *self.common_section.parent_block_id().expect_block_id()
    }

    /// Update the parent block identifier after a temporary parent has been promoted.
    /// The old block format cannot serialize an unresolved parent, so production may
    /// build it with a placeholder and resolve it before finalizing the common section.
    pub fn update_parent_block_id(&mut self, new_parent: BlockIdentifier) {
        self.common_section.set_parent_block_id(ParentBlockId::Block(new_parent));
        self.cached_block_id = None;
    }

    pub fn parent_seq_no(&self) -> BlockSeqNo {
        BlockSeqNo::from(
            self.block.info.read_struct().unwrap().read_prev_ref().unwrap().prev1().unwrap().seq_no,
        )
    }

    pub fn tvm_parent(&self) -> BlockIdentifier {
        BlockIdentifier::from(
            self.block
                .info
                .read_struct()
                .unwrap()
                .read_prev_ref()
                .unwrap()
                .prev1()
                .unwrap()
                .root_hash,
        )
    }

    pub fn identifier(&self) -> BlockIdentifier {
        BlockIdentifier::new(self.old_merkle_block_id())
    }

    pub fn tvm_block_hash(&self) -> BlockIdentifier {
        self.block.hash().unwrap().into()
    }

    /// Compute the previous block ID scheme used by blocks produced by retired versions.
    fn old_merkle_block_id(&self) -> crate::types::ackinacki_block::merkle::MerkleHash {
        use crate::types::ackinacki_block::merkle::leaf_hash;
        use crate::types::ackinacki_block::merkle::merkle_root;

        let common_section_data = self.common_section.full_hash_data();
        let leaf_common = leaf_hash(&common_section_data);

        let tvm_hash_bytes =
            self.block.hash().expect("TVM block must have hash").as_slice().to_vec();
        let leaf_tvm: [u8; 32] = tvm_hash_bytes.try_into().expect("Hash must be 32 bytes");

        let durable: DurableThreadAccountsStateDiffSerDe = self.durable_state_update.clone().into();
        let durable_data = bincode::serialize(&durable).expect("Must serialize durable state");
        let leaf_durable = leaf_hash(&durable_data);

        let leaf_tx = leaf_hash(&self.tx_cnt.to_be_bytes());

        merkle_root(&[leaf_common, leaf_tvm, leaf_durable, leaf_tx])
    }

    pub fn seq_no(&self) -> BlockSeqNo {
        BlockSeqNo::from(self.block.info.read_struct().unwrap().seq_no())
    }

    pub fn directives(&self) -> &Directives {
        self.common_section.directives()
    }

    pub fn set_common_section(
        &mut self,
        common_section: CommonSectionOld,
        update_hash: bool,
    ) -> anyhow::Result<()> {
        self.common_section = common_section;

        if update_hash {
            self.cached_block_id = Some(BlockIdentifier::new(self.old_merkle_block_id()));
            let mut raw_data = self.get_raw_data_without_hash()?;
            self.hash = calculate_hash(&raw_data)?;
            raw_data.extend_from_slice(&self.hash);
            self.raw_data = Some(raw_data);
            #[cfg(feature = "test_nack")]
            if self.seq_no() == BlockSeqNo::from(324)
                && self.common_section.producer_id == NodeIdentifier::some_id()
            {
                tracing::trace!(target: "node", "Skip common section to make fake block");
                self.hash = Sha256Hash::default();
                self.raw_data = None;
            }
        }
        Ok(())
    }

    pub fn raw_block_data(&self) -> anyhow::Result<(Vec<u8>, Cell)> {
        if let Some(raw_data) = &self.raw_data {
            let (common_section_len_data, rest) = raw_data.split_at(8);
            let common_section_len =
                usize::from_be_bytes(common_section_len_data.try_into().unwrap());
            let (_common_section_data, rest) = rest.split_at(common_section_len);

            let (block_len_data, rest) = rest.split_at(8);
            let block_len = usize::from_be_bytes(block_len_data.try_into().unwrap());
            let (block_data, _rest) = rest.split_at(block_len);
            if self.block_cell.is_some() {
                Ok((block_data.to_vec(), self.block_cell.clone().unwrap()))
            } else {
                let block_cell = self
                    .block
                    .serialize()
                    .map_err(|e| anyhow::format_err!("Failed to serialize block: {e}"))?;
                Ok((block_data.to_vec(), block_cell))
            }
        } else if self.block_cell.is_some() {
            let block_cell = self.block_cell.clone().unwrap();
            let data = write_boc(&block_cell)
                .map_err(|e| anyhow::format_err!("Failed to write block cell to bytes: {e}"))?;
            Ok((data, block_cell))
        } else {
            let block_cell = self
                .block
                .serialize()
                .map_err(|e| anyhow::format_err!("Failed to serialize block: {e}"))?;
            let data = write_boc(&block_cell)
                .map_err(|e| anyhow::format_err!("Failed to write block cell to bytes: {e}"))?;
            Ok((data, block_cell))
        }
    }

    pub fn tvm_block(&self) -> &tvm_block::Block {
        &self.block
    }

    pub fn time(&self) -> anyhow::Result<u64> {
        Ok(self
            .tvm_block()
            .info
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .gen_utime_ms())
    }

    /// Resolve the threads table prefab using the given block identifier.
    /// Returns None if the block has no threads table change.
    pub fn resolve_threads_table(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<ThreadsTable>> {
        match &self.common_section.threads_table() {
            Some(prefab) => Ok(Some(prefab.resolve(block_id)?)),
            None => Ok(None),
        }
    }

    pub fn is_thread_splitting(&self) -> bool {
        self.common_section
            .threads_table()
            .as_ref()
            .map(|prefab| prefab.has_insert_instructions())
            .unwrap_or(false)
    }
}

impl AckiNackiBlockVersioned {
    pub fn seq_no(&self) -> BlockSeqNo {
        match self {
            AckiNackiBlockVersioned::New(acki_block) => acki_block.seq_no(),
            AckiNackiBlockVersioned::Old(acki_block) => acki_block.seq_no(),
        }
    }

    pub fn identifier(&self) -> BlockIdentifier {
        match self {
            AckiNackiBlockVersioned::New(acki_block) => acki_block.identifier(),
            AckiNackiBlockVersioned::Old(acki_block) => acki_block.identifier(),
        }
    }

    pub fn tvm_block_hash(&self) -> BlockIdentifier {
        match self {
            AckiNackiBlockVersioned::New(acki_block) => acki_block.tvm_block_hash(),
            AckiNackiBlockVersioned::Old(acki_block) => acki_block.tvm_block_hash(),
        }
    }

    pub fn common_section(&self) -> CommonSectionVersioned {
        match self {
            AckiNackiBlockVersioned::New(acki_block) => {
                CommonSectionVersioned::New(acki_block.common_section().clone())
            }
            AckiNackiBlockVersioned::Old(acki_block) => {
                CommonSectionVersioned::Old(acki_block.common_section().clone())
            }
        }
    }

    pub fn set_common_section(
        &mut self,
        common_section: CommonSectionVersioned,
        update_hash: bool,
    ) -> anyhow::Result<()> {
        match self {
            AckiNackiBlockVersioned::New(an_block) => {
                let CommonSectionVersioned::New(new_common_section) = common_section else {
                    anyhow::bail!("Wrong common section version");
                };
                an_block.set_common_section(new_common_section, update_hash)
            }
            AckiNackiBlockVersioned::Old(an_block) => {
                let CommonSectionVersioned::Old(new_common_section) = common_section else {
                    anyhow::bail!("Wrong common section version");
                };
                an_block.set_common_section(new_common_section, update_hash)
            }
        }
    }

    pub fn tvm_block(&self) -> &tvm_block::Block {
        match self {
            AckiNackiBlockVersioned::New(an_block) => an_block.tvm_block(),
            AckiNackiBlockVersioned::Old(an_block) => an_block.tvm_block(),
        }
    }

    pub fn tx_cnt(&self) -> usize {
        match self {
            AckiNackiBlockVersioned::New(an_block) => *an_block.tx_cnt(),
            AckiNackiBlockVersioned::Old(an_block) => *an_block.tx_cnt(),
        }
    }

    pub fn parent(&self) -> BlockIdentifier {
        match self {
            AckiNackiBlockVersioned::New(an_block) => an_block.parent(),
            AckiNackiBlockVersioned::Old(an_block) => an_block.parent(),
        }
    }

    pub fn is_thread_splitting(&self) -> bool {
        match self {
            AckiNackiBlockVersioned::New(an_block) => an_block.is_thread_splitting(),
            AckiNackiBlockVersioned::Old(an_block) => an_block.is_thread_splitting(),
        }
    }

    pub fn update_parent_block_id(&mut self, new_parent: BlockIdentifier) {
        match self {
            AckiNackiBlockVersioned::New(an_block) => an_block.update_parent_block_id(new_parent),
            AckiNackiBlockVersioned::Old(an_block) => an_block.update_parent_block_id(new_parent),
        }
    }
}

impl Display for AckiNackiBlockVersioned {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self {
            AckiNackiBlockVersioned::New(acki_block) => write!(formatter, "{}", acki_block),
            AckiNackiBlockVersioned::Old(acki_block) => write!(formatter, "{}", acki_block),
        }
    }
}

#[cfg(test)]
mod tests {
    use tvm_block::BlkPrevInfo;
    use tvm_block::Block;
    use tvm_block::BlockExtra;
    use tvm_block::BlockInfo;
    use tvm_block::ExtBlkRef;
    use tvm_block::MerkleUpdate;
    use tvm_block::ValueFlow;
    use tvm_types::UInt256;

    use super::*;
    use crate::types::bp_selector::ProducerSelector;

    fn make_test_block(seq_no: u32) -> Block {
        let mut info = BlockInfo::new();
        info.set_seq_no(seq_no).unwrap();
        info.set_gen_utime_ms(1_770_201_296_000);
        Block::with_params(
            0,
            info,
            ValueFlow::default(),
            MerkleUpdate::default(),
            BlockExtra::default(),
        )
        .unwrap()
    }

    fn make_test_block_with_parent(seq_no: u32, parent_block_id: BlockIdentifier) -> Block {
        let mut info = BlockInfo::new();
        info.set_seq_no(seq_no).unwrap();
        info.set_gen_utime_ms(1_770_201_296_000);
        info.set_prev_stuff(
            false,
            &BlkPrevInfo::Block {
                prev: ExtBlkRef {
                    end_lt: 0,
                    seq_no: seq_no.saturating_sub(1),
                    root_hash: UInt256::from(*parent_block_id.as_array()),
                    file_hash: UInt256::from([2; 32]),
                },
            },
        )
        .unwrap();
        Block::with_params(
            0,
            info,
            ValueFlow::default(),
            MerkleUpdate::default(),
            BlockExtra::default(),
        )
        .unwrap()
    }

    fn make_block_height() -> BlockHeight {
        BlockHeight::builder().thread_identifier(ThreadIdentifier::default()).height(7).build()
    }

    fn make_selector(parent_block_id: BlockIdentifier) -> ProducerSelector {
        ProducerSelector::builder().rng_seed_block_id(parent_block_id).index(0).build()
    }

    fn make_block(parent_block_id: BlockIdentifier) -> AckiNackiBlock {
        AckiNackiBlock::new(
            parent_block_id,
            ThreadIdentifier::default(),
            make_test_block(1),
            NodeIdentifier::some_id(),
            0,
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            BlockRound::default(),
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
            DurableThreadAccountsStateDiff::default(),
            Default::default(),
            Default::default(),
        )
    }

    #[test]
    fn ackinacki_block_construction_uses_concrete_parent() {
        let parent_block_id = BlockIdentifier::default();
        let block = make_block(parent_block_id);

        assert_eq!(block.parent(), parent_block_id);
        assert_eq!(block.common_section().parent_block_id().block_id(), Some(&parent_block_id));
    }

    #[test]
    fn update_parent_block_id_changes_parent_and_invalidates_identifier_cache() {
        let initial_parent = BlockIdentifier::default();
        let next_parent = BlockIdentifier::new([1; 32]);
        let mut block = make_block(initial_parent);
        block.common_section.set_producer_selector(Some(make_selector(initial_parent)));

        let old_identifier = block.identifier();
        block.update_parent_block_id(next_parent);
        block.common_section.set_producer_selector(Some(make_selector(next_parent)));
        let new_identifier = block.identifier();

        assert_eq!(block.parent(), next_parent);
        assert_ne!(old_identifier, new_identifier);
    }

    #[test]
    fn unresolved_parent_block_cannot_serialize_or_calculate_identifier() {
        let mut block = AckiNackiBlock::new_with_unresolved_parent(
            TemporaryBlockId::generate(),
            ThreadIdentifier::default(),
            make_test_block(1),
            NodeIdentifier::some_id(),
            0,
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            BlockRound::default(),
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
            DurableThreadAccountsStateDiff::default(),
            Default::default(),
            Default::default(),
        );
        block.common_section.set_producer_selector(Some(make_selector(BlockIdentifier::default())));

        let err = bincode::serialize(&block).unwrap_err();
        assert!(err.to_string().contains("unresolved parent block ID"));

        #[allow(clippy::disallowed_methods)]
        let identifier_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block.identifier()));
        assert!(identifier_result.is_err());
    }

    #[test]
    fn versioned_old_block_update_parent_block_id_changes_parent() {
        let initial_parent = BlockIdentifier::default();
        let next_parent = BlockIdentifier::new([2; 32]);
        let old_block = AckiNackiBlockOld::new(
            initial_parent,
            ThreadIdentifier::default(),
            make_test_block_with_parent(2, initial_parent),
            NodeIdentifier::some_id(),
            0,
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            BlockRound::default(),
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
            DurableThreadAccountsStateDiff::default(),
        );
        let mut block = AckiNackiBlockVersioned::Old(old_block);

        block.update_parent_block_id(next_parent);

        assert_eq!(block.parent(), next_parent);
    }

    #[test]
    fn deserializes_old_block_with_legacy_merkle_identifier() {
        let parent_block_id = BlockIdentifier::new([9; 32]);
        let mut old_block = AckiNackiBlockOld::new(
            parent_block_id,
            ThreadIdentifier::default(),
            make_test_block_with_parent(2, parent_block_id),
            NodeIdentifier::some_id(),
            0,
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            BlockRound::default(),
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
            DurableThreadAccountsStateDiff::default(),
        );
        let mut common_section = old_block.common_section.clone();
        common_section.set_producer_selector(Some(make_selector(parent_block_id)));
        old_block.set_common_section(common_section, true).unwrap();
        let expected_identifier = old_block.identifier();

        let encoded = bincode::serialize(&old_block).unwrap();
        let decoded: AckiNackiBlock = bincode::deserialize(&encoded).unwrap();

        assert!(decoded.uses_legacy_merkle_block_id());
        assert_eq!(decoded.parent(), parent_block_id);
        assert_eq!(decoded.identifier(), expected_identifier);
        assert_ne!(decoded.identifier(), decoded.tvm_block_hash());
        assert_ne!(decoded.identifier(), BlockIdentifier::new(decoded.merkle_block_id()));
        assert!(decoded.common_section().history_proofs().is_empty());
        assert_eq!(decoded.common_section().tracked_ext_out_messages_root(), &[0u8; 32]);
        assert!(decoded.common_section().tracked_ext_out_messages().is_empty());
        assert!(decoded.common_section().block_keeper_set_change_proof_data().is_none());
    }

    #[test]
    fn deserialized_old_block_uses_legacy_merkle_after_common_section_update() {
        let parent_block_id = BlockIdentifier::new([9; 32]);
        let mut old_block = AckiNackiBlockOld::new(
            parent_block_id,
            ThreadIdentifier::default(),
            make_test_block_with_parent(2, parent_block_id),
            NodeIdentifier::some_id(),
            0,
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            BlockRound::default(),
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
            DurableThreadAccountsStateDiff::default(),
        );
        let mut old_common_section = old_block.common_section.clone();
        old_common_section.set_producer_selector(Some(make_selector(parent_block_id)));
        old_block.set_common_section(old_common_section, true).unwrap();

        let encoded = bincode::serialize(&old_block).unwrap();
        let mut decoded: AckiNackiBlock = bincode::deserialize(&encoded).unwrap();
        let initial_legacy_identifier = decoded.identifier();

        let mut common_section = decoded.common_section().clone();
        common_section.set_directives(Directives::builder().share_state_resources(true).build());
        decoded.set_common_section(common_section, true).unwrap();

        assert!(decoded.uses_legacy_merkle_block_id());
        assert_eq!(decoded.identifier(), BlockIdentifier::new(decoded.old_merkle_block_id()));
        assert_ne!(decoded.identifier(), initial_legacy_identifier);
        assert_ne!(decoded.identifier(), decoded.tvm_block_hash());
        assert_ne!(decoded.identifier(), BlockIdentifier::new(decoded.merkle_block_id()));
    }
}
