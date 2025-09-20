// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_types::write_boc;
use tvm_types::Cell;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::ackinacki_block::common_section::CommonSection;
use crate::types::ackinacki_block::common_section::Directives;
use crate::types::ackinacki_block::hash::calculate_hash;
use crate::types::ackinacki_block::hash::debug_hash;
use crate::types::ackinacki_block::hash::Sha256Hash;
use crate::types::BlockHeight;
use crate::types::BlockIdentifier;
use crate::types::BlockRound;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

pub mod as_signatures_map;
pub mod common_section;
pub mod envelope_hash;
pub mod hash;
mod parse_block_accounts_and_messages;
mod serialize;

pub use hash::compare_hashes;

const BLOCK_SUFFIX_LEN: usize = 32;

#[derive(Clone, PartialEq, Eq)]
pub struct AckiNackiBlock {
    common_section: CommonSection,
    block: tvm_block::Block,
    tx_cnt: usize,
    hash: Sha256Hash,
    raw_data: Option<Vec<u8>>,
    block_cell: Option<Cell>,
}

impl Display for AckiNackiBlock {
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
        write!(f, "seq_no: {:?}, id: {:?}", self.seq_no(), self.identifier(),)
    }
}

impl AckiNackiBlock {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        thread_id: ThreadIdentifier,
        block: tvm_block::Block,
        producer_id: NodeIdentifier,
        tx_cnt: usize,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTable>,
        // changed_dapp_ids: DAppIdTableChangeSet,
        round: BlockRound,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
    ) -> Self {
        // Note: according to the node logic we will update common section of every
        // block so there is no need to calculate hash here
        // res.hash = res.calculate_hash().expect("Failed to calculate wrapped block
        // hash");
        Self {
            common_section: CommonSection::new(
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
            ),
            block,
            tx_cnt,
            hash: [0; 32],
            raw_data: None,
            block_cell: None,
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

    pub fn parent_seq_no(&self) -> BlockSeqNo {
        BlockSeqNo::from(
            self.block.info.read_struct().unwrap().read_prev_ref().unwrap().prev1().unwrap().seq_no,
        )
    }

    pub fn identifier(&self) -> BlockIdentifier {
        self.block.hash().unwrap().into()
    }

    pub fn seq_no(&self) -> BlockSeqNo {
        BlockSeqNo::from(self.block.info.read_struct().unwrap().seq_no())
    }

    pub fn directives(&self) -> Directives {
        self.common_section.directives.clone()
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

    pub fn get_hash(&self) -> Sha256Hash {
        self.hash
    }

    pub fn get_common_section(&self) -> &CommonSection {
        &self.common_section
    }

    pub fn set_common_section(
        &mut self,
        common_section: CommonSection,
        update_hash: bool,
    ) -> anyhow::Result<()> {
        self.common_section = common_section;
        // To save resources and not serialize block several times, update hash only on the final change
        if update_hash {
            let mut raw_data = self.get_raw_data_without_hash()?;
            self.hash = calculate_hash(&raw_data)?;
            raw_data.extend_from_slice(&self.hash);
            self.raw_data = Some(raw_data);
            #[cfg(feature = "nack_test")]
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

    pub fn is_thread_splitting(&self) -> bool {
        let block_identifier = self.identifier();
        if let Some(this_table) = &self.common_section.threads_table {
            this_table.rows().any(|(_, thread_identifier)| {
                thread_identifier.is_spawning_block(&block_identifier)
            })
        } else {
            // No split on threads_table None possible
            false
        }
    }

    pub fn tvm_block(&self) -> &tvm_block::Block {
        &self.block
    }

    pub fn tx_cnt(&self) -> usize {
        self.tx_cnt
    }

    pub fn time(&self) -> anyhow::Result<u64> {
        Ok(self
            .tvm_block()
            .info
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .gen_utime_ms())
    }
}
