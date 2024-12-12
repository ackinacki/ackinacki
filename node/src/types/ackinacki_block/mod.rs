// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;
use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_types::write_boc;
use tvm_types::Cell;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::bls::BLSSignatureScheme;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::ackinacki_block::common_section::CommonSection;
use crate::types::ackinacki_block::common_section::Directives;
use crate::types::ackinacki_block::hash::calculate_hash;
use crate::types::ackinacki_block::hash::debug_hash;
use crate::types::ackinacki_block::hash::Sha256Hash;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

mod common_section;
mod hash;
mod serialize;

pub use hash::compare_hashes;

const BLOCK_SUFFIX_LEN: usize = 32;

#[derive(Clone)]
pub struct AckiNackiBlock<TBLSSignatureScheme>
where
    TBLSSignatureScheme: BLSSignatureScheme,
    TBLSSignatureScheme::Signature:
        Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
{
    common_section: CommonSection<TBLSSignatureScheme>,
    block: tvm_block::Block,
    processed_ext_messages_cnt: usize,
    tx_cnt: usize,
    hash: Sha256Hash,
    raw_data: Option<Vec<u8>>,
    block_cell: Option<Cell>,
}

impl<TBLSSignatureScheme: BLSSignatureScheme> Display for AckiNackiBlock<TBLSSignatureScheme> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seq_no: {:?}, id: {:?}, tx_cnt: {}, ext_messages_cnt: {}, hash: {}, common_section: {:?}, parent: {:?}",
            self.seq_no(),
            self.identifier(),
            self.tx_cnt,
            self.processed_ext_messages_cnt(),
            debug_hash(&self.hash),
            self.common_section,
            self.parent(),
        )
    }
}

impl<TBLSSignatureScheme: BLSSignatureScheme> Debug for AckiNackiBlock<TBLSSignatureScheme> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "seq_no: {:?}, id: {:?}", self.seq_no(), self.identifier(),)
    }
}

impl<TBLSSignatureScheme: BLSSignatureScheme> AckiNackiBlock<TBLSSignatureScheme> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        thread_id: ThreadIdentifier,
        block: tvm_block::Block,
        processed_ext_messages_cnt: usize,
        producer_id: NodeIdentifier,
        tx_cnt: usize,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
    ) -> Self {
        // Note: according to the node logic we will update common section of every
        // block so there is no need to calculate hash here
        // res.hash = res.calculate_hash().expect("Failed to calculate wrapped block
        // hash");
        Self {
            common_section: CommonSection::new(
                thread_id,
                producer_id,
                block_keeper_set_changes,
                verify_complexity,
                refs,
            ),
            block,
            tx_cnt,
            processed_ext_messages_cnt,
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

    pub fn get_common_section(&self) -> &CommonSection<TBLSSignatureScheme> {
        &self.common_section
    }

    pub fn set_common_section(
        &mut self,
        common_section: CommonSection<TBLSSignatureScheme>,
    ) -> anyhow::Result<()> {
        self.common_section = common_section;
        let mut raw_data = self.get_raw_data_without_hash()?;
        self.hash = calculate_hash(&raw_data)?;
        raw_data.extend_from_slice(&self.hash);
        self.raw_data = Some(raw_data);
        if cfg!(feature = "nack_test")
            && self.seq_no() == BlockSeqNo::from(324)
            && self.common_section.producer_id == 1
        {
            tracing::trace!(target: "node", "Skip common section to make fake block");
            self.hash = Sha256Hash::default();
            self.raw_data = None;
        }
        Ok(())
    }

    pub fn tvm_block(&self) -> &tvm_block::Block {
        &self.block
    }

    pub fn tx_cnt(&self) -> usize {
        self.tx_cnt
    }

    pub fn processed_ext_messages_cnt(&self) -> usize {
        self.processed_ext_messages_cnt
    }
}