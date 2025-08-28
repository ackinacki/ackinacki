use std::sync::Arc;

use tvm_block::HashmapAugType;

use crate::bitmask::mask::Bitmask;
use crate::multithreading::load_balancing_service::AckiNackiBlock;
use crate::multithreading::load_balancing_service::OptimisticState;
use crate::types::direct_bit_access_operations::DirectBitAccess;
use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::DAppIdentifier;

#[cfg(feature = "allow-dappid-thread-split")]
const MAX_ROUTE_PART_SPLIT: usize = 2;

#[cfg(not(feature = "allow-dappid-thread-split"))]
const MAX_ROUTE_PART_SPLIT: usize = 1;

#[derive(Clone, Copy, Debug)]
pub struct InThreadAccountsLoad {
    total_transactions_count: i64,
    zero_bits_count: [[u32; 256]; 2],
}

impl InThreadAccountsLoad {
    pub fn new_from<TOptimisticState>(
        block: &AckiNackiBlock,
        block_state: Arc<TOptimisticState>,
    ) -> Self
    where
        TOptimisticState: OptimisticState,
    {
        let mut result = Self::default();
        result.append_from(block, block_state);
        result
    }

    #[allow(clippy::needless_range_loop)]
    pub fn best_split(
        &self,
        current_bitmask: &Bitmask<AccountRouting>,
    ) -> Option<Bitmask<AccountRouting>> {
        let meaningful_mask_bits: [[bool; 256]; 2] =
            current_bitmask.meaningful_mask_bits().clone().into();
        let mut best_proposed = None;
        let mut best_proposed_distance = None;
        for outer in 0..MAX_ROUTE_PART_SPLIT {
            for inner in 0..256 {
                let zero_bits_count_at_this: i64 = self.zero_bits_count[outer][inner].into();
                let one_bits_count_at_this: i64 =
                    self.total_transactions_count - zero_bits_count_at_this;
                if zero_bits_count_at_this == 0 || one_bits_count_at_this == 0 {
                    // This bit is not used.
                    continue;
                }
                if meaningful_mask_bits[outer][inner] {
                    // This bit is already taken
                    continue;
                }
                let distance = (one_bits_count_at_this - zero_bits_count_at_this).abs();
                if best_proposed_distance.is_none() || best_proposed_distance.unwrap() > distance {
                    best_proposed_distance = Some(distance);
                    let split_at_index = outer * 256 + inner;
                    best_proposed = Some(split_at_index);
                }
            }
        }
        best_proposed?;
        let split_at_index = best_proposed.unwrap();
        let mut new_meaningful_mask_bits = current_bitmask.meaningful_mask_bits().clone();
        let mut new_mask_bits = current_bitmask.mask_bits().clone();
        new_meaningful_mask_bits.set_bit_value(split_at_index, true);
        new_mask_bits.set_bit_value(split_at_index, true);
        Some(
            Bitmask::builder()
                .mask_bits(new_mask_bits)
                .meaningful_mask_bits(new_meaningful_mask_bits)
                .build(),
        )
    }

    #[allow(clippy::needless_range_loop)]
    pub fn append_from<TOptimisticState>(
        &mut self,
        block: &AckiNackiBlock,
        block_state: Arc<TOptimisticState>,
    ) where
        TOptimisticState: OptimisticState,
    {
        let dapps = block_state.get_dapp_id_table();
        let _ = block
            .tvm_block()
            .read_extra()
            .unwrap_or_default()
            .read_in_msg_descr()
            .unwrap_or_default()
            .iterate_objects(|in_msg| {
                let Ok(message) = in_msg.read_message() else {
                    return Ok(true);
                };
                let Some(destination) = message.int_dst_account_id() else {
                    return Ok(true);
                };
                let destination = destination.into();
                let Some(dapp_id) = dapps.get(&destination).and_then(|e| e.0.clone()).or(message
                    .int_header()
                    .and_then(|hdr| hdr.src_dapp_id.clone())
                    .map(|dapp_uint| DAppIdentifier(AccountAddress(dapp_uint))))
                else {
                    return Ok(true);
                };

                // Calculate messages from existing dapps only.
                let route = AccountRouting::from((Some(dapp_id), destination));
                let bits: [[bool; 256]; 2] = route.into();
                for outer in 0..bits.len() {
                    for inner in 0..bits[outer].len() {
                        if !bits[outer][inner] {
                            self.zero_bits_count[outer][inner] += 1;
                        }
                    }
                }
                self.total_transactions_count += 1;
                Ok(true)
            });
    }

    pub fn add_in_place(&mut self, other: &Self) {
        for outer in 0..self.zero_bits_count.len() {
            for inner in 0..self.zero_bits_count[outer].len() {
                self.zero_bits_count[outer][inner] += other.zero_bits_count[outer][inner];
            }
        }
        self.total_transactions_count += other.total_transactions_count;
    }

    pub fn sub_in_place(&mut self, other: &Self) {
        for outer in 0..self.zero_bits_count.len() {
            for inner in 0..self.zero_bits_count[outer].len() {
                self.zero_bits_count[outer][inner] -= other.zero_bits_count[outer][inner];
            }
        }
        self.total_transactions_count -= other.total_transactions_count;
    }
}

impl Default for InThreadAccountsLoad {
    fn default() -> Self {
        Self { total_transactions_count: 0, zero_bits_count: [[0u32; 256]; 2] }
    }
}
