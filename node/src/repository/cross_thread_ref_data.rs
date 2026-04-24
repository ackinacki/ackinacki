use std::collections::HashMap;
use std::sync::Arc;

use anyhow::ensure;
use derive_getters::Getters;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use serde::Deserialize;
use serde::Serialize;
// Re-export the trait from thread-reference-state
pub use thread_reference_state::CrossThreadRefDataTrait;
use typed_builder::TypedBuilder;

use crate::message::identifier::MessageIdentifier;
use crate::repository::WrappedMessage;
use crate::types::account::WrappedAccount;
use crate::types::AccountInbox;
use crate::types::BlockSeqNo;
use crate::types::ThreadsTable;

/// This structure has to have minimum and complete data required
/// for other threads to reference a state at a particular block.
#[derive(TypedBuilder, Clone, Serialize, Deserialize, Getters, Debug)]
pub struct CrossThreadRefData {
    block_identifier: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    /// This is an identifier of the block thread (An input state to generate this data)
    block_thread_identifier: ThreadIdentifier,
    // This stores outbound messages for THIS block only. It does not aggregate
    // this info over multiple blocks.
    // TODO: research possible optimizations: we don't have to store messages, seems better to have
    // cell instead. And key is not used now, but it can be used in case of cell value
    outbound_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    outbound_accounts: HashMap<AccountRouting, (Option<WrappedAccount>, Option<AccountInbox>)>,
    threads_table: ThreadsTable,
    parent_block_identifier: BlockIdentifier,
    block_refs: Vec<BlockIdentifier>,
    /// True when this is the last block produced for the thread.
    /// After a tombstone block is referenced via `move_refs`, the thread is
    /// removed from the reference state since no further blocks will follow.
    #[builder(default = false)]
    is_tombstone: bool,
}

impl CrossThreadRefData {
    pub fn as_reference_state_data(&self) -> (ThreadIdentifier, BlockIdentifier, BlockSeqNo) {
        (self.block_thread_identifier, self.block_identifier, self.block_seq_no)
    }

    pub fn set_threads_table(&mut self, threads_table: ThreadsTable) {
        self.threads_table = threads_table;
    }

    pub fn set_block_identifier(&mut self, block_identifier: BlockIdentifier) {
        self.block_identifier = block_identifier;
    }

    pub fn set_block_thread_identifier(&mut self, block_thread_identifier: ThreadIdentifier) {
        self.block_thread_identifier = block_thread_identifier;
    }

    pub fn set_parent_block_identifier(&mut self, parent_block_identifier: BlockIdentifier) {
        self.parent_block_identifier = parent_block_identifier;
    }

    pub fn set_block_refs(&mut self, block_refs: Vec<BlockIdentifier>) {
        self.block_refs = block_refs;
    }

    pub fn set_tombstone(&mut self, tombstone: bool) {
        self.is_tombstone = tombstone;
    }

    pub fn get_produced_threads_table(&self) -> &ThreadsTable {
        &self.threads_table
    }

    pub fn refs(&self) -> &Vec<BlockIdentifier> {
        &self.block_refs
    }

    // This block produced a table that requires new threads to be spawned
    pub fn spawned_threads(&self) -> Vec<ThreadIdentifier> {
        let mut threads: Vec<ThreadIdentifier> = self.threads_table.rows().map(|e| e.1).collect();
        threads.retain(|e| e.is_spawning_block(&self.block_identifier));
        threads
    }

    pub fn finalize_block_identifier_dependencies(
        &mut self,
        final_block_id: BlockIdentifier,
        final_parent_block_id: BlockIdentifier,
        final_thread_identifier: ThreadIdentifier,
        resolved_threads_table: Option<ThreadsTable>,
    ) -> anyhow::Result<()> {
        self.set_block_identifier(final_block_id);
        self.set_parent_block_identifier(final_parent_block_id);
        self.set_block_thread_identifier(final_thread_identifier);
        if let Some(resolved_threads_table) = resolved_threads_table {
            self.set_threads_table(resolved_threads_table);
        }
        self.validate_persisted_invariants(Some(&final_block_id))
    }

    pub fn validate_persisted_invariants(
        &self,
        persisted_key: Option<&BlockIdentifier>,
    ) -> anyhow::Result<()> {
        if let Some(persisted_key) = persisted_key {
            ensure!(
                persisted_key == &self.block_identifier,
                "CrossThreadRefData key mismatch: persisted under {persisted_key}, payload has {}",
                self.block_identifier
            );
        }

        ensure!(
            self.block_identifier != BlockIdentifier::default()
                || self.block_seq_no == BlockSeqNo::default(),
            "Persisted CrossThreadRefData uses default block id for non-genesis seq_no {}",
            self.block_seq_no
        );

        let spawned_threads = self.spawned_threads();
        for spawned_thread in spawned_threads {
            ensure!(
                spawned_thread.is_spawning_block(&self.block_identifier),
                "Spawned thread {spawned_thread:?} is not derived from finalized block {}",
                self.block_identifier
            );
        }

        Ok(())
    }

    // This method filters outbound messages of THIS block only.
    // It does not include messages generated in previous blocks.
    // pub fn select_cross_thread_messages<F>(
    // &self,
    // Function that returns True for messages that should be included
    // to the result. False otherwise.
    // mut filter: F,
    // ) -> std::vec::Vec<WrappedMessage>
    // where
    // F: FnMut(&WrappedMessage) -> bool,
    // {
    // let mut filtered_messages = vec![];
    // for messages in self.outbound_messages.values() {
    // TODO: Remake internal field and this function
    // if messages.is_empty() {
    // continue;
    // }
    // if filter(&messages[0]) {
    // filtered_messages.extend_from_slice(messages);
    // }
    // }
    // filtered_messages
    // }
    // pub fn select_cross_thread_accounts<F>(
    // &self,
    // Function that returns True for messages that should be included
    // to the result. False otherwise.
    // mut filter: F,
    // ) -> std::vec::Vec<WrappedAccount>
    // where
    // F: FnMut(&AccountRouting) -> bool,
    // {
    // let mut filtered_accounts = vec![];
    // for (routing, account) in &self.outbound_accounts {
    // if filter(routing) {
    // filtered_accounts.push(account.clone());
    // }
    // }
    // filtered_accounts
    // }
}

// Implement the trait for ThreadReferencesState
impl CrossThreadRefDataTrait for CrossThreadRefData {
    type BlockId = BlockIdentifier;
    type SeqNo = thread_reference_state::BlockSeqNo;
    type ThreadId = ThreadIdentifier;

    fn block_identifier(&self) -> &Self::BlockId {
        &self.block_identifier
    }

    fn block_seq_no(&self) -> &Self::SeqNo {
        // Note: We cannot return a reference to a converted value since it would be a temporary.
        // Instead, we rely on the fact that both BlockSeqNo types have the same memory layout.
        // Safety: Both types are repr(transparent) wrappers around u32 with identical layout.
        unsafe {
            &*((&self.block_seq_no) as *const BlockSeqNo
                as *const thread_reference_state::BlockSeqNo)
        }
    }

    fn block_thread_identifier(&self) -> &Self::ThreadId {
        &self.block_thread_identifier
    }

    fn parent_block_identifier(&self) -> &Self::BlockId {
        &self.parent_block_identifier
    }

    fn refs(&self) -> &Vec<Self::BlockId> {
        &self.block_refs
    }

    fn spawned_threads(&self) -> Vec<Self::ThreadId> {
        self.spawned_threads()
    }

    fn as_reference_state_data(&self) -> (Self::ThreadId, Self::BlockId, Self::SeqNo) {
        let (thread, block, seq_no) = CrossThreadRefData::as_reference_state_data(self);
        (thread, block, seq_no.into())
    }

    fn is_thread_tombstone(&self) -> bool {
        self.is_tombstone
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_id(seed: u8) -> BlockIdentifier {
        BlockIdentifier::new([seed; 32])
    }

    #[test]
    fn split_block_finalization_uses_canonical_final_id_everywhere() -> anyhow::Result<()> {
        let intermediate_block_id = block_id(1);
        let final_block_id = block_id(2);
        let parent_block_id = block_id(3);
        let thread_id = ThreadIdentifier::new(&block_id(9), 7);
        let base_table = ThreadsTable::new();
        let spawned_thread = ThreadIdentifier::new(&intermediate_block_id, 0);
        let mut unresolved_threads_table = base_table.clone();
        unresolved_threads_table.insert_above(
            0,
            crate::bitmask::mask::Bitmask::<AccountRouting>::builder()
                .meaningful_mask_bits(AccountRouting::default())
                .mask_bits(AccountRouting::default())
                .build(),
            spawned_thread,
        )?;
        let mut resolved_threads_table = base_table;
        let final_spawned_thread = ThreadIdentifier::new(&final_block_id, 0);
        resolved_threads_table.insert_above(
            0,
            crate::bitmask::mask::Bitmask::<AccountRouting>::builder()
                .meaningful_mask_bits(AccountRouting::default())
                .mask_bits(AccountRouting::default())
                .build(),
            final_spawned_thread,
        )?;

        let mut ref_data = CrossThreadRefData::builder()
            .block_identifier(intermediate_block_id)
            .block_seq_no(BlockSeqNo::from(1u32))
            .block_thread_identifier(thread_id)
            .outbound_messages(HashMap::new())
            .outbound_accounts(HashMap::new())
            .threads_table(unresolved_threads_table)
            .parent_block_identifier(block_id(4))
            .block_refs(vec![])
            .build();

        ref_data.finalize_block_identifier_dependencies(
            final_block_id,
            parent_block_id,
            thread_id,
            Some(resolved_threads_table.clone()),
        )?;

        assert_eq!(ref_data.block_identifier(), &final_block_id);
        assert_eq!(ref_data.parent_block_identifier(), &parent_block_id);
        assert_eq!(ref_data.get_produced_threads_table(), &resolved_threads_table);
        assert_eq!(ref_data.spawned_threads(), vec![final_spawned_thread]);
        Ok(())
    }

    #[test]
    fn persisted_key_must_match_payload_block_identifier() {
        let ref_data = CrossThreadRefData::builder()
            .block_identifier(block_id(5))
            .block_seq_no(BlockSeqNo::from(1u32))
            .block_thread_identifier(ThreadIdentifier::new(&block_id(8), 0))
            .outbound_messages(HashMap::new())
            .outbound_accounts(HashMap::new())
            .threads_table(ThreadsTable::new())
            .parent_block_identifier(block_id(4))
            .block_refs(vec![])
            .build();

        let err = ref_data.validate_persisted_invariants(Some(&block_id(6))).unwrap_err();
        assert!(err.to_string().contains("key mismatch"));
    }

    #[test]
    fn promotion_path_rewrites_stale_parent_identifier_to_final_parent() -> anyhow::Result<()> {
        let stale_parent_id = block_id(7);
        let final_parent_id = block_id(8);
        let final_block_id = block_id(9);
        let thread_id = ThreadIdentifier::new(&block_id(10), 1);
        let mut ref_data = CrossThreadRefData::builder()
            .block_identifier(block_id(6))
            .block_seq_no(BlockSeqNo::from(2u32))
            .block_thread_identifier(thread_id)
            .outbound_messages(HashMap::new())
            .outbound_accounts(HashMap::new())
            .threads_table(ThreadsTable::new())
            .parent_block_identifier(stale_parent_id)
            .block_refs(vec![])
            .build();

        ref_data.finalize_block_identifier_dependencies(
            final_block_id,
            final_parent_id,
            thread_id,
            None,
        )?;

        assert_eq!(ref_data.block_identifier(), &final_block_id);
        assert_eq!(ref_data.parent_block_identifier(), &final_parent_id);
        assert_ne!(ref_data.parent_block_identifier(), &stale_parent_id);
        assert!(ref_data.validate_persisted_invariants(Some(&final_block_id)).is_ok());
        Ok(())
    }
}
