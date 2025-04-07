use std::collections::HashMap;
use std::sync::Arc;

use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::message::identifier::MessageIdentifier;
use crate::repository::optimistic_state::DAppIdTable;
use crate::repository::WrappedMessage;
use crate::types::account::WrappedAccount;
use crate::types::AccountInbox;
use crate::types::AccountRouting;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

/// This structure has to have minimum and complete data required
/// for other threads to reference a state at a particular block.
#[derive(TypedBuilder, Clone, Serialize, Deserialize, Getters)]
pub struct CrossThreadRefData {
    block_identifier: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    /// This is an identifier of the block thread (An input state to generate this data)
    block_thread_identifier: ThreadIdentifier,
    dapp_id_table_diff: DAppIdTable,
    // This stores outbound messages for THIS block only. It does not aggregate
    // this info over multiple blocks.
    // TODO: research possible optimizations: we don't have to store messages, seems better to have
    // cell instead. And key is not used now, but it can be used in case of cell value
    outbound_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    outbound_accounts: HashMap<AccountRouting, (Option<WrappedAccount>, Option<AccountInbox>)>,
    threads_table: ThreadsTable,
    parent_block_identifier: BlockIdentifier,
    block_refs: Vec<BlockIdentifier>,
}

impl CrossThreadRefData {
    pub fn as_reference_state_data(&self) -> (ThreadIdentifier, BlockIdentifier, BlockSeqNo) {
        (self.block_thread_identifier, self.block_identifier.clone(), self.block_seq_no)
    }

    pub fn set_threads_table(&mut self, threads_table: ThreadsTable) {
        self.threads_table = threads_table;
    }

    pub fn set_block_refs(&mut self, block_refs: Vec<BlockIdentifier>) {
        self.block_refs = block_refs;
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
