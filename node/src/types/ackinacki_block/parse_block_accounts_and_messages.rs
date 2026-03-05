use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use account_state::ThreadAccountsRepository;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use tvm_block::HashmapAugType;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::accounts::NodeThreadAccountsRef;
use crate::repository::accounts::NodeThreadAccountsRepository;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::account::WrappedAccount;
use crate::types::AckiNackiBlock;

impl AckiNackiBlock {
    pub fn get_data_for_postprocessing(
        &self,
        initial_optimistic_state: &mut OptimisticStateImpl,
        updated_shard_state: NodeThreadAccountsRef,
        thread_accounts_repository: &NodeThreadAccountsRepository,
    ) -> anyhow::Result<(
        HashMap<AccountIdentifier, HashSet<MessageIdentifier>>,
        HashMap<AccountIdentifier, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
        HashMap<AccountRouting, Option<WrappedAccount>>,
        HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    )> {
        let mut consumed_internal_messages: HashMap<AccountIdentifier, HashSet<MessageIdentifier>> =
            HashMap::new();
        let mut accounts_that_changed_their_dapp_id: HashMap<
            AccountRouting,
            Option<WrappedAccount>,
        > = HashMap::new();

        let mut produced_internal_messages: HashMap<
            AccountRouting,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        let block_extra = self
            .tvm_block()
            .read_extra()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
        let out_msg_descr = block_extra
            .read_out_msg_descr()
            .map_err(|e| anyhow::format_err!("Failed to read out msg descr: {e}"))?;
        out_msg_descr
            .iterate_objects(|out_msg| {
                let msg = out_msg
                    .read_message()?
                    .ok_or(tvm_types::error!("Failed to read block out message"))?;
                if let Some(dest_account_id) = msg.int_dst_account_id().map(AccountIdentifier::from)
                {
                    if let Some(header) = msg.int_header() {
                        let dest_dapp_id = header
                            .dest_dapp_id
                            .as_ref()
                            .map(DAppIdentifier::from)
                            .unwrap_or_else(|| dest_account_id.use_as_dapp_id());
                        let wrapped_message = WrappedMessage { message: msg };
                        let message_identifier = MessageIdentifier::from(&wrapped_message);
                        let entry = produced_internal_messages
                            .entry(dest_account_id.routing_with(dest_dapp_id))
                            .or_default();
                        entry.push((message_identifier, Arc::new(wrapped_message)));
                    }
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iter out msgs: {e}"))?;
        let in_msg_descr = block_extra
            .read_in_msg_descr()
            .map_err(|e| anyhow::format_err!("Failed to read in msg descr: {e}"))?;
        in_msg_descr
            .iterate_objects(|in_msg| {
                let msg = in_msg.read_message()?;
                if let Some(header) = msg.int_header() {
                    let dst_account_id = AccountIdentifier::from(header.dst.address());
                    let wrapped_message = WrappedMessage { message: msg };
                    let message_identifier = MessageIdentifier::from(&wrapped_message);
                    let entry = consumed_internal_messages.entry(dst_account_id).or_default();
                    entry.insert(message_identifier);
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iter in msgs: {e}"))?;
        // TODO: need change set in common section
        self.block
            .read_extra()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?
            .read_account_blocks()
            .map_err(|e| anyhow::format_err!("Failed to read account blocks: {e}"))?
            .iterate_objects(|account_block| {
                // tracing::trace!(target: "node", "get_data_for_postprocessing: parse {}", account_block.account_id().to_hex_string());
                if account_block.dapp_id_changed() {
                    // tracing::trace!(target: "node", "get_data_for_postprocessing: dapp id changed {}", account_block.account_id().to_hex_string());
                    let acc_id = AccountIdentifier::from(account_block.account_id());
                    if let Some(account) = thread_accounts_repository
                        .state_account(&updated_shard_state, &acc_id.dapp_originator())
                        .map_err(|err| tvm_types::error!("{}", err))?
                    {
                        match account.get_dapp_id() {
                            Some(new_dapp_id) => {
                                accounts_that_changed_their_dapp_id.insert(
                                    acc_id.routing_with(new_dapp_id),
                                    Some(WrappedAccount { account_id: acc_id, account }),
                                );
                            }
                            None => {
                                accounts_that_changed_their_dapp_id
                                    .insert(acc_id.dapp_originator(), None);
                            }
                        }
                    } else {
                        accounts_that_changed_their_dapp_id.insert(acc_id.dapp_originator(), None);
                    }
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate account blocks: {e}"))?;

        let mut produced_internal_messages_to_the_current_thread: HashMap<
            AccountIdentifier,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        let mut produced_internal_messages_to_other_threads: HashMap<
            AccountRouting,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        for (routing, mut data) in produced_internal_messages {
            if initial_optimistic_state.does_routing_belong_to_the_state(&routing) {
                produced_internal_messages_to_the_current_thread
                    .entry(*routing.account_id())
                    .or_default()
                    .append(&mut data);
            } else {
                produced_internal_messages_to_other_threads
                    .entry(routing)
                    .or_default()
                    .append(&mut data);
            }
        }

        Ok((
            consumed_internal_messages,
            produced_internal_messages_to_the_current_thread,
            accounts_that_changed_their_dapp_id,
            produced_internal_messages_to_other_threads,
        ))
    }
}
