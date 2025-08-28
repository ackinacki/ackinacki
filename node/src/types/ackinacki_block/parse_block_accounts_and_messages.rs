use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use tvm_block::Augmentation;
use tvm_block::HashmapAugType;
use tvm_block::ShardStateUnsplit;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::account::WrappedAccount;
use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::AckiNackiBlock;
use crate::types::DAppIdentifier;

impl AckiNackiBlock {
    pub fn get_data_for_postprocessing(
        &self,
        initial_optimistic_state: &mut OptimisticStateImpl,
        updated_shard_state: Arc<ShardStateUnsplit>,
    ) -> anyhow::Result<(
        HashMap<AccountAddress, HashSet<MessageIdentifier>>,
        HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
        HashMap<AccountRouting, Option<WrappedAccount>>,
        HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    )> {
        let mut consumed_internal_messages: HashMap<AccountAddress, HashSet<MessageIdentifier>> =
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
                if let Some(dest_account_id) = msg.int_dst_account_id() {
                    // TODO: check that message dst belongs to this thread
                    let addr = initial_optimistic_state.get_account_routing(&dest_account_id, None);
                    let wrapped_message = WrappedMessage { message: msg };
                    let message_identifier = MessageIdentifier::from(&wrapped_message);
                    let entry = produced_internal_messages.entry(addr.clone()).or_default();
                    entry.push((message_identifier, Arc::new(wrapped_message)));
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
                    let addr = AccountAddress::from(header.dst.address());
                    let wrapped_message = WrappedMessage { message: msg };
                    let message_identifier = MessageIdentifier::from(&wrapped_message);
                    let entry = consumed_internal_messages.entry(addr.clone()).or_default();
                    entry.insert(message_identifier);
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iter in msgs: {e}"))?;
        let shard_state = updated_shard_state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read accounts: {e}"))?;
        for (addr, (new_dapp_id, _)) in self.get_common_section().changed_dapp_ids.iter() {
            match new_dapp_id {
                Some(new_dapp_id) => {
                    let account = shard_state
                        .account(&addr.into())
                        .map_err(|e| anyhow::format_err!("Failed to read account: {e}"))?
                        .expect("account must exist");
                    accounts_that_changed_their_dapp_id.insert(
                        AccountRouting(new_dapp_id.clone(), addr.clone()),
                        Some(WrappedAccount {
                            account_id: addr.clone(),
                            aug: account.aug().map_err(|e| {
                                anyhow::format_err!("Failed to get account aug: {e}")
                            })?,
                            account,
                        }),
                    );
                }
                None => {
                    accounts_that_changed_their_dapp_id
                        .insert(AccountRouting(DAppIdentifier(addr.clone()), addr.clone()), None);
                }
            }
        }
        let mut produced_internal_messages_to_the_current_thread: HashMap<
            AccountAddress,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        let mut produced_internal_messages_to_other_threads: HashMap<
            AccountRouting,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        for (routing, data) in produced_internal_messages {
            if initial_optimistic_state.does_routing_belong_to_the_state(&routing) {
                produced_internal_messages_to_the_current_thread.insert(routing.1, data);
            } else {
                produced_internal_messages_to_other_threads.insert(routing, data);
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
