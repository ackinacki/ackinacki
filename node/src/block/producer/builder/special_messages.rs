// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use indexset::BTreeMap;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use tvm_block::Augmentation;
use tvm_block::EnqueuedMsg;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_block::Message;
use tvm_block::MsgEnvelope;
use tvm_block::OutMsg;
use tvm_block::Serializable;
use tvm_executor::BlockchainConfig;
use tvm_types::UInt256;
use tvm_vm::executor::MVConfig;

use super::BlockBuilder;
use crate::block::producer::execution_time::ExecutionTimeLimits;
use crate::creditconfig::dappconfig::calculate_dapp_config_account_id;
use crate::creditconfig::dappconfig::create_config_touch_message;
use crate::message::WrappedMessage;
use crate::repository::optimistic_state::OptimisticState;

fn enqueue_config_message(
    queues: &mut HashMap<
        AccountRouting,
        Vec<(crate::message::identifier::MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    message: &Message,
) -> anyhow::Result<()> {
    let header = message.int_header().ok_or_else(|| anyhow::anyhow!("int_header() is None"))?;
    let account_id = AccountIdentifier::from(header.dst.address());
    let routing =
        account_id.routing_or_redirect(header.dest_dapp_id.as_ref().map(DAppIdentifier::from));
    let wrapped = Arc::new(WrappedMessage::from_tvm(message));
    queues.entry(routing).or_default().push((wrapped.identifier(), wrapped));
    Ok(())
}

impl BlockBuilder {
    pub(super) fn execute_dapp_config_messages(
        &mut self,
        blockchain_config: &BlockchainConfig,
        block_unixtime: u32,
        block_lt: u64,
        check_messages_map: &mut Option<HashMap<AccountIdentifier, BTreeMap<u64, UInt256>>>,
        mvconfig: MVConfig,
    ) -> anyhow::Result<()> {
        tracing::trace!(target: "builder", "map of minted shell {:?}", self.dapp_minted_map);
        let mut config_messages: Vec<Message> = Vec::new();
        // execute DappConfig messages
        for (key, value) in self.dapp_minted_map.clone() {
            if value == 0 {
                continue;
            }
            let data = self.dapp_credit_map.get(&key);
            if let Some(configdata) = data {
                if configdata.is_unlimit {
                    continue;
                }
                let addr =
                    calculate_dapp_config_account_id(key, self.base_config_stateinit.clone())
                        .map_err(|e| {
                            anyhow::format_err!("Failed to calculate dapp config address: {e}")
                        })?;
                let message =
                    create_config_touch_message(value, addr, self.block_info.gen_utime().as_u32())
                        .map_err(|e| {
                            anyhow::format_err!("Failed to create config touch message: {e}")
                        })?;

                // Thread ownership follows the actual dApp, but queue keys must
                // match the wire header, just like consumption and the applier.
                // Touch messages have no dest_dapp_id: changing their headers
                // here would also change message hashes and block contents.
                let dst_addr = message
                    .int_dst_account_id()
                    .map(AccountIdentifier::from)
                    .ok_or_else(|| anyhow::anyhow!("message.dst() is None"))?;
                let destination_routing = dst_addr.routing(key);

                if self
                    .initial_optimistic_state
                    .does_routing_belong_to_the_state(&destination_routing)
                {
                    // If message destination matches current thread, save it in cache to possibly execute in the current block
                    enqueue_config_message(
                        &mut self.produced_internal_messages_to_the_current_thread,
                        &message,
                    )?;
                    config_messages.push(message.clone());
                } else {
                    // If message destination matches current thread, save it in cache to possibly execute in the current block
                    tracing::trace!(target: "builder",
                        "New message for another thread: {:?}",
                        message.hash().map(|h| h.to_hex_string())
                    );
                    enqueue_config_message(
                        &mut self.produced_internal_messages_to_other_threads,
                        &message,
                    )?;
                }
                let info =
                    message.int_header().ok_or_else(|| anyhow::anyhow!("int_header() is None"))?;
                let fwd_fee = info.fwd_fee();
                let msg_cell = message.serialize().map_err(|e| {
                    anyhow::format_err!("Failed to serialize dapp config message: {e}")
                })?;
                let env = MsgEnvelope::with_message_and_fee(&message, *fwd_fee).map_err(|e| {
                    anyhow::format_err!("Failed to generate dapp config message envelope: {e}")
                })?;
                let enq = EnqueuedMsg::with_param(info.created_lt, &env).map_err(|e| {
                    anyhow::format_err!("Failed to enqueue dapp config message: {e}")
                })?;
                // Note: this message was generate by node and has no parent transaction, use default tx instead
                let default_tx = tvm_block::Transaction::default().serialize().map_err(|e| {
                    anyhow::format_err!("Failed to serialize default transaction: {e}")
                })?;
                let out_msg = OutMsg::new(enq.out_msg_cell(), default_tx);

                self.out_msg_descr
                    .set(
                        &msg_cell.repr_hash(),
                        &out_msg,
                        &out_msg
                            .aug()
                            .map_err(|e| anyhow::format_err!("Failed to get msg aug: {e}"))?,
                    )
                    .map_err(|e| anyhow::format_err!("Failed to add msg to out msg descr: {e}"))?;
            }
        }
        let mut active_destinations = HashSet::<AccountIdentifier>::new();
        let mut active_ext_threads = VecDeque::new();
        loop {
            if active_ext_threads.len() < self.parallelization_level {
                while !config_messages.is_empty() {
                    if active_ext_threads.len() == self.parallelization_level {
                        break;
                    }
                    if let Some(acc_id) =
                        config_messages[0].int_dst_account_id().map(AccountIdentifier::from)
                    {
                        if !active_destinations.contains(&acc_id) {
                            let msg = config_messages.remove(0); // it's safe
                            tracing::trace!(target: "builder", "Parallel config message: {:?} to {:?}", msg.hash().unwrap(), acc_id.to_hex_string());
                            let thread = self.execute_internal(
                                msg,
                                false,
                                blockchain_config,
                                &acc_id,
                                block_unixtime,
                                block_lt,
                                check_messages_map,
                                &ExecutionTimeLimits::NO_LIMITS,
                                mvconfig.clone(),
                            )?;
                            active_ext_threads.push_back(thread);
                            active_destinations.insert(acc_id);
                        } else {
                            break;
                        }
                    } else {
                        let msg = config_messages.remove(0);
                        tracing::warn!(
                            target: "builder",
                            "Found dapp config msg with not valid internal destination: {msg:?}",
                        );
                    }
                }
            }

            while !active_ext_threads.is_empty() {
                if let Ok(thread_result) = active_ext_threads
                    .front()
                    .expect("Already checked that front value is Some")
                    .result_rx
                    .try_recv()
                {
                    let _ = active_ext_threads.pop_front();
                    let thread_result = thread_result.map_err(|_| {
                        anyhow::format_err!("Failed to execute transaction in parallel")
                    })?;
                    let acc_id = thread_result.account_id;
                    tracing::trace!(target: "builder", "parallel epoch message finished dest: {}", acc_id.to_hex_string());
                    self.after_transaction(thread_result, false)?;
                    active_destinations.remove(&acc_id);
                } else {
                    break;
                }
            }

            if config_messages.is_empty() && active_ext_threads.is_empty() {
                tracing::debug!(target: "builder", "Dapp Config messages stop");
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use account_state::ThreadAccountsRepository;
    use node_types::BlockIdentifier;
    use node_types::ThreadIdentifier;

    use super::*;
    use crate::block::postprocessing::postprocess;
    use crate::repository::optimistic_state::OptimisticStateImpl;
    use crate::storage::MessageDurableStorage;
    use crate::types::BlockInfo;
    use crate::types::BlockSeqNo;
    use crate::types::ThreadsTable;

    #[test]
    fn config_touch_and_transfer_share_the_wire_queue_routing() -> anyhow::Result<()> {
        let dapp_id = DAppIdentifier::new([1; 32]);
        let config_id = AccountIdentifier::new([2; 32]);
        let touch = create_config_touch_message(100_000_000_000, config_id, 100)?;
        assert!(touch.int_header().unwrap().dest_dapp_id.is_none());
        let mut transfer_header = touch.int_header().unwrap().clone();
        transfer_header.created_lt = 1000;
        let transfer = Message::with_int_header(transfer_header);
        let routing = config_id.redirect();
        assert_ne!(routing, config_id.routing(dapp_id));
        let mut produced = HashMap::new();
        enqueue_config_message(&mut produced, &transfer)?;
        enqueue_config_message(&mut produced, &touch)?;
        assert_eq!(produced.len(), 1);
        let messages = &produced[&routing];
        assert_eq!(messages[0].1.tvm_message()?, &transfer);
        assert_eq!(messages[1].1.tvm_message()?, &touch);
        let consumed = messages.iter().map(|(id, _)| id.clone()).collect();
        let dir = tempfile::tempdir()?;
        let repository = ThreadAccountsRepository::builder(dir.path().join("durable")).build()?;
        let state = OptimisticStateImpl::zero();
        let transition =
            repository.state_builder(&state.thread_id, 0, &state.shard_state.0).build(None)?;
        // Both messages execute in this block. The touch's LT is lower, so
        // postprocess must also preserve consumption order before cancellation.
        let (after, _, _) = postprocess(
            state,
            HashMap::from([(routing, consumed)]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            produced,
            HashMap::new(),
            BlockIdentifier::default(),
            BlockSeqNo::default(),
            0,
            transition,
            HashMap::new(),
            BlockInfo::default(),
            ThreadIdentifier::default(),
            ThreadsTable::default(),
            MessageDurableStorage::mem(),
            &repository,
            false,
            #[cfg(feature = "monitor-accounts-number")]
            0,
        )?;
        assert!(after.messages.is_empty());
        assert!(after.high_priority_messages.is_empty());
        crate::repository::repository_impl::validate_snapshot_inboxes(&after, &[])?;

        let mut explicit = touch.clone();
        explicit.int_header_mut().unwrap().set_dest_dapp_id(Some(dapp_id.into()));
        let mut produced = HashMap::new();
        enqueue_config_message(&mut produced, &explicit)?;
        assert_eq!(produced.len(), 1);
        assert!(produced.contains_key(&config_id.routing(dapp_id)));
        Ok(())
    }
}
