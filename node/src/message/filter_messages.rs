use std::collections::VecDeque;

use tvm_block::Augmentation;
use tvm_block::HashmapAugType;
use tvm_block::Message;
use tvm_block::OutMsgQueue;
use tvm_block::OutMsgQueueInfo;
use tvm_block::OutMsgQueueKey;
use tvm_types::AccountId;
use tvm_types::HashmapType;

use crate::types::AccountRouting;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[allow(dead_code)]
pub fn filter_int_messages_for_thread(
    messages: OutMsgQueueInfo,
    threads_table: &ThreadsTable,
    thread_id: ThreadIdentifier,
    account_routing_mapper: impl Fn(AccountId) -> AccountRouting,
) -> tvm_types::Result<OutMsgQueueInfo> {
    let mut filtered_queue = messages.clone();
    let mut filtered_out_msg_queue = OutMsgQueue::new();
    let queue = messages.out_queue().clone();
    for message in queue.iter() {
        // TODO: check if that key can be used for save
        let (_key, mut slice) = message?;
        let (enqueued_message, _create_lt) = OutMsgQueue::value_aug(&mut slice)?;
        let message = enqueued_message.read_out_msg()?.read_message()?;

        if let Some(acc_id) = message.int_dst_account_id() {
            let account_routing = account_routing_mapper(acc_id.clone());
            if threads_table.is_match(&account_routing, thread_id) {
                let prefix = acc_id.clone().get_next_u64()?;
                let key = OutMsgQueueKey::with_workchain_id_and_prefix(
                    0,
                    prefix,
                    enqueued_message.out_msg.cell().repr_hash(),
                );
                filtered_out_msg_queue.set(&key, &enqueued_message, &enqueued_message.aug()?)?;
            }
        }
    }
    filtered_queue.set_out_queue(filtered_out_msg_queue);
    Ok(filtered_queue)
}

#[allow(dead_code)]
pub fn filter_ext_messages_for_thread(
    ext_messages_queue: VecDeque<Message>,
    threads_table: &ThreadsTable,
    thread_id: ThreadIdentifier,
    account_routing_mapper: impl Fn(AccountId) -> AccountRouting,
) -> tvm_types::Result<VecDeque<Message>> {
    let mut filtered_messages = VecDeque::new();
    for message in ext_messages_queue {
        if let Some(acc_id) = message.int_dst_account_id() {
            let account_routing = account_routing_mapper(acc_id.clone());
            if threads_table.is_match(&account_routing, thread_id) {
                filtered_messages.push_back(message);
            }
        }
    }
    Ok(filtered_messages)
}
