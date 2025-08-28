use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::range::MessagesRange;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::storage::MessageDurableStorage;
use crate::types::thread_message_queue::order_set::OrderSet;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;

const MAX_MESSAGES: usize = 100;

impl ThreadMessageQueueState {
    pub fn load_state(
        db: &MessageDurableStorage,
        state_messages: &BTreeMap<
            AccountAddress,
            MessagesRange<MessageIdentifier, Arc<WrappedMessage>>,
        >,
    ) -> anyhow::Result<Self> {
        let mut state_order = OrderSet::new();
        let state_cursor = 0;
        let mut new_state_messages: BTreeMap<
            AccountAddress,
            MessagesRange<MessageIdentifier, Arc<WrappedMessage>>,
        > = BTreeMap::new();
        for (account_address, range) in state_messages {
            let mut tail = VecDeque::new();
            let mut it = MessagesRangeIterator::new(db, range.clone());
            let start_message_id = it
                .next()
                .and_then(|res| res.ok().map(|(_, key)| key))
                .expect("Empty range detected");
            let mut db_cursor;
            let db_cursor_res =
                db.get_rowid_by_hash(&start_message_id.inner().hash.to_hex_string());
            match db_cursor_res {
                Ok(Some(data)) => db_cursor = data,
                Ok(None) => {
                    panic!("Start of the range not found in database");
                }
                Err(_) => {
                    panic!("Start of the range not found in database");
                }
            }
            let mut db_iter = db
                .next_simple(&account_address.0.to_hex_string(), db_cursor, MAX_MESSAGES)?
                .0
                .into_iter();
            for db_message in db_iter.by_ref() {
                let message_id = MessageIdentifier::from(&db_message);
                if let Some(Ok((_, key))) = it.next() {
                    if key == message_id {
                        continue;
                    } else {
                        panic!("Mismatch: expected message {key:?} but found {message_id:?}")
                    }
                } else {
                    tail.push_back((message_id, Arc::new(db_message)));
                }
            }
            db_cursor += MAX_MESSAGES as i64;
            loop {
                let (new_messages, _) =
                    db.next_simple(&account_address.0.to_hex_string(), db_cursor, MAX_MESSAGES)?;
                if new_messages.is_empty() {
                    break;
                }
                db_iter = new_messages.into_iter();
                for db_message in db_iter.by_ref() {
                    let message_id = MessageIdentifier::from(&db_message);
                    tail.push_back((message_id, Arc::new(db_message)));
                }
                db_cursor += MAX_MESSAGES as i64;
            }
            let mut new_range = range.clone();
            new_range.set_tail_sequence(tail);
            new_state_messages.insert(account_address.clone(), new_range);
            state_order.insert(account_address.clone());
        }

        Ok(ThreadMessageQueueState {
            messages: new_state_messages,
            order_set: state_order,
            cursor: state_cursor,
        })
    }
}

// #[cfg(test)]
// #[cfg(feature = "messages_db")]
// mod tests {
//     use std::collections::BTreeMap;
//     use std::ops::RangeInclusive;
//     use std::sync::Arc;

//     use account_inbox::range::MessagesRange;
//     use tvm_block::Deserializable;
//     use tvm_block::GetRepresentationHash;
//     use tvm_block::MsgAddrStd;
//     use tvm_block::MsgAddressInt;
//     use tvm_block::OutMsgQueueKey;
//     use tvm_types::base64_decode;
//     use tvm_types::AccountId;
//     use tvm_types::UInt256;

//     use crate::message::identifier::MessageIdentifier;
//     use crate::message::WrappedMessage;
//     use crate::message_storage::MessageDurableStorage;
//     use crate::types::thread_message_queue::ThreadMessageQueueState;
//     use crate::types::AccountAddress;

// #[test]
// fn test_load_state_valid_range_1() {
//     let db_path = tempfile::tempdir().unwrap().keep().join("test_load_1.db");
//     let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
//     let dest_account = "6a711e584f6cb612c86a1f8b144c92ea851f90eccd98577d8f1431b1a3ad7eeb";
//     let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//     let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//     let first_id = MessageIdentifier::from(&WrappedMessage { message: message.clone() });
//     let mut message2 = message.clone();
//     message2.set_src_address(MsgAddressInt::AddrStd(MsgAddrStd::default()));
//     let mut message3 = message.clone();
//     let header = message3.int_header_mut().unwrap();
//     header.bounced = true;
//     let account_addr = AccountAddress::default();
//     let messages = vec![
//         (
//             message.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap(),
//         ),
//         (
//             message2.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message2.clone() }).unwrap(),
//         ),
//         (
//             message3.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message3.clone() }).unwrap(),
//         ),
//     ];
//     for (hash, _, blob) in &messages {
//         storage
//             .write_message(&account_addr.0.to_hex_string(), blob, hash)
//             .expect("Failed to write message");
//     }
//     let mut state_messages = BTreeMap::new();

//     let mut range = MessagesRange::<MessageIdentifier, Arc<WrappedMessage>>::empty();
//     let range_ref = Some(RangeInclusive::new(first_id.clone(), first_id.clone()));
//     range.set_compacted_history(range_ref);
//     state_messages.insert(account_addr.clone(), range);

//     let result = ThreadMessageQueueState::load_state(&storage, &state_messages);
//     assert!(result.is_ok(), "load_state returned an error: {:?}", result.err());

//     let state = result.unwrap();
//     assert_eq!(state.messages.len(), 1, "State messages should contain 1 account address");
//     let account_state = state.messages.get(&account_addr).unwrap();
//     let tail = account_state.tail_sequence().clone();
//     assert_eq!(tail.len(), 2, "Account should have 2 messages in the tail");
// }

// #[test]
// fn test_load_state_valid_range_2() {
//     let db_path = tempfile::tempdir().unwrap().keep().join("test_load_2.db");
//     let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
//     let dest_account = "6a711e584f6cb612c86a1f8b144c92ea851f90eccd98577d8f1431b1a3ad7eeb";
//     let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//     let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//     let mut message2 = message.clone();
//     message2.set_src_address(MsgAddressInt::AddrStd(MsgAddrStd::default()));
//     let mut message3 = message.clone();
//     let header = message3.int_header_mut().unwrap();
//     header.bounced = true;
//     let account_addr = AccountAddress::default();
//     let first_id = MessageIdentifier::from(&WrappedMessage { message: message2.clone() });
//     let messages = vec![
//         (
//             message.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap(),
//         ),
//         (
//             message2.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message2.clone() }).unwrap(),
//         ),
//         (
//             message3.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message3.clone() }).unwrap(),
//         ),
//     ];
//     for (hash, _, blob) in &messages {
//         storage
//             .write_message(&account_addr.0.to_hex_string(), blob, hash)
//             .expect("Failed to write message");
//     }
//     let mut state_messages = BTreeMap::new();

//     let mut range = MessagesRange::<MessageIdentifier, Arc<WrappedMessage>>::empty();
//     let range_ref = Some(RangeInclusive::new(first_id.clone(), first_id.clone()));
//     range.set_compacted_history(range_ref);
//     state_messages.insert(account_addr.clone(), range);

//     let result = ThreadMessageQueueState::load_state(&storage, &state_messages);
//     assert!(result.is_ok(), "load_state returned an error: {:?}", result.err());

//     let state = result.unwrap();
//     assert_eq!(state.messages.len(), 1, "State messages should contain 1 account address");
//     let account_state = state.messages.get(&account_addr).unwrap();
//     let tail = account_state.tail_sequence().clone();
//     assert_eq!(tail.len(), 1, "Account should have 2 messages in the tail");
// }

// #[test]
// #[should_panic]
// fn test_load_state_invalid_range_3() {
//     let db_path = tempfile::tempdir().unwrap().keep().join("test_load_3.db");
//     let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
//     let dest_account = "6a711e584f6cb612c86a1f8b144c92ea851f90eccd98577d8f1431b1a3ad7eeb";
//     let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//     let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//     let mut message2 = message.clone();
//     message2.set_src_address(MsgAddressInt::AddrStd(MsgAddrStd::default()));
//     let mut message3 = message.clone();
//     let header = message3.int_header_mut().unwrap();
//     header.bounced = true;
//     let account_addr = AccountAddress::default();
//     let key = OutMsgQueueKey::with_workchain_id_and_prefix(0, 0, UInt256::rand());
//     let first_id = MessageIdentifier::from(key);
//     let messages = vec![
//         (
//             message.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap(),
//         ),
//         (
//             message2.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message2.clone() }).unwrap(),
//         ),
//         (
//             message3.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message3.clone() }).unwrap(),
//         ),
//     ];
//     for (hash, _, blob) in &messages {
//         storage
//             .write_message(&account_addr.0.to_hex_string(), blob, hash)
//             .expect("Failed to write message");
//     }
//     let mut state_messages = BTreeMap::new();

//     let mut range = MessagesRange::<MessageIdentifier, Arc<WrappedMessage>>::empty();
//     let range_ref = Some(RangeInclusive::new(first_id.clone(), first_id.clone()));
//     range.set_compacted_history(range_ref);
//     state_messages.insert(account_addr.clone(), range);

//     let _result = ThreadMessageQueueState::load_state(&storage, &state_messages);
// }

// #[test]
// fn test_load_state_valid_range_4() {
//     let db_path = tempfile::tempdir().unwrap().keep().join("test_load_4.db");
//     let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
//     let dest_account = "6a711e584f6cb612c86a1f8b144c92ea851f90eccd98577d8f1431b1a3ad7eeb";
//     let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//     let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//     let first_id = MessageIdentifier::from(&WrappedMessage { message: message.clone() });
//     let mut message2 = message.clone();
//     message2.set_src_address(MsgAddressInt::AddrStd(MsgAddrStd::default()));
//     let mut message3 = message.clone();
//     let header = message3.int_header_mut().unwrap();
//     header.bounced = true;
//     let second_id = MessageIdentifier::from(&WrappedMessage { message: message3.clone() });
//     let account_addr = AccountAddress::default();
//     let account_addr_1 = AccountAddress(AccountId::from(UInt256::max()));
//     let messages1 = vec![
//         (
//             message.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap(),
//         ),
//         (
//             message2.hash().unwrap().to_hex_string(),
//             dest_account,
//             bincode::serialize(&WrappedMessage { message: message2.clone() }).unwrap(),
//         ),
//     ];
//     let messages2 = vec![(
//         message3.hash().unwrap().to_hex_string(),
//         dest_account,
//         bincode::serialize(&WrappedMessage { message: message3.clone() }).unwrap(),
//     )];
//     for (hash, _, blob) in &messages1 {
//         storage
//             .write_message(&account_addr.0.to_hex_string(), blob, hash)
//             .expect("Failed to write message");
//     }
//     for (hash, _, blob) in &messages2 {
//         storage
//             .write_message(&account_addr_1.0.to_hex_string(), blob, hash)
//             .expect("Failed to write message");
//     }
//     let mut state_messages = BTreeMap::new();

//     let mut range = MessagesRange::<MessageIdentifier, Arc<WrappedMessage>>::empty();
//     let range_ref = Some(RangeInclusive::new(first_id.clone(), first_id.clone()));
//     range.set_compacted_history(range_ref);
//     state_messages.insert(account_addr.clone(), range);

//     let mut range2 = MessagesRange::<MessageIdentifier, Arc<WrappedMessage>>::empty();
//     let range_ref2 = Some(RangeInclusive::new(second_id.clone(), second_id.clone()));
//     range2.set_compacted_history(range_ref2);
//     state_messages.insert(account_addr_1.clone(), range2);

//     let result = ThreadMessageQueueState::load_state(&storage, &state_messages);
//     assert!(result.is_ok(), "load_state returned an error: {:?}", result.err());

//     let state = result.unwrap();
//     assert_eq!(state.messages.len(), 2, "State messages should contain 2 account address");
//     let account_state = state.messages.get(&account_addr).unwrap();
//     let tail = account_state.tail_sequence().clone();
//     assert_eq!(tail.len(), 1, "Account should have 1 messages in the tail");
//     let account_state = state.messages.get(&account_addr_1).unwrap();
//     let tail = account_state.tail_sequence().clone();
//     assert_eq!(tail.len(), 0, "Account should have 1 messages in the tail");
// }
// }
