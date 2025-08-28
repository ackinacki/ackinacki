// #[cfg(test)]
// #[cfg(feature = "messages_db")]
// #[allow(clippy::module_inception)]
// mod tests {
//     use std::path::PathBuf;

//     use account_inbox::storage::DurableStorageRead;
//     use tvm_block::Deserializable;
//     use tvm_block::GetRepresentationHash;
//     use tvm_block::MsgAddrStd;
//     use tvm_block::MsgAddressInt;
//     use tvm_types::base64_decode;

//     use crate::message::identifier::MessageIdentifier;
//     use crate::message::Message;
//     use crate::message::WrappedMessage;
//     use crate::message_storage::MessageDurableStorage;

//     fn setup_db(db_path: PathBuf) -> MessageDurableStorage {
//         MessageDurableStorage::new(db_path).expect("Failed to create DB")
//     }

//     #[test]
//     fn test_write_and_read_message() {
//         // let db_path = tempfile::tempdir().unwrap().into_path().join("test_write_and_read_message.db");
//         let db_path = PathBuf::from("/tmp/db.db");
//         let storage = setup_db(db_path);
//         let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//         let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//         let message_blob =
//             bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap();
//         let message_hash = message.hash().unwrap().to_hex_string();
//         let dest_account = message.int_dst_account_id().unwrap().to_hex_string();
//         storage
//             .write_message(&dest_account, &message_blob, &message_hash)
//             .expect("Failed to write message");
//         let result = storage.read_message(&message_hash).expect("Failed to read message");
//         assert!(result.is_some());
//         let (_, stored_message) = result.unwrap();
//         assert_eq!(stored_message.destination().to_hex_string(), dest_account);
//         assert_eq!(stored_message.message.hash().unwrap().to_hex_string(), message_hash);
//         assert_eq!(stored_message, WrappedMessage { message });
//     }

//     #[test]
//     fn test_get_rowid_by_hash() {
//         let db_path = tempfile::tempdir().unwrap().keep().join("test_get_rowid_by_hash.db");
//         let storage = setup_db(db_path);
//         let message_hash = "ce99fef949de549fae054e67ffabbfbde307fd68a6aeb099b46c7d0e49e06a95";
//         let dest_account = "6a711e584f6cb612c86a1f8b144c92ea851f90eccd98577d8f1431b1a3ad7eeb";
//         let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//         let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//         let message_blob =
//             bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap();
//         storage
//             .write_message(dest_account, &message_blob, message_hash)
//             .expect("Failed to write message");
//         let rowid = storage.get_rowid_by_hash(message_hash).expect("Failed to get rowid");
//         assert!(rowid.is_some());
//     }

//     #[test]
//     fn test_next_simple() {
//         let db_path = tempfile::tempdir().unwrap().keep().join("test_next_simple.db");
//         let storage = setup_db(db_path);

//         let dest_account = "6a711e584f6cb612c86a1f8b144c92ea851f90eccd98577d8f1431b1a3ad7eeb";
//         let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//         let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//         let mut message2 = message.clone();
//         message2.set_src_address(MsgAddressInt::AddrStd(MsgAddrStd::default()));
//         let mut message3 = message.clone();
//         let header = message3.int_header_mut().unwrap();
//         header.bounced = true;

//         let messages = vec![
//             (
//                 message.hash().unwrap().to_hex_string(),
//                 dest_account,
//                 bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap(),
//             ),
//             (
//                 message2.hash().unwrap().to_hex_string(),
//                 dest_account,
//                 bincode::serialize(&WrappedMessage { message: message2.clone() }).unwrap(),
//             ),
//             (
//                 message3.hash().unwrap().to_hex_string(),
//                 dest_account,
//                 bincode::serialize(&WrappedMessage { message: message3.clone() }).unwrap(),
//             ),
//         ];
//         for (hash, account, blob) in &messages {
//             storage.write_message(account, blob, hash).expect("Failed to write message");
//         }
//         let start_cursor =
//             storage.get_rowid_by_hash(&message.hash().unwrap().to_hex_string()).unwrap().unwrap();
//         let (result, _) =
//             storage.next_simple(dest_account, start_cursor, 10).expect("Failed to fetch messages");
//         assert_eq!(result.len(), 2);
//         assert_eq!(
//             result[0].message.hash().unwrap().to_hex_string(),
//             message2.hash().unwrap().to_hex_string()
//         );
//         assert_eq!(
//             result[1].message.hash().unwrap().to_hex_string(),
//             message3.hash().unwrap().to_hex_string()
//         );
//     }

//     #[test]
//     fn test_remaining() {
//         let db_path = tempfile::tempdir().unwrap().keep().join("test_remaining.db");
//         let storage = setup_db(db_path);

//         let dest_account = "6a711e584f6cb612c86a1f8b144c92ea851f90eccd98577d8f1431b1a3ad7eeb";
//         let message_blob = base64_decode("te6ccgEBAgEAogAB8WgA7PqVWKlxS6/8ap0gb47CAlCbCrxLXVZggTrJUgnJ3uUAGpxHlhPbLYSyGofixRMkuqFH5DszZhXfY8UMbGjrX7rQjrhggAYcPToAAAAAAALR2s9Zn5e8iRf43RaVSGOZFlW+iS7Nqg5EyEGL/OxozzhXDjYJ12ABAEho1ZpadA2/WldTKvjbt6bgdMEho8eA4U8K1pvQDBJkcuy4aag=").unwrap();
//         let message = tvm_block::Message::construct_from_bytes(&message_blob).unwrap();
//         let mut message2 = message.clone();
//         message2.set_src_address(MsgAddressInt::AddrStd(MsgAddrStd::default()));
//         let mut message3 = message.clone();
//         let header = message3.int_header_mut().unwrap();
//         header.bounced = true;

//         let messages = vec![
//             (
//                 message.hash().unwrap().to_hex_string(),
//                 dest_account,
//                 bincode::serialize(&WrappedMessage { message: message.clone() }).unwrap(),
//             ),
//             (
//                 message2.hash().unwrap().to_hex_string(),
//                 dest_account,
//                 bincode::serialize(&WrappedMessage { message: message2.clone() }).unwrap(),
//             ),
//             (
//                 message3.hash().unwrap().to_hex_string(),
//                 dest_account,
//                 bincode::serialize(&WrappedMessage { message: message3.clone() }).unwrap(),
//             ),
//         ];
//         for (hash, account, blob) in &messages {
//             storage.write_message(account, blob, hash).expect("Failed to write message");
//         }
//         let messages = storage
//             .remaining_messages(
//                 &MessageIdentifier::from(&WrappedMessage { message: message.clone() }),
//                 usize::MAX,
//             )
//             .expect("Failed to get remaining messages");
//         assert_eq!(messages.len(), 3);
//         assert_eq!(
//             messages[0].message.hash().unwrap().to_hex_string(),
//             message.hash().unwrap().to_hex_string()
//         );
//         assert_eq!(
//             messages[1].message.hash().unwrap().to_hex_string(),
//             message2.hash().unwrap().to_hex_string()
//         );
//         assert_eq!(
//             messages[2].message.hash().unwrap().to_hex_string(),
//             message3.hash().unwrap().to_hex_string()
//         );
//     }
// }
