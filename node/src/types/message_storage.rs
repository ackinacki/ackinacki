use std::sync::Arc;

use account_inbox::storage::DurableStorageRead;
use account_inbox::storage::LoadErr;
use tvm_block::GetRepresentationHash;
use tvm_block::OutMsgQueueKey;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;

const BATCH_SIZE: usize = 50;

impl DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>> for MessageDurableStorage {
    type LoadError = LoadErr;

    fn load_message(
        &self,
        key: &MessageIdentifier,
    ) -> Result<Arc<WrappedMessage>, Self::LoadError> {
        let msg_hash = key.inner().hash.to_hex_string();
        let (_, message) = self
            .read_message(&msg_hash)
            .map_err(|e| {
                tracing::error!("Failed to load message {}: {:?}", msg_hash, e);
                LoadErr::NoState
            })?
            .ok_or(LoadErr::NoState)?;
        Ok(Arc::new(message))
    }

    fn next(&self, key: &MessageIdentifier) -> Result<Option<MessageIdentifier>, Self::LoadError> {
        let msg_hash = key.inner().hash.to_hex_string();
        let wrapped_message_res = self.read_message(&msg_hash);
        match wrapped_message_res {
            Err(_) => Err(LoadErr::NoState),
            Ok(None) => Ok(None),
            Ok(Some((row_id, stored_message))) => {
                let dest_account = &stored_message
                    .message
                    .int_dst_account_id()
                    .ok_or(Self::LoadError::DeserializationError)?
                    .to_hex_string();
                let (messages, _) = self.next_simple(dest_account, row_id, 1).map_err(|e| {
                    tracing::error!("Failed to load message {}: {:?}", msg_hash, e);
                    LoadErr::NoState
                })?;
                if messages.len() != 1 {
                    return Ok(None);
                }
                let next_message = messages[0]
                    .message
                    .hash()
                    .map_err(|_| Self::LoadError::DeserializationError)?
                    .to_hex_string();
                let data = OutMsgQueueKey::with_workchain_id_and_prefix(
                    key.inner().workchain_id,
                    key.inner().prefix,
                    next_message.parse().unwrap(),
                );
                Ok(Some(MessageIdentifier::from(data)))
            }
        }
    }

    fn remaining_messages(
        &self,
        starting_key: &MessageIdentifier,
        mut limit: usize,
    ) -> Result<Vec<Arc<WrappedMessage>>, Self::LoadError> {
        let msg_hash = starting_key.inner().hash.to_hex_string();
        let mut res = vec![];
        let wrapped_message_res = self.read_message(&msg_hash);
        match wrapped_message_res {
            Err(_) => Err(LoadErr::NoState),
            Ok(None) => Ok(res),
            Ok(Some((mut row_id, stored_message))) => {
                let dest_account = &stored_message
                    .message
                    .int_dst_account_id()
                    .ok_or(Self::LoadError::DeserializationError)?
                    .to_hex_string();
                res.push(Arc::new(stored_message));
                limit -= 1;
                while limit > 0 {
                    let requested_cnt = std::cmp::min(BATCH_SIZE, limit);
                    let (messages, last_row_id) =
                        self.next_simple(dest_account, row_id, requested_cnt).map_err(|e| {
                            tracing::error!("Failed to load message {}: {:?}", msg_hash, e);
                            LoadErr::NoState
                        })?;
                    let messages_cnt = messages.len();
                    res.extend(messages.into_iter().map(Arc::new));
                    if messages_cnt != requested_cnt {
                        break;
                    }
                    row_id = last_row_id.unwrap();
                    limit -= messages_cnt;
                }
                Ok(res)
            }
        }
    }
}
