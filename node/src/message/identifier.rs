use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde_with::serde_as;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::OutMsgQueueKey;
use tvm_block::Serializable;

use crate::message::WrappedMessage;

#[serde_as]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct MessageIdentifier(OutMsgQueueKey);

impl MessageIdentifier {
    pub fn inner(&self) -> &OutMsgQueueKey {
        &self.0
    }
}

impl Debug for MessageIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageIdentifier({})", self.0.hash.to_hex_string())
    }
}

impl Serialize for MessageIdentifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let data = self.0.write_to_bytes().expect("Message Identifier must be serializable");
        data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MessageIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = <&[u8]>::deserialize(deserializer)?;
        let out_msg_queue_key = OutMsgQueueKey::construct_from_bytes(data)
            .expect("Message Identifier must be deserializable");
        Ok(Self(out_msg_queue_key))
    }
}

impl From<&WrappedMessage> for MessageIdentifier {
    fn from(value: &WrappedMessage) -> Self {
        let dest_acc_id = value
            .message
            .int_dst_account_id()
            .expect("Failed to get MessageIdentifier for message with ext destination");
        let prefix = dest_acc_id
            .clone()
            .get_next_u64()
            .map_err(|e| anyhow::format_err!("Failed to calculate acc prefix: {e}"))
            .expect("Failed to get acc prefix");
        Self(OutMsgQueueKey::with_workchain_id_and_prefix(
            0,
            prefix,
            value.message.hash().expect("Failed to get message hash"),
        ))
    }
}

impl From<Arc<WrappedMessage>> for MessageIdentifier {
    fn from(value: Arc<WrappedMessage>) -> Self {
        Self::from(value.as_ref())
    }
}

impl From<OutMsgQueueKey> for MessageIdentifier {
    fn from(key: OutMsgQueueKey) -> Self {
        MessageIdentifier(key)
    }
}
