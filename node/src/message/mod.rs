// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_types::UInt256;

// TODO: remove later
// pub mod inbox_message_ref;

pub mod identifier;
#[cfg(test)]
pub mod message_stub;

pub trait Message: Debug + Clone + Sync + Send + Serialize + for<'b> Deserialize<'b> {
    type AccountId;

    // fn is_internal(&self) -> bool;

    fn destination(&self) -> Self::AccountId;
}

#[derive(Clone)]
pub struct WrappedMessage {
    pub message: tvm_block::Message,
}

impl PartialEq for WrappedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for WrappedMessage {}

impl PartialOrd for WrappedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WrappedMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.message.lt(), other.message.lt()) {
            (Some(a_lt), Some(b_lt)) => match a_lt.cmp(&b_lt) {
                std::cmp::Ordering::Equal => self
                    .message
                    .hash()
                    .expect("Message hash must be set")
                    .cmp(&other.message.hash().expect("Message hash must be set")),
                other => other,
            },
            _ => self
                .message
                .hash()
                .expect("Message hash must be set")
                .cmp(&other.message.hash().expect("Message hash must be set")),
        }
    }
}

impl Debug for WrappedMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.message.hash().expect("Failed to get hash for message").to_hex_string()
        )
    }
}

#[derive(Serialize, Deserialize)]
struct WrappedMessageData {
    data: Vec<u8>,
}

impl WrappedMessage {
    fn wrap_serialize(&self) -> WrappedMessageData {
        WrappedMessageData { data: self.message.write_to_bytes().unwrap() }
    }

    fn wrap_deserialize(data: WrappedMessageData) -> Self {
        Self { message: tvm_block::Message::construct_from_bytes(&data.data).unwrap() }
    }
}

impl Serialize for WrappedMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wrap_serialize().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WrappedMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = WrappedMessageData::deserialize(deserializer)?;
        let message = WrappedMessage::wrap_deserialize(data);
        Ok(message)
    }
}
impl Message for WrappedMessage {
    type AccountId = tvm_types::AccountId;

    //    fn is_internal(&self) -> bool {
    //        self.message.is_internal()
    //    }

    fn destination(&self) -> Self::AccountId {
        self.message.int_dst_account_id().unwrap_or(tvm_types::AccountId::from(UInt256::default()))
    }
}
