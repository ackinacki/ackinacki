// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::convert::TryFrom;
use std::fmt;

use ext_messages_auth::auth::Token;
use serde::Deserialize;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;

use crate::api::ext_messages::ThreadIdentifier;
use crate::helpers::parse_message;

#[derive(Deserialize)]
pub struct IncomingExternalMessage {
    id: String,
    body: String,
    thread_id: Option<String>,
    ext_message_token: Option<Token>,
}

impl IncomingExternalMessage {
    pub fn id(&self) -> &str {
        self.id.as_str()
    }
}

pub struct ExternalMessage {
    hash: String,
    message: Message,
    thread_id: ThreadIdentifier,
    pub ext_message_token: Option<Token>,
}

impl ExternalMessage {
    pub fn is_dst_exists(&self) -> bool {
        self.message.int_dst_account_id().is_some()
    }

    pub fn hash(&self) -> String {
        self.message.hash().map(|h| h.to_hex_string()).unwrap_or("".to_string())
    }

    pub fn tvm_message(&self) -> Message {
        self.message.clone()
    }

    pub fn thread_id(&self) -> ThreadIdentifier {
        self.thread_id
    }
}

impl TryFrom<&IncomingExternalMessage> for ExternalMessage {
    type Error = anyhow::Error;

    fn try_from(incoming: &IncomingExternalMessage) -> Result<Self, Self::Error> {
        let IncomingExternalMessage { id, body, thread_id, ext_message_token } = incoming;

        let message = parse_message(id, body)
            .map_err(|err| anyhow::anyhow!("Failed to parse message {id:?}: {err}"))?;

        let thread_id = thread_id.as_deref().map_or_else(ThreadIdentifier::default, |s| {
            s.to_string().try_into().unwrap_or_default()
        });

        Ok(ExternalMessage {
            hash: id.clone(),
            message,
            thread_id,
            ext_message_token: ext_message_token.clone(),
        })
    }
}

impl fmt::Debug for ExternalMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalMessage")
            .field("hash", &self.hash)
            .field("thread_id", &self.thread_id)
            .field("ext_message_token", &self.ext_message_token)
            .finish()
    }
}
