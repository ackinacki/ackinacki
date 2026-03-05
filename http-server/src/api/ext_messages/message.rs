// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use anyhow::bail;
use ext_messages_auth::auth::Token;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;
use tvm_types::UInt256;

use crate::helpers::parse_message;

#[derive(Clone)]
pub struct NotQueuedExtMessage {
    nonce: String, // Some external identifier, currently used only for debugging output.
    tvm_message: Message,
    hash: UInt256,
    thread_id: ThreadIdentifier,
    pub ext_message_token: Option<Token>,
    dst_dapp_id: Option<DAppIdentifier>,
}

impl NotQueuedExtMessage {
    pub fn try_new(
        nonce: &str,
        tvm_message_base64: &str,
        thread_id: Option<String>,
        ext_message_token: Option<Token>,
        dst_dapp_id: Option<String>,
    ) -> anyhow::Result<Self> {
        let tvm_message = match parse_message(tvm_message_base64) {
            Ok(message) => message,
            Err(err) => {
                bail!("Can't parse tvm_message: {err}")
            }
        };

        if !tvm_message.is_inbound_external() {
            bail!("This message is not inbound external, id: {nonce}");
        }
        if tvm_message.int_dst_account_id().is_none() {
            bail!("Invalid destination");
        }
        let Ok(hash) = tvm_message.hash() else {
            bail!("Invalid ExternalMessage, id {nonce}: hash() must return UInt256")
        };

        let thread_id = match thread_id.map(ThreadIdentifier::try_from).transpose() {
            Ok(Some(id)) => id,
            Ok(None) => ThreadIdentifier::default(),
            Err(_) => {
                bail!("thread_id can not be parsed")
            }
        };

        let dst_dapp_id = match dst_dapp_id.map(|s| DAppIdentifier::from_str(&s)).transpose() {
            Ok(id) => id,
            Err(_) => bail!("dst_dapp_id cannot be parsed"),
        };

        Ok(Self {
            nonce: nonce.to_string(),
            tvm_message,
            hash,
            thread_id,
            ext_message_token,
            dst_dapp_id,
        })
    }

    pub fn hash(&self) -> &UInt256 {
        &self.hash
    }

    pub fn tvm_message(&self) -> &Message {
        &self.tvm_message
    }

    pub fn into_tvm_message(self) -> Message {
        self.tvm_message
    }

    pub fn thread_id(&self) -> ThreadIdentifier {
        self.thread_id
    }

    pub fn dst_dapp_id(&self) -> Option<DAppIdentifier> {
        self.dst_dapp_id
    }

    pub fn is_dst_exists(&self) -> bool {
        self.tvm_message.int_dst_account_id().is_some()
    }
}

impl fmt::Debug for NotQueuedExtMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalMessage")
            .field("nonce", &self.nonce)
            .field("hash", &self.hash)
            .field("thread_id", &self.thread_id)
            .field("ext_message_token", &self.ext_message_token)
            .field("dst_dapp_id", &self.dst_dapp_id)
            .finish()
    }
}
