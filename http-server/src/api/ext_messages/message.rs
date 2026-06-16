// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use anyhow::bail;
use ext_messages_auth::auth::Token;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;
use tvm_types::UInt256;

use crate::api::validation::parse_hex32;
use crate::helpers::parse_message;

#[derive(Clone)]
pub struct NotQueuedExtMessage {
    nonce: String, // Some external identifier, currently used only for debugging output.
    tvm_message: Message,
    hash: UInt256,
    thread_id: ThreadIdentifier,
    pub ext_message_token: Option<Token>,
    dapp_id: DAppIdentifier,
    account_id: AccountIdentifier,
}

impl NotQueuedExtMessage {
    pub fn try_new(
        nonce: &str,
        tvm_message_base64: &str,
        thread_id: Option<String>,
        ext_message_token: Option<Token>,
        dapp_id: String,
        account_id: String,
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
        let Some(dst_account_id) = tvm_message.int_dst_account_id() else {
            bail!("Invalid destination");
        };
        let Ok(hash) = tvm_message.hash() else {
            bail!("Invalid ExternalMessage, id {nonce}: hash() must return UInt256")
        };

        let thread_id = match thread_id.clone().map(ThreadIdentifier::try_from).transpose() {
            Ok(Some(id)) => id,
            Ok(None) => ThreadIdentifier::default(),
            Err(err) => {
                bail!(
                    "thread_id can not be parsed: {err}, thread_id: {}",
                    thread_id.unwrap_or_default()
                )
            }
        };

        let dapp_id_normalized =
            parse_hex32(&dapp_id, "dapp_id").map_err(|e| anyhow::anyhow!(e))?;
        let account_id_normalized =
            parse_hex32(&account_id, "account_id").map_err(|e| anyhow::anyhow!(e))?;

        let dapp_id = DAppIdentifier::from_str(&dapp_id_normalized)
            .map_err(|e| anyhow::anyhow!("dapp_id cannot be parsed: {e}"))?;
        let account_id = AccountIdentifier::from_str(&account_id_normalized)
            .map_err(|e| anyhow::anyhow!("account_id cannot be parsed: {e}"))?;
        if account_id != AccountIdentifier::from(dst_account_id) {
            bail!("account_id does not match message destination");
        }

        Ok(Self {
            nonce: nonce.to_string(),
            tvm_message,
            hash,
            thread_id,
            ext_message_token,
            dapp_id,
            account_id,
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

    pub fn dapp_id(&self) -> DAppIdentifier {
        self.dapp_id
    }

    pub fn account_id(&self) -> AccountIdentifier {
        self.account_id
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
            .field("dapp_id", &self.dapp_id)
            .field("account_id", &self.account_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use tvm_block::ExternalInboundMessageHeader;
    use tvm_block::Message as TvmMessage;
    use tvm_block::MsgAddressExt;
    use tvm_block::MsgAddressInt;
    use tvm_block::Serializable;
    use tvm_types::AccountId;

    use super::*;

    const VALID_HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OTHER_HEX: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    fn boc_base64_of_inbound_external(dst_account_id: &str) -> anyhow::Result<String> {
        let dst = MsgAddressInt::with_standart(
            None,
            0,
            AccountId::from_string(dst_account_id).map_err(|e| anyhow::anyhow!("{e}"))?,
        )?;
        let header = ExternalInboundMessageHeader::new(MsgAddressExt::AddrNone, dst);
        let message = TvmMessage::with_ext_in_header(header);
        let cell = message.serialize().map_err(|e| anyhow::anyhow!("{e}"))?;
        let boc = tvm_types::write_boc(&cell).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(tvm_types::base64_encode(&boc))
    }

    #[test]
    fn try_new_rejects_invalid_dapp_id_format() {
        let err = NotQueuedExtMessage::try_new(
            "id",
            &boc_base64_of_inbound_external(VALID_HEX).unwrap(),
            None,
            None,
            "0xnothex".to_string(),
            VALID_HEX.to_string(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("dapp_id"));
    }

    #[test]
    fn try_new_rejects_invalid_account_id_format() {
        let err = NotQueuedExtMessage::try_new(
            "id",
            &boc_base64_of_inbound_external(VALID_HEX).unwrap(),
            None,
            None,
            VALID_HEX.to_string(),
            "tooshort".to_string(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("account_id"));
    }

    #[test]
    fn try_new_rejects_account_id_that_does_not_match_message_destination() {
        let err = NotQueuedExtMessage::try_new(
            "id",
            &boc_base64_of_inbound_external(VALID_HEX).unwrap(),
            None,
            None,
            VALID_HEX.to_string(),
            OTHER_HEX.to_string(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("account_id does not match message destination"));
    }
}
