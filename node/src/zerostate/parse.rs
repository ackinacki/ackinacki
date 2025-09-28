// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use account_inbox::storage::DurableStorageRead;
use account_inbox::storage::LoadErr;
use serde::Serialize;
use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_types::base64_encode;
use tvm_types::write_boc;

use super::ZeroState;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;
use crate::types::ThreadIdentifier;

#[derive(Default)]
struct DummyMessageStorage;

impl DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>> for DummyMessageStorage {
    type LoadError = LoadErr;

    fn next(&self, _key: &MessageIdentifier) -> Result<Option<MessageIdentifier>, Self::LoadError> {
        Ok(None)
    }

    fn load_message(
        &self,
        _key: &MessageIdentifier,
    ) -> Result<Arc<WrappedMessage>, Self::LoadError> {
        unreachable!()
    }

    fn remaining_messages(
        &self,
        _starting_key: &MessageIdentifier,
        _limit: usize,
    ) -> Result<Vec<Arc<WrappedMessage>>, Self::LoadError> {
        Ok(vec![])
    }
}

#[derive(Serialize)]
pub struct ZeroStateMessage {
    id: String,
    source: String,
    destination: String,
    value: u128,
    ecc: HashMap<u32, String>,
    src_dapp_id: Option<String>,
    boc: String,
}

#[derive(Serialize)]
pub struct ZeroStateAccount {
    address: String,
    code_hash: String,
    balance: u128,
    dapp_id: String,
}

impl ZeroState {
    pub fn read_all_accounts(
        &mut self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<ZeroStateAccount>> {
        let mut zs_accounts = vec![];
        let accounts = self.get_shard_state(thread_identifier)?.read_accounts();
        if let Ok(accounts) = accounts {
            accounts
                .iterate_accounts(|_, v| {
                    let account = v.read_account().unwrap().as_struct()?;
                    zs_accounts.push(ZeroStateAccount {
                        address: account
                            .get_id()
                            .map(|acc| acc.to_hex_string())
                            .unwrap_or("None".to_string()),
                        code_hash: account
                            .get_code_hash()
                            .map(|code| code.to_hex_string())
                            .unwrap_or("None".to_string()),
                        balance: account.balance().map(|cc| cc.grams.inner()).unwrap_or(0),
                        dapp_id: v
                            .get_dapp_id()
                            .map(|val| val.to_hex_string())
                            .unwrap_or("None".to_string()),
                    });
                    Ok(true)
                })
                .map_err(|e| anyhow::format_err!("Failed to iterate accounts: {e}"))?;
        }
        Ok(zs_accounts)
    }

    pub fn read_all_messages(
        &mut self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<ZeroStateMessage>> {
        let mut zs_messages = vec![];
        let state = self.state(thread_identifier)?;
        let messages_queue = state.messages().clone();
        let message_db = DummyMessageStorage;
        let acc_iter = messages_queue.iter(&message_db);
        for account_messages in acc_iter {
            for msg in account_messages {
                let (message, _) =
                    msg.map_err(|e| anyhow::format_err!("Failed to unpack message: {e:?}"))?;
                let message = message.message.clone();
                let message_cell = message.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
                let byes = write_boc(&message_cell).map_err(|e| anyhow::format_err!("{e}"))?;
                let boc = base64_encode(&byes);
                zs_messages.push(ZeroStateMessage {
                    source: message
                        .get_int_src_account_id()
                        .map(|src| src.to_hex_string())
                        .unwrap_or("None".to_string()),
                    destination: message
                        .int_dst_account_id()
                        .map(|dst| dst.to_hex_string())
                        .unwrap_or("None".to_string()),
                    value: message.value().map(|cc| cc.grams.inner()).unwrap_or(0),
                    ecc: message
                        .value()
                        .map(|cc| {
                            let mut res = HashMap::new();
                            cc.other
                                .iterate_with_keys(|k, v| {
                                    res.insert(k, v.value().to_string());
                                    Ok(true)
                                })
                                .unwrap();
                            res
                        })
                        .unwrap_or_default(),
                    id: message.hash().unwrap().to_hex_string(),
                    src_dapp_id: message
                        .int_header()
                        .and_then(|hdr| hdr.src_dapp_id.clone().map(|val| val.to_hex_string())),
                    boc,
                });
            }
        }
        Ok(zs_messages)
    }
}
