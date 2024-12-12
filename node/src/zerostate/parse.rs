// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use serde::Serialize;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;

use super::ZeroState;
use crate::types::ThreadIdentifier;

#[derive(Serialize)]
pub struct ZeroStateMessage {
    id: String,
    source: String,
    destination: String,
    value: u128,
    ecc: HashMap<u32, String>,
    src_dapp_id: Option<String>,
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
                .iterate_with_keys(|_k, v| {
                    let account = v.read_account().unwrap();
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
                        dapp_id: account
                            .get_dapp_id()
                            .map(|val| {
                                val.clone()
                                    .map(|dapp_id| dapp_id.to_hex_string())
                                    .unwrap_or("None".to_string())
                            })
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
        let out_msgs = self.get_shard_state(thread_identifier)?.read_out_msg_queue_info();
        if let Ok(msgs) = out_msgs {
            msgs.out_queue()
                .iterate_with_keys(|_k, v| {
                    let message = v.out_msg.read_struct().unwrap().read_message().unwrap();
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
                        id: message.hash()?.to_hex_string(),
                        src_dapp_id: message
                            .int_header()
                            .and_then(|hdr| hdr.src_dapp_id.clone().map(|val| val.to_hex_string())),
                    });
                    Ok(true)
                })
                .map_err(|e| anyhow::format_err!("Failed to iterate out msgs: {e}"))?;
        }
        Ok(zs_messages)
    }
}
