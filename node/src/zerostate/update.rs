// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::str::FromStr;
use std::time::SystemTime;

use num_bigint::BigUint;
use tvm_block::Account;
use tvm_block::AccountStorage;
use tvm_block::Augmentation;
use tvm_block::CurrencyCollection;
use tvm_block::Deserializable;
use tvm_block::EnqueuedMsg;
use tvm_block::ExtraCurrencyCollection;
use tvm_block::GetRepresentationHash;
use tvm_block::Grams;
use tvm_block::HashmapAugType;
use tvm_block::InternalMessageHeader;
use tvm_block::Message;
use tvm_block::MsgAddressInt;
use tvm_block::MsgEnvelope;
use tvm_block::OutMsgQueueKey;
use tvm_block::Serializable;
use tvm_block::ShardAccount;
use tvm_block::StateInit;
use tvm_block::StorageInfo;
use tvm_types::AccountId;
use tvm_types::Cell;
use tvm_types::UInt256;

use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperStatus;
use crate::bls::gosh_bls::PubKey;
use crate::node::SignerIndex;
use crate::zerostate::ZeroState;

const ZERO_ACCOUNT: &str = "0000000000000000000000000000000000000000000000000000000000000000";

impl ZeroState {
    pub fn add_account<P: AsRef<Path>>(
        &mut self,
        path: P,
        address: Option<String>,
        dapp: Option<UInt256>,
        balance: Option<CurrencyCollection>,
        salt: Option<Cell>,
    ) -> anyhow::Result<String> {
        // Load state init
        let mut state_init = StateInit::construct_from_file(path.as_ref()).map_err(|e| {
            anyhow::format_err!("Failed to construct StateInit from file {:?}: {e}", path.as_ref())
        })?;

        if let Some(salt) = salt {
            if let Some(code) = state_init.code.clone() {
                let new_code = tvm_client::boc::set_code_salt_cell(code, salt).unwrap();
                state_init.set_code(new_code);
            }
        }

        // Generate account helper structs and account itself
        let account_id = if let Some(address) = address {
            UInt256::from_str(&address).map_err(|e| anyhow::format_err!("{e}"))?
        } else {
            state_init.hash().map_err(|e| anyhow::format_err!("{e}"))?
        };
        let address = MsgAddressInt::with_standart(None, 0, AccountId::from(account_id.clone()))
            .map_err(|e| anyhow::format_err!("{e}"))?;
        let storage = AccountStorage::active_by_init_code_hash(
            0,
            CurrencyCollection::default(),
            state_init,
            true,
        );
        let dapp_id = if let Some(dapp) = dapp { dapp } else { UInt256::default() };
        let last_paid = SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs() as u32;
        let storage_stat = StorageInfo::with_values(last_paid, None);
        let mut account = Account::with_storage(&address, dapp_id, &storage_stat, &storage);
        // We can't create valid storage stat, it should be updated internally
        account.update_storage_stat().map_err(|e| anyhow::format_err!("{e}"))?;
        if let Some(balance) = balance {
            account.set_balance(balance);
        }

        let shard_account = ShardAccount::with_params(&account, UInt256::default(), 0)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        // Add account to zerostate
        self.shard_state
            .insert_account(&account_id, &shard_account)
            .map_err(|e| anyhow::format_err!("{e}"))?;

        Ok(account_id.to_hex_string())
    }

    pub fn add_message_from_zeroes(
        &mut self,
        dest: String,
        value: Option<u128>,
        ecc: Option<ExtraCurrencyCollection>,
        dapp_id: Option<UInt256>,
    ) -> anyhow::Result<()> {
        // Generate internal message header
        let dest_acc_id = AccountId::from_string(&dest).map_err(|e| anyhow::format_err!("{e}"))?;

        let mut cc = if let Some(value) = value {
            CurrencyCollection::from_grams(
                Grams::new(value).map_err(|e| anyhow::format_err!("{e}"))?,
            )
        } else {
            CurrencyCollection::default()
        };
        if let Some(ecc) = ecc {
            cc.other = ecc;
        }

        let mut header = InternalMessageHeader::with_addresses(
            MsgAddressInt::with_standart(
                None,
                0,
                AccountId::from_string(ZERO_ACCOUNT).map_err(|e| anyhow::format_err!("{e}"))?,
            )
            .map_err(|e| anyhow::format_err!("{e}"))?,
            MsgAddressInt::with_standart(None, 0, dest_acc_id.clone())
                .map_err(|e| anyhow::format_err!("{e}"))?,
            cc,
        );
        header.set_src_dapp_id(dapp_id);

        // Generate internal message
        let message = Message::with_int_header(header.clone());
        self.add_message(message)
    }

    pub fn add_message(&mut self, message: Message) -> anyhow::Result<()> {
        assert!(message.is_internal());
        // Add message to message queue
        let info = message.int_header().unwrap();
        let dest_acc_id = message.int_header().unwrap().dst.address();
        let fwd_fee = *info.fwd_fee();
        let msg_cell = message.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
        let envelope = MsgEnvelope::with_message_and_fee(&message, fwd_fee)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        let enq = EnqueuedMsg::with_param(info.created_lt, &envelope)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        let prefix = dest_acc_id.clone().get_next_u64().map_err(|e| anyhow::format_err!("{e}"))?;
        let key = OutMsgQueueKey::with_workchain_id_and_prefix(0, prefix, msg_cell.repr_hash());
        let mut message_queue =
            self.shard_state.read_out_msg_queue_info().map_err(|e| anyhow::format_err!("{e}"))?;
        message_queue
            .out_queue_mut()
            .set(&key, &enq, &enq.aug().map_err(|e| anyhow::format_err!("{e}"))?)
            .map_err(|e| anyhow::format_err!("{e}"))?;

        // Update zerostate
        self.shard_state
            .write_out_msg_queue_info(&message_queue)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        Ok(())
    }

    pub fn add_block_keeper(
        &mut self,
        index: SignerIndex,
        pubkey: String,
        epoch_finish_timestamp: u32,
        stake: BigUint,
    ) {
        self.block_keeper_set.insert(
            index,
            BlockKeeperData {
                index,
                pubkey: PubKey::from_str(&pubkey).expect("Failed to load pubkey from str"),
                epoch_finish_timestamp,
                status: BlockKeeperStatus::Active,
                address: String::new(),
                stake,
            },
        );
    }
}
