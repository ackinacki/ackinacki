// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use num_bigint::BigUint;
use tvm_block::Account;
use tvm_block::AccountStorage;
use tvm_block::CurrencyCollection;
use tvm_block::Deserializable;
use tvm_block::ExtraCurrencyCollection;
use tvm_block::GetRepresentationHash;
use tvm_block::Grams;
use tvm_block::InternalMessageHeader;
use tvm_block::Message;
use tvm_block::MsgAddressInt;
use tvm_block::Serializable;
use tvm_block::ShardAccount;
use tvm_block::StateInit;
use tvm_block::StorageInfo;
use tvm_client::boc::set_code_salt_cell;
use tvm_types::AccountId;
use tvm_types::BuilderData;
use tvm_types::Cell;
use tvm_types::UInt256;

use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperStatus;
use crate::bls::gosh_bls::PubKey;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::node::SignerIndex;
use crate::storage::MessageDurableStorage;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::ThreadIdentifier;
use crate::zerostate::ZeroState;

const ZERO_ACCOUNT: &str = "0000000000000000000000000000000000000000000000000000000000000000";

impl ZeroState {
    pub fn add_account<P: AsRef<Path>>(
        &mut self,
        path: P,
        address: Option<String>,
        dapp_id: Option<UInt256>,
        balance: Option<CurrencyCollection>,
        salt: Option<Cell>,
        thread_identifier: &ThreadIdentifier,
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
            UInt256::from_str(&address)
                .map_err(|e| anyhow::format_err!("Failed to convert address to UInt256: {e}"))?
        } else {
            state_init
                .hash()
                .map_err(|e| anyhow::format_err!("Failed to calculate state init hash: {e}"))?
        };
        let address = MsgAddressInt::with_standart(None, 0, AccountId::from(account_id.clone()))
            .map_err(|e| anyhow::format_err!("Failed to construct msg address: {e}"))?;
        let storage = AccountStorage::active_by_init_code_hash(
            0,
            CurrencyCollection::default(),
            state_init,
            true,
        );
        let last_paid = SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs() as u32;
        let storage_stat = StorageInfo::with_values(last_paid, None);
        let mut account = Account::with_storage(&address, &storage_stat, &storage);
        // We can't create valid storage stat, it should be updated internally
        account
            .update_storage_stat()
            .map_err(|e| anyhow::format_err!("Failed to update account storage stat: {e}"))?;
        if let Some(balance) = balance {
            account.set_balance(balance);
        }

        let shard_account =
            ShardAccount::with_params(&account, UInt256::default(), 0, dapp_id.clone())
                .map_err(|e| anyhow::format_err!("Failed to create shard account: {e}"))?;
        // Add account to zerostate
        let mut shard_state = self.get_shard_state(thread_identifier)?.deref().clone();
        shard_state
            .insert_account(&account_id, &shard_account)
            .map_err(|e| anyhow::format_err!("Failed to insert account to shard state: {e}"))?;
        self.state_mut(thread_identifier)?.set_shard_state(Arc::new(shard_state));
        #[cfg(feature = "monitor-accounts-number")]
        {
            let state = self.state_mut(thread_identifier)?;
            state.accounts_number += 1;
        }
        Ok(account_id.to_hex_string())
    }

    pub fn add_account_to_zerostate(
        &mut self,
        thread_id: &ThreadIdentifier,
        account: Account,
        dapp_id: UInt256,
    ) -> anyhow::Result<()> {
        let account_id = account
            .get_id()
            .ok_or(anyhow::format_err!("Account does not have account id set"))?
            .to_hex_string();
        let account_id = UInt256::from_str(&account_id)
            .map_err(|e| anyhow::format_err!("failed to convert account_id {e}"))?;
        let shard_account =
            ShardAccount::with_params(&account, UInt256::default(), 0, Some(dapp_id))
                .map_err(|e| anyhow::format_err!("Failed to create shard account: {e}"))?;
        // Add account to zerostate
        let mut shard_state = self.get_shard_state(thread_id)?.deref().clone();
        shard_state
            .insert_account(&account_id, &shard_account)
            .map_err(|e| anyhow::format_err!("Failed to insert account to shard state: {e}"))?;
        self.state_mut(thread_id)?.set_shard_state(Arc::new(shard_state));
        #[cfg(feature = "monitor-accounts-number")]
        {
            let state = self.state_mut(thread_id)?;
            state.accounts_number += 1;
        }
        Ok(())
    }

    pub fn add_message_from_zeroes(
        &mut self,
        dest: String,
        value: Option<u128>,
        ecc: Option<ExtraCurrencyCollection>,
        dapp_id: Option<UInt256>,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        // Generate internal message header
        let dest_acc_id = AccountId::from_string(&dest)
            .map_err(|e| anyhow::format_err!("Failed to construct dest address: {e}"))?;

        let mut cc = if let Some(value) = value {
            CurrencyCollection::from_grams(
                Grams::new(value).map_err(|e| anyhow::format_err!("Failed to init grams: {e}"))?,
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
                AccountId::from_string(ZERO_ACCOUNT)
                    .map_err(|e| anyhow::format_err!("Failed to convert address: {e}"))?,
            )
            .map_err(|e| anyhow::format_err!("Failed to create message header: {e}"))?,
            MsgAddressInt::with_standart(None, 0, dest_acc_id.clone())
                .map_err(|e| anyhow::format_err!("Failed to convert dest address: {e}"))?,
            cc,
        );
        header.set_src_dapp_id(dapp_id);

        // Generate internal message
        let message = Message::with_int_header(header.clone());
        self.add_message(message, thread_identifier)
    }

    pub fn add_message(
        &mut self,
        message: Message,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        assert!(message.is_internal());
        // Add message to message queue
        // let info = message.int_header().unwrap();
        // let dest_acc_id = message.int_header().unwrap().dst.address();
        // let fwd_fee = *info.fwd_fee();
        // let msg_cell = message
        //     .serialize()
        //     .map_err(|e| anyhow::format_err!("Failed to serialize message: {e}"))?;
        // let envelope = MsgEnvelope::with_message_and_fee(&message, fwd_fee)
        //     .map_err(|e| anyhow::format_err!("Failed to envelope message: {e}"))?;
        // let enq = EnqueuedMsg::with_param(info.created_lt, &envelope)
        //     .map_err(|e| anyhow::format_err!("Failed to enqueue message: {e}"))?;
        // let prefix = dest_acc_id
        //     .clone()
        //     .get_next_u64()
        //     .map_err(|e| anyhow::format_err!("Failed to get acc prefix: {e}"))?;
        // let key = OutMsgQueueKey::with_workchain_id_and_prefix(0, prefix, msg_cell.repr_hash());

        {
            let optimistic_state = self.state_mut(thread_identifier)?;
            let dest = message.int_dst_account_id().map(From::from).unwrap();
            let message = WrappedMessage { message };
            let message_key = MessageIdentifier::from(&message);
            let message_db = MessageDurableStorage::mem();
            optimistic_state.messages = ThreadMessageQueueState::build_next()
                .with_initial_state(optimistic_state.messages.clone())
                .with_consumed_messages(HashMap::new())
                .with_produced_messages(HashMap::from([(
                    dest,
                    vec![(message_key, Arc::new(message))],
                )]))
                .with_removed_accounts(vec![])
                .with_added_accounts(BTreeMap::new())
                .with_db(message_db.clone())
                .build()
                .unwrap();
        }

        // let mut shard_state = self.get_shard_state(thread_identifier)?.deref().clone();

        // let mut message_queue = shard_state
        //     .read_out_msg_queue_info()
        //     .map_err(|e| anyhow::format_err!("Failed to read out msg queue info: {e}"))?;
        // message_queue
        //     .out_queue_mut()
        //     .set(&key, &enq, &enq.aug().map_err(|e| anyhow::format_err!("Failed to get aug: {e}"))?)
        //     .map_err(|e| anyhow::format_err!("Failed to put msg to queue: {e}"))?;
        //
        // // Update zerostate
        // shard_state
        //     .write_out_msg_queue_info(&message_queue)
        //     .map_err(|e| anyhow::format_err!("Failed to write out msg queue info: {e}"))?;
        //
        // self.state_mut(thread_identifier)?.set_shard_state(Arc::new(shard_state));
        Ok(())
    }

    pub fn calculate_block_keeper_wallet_address(
        _owner_pubkey: UInt256,
        mut data: StateInit,
    ) -> anyhow::Result<UInt256> {
        let code = data.code().unwrap();
        let mut b = BuilderData::new();
        _owner_pubkey.write_to(&mut b).map_err(|e| anyhow::format_err!("{e}"))?;
        let stateinitcode = set_code_salt_cell(code.clone(), b.into_cell().unwrap())
            .map_err(|e| anyhow::format_err!("{e}"))?;
        data.set_code(stateinitcode);
        data.hash().map_err(|e| anyhow::format_err!("{e}"))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_block_keeper(
        &mut self,
        wallet_address: String,
        pubkey: String,
        epoch_finish_seq_no: u64,
        wait_step: u64,
        stake: BigUint,
        thread_id: ThreadIdentifier,
        signer_index: SignerIndex,
        owner_pubkey: String,
    ) {
        let wallet_address = AccountId::from_string(&wallet_address).unwrap();
        let owner_pubkey =
            UInt256::from_str(&owner_pubkey).expect("Failed to load owner_pubkey from str");
        self.block_keeper_set.entry(thread_id).or_default().insert(
            signer_index,
            BlockKeeperData {
                pubkey: PubKey::from_str(&pubkey).expect("Failed to load pubkey from str"),
                epoch_finish_seq_no: Some(epoch_finish_seq_no),
                wait_step,
                status: BlockKeeperStatus::Active,
                address: String::new(),
                stake,
                owner_address: wallet_address.into(),
                signer_index,
                owner_pubkey: owner_pubkey.inner(),
            },
        );
    }
}
