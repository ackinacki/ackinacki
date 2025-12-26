use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "mirror_repair")]
use parking_lot::Mutex;
use tracing::instrument;
#[cfg(feature = "mirror_repair")]
use tvm_abi::TokenValue;
#[cfg(feature = "mirror_repair")]
use tvm_block::Account;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_block::ShardAccount;
#[cfg(feature = "mirror_repair")]
use tvm_block::ShardAccounts;
use tvm_types::AccountId;

#[cfg(feature = "mirror_repair")]
use crate::config::UPDATE_MIRRORS_BLOCK_SEQ_NO;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;
use crate::types::AccountInbox;
use crate::types::AccountRouting;
use crate::types::BlockIdentifier;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[cfg(feature = "mirror_repair")]
pub static MIRROR_TVC: &[u8] =
    include_bytes!("../../../contracts/0.79.3_compiled/mvsystem/Mirror.tvc.temp");
#[cfg(feature = "mirror_repair")]
pub static MVROOT_TVC: &[u8] =
    include_bytes!("../../repair_accounts_data/MobileVerifiersContractRoot.tvc");
#[cfg(feature = "mirror_repair")]
pub static MIRROR_ABI: &str =
    include_str!("../../../contracts/0.79.3_compiled/mvsystem/Mirror.abi.json");

#[cfg(feature = "mirror_repair")]
static GAME_ROOT_ADDR: &str = "0:0505050505050505050505050505050505050505050505050505050505050505";
#[cfg(feature = "mirror_repair")]
static GAME_ROOT_DAPP_ID: &str = "0000000000000000000000000000000000000000000000000000000000000001";
#[cfg(feature = "mirror_repair")]
static GAME_ROOT_TVC: &[u8] = include_bytes!(
    "../../../contracts/0.79.3_compiled/mvsystem/MobileVerifiersContractGameRoot.tvc.temp"
);
#[cfg(feature = "mirror_repair")]
static GAME_ROOT_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/mvsystem/MobileVerifiersContractGameRoot.abi.json"
);
#[cfg(feature = "mirror_repair")]
static ROOT_MV_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/mvsystem/MobileVerifiersContractRoot.abi.json"
);
#[cfg(feature = "mirror_repair")]
static ROOT_MV_ADDRESS: &str = "0:2222222222222222222222222222222222222222222222222222222222222222";
#[cfg(feature = "mirror_repair")]
static MV_CONFIG_ADDRESS: &str =
    "0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn postprocess(
    mut initial_optimistic_state: OptimisticStateImpl,
    consumed_internal_messages: HashMap<AccountAddress, HashSet<MessageIdentifier>>,
    mut produced_internal_messages_to_the_current_thread: HashMap<
        AccountAddress,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    accounts_that_changed_their_dapp_id: HashMap<AccountRouting, Option<WrappedAccount>>,
    block_id: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    mut new_state: OptimisticShardState,
    mut produced_internal_messages_to_other_threads: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    block_info: BlockInfo,
    thread_id: ThreadIdentifier,
    threads_table: ThreadsTable,
    block_accounts: HashSet<AccountAddress>,
    accounts_repo: AccountsRepository,
    db: MessageDurableStorage,
    #[cfg(feature = "mirror_repair")] is_updated_mv: Arc<Mutex<bool>>,
    #[cfg(feature = "monitor-accounts-number")] updated_accounts_number: u64,
    is_block_of_retired_version: bool,
) -> anyhow::Result<(OptimisticStateImpl, CrossThreadRefData)> {
    // Prepare produced_internal_messages_to_the_current_thread
    for (addr, messages) in produced_internal_messages_to_the_current_thread.iter_mut() {
        let mut sorted = if let Some(consumed_messages) = consumed_internal_messages.get(addr) {
            let ids_set =
                HashSet::<MessageIdentifier>::from_iter(messages.iter().map(|(id, _)| id.clone()));
            let intersection: HashSet<&MessageIdentifier> =
                HashSet::from_iter(ids_set.intersection(consumed_messages));
            let mut consumed_part = vec![];
            messages.retain(|el| {
                if intersection.contains(&el.0) {
                    consumed_part.push((el.0.clone(), el.1.clone()));
                    false
                } else {
                    true
                }
            });
            consumed_part
        } else {
            vec![]
        };
        messages.sort_by(|a, b| a.1.cmp(&b.1));
        sorted.extend(messages.clone());
        *messages = sorted;
    }

    produced_internal_messages_to_other_threads
        .iter_mut()
        .for_each(|(_addr, messages)| messages.sort_by(|a, b| a.1.cmp(&b.1)));

    let mut new_thread_refs = initial_optimistic_state.thread_refs_state.clone();
    let current_thread_id = *initial_optimistic_state.get_thread_id();
    let current_thread_last_block = (current_thread_id, block_id.clone(), block_seq_no);
    new_thread_refs.update(current_thread_id, current_thread_last_block.clone());

    let mut removed_accounts: Vec<AccountAddress> = vec![];
    let mut outbound_accounts: HashMap<
        AccountRouting,
        (Option<WrappedAccount>, Option<AccountInbox>),
    > = HashMap::new();

    let messages = ThreadMessageQueueState::build_next()
        .with_initial_state(initial_optimistic_state.messages)
        .with_consumed_messages(consumed_internal_messages)
        .with_produced_messages(produced_internal_messages_to_the_current_thread)
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .with_db(db.clone())
        .build()?;
    initial_optimistic_state.messages = messages;

    for (routing, account) in &accounts_that_changed_their_dapp_id {
        // Note: we are using input state threads table to determine if this account should be in
        //  the descendant state. In case of thread split those accounts will be cropped into the
        // correct thread.
        if !initial_optimistic_state.does_routing_belong_to_the_state(routing) {
            removed_accounts.push(routing.1.clone());
            let account_inbox =
                initial_optimistic_state.messages.account_inbox(&routing.1).cloned();
            outbound_accounts.insert(routing.clone(), (account.clone(), account_inbox));
        }
    }
    if !outbound_accounts.is_empty() {
        let mut shard_state = new_state.into_shard_state().as_ref().clone();
        let mut shard_accounts = shard_state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read accounts from shard state: {e}"))?;
        for (account_routing, (_, _)) in &outbound_accounts {
            let account_id = account_routing.1 .0.clone();
            // let default_account_routing = AccountRouting(
            //     DAppIdentifier(account_routing.1.clone()),
            //     account_routing.1.clone(),
            // );
            // if initial_optimistic_state.does_routing_belong_to_the_state(&default_account_routing) {

            let acc_id: AccountId = account_id.clone().into();
            if shard_accounts
                .account(&acc_id)
                .map_err(|e| anyhow::format_err!("Failed to check account: {e}"))?
                .is_some()
            {
                tracing::debug!(target: "node", "replace account with redirect: {:?}", account_routing);
                shard_accounts.replace_with_redirect(&account_id).map_err(|e| {
                    anyhow::format_err!("Failed to insert stub to shard state: {e}")
                })?;
            }
            // }
        }
        shard_state
            .write_accounts(&shard_accounts)
            .map_err(|e| anyhow::format_err!("Failed to write accounts to shard state: {e}"))?;
        new_state = shard_state.into();
    }

    let messages = ThreadMessageQueueState::build_next()
        .with_initial_state(initial_optimistic_state.messages)
        .with_consumed_messages(HashMap::new())
        .with_produced_messages(HashMap::new())
        .with_removed_accounts(removed_accounts)
        .with_added_accounts(BTreeMap::new())
        .with_db(db.clone())
        .build()?;
    initial_optimistic_state.messages = messages;

    let mut changed_accounts = initial_optimistic_state.changed_accounts;
    let mut cached_accounts = initial_optimistic_state.cached_accounts;
    if let Some(unload_after) = accounts_repo.get_unload_after() {
        changed_accounts.extend(block_accounts.into_iter().map(|acc| (acc, block_seq_no)));
        cached_accounts.retain(|account_id, (seq_no, _)| {
            if *seq_no + accounts_repo.get_store_after() >= block_seq_no
                && !changed_accounts.contains_key(account_id)
            {
                true
            } else {
                tracing::trace!(
                    account_id = account_id.to_hex_string(),
                    "Removing account from cache"
                );
                false
            }
        });
        let mut shard_state = new_state.into_shard_state().as_ref().clone();
        let mut shard_accounts = shard_state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read accounts from shard state: {e}"))?;
        let mut deleted = Vec::new();
        for (account_id, seq_no) in std::mem::take(&mut changed_accounts) {
            if seq_no + unload_after <= block_seq_no {
                if let Some(mut account) =
                    shard_accounts.account(&(&account_id).into()).map_err(|e| {
                        anyhow::format_err!("Failed to read account from shard state: {e}")
                    })?
                {
                    tracing::trace!(
                        account_id = account_id.to_hex_string(),
                        "Unloading account from state"
                    );
                    let cell = account
                        .replace_with_external()
                        .map_err(|e| anyhow::format_err!("Failed to set account external: {e}"))?;
                    cached_accounts.insert(account_id.clone(), (block_seq_no, cell));
                    shard_accounts.insert(&account_id.0, &account).map_err(|e| {
                        anyhow::format_err!("Failed to insert account into shard state: {e}")
                    })?;
                } else {
                    deleted.push(account_id);
                }
            } else {
                changed_accounts.insert(account_id, seq_no);
            }
        }
        if !deleted.is_empty() {
            accounts_repo.accounts_deleted(&thread_id, deleted, block_info.prev1().unwrap().end_lt);
        }
        shard_state
            .write_accounts(&shard_accounts)
            .map_err(|e| anyhow::format_err!("Failed to write accounts: {e}"))?;
        new_state = shard_state.into();
    }

    #[cfg(feature = "mirror_repair")]
    {
        if <u32>::from(block_seq_no) >= UPDATE_MIRRORS_BLOCK_SEQ_NO && !is_block_of_retired_version
        {
            let mut lock = is_updated_mv.lock();
            if !*lock {
                let mut shard_state = new_state.into_shard_state().as_ref().clone();
                let mut shard_accounts = shard_state.read_accounts().map_err(|e| {
                    anyhow::format_err!("Failed to read accounts from shard state: {e}")
                })?;
                if !matches!(check_updated_mv_accounts(&mut shard_accounts), Ok(true)) {
                    let res = mv_config_update_dapp(&mut shard_accounts);
                    tracing::trace!("mv_config_update_dapp result: {res:?}");
                    let res = repair_mirror_accounts(&mut shard_accounts);
                    tracing::trace!("repair_mirror_accounts result: {res:?}");
                    let res = init_mv_game_root(&mut shard_accounts);
                    tracing::trace!("init_mv_game_root result: {res:?}");
                    let res = change_mv_root(&mut shard_accounts);
                    tracing::trace!("change_mv_root result: {res:?}");
                    shard_state
                        .write_accounts(&shard_accounts)
                        .map_err(|e| anyhow::format_err!("Failed to write accounts: {e}"))?;
                    new_state = shard_state.into();
                }
                *lock = true;
            }
        }
    }

    #[cfg(feature = "monitor-accounts-number")]
    let new_state = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id.clone())
        .shard_state(new_state)
        .messages(initial_optimistic_state.messages)
        .high_priority_messages(initial_optimistic_state.high_priority_messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped)
        .changed_accounts(changed_accounts)
        .cached_accounts(cached_accounts)
        .accounts_number(updated_accounts_number)
        .build();

    #[cfg(not(feature = "monitor-accounts-number"))]
    let new_state = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id.clone())
        .shard_state(new_state)
        .messages(initial_optimistic_state.messages)
        .high_priority_messages(initial_optimistic_state.high_priority_messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped)
        .changed_accounts(changed_accounts)
        .cached_accounts(cached_accounts)
        .build();

    let cross_thread_ref_data = CrossThreadRefData::builder()
        .block_identifier(block_id.clone())
        .block_seq_no(block_seq_no)
        .block_thread_identifier(thread_id)
        .outbound_messages(produced_internal_messages_to_other_threads)
        .outbound_accounts(outbound_accounts)
        .threads_table(threads_table.clone())
        .parent_block_identifier(initial_optimistic_state.block_id.clone())
        .block_refs(vec![]) // set up later
        .build();

    Ok((new_state, cross_thread_ref_data))
}

#[cfg(feature = "mirror_repair")]
fn check_updated_mv_accounts(accounts: &mut ShardAccounts) -> anyhow::Result<bool> {
    use tvm_block::StateInit;
    let root_game = AccountAddress::from_str(GAME_ROOT_ADDR)
        .map_err(|e| anyhow::format_err!("Failed to calculate root_mv addr: {e}"))?;
    let acc_id: AccountId = root_game.clone().into();
    let shard_account = accounts
        .account(&acc_id)
        .map_err(|e| anyhow::format_err!("Failed to read account from shard state: {e}"))?
        .ok_or_else(|| anyhow::format_err!("Account not found in shard state"))?;
    let old_account = shard_account
        .read_account()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?
        .as_struct()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?;
    let state_init = StateInit::construct_from_bytes(GAME_ROOT_TVC)
        .map_err(|e| anyhow::format_err!("Failed to load TVC from bytes: {e}"))?;
    if let Some(hash_code) = old_account.get_code_hash() {
        return Ok(state_init.code().is_some_and(|code_cell| hash_code == code_cell.repr_hash()));
    }
    Ok(false)
}

#[cfg(feature = "mirror_repair")]
fn mv_config_update_dapp(accounts: &mut ShardAccounts) -> anyhow::Result<()> {
    use tvm_types::UInt256;

    let mv_config = AccountAddress::from_str(MV_CONFIG_ADDRESS)
        .map_err(|e| anyhow::format_err!("Failed to calculate root_mv addr: {e}"))?;
    let acc_id: AccountId = mv_config.clone().into();
    let shard_account = match accounts
        .account(&acc_id)
        .map_err(|e| anyhow::format_err!("Failed to read account from shard state: {e}"))?
    {
        Some(account) => account,
        None => {
            tracing::error!(
                "mirror_repair: mvconfig account was not found in state: {mv_config:?}"
            );
            return Err(anyhow::format_err!("mvconfig account not found"));
        }
    };
    let old_account = shard_account
        .read_account()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?
        .as_struct()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?;
    let new_account_cell = old_account.serialize().map_err(|e| {
        tracing::error!("mirror_repair: Failed to serialize new account: {e}");
        anyhow::format_err!("Failed to serialize new account")
    })?;
    let dapp_id = UInt256::from_str(GAME_ROOT_DAPP_ID)
        .map_err(|e| anyhow::format_err!("Failed to convert DApp ID: {e}"))?;
    let new_shard_account = ShardAccount::with_account_root(
        new_account_cell,
        shard_account.last_trans_hash().clone(),
        shard_account.last_trans_lt(),
        Some(dapp_id.clone()),
    );
    if let Err(e) = accounts.insert(&mv_config.0, &new_shard_account) {
        tracing::error!("mirror_repair: Failed to insert new shard account: {e}");
    }
    tracing::error!("mirror_repair: Success: {mv_config:?}");
    Ok(())
}

#[cfg(feature = "mirror_repair")]
fn repair_mirror_accounts(accounts: &mut ShardAccounts) -> anyhow::Result<()> {
    use tvm_block::CurrencyCollection;
    use tvm_block::Deserializable;
    use tvm_block::MsgAddressInt;
    use tvm_block::StateInit;
    use tvm_client::abi::Abi;
    use tvm_client::abi::AbiContract;
    use tvm_client::encoding::slice_from_cell;
    use tvm_types::UInt256;

    let mirror_stateinit = StateInit::construct_from_bytes(MIRROR_TVC)
        .map_err(|e| anyhow::format_err!("Failed to construct mirror tvc: {e}"))?;
    let root_mv = AccountAddress::from_str(ROOT_MV_ADDRESS)
        .map_err(|e| anyhow::format_err!("Failed to calculate root_mv addr: {e}"))?;
    let acc_id: AccountId = root_mv.clone().into();
    let shard_account = accounts
        .account(&acc_id)
        .map_err(|e| anyhow::format_err!("Failed to read account from shard state: {e}"))?
        .ok_or_else(|| anyhow::format_err!("Account not found in shard state"))?;
    let old_account = shard_account
        .read_account()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?
        .as_struct()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?;
    let first_data: HashMap<String, TokenValue> = decode_root_mv_data(&old_account)
        .map_err(|e| anyhow::format_err!("Failed to get root_mv data: {e}"))?;
    let mut new_data: HashMap<String, TokenValue> = HashMap::new();
    let pubkey_value = first_data
        .get("_pubkey")
        .ok_or_else(|| anyhow::format_err!("Key '_pubkey' not found in decoded data"))?;
    new_data.insert("_rootPubkey".to_string(), pubkey_value.clone());
    let balance = CurrencyCollection::with_grams(1_000_000_000_000);
    let last_paid = 0;
    let address = MsgAddressInt::from_str(GAME_ROOT_ADDR)
        .map_err(|e| anyhow::format_err!("Failed to convert game root address: {e}"))?;
    let mut account = Account::active_by_init_code_hash(
        address.clone(),
        balance,
        last_paid,
        mirror_stateinit,
        true,
    )
    .map_err(|e| anyhow::format_err!("Failed to create account: {e}"))?;

    let abi = serde_json::from_str::<AbiContract>(MIRROR_ABI)
        .map_err(|e| anyhow::format_err!("Failed to decode abi: {e}"))?;
    let abi = Abi::Contract(abi);
    let abi_contract =
        abi.abi().map_err(|e| anyhow::anyhow!("Failed to convert abi to contract: {e}"))?;
    let data = account.get_data().ok_or(anyhow::anyhow!("Account has no data"))?;
    let mut tokens = abi_contract
        .decode_storage_fields(
            slice_from_cell(data)
                .map_err(|e| anyhow::anyhow!("Failed to convert data cell to slice: {e}"))?,
            true,
        )
        .map_err(|e| anyhow::anyhow!("Failed to decode storage field: {e}"))?;
    for token in &mut tokens {
        if let Some(new_value) = new_data.get(&token.name) {
            token.value = new_value.clone();
        }
    }
    let data_cell =
        TokenValue::pack_values_into_chain(&tokens, vec![], abi.abi().unwrap().version())
            .map_err(|e| anyhow::anyhow!("Failed to encode storage fields: {e}"))?
            .into_cell()
            .map_err(|e| anyhow::anyhow!("Failed to convert cell: {e}"))?;
    account.set_data(data_cell);
    let dapp_id = UInt256::from_str(GAME_ROOT_DAPP_ID)
        .map_err(|e| anyhow::format_err!("Failed to convert DApp ID: {e}"))?;

    for i in 1..=1000 {
        let hex_part = format!("2{i:063x}");
        let address = format!("0:{hex_part}");
        let mirror_addr = MsgAddressInt::from_str(&address)
            .map_err(|e| anyhow::format_err!("Failed to calculate mirror addr: {e}"))?;
        let mut new_account = account.clone();
        new_account.set_addr(mirror_addr.clone());
        let new_account_cell = new_account
            .serialize()
            .map_err(|e| anyhow::anyhow!("Failed to serialize account: {e}"))?;
        let new_shard_account = ShardAccount::with_account_root(
            new_account_cell,
            UInt256::new(),
            0,
            Some(dapp_id.clone()),
        );
        let address: UInt256 = mirror_addr
            .address()
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to prepare address: {e}"))?;
        if let Err(e) = accounts.insert(&address, &new_shard_account) {
            tracing::error!("mirror_repair: Failed to insert new shard account: {e}");
            continue;
        }
        tracing::trace!("mirror_repair: Success: {mirror_addr:?}");
    }
    Ok(())
}

#[cfg(feature = "mirror_repair")]
fn init_mv_game_root(accounts: &mut ShardAccounts) -> anyhow::Result<()> {
    use std::str::FromStr;

    use tvm_abi::TokenValue;
    use tvm_block::Account;
    use tvm_block::CurrencyCollection;
    use tvm_block::Deserializable;
    use tvm_block::MsgAddressInt;
    use tvm_block::Serializable;
    use tvm_block::ShardAccount;
    use tvm_block::StateInit;
    use tvm_client::abi::Abi;
    use tvm_client::abi::AbiContract;
    use tvm_client::encoding::slice_from_cell;
    use tvm_types::UInt256;

    tracing::trace!("init_mv_game_root: start");
    let root_mv = AccountAddress::from_str(ROOT_MV_ADDRESS)
        .map_err(|e| anyhow::format_err!("Failed to calculate root_mv addr: {e}"))?;
    let acc_id: AccountId = root_mv.clone().into();
    let shard_account = accounts
        .account(&acc_id)
        .map_err(|e| anyhow::format_err!("Failed to read account from shard state: {e}"))?
        .ok_or_else(|| anyhow::format_err!("Account not found in shard state"))?;
    let old_account = shard_account
        .read_account()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?
        .as_struct()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?;
    let new_data: HashMap<String, TokenValue> = decode_root_mv_data(&old_account)
        .map_err(|e| anyhow::format_err!("Failed to get root_mv data: {e}"))?;
    tracing::trace!("init_mv_game_root: data: {new_data:?}");

    let address = MsgAddressInt::from_str(GAME_ROOT_ADDR)
        .map_err(|e| anyhow::format_err!("Failed to convert game root address: {e}"))?;
    // TODO: check if any ecc or last_paid is needed
    let balance = CurrencyCollection::with_grams(1_000_000_000_000);
    let last_paid = 0;

    let state_init = StateInit::construct_from_bytes(GAME_ROOT_TVC)
        .map_err(|e| anyhow::format_err!("Failed to load TVC from bytes: {e}"))?;

    let mut account =
        Account::active_by_init_code_hash(address.clone(), balance, last_paid, state_init, true)
            .map_err(|e| anyhow::format_err!("Failed to create account: {e}"))?;

    let abi = serde_json::from_str::<AbiContract>(GAME_ROOT_ABI)
        .map_err(|e| anyhow::format_err!("Failed to decode abi: {e}"))?;
    let abi = Abi::Contract(abi);
    let abi_contract =
        abi.abi().map_err(|e| anyhow::anyhow!("Failed to convert abi to contract: {e}"))?;
    let data = account.get_data().ok_or(anyhow::anyhow!("Account has no data"))?;
    let mut tokens = abi_contract
        .decode_storage_fields(
            slice_from_cell(data)
                .map_err(|e| anyhow::anyhow!("Failed to convert data cell to slice: {e}"))?,
            true,
        )
        .map_err(|e| anyhow::anyhow!("Failed to decode storage field: {e}"))?;
    for token in &mut tokens {
        if let Some(new_value) = new_data.get(&token.name) {
            token.value = new_value.clone();
        }
    }
    let data_cell =
        TokenValue::pack_values_into_chain(&tokens, vec![], abi.abi().unwrap().version())
            .map_err(|e| anyhow::anyhow!("Failed to encode storage fields: {e}"))?
            .into_cell()
            .map_err(|e| anyhow::anyhow!("Failed to convert cell: {e}"))?;
    account.set_data(data_cell);

    let new_account_cell =
        account.serialize().map_err(|e| anyhow::anyhow!("Failed to serialize account: {e}"))?;
    let dapp_id = UInt256::from_str(GAME_ROOT_DAPP_ID)
        .map_err(|e| anyhow::format_err!("Failed to convert DApp ID: {e}"))?;
    let new_shard_account =
        ShardAccount::with_account_root(new_account_cell, UInt256::new(), 0, Some(dapp_id));
    let address: UInt256 = address
        .address()
        .try_into()
        .map_err(|e| anyhow::anyhow!("Failed to prepare address: {e}"))?;
    accounts
        .insert(&address, &new_shard_account)
        .map_err(|e| anyhow::format_err!("Failed to insert shard account: {e}"))?;

    tracing::trace!("init_mv_game_root: success");
    Ok(())
}

#[cfg(feature = "mirror_repair")]
fn get_root_mv_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(ROOT_MV_ABI.to_string())
}

#[cfg(feature = "mirror_repair")]
pub fn decode_root_mv_data(account: &Account) -> anyhow::Result<HashMap<String, TokenValue>> {
    let abi = get_root_mv_abi();
    let mut result_data: HashMap<String, TokenValue> = HashMap::new();
    if let Some(data) = account.get_data() {
        use tvm_client::encoding::slice_from_cell;
        let decoded_data = abi
            .abi()
            .map_err(|e| anyhow::format_err!("Failed to get DAPP config abi: {e}"))?
            .decode_storage_fields(
                slice_from_cell(data)
                    .map_err(|e| anyhow::format_err!("Failed to convert cell to slice: {e}"))?,
                true,
            )
            .map_err(|e| anyhow::format_err!("Failed to decode DAPP config storage: {e}"))?;
        for token in decoded_data {
            if token.name == "_reward_sum" {
                result_data.insert("_reward_sum".to_string(), token.value.clone());
            }
            if token.name == "_reward_adjustment" {
                result_data.insert("_reward_adjustment".to_string(), token.value.clone());
            }
            if token.name == "_reward_last_time" {
                result_data.insert("_reward_last_time".to_string(), token.value.clone());
            }
            if token.name == "_min_reward_period" {
                result_data.insert("_min_reward_period".to_string(), token.value.clone());
            }
            if token.name == "_reward_period" {
                result_data.insert("_reward_period".to_string(), token.value.clone());
            }
            if token.name == "_calc_reward_num" {
                result_data.insert("_calc_reward_num".to_string(), token.value.clone());
            }
            if token.name == "_networkStart" {
                result_data.insert("_networkStart".to_string(), token.value.clone());
            }
            if token.name == "_pubkey" {
                result_data.insert("_pubkey".to_string(), token.value.clone());
            }
        }
        tracing::trace!(target: "builder", "MV root result {:?}", result_data);
    }
    Ok(result_data)
}

#[cfg(feature = "mirror_repair")]
fn change_mv_root(accounts: &mut ShardAccounts) -> anyhow::Result<()> {
    use tvm_block::StateInit;

    let mv_root_stateinit = StateInit::construct_from_bytes(MVROOT_TVC)
        .map_err(|e| anyhow::format_err!("Failed to construct mirror tvc: {e}"))?;

    let mv_root_code = mv_root_stateinit
        .code()
        .ok_or_else(|| anyhow::format_err!("Mirror TVC doesn't contain code"))?
        .clone();
    let root_mv = AccountAddress::from_str(ROOT_MV_ADDRESS)
        .map_err(|e| anyhow::format_err!("Failed to calculate root_mv addr: {e}"))?;
    let acc_id: AccountId = root_mv.clone().into();
    let shard_account = match accounts
        .account(&acc_id)
        .map_err(|e| anyhow::format_err!("Failed to read account from shard state: {e}"))?
    {
        Some(account) => account,
        None => {
            tracing::error!("mirror_repair: mvroot account was not found in state: {root_mv:?}");
            return Err(anyhow::format_err!("mvroot account not found"));
        }
    };
    let mut old_account = shard_account
        .read_account()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?
        .as_struct()
        .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?;
    if let Some(old_account_stateinit) = old_account.state_init_mut() {
        old_account_stateinit.set_code(mv_root_code.clone());
    } else {
        tracing::error!("mirror_repair: Failed to get old stateinit account");
    }
    let new_account_cell = old_account.serialize().map_err(|e| {
        tracing::error!("mirror_repair: Failed to serialize new account: {e}");
        anyhow::format_err!("Failed to serialize new account")
    })?;
    let new_shard_account = ShardAccount::with_account_root(
        new_account_cell,
        shard_account.last_trans_hash().clone(),
        shard_account.last_trans_lt(),
        shard_account.get_dapp_id().cloned(),
    );
    if let Err(e) = accounts.insert(&root_mv.0, &new_shard_account) {
        tracing::error!("mirror_repair: Failed to insert new shard account: {e}");
    }
    tracing::error!("mirror_repair: Success: {root_mv:?}");
    Ok(())
}
