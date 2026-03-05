use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use account_state::ThreadAccountsBuilder;
use account_state::ThreadAccountsRepository;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use tracing::instrument;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::accounts::AccountsRepository;
use crate::repository::accounts::NodeThreadAccountsRef;
use crate::repository::accounts::NodeThreadAccountsRepository;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountInbox;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::ThreadsTable;

#[cfg(feature = "usdc_name_repair")]
static ROOTTOKEN_ABI: &str =
    include_str!("../../../contracts/0.79.3_compiled/token/RootToken.abi.json");

#[cfg(feature = "usdc_name_repair")]
static USDC_TOKEN_ADDRESS: &str =
    "0:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

#[cfg(feature = "usdc_name_repair")]
static USDC_TOKEN_DAPP_ID: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[cfg(feature = "usdc_name_repair")]
static USDC_MV_DAPP_ID: &str = "0000000000000000000000000000000000000000000000000000000000000001";

#[cfg(feature = "usdc_name_repair")]
static USDC_DAPPID_REPAIR_ADDRESSES: &[&str] = &[
    // USDC Root
    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
    // Accumulator contract
    "3535353535353535353535353535353535353535353535353535353535353535",
    // TokenWallet addresses
    "009aec75a8e44a4c743bb921952a4e6cdd40be369f53a1a1094e46e9f0307f9d",
    "05590383bc34cad9c8549739bdc3fc9321659ded6eb32e884550874ade104802",
    "06faeffbb83b3adb4d8aa03dd22a552419fc14cfd94b7a03eba9a3daf12e0b78",
    "0c9e57b9f04e8625440a08b933d3d1a9df77952ff1be8eb9643a32dd22e01be3",
    "0da49906d84d8ba95820b2d698d454a2f473d655a97e03a4b313e72cd245dd29",
    "0dc4f59d34ffd1ab8cb6295b4dd8f349e4b3cc888144d1c7cdc661053d3131a6",
    "19c472a50a18b53e780708971947c9c36b7033feb2949cfb4552890a3193dd3b",
    "1cab87e99420b28c6e1a8a8e5686f547687351fba5f2571276e472def736b43f",
    "24c7a1c9c645a407ca574aad78d22abf4d14d041091f6b5ebf126bc9d93d6dda",
    "2638364733b0f4ef3f25a3dd0f1525894da5caed5423f126bde74faa5fdef5a6",
    "2de42c44125743e281ce24d73e842326a354b7732f564af2358546685df3f076",
    "2e33c0d95c39bdcb5c433da0d448d81bbdfacffd2bb93bb31e0bdd06c446a6ea",
    "305b3acf392ebc199b3ccf50e4b48ebd533fb2975921a13c2e6c64f6c4ca27de",
    "3480e30b09cd2804ee6fe7fa038c74163b0330bee5168249cd45876e0165ccfa",
    "35b731e5285891b6d58466ded8d9de2e1da65c3fc0bb75cfe4fd54be4c8b6325",
    "40ec962b066f4589c9e22e7a0424c779565e141a166a7d05b7e1da987be85476",
    "5329b3715c464e7dc620f71593431f45632b27a81a5b7a237e0fe9452cc54898",
    "5d1adf93f3e9e3c35d70ba8ebd54f0ee36274b354b9ec65779e60a8ca77b50fe",
    "5dc642ef94a0986af4ca75ab25f97f3c7fb64c71da72364f63051ce50df394b7",
    "618568d476b32ce50aab4267597681214e612576b97430f95a7ac187ee29ce38",
    "69f89f8ae34431ad2a95bb4e388c16fad69ddc9bc30bb06456add059ab0d6512",
    "6e47e2a9a9ee9df5cf4769025ab119e0686b2a26aca9773d834596180a0eac7f",
    "6f1c9ca591da1b1c42b3fbbb86aec8934503b180b0473c6b925ebdaf8bac6105",
    "70411f62fbfa873144203dd042e05b086a941c3d603b9e1455e2ce06c53f90a2",
    "811a0b9a19ea29aef9cc1bf7360bd2dcd01a9f5bd96d0679625c31ff71cab669",
    "827ea5510dae807f35833a1aefd1e224bdee22ce2f15e8ba409643534dbb0149",
    "8af33a0e3ea8ea34589d42b9df49b8b0ea6f26cab3a859863d82921662865d5c",
    "8e5d252ff3d894adbc359c7a6a818754360285b727bbd2452d2520d2e2eadd4a",
    "8f26aadd8335e18e180a26989b39c6c22afd8551f7c3f96d9e3d20f45c0f77dc",
    "99313d23ff7c69c67690eb24e6cd3f658430223203355db1e904c449e67f8a85",
    "9c1138e5e3e451f1d6f7c9b361537f21f596cc818221ace8817dc03a008127ce",
    "9e3d17e0bbee46d3d7c6545189a19cedb614b30b7b03f03539f52c382a54484b",
    "a4bde35a2235126901168bcd34e754d098ed0424fbef419971ad9b44f597e095",
    "a8c3afd5c5e8c177f1bde3375d8af717e0c2094282a6a8b8a027984479b187bd",
    "a911835ab9dcbfc9ac43827fec57e6f7fe6d708f1e17d38ef49b771be96b2600",
    "aa177f2bfa37f8b025ed06fc41181c1ea265a8fbda90447a2ebde330deb67b2e",
    "bcbeb04c5327fd9595a3e8fac0028735efc584df14120b2cc030d7d4eccdda88",
    "bd9d5dc72efbe1b6518d8fd5e9e1281b7a5c4130437d578b8568c1efa5ffe145",
    "c1c679f61549f539fa0a100bd6f8fd51cd40416367d66396e4573bb1eb98a991",
    "c3deb60fe06c4dc3d1b7cbf459793399a7e1be014f23c7d18eabe63f2752ad07",
    "ca6cbf652ea450968740045715872cfcc9bfeef180fa93b378812b697718fa11",
    "cb3d882e6623ce852f354f33c072b02f54873dd61a235e41346938b1dd08854b",
    "cb5e016fbee5029ce4d9efa98495fc84ea961b206553abed83472d76e539f83f",
    "cf465087059aa13ddee58c876e6f54ada4d01491386de63a54917aba7b1a5b0b",
    "d286a56c59b1eb5390aff6a050985e50dca09a834ec52feb8d7c6a0c7dd3f20a",
    "d296934182c7e0470b7369feabde554e825e0f1317dcf046396ebf2f54b809ae",
    "d46c13a01da53c038e990d9dc455d45cfcc06f2711d96b8153bf3abc292a456e",
    "d7fddaff60539adaea268b879a3e4505f2e14c4e6d0b16628398fe3dc28ca11f",
    "e1467ef1871fddcf55269c6c6dc6ff36b9b5ef54669e39e4a6bc711d325d6585",
    "e3b5e51cbb62ed853b154833ba1a3607a3208da94e185de5332a036e9ce18993",
    "e5fd2e4ec0db231e494ffd9d1c52c3cd19f8f97a94c59786e150c5a9e09eb72b",
    "e6bb034c8f86d83fda8cc4e4af536cc4065fea41acf9fd5f966cfdff8323b4e3",
    "e879412bfe2549b64a87e11f84e3f0091f26944da5209ccd5675ddb2f3476e4f",
    "ef2f5809fb0c31ce0e4f48be4fd055ee8184928be06549a142c2aefb0cac81ec",
    "f3b8b70ad34135dea8edae042254f7098d578ecddea2eebf5045971a9cc537eb",
    "f4d20f554e7644b1c76ce02404af99494f67e8ee3cd742ca295b4eaa2eedb472",
    "f80b75cd1c8e6a5519baf7bf3aca00f943e324598754acd24e44bc68b6281d7d",
    "fff824595e71813ae93a377ee029967418eed3c6f41a4dae2e01c5ec2c01fd05",
];

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn postprocess(
    mut initial_optimistic_state: OptimisticStateImpl,
    consumed_internal_messages: HashMap<AccountIdentifier, HashSet<MessageIdentifier>>,
    mut produced_internal_messages_to_the_current_thread: HashMap<
        AccountIdentifier,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    accounts_that_changed_their_dapp_id: HashMap<AccountRouting, Option<WrappedAccount>>,
    block_id: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    mut new_state: NodeThreadAccountsRef,
    mut produced_internal_messages_to_other_threads: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    block_info: BlockInfo,
    thread_id: ThreadIdentifier,
    threads_table: ThreadsTable,
    block_accounts: HashSet<AccountIdentifier>,
    accounts_repo: AccountsRepository,
    thread_accounts_repository: &NodeThreadAccountsRepository,
    db: MessageDurableStorage,
    apply_to_durable: bool,
    #[cfg(feature = "monitor-accounts-number")] updated_accounts_number: u64,
    #[cfg(feature = "usdc_name_repair")] is_block_of_retired_version: bool,
    #[cfg(feature = "usdc_name_repair")] usdc_name_repaired: std::sync::Arc<
        parking_lot::Mutex<Option<BlockSeqNo>>,
    >,
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
    let current_thread_last_block = (current_thread_id, block_id, block_seq_no);
    new_thread_refs.update(current_thread_id, current_thread_last_block);

    let mut removed_accounts: Vec<AccountIdentifier> = vec![];
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
            removed_accounts.push(*routing.account_id());
            let account_inbox =
                initial_optimistic_state.messages.account_inbox(routing.account_id()).cloned();
            outbound_accounts.insert(*routing, (account.clone(), account_inbox));
        }
    }
    if !outbound_accounts.is_empty() {
        let mut shard_state = thread_accounts_repository.state_builder(&new_state);
        shard_state.set_apply_to_durable(apply_to_durable);
        for (account_routing, (_, _)) in &outbound_accounts {
            // let default_account_routing = AccountRouting(
            //     DAppIdentifier(account_routing.1.clone()),
            //     account_routing.1.clone(),
            // );
            // if initial_optimistic_state.does_routing_belong_to_the_state(&default_account_routing) {

            if shard_state.account(account_routing)?.is_some() {
                tracing::debug!(target: "node", "replace account with redirect: {:?}", account_routing);
                shard_state.replace_with_redirect(account_routing)?;
            }
            // }
        }
        new_state = shard_state.build(None)?.new_state;
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
        let mut shard_state = thread_accounts_repository.state_builder(&new_state);
        shard_state.set_apply_to_durable(apply_to_durable);
        let mut deleted = Vec::new();
        for (account_id, seq_no) in std::mem::take(&mut changed_accounts) {
            let account_routing = account_id.dapp_originator();
            if seq_no + unload_after <= block_seq_no {
                if let Some(mut account) = shard_state.account(&account_routing)? {
                    tracing::trace!(
                        account_id = account_id.to_hex_string(),
                        "Unloading account from state"
                    );
                    let state = account
                        .unload_account()
                        .map_err(|e| anyhow::format_err!("Failed to set account external: {e}"))?;
                    cached_accounts.insert(account_id, (block_seq_no, state));
                    shard_state.insert_account(&account_routing, &account);
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
        new_state = shard_state.build(None)?.new_state;
    }

    #[cfg(feature = "usdc_name_repair")]
    {
        let mut lock = usdc_name_repaired.lock();
        if lock.is_none() && is_usdc_dappid_repaired(thread_accounts_repository, &new_state) {
            // Set to block 0 so subsequent blocks never match and repair is skipped.
            *lock = Some(BlockSeqNo::from(0));
        }
        let should_repair = match *lock {
            None => true,
            Some(seq_no) => seq_no == block_seq_no,
        };
        if should_repair && !is_block_of_retired_version {
            match repair_usdc_name(thread_accounts_repository, new_state.clone()) {
                Ok(repaired_state) => {
                    new_state = repaired_state;
                }
                Err(e) => {
                    tracing::trace!("Failed to repair USDC name: {e}");
                }
            }
            match repair_usdc_dappid(thread_accounts_repository, new_state.clone()) {
                Ok(repaired_state) => {
                    new_state = repaired_state;
                }
                Err(e) => {
                    tracing::trace!("Failed to repair USDC dappid: {e}");
                }
            }
            if lock.is_none() {
                *lock = Some(block_seq_no);
            }
        }
    }

    let new_state_builder = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id)
        .shard_state(OptimisticShardState(new_state))
        .messages(initial_optimistic_state.messages)
        .high_priority_messages(initial_optimistic_state.high_priority_messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped)
        .changed_accounts(changed_accounts)
        .cached_accounts(cached_accounts);

    #[cfg(feature = "monitor-accounts-number")]
    let new_state_builder = new_state_builder.accounts_number(updated_accounts_number);

    let new_state = new_state_builder.build();

    let cross_thread_ref_data = CrossThreadRefData::builder()
        .block_identifier(block_id)
        .block_seq_no(block_seq_no)
        .block_thread_identifier(thread_id)
        .outbound_messages(produced_internal_messages_to_other_threads)
        .outbound_accounts(outbound_accounts)
        .threads_table(threads_table.clone())
        .parent_block_identifier(initial_optimistic_state.block_id)
        .block_refs(vec![]) // set up later
        .build();

    Ok((new_state, cross_thread_ref_data))
}

#[cfg(feature = "usdc_name_repair")]
fn repair_usdc_name(
    thread_accounts_repository: &NodeThreadAccountsRepository,
    state: NodeThreadAccountsRef,
) -> anyhow::Result<NodeThreadAccountsRef> {
    use std::str::FromStr;

    use account_state::ThreadAccount;
    use node_types::DAppIdentifier;
    use tvm_abi::TokenValue;
    use tvm_block::Serializable;
    use tvm_client::abi::Abi;
    use tvm_client::encoding::slice_from_cell;
    use tvm_types::UInt256;

    let account_id = AccountIdentifier::from(
        UInt256::from_str(&USDC_TOKEN_ADDRESS[2..]) // strip "0:" prefix
            .map_err(|e| anyhow::format_err!("Failed to parse USDC address: {e}"))?,
    );
    let dapp_id = DAppIdentifier::from(
        UInt256::from_str(USDC_TOKEN_DAPP_ID)
            .map_err(|e| anyhow::format_err!("Failed to parse USDC dapp id: {e}"))?,
    );
    let routing = AccountRouting::new(dapp_id, account_id);

    let mut builder = thread_accounts_repository.state_builder(&state);
    builder.set_apply_to_durable(true);
    let thread_state_account = builder
        .account(&routing)?
        .ok_or_else(|| anyhow::format_err!("USDC token account not found in shard state"))?;

    // Get the underlying account
    let thread_account = thread_state_account.account()?;
    let mut tvm_account = tvm_block::Account::try_from(&thread_account)?;

    // Parse ABI
    let abi = Abi::Json(ROOTTOKEN_ABI.to_string());
    let abi_contract =
        abi.abi().map_err(|e| anyhow::format_err!("Failed to parse RootToken ABI: {e}"))?;

    // Get data cell and decode storage fields
    let data = tvm_account
        .get_data()
        .ok_or_else(|| anyhow::format_err!("USDC token account has no data"))?;
    let mut tokens = abi_contract
        .decode_storage_fields(
            slice_from_cell(data)
                .map_err(|e| anyhow::format_err!("Failed to convert cell to slice: {e}"))?,
            true,
        )
        .map_err(|e| anyhow::format_err!("Failed to decode storage fields: {e}"))?;

    // Modify _name field
    for token in &mut tokens {
        if token.name == "_name" {
            token.value = TokenValue::String("USDStable".to_string());
            break;
        }
    }

    // Re-encode storage fields
    let data_cell = TokenValue::pack_values_into_chain(&tokens, vec![], abi_contract.version())
        .map_err(|e| anyhow::format_err!("Failed to pack values: {e}"))?
        .into_cell()
        .map_err(|e| anyhow::format_err!("Failed to convert to cell: {e}"))?;

    // Set data back on account
    tvm_account.set_data(data_cell);

    // Serialize account back to cell and create ThreadAccount
    let cell: tvm_types::Cell = tvm_account
        .serialize()
        .map_err(|e| anyhow::format_err!("Failed to serialize account: {e}"))?;
    let mut updated_state_account = thread_state_account.clone();
    updated_state_account.set_account(ThreadAccount::Tvm(cell))?;

    // Insert back and build
    builder.insert_account(&routing, &updated_state_account);
    let transition = builder.build(None)?;

    tracing::info!("USDC token _name changed to USDStable");
    Ok(transition.new_state)
}

#[cfg(feature = "usdc_name_repair")]
fn repair_usdc_dappid(
    thread_accounts_repository: &NodeThreadAccountsRepository,
    state: NodeThreadAccountsRef,
) -> anyhow::Result<NodeThreadAccountsRef> {
    use std::str::FromStr;

    use account_state::ThreadStateAccount;
    use node_types::DAppIdentifier;
    use tvm_types::UInt256;

    let old_dapp_id = DAppIdentifier::from(
        UInt256::from_str(USDC_TOKEN_DAPP_ID)
            .map_err(|e| anyhow::format_err!("Failed to parse old dapp id: {e}"))?,
    );
    let new_dapp_id = DAppIdentifier::from(
        UInt256::from_str(USDC_MV_DAPP_ID)
            .map_err(|e| anyhow::format_err!("Failed to parse new dapp id: {e}"))?,
    );

    let mut builder = thread_accounts_repository.state_builder(&state);
    builder.set_apply_to_durable(true);
    let mut any_moved = false;

    for addr_hex in USDC_DAPPID_REPAIR_ADDRESSES {
        let account_id = AccountIdentifier::from(
            UInt256::from_str(addr_hex)
                .map_err(|e| anyhow::format_err!("Failed to parse address {addr_hex}: {e}"))?,
        );
        let old_routing = AccountRouting::new(old_dapp_id, account_id);

        let account = match builder.account(&old_routing)? {
            Some(acc) => acc,
            None => {
                tracing::trace!("Account {addr_hex} not found at old routing, skipping");
                continue;
            }
        };

        let thread_account = account.account()?;
        let last_trans_hash = account.last_trans_hash();
        let last_trans_lt = account.last_trans_lt();

        let new_account = ThreadStateAccount::new(
            thread_account,
            last_trans_hash,
            last_trans_lt,
            Some(new_dapp_id),
        )?;

        let new_routing = AccountRouting::new(new_dapp_id, account_id);
        builder.remove_account(&old_routing);
        builder.insert_account(&new_routing, &new_account);
        any_moved = true;

        tracing::info!("Moved account {addr_hex} from dapp_id ZERO to MV_DAPP_ID");
    }

    if any_moved {
        let transition = builder.build(None)?;
        Ok(transition.new_state)
    } else {
        Ok(state)
    }
}

#[cfg(feature = "usdc_name_repair")]
fn is_usdc_dappid_repaired(
    thread_accounts_repository: &NodeThreadAccountsRepository,
    state: &NodeThreadAccountsRef,
) -> bool {
    use std::str::FromStr;

    use node_types::DAppIdentifier;
    use tvm_types::UInt256;

    let old_dapp_id = match UInt256::from_str(USDC_TOKEN_DAPP_ID) {
        Ok(v) => DAppIdentifier::from(v),
        Err(_) => return false,
    };
    let new_dapp_id = match UInt256::from_str(USDC_MV_DAPP_ID) {
        Ok(v) => DAppIdentifier::from(v),
        Err(_) => return false,
    };

    // USDC Root is the first address in the list
    let usdc_root_hex = USDC_DAPPID_REPAIR_ADDRESSES[0];
    let account_id = match UInt256::from_str(usdc_root_hex) {
        Ok(v) => AccountIdentifier::from(v),
        Err(_) => return false,
    };

    let old_routing = AccountRouting::new(old_dapp_id, account_id);
    let mut builder = thread_accounts_repository.state_builder(state);
    builder.set_apply_to_durable(true);
    if let Ok(Some(_)) = builder.account(&old_routing) {
        tracing::info!("USDC root still at old dapp_id, repair needed");
        return false;
    }

    let new_routing = AccountRouting::new(new_dapp_id, account_id);
    match builder.account(&new_routing) {
        Ok(Some(_)) => {
            tracing::info!("USDC root already at new dapp_id, marking repair as done");
            true
        }
        _ => false,
    }
}
