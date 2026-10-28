// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use account_state::ThreadAccount;
use account_state::ThreadAccountsRepository;
use anyhow::Context;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_types::UInt256;

use super::BlockBuilder;
use super::PreparedBlock;
use crate::block::producer::execution_time::ExecutionTimeLimits;
use crate::block::producer::wasm::WasmNodeCache;
use crate::config::load_blockchain_config;
use crate::config::DEFAULT_BLOCKCAHIN_CONFIG_HASH;
use crate::message::WrappedMessage;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::storage::MessageDurableStorage;
use crate::types::thread_message_queue::account_messages_iterator::legacy_test_support::with_indexed_walk;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountInbox;

type ReplayMessages = HashMap<AccountIdentifier, indexset::BTreeMap<u64, UInt256>>;

// Cursors are scheduling hints, not part of the committed inbox root. A validator can
// therefore start at a different DApp/account after restoring the same parent state.
// Exercise the actual builder and TVM, rather than only comparing iterator output:
// replay must reproduce transaction LTs, account updates and the complete block.
#[test]
fn inbox_cursor_models_preserve_validator_replay() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let repo = ThreadAccountsRepository::builder(directory.path().join("accounts")).build()?;
    let mut parent = OptimisticStateImpl::zero();
    let mut accounts = repo.state_builder(&parent.thread_id, 0, &parent.shard_state.0);
    let init = tvm_block::StateInit::construct_from_bytes(include_bytes!(
        "../../../../../contracts/test_contracts/contract.tvc"
    ))
    .map_err(anyhow::Error::msg)?;
    for dapp in [1u8, 127, 255] {
        for account in 1..=3 {
            let mut id = [account; 32];
            id[0] = dapp;
            let id = AccountIdentifier::new(id);
            let routing = id.routing(DAppIdentifier::new([dapp; 32]));
            let address = tvm_block::MsgAddressInt::with_standart(None, 0, id.into())
                .map_err(anyhow::Error::msg)?;
            let storage = tvm_block::AccountStorage::active_by_init_code_hash(
                0,
                tvm_block::CurrencyCollection::from_grams(1_000_000_000_000u64.into()),
                init.clone(),
                true,
            );
            let mut body = tvm_block::Account::with_storage(
                &address,
                &tvm_block::StorageInfo::with_values(1_700_000_000, None),
                &storage,
            );
            body.update_storage_stat().map_err(anyhow::Error::msg)?;
            let body = ThreadAccount::new(
                body.try_into()?,
                Default::default(),
                0,
                Some(*routing.dapp_id()),
            )?;
            accounts.insert_account(&routing, &body);
            // Both priorities have multiple messages for each account. Nonces make
            // swapping two messages observable even when their destinations match.
            for (priority, queue) in
                [(0, &mut parent.high_priority_messages), (1, &mut parent.messages)]
            {
                let mut inbox = AccountInbox::empty();
                for nonce in 0..3 {
                    let call = tvm_abi::encode_function_call(
                        include_str!("../../../../../contracts/test_contracts/contract.abi.json"),
                        "single_inc_with_nonce",
                        None,
                        &format!(r#"{{"nonce":{}}}"#, priority * 100 + nonce),
                        true,
                        None,
                        None,
                    )
                    .map_err(anyhow::Error::msg)?;
                    let mut header = tvm_block::InternalMessageHeader::with_addresses(
                        address.clone(),
                        address.clone(),
                        tvm_block::CurrencyCollection::from_grams(1_000_000_000u64.into()),
                    );
                    header.created_lt = 10 + priority * 100 + nonce;
                    header.dest_dapp_id = Some((*routing.dapp_id()).into());
                    let mut message = tvm_block::Message::with_int_header(header);
                    message.set_body(
                        tvm_types::SliceData::load_cell(
                            call.into_cell().map_err(anyhow::Error::msg)?,
                        )
                        .map_err(anyhow::Error::msg)?,
                    );
                    let message = Arc::new(WrappedMessage::from_tvm(&message));
                    inbox.commitment.add(&message.identifier());
                    inbox.add_messages(vec![(message.identifier(), message)])?;
                }
                queue.set_inbox(routing, inbox);
            }
        }
    }
    parent.shard_state = OptimisticShardState(accounts.build(None)?.new_state);
    let config = load_blockchain_config()?.get(&DEFAULT_BLOCKCAHIN_CONFIG_HASH)?;
    let run = |state: OptimisticStateImpl,
               parallelism,
               limit,
               checker: Option<ReplayMessages>,
               indexed| {
        with_indexed_walk(indexed, || {
            BlockBuilder::builder()
                .thread_id(ThreadIdentifier::default())
                .block_height(0)
                .initial_optimistic_state(state)
                .gen_utime_ms(1_700_000_001_000)
                .block_gas_limit(limit)
                .rand_seed(Some(UInt256::from([42; 32])))
                .control_rx_stop(None)
                .thread_accounts_repository(repo.clone())
                .block_keeper_epoch_code_hash(String::new())
                .block_keeper_preepoch_code_hash(String::new())
                .parallelization_level(parallelism)
                .produced_internal_messages_to_other_threads(HashMap::new())
                .added_high_priority_messages(HashMap::new())
                .settled_messages(HashMap::new())
                .tracked_ext_out_account_routings(BTreeSet::new())
                .metrics(None)
                .wasm_cache(WasmNodeCache::new()?)
                .is_verifier(checker.is_some())
                .check_history_proof_hash(None)
                .build()?
                .build_block(
                    inbound_external_messages::grouped_scheduler(HashMap::new()),
                    &config,
                    vec![],
                    checker,
                    HashSet::new(),
                    MessageDurableStorage::mem(),
                    &ExecutionTimeLimits::NO_LIMITS,
                )
                .map(|(block, _, _)| block)
        })
    };
    // Repeat without high-priority traffic so the gas cutoff also lands inside
    // the normal queue. With both priorities present it lands in the high queue.
    for high_priority in [false, true] {
        let mut parent = parent.clone();
        if !high_priority {
            parent.high_priority_messages = ThreadMessageQueueState::empty();
        }
        for producer_indexed in [false, true] {
            for producer_parallelism in [1, 4] {
                for limit in [1, 10_000_000] {
                    let produced =
                        run(parent.clone(), producer_parallelism, limit, None, producer_indexed)?;
                    assert!(produced.tx_cnt > 0);
                    if limit == 1 {
                        assert!(
                            produced.tx_cnt < 27,
                            "gas cutoff must leave inbox messages pending"
                        );
                    } else {
                        assert_eq!(
                            produced.tx_cnt,
                            if high_priority { 54 } else { 27 },
                            "all priorities must be drained"
                        );
                    }
                    let checker = replay_messages(&produced)?;
                    for offset in [0, 1, 2] {
                        let mut replay_parent = parent.clone();
                        for queue in
                            [&mut replay_parent.messages, &mut replay_parent.high_priority_messages]
                        {
                            let ids: Vec<_> = queue.messages.iter().map(|(id, _)| id).collect();
                            for id in ids {
                                let mut dapp = queue.messages.get(&id).unwrap().as_ref().clone();
                                dapp.advance_cursor(offset);
                                queue.messages.insert(id, Arc::new(dapp));
                            }
                            queue.advance_cursor(offset);
                        }
                        assert_eq!(parent.messages.root_hash(), replay_parent.messages.root_hash());
                        assert_eq!(
                            parent.high_priority_messages.root_hash(),
                            replay_parent.high_priority_messages.root_hash()
                        );
                        // In both upgrade directions deliberately give the other
                        // cursor model different DApp/account starting positions.
                        let verified = run(replay_parent, 4, limit, Some(checker.clone()), !producer_indexed)
                        .with_context(|| {
                            format!(
                                "indexed producer={producer_indexed}, priority={high_priority}, parallelism={producer_parallelism}, \
                                 limit={limit}, offset={offset}, transactions={}",
                                produced.tx_cnt,
                            )
                        })?;
                        assert_eq!(produced.tx_cnt, verified.tx_cnt);
                        assert_eq!(
                            produced.block, verified.block,
                            "parallelism={producer_parallelism}, limit={limit}, offset={offset}"
                        );
                        assert_eq!(
                            produced.state.messages.root_hash(),
                            verified.state.messages.root_hash()
                        );
                        assert_eq!(
                            produced.state.high_priority_messages.root_hash(),
                            verified.state.high_priority_messages.root_hash()
                        );
                    }
                    if limit > 1 {
                        let mut invalid = checker;
                        let messages = invalid.values_mut().next().unwrap();
                        let keys: Vec<_> = messages.keys().copied().take(2).collect();
                        let first = messages[&keys[0]].clone();
                        let second = messages[&keys[1]].clone();
                        messages.insert(keys[0], second);
                        messages.insert(keys[1], first);
                        let error =
                            match run(parent.clone(), 4, limit, Some(invalid), !producer_indexed) {
                                Ok(_) => panic!(
                                    "validator accepted reordered messages within an account"
                                ),
                                Err(error) => error,
                            };
                        assert!(
                            error.to_string().contains("Wrong int messages order"),
                            "{error:#}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

// Match SingleBlockVerifier: only the messages actually included in the proposed
// block are replayed, in transaction LT order within each destination account.
fn replay_messages(block: &PreparedBlock) -> anyhow::Result<ReplayMessages> {
    let mut checker = ReplayMessages::new();
    let messages = block
        .block
        .read_extra()
        .map_err(anyhow::Error::msg)?
        .read_in_msg_descr()
        .map_err(anyhow::Error::msg)?;
    messages
        .iterate_objects(|entry| {
            let transaction = entry.read_transaction()?.expect("inbox message has transaction");
            let description = transaction.read_description()?;
            assert!(!description.is_aborted(), "fixture transaction aborted: {description:?}");
            assert!(
                matches!(description.compute_phase_ref(), Some(tvm_block::TrComputePhase::Vm(vm)) if vm.success),
                "fixture must execute TVM successfully: {description:?}"
            );
            let message = entry.read_message()?;
            checker
                .entry(message.int_dst_account_id().unwrap().into())
                .or_default()
                .insert(transaction.logical_time(), message.hash()?);
            Ok(true)
        })
        .map_err(anyhow::Error::msg)?;
    Ok(checker)
}
