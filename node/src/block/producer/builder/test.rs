#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use account_state::ThreadAccountsRepository;
    use chrono::Utc;
    use http_server::NotQueuedExtMessage;
    use indexset::BTreeMap;
    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;
    use node_types::ThreadIdentifier;
    use serde_json::json;
    use telemetry_utils::mpsc::instrumented_channel;
    use telemetry_utils::now_ms;
    use tvm_block::GetRepresentationHash;
    use tvm_block::HashmapAugType;
    use tvm_block::MsgAddrStd;
    use tvm_block::MsgAddressExt;
    use tvm_block::MsgAddressInt;
    use tvm_block::Serializable;
    use tvm_sdk::FunctionCallSet;
    use tvm_types::AccountId;
    use tvm_types::UInt256;

    // use tracing_subscriber::EnvFilter;
    use crate::block::producer::builder::build_actions::EXTRA_EXTERNAL_MSG;
    use crate::block::producer::builder::BlockBuilder;
    use crate::block::producer::execution_time::ExecutionTimeLimits;
    use crate::block::producer::wasm::WasmNodeCache;
    use crate::config::load_blockchain_config;
    use crate::config::DEFAULT_BLOCKCAHIN_CONFIG_HASH;
    use crate::external_messages::ExtMessageDst;
    use crate::external_messages::ExternalMessagesThreadState;
    use crate::external_messages::QueuedExtMessage;
    use crate::external_messages::Stamp;
    use crate::helper::metrics::BlockProductionMetrics;
    use crate::storage::MessageDurableStorage;
    use crate::zerostate::ZeroState;

    pub static TEST_CONTRACT_ABI: &str =
        include_str!("../../../../../contracts/test_contracts/contract.abi.json");
    const TEST_DAPP_ID: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    fn make_test_ext_message(
        account_suffix: u8,
        iterations: u64,
    ) -> anyhow::Result<(ExtMessageDst, (Stamp, QueuedExtMessage))> {
        let account = AccountId::from_string(&format!(
            "000000000000000000000000000000000000000000000000000000000000000{}",
            account_suffix
        ))
        .unwrap();
        let account_id = account.to_hex_string();
        let message = tvm_sdk::Contract::construct_call_ext_in_message_json(
            MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, 0, account)),
            MsgAddressExt::AddrNone,
            &FunctionCallSet {
                func: "test_gas".to_string(),
                header: None,
                input: json!({"iterations": iterations}).to_string(),
                abi: TEST_CONTRACT_ABI.to_string(),
            },
            None,
        )?
        .message;
        let message_cell = message
            .serialize()
            .map_err(|e| anyhow::format_err!("Failed to serialize message: {e}"))?;
        let message_bytes = tvm_types::write_boc(&message_cell)
            .map_err(|e| anyhow::format_err!("Failed to write boc: {e}"))?;
        let message_base64 = tvm_types::base64_encode(&message_bytes);
        let ext_message = NotQueuedExtMessage::try_new(
            "cafe911",
            &message_base64,
            None,
            None,
            TEST_DAPP_ID.to_string(),
            account_id,
        )
        .map_err(|e| anyhow::format_err!("Failed to create ext message: {e}"))?;
        let queued_ext_message = QueuedExtMessage::try_from_incoming(ext_message)?;
        let now = Utc::now();
        let dst =
            ExtMessageDst::from_message(&message, Some(DAppIdentifier::from_str(TEST_DAPP_ID)?))?;
        Ok((dst, (Stamp { index: 1, timestamp: now }, queued_ext_message)))
    }

    #[test]
    #[ignore]
    fn test_block_production_and_verify() -> anyhow::Result<()> {
        // tracing_subscriber::fmt()
        //     .with_env_filter(
        //         EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace")),
        //     )
        //     .with_ansi(false)
        //     .init();
        let zerostate = ZeroState::load_from_file("./tests/test_verification/zerostate")?;
        let opt_state = zerostate.state(&ThreadIdentifier::default())?.clone();
        let ext_queue: HashMap<ExtMessageDst, VecDeque<(Stamp, QueuedExtMessage)>> = HashMap::new();
        let bc_config = load_blockchain_config()?.get(&DEFAULT_BLOCKCAHIN_CONFIG_HASH)?;
        let now = now_ms();
        let mut ext_messages = vec![];
        for i in 0..5 {
            let iterations = if i == 4 { 1 } else { 1200 };
            let account = AccountId::from_string(&format!(
                "000000000000000000000000000000000000000000000000000000000000000{}",
                i + 1
            ))
            .unwrap();
            let account_id = account.to_hex_string();
            let message = tvm_sdk::Contract::construct_call_ext_in_message_json(
                MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, 0, account)),
                MsgAddressExt::AddrNone,
                &FunctionCallSet {
                    func: "test_gas".to_string(),
                    header: None,
                    input: json!({"iterations": iterations}).to_string(),
                    abi: TEST_CONTRACT_ABI.to_string(),
                },
                None,
            )
            .unwrap()
            .message;

            let message_cell = message
                .serialize()
                .map_err(|e| anyhow::format_err!("Failed to serialize message: {e}"))?;
            let message_bytes = tvm_types::write_boc(&message_cell)
                .map_err(|e| anyhow::format_err!("Failed to write boc: {e}"))?;
            let message_base64 = tvm_types::base64_encode(&message_bytes);

            let ext_message = NotQueuedExtMessage::try_new(
                "cafe911",
                &message_base64,
                None,
                None,
                TEST_DAPP_ID.to_string(),
                account_id,
            )
            .unwrap();

            let queued_ext_message = QueuedExtMessage::try_from_incoming(ext_message).unwrap();
            let mut guard = EXTRA_EXTERNAL_MSG.lock();
            let now = Utc::now();
            let dst = ExtMessageDst::from_message(
                &message,
                Some(DAppIdentifier::from_str(TEST_DAPP_ID).unwrap()),
            )
            .unwrap();
            guard.push((dst, (Stamp { index: i, timestamp: now }, queued_ext_message.clone())));
            ext_messages
                .push((dst, (Stamp { index: i, timestamp: now }, queued_ext_message.clone())));
        }
        let seed = Some(UInt256::rand());
        let bp_builder = BlockBuilder::with_params(
            ThreadIdentifier::default(),
            0,
            opt_state.clone(),
            now,
            1_000_000,
            seed.clone(),
            None,
            ThreadAccountsRepository::builder(tempfile::tempdir()?.path().join("durable"))
                .build()?,
            String::new(),
            String::new(),
            1,
            HashMap::new(),
            BTreeSet::new(),
            None,
            WasmNodeCache::new()?,
            false,
            None,
        )?;

        let (block, _, _) = bp_builder.build_block(
            ext_queue.clone(),
            &bc_config,
            vec![],
            None,
            HashSet::new(),
            MessageDurableStorage::mem(),
            &ExecutionTimeLimits::NO_LIMITS,
        )?;

        let mut check_messages_map: HashMap<AccountIdentifier, BTreeMap<u64, UInt256>> =
            HashMap::new();
        assert!(block
            .block
            .read_extra()
            .unwrap_or_default()
            .read_in_msg_descr()
            .unwrap_or_default()
            .iterate_objects(|in_msg| {
                let Some(trans) = in_msg.read_transaction()? else {
                    tracing::trace!("InMsg does not contain transaction");
                    return Ok(false);
                };
                let in_msg = in_msg.read_message()?;
                let msg_hash = in_msg.hash().unwrap();
                let Some(dest_account_address) = in_msg.int_dst_account_id().map(|x| x.into())
                else {
                    tracing::trace!("InMsg does not contain internal destination");
                    return Ok(false);
                };

                if check_messages_map
                    .entry(dest_account_address)
                    .or_default()
                    .insert(trans.logical_time(), msg_hash.clone())
                    .is_some()
                {
                    tvm_types::fail!("Incoming block has non unique messages");
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to parse incoming block messages: {e}"))?);

        let verifier_builder = BlockBuilder::with_params(
            ThreadIdentifier::default(),
            0,
            opt_state.clone(),
            now,
            10_000_000,
            seed,
            None,
            ThreadAccountsRepository::builder(tempfile::tempdir()?.path().join("durable"))
                .build()?,
            String::new(),
            String::new(),
            1,
            HashMap::new(),
            BTreeSet::new(),
            None,
            WasmNodeCache::new()?,
            true,
            None,
        )?;

        let data = ext_messages.remove(4);
        ext_messages.insert(0, data);
        for (addr, data) in ext_messages {
            let mut guard = EXTRA_EXTERNAL_MSG.lock();
            guard.push((addr, data));
        }

        let (verify_block, _, _) = verifier_builder.build_block(
            ext_queue,
            &bc_config,
            vec![],
            Some(check_messages_map),
            HashSet::new(),
            MessageDurableStorage::mem(),
            &ExecutionTimeLimits::NO_LIMITS,
        )?;
        assert_eq!(verify_block.block, block.block);

        Ok(())
    }

    #[test]
    fn test_started_external_message_not_marked_processed_on_stop() -> anyhow::Result<()> {
        let zerostate = ZeroState::load_from_file("./tests/test_verification/zerostate")?;
        let opt_state = zerostate.state(&ThreadIdentifier::default())?.clone();
        let bc_config = load_blockchain_config()?.get(&DEFAULT_BLOCKCAHIN_CONFIG_HASH)?;
        let now = now_ms();

        let (stop_tx, stop_rx) =
            instrumented_channel::<()>(None::<BlockProductionMetrics>, "test-builder-stop");
        thread::Builder::new().name("test".to_string()).spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let _ = stop_tx.send(());
        })?;

        let mut ext_queue: HashMap<ExtMessageDst, VecDeque<(Stamp, QueuedExtMessage)>> =
            HashMap::new();
        let (dst, entry) = make_test_ext_message(1, 5_000_000)?;
        let enqueued_stamp = entry.0.clone();
        ext_queue.entry(dst).or_default().push_back(entry);

        let builder = BlockBuilder::with_params(
            ThreadIdentifier::default(),
            0,
            opt_state,
            now,
            10_000_000,
            Some(UInt256::rand()),
            Some(stop_rx),
            ThreadAccountsRepository::builder(tempfile::tempdir()?.path().join("durable"))
                .build()?,
            String::new(),
            String::new(),
            1,
            HashMap::new(),
            BTreeSet::new(),
            None,
            WasmNodeCache::new()?,
            false,
            None,
        )?;

        let (_block, processed_stamps, _feedbacks) = builder.build_block(
            ext_queue,
            &bc_config,
            vec![],
            None,
            HashSet::new(),
            MessageDurableStorage::mem(),
            &ExecutionTimeLimits::NO_LIMITS,
        )?;

        assert!(
            !processed_stamps.contains(&enqueued_stamp),
            "in-flight message stamp must not be marked processed on stop/cutoff"
        );
        Ok(())
    }

    #[test]
    fn test_unfinished_external_stamp_is_not_erased_from_thread_state() -> anyhow::Result<()> {
        let (feedback_tx, _feedback_rx) =
            instrumented_channel(None::<BlockProductionMetrics>, "test-ext-feedback");
        let thread_state = ExternalMessagesThreadState::builder()
            .with_report_metrics(None)
            .with_thread_id(ThreadIdentifier::default())
            .with_cache_size(10)
            .with_feedback_sender(feedback_tx)
            .with_is_producing(Arc::new(AtomicBool::new(true)))
            .build()?;

        let (_dst, (_stamp, msg)) = make_test_ext_message(2, 10)?;
        thread_state.push_external_messages(&[msg])?;
        let before = thread_state.get_remaining_external_messages();
        assert_eq!(before.values().map(std::collections::VecDeque::len).sum::<usize>(), 1);

        // Simulate the producer getting no terminally processed stamps for an in-flight stop.
        let processed_stamps: Vec<Stamp> = vec![];
        thread_state.erase_processed(&processed_stamps);
        let after = thread_state.get_remaining_external_messages();
        assert_eq!(after.values().map(std::collections::VecDeque::len).sum::<usize>(), 1);
        Ok(())
    }
}
