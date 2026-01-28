#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::collections::VecDeque;

    use chrono::Utc;
    use indexset::BTreeMap;
    use serde_json::json;
    use telemetry_utils::now_ms;
    // use tracing_subscriber::EnvFilter;
    use tvm_block::GetRepresentationHash;
    use tvm_block::HashmapAugType;
    use tvm_block::Message;
    use tvm_block::MsgAddrStd;
    use tvm_block::MsgAddressExt;
    use tvm_block::MsgAddressInt;
    use tvm_sdk::FunctionCallSet;
    use tvm_types::AccountId;
    use tvm_types::UInt256;

    use crate::block::producer::builder::build_actions::EXTRA_EXTERNAL_MSG;
    use crate::block::producer::builder::BlockBuilder;
    use crate::block::producer::execution_time::ExecutionTimeLimits;
    use crate::block::producer::wasm::WasmNodeCache;
    use crate::config::load_blockchain_config;
    use crate::external_messages::Stamp;
    use crate::repository::accounts::AccountsRepository;
    use crate::storage::MessageDurableStorage;
    use crate::types::AccountAddress;
    use crate::types::BlockSeqNo;
    use crate::types::ThreadIdentifier;
    use crate::zerostate::ZeroState;

    pub static TEST_CONTRACT_ABI: &str =
        include_str!("../../../../../contracts/test_contracts/contract.abi.json");

    #[test]
    fn test_block_production_and_verify() -> anyhow::Result<()> {
        // tracing_subscriber::fmt()
        //     .with_env_filter(
        //         EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace")),
        //     )
        //     .with_ansi(false)
        //     .init();
        let zerostate = ZeroState::load_from_file("./tests/test_verification/zerostate")?;
        let opt_state = zerostate.state(&ThreadIdentifier::default())?.clone();
        let ext_queue: HashMap<AccountAddress, VecDeque<(Stamp, Message)>> = HashMap::new();
        let bc_config = load_blockchain_config()?.get(&BlockSeqNo::from(0));
        let now = now_ms();
        let mut ext_messages = vec![];
        for i in 0..5 {
            let iterations = if i == 4 { 1 } else { 1200 };
            let account = AccountId::from_string(&format!(
                "000000000000000000000000000000000000000000000000000000000000000{}",
                i + 1
            ))
            .unwrap();
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
            let mut guard = EXTRA_EXTERNAL_MSG.lock();
            let now = Utc::now();
            guard.push((
                message.dst().unwrap().address().into(),
                (Stamp { index: i, timestamp: now }, message.clone()),
            ));
            ext_messages.push((
                message.dst().unwrap().address().into(),
                (Stamp { index: i, timestamp: now }, message.clone()),
            ));
        }
        let seed = Some(UInt256::rand());
        let bp_builder = BlockBuilder::with_params(
            ThreadIdentifier::default(),
            opt_state.clone(),
            now,
            1_000_000,
            seed.clone(),
            None,
            AccountsRepository::new(tempfile::tempdir()?.keep(), None, 1),
            String::new(),
            String::new(),
            1,
            HashMap::new(),
            None,
            WasmNodeCache::new()?,
            false,
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

        let mut check_messages_map: HashMap<AccountAddress, BTreeMap<u64, UInt256>> =
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
            opt_state.clone(),
            now,
            10_000_000,
            seed,
            None,
            AccountsRepository::new(tempfile::tempdir()?.keep(), None, 1),
            String::new(),
            String::new(),
            1,
            HashMap::new(),
            None,
            WasmNodeCache::new()?,
            true,
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
}
