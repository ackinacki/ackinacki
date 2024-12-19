// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::ensure;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_executor::BlockchainConfig;
use tvm_types::HashmapType;
use typed_builder::TypedBuilder;

use crate::block::producer::builder::BlockBuilder;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::node::shared_services::SharedServices;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::types::AckiNackiBlock;

// Note: produces single verification block.
pub trait BlockVerifier {
    type OptimisticState: OptimisticState;
    type Message: Message;

    fn generate_verify_block<'a, I>(
        self,
        block: &AckiNackiBlock<GoshBLS>,
        initial_state: Self::OptimisticState,
        refs: I,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState)>
    where
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a;
}

#[derive(TypedBuilder)]
#[allow(dead_code)]
pub struct TVMBlockVerifier {
    blockchain_config: Arc<BlockchainConfig>,
    node_config: Config,
    // TODO: need to fill this data for verifier
    epoch_block_keeper_data: Vec<BlockKeeperData>,
    shared_services: SharedServices,
}

impl TVMBlockVerifier {
    fn print_block_info(block: &tvm_block::Block) {
        let extra = block.read_extra().unwrap();
        tracing::info!(target: "node",
            "block: gen time = {}, in msg count = {}, out msg count = {}, account_blocks = {}",
            block.read_info().unwrap().gen_utime(),
            extra.read_in_msg_descr().unwrap().len().unwrap(),
            extra.read_out_msg_descr().unwrap().len().unwrap(),
            extra.read_account_blocks().unwrap().len().unwrap());
    }
}

impl BlockVerifier for TVMBlockVerifier {
    type Message = WrappedMessage;
    type OptimisticState = OptimisticStateImpl;

    fn generate_verify_block<'a, I>(
        mut self,
        block: &AckiNackiBlock<GoshBLS>,
        parent_block_state: Self::OptimisticState,
        refs: I,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState)>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a,
    {
        let thread_identifier = block.get_common_section().thread_id;
        let (initial_state, _in_threads_table) = self.shared_services.exec(|container| {
            crate::block::preprocessing::preprocess(
                parent_block_state,
                refs.clone(),
                &thread_identifier,
                &container.cross_thread_ref_data_service,
            )
        })?;
        let mut ref_ids = vec![];
        for state in refs.clone() {
            ref_ids.push(state.block_identifier());
        }

        let time = block
            .tvm_block()
            .info
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .gen_utime()
            .as_u32();
        let block_extra = block
            .tvm_block()
            .extra
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
        let rand_seed = block_extra.rand_seed.clone();

        let mut ext_messages = Vec::new();
        let mut check_messages_map = HashMap::new();
        ensure!(
            block
                .tvm_block()
                .read_extra()
                .unwrap_or_default()
                .read_in_msg_descr()
                .unwrap_or_default()
                .iterate_objects(|in_msg| {
                    let trans =
                        in_msg.read_transaction()?.expect("Failed to read in_msg transaction");
                    let in_msg = in_msg.read_message()?;
                    assert_eq!(
                        check_messages_map.insert(in_msg.hash().unwrap(), trans.logical_time()),
                        None,
                        "Incoming block has non unique messages"
                    );
                    if in_msg.is_inbound_external() {
                        ext_messages.push((trans.logical_time(), in_msg));
                    }
                    Ok(true)
                })
                .map_err(|e| anyhow::format_err!("Failed to parse incoming block messages: {e}"))?,
            "Failed to parse incoming block messages"
        );
        ext_messages.sort_by(|(lt_a, _), (lt_b, _)| lt_a.cmp(lt_b));
        let ext_messages = VecDeque::from_iter(ext_messages.into_iter().map(|(_lt, msg)| msg));

        let block_gas_limit = self.blockchain_config.get_gas_config(false).block_gas_limit;

        tracing::debug!(target: "node", "PARENT block: {:?}", initial_state.get_block_info());

        let producer = BlockBuilder::with_params(
            initial_state,
            time,
            block_gas_limit,
            Some(rand_seed),
            None,
            self.node_config.clone(),
        )
        .map_err(|e| anyhow::format_err!("Failed to create block builder: {e}"))?;
        let (verify_block, _) = producer.build_block(
            ext_messages,
            &self.blockchain_config,
            vec![],
            vec![],
            Some(check_messages_map),
        )?;
        tracing::trace!(target: "node", "verify block generated successfully");
        Self::print_block_info(&verify_block.block);

        let res = (
            AckiNackiBlock::new(
                // TODO: fix single thread implementation
                verify_block.state.thread_id,
                //
                verify_block.block,
                0,
                block.get_common_section().producer_id,
                verify_block.tx_cnt,
                verify_block.block_keeper_set_changes,
                block.get_common_section().verify_complexity,
                block.get_common_section().refs.clone(),
                // Skip checking load balancer actions.
                block.get_common_section().threads_table.clone(),
            ),
            verify_block.state,
        );

        Ok(res)
    }
}
