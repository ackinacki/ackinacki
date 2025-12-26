// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::ensure;
use chrono::Utc;
use indexset::BTreeMap;
#[cfg(feature = "mirror_repair")]
use parking_lot::Mutex;
use tracing::instrument;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_block::TrComputePhase;
use tvm_block::TransactionDescr;
use tvm_types::ExceptionCode;
use tvm_types::HashmapType;
use tvm_types::UInt256;
use typed_builder::TypedBuilder;

use crate::block::producer::builder::BlockBuilder;
use crate::block::producer::execution_time::ExecutionTimeLimits;
use crate::block::producer::wasm::WasmNodeCache;
use crate::block_keeper_system::wallet_config::create_wallet_slash_message;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSlashData;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::BlockchainConfigRead;
use crate::config::Config;
use crate::config::GlobalConfig;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::node::associated_types::NackData;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::next_seq_no;
use crate::types::AccountAddress;
use crate::types::AckiNackiBlock;

// Note: produces single verification block.
pub trait BlockVerifier {
    type OptimisticState: OptimisticState;
    type Message: Message;

    fn generate_verify_block<'a, I>(
        self,
        block: &AckiNackiBlock,
        initial_state: Self::OptimisticState,
        refs: I,
        message_db: MessageDurableStorage,
        is_block_of_retired_version: bool,
    ) -> anyhow::Result<(AckiNackiBlock, Self::OptimisticState)>
    where
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a;
}

#[derive(TypedBuilder)]
#[allow(dead_code)]
pub struct TVMBlockVerifier {
    blockchain_config: BlockchainConfigRead,
    node_config: Config,
    node_global_config: GlobalConfig,
    // TODO: need to fill this data for verifier
    epoch_block_keeper_data: Vec<BlockKeeperData>,
    block_nack: Vec<Envelope<GoshBLS, NackData>>,
    shared_services: SharedServices,
    accounts_repository: AccountsRepository,
    block_state_repository: BlockStateRepository,
    metrics: Option<BlockProductionMetrics>,
    wasm_cache: WasmNodeCache,
    #[cfg(feature = "mirror_repair")]
    is_updated_mv: Arc<Mutex<bool>>,
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

    #[instrument(skip_all)]
    fn generate_verify_block<'a, I>(
        mut self,
        block: &AckiNackiBlock,
        parent_block_state: Self::OptimisticState,
        refs: I,
        message_db: MessageDurableStorage,
        is_block_of_retired_version: bool,
    ) -> anyhow::Result<(AckiNackiBlock, Self::OptimisticState)>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a,
    {
        let thread_identifier = block.get_common_section().thread_id;
        let mut time_limits = ExecutionTimeLimits::verification(&self.node_global_config);
        let mut wrapped_slash_messages = vec![];
        let mut white_list_of_slashing_messages_hashes = HashSet::new();
        for nack in self.block_nack.iter() {
            tracing::trace!("push nack into slash {:?}", nack);
            let reason = nack.data().reason.clone();
            if let Some((id, bls_key, addr)) =
                reason.get_node_data(self.block_state_repository.clone())
            {
                let epoch_nack_data =
                    BlockKeeperSlashData { node_id: id, bls_pubkey: bls_key, addr, slash_type: 0 };
                let msg = create_wallet_slash_message(&epoch_nack_data)?;
                let wrapped_message = WrappedMessage { message: msg.clone() };
                wrapped_slash_messages.push(Arc::new(wrapped_message));
                white_list_of_slashing_messages_hashes.insert(msg.hash().unwrap());
            }
        }

        let parent_block_seq_no = *parent_block_state.get_block_seq_no();
        let preprocessing_result = self.shared_services.exec(|container| {
            crate::block::preprocessing::preprocess(
                parent_block_state,
                refs.clone(),
                &thread_identifier,
                &container.cross_thread_ref_data_service,
                wrapped_slash_messages,
                Vec::new(),
                message_db.clone(),
                self.metrics.clone(),
            )
        })?;
        let mut ref_ids = vec![];
        for state in refs.clone() {
            ref_ids.push(state.block_identifier());
        }

        let time = block.time()?;
        let block_extra = block
            .tvm_block()
            .extra
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
        let rand_seed = block_extra.rand_seed.clone();

        let mut ext_messages = Vec::new();
        let mut check_messages_map: HashMap<AccountAddress, BTreeMap<u64, UInt256>> =
            HashMap::new();
        let block_parse_result = block
            .tvm_block()
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

                if in_msg.is_inbound_external() {
                    ext_messages.push((trans.logical_time(), in_msg));
                }
                if let Ok(TransactionDescr::Ordinary(tx)) = trans.read_description() {
                    if tx.aborted {
                        if let TrComputePhase::Vm(vm) = tx.compute_ph {
                            if vm.exit_code == ExceptionCode::ExecutionTimeout as i32 {
                                time_limits.add_alternative_message(msg_hash);
                            }
                        }
                    }
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to parse incoming block messages: {e}"));
        ensure!(block_parse_result?, "Failed to parse incoming block messages");
        ext_messages.sort_by(|(lt_a, _), (lt_b, _)| lt_a.cmp(lt_b));
        let timestamp = Utc::now();
        let mut grouped_ext_messages = HashMap::new();

        for (i, (_, msg)) in ext_messages.into_iter().enumerate() {
            let stamp = Stamp { index: i as u64, timestamp };
            let acc_id = AccountAddress::from(msg.int_dst_account_id().unwrap_or_default());

            grouped_ext_messages
                .entry(acc_id)
                .or_insert_with(VecDeque::new)
                .push_back((stamp, msg));
        }

        let blockchain_config = self.blockchain_config.get(&next_seq_no(parent_block_seq_no));
        let block_gas_limit = blockchain_config.get_gas_config(false).block_gas_limit;

        tracing::debug!(target: "node", "PARENT block: {:?}", preprocessing_result.state.get_block_info());

        let producer = BlockBuilder::with_params(
            thread_identifier,
            preprocessing_result.state,
            time,
            block_gas_limit,
            Some(rand_seed),
            None,
            self.accounts_repository.clone(),
            self.node_global_config.block_keeper_epoch_code_hash.clone(),
            self.node_global_config.block_keeper_preepoch_code_hash.clone(),
            self.node_config.local.parallelization_level,
            preprocessing_result.redirected_messages,
            self.metrics,
            self.wasm_cache,
            #[cfg(feature = "mirror_repair")]
            self.is_updated_mv,
        )
        .map_err(|e| anyhow::format_err!("Failed to create block builder: {e}"))?;
        let (verify_block, _, _) = producer.build_block(
            grouped_ext_messages,
            &blockchain_config,
            vec![],
            Some(check_messages_map),
            white_list_of_slashing_messages_hashes,
            message_db.clone(),
            &time_limits,
            is_block_of_retired_version,
        )?;

        tracing::trace!(target: "node", "verify block generated successfully");
        Self::print_block_info(&verify_block.block);

        let mut new_state = verify_block.state;
        if let Some(threads_table) = block.get_common_section().threads_table.clone() {
            new_state.threads_table = threads_table;
        }

        let res = (
            AckiNackiBlock::new(
                // TODO: fix single thread implementation
                new_state.thread_id,
                //
                verify_block.block,
                block.get_common_section().producer_id.clone(),
                verify_block.tx_cnt,
                verify_block.block_keeper_set_changes,
                block.get_common_section().verify_complexity,
                block.get_common_section().refs.clone(),
                // Skip checking load balancer actions.
                block.get_common_section().threads_table.clone(),
                block.get_common_section().round,
                block.get_common_section().block_height,
                #[cfg(feature = "monitor-accounts-number")]
                block.get_common_section().accounts_number_diff,
                #[cfg(feature = "protocol_version_hash_in_block")]
                block.get_common_section().protocol_version_hash().clone(),
            ),
            new_state,
        );
        Ok(res)
    }
}
