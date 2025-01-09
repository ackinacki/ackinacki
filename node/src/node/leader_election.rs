// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

impl<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

        >,
        TRepository: Repository<
            BLS = GoshBLS,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<GoshBLS, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<GoshBLS, AttestationData>,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
        TRandomGenerator: rand::Rng,
{

    pub(crate) fn update_producer_group(
        &mut self,
        thread_id: &ThreadIdentifier,
        initial_group: Vec<NodeIdentifier>
    ) -> anyhow::Result<()> {
        tracing::trace!("update_producer_group: thread_id:{thread_id:?} initial_group: {initial_group:?}");

        let (_, last_block_seq_no) = self.find_thread_last_block_id_this_node_can_continue(thread_id)?;
        let mut block_keeper_indexes: Vec<NodeIdentifier> = {
            let bk_set = self.block_keeper_sets.with_last_entry(|e| {
                let e = e.expect("node should have actual BK set");
                e.get().clone()
            });
            bk_set
                .keys()
                .map(|k| k.to_owned() as NodeIdentifier)
                .filter(|k| !initial_group.contains(k))
                .collect()
        };
        // Sort the list, because iteration over map is random
        block_keeper_indexes.sort();
        let mut producer_group = initial_group;
        tracing::trace!(
            "update_producer_group: initial_group:{:?} block_keeper_indexes: {:?}, last_block_seq_no: {:?}",
            producer_group,
            block_keeper_indexes,
            last_block_seq_no,
        );
        while producer_group.len() < self.config.global.producer_group_size {
            if block_keeper_indexes.is_empty() {
                // TODO: Seems like total amount of block keepers is not enough.
                // break for now
                break;
            }
            producer_group.push(block_keeper_indexes.remove(self.producer_election_rng.gen_range(0..block_keeper_indexes.len())));
        }
        let next_block_seq_no = next_seq_no(last_block_seq_no);
        tracing::trace!("select_producer_group for {last_block_seq_no:?}: producer_group: {:?}", producer_group);
        let entry = self.block_producer_groups.entry(*thread_id).or_default();
        // Note: Producer group map must be initialized from zero block seq_no if it was empty,
        // because node can receive old blocks
        if entry.is_empty() {
            entry.insert(BlockSeqNo::default(), producer_group);
        } else {
            entry.insert(next_block_seq_no, producer_group);
        }
        tracing::trace!("Producer groups: {:?}", self.block_producer_groups);
        Ok(())
    }

    pub fn rotate_producer_group(
        &mut self,
        thread_id: &ThreadIdentifier,
    )  -> anyhow::Result<()> {
        tracing::info!("rotate_producer_group thread:{thread_id:?}");
        // Remove current producer leader
        let mut group = self.get_latest_producer_group(thread_id);
        assert!(!group.is_empty());
        let removed_block_keeper = group.remove(0);
        tracing::trace!("Removed producer node_id: {removed_block_keeper}");
        tracing::trace!("The rest producer group: {:?}", group);
        self.update_producer_group(thread_id, group)?;
        Ok(())
    }

    pub(crate) fn increase_block_gaps(&mut self) -> anyhow::Result<bool> {
        // TODO: we should have a mechanism to rotate producer if it works almost bad
        tracing::trace!("Increase block gap");
        let mut this_node_became_a_producer = false;
        let max_gap_size = self.config.global.producer_change_gap_size;
        for thread in self.list_threads()? {
            let do_rotate = {
                let entry = self.block_gap_length.entry(thread).or_default();
                *entry += 1;
                tracing::trace!("Increase block gap for thread {:?}: gap = {}", thread, *entry);
                if *entry >= max_gap_size {
                    *entry = 0;
                    true
                } else {
                    false
                }
            };
            if do_rotate {
                self.rotate_producer_group(&thread)?;
                this_node_became_a_producer = self.is_this_node_a_producer_for_new_block(
                    &self.thread_id,
                );
            }
        }
        if !this_node_became_a_producer {
            let thread_id = self.thread_id;
            self.resend_attestations_on_bp_change(self.get_latest_block_producer(&thread_id))?;
        }
        Ok(this_node_became_a_producer)
    }

    pub(crate) fn clear_block_gap(&mut self, thread_id: &ThreadIdentifier) {
        tracing::trace!("Clear thread gap: {thread_id:?}");
        self.block_gap_length.entry(*thread_id).and_modify(|val| *val = 0).or_insert(0);
    }

    pub(crate) fn set_producer_groups_from_finalized_state(&mut self, thread: ThreadIdentifier, block_seq_no: BlockSeqNo, producer_group: Vec<NodeIdentifier>) {
        tracing::trace!("Set producer groups from finalized state thread: {thread:?}, block_seq_no: {block_seq_no:?}. producer_group: {producer_group:?}");
        let groups = self.block_producer_groups.entry(thread).or_default();
        groups.insert(block_seq_no, producer_group);
        tracing::trace!("Set producer groups from finalized state finish {:?}", self.block_producer_groups);
    }

    pub(crate) fn is_this_node_a_producer_for(
        &self,
        thread_id: &ThreadIdentifier,
        block_seq_no: &BlockSeqNo,
    ) -> bool {
        self.current_block_producer_id(thread_id, block_seq_no) == self.config.local.node_id
    }

    pub(crate) fn is_this_node_a_producer_for_new_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> bool {
        let group = self.get_latest_producer_group(thread_id);
        group[0] == self.config.local.node_id
    }

    pub(crate) fn get_latest_producer_group(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> Vec<NodeIdentifier> {
        tracing::trace!("get_latest_producer_group: {thread_id:?} {:?}", self.block_producer_groups);
        let producer_groups = self.block_producer_groups.get(thread_id).expect("Thread producer group must be set");
        producer_groups.last_key_value().expect("Thread producer group should not be empty").1.clone()
    }

    pub(crate) fn get_latest_block_producer(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> NodeIdentifier {
        tracing::trace!("get_latest_producer_group: {thread_id:?} {:?}", self.block_producer_groups);
        let producer_groups = self.block_producer_groups.get(thread_id).expect("Thread producer group must be set");
        producer_groups.last_key_value().expect("Thread producer group should not be empty").1.clone()[0]
    }

    pub(crate) fn get_latest_producer_groups_for_all_threads(
        &self,
    ) -> HashMap<ThreadIdentifier, Vec<NodeIdentifier>> {
        self.block_producer_groups.clone().into_iter().map(|(k, v)| {
            (k, v.last_key_value().expect("Producer group should be set for the active thread").1.clone())
        }).collect()
    }

    pub(crate) fn get_producer_group(
        &self,
        thread_id: &ThreadIdentifier,
        block_seq_no: &BlockSeqNo,
    ) -> Vec<NodeIdentifier> {
        tracing::trace!("get_latest_producer_group: {thread_id:?} {:?}", self.block_producer_groups);
        let producer_groups = self.block_producer_groups.get(thread_id).expect("Thread producer group must be set");
        for seq_no in producer_groups.keys().rev() {
            if block_seq_no >= seq_no {
                return producer_groups.get(seq_no).unwrap().clone();
            }
        }
        tracing::trace!("get_producer_group: {producer_groups:?} {block_seq_no:?}");
        panic!("Failed to find right producers group for thread_id: {thread_id:?}, block_seq_no: {block_seq_no:?}, block_producer_groups: {:?}", self.block_producer_groups)
    }

    pub(crate) fn current_block_producer_id(
        &self,
        thread_id: &ThreadIdentifier,
        block_seq_no: &BlockSeqNo,
    ) -> NodeIdentifier {
        self.get_producer_group(thread_id, block_seq_no)[0]
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Arc;

    use num_bigint::BigUint;
    use num_traits::Zero;
    use parking_lot::lock_api::Mutex;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;

    use crate::block::keeper::process::TVMBlockKeeperProcess;
    use crate::block::producer::process::TVMBlockProducerProcess;
    use crate::block_keeper_system::BlockKeeperData;
    use crate::block_keeper_system::BlockKeeperSet;
    use crate::block_keeper_system::BlockKeeperStatus;
    use crate::bls::GoshBLS;
    use crate::helper::key_handling::key_pair_from_file;
    use crate::multithreading::routing::service::RoutingService;
    use crate::node::attestation_processor::AttestationProcessorImpl;
    use crate::node::services::sync::StateSyncServiceStub;
    use crate::node::shared_services::SharedServices;
    use crate::node::BlockStateRepository;
    use crate::node::Node;
    use crate::node::NodeIdentifier;
    use crate::node::SignerIndex;
    use crate::repository::repository_impl::RepositoryImpl;
    use crate::tests::default_config;
    use crate::types::block_keeper_ring::BlockKeeperRing;
    use crate::types::AccountAddress;
    use crate::types::BlockIdentifier;
    use crate::types::BlockSeqNo;
    use crate::types::ThreadIdentifier;
    use crate::utilities::FixedSizeHashSet;

    #[test]
    fn test_leader_election() -> anyhow::Result<()> {
        tracing::trace!("Start test");

        let total_nodes_cnt = 1000;
        let iterations_cnt = 5000;

        // Run node entity
        let config = default_config(1);
        std::fs::create_dir_all("/tmp/leader_election/")?;
        Command::new("../target/release/migration-tool")
            .arg("-p")
            .arg("/tmp/leader_election")
            .arg("-n")
            .arg("-a")
            .status()?;
        let thread_id = ThreadIdentifier::default();
        let blocks_states =
            BlockStateRepository::new(PathBuf::from("/tmp/leader_election/block-state/"));
        let repository = RepositoryImpl::new(
            PathBuf::from("/tmp/leader_election/"),
            None,
            1,
            SharedServices::test_start(RoutingService::stub().0),
            BlockKeeperRing::default(),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            blocks_states,
        );
        let (router, _router_rx) = crate::multithreading::routing::service::RoutingService::stub();
        let shared_services = crate::node::shared_services::SharedServices::test_start(router);

        let production_process = TVMBlockProducerProcess::new(
            config.clone(),
            repository.clone(),
            None,
            Arc::new(Mutex::new(vec![])),
            shared_services.clone(),
        )?;

        let validation_process = TVMBlockKeeperProcess::new(
            "./tests/resources/blockchain.conf.json",
            repository.clone(),
            config.clone(),
            None,
            Some(BlockIdentifier::default()),
            thread_id,
            shared_services,
            BlockKeeperRing::default(),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
        )?;

        let (_incoming_messages_sender, incoming_messages_receiver) = std::sync::mpsc::channel();
        let (broadcast_sender, _broadcast_recv) = std::sync::mpsc::channel();
        let (single_sender, _single_recv) = std::sync::mpsc::channel();
        let (raw_block_sender, _raw_block_recv) = std::sync::mpsc::channel();
        let (feedback_sender, _feedback_receiver) = std::sync::mpsc::channel();

        let (pubkey, secret) =
            key_pair_from_file::<GoshBLS>("../config/block_keeper1_bls.keys.json".to_string());

        let block_keeper_rng = SmallRng::from_seed(secret.take_as_seed());

        let sync_state_service = StateSyncServiceStub::new();

        let mut block_keeper_sets = BlockKeeperSet::new();

        for i in 0..total_nodes_cnt {
            block_keeper_sets.insert(
                i,
                BlockKeeperData {
                    owner_address: AccountAddress::default(),
                    wallet_index: i as NodeIdentifier,
                    pubkey: pubkey.clone(),
                    epoch_finish_timestamp: 0,
                    status: BlockKeeperStatus::Active,
                    address: "".to_string(),
                    stake: BigUint::zero(),
                    signer_index: i as SignerIndex,
                },
            );
        }

        let block_keeper_set = BlockKeeperRing::new(BlockSeqNo::from(0), block_keeper_sets);

        let block_processor = AttestationProcessorImpl::new(
            repository.clone(),
            block_keeper_set.clone(),
            ThreadIdentifier::default(),
        );
        let (router, _router_rx) = crate::multithreading::routing::service::RoutingService::stub();
        let shared_services = crate::node::shared_services::SharedServices::test_start(router);
        let repo_path = PathBuf::from("/tmp/leader_election/");
        let blocks_states = BlockStateRepository::new(repo_path.clone().join("blocks-states"));
        let mut node = Node::new(
            shared_services,
            sync_state_service,
            production_process,
            validation_process,
            repository,
            incoming_messages_receiver,
            broadcast_sender,
            single_sender,
            raw_block_sender,
            pubkey,
            secret.clone(),
            config.clone(),
            block_processor,
            block_keeper_set,
            block_keeper_rng.clone(),
            block_keeper_rng,
            ThreadIdentifier::default(),
            feedback_sender,
            true,
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            blocks_states,
        );

        // Prepare set that contains all node ids
        let mut all_nodes_set: HashSet<i32> =
            HashSet::from_iter(0..total_nodes_cnt as NodeIdentifier);

        // Start loop that rotates producer group and removes all producer nodes from set
        for _ in 0..iterations_cnt {
            let producer_group = node.get_latest_producer_group(&thread_id);
            for node_id in producer_group {
                let _ = all_nodes_set.remove(&node_id);
            }
            if all_nodes_set.is_empty() {
                break;
            }
            // Rotate several times to renew the whole producer group
            for _ in 0..config.global.producer_group_size {
                node.rotate_producer_group(&thread_id)?;
            }
        }
        assert!(all_nodes_set.is_empty());
        Ok(())
    }
}
