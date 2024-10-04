// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;

use num_bigint::BigUint;
use num_traits::Zero;
use rand::prelude::Rng;
use rand::prelude::SeedableRng;
use rand::prelude::SmallRng;
use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::block::BlockSeqNo;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::ThreadIdentifierFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        BlockProducerProcess<Block = BlockFor<TBlockProducerProcess>, Repository = TRepository>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BLSSignatureScheme = TBLSSignatureScheme>,
        <<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block as Block>::BlockSeqNo:
        Eq + Hash,
        ThreadIdentifierFor<TBlockProducerProcess>: Default,
        BlockFor<TBlockProducerProcess>: Clone + Display,
        BlockIdentifierFor<TBlockProducerProcess>: Serialize + for<'de> Deserialize<'de>,
        TValidationProcess: BlockKeeperProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
            BlockSeqNo = BlockSeqNoFor<TBlockProducerProcess>,
            BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,
            ThreadIdentifier = ThreadIdentifierFor<TBlockProducerProcess>,
            Block = BlockFor<TBlockProducerProcess>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Block: From<<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block>,
        TRandomGenerator: rand::Rng,
{

    pub(crate) fn update_producer_group(
        &mut self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
        initial_group: Vec<NodeIdentifier>
    ) -> anyhow::Result<()> {
        let last_block_id = self.repository.get_latest_block_id_with_producer_group_change(thread_id)?;
        tracing::trace!("update_producer_group: thread_id:{thread_id:?} last_block_id:{last_block_id:?} initial_group: {initial_group:?}");
        let seed_bytes: [u8; 32] = last_block_id.as_ref().try_into().unwrap();

        let (_, last_block_seq_no) = self.find_thread_last_block_id_this_node_can_continue(thread_id)?;
        let mut small_rng = SmallRng::from_seed(seed_bytes);
        let mut block_keeper_indexes: Vec<NodeIdentifier> = self
            .block_keeper_ring_pubkeys
            .lock()
            .keys()
            .map(|k| k.to_owned() as NodeIdentifier)
            .filter(|k| !initial_group.contains(k))
            .collect();
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
            producer_group.push(block_keeper_indexes.remove(small_rng.gen_range(0..block_keeper_indexes.len())));
        }
        let next_block_seq_no = last_block_seq_no.next();
        tracing::trace!("select_producer_group for {last_block_seq_no:?}: producer_group: {:?}", producer_group);
        self.block_producer_groups.entry(thread_id.clone()).or_default().insert(next_block_seq_no, producer_group);
        tracing::trace!("Producer groups: {:?}", self.block_producer_groups);
        Ok(())
    }

    pub fn rotate_producer_group(
        &mut self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
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
                let entry = self.block_gap_length.entry(thread.clone()).or_default();
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
                    &ThreadIdentifierFor::<TBlockProducerProcess>::default(),
                );
            }
        }
        Ok(this_node_became_a_producer)
    }

    pub(crate) fn clear_block_gap(&mut self, thread_id: &ThreadIdentifierFor<TBlockProducerProcess>) {
        tracing::trace!("Clear thread gap: {thread_id:?}");
        self.block_gap_length.entry(thread_id.clone()).and_modify(|val| *val = 0).or_insert(0);
    }

    pub(crate) fn set_producer_groups_from_finalized_state(&mut self, thread: ThreadIdentifierFor<TBlockProducerProcess>, block_seq_no: BlockSeqNoFor<TBlockProducerProcess>, producer_group: Vec<NodeIdentifier>) {
        tracing::trace!("Set producer groups from finalized state thread: {thread:?}, block_seq_no: {block_seq_no:?}. producer_group: {producer_group:?}");
        let groups = self.block_producer_groups.entry(thread).or_default();
        groups.insert(block_seq_no, producer_group);
        tracing::trace!("Set producer groups from finalized state finish {:?}", self.block_producer_groups);
    }

    pub(crate) fn is_this_node_a_producer_for(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
        block_seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> bool {
        self.current_block_producer_id(thread_id, block_seq_no) == self.config.local.node_id
    }

    pub(crate) fn is_this_node_a_producer_for_new_block(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
    ) -> bool {
        let group = self.get_latest_producer_group(thread_id);
        group[0] == self.config.local.node_id
    }

    pub(crate) fn get_latest_producer_group(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
    ) -> Vec<NodeIdentifier> {
        let producer_groups = self.block_producer_groups.get(thread_id).expect("Thread producer group must be set");
        producer_groups.last_key_value().expect("Thread producer group should not be empty").1.clone()
    }

    pub(crate) fn get_latest_block_producer(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
    ) -> NodeIdentifier {
        let producer_groups = self.block_producer_groups.get(thread_id).expect("Thread producer group must be set");
        producer_groups.last_key_value().expect("Thread producer group should not be empty").1.clone()[0]
    }

    pub(crate) fn get_latest_producer_groups_for_all_threads(
        &self,
    ) -> HashMap<ThreadIdentifierFor<TBlockProducerProcess>, Vec<NodeIdentifier>> {
        self.block_producer_groups.clone().into_iter().map(|(k, v)| {
            (k, v.last_key_value().expect("Producer group should be set for the active thread").1.clone())
        }).collect()
    }

    pub(crate) fn get_producer_group(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
        block_seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> Vec<NodeIdentifier> {
        let producer_groups = self.block_producer_groups.get(thread_id).expect("Thread producer group must be set");
        tracing::trace!("get_producer_group: {producer_groups:?} {block_seq_no:?}");
        for seq_no in producer_groups.keys().rev() {
            if block_seq_no >= seq_no {
                return producer_groups.get(seq_no).unwrap().clone();
            }
        }
        panic!("Failed to find right producers group for thread_id: {thread_id:?}, block_seq_no: {block_seq_no:?}, block_producer_groups: {:?}", self.block_producer_groups)
    }

    pub(crate) fn current_block_producer_id(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
        block_seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> NodeIdentifier {
        self.get_producer_group(thread_id, block_seq_no)[0]
    }

    pub(crate) fn slash_bp_stake(
        &mut self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
        block_seq_no: BlockSeqNoFor<TBlockProducerProcess>,
        block_producer_id: NodeIdentifier,
    ) -> anyhow::Result<()> {
        if self.current_block_producer_id(thread_id, &block_seq_no) == block_producer_id {
            self.rotate_producer_group(thread_id)?;
        }
        {
            // Set BP stake to zero
            tracing::trace!("Set nacked BP stake to zero");
            let mut block_keeper_set = self.block_keeper_ring_pubkeys.lock();
            block_keeper_set.entry(block_producer_id as SignerIndex).and_modify(|data| data.stake = BigUint::zero());
        }
        Ok(())
    }
}
