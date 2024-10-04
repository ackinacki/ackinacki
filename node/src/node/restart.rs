// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::block::BlockIdentifier;
use crate::block::BlockSeqNo;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::SynchronizationResult;
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
    fn replay_follow_fork_choice_rule(
        &self,
        mut replay: VecDeque<(
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
        )>,
    ) -> anyhow::Result<
        VecDeque<(BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>)>,
    > {
        let mut tails = VecDeque::new();
        while let Some((block_id, block_seq_no)) = replay.pop_front() {
            if !BlockIdentifier::is_zero(&block_id) {
                let block: <Self as NodeAssociatedTypes>::CandidateBlock = self.repository.get_block(&block_id)?
                    .unwrap_or_else(|| panic!("Block {:?} must be saved. Fail immediately otherwise. In case of this type of an issue another node must become a leader and continue over the state it has and assumes to be the latest finalized or accepted as the main candidate by majority.", &block_id));

                self.broadcast_candidate_block(block.clone())?;
            }
            // Now. We have to find what is the next block to resend to continue.
            let next_block_seq_no = block_seq_no.next();
            let block_descendants = self
                .repository
                .list_blocks_with_seq_no(&next_block_seq_no)?
                .into_iter()
                .filter(|e| e.data().parent() == block_id)
                .collect::<Vec<<Self as NodeAssociatedTypes>::CandidateBlock>>();
            match self.fork_choice_rule(block_descendants.clone())? {
                Some(next) => replay.push_back((next.data().identifier(), next.data().seq_no())),
                None => tails.push_back((block_id, block_seq_no)),
            }
        }

        Ok(tails)
    }

    fn replay_follow_signed_descendants(
        &self,
        mut replay: VecDeque<(
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
        )>,
    ) -> anyhow::Result<
        VecDeque<(BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>)>,
    > {
        let mut tails = VecDeque::new();
        while let Some((block_id, block_seq_no)) = replay.pop_front() {
            let next_block_seq_no = block_seq_no.next();
            let mut next = vec![];
            for descendant in
            self.repository.list_blocks_with_seq_no(&next_block_seq_no)?.into_iter()
            {
                if self.is_candidate_block_signed_by_this_node(&descendant)? {
                    next.push(descendant);
                }
            }
            if next.is_empty() {
                tails.push_back((block_id.clone(), block_seq_no));
            }
            if next.len() == 1 {
                let next = next.pop().unwrap();
                assert!(
                    next.data().parent() == block_id,
                    "This node has signed another sequence. Can not continue as a producer or can be penalized for 2 blocks with the same seq_no signed"
                );
                replay.push_back((next.data().identifier(), next.data().seq_no()));
                self.broadcast_candidate_block(next)?;
            }
            if next.len() > 1 {
                panic!(
                    "Something went terribly wrong. This node has signed two blocks of the same seq_no and will be penalized"
                );
            }
        }
        Ok(tails)
    }

    pub(crate) fn execute_restarted_producer(
        &mut self,
    ) -> anyhow::Result<SynchronizationResult<<Self as NodeAssociatedTypes>::NetworkMessage>> {
        tracing::info!("Restarted producer");
        let mut replay = VecDeque::new();
        for thread in self.list_threads()? {
            replay.push_back(self.repository.select_thread_last_finalized_block(&thread)?);
        }
        // 1. Follow blocks with the fork choice rule.
        // This ensures all blocks this node is absolutely sure about are replayed.
        let replay = self.replay_follow_fork_choice_rule(replay)?;

        // 2. Continue with blocks descendants this node has signed.
        // Note: all of replay blocks returned from the above were broadcasted already.
        let mut replay = self.replay_follow_signed_descendants(replay)?;

        // Note:
        // Single thread implementation. Lot's will change when this changes. Skipping
        // 3. Ensure tails match list of threads 1:1
        // 4. Ensure there are no tails that are behind the following:
        //    self.find_thread_last_block_id_this_node_can_continue(&thread)
        // 5. Ensure for each thread that there are no next blocks of the same height
        //    signed by this node.
        // ---

        // 6. Reshift cache_forward_optimistic for
        // find_thread_last_block_id_this_node_can_continue method.
        // TODO: refactor find_thread_last_block_id_this_node_can_continue method
        // into a separate class to explicitly indicate when synchronization moves
        // thread <heads>.
        assert!(!replay.is_empty(), "Sanity check. Must never be empty");
        while let Some((block_id, block_seq_no)) = replay.pop_front() {
            let thread_id = {
                if !BlockIdentifier::is_zero(&block_id) {
                    let block = self
                        .repository
                        .get_block(&block_id)?
                        .expect("We had sent the same block in one of the replay sections above.");
                    self.get_block_thread_id(&block)?
                } else {
                    ThreadIdentifierFor::<TBlockProducerProcess>::default()
                }
            };
            self.cache_forward_optimistic.insert(
                thread_id,
                OptimisticForwardState::ProducedBlock(block_id.clone(), block_seq_no),
            );
        }
        Ok(SynchronizationResult::Ok)
    }

    pub(crate) fn restart_bk(&mut self) -> anyhow::Result<()> {
        for thread_id in self.list_threads()? {
            let (cursor_id, cursor_seq_no) =
                self.repository.select_thread_last_main_candidate_block(&thread_id)?;
            let mut next_seq_no = cursor_seq_no.next();
            loop {
                let mut found_block = false;
                for candidate in self.repository.list_blocks_with_seq_no(&next_seq_no)? {
                    if candidate.data().parent() == cursor_id {
                        found_block = true;
                        next_seq_no = next_seq_no.next();
                        if self.is_candidate_block_signed_by_this_node(&candidate)? {
                            for attestation in candidate.data().get_common_section().block_attestations {
                                self.attestation_processor.process_block_attestation(attestation.clone());
                            }
                        } else {
                            let block_process_res = self.on_incoming_candidate_block(
                                &candidate,
                                false,
                            )?;
                            if block_process_res != BlockStatus::Ok {
                                found_block = false;
                            }
                        }
                    } else {
                        continue;
                    }
                }
                if !found_block {
                    break;
                }
            }
        }
        Ok(())
    }
}
