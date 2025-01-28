// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::SynchronizationResult;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;

impl<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = RepositoryImpl>,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateImpl,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = RepositoryImpl
        >,
        TRandomGenerator: rand::Rng,
{
    pub(crate) fn execute_restarted_producer(
        &mut self,
        block_id_to_continue: BlockIdentifier,
        block_seq_no_to_continue: BlockSeqNo,
    ) -> anyhow::Result<SynchronizationResult<NetworkMessage>> {
        tracing::info!("Restarted producer: {} {:?}", block_seq_no_to_continue, block_id_to_continue);

        // Continue BP from the latest applied block
        // Resend blocks from the chosen chain
        let (finalized_block_id, finalized_block_seq_no) = self.repository.select_thread_last_finalized_block(&self.thread_id)?;

        let block_state_to_continue = self.blocks_states.get(&block_id_to_continue)?;
        let mut chain = self.blocks_states.select_unfinalized_ancestor_blocks(block_state_to_continue, finalized_block_seq_no)?;

        let finalized_block_state = self.blocks_states.get(&finalized_block_id)?;
        chain.insert(0, finalized_block_state);

        for block in chain.iter() {
            let block_id = block.block_identifier();
            // For default block id there is no block to broadcast, so skip it
            if block_id != &BlockIdentifier::default() {
                let block = self.repository.get_block(block_id)?.expect("block must exist");
                self.broadcast_candidate_block_that_was_possibly_produced_by_another_node(block.clone())?;
            }
        };

        // Save latest block id and seq_no in cache
        self.cache_forward_optimistic = Some(OptimisticForwardState::ProducedBlock(block_id_to_continue, block_seq_no_to_continue));

        Ok(SynchronizationResult::Ok)
    }

    pub(crate) fn restart_bk(&mut self) -> anyhow::Result<()> {
        tracing::trace!("Restart BK");
        let mut unprocessed_guarded = self.unprocessed_blocks_cache.lock();
        unprocessed_guarded.sort_by(|a, b| {
            let a_seq_no = a.guarded(|e| *e.block_seq_no()).expect("seq_no must be set");
            let b_seq_no = b.guarded(|e| *e.block_seq_no()).expect("seq_no must be set");
            a_seq_no.cmp(&b_seq_no)
        });
        for block_state in unprocessed_guarded.iter() {
            // If block was applied, perform on_block_appended
            if let Some(block_id) = block_state.guarded(|e| { if e.is_block_already_applied() { Some(e.block_identifier().clone()) } else { None } }) {
                let candidate_block = self.repository.get_block(&block_id)?.expect("Candidate block must exist");
                self.shared_services.on_block_appended(candidate_block.data());
            }
        }
        Ok(())
    }
}
