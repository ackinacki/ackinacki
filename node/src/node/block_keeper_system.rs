// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::block_keeper_system::BlockKeeperStatus;
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

const EPOCH_TOUCH_RETRY_TIME_DELTA: u32 = 5;

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
    // BP node checks current epoch contracts and sends touch message to finish them
    pub(crate) fn check_and_touch_block_keeper_epochs(&mut self) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp() as u32;
        tracing::trace!("check block keepers: now={now}");
        let mut block_keeper_set = self.block_keeper_ring_pubkeys.lock();
        let data_vec: Vec<BlockKeeperData> = block_keeper_set.values().cloned().collect();
        for mut data in data_vec {
            if data.epoch_finish_timestamp < now {
                tracing::trace!("Epoch is outdated: now={now} {data:?}");

                match data.status {
                    // If block keeper was not touched, send touch message, change its status
                    // and increase saved timestamp with 5 seconds
                    BlockKeeperStatus::Active => {
                        self.production_process.send_epoch_message(
                            &ThreadIdentifierFor::<TBlockProducerProcess>::default(),
                            data.clone(),
                        );
                        data.epoch_finish_timestamp += EPOCH_TOUCH_RETRY_TIME_DELTA;
                        data.status = BlockKeeperStatus::CalledToFinish;
                        block_keeper_set.insert(
                            data.index,
                            data,
                        );
                    },
                    // If block keeper was already touched, touch it one more time and
                    // change status for not to change.
                    BlockKeeperStatus::CalledToFinish => {
                        self.production_process.send_epoch_message(
                            &ThreadIdentifierFor::<TBlockProducerProcess>::default(),
                            data.clone(),
                        );
                        data.status = BlockKeeperStatus::Expired;
                        block_keeper_set.insert(
                            data.index,
                            data,
                        );
                    },
                    BlockKeeperStatus::Expired => {},
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_block_keeper_set_from_common_section(
        &mut self,
        block: &BlockFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        let common_section = block.get_common_section();
        let mut block_keeper_set = self.block_keeper_ring_pubkeys.lock();
        for block_keeper_change in common_section.block_keeper_set_changes {
            match block_keeper_change {
                BlockKeeperSetChange::BlockKeeperAdded((signer_index, block_keeper_data)) => {
                    tracing::trace!("insert block keeper key: {signer_index} {block_keeper_data:?}");
                    tracing::trace!("insert block keeper key: {:?}", block_keeper_set);
                    block_keeper_set.insert(signer_index, block_keeper_data);
                },
                BlockKeeperSetChange::BlockKeeperRemoved((signer_index, block_keeper_data)) => {
                    tracing::trace!("Remove block keeper key: {signer_index} {block_keeper_data:?}");
                    tracing::trace!("Remove block keeper key: {:?}", block_keeper_set);
                    let block_keeper_data = block_keeper_set.remove(&signer_index);
                    tracing::trace!("Removed block keeper key: {:?}", block_keeper_data);
                },
            }
        }
        Ok(())
    }
}
