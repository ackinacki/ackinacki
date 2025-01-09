// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

#[cfg(test)]
pub struct StubValidationProcess<TBlockProducer: BlockProducer> {
    _phantom_producer: PhantomData<TBlockProducer>,
}

#[cfg(test)]
impl<TBlockProducer: BlockProducer> StubValidationProcess<TBlockProducer> {
    pub fn for_producer(_producer: &TBlockProducer) -> Self {
        Self { _phantom_producer: PhantomData }
    }

    pub fn for_production_process<T>(_process: &T) -> Self
    where
        T: BlockProducerProcess<BlockProducer = TBlockProducer>,
    {
        Self { _phantom_producer: PhantomData }
    }
}

#[cfg(test)]
impl<TBlockProducer: BlockProducer> BlockKeeperProcess for StubValidationProcess<TBlockProducer> {
    type BLSSignatureScheme = ();
    type CandidateBlock = ();
    type OptimisticState = ();

    fn validate<T: Into<Self::CandidateBlock>>(&mut self, _block: T) -> anyhow::Result<()> {
        Ok(())
    }

    fn apply_block<T: Into<Self::CandidateBlock>>(&mut self, _block: T) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_candidate_block_can_be_applied(&self, _block_id: &BlockIdentifier) -> bool {
        todo!()
    }

    fn get_verification_results(&self) -> anyhow::Result<Vec<(BlockIdentifier, BlockSeqNo, bool)>> {
        todo!()
    }

    fn get_last_state(&self) -> Option<Self::OptimisticState> {
        todo!()
    }
}
