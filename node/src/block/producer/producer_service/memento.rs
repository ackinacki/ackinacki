use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use derive_getters::Getters;
use http_server::ExtMsgFeedbackList;
use typed_builder::TypedBuilder;

use crate::node::block_state::repository::BlockState;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::AckiNackiBlockVersioned;
use crate::versioning::ProtocolVersion;

// Intentionally not allowing direct read of assumptions.
// This way we force writing all assumptions to be listed
// before it can be checked if assumed holds.
#[derive(TypedBuilder, PartialEq, Eq, Debug)]
pub struct Assumptions {
    // Note: Stub for future preattestations impl
    new_to_bk_set: BTreeSet<SignerIndex>,
    block_version: ProtocolVersion,
}

#[derive(TypedBuilder, Getters)]
pub struct BlockProducerMemento {
    produced_blocks: Vec<ProducedBlock>,
    #[builder(default)]
    last_attestation_notification: Option<u32>,
}

impl BlockProducerMemento {
    pub fn produced_blocks_mut(&mut self) -> &mut Vec<ProducedBlock> {
        &mut self.produced_blocks
    }

    pub fn set_last_attestation_notification(&mut self, last_attestation_notification: u32) {
        self.last_attestation_notification = Some(last_attestation_notification);
    }
}

#[derive(TypedBuilder, Getters)]
pub struct ProducedBlock {
    assumptions: Assumptions,
    block: AckiNackiBlockVersioned,
    optimistic_state: Arc<OptimisticStateImpl>,
    feedbacks: ExtMsgFeedbackList,
    block_state: BlockState,
    metrics_memento_init_time: Option<Instant>,
}

impl ProducedBlock {
    pub fn set_memento_init_time(&mut self, memento_init_time: Instant) {
        self.metrics_memento_init_time = Some(memento_init_time);
    }
}
