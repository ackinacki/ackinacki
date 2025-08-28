use typed_builder::TypedBuilder;
use std::sync::Arc;
use crate::types::AckiNackiBlock;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::bls::GoshBLS;
use crate::bls::envelope::Envelope;


// Clone is expensive, therefore not directly allowed
#[derive(TypedBuilder, Getters)]
pub struct Checkpoint {

    #[builder(strip_option)]
    block: Option<Envelope<GoshBLS, AckiNackiBlock>>,

    state_after_block_applied: Arc<OptimisticStateImpl>,

    #[getter(skip)]
    shared_services: SharedServices,

    #[getter(skip)]
    block_state_repository: BlockStateRepository,

    #[getter(skip)]
    accounts_repository: AccountsRepository,

    #[getter(skip)]
    message_db: MessageDurableStorage,

    // TODO: remove:
    #[getter(skip)]
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
}


impl Checkpoint {
    // There is an assumption in place:
    // - All move calls must be executed with blocks that had
    //   signatures verified. It means that any failure to
    //   apply those blocks will result in a NACK and slashing.
    // Constrain:
    // - Clone of a state is an expensive operation. So it's
    //   better to avoid it as much as possible.
    pub fn apply(
        self,
        next_block: Envelope<GoshBLS, AckiNackiBlock>,
    ) -> anyhow::Result<Self> {
        let Checkpoint {
            block: _,
            state_after_block_applied: mut state,
            shared_services,
            block_state_repository,
            accounts_repository,
            message_db,
            nack_set_cache,
        } = self;
        let (cross_thread_ref_data, all_added_messages) = state.apply_block(
            next_block.borrow().data(),
            &shared_services,
            block_state_repository.clone(),
            Arc::clone(&nack_set_cache),
            accounts_repository.clone(),
            message_db.clone(),
        )?;
        shared_services.exec(|e| {
            // TODO: add to block_state
            e.cross_thread_ref_data_service.set_cross_thread_ref_data(cross_thread_ref_data)
        })?;
    }
}
