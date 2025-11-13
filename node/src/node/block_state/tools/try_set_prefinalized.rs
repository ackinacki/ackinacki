use crate::node::block_state::tools::invalidate_branch;
use crate::node::unprocessed_blocks_collection::FilterPrehistoric;
use crate::node::AttestationData;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::Envelope;
use crate::node::GoshBLS;
use crate::types::BlockRound;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn try_set_prefinalized(
    block_state: &BlockState,
    block_state_repository: &BlockStateRepository,
    proof: Envelope<GoshBLS, AttestationData>,
) -> anyhow::Result<()> {
    tracing::trace!("try_set_prefinalized: {:?}", block_state);
    let Some(parent_block_identifier) =
        block_state.guarded(|e| e.parent_block_identifier().clone())
    else {
        tracing::trace!("parent block identifier is not set");
        anyhow::bail!("parent block identifier is not set");
    };
    let Some(block_thread_identifier) = block_state.guarded(|e| *e.thread_identifier()) else {
        tracing::trace!("thread_identifier is not set");
        anyhow::bail!("thread_identifier is not set");
    };
    let Some(block_round) = block_state.guarded(|e| *e.block_round()) else {
        tracing::trace!("block_round is not set");
        anyhow::bail!("block_round is not set");
    };
    if !block_state.guarded(|e| e.is_signatures_verified()) {
        tracing::trace!(
            "prefinalized marker can not be set on a block that has not signatures verified"
        );
        anyhow::bail!(
            "prefinalized marker can not be set on a block that has not signatures verified"
        );
    }

    let parent_block_state = block_state_repository.get(&parent_block_identifier)?;
    parent_block_state.guarded(|p| {
        let Some(sibling_identifiers) = p.known_children(&block_thread_identifier)
        else {
            // No other siblings yet. nothing to worry about.
            return Ok(());
        };
        let other_prefinalized: Vec<BlockState> =  sibling_identifiers
            .iter()
            .map(|s| block_state_repository.get(s).expect("block state access must not fail"))
            .filter(|s| s.guarded(|e| e.is_prefinalized() && e.block_identifier() != block_state.block_identifier() && !e.is_invalidated() ))
            .collect();
        if other_prefinalized.is_empty() {
            // No other prefinalized siblings yet. nothing to worry about.
            block_state.guarded_mut(|e| e.set_prefinalized(proof))
                .expect("set prefinalized must not fail");

            return Ok(());
        }
        let (other_prefinalized_block_identifier, other_prefinalized_max_block_round) = other_prefinalized.iter().map(|s| (
            s.block_identifier().clone(),
            s.guarded(|e| e.block_round().expect("Block round of a prefeinalized block must be set"))))
            .max_by(|a, b| (a.1).cmp(&b.1))
            .unwrap();
        match BlockRound::cmp(&block_round, &other_prefinalized_max_block_round) {
            std::cmp::Ordering::Greater =>  {
                block_state.guarded_mut(|e| e.set_prefinalized(proof))
                    .expect("set prefinalized must not fail");
                other_prefinalized.into_iter()
                    .for_each(|s| invalidate_branch(
                        s,
                        block_state_repository,
                        &FilterPrehistoric::builder()
                            .block_seq_no(BlockSeqNo::default())
                            .build()
                    ));
                Ok(())
            }
            std::cmp::Ordering::Less => {
                // We expect all blocks to have their prefinalization set with this function.
                // This mean another prefinalized block has it's signatures verified.
                invalidate_branch(
                    block_state.clone(),
                    block_state_repository,
                    &FilterPrehistoric::builder()
                        .block_seq_no(BlockSeqNo::default())
                        .build()
                );
                Ok(())
            }
            std::cmp::Ordering::Equal => {
                tracing::error!("Protocol violation — duplicate blocks for round {}: blocks {} and {} were both produced. This is a slashable offense; Triggering slashing procedures.", block_round, block_state.block_identifier(), other_prefinalized_block_identifier);
                // TODO: fire NACK and slashing procedure
                anyhow::bail!("protocol violation");
            }
        }
    })
}
