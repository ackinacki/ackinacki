use super::invalidate_branch;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

macro_rules! connect {
    (parent = $parent:ident,child = $child:ident, $block_state_repository:expr) => {
        $crate::node::block_state::tools::link_parent_child::do_link(
            $crate::node::block_state::tools::link_parent_child::Link {
                parent: crate::node::block_state::repository::BlockState::clone(&$parent),
                child: crate::node::block_state::repository::BlockState::clone(&$child),
            },
            $block_state_repository,
        );
    };
}

pub(crate) use connect;

use crate::node::unprocessed_blocks_collection::FilterPrehistoric;
use crate::types::BlockSeqNo;

pub struct Link {
    pub parent: BlockState,
    pub child: BlockState,
}
pub fn do_link(link: Link, block_state_repository: &BlockStateRepository) {
    let Link { parent, child } = link;
    let thread_identifier = child.guarded(|e| {
        (*e.thread_identifier()).expect(
            "It is not possible to connect a child that has no thread identifier information set.",
        )
    });
    let (is_parent_finalized, is_parent_invalidated) = parent.guarded_mut(|e| {
        e.add_child(thread_identifier, child.block_identifier().clone()).unwrap();
        (e.is_finalized(), e.is_invalidated())
    });
    if is_parent_invalidated && is_parent_finalized {
        let parent_block_identifier = parent.block_identifier().clone();
        panic!("Critical: wrong block state. Block {parent_block_identifier:?} is invalidated and finalized at the same time");
    }
    if is_parent_invalidated {
        tracing::trace!(target: "monit", "do_link(parent={:?},child={:?}): parent is invalidated. Invalidate branch.", parent, child);
        invalidate_branch(
            child.clone(),
            block_state_repository,
            &FilterPrehistoric::builder().block_seq_no(BlockSeqNo::default()).build(),
        );
    }

    child.guarded_mut(|e| {
        e.set_parent_block_identifier(parent.block_identifier().clone()).unwrap();
    });
    if is_parent_finalized {
        child.guarded_mut(|e| {
            e.set_has_parent_finalized().unwrap();
        });
    }
}
