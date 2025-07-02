use std::fmt::Debug;
use std::fmt::Formatter;

use thiserror::Error;

use super::repository::BlockStateRepository;
use crate::node::BlockState;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;

#[derive(Error)]
pub enum UnfinalizedAncestorBlocksSelectError {
    #[error("Incomplete history")]
    IncompleteHistory,

    #[error("Recursion limit exceeded")]
    BlockSeqNoCutoff(Vec<BlockState>),

    #[error("Search hit an invalidated ancestor")]
    InvalidatedParent(Vec<BlockState>),

    #[error("Failed to load one of ancestors state")]
    FailedToLoadBlockState,
}

impl Debug for UnfinalizedAncestorBlocksSelectError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use UnfinalizedAncestorBlocksSelectError::*;
        match self {
            IncompleteHistory => write!(f, "Incomplete history"),
            BlockSeqNoCutoff(chain) => write!(f, "Recursion limit exceeded ({chain:?})"),
            InvalidatedParent(chain) => {
                write!(f, "Search hit an invalidated ancestor ({chain:?})")
            }
            FailedToLoadBlockState => write!(f, "Failed to load one of ancestors state"),
        }
    }
}

pub trait UnfinalizedAncestorBlocks {
    fn select_unfinalized_ancestor_blocks(
        &self,
        tail: &BlockState,
        cutoff: BlockSeqNo,
    ) -> anyhow::Result<Vec<BlockState>, UnfinalizedAncestorBlocksSelectError>;
}

impl UnfinalizedAncestorBlocks for BlockStateRepository {
    fn select_unfinalized_ancestor_blocks(
        &self,
        tail: &BlockState,
        cutoff: BlockSeqNo,
    ) -> anyhow::Result<Vec<BlockState>, UnfinalizedAncestorBlocksSelectError> {
        use UnfinalizedAncestorBlocksSelectError::*;
        let mut chain = vec![];
        let mut cursor = tail.clone();
        loop {
            let (is_finalized, parent, block_seq_no) = cursor.guarded(
                |e| -> Result<
                    (bool, BlockIdentifier, BlockSeqNo),
                    UnfinalizedAncestorBlocksSelectError,
                > {
                    if e.is_invalidated() && !e.is_finalized() {
                        chain.push(cursor.clone());
                        return Err(InvalidatedParent(chain.clone()));
                    }
                    let block_seq_no = e.block_seq_no().ok_or(IncompleteHistory)?;
                    let parent_block_identifier =
                        e.parent_block_identifier().clone().ok_or(IncompleteHistory)?;
                    Ok((e.is_finalized(), parent_block_identifier.clone(), block_seq_no))
                },
            )?;
            if is_finalized {
                return Ok(chain.into_iter().rev().collect());
            }
            if block_seq_no < cutoff {
                chain.push(cursor.clone());
                return Err(BlockSeqNoCutoff(chain.clone()));
            }
            chain.push(cursor);
            cursor = self.get(&parent).map_err(|_e| FailedToLoadBlockState)?;
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use crate::types::BlockIdentifier;
    use crate::types::BlockSeqNo;
    use crate::types::ThreadIdentifier;
    use crate::utilities::guarded::GuardedMut;

    #[test]
    fn a_really_old_fork_must_return_cutoff_error() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        let zero_thread = ThreadIdentifier::new(&BlockIdentifier::default(), 0);
        let repo = BlockStateRepository::new(tmp_path);
        let finalized_really_old = BlockIdentifier::from_str(
            "ffa1345a4a9ef86615040207e6f4af9f399d8f3ad4a7fc491e4e985f34c351eb",
        )
        .unwrap();
        let tail_block_id = BlockIdentifier::from_str(
            "0e42bf59d3e8cad9422c9e503b4a950c625e0e662b22f1d35377d4203a3202c8",
        )
        .unwrap();
        let tail = repo.get(&tail_block_id).unwrap();
        tail.guarded_mut(|e| -> anyhow::Result<()> {
            e.set_thread_identifier(zero_thread)?;
            e.set_parent_block_identifier(finalized_really_old.clone())?;
            e.set_block_seq_no(30283.into())?;
            Ok(())
        })
        .unwrap();
        let cutoff = BlockSeqNo::from(40664);
        repo.get(&finalized_really_old)
            .unwrap()
            .guarded_mut(|e| -> anyhow::Result<()> {
                e.set_finalized()?;
                Ok(())
            })
            .unwrap();
        match repo.select_unfinalized_ancestor_blocks(&tail, cutoff) {
            Err(UnfinalizedAncestorBlocksSelectError::BlockSeqNoCutoff(chain)) => {
                let id_chain: Vec<_> = chain.iter().map(|e| e.block_identifier().clone()).collect();
                assert_eq!(&id_chain, &(vec![tail_block_id]), "Unexpected chain: {chain:?}");
            }
            e => {
                panic!("unexpected result: {e:?}");
            }
        }
    }
}
