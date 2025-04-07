use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

#[derive(Clone, Debug)]
pub struct CompactedMapKey((BlockIdentifier, BlockSeqNo));

impl CompactedMapKey {
    pub fn block_identifier(&self) -> &BlockIdentifier {
        &self.0 .0
    }

    pub fn block_seq_no(&self) -> &BlockSeqNo {
        &self.0 .1
    }
}

impl Ord for CompactedMapKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.0 .1.cmp(&other.0 .1) {
            std::cmp::Ordering::Equal => BlockIdentifier::compare(&self.0 .0, &other.0 .0),
            e => e,
        }
    }
}

impl From<(BlockIdentifier, BlockSeqNo)> for CompactedMapKey {
    fn from(value: (BlockIdentifier, BlockSeqNo)) -> Self {
        Self(value)
    }
}

impl PartialOrd for CompactedMapKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CompactedMapKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for CompactedMapKey {}
