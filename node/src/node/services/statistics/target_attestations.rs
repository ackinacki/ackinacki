use crate::types::BlockIdentifier;

#[allow(dead_code)]
#[derive(PartialEq)]
pub struct BlockStatistics {
    window_size: usize,
}

impl BlockStatistics {
    fn _next(
        // Intentionally consuming
        self,
        _block_identifier: BlockIdentifier,
        // Must be propagated from the common section.
        _new_window_size: Option<usize>,
    ) -> Self {
        // This is a stub implementation.
        // Always assumes 2/3.
        self
    }

    fn _expected_number_of_attestations_for_descendant_block(
        &self,
        total_number_of_block_keepers_for_descendant_block: usize,
    ) -> usize {
        usize::div_ceil(2 * total_number_of_block_keepers_for_descendant_block, 3)
    }
}
