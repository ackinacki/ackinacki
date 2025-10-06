use std::num::NonZeroUsize;

use serde::Deserialize;
use serde::Serialize;

pub const BLOCK_STATISTICS_INITIAL_WINDOW_SIZE: usize = 15;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct BlockStatistics {
    // window_size: usize, <- Note: stats.len is enough
    stats: Vec<usize>,
    cursor: usize,
    // dynamic parameter \beta
    median_descendants_chain_length_to_meet_threshold: usize,
}

impl BlockStatistics {
    pub fn zero(
        initial_window_size: NonZeroUsize,
        preset_median_descendants_chain_length_to_meet_threshold: NonZeroUsize,
    ) -> Self {
        let mut stats = Vec::with_capacity(initial_window_size.into());
        for _i in 0..initial_window_size.into() {
            stats.push(preset_median_descendants_chain_length_to_meet_threshold.into());
        }
        Self {
            stats,
            cursor: 0_usize,
            median_descendants_chain_length_to_meet_threshold:
                preset_median_descendants_chain_length_to_meet_threshold.into(),
        }
    }

    pub fn median_descendants_chain_length_to_meet_threshold(&self) -> usize {
        self.median_descendants_chain_length_to_meet_threshold
    }

    pub fn next(
        // Intentionally consuming
        mut self,
        finalization_distances_sorted_by_seq_no: &[usize],
        // It is possible to override window size.
        // Must be propagated from the common section.
        new_window_size: Option<usize>,
    ) -> Self {
        if let Some(resize) = new_window_size {
            let resize = if resize != 0 { resize } else { 1 };
            // TODO: note that this is not correct in terms of it will delete a random chunk.
            // to make it "correct" we need to delete the oldest (n items after self.cursor)
            self.stats.resize(resize, self.median_descendants_chain_length_to_meet_threshold);
            self.cursor %= self.stats.len();
        }

        for e in finalization_distances_sorted_by_seq_no.iter() {
            self.stats[self.cursor] = *e;
            self.cursor = (self.cursor + 1) % self.stats.len();
        }

        let mut copy = self.stats.clone();
        let (_, median, _) = copy.select_nth_unstable(self.stats.len() / 2);
        self.median_descendants_chain_length_to_meet_threshold =
            if *median > 0 { *median } else { 1 };
        self
    }
}
