use super::in_thread_accounts_load::InThreadAccountsLoad;
use crate::bitmask::mask::Bitmask;
use crate::repository::optimistic_state::OptimisticState;
use crate::types::AccountRouting;
use crate::types::AckiNackiBlock;

const WINDOW_SIZE: usize = 100;
pub(super) type Load = usize;

/// An inner structure to calculate an aggregated load
/// on a sliding tail (window) of a particular thread.
#[derive(Debug)]
pub(super) struct AggregatedLoad {
    window: [(Load, InThreadAccountsLoad); WINDOW_SIZE],
    cursor: usize,
    aggregated_value: (Load, InThreadAccountsLoad),
    is_filled: bool,
}

impl AggregatedLoad {
    pub fn new() -> Self {
        Self {
            window: [(0, InThreadAccountsLoad::default()); WINDOW_SIZE],
            cursor: 0,
            aggregated_value: (0, InThreadAccountsLoad::default()),
            is_filled: false,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.is_filled
    }

    pub fn load_value(&self) -> Load {
        self.aggregated_value.0
    }

    pub fn propose_new_bitmask(
        &self,
        current_bitmask: &Bitmask<AccountRouting>,
    ) -> Option<Bitmask<AccountRouting>> {
        self.aggregated_value.1.best_split(current_bitmask)
    }

    pub fn append_from<TOptimisticState>(
        &mut self,
        block: &AckiNackiBlock,
        block_state: &mut TOptimisticState,
    ) where
        TOptimisticState: OptimisticState,
    {
        self.cursor += 1;
        if self.cursor >= WINDOW_SIZE {
            self.is_filled = true;
            self.cursor = 0;
        }
        let prev = self.window[self.cursor];
        self.window[self.cursor].0 = snapshot_load(block_state);
        self.aggregated_value.0 += self.window[self.cursor].0;
        self.aggregated_value.0 -= prev.0;
        self.window[self.cursor].1 = InThreadAccountsLoad::new_from(block, block_state);
        self.aggregated_value.1.add_in_place(&self.window[self.cursor].1);
        self.aggregated_value.1.sub_in_place(&prev.1);
    }
}

fn snapshot_load<T>(block_state: &mut T) -> Load
where
    T: OptimisticState,
{
    block_state.get_internal_message_queue_length()
}
