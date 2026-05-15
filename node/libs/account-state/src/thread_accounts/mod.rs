mod composite;
mod durable;
mod tvm;

use std::collections::HashMap;

pub use composite::ThreadAccountsRepository;
pub use composite::ThreadAccountsState;
pub use composite::ThreadAccountsStateBuilder;
pub use composite::ThreadAccountsStateDiff;
pub use durable::*;
use node_types::AccountRouting;

use crate::ThreadAccount;

#[derive(Debug, Clone)]
pub enum ArchiveOperation {
    UpdateOrInsert(ThreadAccount),
    Remove,
}

pub struct ThreadAccountsStateSplit {
    pub without_branch: ThreadAccountsState,
    pub branch: ThreadAccountsState,
}

#[derive(Debug)]
pub struct ThreadAccountsStateTransition {
    pub new_state: ThreadAccountsState,
    pub diff: ThreadAccountsStateDiff,
    pub account_operations: HashMap<AccountRouting, ArchiveOperation>,
    pub apply_timings: ThreadAccountsApplyTimings,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ThreadAccountsApplyTimings {
    pub durable_apply_ms: u64,
    pub tvm_apply_ms: u64,
}

impl ThreadAccountsStateTransition {
    pub fn apply(&mut self, new_transition: Self) {
        self.new_state = new_transition.new_state;
        self.account_operations.extend(new_transition.account_operations);
        self.diff.durable.accounts.extend(new_transition.diff.durable.accounts);
        self.apply_timings.durable_apply_ms += new_transition.apply_timings.durable_apply_ms;
        self.apply_timings.tvm_apply_ms += new_transition.apply_timings.tvm_apply_ms;
    }
}
