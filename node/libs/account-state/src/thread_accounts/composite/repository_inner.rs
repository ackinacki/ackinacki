use std::collections::HashMap;

use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadIdentifier;
use tvm_block::ShardStateUnsplit;

use crate::thread_accounts::archive::control::ThreadControlState;
use crate::thread_accounts::archive::update::ThreadSnapshot;
use crate::thread_accounts::repository::DurableThreadAccountsState;
use crate::thread_accounts::tvm::TvmThreadAccountsState;
use crate::thread_accounts::ArchiveOperation;
use crate::thread_accounts::ThreadAccountsStateSplit;
use crate::DurableThreadAccountsRepository;
use crate::ThreadAccount;
use crate::ThreadAccountsApplyTimings;
use crate::ThreadAccountsState;
use crate::ThreadAccountsStateDiff;
use crate::ThreadAccountsStateTransition;

pub(crate) struct ThreadAccountsRepositoryInner {
    pub(crate) apply_to_durable: bool,
    pub(crate) durable: DurableThreadAccountsRepository,
}

impl Drop for ThreadAccountsRepositoryInner {
    fn drop(&mut self) {
        self.durable.shutdown();
    }
}

impl ThreadAccountsRepositoryInner {
    fn state_ref(
        durable: DurableThreadAccountsState,
        tvm: TvmThreadAccountsState,
    ) -> ThreadAccountsState {
        ThreadAccountsState::from_parts(durable, tvm)
    }

    pub fn state_split(
        &self,
        state: &ThreadAccountsState,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<ThreadAccountsStateSplit> {
        let (without_branch, branch) = self.durable.state_split(&state.durable, dapp_id_path)?;
        Ok(ThreadAccountsStateSplit {
            without_branch: ThreadAccountsState::from_parts(without_branch, state.tvm.clone()),
            branch: Self::state_ref(
                branch,
                TvmThreadAccountsState::with_shard_state(ShardStateUnsplit::default())?,
            ),
        })
    }

    pub fn merge(
        &self,
        a: &ThreadAccountsState,
        b: &ThreadAccountsState,
    ) -> anyhow::Result<ThreadAccountsState> {
        let durable = self.durable.merge(&a.durable, &b.durable)?;
        Ok(Self::state_ref(durable, a.tvm.clone()))
    }

    pub fn state_apply_diff(
        &self,
        state: &ThreadAccountsState,
        diff: ThreadAccountsStateDiff,
        thread_id: ThreadIdentifier,
        block_height: u64,
    ) -> anyhow::Result<ThreadAccountsStateTransition> {
        let durable_apply_started_at = std::time::Instant::now();
        let new_durable_result = self.durable.state_apply_diff(
            &thread_id,
            block_height,
            &state.tvm.shard_accounts,
            &state.durable,
            &diff.durable,
        );
        let durable_apply_ms = durable_apply_started_at.elapsed().as_millis() as u64;
        let (new_durable, account_operations) = new_durable_result?;

        let tvm_apply_started_at = std::time::Instant::now();
        let new_tvm_result = state.tvm.apply_diff(&diff.tvm);
        let tvm_apply_ms = tvm_apply_started_at.elapsed().as_millis() as u64;
        let new_tvm = new_tvm_result?;

        let new_state = Self::state_ref(new_durable, new_tvm);
        Ok(ThreadAccountsStateTransition {
            new_state,
            diff,
            account_operations,
            apply_timings: ThreadAccountsApplyTimings { durable_apply_ms, tvm_apply_ms },
        })
    }

    pub fn state_load(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        shard_state: ShardStateUnsplit,
    ) -> anyhow::Result<ThreadAccountsState> {
        let durable = self
            .durable
            .state_load(block_id, thread_id)?
            .unwrap_or_else(DurableThreadAccountsRepository::new_state);
        let tvm = TvmThreadAccountsState::with_shard_state(shard_state)?;
        Ok(Self::state_ref(durable, tvm))
    }

    pub fn state_save(
        &self,
        block_id: &BlockIdentifier,
        state: &ThreadAccountsState,
    ) -> anyhow::Result<()> {
        self.durable.state_save(&state.durable, block_id)
    }

    pub fn state_account(
        &self,
        state: &ThreadAccountsState,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        if let Some(durable) = self.durable.state_account(&state.durable, routing)? {
            // If this is a durable redirect stub, follow it to the real account
            if durable.is_redirect() {
                if let Some(real_dapp_id) = durable.get_dapp_id() {
                    let real_routing = routing.account_id().routing(real_dapp_id);
                    if let Some(real_account) =
                        self.durable.state_account(&state.durable, &real_routing)?
                    {
                        tracing::trace!(
                            target: "monit", "{}",
                            found_info("durable (via redirect)", &real_routing, &real_account)
                        );
                        return Ok(Some(real_account));
                    }
                }
                // Could not follow redirect — return it as-is for the caller to handle
                tracing::trace!(
                    target: "monit",
                    "Durable redirect stub at {} could not be followed",
                    routing_info(routing)
                );
                return Ok(Some(durable));
            }
            tracing::trace!(target: "monit", "{}", found_info("durable", routing, &durable));
            Ok(Some(durable))
        } else {
            let tvm = state.tvm.account(routing)?;
            if let Some(tvm) = tvm.as_ref() {
                tracing::trace!(target: "monit", "{}", found_info("TVM", routing, tvm));
            } else {
                // Note: we ended up being here
                tracing::trace!(target: "monit", "Account not found: {}", routing_info(routing));
            }
            Ok(tvm)
        }
    }

    pub fn finalize_thread_transition(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        block_height: u64,
        state: &ThreadAccountsState,
        account_operations: HashMap<AccountRouting, ArchiveOperation>,
    ) -> anyhow::Result<()> {
        self.durable.finalize_thread_transition(
            block_id,
            thread_id,
            block_height,
            &state.durable,
            account_operations,
        )
    }

    #[allow(deprecated)]
    pub fn flush_accumulator(&self) -> crate::thread_accounts::FlushGuard<'_> {
        self.durable.flush_accumulator()
    }

    pub fn wait_for_drain(&self, timeout: std::time::Duration) -> bool {
        self.durable.wait_for_drain(timeout)
    }

    pub fn flush_pending_and_wait_for_drain(&self, timeout: std::time::Duration) -> bool {
        self.durable.flush_pending_and_wait_for_drain(timeout)
    }

    pub fn request_snapshot_pin(
        &self,
        anchor: crate::thread_accounts::AnchorBlockRef,
    ) -> crate::thread_accounts::PinRequestGuard {
        self.durable.request_snapshot_pin(anchor)
    }

    pub fn cancel_snapshot_pin(
        &self,
        expected_thread: node_types::ThreadIdentifier,
        expected_block: node_types::BlockIdentifier,
    ) {
        self.durable.cancel_snapshot_pin(expected_thread, expected_block);
    }

    pub fn acquire_snapshot_pin(
        &self,
        expected_thread: node_types::ThreadIdentifier,
        expected_block: node_types::BlockIdentifier,
        timeout: std::time::Duration,
    ) -> Option<crate::thread_accounts::PinHandle> {
        self.durable.acquire_snapshot_pin(expected_thread, expected_block, timeout)
    }

    pub fn get_archive_thread_control_states(&self) -> anyhow::Result<Vec<ThreadControlState>> {
        self.durable.get_archive_thread_control_states()
    }

    pub fn reset_archive(&self) -> anyhow::Result<()> {
        self.durable.reset_archive()
    }

    pub fn reset_accumulator(&self) -> anyhow::Result<()> {
        self.durable.reset_accumulator()
    }

    pub fn thread_emerge(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: &BlockIdentifier,
        state: &ThreadAccountsState,
        account_operations: HashMap<AccountRouting, ArchiveOperation>,
    ) -> anyhow::Result<()> {
        self.durable.thread_emerge(thread_id, initial_block_id, &state.durable, account_operations)
    }

    pub fn thread_collapse(&self, thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        self.durable.thread_collapse(thread_id)
    }

    pub fn thread_init(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: &BlockIdentifier,
        snapshot: &ThreadSnapshot,
    ) -> anyhow::Result<()> {
        self.durable.thread_init(thread_id, initial_block_id, snapshot)
    }

    pub fn ensure_thread(
        &self,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        overwrite: bool,
    ) -> anyhow::Result<()> {
        self.durable.ensure_thread(thread_id, block_id, overwrite)
    }

    pub fn state_iterate_tvm_accounts(
        &self,
        state: &ThreadAccountsState,
        it: impl FnMut(&AccountIdentifier, ThreadAccount) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()> {
        state.tvm.iterate_accounts(it)
    }
}

fn routing_info(routing: &AccountRouting) -> String {
    if *routing.dapp_id() == routing.account_id().redirect_dapp_id() {
        routing.account_id().to_hex_string()
    } else {
        format!("{}:{}", routing.dapp_id().to_hex_string(), routing.account_id().to_hex_string(),)
    }
}

fn found_info(state: &str, routing: &AccountRouting, state_account: &ThreadAccount) -> String {
    let hash_str = if state_account.is_redirect() {
        format!("redirect -> {:?}", state_account.get_dapp_id())
    } else {
        state_account
            .vm_account()
            .map(|a| a.hash().to_hex_string())
            .unwrap_or_else(|_| "<error>".to_string())
    };
    let info =
        format!("Account found in {state} state: {} hash: {hash_str}", routing_info(routing),);
    let dapp_id = state_account.get_dapp_id();
    if dapp_id == Some(*routing.dapp_id()) {
        info
    } else {
        format!("{info} dapp_id: {dapp_id:?}")
    }
}
