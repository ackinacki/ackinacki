use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifierPath;
use tvm_block::ShardStateUnsplit;

use crate::thread_accounts::tvm::TvmThreadState;
use crate::thread_accounts::AccountsRepository;
use crate::thread_accounts::DAppAccountMapRepository;
use crate::thread_accounts::ThreadAccountsSplit;
use crate::thread_accounts::ThreadDAppMapRepository;
use crate::CompositeThreadAccountsDiff;
use crate::CompositeThreadAccountsRef;
use crate::DurableThreadAccountsRepository;
use crate::ThreadStateAccount;

pub(crate) struct CompositeThreadAccountsRepositoryInner<
    ThreadDAppsRepo,
    DAppAccountsRepo,
    AccountsRepo,
> where
    ThreadDAppsRepo: ThreadDAppMapRepository,
    DAppAccountsRepo: DAppAccountMapRepository,
    AccountsRepo: AccountsRepository,
{
    pub(crate) apply_to_durable: bool,
    pub(crate) durable:
        DurableThreadAccountsRepository<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>,
}

impl<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>
    CompositeThreadAccountsRepositoryInner<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>
where
    ThreadDAppsRepo: ThreadDAppMapRepository,
    DAppAccountsRepo: DAppAccountMapRepository,
    AccountsRepo: AccountsRepository,
{
    fn state_ref(
        durable: ThreadDAppsRepo::MapRef,
        tvm: TvmThreadState,
    ) -> CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef> {
        CompositeThreadAccountsRef { durable, tvm }
    }

    pub fn state_split(
        &self,
        state: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<ThreadAccountsSplit<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>>>
    {
        let (without_branch, branch) = self.durable.state_split(&state.durable, dapp_id_path)?;
        Ok(ThreadAccountsSplit::<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>> {
            without_branch: Self::state_ref(without_branch, state.tvm.clone()),
            branch: Self::state_ref(
                branch,
                TvmThreadState::with_shard_state(ShardStateUnsplit::default())?,
            ),
        })
    }

    pub fn merge(
        &self,
        a: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
        b: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
    ) -> anyhow::Result<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>> {
        let durable = self.durable.merge(&a.durable, &b.durable)?;
        Ok(Self::state_ref(durable, a.tvm.clone()))
    }

    pub fn state_apply_diff(
        &self,
        state: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
        diff: CompositeThreadAccountsDiff,
    ) -> anyhow::Result<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>> {
        Ok(Self::state_ref(
            self.durable.state_apply_diff(
                &state.tvm.shard_accounts,
                &state.durable,
                &diff.durable,
            )?,
            state.tvm.apply_diff(&diff.tvm)?,
        ))
    }

    pub fn get_state(
        &self,
        block_id: &BlockIdentifier,
        shard_state: ShardStateUnsplit,
    ) -> anyhow::Result<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>> {
        let durable =
            self.durable.get_state(block_id)?.unwrap_or_else(|| ThreadDAppsRepo::new_map());
        let tvm = TvmThreadState::with_shard_state(shard_state)?;
        Ok(Self::state_ref(durable, tvm))
    }

    pub fn set_state(
        &self,
        block_id: &BlockIdentifier,
        state: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
    ) -> anyhow::Result<()> {
        self.durable.set_state(block_id, &state.durable)
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        self.durable.commit()
    }

    pub fn state_account(
        &self,
        state: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadStateAccount>> {
        if let Some(durable) = self.durable.state_account(&state.durable, routing)? {
            // If this is a durable redirect stub, follow it to the real account
            if durable.is_redirect() {
                if let Some(real_dapp_id) = durable.get_dapp_id() {
                    let real_routing = routing.account_id().routing_with(real_dapp_id);
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
                // Could not follow redirect — return it as-is for caller to handle
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
                tracing::trace!(target: "monit", "Account not found: {}", routing_info(routing));
            }
            Ok(tvm)
        }
    }

    pub fn state_iterate_all_accounts(
        &self,
        state: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
        it: impl FnMut(&AccountRouting, ThreadStateAccount) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()> {
        state.tvm.iterate_accounts(it)
    }
}

fn routing_info(routing: &AccountRouting) -> String {
    if *routing.dapp_id() == routing.account_id().use_as_dapp_id() {
        routing.account_id().to_hex_string()
    } else {
        format!("{}:{}", routing.dapp_id().to_hex_string(), routing.account_id().to_hex_string(),)
    }
}

fn found_info(state: &str, routing: &AccountRouting, state_account: &ThreadStateAccount) -> String {
    let hash_str = if state_account.is_redirect() {
        format!("redirect -> {:?}", state_account.get_dapp_id())
    } else {
        state_account
            .account()
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
