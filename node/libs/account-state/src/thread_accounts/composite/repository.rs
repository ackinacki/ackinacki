use std::collections::HashMap;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use node_types::ThreadIdentifier;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_block::ShardAccounts;
use tvm_block::ShardStateUnsplit;
use tvm_types::Cell;
use tvm_types::UsageTree;

use crate::live_metrics::LiveThreadAccountsStateCounter;
use crate::thread_accounts::archive::control::ThreadControlState;
use crate::thread_accounts::archive::update::ThreadSnapshot;
use crate::thread_accounts::composite::builder::ThreadAccountsStateBuilder;
use crate::thread_accounts::composite::repository_inner::ThreadAccountsRepositoryInner;
use crate::thread_accounts::composite::DEFAULT_APPLY_TO_DURABLE;
use crate::thread_accounts::durable::archive::config::ArchiveStoreConfig;
use crate::thread_accounts::durable::archive::store::ArchiveStateStore;
use crate::thread_accounts::durable::kv_store::aerospike::AerospikeKVConfig;
use crate::thread_accounts::durable::kv_store::aerospike::AerospikeKVStore;
use crate::thread_accounts::durable::kv_store::KVStore;
use crate::thread_accounts::durable::DurableThreadAccountsStateDiff;
use crate::thread_accounts::kv_store::aerospike::AerospikeAccountsCacheStat;
use crate::thread_accounts::repository::DurableThreadAccountsState;
use crate::thread_accounts::tvm::TvmThreadAccountsState;
use crate::thread_accounts::tvm::TvmThreadAccountsStateDiff;
use crate::thread_accounts::ArchiveOperation;
use crate::thread_accounts::ThreadAccountsStateSplit;
use crate::DurableStateSnapshot;
use crate::DurableThreadAccountsIter;
use crate::DurableThreadAccountsRepository;
use crate::StateAccountsMetrics;
use crate::ThreadAccount;
use crate::ThreadAccountsStateTransition;

#[derive(Debug)]
pub struct ThreadAccountsStateDiff {
    pub durable: DurableThreadAccountsStateDiff,
    pub tvm: TvmThreadAccountsStateDiff,
}

#[derive(Clone, Debug)]
pub struct ThreadAccountsState {
    pub durable: DurableThreadAccountsState,
    pub tvm: TvmThreadAccountsState,
    pub(crate) _live_counter: LiveThreadAccountsStateCounter,
}

impl ThreadAccountsState {
    pub fn from_parts(durable: DurableThreadAccountsState, tvm: TvmThreadAccountsState) -> Self {
        Self { durable, tvm, _live_counter: LiveThreadAccountsStateCounter::new() }
    }

    pub fn new(
        durable: DurableThreadAccountsState,
        tvm_shard_state: ShardStateUnsplit,
        tvm_shard_accounts: ShardAccounts,
    ) -> Self {
        Self::from_parts(
            durable,
            TvmThreadAccountsState {
                shard_state: tvm_shard_state,
                shard_accounts: tvm_shard_accounts,
            },
        )
    }

    pub fn with_tvm_usage_tree(&self) -> anyhow::Result<(Self, UsageTree)> {
        let usage_tree = UsageTree::with_params(self.tvm_cell()?, true);
        let shard_state = ShardStateUnsplit::construct_from_cell(usage_tree.root_cell())
            .map_err(|err| anyhow::format_err!("Failed to deserialize shard state: {}", err))?;
        let state = Self::from_parts(
            self.durable.clone(),
            TvmThreadAccountsState::with_shard_state(shard_state)?,
        );
        Ok((state, usage_tree))
    }

    pub fn tvm_cell(&self) -> anyhow::Result<Cell> {
        self.tvm
            .shard_state
            .serialize()
            .map_err(|err| anyhow::anyhow!("Failed to serialize shard state: {err}"))
    }

    pub fn with_durable_state(
        &self,
        new_durable_state: DurableThreadAccountsState,
    ) -> ThreadAccountsState {
        Self::from_parts(new_durable_state, self.tvm.clone())
    }
}

// Composite thread accounts state
// Includes two parts:
// - legacy TVM shard state;
// - new durable state v2.
//
#[derive(Clone)]
pub struct ThreadAccountsRepository(pub(crate) Arc<ThreadAccountsRepositoryInner>);

impl ThreadAccountsRepository {
    pub fn builder(durable_path: impl Into<PathBuf>) -> ThreadAccountsRepositoryBuilder {
        ThreadAccountsRepositoryBuilder {
            durable_path: durable_path.into(),
            apply_to_durable: DEFAULT_APPLY_TO_DURABLE,
            aerospike: None,
            metrics: None,
        }
    }

    pub fn new(
        durable_path: PathBuf,
        accounts: ArchiveStateStore,
        apply_to_durable: bool,
        metrics: Option<StateAccountsMetrics>,
    ) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(ThreadAccountsRepositoryInner {
            durable: DurableThreadAccountsRepository::new(durable_path, accounts, metrics)?,
            apply_to_durable,
        })))
    }

    pub fn start_archive_update_service(&self) -> anyhow::Result<()> {
        self.0.durable.start_archive_update_service()
    }

    /// Return a human-readable summary of the archive state store.
    pub fn archive_summary(&self) -> String {
        self.0.durable.archive_summary()
    }

    pub fn aerospike_cache_stat(&self) -> Option<AerospikeAccountsCacheStat> {
        self.0.durable.aerospike_cache_stat()
    }

    pub fn state_accounts_metrics(&self) -> Option<StateAccountsMetrics> {
        self.0.durable.state_accounts_metrics()
    }

    pub fn durable_map_repo(&self) -> &DurableThreadAccountsRepository {
        &self.0.durable
    }

    pub fn durable_map_iter<'a>(
        &'a self,
        state: &'a DurableThreadAccountsState,
    ) -> DurableThreadAccountsIter<'a> {
        self.0.durable.map_repo().map_iter(state.map())
    }

    pub fn state_with_tvm_cell_and_empty_durable_state(
        cell: Cell,
    ) -> anyhow::Result<ThreadAccountsState> {
        let shard_state = ShardStateUnsplit::construct_from_cell(cell)
            .map_err(|err| anyhow::format_err!("Failed to deserialize shard state: {}", err))?;
        Ok(ThreadAccountsState::from_parts(
            DurableThreadAccountsRepository::new_state(),
            TvmThreadAccountsState::with_shard_state(shard_state)?,
        ))
    }

    pub fn state_to_tvm_cell(state: &ThreadAccountsState) -> anyhow::Result<Cell> {
        state
            .tvm
            .shard_state
            .serialize()
            .map_err(|err| anyhow::anyhow!("Failed to serialize shard state: {err}"))
    }

    pub fn export_durable_snapshot(
        &self,
        state: &ThreadAccountsState,
    ) -> anyhow::Result<DurableStateSnapshot> {
        self.0.durable.export_durable_snapshot(&state.durable)
    }

    pub fn import_durable_snapshot(
        &self,
        snapshot: DurableStateSnapshot,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<DurableThreadAccountsState> {
        self.0.durable.import_durable_snapshot(snapshot, thread_id, block_id)
    }

    pub fn export_durable_snapshot_to_writer<W: Write + Seek>(
        &self,
        state: &ThreadAccountsState,
        writer: &mut W,
    ) -> anyhow::Result<()> {
        self.0.durable.export_durable_snapshot_to_writer(&state.durable, writer)
    }

    pub fn import_durable_snapshot_from_reader<R: Read>(
        &self,
        reader: &mut R,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<DurableThreadAccountsState> {
        self.0.durable.import_durable_snapshot_from_reader(reader, thread_id, block_id)
    }

    pub fn new_state() -> ThreadAccountsState {
        ThreadAccountsState::from_parts(
            DurableThreadAccountsRepository::new_state(),
            TvmThreadAccountsState::with_shard_state(ShardStateUnsplit::default()).unwrap(),
        )
    }

    pub fn state_builder(
        &self,
        thread_id: &ThreadIdentifier,
        block_height: u64,
        original: &ThreadAccountsState,
    ) -> ThreadAccountsStateBuilder {
        ThreadAccountsStateBuilder::new(self.0.clone(), *thread_id, block_height, original.clone())
    }

    pub fn state_split(
        &self,
        state: &ThreadAccountsState,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<ThreadAccountsStateSplit> {
        self.0.state_split(state, dapp_id_path)
    }

    pub fn merge(
        &self,
        a: &ThreadAccountsState,
        b: &ThreadAccountsState,
    ) -> anyhow::Result<ThreadAccountsState> {
        self.0.merge(a, b)
    }

    pub fn state_apply_diff(
        &self,
        prev_state: &ThreadAccountsState,
        diff: ThreadAccountsStateDiff,
        thread_id: ThreadIdentifier,
        block_height: u64,
    ) -> anyhow::Result<ThreadAccountsStateTransition> {
        self.0.state_apply_diff(prev_state, diff, thread_id, block_height)
    }

    pub fn state_load(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        tvm: ShardStateUnsplit,
    ) -> anyhow::Result<ThreadAccountsState> {
        self.0.state_load(block_id, thread_id, tvm)
    }

    pub fn finalize_thread_transition(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        block_height: u64,
        state: &ThreadAccountsState,
        account_operations: HashMap<AccountRouting, ArchiveOperation>,
    ) -> anyhow::Result<()> {
        self.0.finalize_thread_transition(
            block_id,
            thread_id,
            block_height,
            state,
            account_operations,
        )
    }

    #[deprecated(note = "use `acquire_snapshot_pin` for snapshot consistency \
        or `wait_for_drain` for visibility")]
    #[allow(deprecated)]
    pub fn flush_accumulator(&self) -> crate::thread_accounts::FlushGuard<'_> {
        self.0.flush_accumulator()
    }

    pub fn wait_for_drain(&self, timeout: std::time::Duration) -> bool {
        self.0.wait_for_drain(timeout)
    }

    pub fn flush_pending_and_wait_for_drain(&self, timeout: std::time::Duration) -> bool {
        self.0.flush_pending_and_wait_for_drain(timeout)
    }

    pub fn request_snapshot_pin(
        &self,
        anchor: crate::thread_accounts::AnchorBlockRef,
    ) -> crate::thread_accounts::PinRequestGuard {
        self.0.request_snapshot_pin(anchor)
    }

    pub fn cancel_snapshot_pin(
        &self,
        expected_thread: ThreadIdentifier,
        expected_block: BlockIdentifier,
    ) {
        self.0.cancel_snapshot_pin(expected_thread, expected_block)
    }

    pub fn acquire_snapshot_pin(
        &self,
        expected_thread: ThreadIdentifier,
        expected_block: BlockIdentifier,
        timeout: std::time::Duration,
    ) -> Option<crate::thread_accounts::PinHandle> {
        self.0.acquire_snapshot_pin(expected_thread, expected_block, timeout)
    }

    pub fn get_archive_thread_control_states(&self) -> anyhow::Result<Vec<ThreadControlState>> {
        self.0.get_archive_thread_control_states()
    }

    pub fn reset_archive(&self) -> anyhow::Result<()> {
        self.0.reset_archive()
    }

    pub fn reset_accumulator(&self) -> anyhow::Result<()> {
        self.0.reset_accumulator()
    }

    pub fn finalize_thread_emerge(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: &BlockIdentifier,
        state: &ThreadAccountsState,
        account_operations: HashMap<AccountRouting, ArchiveOperation>,
    ) -> anyhow::Result<()> {
        tracing::trace!(target: "monit", "thread_emerge: thread_id={:?}, initial_block_id={:?}", thread_id, initial_block_id);
        self.0.thread_emerge(thread_id, initial_block_id, state, account_operations)
    }

    pub fn thread_collapse(&self, thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        self.0.thread_collapse(thread_id)
    }

    pub fn thread_init(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: &BlockIdentifier,
        snapshot: &ThreadSnapshot,
    ) -> anyhow::Result<()> {
        self.0.thread_init(thread_id, initial_block_id, snapshot)
    }

    /// Ensure a thread exists in the archive store (idempotent).
    pub fn ensure_thread(
        &self,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        overwrite: bool,
    ) -> anyhow::Result<()> {
        tracing::trace!(target: "monit", "ensure_thread: thread_id={:?}, block_id={:?}", thread_id, block_id);
        self.0.ensure_thread(thread_id, block_id, overwrite)
    }

    pub fn state_save(
        &self,
        block_id: &BlockIdentifier,
        state: &ThreadAccountsState,
    ) -> anyhow::Result<()> {
        self.0.state_save(block_id, state)
    }

    pub fn state_hash(&self, state: &ThreadAccountsState) -> ThreadAccountsHash {
        state.tvm.hash()
    }

    pub fn state_seq_no(&self, state: &ThreadAccountsState) -> u32 {
        state.tvm.seq_no()
    }

    pub fn state_global_id(&self, state: &ThreadAccountsState) -> i32 {
        state.tvm.global_id()
    }

    pub fn state_account(
        &self,
        state: &ThreadAccountsState,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        self.0.state_account(state, routing)
    }

    pub fn state_iterate_tvm_accounts(
        &self,
        state: &ThreadAccountsState,
        it: impl FnMut(&AccountIdentifier, ThreadAccount) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()> {
        self.0.state_iterate_tvm_accounts(state, it)
    }
}

pub const DEFAULT_NUM_WRITE_THREADS: usize = 48;

enum AerospikeBackendKind {
    V1,
    V2,
}

struct AerospikeBuilderConfig {
    kv: AerospikeKVConfig,
    base_set: String,
    backend: AerospikeBackendKind,
}

pub struct ThreadAccountsRepositoryBuilder {
    durable_path: PathBuf,
    apply_to_durable: bool,
    aerospike: Option<AerospikeBuilderConfig>,
    metrics: Option<StateAccountsMetrics>,
}

impl ThreadAccountsRepositoryBuilder {
    pub fn build(self) -> anyhow::Result<ThreadAccountsRepository> {
        let accounts = if let Some(config) = self.aerospike {
            let store_config = ArchiveStoreConfig {
                aerospike_address: Some(config.kv.address.clone()),
                node_id: config.base_set.clone(),
                write_parallelism: config.kv.num_write_threads,
            };
            let kv = match config.backend {
                AerospikeBackendKind::V1 => KVStore::Aerospike(AerospikeKVStore::new(config.kv)?),
                AerospikeBackendKind::V2 => {
                    KVStore::Aerospike(AerospikeKVStore::new_v2(config.kv)?)
                }
            };
            ArchiveStateStore::new(kv, store_config).map_err(|e| anyhow::anyhow!("{e}"))?
        } else {
            ArchiveStateStore::in_memory()
        };
        ThreadAccountsRepository::new(
            self.durable_path,
            accounts,
            self.apply_to_durable,
            self.metrics,
        )
    }

    pub fn set_apply_to_durable(mut self, apply_to_durable: bool) -> Self {
        self.apply_to_durable = apply_to_durable;
        self
    }

    pub fn set_metrics(mut self, metrics: Option<StateAccountsMetrics>) -> Self {
        self.metrics = metrics.clone();
        if let Some(config) = &mut self.aerospike {
            config.kv.metrics = metrics;
        }
        self
    }

    pub fn set_accounts_aerospike_store(
        mut self,
        address: impl AsRef<str>,
        namespace: impl AsRef<str>,
        set: impl AsRef<str>,
    ) -> Self {
        self.aerospike = Some(AerospikeBuilderConfig {
            kv: AerospikeKVConfig {
                address: address.as_ref().to_string(),
                namespace: namespace.as_ref().to_string(),
                num_write_threads: DEFAULT_NUM_WRITE_THREADS,
                metrics: self.metrics.clone(),
            },
            base_set: set.as_ref().to_string(),
            backend: AerospikeBackendKind::V1,
        });
        self
    }

    pub fn set_accounts_aerospike2_store(
        mut self,
        address: impl AsRef<str>,
        namespace: impl AsRef<str>,
        set: impl AsRef<str>,
    ) -> Self {
        self.aerospike = Some(AerospikeBuilderConfig {
            kv: AerospikeKVConfig {
                address: address.as_ref().to_string(),
                namespace: namespace.as_ref().to_string(),
                num_write_threads: DEFAULT_NUM_WRITE_THREADS,
                metrics: self.metrics.clone(),
            },
            base_set: set.as_ref().to_string(),
            backend: AerospikeBackendKind::V2,
        });
        self
    }

    pub fn set_accounts_aerospike_parallel_batch_mode(mut self, num_threads: usize) -> Self {
        if let Some(config) = &mut self.aerospike {
            config.kv.num_write_threads = num_threads;
        }
        self
    }

    pub fn set_accounts_aerospike_sequential_batch_mode(mut self) -> Self {
        if let Some(config) = &mut self.aerospike {
            config.kv.num_write_threads = 0;
        }
        self
    }
}
