#[cfg(debug_assertions)]
use aerospike::Bin;
use aerospike::Key;

use crate::helper::metrics::BlockProductionMetrics;
use crate::storage::mem::MemStore;
use crate::storage::AerospikeStore;
use crate::storage::BatchGet;
use crate::storage::BinMap;
use crate::storage::KeyValueStore;
use crate::storage::SplitValueStore;

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum DurableStore {
    Mem(MemStore),
    Aerospike(SplitValueStore<AerospikeStore>),
}

impl DurableStore {
    pub fn mem() -> DurableStore {
        Self::Mem(MemStore::new())
    }

    pub fn aerospike(
        socket_address: String,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<Self> {
        let aerospike = AerospikeStore::new(socket_address, metrics)?;
        Ok(Self::Aerospike(SplitValueStore::new(aerospike, 1024 * 1020)))
    }
}

impl KeyValueStore for DurableStore {
    fn get(&self, key: &Key, bins: &[&str], label: &'static str) -> anyhow::Result<Option<BinMap>> {
        match self {
            DurableStore::Mem(store) => store.get(key, bins, label),
            DurableStore::Aerospike(store) => store.get(key, bins, label),
        }
    }

    fn put(&self, key: &Key, bins: &[Bin], until: bool, label: &'static str) -> anyhow::Result<()> {
        match self {
            DurableStore::Mem(store) => store.put(key, bins, until, label),
            DurableStore::Aerospike(store) => store.put(key, bins, until, label),
        }
    }

    fn batch_get(&self, reads: Vec<BatchGet>) -> anyhow::Result<Vec<Option<BinMap>>> {
        match self {
            DurableStore::Mem(store) => store.batch_get(reads),
            DurableStore::Aerospike(store) => store.batch_get(reads),
        }
    }

    #[cfg(debug_assertions)]
    fn db_reads(&self) -> usize {
        match self {
            DurableStore::Mem(store) => store.db_reads(),
            DurableStore::Aerospike(store) => store.db_reads(),
        }
    }

    #[cfg(debug_assertions)]
    fn db_writes(&self) -> usize {
        match self {
            DurableStore::Mem(store) => store.db_writes(),
            DurableStore::Aerospike(store) => store.db_writes(),
        }
    }
}
