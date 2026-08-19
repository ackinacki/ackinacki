mod aerospike_mock;
mod aerospike_v1;

pub mod aerospike;
pub mod in_memory;

use self::aerospike::AerospikeKVStore;
use self::in_memory::InMemoryKVStore;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KVRecord {
    pub key: Vec<u8>,
    /// Aerospike generation counter. Incremented on each write.
    /// Used for CAS (compare-and-swap) when `cas: true` in put().
    pub generation: u64,
    /// Optional epoch tag for garbage collection.
    pub data_epoch: Option<u32>,
    /// Main payload.
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub enum KVStore {
    Aerospike(AerospikeKVStore),
    InMemory(InMemoryKVStore),
}

impl KVStore {
    /// Read records by keys from a set.
    /// Returns one Option<KVRecord> per input key, in the same order.
    /// None means the key does not exist.
    pub fn get(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<KVRecord>>> {
        match self {
            Self::Aerospike(s) => s.get(set, keys),
            Self::InMemory(s) => s.get(set, keys),
        }
    }

    /// Write records to a set.
    /// If cas is true, only write if the record's generation matches
    /// the current generation in the store (compare-and-swap).
    pub fn put(&self, set: &str, records: Vec<KVRecord>, cas: bool) -> anyhow::Result<()> {
        match self {
            Self::Aerospike(s) => s.put(set, records, cas),
            Self::InMemory(s) => s.put(set, records, cas),
        }
    }

    /// Fast non-CAS logical-record write path for archive account bodies.
    /// Skips the old-head read and stale chunk cleanup performed by `put`.
    /// Reads remain correct because the current head record's chunk count
    /// controls which chunk keys are loaded; old extra chunks may remain until
    /// later epoch/set cleanup.
    pub fn fast_put_without_cas_and_stale_chunk_cleanup(
        &self,
        set: &str,
        records: Vec<KVRecord>,
    ) -> anyhow::Result<()> {
        match self {
            Self::Aerospike(s) => s.fast_put_without_cas_and_stale_chunk_cleanup(set, records),
            Self::InMemory(s) => s.put(set, records, false),
        }
    }

    /// Delete records by keys from a set.
    pub fn delete(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        match self {
            Self::Aerospike(s) => s.delete(set, keys),
            Self::InMemory(s) => s.delete(set, keys),
        }
    }

    /// Server-side wipe of every record in a set. Used by `archive.reset()`
    /// to ensure a stale account body from before a snapshot import can
    /// never be served by a subsequent read.
    pub fn truncate_set(&self, set: &str) -> anyhow::Result<()> {
        match self {
            Self::Aerospike(s) => s.truncate_set(set),
            Self::InMemory(s) => s.truncate_set(set),
        }
    }

    pub fn enumerate(
        &self,
        set: &str,
        on_record: &mut dyn FnMut(KVRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.enumerate_checked(set, &|| false, on_record)
    }

    pub fn enumerate_checked(
        &self,
        set: &str,
        should_cancel: &dyn Fn() -> bool,
        on_record: &mut dyn FnMut(KVRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        match self {
            Self::Aerospike(s) => s.enumerate_checked(set, should_cancel, on_record),
            Self::InMemory(s) => s.enumerate_checked(set, should_cancel, on_record),
        }
    }
}
