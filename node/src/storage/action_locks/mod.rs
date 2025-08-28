use std::sync::Arc;

use aerospike::as_bin;
use aerospike::as_key;
use aerospike::Key;
use aerospike::Value;
use serde::Deserialize;
use serde::Serialize;

use crate::helper::metrics::AEROSPIKE_OBJECT_TYPE_CROSS_ACTION_LOCK;
use crate::storage::AerospikeStore;
use crate::storage::KeyValueStore;
use crate::storage::BIN_BLOB;
use crate::storage::NAMESPACE;

#[derive(Clone)]
pub struct ActionLockStorage {
    store: Option<Arc<AerospikeStore>>,
    set_prefix: String,
}

impl ActionLockStorage {
    pub fn new(store: AerospikeStore, set_prefix: &str) -> Self {
        Self { store: Some(Arc::new(store)), set_prefix: set_prefix.to_string() }
    }

    pub fn as_noop() -> Self {
        Self { store: None, set_prefix: "".to_string() }
    }

    fn message_key(&self, hash: &str) -> Key {
        as_key!(NAMESPACE, &self.set_prefix, hash)
    }

    fn get_store(&self) -> Option<&AerospikeStore> {
        self.store.as_ref().map(AsRef::as_ref)
    }

    pub fn read_blob<T: for<'de> Deserialize<'de>>(&self, path: &str) -> anyhow::Result<Option<T>> {
        let Some(store) = self.get_store() else {
            #[cfg(test)]
            return Ok(None);

            #[cfg(not(test))]
            panic!("Storage instance was not created properly, use new() to create it.");
        };

        let key = &self.message_key(path);

        if let Some(bins) = store.get(key, &[BIN_BLOB], AEROSPIKE_OBJECT_TYPE_CROSS_ACTION_LOCK)? {
            let Some(Value::Blob(blob)) = bins.get(BIN_BLOB) else {
                return Err(anyhow::anyhow!("Failed to read record {path}: missing blob"));
            };
            let data = bincode::deserialize::<T>(blob)?;
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    pub fn write_blob<T: Serialize>(
        &self,
        path: &str,
        data: T,
        until_success: bool,
    ) -> anyhow::Result<()> {
        let Some(store) = self.get_store() else {
            #[cfg(test)]
            return Ok(());
            #[cfg(not(test))]
            panic!("Storage instance was not created properly, use new() to create it.");
        };

        let key = &self.message_key(path);
        let bins = &[as_bin!(BIN_BLOB, bincode::serialize(&data)?)];
        store.put(key, bins, until_success, AEROSPIKE_OBJECT_TYPE_CROSS_ACTION_LOCK)?;
        Ok(())
    }
}
