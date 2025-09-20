use std::sync::Arc;

use aerospike::as_bin;
use aerospike::as_key;
use aerospike::Key;
use aerospike::Value;
use serde::Deserialize;
use serde::Serialize;

use crate::helper::metrics::AEROSPIKE_OBJECT_TYPE_CROSS_REF_DATA;
use crate::storage::mem::MemStore;
use crate::storage::KeyValueStore;
use crate::storage::BIN_BLOB;
use crate::storage::NAMESPACE;

#[derive(Clone)]
pub struct CrossRefStorage {
    store: Arc<dyn KeyValueStore>,
    set_prefix: String,
}

impl CrossRefStorage {
    pub fn new(store: impl KeyValueStore + 'static, set_prefix: &str) -> Self {
        Self { store: Arc::new(store), set_prefix: set_prefix.to_string() }
    }

    pub fn mem() -> CrossRefStorage {
        Self::new(MemStore::new(), "")
    }

    fn message_key(&self, hash: &str) -> Key {
        as_key!(NAMESPACE, &self.set_prefix, hash)
    }

    pub fn read_blob<T: for<'de> Deserialize<'de>>(&self, path: &str) -> anyhow::Result<Option<T>> {
        let key = &self.message_key(path);

        if let Some(bins) =
            self.store.get(key, &[BIN_BLOB].into(), AEROSPIKE_OBJECT_TYPE_CROSS_REF_DATA)?
        {
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
        let key = &self.message_key(path);
        let bins = &[as_bin!(BIN_BLOB, bincode::serialize(&data)?)];
        self.store.put(key, bins, until_success, AEROSPIKE_OBJECT_TYPE_CROSS_REF_DATA)?;
        Ok(())
    }
}
