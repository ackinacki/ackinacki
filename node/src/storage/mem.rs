use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use aerospike::Bin;
use aerospike::Bins;
use aerospike::Key;
use aerospike::Value;

use crate::storage::KeyValueStore;
use crate::storage::ValueMap;

#[derive(Clone)]
pub struct MemStore {
    inner: Arc<parking_lot::Mutex<HashMap<String, ValueMap>>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self { inner: Arc::new(parking_lot::Mutex::new(HashMap::new())) }
    }
}

fn key_to_string(key: &Key) -> String {
    format!(
        "{}.{}.{}",
        key.namespace,
        key.set_name,
        key.user_key.as_ref().map(|x| x.to_string()).unwrap_or_default()
    )
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Blob(blob) => format!("[{}]", blob.len()),
        _ => value.to_string(),
    }
}

impl Debug for MemStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemStore\n")?;
        for (key, bins) in self.inner.lock().iter() {
            write!(f, "  {key}:")?;
            for (name, value) in bins {
                write!(f, " {name}={}", value_to_string(value))?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStore for MemStore {
    fn get(
        &self,
        key: &Key,
        _values: &Bins,
        _label: &'static str,
    ) -> anyhow::Result<Option<ValueMap>> {
        Ok(self.inner.lock().get(&key_to_string(key)).cloned())
    }

    fn put(
        &self,
        key: &Key,
        values: &[Bin],
        _until: bool,
        _label: &'static str,
    ) -> anyhow::Result<()> {
        self.inner.lock().insert(
            key_to_string(key),
            ValueMap::from_iter(values.iter().map(|x| (x.name.to_string(), x.value.clone()))),
        );
        Ok(())
    }

    #[cfg(debug_assertions)]
    fn db_reads(&self) -> usize {
        todo!()
    }

    #[cfg(debug_assertions)]
    fn db_writes(&self) -> usize {
        todo!()
    }
}
