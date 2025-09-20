use std::collections::HashMap;

use aerospike::Bin;
use aerospike::Bins;
use aerospike::Key;
use aerospike::Value;

pub type ValueMap = HashMap<String, Value>;

pub trait KeyValueStore: Send + Sync {
    fn get(
        &self,
        key: &Key,
        values: &Bins,
        label: &'static str,
    ) -> anyhow::Result<Option<ValueMap>>;

    fn batch_get(
        &self,
        gets: Vec<(Key, Bins)>,
        label: &'static str,
    ) -> anyhow::Result<Vec<Option<ValueMap>>> {
        let mut result = Vec::new();
        for (key, values) in &gets {
            result.push(self.get(key, values, label)?);
        }
        Ok(result)
    }

    fn put(
        &self,
        key: &Key,
        bins: &[Bin],
        until_success: bool,
        label: &'static str,
    ) -> anyhow::Result<()>;

    fn batch_put(
        &self,
        puts: Vec<(Key, Vec<Bin>)>,
        until_success: bool,
        label: &'static str,
    ) -> anyhow::Result<()> {
        for (k, v) in puts {
            self.put(&k, &v, until_success, label)?;
        }
        Ok(())
    }
    #[cfg(debug_assertions)]
    fn db_reads(&self) -> usize;

    #[cfg(debug_assertions)]
    fn db_writes(&self) -> usize;
}
