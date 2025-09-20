use aerospike::as_bin;
use aerospike::Bin;
use aerospike::Bins;
use aerospike::FloatValue;
use aerospike::Key;
use aerospike::Value;

use crate::storage::KeyValueStore;
use crate::storage::ValueMap;

const BIN_SPLIT_COUNT: &str = "__split_count";

#[derive(Clone)]
pub struct SplitValueStore<Inner: KeyValueStore + Clone> {
    inner: Inner,
    max_size: usize,
}

impl<Inner: KeyValueStore + Clone> SplitValueStore<Inner> {
    pub fn new(inner: Inner, max_size: usize) -> Self {
        Self { inner, max_size }
    }
}
impl<Inner: KeyValueStore + Clone> KeyValueStore for SplitValueStore<Inner> {
    fn get(
        &self,
        key: &Key,
        values: &Bins,
        label: &'static str,
    ) -> anyhow::Result<Option<ValueMap>> {
        let mut values_with_split_count = values.clone();
        if let Bins::Some(names) = &mut values_with_split_count {
            names.push(BIN_SPLIT_COUNT.to_string());
        }
        let mut record = self.inner.get(key, &values_with_split_count, label)?;
        if let Some(record) = &mut record {
            merge_bins(&self.inner, key, values, record, label)?;
        }
        Ok(record)
    }

    fn put(
        &self,
        key: &Key,
        bins: &[Bin],
        until_success: bool,
        label: &'static str,
    ) -> anyhow::Result<()> {
        if let Some(records) = split_bins(key, bins, self.max_size)? {
            for (record_key, record_bins) in records {
                self.inner.put(&record_key, &record_bins, until_success, label)?;
            }
        } else {
            self.inner.put(key, bins, until_success, label)?;
        }
        Ok(())
    }

    fn batch_get(
        &self,
        gets: Vec<(Key, Bins)>,
        label: &'static str,
    ) -> anyhow::Result<Vec<Option<ValueMap>>> {
        let mut first_gets = gets.clone();
        for (_, values) in &mut first_gets {
            if let Bins::Some(some) = values {
                some.push(BIN_SPLIT_COUNT.to_string());
            }
        }
        let mut records: Vec<Option<ValueMap>> = self.inner.batch_get(first_gets, label)?;

        // Merge records with chunks if required
        for ((key, values), record) in gets.iter().zip(records.iter_mut()) {
            if let Some(record) = record {
                merge_bins(&self.inner, key, values, record, "")?;
            }
        }
        Ok(records)
    }

    #[cfg(debug_assertions)]
    fn db_reads(&self) -> usize {
        self.inner.db_reads()
    }

    #[cfg(debug_assertions)]
    fn db_writes(&self) -> usize {
        self.inner.db_writes()
    }
}

fn split_bins<'b>(
    key: &Key,
    bins: &[Bin<'b>],
    max_size: usize,
) -> anyhow::Result<Option<Vec<(Key, Vec<Bin<'b>>)>>> {
    let mut total_size = 0;
    let mut sorted_by_size = Vec::with_capacity(bins.len());
    for bin in bins {
        let size = bin.value.estimate_size().unwrap_or_default();
        total_size += size;
        sorted_by_size.push((size, bin));
    }
    if total_size < max_size {
        return Ok(None);
    }
    sorted_by_size.sort_by_key(|(size, _)| *size);

    let mut split = Split::new(key, max_size)?;
    for (bin_size, bin) in sorted_by_size {
        split.add_bin(bin_size, bin)?;
    }
    Ok(Some(split.into_records()?))
}

fn merge_bins(
    store: &(impl KeyValueStore + ?Sized),
    key: &Key,
    values: &Bins,
    record: &mut ValueMap,
    label: &'static str,
) -> anyhow::Result<()> {
    let Some(split_count) = record.remove(BIN_SPLIT_COUNT) else {
        return Ok(());
    };
    let split_count = match split_count {
        Value::Float(FloatValue::F32(f)) => f as usize,
        Value::Float(FloatValue::F64(f)) => f as usize,
        Value::Int(i) => i as usize,
        Value::UInt(i) => i as usize,
        _ => return Err(anyhow::anyhow!("Invalid split_count bin")),
    };
    let mut values = values.clone();
    if let Bins::Some(values) = &mut values {
        values.retain(|x| x.as_str() != BIN_SPLIT_COUNT)
    }
    for i in 1..split_count {
        let key = chunk_key(key, i)?;
        let chunk = store
            .get(&key, &values, label)?
            .ok_or_else(|| anyhow::anyhow!("Missing chunk record"))?;
        for (name, chunk_value) in chunk {
            if let Some(record_value) = record.get_mut(&name) {
                match (record_value, chunk_value) {
                    (Value::Blob(ref mut record_blob), Value::Blob(chunk_blob)) => {
                        record_blob.extend(chunk_blob);
                    }
                    _ => {
                        return Err(anyhow::anyhow!("Chunk value must be a blob"));
                    }
                }
            } else {
                record.insert(name, chunk_value);
            }
        }
    }
    Ok(())
}

pub fn chunk_key(key: &Key, i: usize) -> anyhow::Result<Key> {
    let base_user_key = key
        .user_key
        .as_ref()
        .map(|x| x.to_string())
        .ok_or_else(|| anyhow::anyhow!("Aerospike key must have a user key"))?;
    let user_key = Value::String(format!("__{base_user_key}+{i}"));
    Key::new(key.namespace.clone(), key.set_name.clone(), user_key)
        .map_err(|err| anyhow::anyhow!("Aerospike key failed: {err}"))
}

pub fn next_chunk<'n>(
    bin: &Bin<'n>,
    offset: usize,
    max_size: usize,
) -> anyhow::Result<(usize, Bin<'n>)> {
    let Value::Blob(blob) = &bin.value else {
        return Err(anyhow::anyhow!("Split bin must be blob"));
    };
    if offset >= blob.len() {
        return Err(anyhow::anyhow!("Invalid split offset"));
    };
    let size = max_size.min(blob.len() - offset);
    let value = blob[offset..(offset + size)].to_vec();
    Ok((size, as_bin!(bin.name, value)))
}

struct Split<'k, 'b> {
    max_size: usize,
    key: &'k Key,
    records: Vec<(Key, Vec<Bin<'b>>)>,
    bins: Vec<Bin<'b>>,
    size: usize,
}

impl<'k, 'b> Split<'k, 'b> {
    fn new(key: &'k Key, max_size: usize) -> anyhow::Result<Self> {
        Ok(Self { max_size, key, records: Vec::new(), bins: Vec::new(), size: 0 })
    }

    fn add_bin(&mut self, bin_size: usize, bin: &Bin<'b>) -> anyhow::Result<()> {
        if bin_size < self.max_size - self.size {
            self.bins.push(Bin { name: bin.name, value: bin.value.clone() });
            self.size += bin_size;
            return Ok(());
        }
        let mut bin_offset = 0;
        loop {
            let (chunk_size, chunk_bin) = next_chunk(bin, bin_offset, self.max_size - self.size)?;
            self.bins.push(chunk_bin);
            bin_offset += chunk_size;
            self.size += chunk_size;
            if bin_offset < bin_size {
                self.flush_record()?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn flush_record(&mut self) -> anyhow::Result<()> {
        if !self.bins.is_empty() {
            let key = if self.records.is_empty() {
                self.key.clone()
            } else {
                chunk_key(self.key, self.records.len())?
            };
            let bins = std::mem::take(&mut self.bins);
            self.records.push((key, bins));
            self.size = 0;
        }
        Ok(())
    }

    fn into_records(mut self) -> anyhow::Result<Vec<(Key, Vec<Bin<'b>>)>> {
        self.flush_record()?;
        let count = self.records.len();
        self.records[0].1.push(Bin::new(BIN_SPLIT_COUNT, Value::Int(count as i64)));
        Ok(self.records)
    }
}

#[cfg(test)]
mod tests {
    use aerospike::as_key;
    use rand::RngCore;

    use super::*;
    use crate::storage::mem::MemStore;

    fn key(value: &str) -> Key {
        as_key!("n", "s", value)
    }
    fn blobs<'n>(names_sizes: &[(&'n str, usize)]) -> Vec<Bin<'n>> {
        names_sizes
            .iter()
            .map(|(name, size)| {
                let mut buf = vec![0u8; *size];
                rand::thread_rng().fill_bytes(&mut buf);
                Bin::new(name, Value::Blob(buf))
            })
            .collect()
    }

    fn rec<'n>(key_value: &str, names_sizes: &[(&'n str, usize)]) -> (Key, Vec<Bin<'n>>) {
        (key(key_value), blobs(names_sizes))
    }

    fn to_map(v: &[Bin<'_>]) -> ValueMap {
        ValueMap::from_iter(v.iter().map(|x| (x.name.to_string(), x.value.clone())))
    }

    fn get(store: &impl KeyValueStore, key: &Key, bins: &[Bin]) -> ValueMap {
        store
            .get(key, &Bins::Some(bins.iter().map(|x| x.name.to_string()).collect::<Vec<_>>()), "")
            .unwrap()
            .unwrap()
    }

    #[test]
    fn test_split_store() -> anyhow::Result<()> {
        let mem = MemStore::new();
        let split = SplitValueStore::new(mem.clone(), 1000);
        let (k1, v1) = rec("1", &[("a", 10), ("b", 20)]);
        split.put(&k1, &v1, true, "")?;
        let r1 = get(&split, &k1, &v1);
        assert_eq!(r1, to_map(&v1));

        let (k2, v2) = rec("2", &[("a", 10), ("b", 900), ("c", 800), ("d", 10000)]);
        split.put(&k2, &v2, true, "")?;
        let r2 = get(&split, &k2, &v2);
        assert_eq!(r2, to_map(&v2));

        println!("{mem:?}");
        Ok(())
    }
}
