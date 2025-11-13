use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use aerospike::Bin;
use aerospike::Bins;
use aerospike::FloatValue;
use aerospike::Key;
use aerospike::Value;
use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;
use tempfile::NamedTempFile;

use crate::storage::KeyValueStore;
use crate::storage::ValueMap;

#[derive(Clone, Debug)]
pub struct FsStore {
    data_dir: PathBuf,
}

impl FsStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir).context("cell repository directory {cells_dir}")?;
        Ok(Self { data_dir })
    }

    fn persist(tmp: NamedTempFile, path: &Path) -> anyhow::Result<()> {
        if let Err(err) = tmp.persist(path) {
            if err.error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(err.into());
            }
        }
        Ok(())
    }

    fn key_path(&self, key: &Key) -> PathBuf {
        self.data_dir.join(key.to_string())
    }
}

#[derive(Default, Clone)]
pub struct StoreStub;

impl KeyValueStore for StoreStub {
    fn get(
        &self,
        _key: &Key,
        _values: &Bins,
        _label: &'static str,
    ) -> anyhow::Result<Option<ValueMap>> {
        Ok(None)
    }

    fn put(
        &self,
        _key: &Key,
        _bins: &[Bin],
        _until_success: bool,
        _label: &'static str,
    ) -> anyhow::Result<()> {
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

impl KeyValueStore for FsStore {
    fn get(
        &self,
        key: &Key,
        _values: &Bins,
        _label: &'static str,
    ) -> anyhow::Result<Option<ValueMap>> {
        let key_path = self.key_path(key);
        if !key_path.exists() {
            return Ok(None);
        }
        tracing::trace!("Load key: {}", key.to_string());
        let data = std::fs::read(&key_path).map_err(|err| {
            anyhow::format_err!("Failed to read cell {}: {err}", key_path.display())
        })?;
        let bin_map = ValueMap::from_iter(
            bincode::deserialize::<HashMap<String, SerdeValue>>(&data)?
                .into_iter()
                .map(|(k, v)| (k, v.into())),
        );
        Ok(Some(bin_map))
    }

    fn put(
        &self,
        key: &Key,
        bins: &[Bin],
        _until: bool,
        _label: &'static str,
    ) -> anyhow::Result<()> {
        // TODO: this fn seems to be broken
        let path = self.key_path(key);
        let bin_map =
            ValueMap::from_iter(bins.iter().map(|bin| (bin.name.to_string(), bin.value.clone())));
        let mut tmp = NamedTempFile::new_in(&self.data_dir)?;
        tmp.write_all(&bincode::serialize(&HashMap::<String, SerdeValue>::from_iter(
            bin_map.into_iter().map(|(k, v)| (k, v.into())),
        ))?)?;
        Self::persist(tmp, &path)?;
        tracing::trace!("File saved: {:?}", path);
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

#[derive(Serialize, Deserialize)]
enum SerdeFloatValue {
    F32(u32),
    F64(u64),
}

impl From<FloatValue> for SerdeFloatValue {
    fn from(val: FloatValue) -> SerdeFloatValue {
        match val {
            FloatValue::F32(val) => SerdeFloatValue::F32(val),
            FloatValue::F64(val) => SerdeFloatValue::F64(val),
        }
    }
}

impl From<SerdeFloatValue> for FloatValue {
    fn from(val: SerdeFloatValue) -> FloatValue {
        match val {
            SerdeFloatValue::F32(val) => FloatValue::F32(val),
            SerdeFloatValue::F64(val) => FloatValue::F64(val),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum SerdeValue {
    Nil,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(SerdeFloatValue),
    String(String),
    Blob(Vec<u8>),
    List(Vec<SerdeValue>),
    HashMap(Vec<(SerdeValue, SerdeValue)>),
    OrderedMap(Vec<(SerdeValue, SerdeValue)>),
    GeoJSON(String),
    #[allow(clippy::upper_case_acronyms)]
    HLL(Vec<u8>),
}

impl From<SerdeValue> for Value {
    fn from(v: SerdeValue) -> Self {
        match v {
            SerdeValue::Nil => Value::Nil,
            SerdeValue::Bool(b) => Value::Bool(b),
            SerdeValue::Int(i) => Value::Int(i),
            SerdeValue::UInt(u) => Value::UInt(u),
            SerdeValue::Float(f) => Value::Float(f.into()),
            SerdeValue::String(s) => Value::String(s),
            SerdeValue::Blob(b) => Value::Blob(b),
            SerdeValue::List(l) => Value::List(l.into_iter().map(Into::into).collect()),
            SerdeValue::HashMap(h) => {
                Value::HashMap(h.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
            SerdeValue::OrderedMap(o) => {
                Value::OrderedMap(o.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
            SerdeValue::GeoJSON(g) => Value::GeoJSON(g),
            SerdeValue::HLL(h) => Value::HLL(h),
        }
    }
}

impl From<Value> for SerdeValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Nil => Self::Nil,
            Value::Bool(b) => Self::Bool(b),
            Value::Int(i) => Self::Int(i),
            Value::UInt(u) => Self::UInt(u),
            Value::Float(f) => Self::Float(f.into()),
            Value::String(s) => Self::String(s),
            Value::Blob(b) => Self::Blob(b),
            Value::List(l) => Self::List(l.into_iter().map(Into::into).collect()),
            Value::HashMap(h) => {
                Self::HashMap(h.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
            Value::OrderedMap(o) => {
                Self::OrderedMap(o.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
            Value::GeoJSON(g) => Self::GeoJSON(g),
            Value::HLL(h) => Self::HLL(h),
        }
    }
}
