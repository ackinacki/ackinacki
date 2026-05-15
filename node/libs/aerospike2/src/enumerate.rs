use std::collections::HashMap;

use aerospike::Bins;
use aerospike::Client;
use aerospike::ClientPolicy;
use aerospike::PartitionFilter;
use aerospike::QueryPolicy;
use aerospike::Statement;
use aerospike::Value;

use crate::runtime::block_on;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EnumeratedKey {
    Int(i64),
    String(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumeratedValue {
    Bool(bool),
    Int(i64),
    String(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumeratedRecord {
    pub user_key: EnumeratedKey,
    pub generation: u32,
    pub bins: HashMap<String, EnumeratedValue>,
}

pub fn enumerate_set(
    socket_address: &str,
    namespace: &str,
    set_name: &str,
) -> anyhow::Result<Vec<EnumeratedRecord>> {
    let client = block_on(Client::new(&ClientPolicy::default(), &socket_address))?
        .map_err(|err| anyhow::anyhow!("Failed to connect to Aerospike: {err}"))?;
    let mut result = Vec::new();
    enumerate_set_with_client(&client, namespace, set_name, &mut |record| {
        result.push(record);
        Ok(())
    })?;
    Ok(result)
}

pub fn enumerate_set_with(
    socket_address: &str,
    namespace: &str,
    set_name: &str,
    on_record: &mut dyn FnMut(EnumeratedRecord) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let client = block_on(Client::new(&ClientPolicy::default(), &socket_address))?
        .map_err(|err| anyhow::anyhow!("Failed to connect to Aerospike: {err}"))?;
    enumerate_set_with_client(&client, namespace, set_name, on_record)
}

// Tokio runtime handling moved to the shared `crate::runtime` module.

pub(crate) fn enumerate_set_with_client(
    client: &Client,
    namespace: &str,
    set_name: &str,
    on_record: &mut dyn FnMut(EnumeratedRecord) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    use futures::StreamExt;

    let records = block_on(client.query(
        &QueryPolicy::default(),
        PartitionFilter::all(),
        Statement::new(namespace, set_name, Bins::All),
    ))?
    .map_err(|err| anyhow::anyhow!("Failed to enumerate set via PI query: {err}"))?;

    let mut stream = records.into_stream();
    while let Some(record) = block_on(stream.next())? {
        let record = record.map_err(|err| anyhow::anyhow!("Enumeration record error: {err}"))?;
        let key = record
            .key
            .ok_or_else(|| anyhow::anyhow!("Enumerated record is missing key metadata"))?;
        let user_key = key
            .user_key
            .ok_or_else(|| anyhow::anyhow!("Enumerated record is missing stored user key"))?;
        let user_key = convert_key(user_key)?;

        let mut bins = HashMap::with_capacity(record.bins.len());
        for (name, value) in record.bins {
            bins.insert(name, convert_value(value)?);
        }
        on_record(EnumeratedRecord { user_key, generation: record.generation, bins })?;
    }

    Ok(())
}

fn convert_key(value: Value) -> anyhow::Result<EnumeratedKey> {
    match value {
        Value::Int(value) => Ok(EnumeratedKey::Int(value)),
        Value::String(value) => Ok(EnumeratedKey::String(value)),
        Value::Blob(value) => Ok(EnumeratedKey::Blob(value)),
        other => Err(anyhow::anyhow!("Unsupported Aerospike key type: {other:?}")),
    }
}

fn convert_value(value: Value) -> anyhow::Result<EnumeratedValue> {
    match value {
        Value::Bool(value) => Ok(EnumeratedValue::Bool(value)),
        Value::Int(value) => Ok(EnumeratedValue::Int(value)),
        Value::String(value) => Ok(EnumeratedValue::String(value)),
        Value::Blob(value) => Ok(EnumeratedValue::Blob(value)),
        other => Err(anyhow::anyhow!("Unsupported Aerospike bin value type: {other:?}")),
    }
}
