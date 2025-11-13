use std::collections::HashMap;
use std::sync::Arc;

use aerospike::as_bin;
use aerospike::as_key;
use aerospike::Bins;
use aerospike::Key;
use aerospike::Value;
use parking_lot::Mutex;
use telemetry_utils::now_ms;
use tvm_block::GetRepresentationHash;

use crate::helper::metrics::AEROSPIKE_OBJECT_TYPE_INT_MESSAGES;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::storage::mem::MemStore;
use crate::storage::KeyValueStore;
use crate::storage::BIN_BLOB;
use crate::storage::BIN_HASH;
use crate::storage::BIN_SEQ;
use crate::storage::NAMESPACE;
use crate::types::AccountAddress;

// ============================
// MessageDurableStorage
// ============================

#[derive(Clone)]
pub struct MessageDurableStorage {
    store: Arc<dyn KeyValueStore>,
    set_prefix: String,
    seq: Arc<Mutex<HashMap<String, i64>>>,
}

impl MessageDurableStorage {
    pub fn new(store: impl KeyValueStore + 'static, set_prefix: &str) -> Self {
        Self {
            store: Arc::new(store),
            set_prefix: set_prefix.to_string(),
            seq: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn mem() -> Self {
        Self::new(MemStore::new(), "")
    }

    fn message_key(&self, hash: &str) -> Key {
        as_key!(NAMESPACE, &self.set_prefix, hash)
    }

    fn index_key(&self, account: &str, seq: i64) -> Key {
        as_key!(NAMESPACE, &self.set_prefix, format!("{account}-{seq}"))
    }

    // Writes records:
    // 1.  {PKey: message_hash, values: message, seq}
    // 2.  {PKey: address+seqno, value: message_hash}  - kind of reverse index
    pub fn write_messages(
        &self,
        messages: HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    ) -> anyhow::Result<()> {
        if !cfg!(feature = "messages_db") || cfg!(feature = "disable_db_for_messages") {
            return Ok(());
        }

        for (addr, messages) in messages {
            let dest = addr.0.to_hex_string();
            for message in messages {
                let hash =
                    message.1.message.hash().expect("message must have hash").to_hex_string();
                let blob = bincode::serialize(&message.1)?;

                let last_seq = {
                    let seq = self.seq.lock();
                    seq.get(&dest)
                        .copied()
                        .unwrap_or_else(|| now_ms().try_into().unwrap_or_default())
                };
                let next_seq = last_seq + 1;

                // write message
                let key = self.message_key(&hash);
                self.store.put(
                    &key,
                    &[as_bin!(BIN_BLOB, blob), as_bin!(BIN_SEQ, &next_seq)],
                    true,
                    AEROSPIKE_OBJECT_TYPE_INT_MESSAGES,
                )?;

                // write index
                let idx_key = self.index_key(&dest, next_seq);
                self.store.put(
                    &idx_key,
                    &[as_bin!(BIN_HASH, &hash)],
                    true,
                    AEROSPIKE_OBJECT_TYPE_INT_MESSAGES,
                )?;
                // Update seq in a hashmap
                self.seq.lock().insert(dest.clone(), next_seq);
            }
        }
        Ok(())
    }

    pub fn read_message(&self, hash: &str) -> anyhow::Result<Option<(i64, WrappedMessage)>> {
        if !cfg!(feature = "messages_db") || cfg!(feature = "disable_db_for_messages") {
            return Ok(None);
        }

        let key = self.message_key(hash);
        if let Some(bins) =
            self.store.get(&key, &[BIN_SEQ, BIN_BLOB].into(), AEROSPIKE_OBJECT_TYPE_INT_MESSAGES)?
        {
            let seq = match bins.get(BIN_SEQ) {
                Some(Value::Int(s)) => *s,
                _ => return Err(anyhow::anyhow!("Missing seq")),
            };
            let blob = match bins.get(BIN_BLOB) {
                Some(Value::Blob(b)) => b.clone(),
                _ => return Err(anyhow::anyhow!("Missing blob")),
            };
            let msg: WrappedMessage = bincode::deserialize(&blob)?;
            Ok(Some((seq, msg)))
        } else {
            Ok(None)
        }
    }

    pub fn get_rowid_by_hash(&self, hash: &str) -> anyhow::Result<Option<i64>> {
        // TODO: we can repeat code from read_message to exclude field WrappedMessage
        self.read_message(hash).map(|opt| opt.map(|(id, _msg)| id))
    }

    fn batch_read_messages(
        &self,
        hashes: Vec<String>,
    ) -> anyhow::Result<Vec<(i64, WrappedMessage)>> {
        if !cfg!(feature = "messages_db") || cfg!(feature = "disable_db_for_messages") {
            return Ok(vec![]);
        }
        let mut ret_val = vec![];

        let num_messages = hashes.len();

        let mut batch_gets: Vec<(Key, Bins)> = vec![];

        for hash in hashes {
            let key = self.message_key(&hash);
            batch_gets.push((key, [BIN_SEQ, BIN_BLOB].into()));
        }

        let batch_results = self
            .store
            .batch_get(batch_gets, AEROSPIKE_OBJECT_TYPE_INT_MESSAGES)
            .map_err(|e| anyhow::anyhow!("Error executing batch request: {e}"))?;

        for record in batch_results.into_iter().flatten() {
            let Some(Value::Int(seq)) = record.get(BIN_SEQ) else {
                return Err(anyhow::anyhow!("Failed to read message, missing seq"));
            };
            let Some(Value::Blob(message_blob)) = record.get(BIN_BLOB) else {
                return Err(anyhow::anyhow!("Failed to read message, missing blob"));
            };
            let wrapped_message = bincode::deserialize(message_blob)?;
            ret_val.push((*seq, wrapped_message));
        }

        // This is not an error, but this interesting, and may be we need to tune BatchPolicy
        if ret_val.len() != num_messages {
            tracing::warn!("Found only {} from {} records", ret_val.len(), num_messages)
        }
        Ok(ret_val)
    }

    fn batch_get_hashes_by_rowid(&self, dest: &str, seqs: Vec<i64>) -> anyhow::Result<Vec<String>> {
        if !cfg!(feature = "messages_db") || cfg!(feature = "disable_db_for_messages") {
            return Ok(vec![]);
        }
        let mut ret_val = vec![];

        let mut batch_gets = vec![];

        for seq in seqs.clone() {
            batch_gets.push((self.index_key(dest, seq), [BIN_HASH].into()));
        }

        let batch_results = self
            .store
            .batch_get(batch_gets, AEROSPIKE_OBJECT_TYPE_INT_MESSAGES)
            .map_err(|e| anyhow::anyhow!("Error executing batch request: {e}"))?;

        for record in batch_results.into_iter().flatten() {
            let Some(Value::String(hash)) = record.get(BIN_HASH) else {
                return Err(anyhow::anyhow!("Failed to read index, missing bin hash"));
            };
            ret_val.push(hash.to_owned());
        }
        Ok(ret_val)
    }

    pub fn next_simple(
        &self,
        dest: &str,
        start_cursor: i64,
        limit: usize,
    ) -> anyhow::Result<(Vec<WrappedMessage>, Option<i64>)> {
        if !cfg!(feature = "messages_db") || cfg!(feature = "disable_db_for_messages") {
            return Ok((vec![], None));
        }
        // Gather all message hashes
        let next_cursor = start_cursor + 1;

        let seqs: Vec<i64> = (next_cursor..next_cursor + limit as i64).collect();

        let hashes = self.batch_get_hashes_by_rowid(dest, seqs)?;

        // Gather all messages and sort them by seq in ascending order
        let mut xs: Vec<(i64, WrappedMessage)> = self.batch_read_messages(hashes)?;
        xs.sort_by_key(|pair| pair.0);
        if let Some(first) = xs.first() {
            if first.0 != next_cursor {
                return Ok((vec![], None));
            }
        }
        let indexes: Vec<i64> = xs.iter().map(|x| x.0).collect();
        xs.truncate(monotonic_prefix_len(&indexes));
        let last_seq = xs.last().map(|x| x.0);
        let messages = xs.into_iter().map(|x| x.1).collect();
        Ok((messages, last_seq))
    }

    #[cfg(debug_assertions)]
    pub fn db_reads(&self) -> usize {
        self.store.db_reads()
    }

    #[cfg(debug_assertions)]
    pub fn db_writes(&self) -> usize {
        self.store.db_writes()
    }
}

fn monotonic_prefix_len(xs: &[i64]) -> usize {
    if xs.is_empty() {
        return 0;
    }
    if xs.len() == 1 {
        return 1;
    }
    for i in 0..xs.len() - 1 {
        if xs[i] + 1 != xs[i + 1] {
            return i + 1;
        }
    }
    xs.len()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_monotonicity_border() {
        let xs = vec![1, 2, 3, 4, 5];
        assert_eq!(monotonic_prefix_len(&xs), 5);
    }

    #[test]
    fn test_find_monotonicity_boder_at_start() {
        let xs = vec![5, 1, 2, 3];
        assert_eq!(monotonic_prefix_len(&xs), 1);
    }

    #[test]
    fn test_find_monotonicity_boder_in_middle() {
        let xs = vec![1, 2, 5, 3, 4];
        assert_eq!(monotonic_prefix_len(&xs), 2);
    }

    #[test]
    fn test_find_monotonicity_boder_at_end() {
        let xs = vec![1, 2, 3, 2];
        assert_eq!(monotonic_prefix_len(&xs), 3);
    }

    #[test]
    fn test_find_monotonicity_boder_empty() {
        let xs: Vec<i64> = vec![];
        assert_eq!(monotonic_prefix_len(&xs), 0);
    }

    #[test]
    fn test_find_monotonicity_boder_single_element() {
        let xs = vec![42];
        assert_eq!(monotonic_prefix_len(&xs), 1);
    }

    #[test]
    fn test_find_monotonicity_boder_all_equal() {
        let xs = vec![7, 7, 7, 7];
        assert_eq!(monotonic_prefix_len(&xs), 1);
    }

    #[test]
    fn test_find_monotonicity_boder_multiple_breaks() {
        let xs = vec![1, 2, 1, 2, 1];
        assert_eq!(monotonic_prefix_len(&xs), 2);
    }
}
