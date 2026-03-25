pub mod convert;
pub mod durable;
pub mod node;
pub mod ops;
pub mod repo;

use std::io;

pub use durable::DurableMultiMapRepository;
use node_types::Blake3Hashable;
pub use repo::MultiMapRef;
pub use repo::MultiMapRepository;
pub use trie_map::MapHash;
pub use trie_map::MapKey;
pub use trie_map::MapKeyPath;
pub use trie_map::MapRepository;
pub use trie_map::MapValue;
pub use trie_map::TrieMapSnapshot;

/// Relaxed value trait for MultiMap — like `MapValue` but without `Copy` and `Serialize`.
/// Allows embedding non-serializable, non-Copy types (e.g., `MultiMapRef<V>`) as values.
pub trait MultiMapValue: Blake3Hashable + Clone + Send + Sync {
    /// Serialize this value to a writer.
    fn write_value<W: io::Write>(&self, w: &mut W) -> io::Result<()>;
    /// Deserialize a value from a reader.
    fn read_value<R: io::Read>(r: &mut R) -> io::Result<Self>;
}

/// Blanket impl: every MapValue is also a MultiMapValue (uses bincode for serialization).
impl<T: MapValue> MultiMapValue for T {
    fn write_value<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
        let bytes = bincode::serialize(self).map_err(io::Error::other)?;
        w.write_all(&(bytes.len() as u32).to_le_bytes())?;
        w.write_all(&bytes)
    }

    fn read_value<R: io::Read>(r: &mut R) -> io::Result<Self> {
        let mut len_bytes = [0u8; 4];
        r.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut val_bytes = vec![0u8; len];
        r.read_exact(&mut val_bytes)?;
        bincode::deserialize(&val_bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
