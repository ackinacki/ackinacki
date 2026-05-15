pub mod live_metrics;
pub mod node;
pub mod ops;
pub mod repo;

use std::io;

use node_types::Blake3Hashable;
pub use repo::MapIter;
pub use repo::MultiMap;
pub use repo::MultiMapRepository;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MapHash(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MapKey(pub [u8; 32]);

/// Prefix view: `len` is number of HIGH BITS fixed in `prefix`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MapKeyPath {
    pub prefix: MapKey,
    pub len: u8,
}

impl Default for MapKeyPath {
    fn default() -> Self {
        Self { prefix: MapKey([0; 32]), len: 0 }
    }
}

/// Relaxed value trait for MultiMap — like `MapValue` but without `Copy` and `Serialize`.
/// Allows embedding non-serializable, non-Copy types (e.g., `MultiMapRef<V>`) as values.
pub trait MultiMapValue: Blake3Hashable + Clone + Send + Sync {
    /// Serialize this value to a writer.
    fn write_value<W: io::Write>(&self, w: &mut W) -> anyhow::Result<()>;
    /// Deserialize a value from a reader.
    fn read_value<R: io::Read>(r: &mut R) -> anyhow::Result<Self>;
    /// Serialize without hashes (compact format). Default: same as write_value.
    fn write_value_no_hash<W: io::Write>(&self, w: &mut W) -> anyhow::Result<()> {
        self.write_value(w)
    }
    /// Deserialize a compact format, recomputing hashes. Default: same as read_value.
    fn read_value_no_hash<R: io::Read>(r: &mut R) -> anyhow::Result<Self> {
        Self::read_value(r)
    }
}
