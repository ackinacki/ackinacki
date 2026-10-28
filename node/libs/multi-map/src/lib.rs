pub mod live_metrics;
pub mod node;
pub mod ops;
pub mod repo;

use std::io;

use node_types::Blake3Hashable;
pub use ops::partition_nested_by_capacity;
pub use ops::CapacityPartitionError;
pub use ops::DisjointMergeError;
pub use ops::NestedCapacityPartition;
pub use repo::CapacityPartition;
pub use repo::MapIter;
pub use repo::MapIterAug;
pub use repo::MultiMap;
pub use repo::MultiMapAug;
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
    /// Number of high bits fixed in `prefix`. `u16` so a full 256-bit key
    /// path (every bit of a 256-bit key fixed) can be represented; `u8`
    /// saturated at 255 and could not encode a leaf at full depth.
    pub len: u16,
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

impl<V: MultiMapValue> MultiMapValue for std::sync::Arc<V> {
    fn write_value<W: io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        self.as_ref().write_value(writer)
    }

    fn read_value<R: io::Read>(reader: &mut R) -> anyhow::Result<Self> {
        Ok(std::sync::Arc::new(V::read_value(reader)?))
    }

    fn write_value_no_hash<W: io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        self.as_ref().write_value_no_hash(writer)
    }

    fn read_value_no_hash<R: io::Read>(reader: &mut R) -> anyhow::Result<Self> {
        Ok(std::sync::Arc::new(V::read_value_no_hash(reader)?))
    }
}

/// Derived data cached on Merkle-map nodes but excluded from their hashes.
///
/// `combine` must be associative. Branches combine children in nibble order;
/// extension nodes forward their child's augmentation and empty nodes use
/// `identity`.
pub trait MultiMapAugmentation: Clone + Send + Sync {
    fn identity() -> Self;
    fn combine(left: &Self, right: &Self) -> Self;

    /// Incrementally replace one child in an existing branch aggregate.
    /// Augmentations without an efficient inverse may retain the default and
    /// let the map recompute the aggregate from all children.
    fn replace_child(
        _current: &Self,
        _old_child: Option<&Self>,
        _new_child: Option<&Self>,
    ) -> Option<Self> {
        None
    }

    fn write<W: io::Write>(&self, writer: &mut W) -> anyhow::Result<()>;
    fn read<R: io::Read>(reader: &mut R) -> anyhow::Result<Self>;
}

impl MultiMapAugmentation for () {
    fn identity() -> Self {}

    fn combine(_left: &Self, _right: &Self) -> Self {}

    fn replace_child(
        _current: &Self,
        _old_child: Option<&Self>,
        _new_child: Option<&Self>,
    ) -> Option<Self> {
        Some(())
    }

    fn write<W: io::Write>(&self, _writer: &mut W) -> anyhow::Result<()> {
        Ok(())
    }

    fn read<R: io::Read>(_reader: &mut R) -> anyhow::Result<Self> {
        Ok(())
    }
}
#[cfg(test)]
mod edge_tests;
