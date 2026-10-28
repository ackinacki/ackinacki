//! Write legacy hashes using the current canonical account-map topology.
//!
//! Legacy serialization is preorder (parent before children), but translated hashes are
//! computed bottom-up. Building a second Arc-based tree wastes gigabytes and makes its
//! destruction expensive, especially under swap pressure. Instead, reuse a bounded preorder
//! hash buffer for one subtree at a time. Only nodes above those subtrees need seek-back
//! patches; doing a file seek for every node would defeat buffered output.
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use anyhow::ensure;
use multi_map::node::NodeAug;
use multi_map::ops::hash_branch_empty;
use multi_map::ops::hash_branch_sparse;
use multi_map::ops::hash_ext;
use multi_map::ops::hash_leaf_value;
use multi_map::MapKey;
use multi_map::MultiMapValue;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;

use super::maps::L1MapValue;
use super::AccountCapacity;
use super::AccountInfo;
use super::ThreadAccountMap;

const HASH_BUFFER_NODES: usize = 32 * 1024; // 1 MiB, independent of total map size.

/// Return serialized bytes and account leaves in one topology-only walk. Legacy fields are
/// fixed-width: a full-map path is 33 bytes, each nonempty node starts with tag + 32-byte hash,
/// and either AccountInfo variant occupies 33 bytes. Neither hashes nor augmentation need
/// computing/serializing. The caller can seek over this area in its unpublished temporary
/// file and fill it once the account scan has supplied the legacy hashes.
pub(super) fn account_map_layout<F: Fn() -> bool>(
    map: &ThreadAccountMap,
    cancelled: &F,
) -> anyhow::Result<(u64, usize)> {
    fn visit<F: Fn() -> bool>(view: View<'_>, cancelled: &F) -> anyhow::Result<(u64, usize)> {
        ensure!(!cancelled(), "snapshot save cancelled");
        match view.kind() {
            Kind::Empty => Ok((1, 0)),
            Kind::Account(_) => Ok((1 + 32 + 33, 1)),
            Kind::Dapp(value) => {
                ensure!(
                    value.0.root_path.len == 0 && !value.0.root.is_empty(),
                    "legacy export requires full nonempty DApp maps"
                );
                let (bytes, count) =
                    visit(View::Accounts(MapKey([0; 32]), &value.0.root), cancelled)?;
                Ok((1 + 32 + 33 + bytes, count))
            }
            Kind::Ext(nibbles) => {
                let (bytes, count) = visit(view.child(0), cancelled)?;
                Ok((1 + 32 + 1 + nibbles.len() as u64 + bytes, count))
            }
            Kind::Branch(_) => {
                let mut bytes = 1 + 32 + 2;
                let mut count = 0;
                // Empty child tags are serialized too, independently of the branch bitmap.
                for i in 0..16 {
                    let (child_bytes, child_count) = visit(view.child(i), cancelled)?;
                    bytes += child_bytes;
                    count += child_count;
                }
                Ok((bytes, count))
            }
        }
    }
    ensure!(map.dapps.root_path.len == 0, "legacy export requires a full map");
    let (bytes, count) = visit(View::Dapps(&map.dapps.root), cancelled)?;
    Ok((33 + bytes, count))
}

#[derive(Clone, Copy)]
enum View<'a> {
    Dapps(&'a NodeAug<L1MapValue, AccountCapacity>),
    Accounts(MapKey, &'a NodeAug<AccountInfo, AccountCapacity>),
}

enum Kind<'a> {
    Empty,
    Dapp(&'a L1MapValue),
    Account(MapKey),
    Branch(u16),
    Ext(&'a [u8]),
}

impl<'a> View<'a> {
    fn kind(self) -> Kind<'a> {
        match self {
            Self::Dapps(node) => match node {
                NodeAug::Empty => Kind::Empty,
                NodeAug::Leaf(leaf) => Kind::Dapp(&leaf.value),
                NodeAug::Branch(branch) => Kind::Branch(branch.bitmap),
                NodeAug::Ext(ext) => Kind::Ext(&ext.nibbles[..ext.nibble_count as usize]),
            },
            Self::Accounts(dapp, node) => match node {
                NodeAug::Empty => Kind::Empty,
                NodeAug::Leaf(_) => Kind::Account(dapp),
                NodeAug::Branch(branch) => Kind::Branch(branch.bitmap),
                NodeAug::Ext(ext) => Kind::Ext(&ext.nibbles[..ext.nibble_count as usize]),
            },
        }
    }

    fn child(self, index: usize) -> Self {
        match self {
            Self::Dapps(NodeAug::Branch(branch)) => Self::Dapps(&branch.children[index]),
            Self::Accounts(dapp, NodeAug::Branch(branch)) => {
                Self::Accounts(dapp, &branch.children[index])
            }
            Self::Dapps(NodeAug::Ext(ext)) => Self::Dapps(&ext.child),
            Self::Accounts(dapp, NodeAug::Ext(ext)) => Self::Accounts(dapp, &ext.child),
            _ => unreachable!("only branches and extensions have children"),
        }
    }
}

#[derive(Default)]
pub(super) struct MapWriteStats {
    pub hash_buffer_peak_bytes: usize,
    pub patched_nodes: usize,
}

struct Translator<'a, F> {
    legacy: &'a [(AccountRouting, AccountInfo)],
    next_index: usize,
    hashes: Vec<[u8; 32]>,
    limit: usize,
    cancelled: &'a F,
    stats: MapWriteStats,
}

fn with_nibble(mut key: MapKey, depth: usize, nibble: u8) -> MapKey {
    if depth.is_multiple_of(2) {
        key.0[depth / 2] = (key.0[depth / 2] & 0x0f) | (nibble << 4);
    } else {
        key.0[depth / 2] = (key.0[depth / 2] & 0xf0) | nibble;
    }
    key
}

fn extension_key(mut key: MapKey, depth: usize, nibbles: &[u8]) -> MapKey {
    for (i, nibble) in nibbles.iter().enumerate() {
        key = with_nibble(key, depth + i, *nibble);
    }
    key
}

fn extension_hash(nibbles: &[u8], child: &[u8; 32]) -> [u8; 32] {
    let mut packed = [0; 32];
    for (i, nibble) in nibbles.iter().enumerate() {
        packed[i / 2] |= nibble << if i.is_multiple_of(2) { 4 } else { 0 };
    }
    hash_ext(nibbles.len() as u8, &packed[..nibbles.len().div_ceil(2)], child)
}

fn dapp_hash(root: [u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[1]);
    hasher.update(&root);
    *hasher.finalize().as_bytes()
}

impl<F: Fn() -> bool> Translator<'_, F> {
    fn check_cancelled(&self) -> anyhow::Result<()> {
        ensure!(!(self.cancelled)(), "snapshot save cancelled");
        Ok(())
    }

    fn info(&self, dapp: MapKey, account: MapKey, next: &mut usize) -> anyhow::Result<AccountInfo> {
        let routing =
            AccountRouting::new(DAppIdentifier::new(dapp.0), AccountIdentifier::new(account.0));
        let (expected, info) = self
            .legacy
            .get(*next)
            .ok_or_else(|| anyhow::anyhow!("missing legacy value: {routing}"))?;
        ensure!(
            *expected == routing,
            "legacy routing mismatch at index {}: expected {expected}, visited {routing}",
            *next
        );
        *next += 1;
        Ok(*info)
    }

    fn accounts(value: &L1MapValue, dapp: MapKey) -> anyhow::Result<View<'_>> {
        ensure!(
            value.0.root_path.len == 0 && !value.0.root.is_empty(),
            "legacy export requires full nonempty DApp maps"
        );
        Ok(View::Accounts(dapp, &value.0.root))
    }

    /// Reserve preorder slots, fill them bottom-up. Stop immediately at the hard limit;
    /// retrying smaller children only revisits a bounded prefix, never an entire large tree.
    fn collect(
        &mut self,
        view: View<'_>,
        key: MapKey,
        depth: usize,
        next: &mut usize,
    ) -> anyhow::Result<Option<[u8; 32]>> {
        self.check_cancelled()?;
        let kind = view.kind();
        if matches!(kind, Kind::Empty) {
            return Ok(Some(hash_branch_empty()));
        }
        if self.hashes.len() == self.limit {
            return Ok(None);
        }
        let slot = self.hashes.len();
        self.hashes.push([0; 32]);
        let hash = match kind {
            Kind::Empty => unreachable!(),
            Kind::Account(dapp) => {
                ensure!(depth == 64, "incomplete account key");
                hash_leaf_value(&self.info(dapp, key, next)?)
            }
            Kind::Dapp(value) => {
                ensure!(depth == 64, "incomplete DApp key");
                let Some(root) =
                    self.collect(Self::accounts(value, key)?, MapKey([0; 32]), 0, next)?
                else {
                    return Ok(None);
                };
                dapp_hash(root)
            }
            Kind::Ext(nibbles) => {
                let Some(hash) = self.collect(
                    view.child(0),
                    extension_key(key, depth, nibbles),
                    depth + nibbles.len(),
                    next,
                )?
                else {
                    return Ok(None);
                };
                extension_hash(nibbles, &hash)
            }
            Kind::Branch(bitmap) => {
                let mut nibs = [0; 16];
                let mut hashes = [[0; 32]; 16];
                let mut count = 0;
                for i in 0..16 {
                    if bitmap & (1 << i) == 0 {
                        continue;
                    }
                    let Some(hash) = self.collect(
                        view.child(i),
                        with_nibble(key, depth, i as u8),
                        depth + 1,
                        next,
                    )?
                    else {
                        return Ok(None);
                    };
                    nibs[count] = i as u8;
                    hashes[count] = hash;
                    count += 1;
                }
                hash_branch_sparse(bitmap, &nibs[..count], &hashes[..count])
            }
        };
        self.hashes[slot] = hash;
        Ok(Some(hash))
    }

    fn emit(
        &self,
        view: View<'_>,
        key: MapKey,
        depth: usize,
        hashes: &mut std::slice::Iter<'_, [u8; 32]>,
        next: &mut usize,
        writer: &mut impl Write,
    ) -> anyhow::Result<()> {
        self.check_cancelled()?;
        let kind = view.kind();
        let tag = match kind {
            Kind::Empty => 0,
            Kind::Dapp(_) | Kind::Account(_) => 1,
            Kind::Branch(..) => 2,
            Kind::Ext(..) => 3,
        };
        writer.write_all(&[tag])?;
        if tag == 0 {
            return Ok(());
        }
        writer.write_all(
            hashes.next().ok_or_else(|| anyhow::anyhow!("missing buffered legacy hash"))?,
        )?;
        match kind {
            Kind::Empty => unreachable!(),
            Kind::Account(dapp) => self.info(dapp, key, next)?.write_value(writer)?,
            Kind::Dapp(value) => {
                writer.write_all(&[0; 33])?;
                self.emit(Self::accounts(value, key)?, MapKey([0; 32]), 0, hashes, next, writer)?;
            }
            Kind::Ext(nibbles) => {
                writer.write_all(&[nibbles.len() as u8])?;
                writer.write_all(nibbles)?;
                self.emit(
                    view.child(0),
                    extension_key(key, depth, nibbles),
                    depth + nibbles.len(),
                    hashes,
                    next,
                    writer,
                )?;
            }
            Kind::Branch(bitmap) => {
                writer.write_all(&bitmap.to_le_bytes())?;
                for i in 0..16 {
                    self.emit(
                        view.child(i),
                        with_nibble(key, depth, i as u8),
                        depth + 1,
                        hashes,
                        next,
                        writer,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn write(
        &mut self,
        view: View<'_>,
        key: MapKey,
        depth: usize,
        writer: &mut (impl Write + Seek),
    ) -> anyhow::Result<[u8; 32]> {
        self.hashes.clear();
        // A discarded probe must not advance the committed cursor. Serialization repeats the
        // successful probe from the same index, then checks that both walks consumed its leaves.
        let mut probe_next = self.next_index;
        let hash = self.collect(view, key, depth, &mut probe_next)?;
        self.stats.hash_buffer_peak_bytes =
            self.stats.hash_buffer_peak_bytes.max(self.hashes.len() * 32);
        if let Some(hash) = hash {
            let mut hashes = self.hashes.iter();
            let mut emitted_next = self.next_index;
            self.emit(view, key, depth, &mut hashes, &mut emitted_next, writer)?;
            ensure!(
                hashes.next().is_none() && emitted_next == probe_next,
                "legacy hash/leaf traversal mismatch"
            );
            self.next_index = emitted_next;
            return Ok(hash);
        }
        self.hashes.clear();
        let kind = view.kind();
        let tag = match kind {
            Kind::Dapp(_) => 1,
            Kind::Branch(..) => 2,
            Kind::Ext(..) => 3,
            _ => anyhow::bail!("legacy leaf exceeds hash buffer"),
        };
        writer.write_all(&[tag])?;
        let hash_pos = writer.stream_position()?;
        writer.write_all(&[0; 32])?;
        let hash = match kind {
            Kind::Dapp(value) => {
                writer.write_all(&[0; 33])?;
                dapp_hash(self.write(Self::accounts(value, key)?, MapKey([0; 32]), 0, writer)?)
            }
            Kind::Ext(nibbles) => {
                writer.write_all(&[nibbles.len() as u8])?;
                writer.write_all(nibbles)?;
                extension_hash(
                    nibbles,
                    &self.write(
                        view.child(0),
                        extension_key(key, depth, nibbles),
                        depth + nibbles.len(),
                        writer,
                    )?,
                )
            }
            Kind::Branch(bitmap) => {
                writer.write_all(&bitmap.to_le_bytes())?;
                let mut nibs = [0; 16];
                let mut hashes = [[0; 32]; 16];
                let mut count = 0;
                for i in 0..16 {
                    let hash = self.write(
                        view.child(i),
                        with_nibble(key, depth, i as u8),
                        depth + 1,
                        writer,
                    )?;
                    if bitmap & (1 << i) != 0 {
                        nibs[count] = i as u8;
                        hashes[count] = hash;
                        count += 1;
                    }
                }
                hash_branch_sparse(bitmap, &nibs[..count], &hashes[..count])
            }
            _ => unreachable!(),
        };
        let end = writer.stream_position()?;
        writer.seek(SeekFrom::Start(hash_pos))?;
        writer.write_all(&hash)?;
        writer.seek(SeekFrom::Start(end))?;
        self.stats.patched_nodes += 1;
        Ok(hash)
    }
}

/// Entries must be sorted by full routing and match the source map exactly. On cancellation,
/// IO error or metadata mismatch the caller must discard the partial snapshot, not publish it.
pub(super) fn write_account_map<W: Write + Seek, F: Fn() -> bool>(
    map: &ThreadAccountMap,
    entries: &[(AccountRouting, AccountInfo)],
    writer: &mut W,
    cancelled: &F,
) -> anyhow::Result<MapWriteStats> {
    write_with_buffer(map, entries, writer, cancelled, HASH_BUFFER_NODES)
}

fn write_with_buffer<W: Write + Seek, F: Fn() -> bool>(
    map: &ThreadAccountMap,
    entries: &[(AccountRouting, AccountInfo)],
    writer: &mut W,
    cancelled: &F,
    limit: usize,
) -> anyhow::Result<MapWriteStats> {
    ensure!(limit > 0 && limit <= HASH_BUFFER_NODES, "invalid legacy hash buffer size");
    ensure!(map.dapps.root_path.len == 0, "legacy export requires a full map");
    let mut translator = Translator {
        legacy: entries,
        next_index: 0,
        hashes: Vec::with_capacity(limit),
        limit,
        cancelled,
        stats: MapWriteStats::default(),
    };
    translator.check_cancelled()?;
    writer.write_all(&[0; 33])?;
    translator.write(View::Dapps(&map.dapps.root), MapKey([0; 32]), 0, writer)?;
    ensure!(translator.next_index == entries.len(), "unused legacy entries");
    Ok(translator.stats)
}

#[cfg(test)]
#[path = "snapshot_v1_export_tests.rs"]
mod tests;
