//! Read-only forensic check of legacy node hashes and extension normalization.
use multi_map::ops::hash_branch_empty;
use multi_map::ops::hash_branch_sparse;
use multi_map::ops::hash_ext;
use multi_map::ops::hash_leaf_value;

use super::*;

#[derive(Default, Debug)]
struct Stats {
    nodes: u64,
    accounts: u64,
    dapps: u64,
    empty_dapps: u64,
    adjacent_extensions: [u64; 2],
    invalid_local_hashes: [u64; 2],
}

struct Digest {
    stored: [u8; 32],
    recomputed: [u8; 32],
    canonical: [u8; 32],
    tag: u8,
    extension: Vec<u8>,
    base: [u8; 32],
}

fn leaf_hash(value: [u8; 32]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(&[1]);
    h.update(&value);
    h.finalize().into()
}

fn ext_hash(nibbles: &[u8], child: &[u8; 32]) -> [u8; 32] {
    let packed: Vec<_> =
        nibbles.chunks(2).map(|n| n[0] << 4 | n.get(1).copied().unwrap_or(0)).collect();
    hash_ext(nibbles.len() as u8, &packed, child)
}

fn tree(reader: &mut impl Read, level: usize, stats: &mut Stats) -> anyhow::Result<Digest> {
    ensure!(read_array::<33>(reader)? == [0; 33], "Expected full legacy map");
    node(reader, level, 0, stats)
}

fn node(
    reader: &mut impl Read,
    level: usize,
    depth: usize,
    stats: &mut Stats,
) -> anyhow::Result<Digest> {
    let tag = read_array::<1>(reader)?[0];
    stats.nodes += 1;
    if tag == 0 {
        let hash = hash_branch_empty();
        return Ok(Digest {
            stored: hash,
            recomputed: hash,
            canonical: hash,
            tag,
            extension: Vec::new(),
            base: hash,
        });
    }
    let stored = read_array::<32>(reader)?;
    let (local, recomputed, canonical, extension, base) = match tag {
        1 => {
            ensure!(depth == 64, "Incomplete legacy leaf key");
            let (local, raw, canonical) = if level == 0 {
                stats.dapps += 1;
                let child = tree(reader, 1, stats)?;
                if child.tag == 0 {
                    stats.empty_dapps += 1;
                }
                (
                    leaf_hash(child.stored),
                    leaf_hash(child.recomputed),
                    if child.canonical == hash_branch_empty() {
                        hash_branch_empty()
                    } else {
                        leaf_hash(child.canonical)
                    },
                )
            } else {
                stats.accounts += 1;
                let hash = hash_leaf_value(&AccountInfo::read_value(reader)?);
                (hash, hash, hash)
            };
            (local, raw, canonical, Vec::new(), canonical)
        }
        2 => {
            ensure!(depth < 64, "Branch beyond key");
            let bitmap = u16::from_le_bytes(read_array(reader)?);
            ensure!(bitmap.count_ones() >= 2, "Noncanonical branch");
            let mut nibs = Vec::new();
            let mut stored_children = Vec::new();
            let mut raw_children = Vec::new();
            let mut canonical_children = Vec::new();
            let mut canonical_nibs = Vec::new();
            let mut canonical_bitmap = 0;
            let mut only_extension = Vec::new();
            let mut only_base = hash_branch_empty();
            for nib in 0..16 {
                let child = node(reader, level, depth + 1, stats)?;
                ensure!((child.tag != 0) == (bitmap & (1 << nib) != 0), "Bitmap mismatch");
                if child.tag != 0 {
                    nibs.push(nib);
                    stored_children.push(child.stored);
                    raw_children.push(child.recomputed);
                    if child.canonical != hash_branch_empty() {
                        canonical_nibs.push(nib);
                        canonical_bitmap |= 1 << nib;
                        canonical_children.push(child.canonical);
                        only_base =
                            if child.extension.is_empty() { child.canonical } else { child.base };
                        only_extension = child.extension;
                    }
                }
            }
            let (canonical, extension, base) = if canonical_nibs.len() == 1 {
                let mut prefix = vec![canonical_nibs[0]];
                prefix.extend(only_extension);
                (ext_hash(&prefix, &only_base), prefix, only_base)
            } else {
                let hash =
                    hash_branch_sparse(canonical_bitmap, &canonical_nibs, &canonical_children);
                (hash, Vec::new(), hash)
            };
            (
                hash_branch_sparse(bitmap, &nibs, &stored_children),
                hash_branch_sparse(bitmap, &nibs, &raw_children),
                canonical,
                extension,
                base,
            )
        }
        3 => {
            let count = usize::from(read_array::<1>(reader)?[0]);
            ensure!(count > 0 && depth + count <= 64, "Invalid extension length");
            let mut nibbles = vec![0; count];
            reader.read_exact(&mut nibbles)?;
            ensure!(nibbles.iter().all(|n| *n < 16), "Invalid nibble");
            let child = node(reader, level, depth + count, stats)?;
            ensure!(child.tag != 0, "Empty extension child");
            if child.tag == 3 {
                stats.adjacent_extensions[level] += 1;
            }
            let local = ext_hash(&nibbles, &child.stored);
            let raw = ext_hash(&nibbles, &child.recomputed);
            let base = if child.extension.is_empty() { child.canonical } else { child.base };
            nibbles.extend(child.extension);
            let canonical = if child.canonical == hash_branch_empty() {
                nibbles.clear();
                hash_branch_empty()
            } else {
                ext_hash(&nibbles, &base)
            };
            (local, raw, canonical, nibbles, base)
        }
        _ => anyhow::bail!("Invalid node tag {tag}"),
    };
    if local != stored {
        stats.invalid_local_hashes[level] += 1;
        if stats.invalid_local_hashes[level] <= 3 {
            eprintln!("LEGACY_HASH_MISMATCH level={level} depth={depth} tag={tag} stored={stored:02x?} calculated={local:02x?}");
        }
    }
    Ok(Digest { stored, recomputed, canonical, tag, extension, base })
}

#[test]
#[ignore = "requires mainnet SNAP2V1 and zstd"]
fn audit_mainnet_legacy_hashes() -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::BufReader;
    use std::process::Command;
    use std::process::Stdio;
    let path = std::env::var("LOCAL_SNAPSHOT_PATH")
        .unwrap_or_else(|_| "/Volumes/x5/_/latest-main-snapshot".into());
    let mut file = File::open(path)?;
    ensure!(read_array::<4>(&mut file)? == *b"ZSTD", "Expected compressed snapshot");
    let mut child = Command::new("zstd")
        .arg("-dc")
        .stdin(Stdio::from(file))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, child.stdout.take().unwrap());
    let result = (|| -> anyhow::Result<_> {
        ensure!(read_array::<7>(&mut reader)? == *b"SNAP2V1", "Expected legacy snapshot");
        let prefix_len = u64::from_le_bytes(read_array(&mut reader)?);
        ensure!(
            std::io::copy(&mut (&mut reader).take(prefix_len), &mut std::io::sink())? == prefix_len,
            "Truncated prefix"
        );
        let len = u64::from_le_bytes(read_array(&mut reader)?);
        let mut source = (&mut reader).take(len);
        let mut stats = Stats::default();
        let started = Instant::now();
        let root = tree(&mut source, 0, &mut stats)?;
        ensure!(source.limit() == 0, "Trailing map bytes");
        eprintln!("LEGACY_HASH_AUDIT elapsed_s={:.3} {stats:?} stored_root={:02x?} recomputed_root={:02x?} canonical_root={:02x?}", started.elapsed().as_secs_f64(), root.stored, root.recomputed, root.canonical);
        ensure!(root.stored == root.recomputed, "Stored legacy root is inconsistent with its tree");
        if let Ok(expected) = std::env::var("LOCAL_EXPECTED_ACCOUNT_MAP_ROOT") {
            ensure!(
                format!("{:?}", node_types::ThreadAccountsHash::new(root.canonical)) == expected,
                "Normalized legacy root does not match rebuilt map"
            );
        }
        Ok(())
    })();
    drop(reader);
    let _ = child.kill();
    child.wait()?;
    result
}
