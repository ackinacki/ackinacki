//! Production account-map partition benchmark. No node, account bodies, or DB.
//! Leaves contain synthetic hashes; canonical byte sizes live only in augmentation.
//! Run: cargo bench -p account-state --bench account_partition -- count dapps target_mib rounds
//! Without parameters (including `cargo test --all-targets`), runs fixture checks only.
use std::hint::black_box;
use std::time::Instant;

use account_state::AccountCapacity;
use account_state::AccountInfo;
use account_state::AccountPartition;
use account_state::ThreadAccountMap;
use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;

/// Synthetic canonical size without allocating an account body. Most accounts
/// are 1–16 KiB; every 100,003rd account is 256 KiB–2 MiB to exercise partitioning
/// with rare large leaves. Hash-derived sizes keep benchmark datasets reproducible.
fn synthetic_account_size(index: usize, hash: &[u8; 32]) -> u64 {
    if index.is_multiple_of(100_003) {
        256 * 1024
            + u64::from_le_bytes(hash[8..16].try_into().unwrap())
                % (2 * 1024 * 1024 - 256 * 1024 + 1)
    } else {
        1024 + u64::from_le_bytes(hash[..8].try_into().unwrap()) % (15 * 1024 + 1)
    }
}

fn build(count: usize, dapps: usize) -> ThreadAccountMap {
    let mut map = ThreadAccountMap::new();
    for dapp in 0..dapps {
        let id = DAppIdentifier::new(*blake3::hash(&(dapp as u64).to_le_bytes()).as_bytes());
        let updates: Vec<_> = (dapp * (count / dapps)..(dapp + 1) * (count / dapps))
            .map(|index| {
                let hash = *blake3::hash(&(index as u64).to_le_bytes()).as_bytes();
                let size = synthetic_account_size(index, &hash);
                let routing = AccountRouting::new(id, AccountIdentifier::new(hash));
                let value =
                    AccountInfo::AccountHash(AccountHash::new(*blake3::hash(&hash).as_bytes()));
                (routing, Some((value, AccountCapacity::new(size))))
            })
            .collect();
        map = map.update(&updates);
    }
    map
}

fn fingerprint(parts: &[AccountPartition]) -> Vec<([u8; 32], u64)> {
    parts.iter().map(|part| (*part.map.hash().as_array(), part.capacity)).collect()
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).filter(|arg| arg != "--bench").collect();
    if args.is_empty() || args == ["--test"] {
        tests::synthetic_sizes_include_both_range_boundaries();
        tests::synthetic_dataset_uses_production_maps_and_reconstructs();
        println!("Account partition benchmark fixture checks passed");
        return;
    }
    assert_eq!(args.len(), 4, "usage: cargo bench -p account-state --bench account_partition -- count dapps target_mib rounds");
    let count: usize = args[0].parse().unwrap();
    let dapps: usize = args[1].parse().unwrap();
    let target: u64 = args[2].parse::<u64>().unwrap().checked_mul(1024 * 1024).unwrap();
    let rounds: usize = args[3].parse().unwrap();
    assert!(count > 0 && dapps > 0 && count.is_multiple_of(dapps) && target > 0 && rounds > 0);

    let start = Instant::now();
    let source = build(count, dapps);
    println!("DATASET leaves={count} dapps={dapps} accounts_per_dapp={} build_s={:.3} total_bytes={} target_bytes={target} bodies=none", count / dapps, start.elapsed().as_secs_f64(), source.capacity().bytes());
    let source_root = source.hash();
    let expected = source.partition_by_capacity(target).unwrap();
    let roots = fingerprint(&expected);
    let mut original = source.iter();
    let mut seen = 0;
    for part in &expected {
        assert_eq!(part.capacity, part.map.capacity().bytes());
        for (key, value) in part.map.iter() {
            assert_eq!(original.next(), Some((key, value)));
            seen += 1;
        }
    }
    assert_eq!(seen, count);
    assert!(original.next().is_none());
    let mut partition_times = Vec::new();
    let mut merge_times = Vec::new();
    for _ in 0..rounds {
        let start = Instant::now();
        let parts = (black_box(&source)).partition_by_capacity(black_box(target)).unwrap();
        partition_times.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(fingerprint(&parts), roots);
        let start = Instant::now();
        let merged = ThreadAccountMap::merge_partitions(black_box(&parts)).unwrap();
        merge_times.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(merged.hash(), source_root);
        assert_eq!(merged.capacity(), source.capacity());
        black_box((&parts, &merged));
    }
    assert_eq!(source.hash(), source_root);
    let sizes: Vec<_> = expected.iter().map(|part| part.capacity).collect();
    println!("PARTITIONS count={} sizes_bytes={sizes:?}", sizes.len());
    println!("TIMING rounds={rounds} partition_with_map_assembly_median_ms={:.6} reverse_merge_median_ms={:.6} verification_and_drop=excluded", median(&mut partition_times), median(&mut merge_times));
    println!("VERIFIED ordered_leaves={seen} deterministic_roots=true original_unchanged=true merged_root_matches=true");
}

mod tests {
    use super::*;

    pub fn synthetic_sizes_include_both_range_boundaries() {
        let mut hash = [0; 32];
        assert_eq!(synthetic_account_size(1, &hash), 1024);
        assert_eq!(synthetic_account_size(0, &hash), 256 * 1024);
        hash[..8].copy_from_slice(&(15_u64 * 1024).to_le_bytes());
        hash[8..16].copy_from_slice(&(2_u64 * 1024 * 1024 - 256 * 1024).to_le_bytes());
        assert_eq!(synthetic_account_size(1, &hash), 16 * 1024);
        assert_eq!(synthetic_account_size(100_003, &hash), 2 * 1024 * 1024);
    }

    pub fn synthetic_dataset_uses_production_maps_and_reconstructs() {
        for dapps in [1, 10, 100] {
            let source = build(100, dapps);
            let parts = source.partition_by_capacity(100_000).unwrap();
            assert_eq!((ThreadAccountMap::merge_partitions(&parts).unwrap()).hash(), source.hash());
            assert_eq!(source.iter().count(), 100);
        }
    }
}
