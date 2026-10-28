use super::*;
use crate::block_keeper_system::BlockKeeperData;

fn node(seed: u8) -> NodeIdentifier {
    AccountIdentifier::new([seed; 32]).into()
}

fn bk_set(reverse: bool) -> BlockKeeperSet {
    let order: Vec<_> = if reverse { (1..=12u8).rev().collect() } else { (1..=12u8).collect() };
    let mut set = BlockKeeperSet::new();
    for i in order {
        // Deliberately use signer indexes unrelated to raw node-ID order.
        set.insert(
            (100 - i) as u16,
            BlockKeeperData {
                owner_address: AccountIdentifier::new([i; 32]),
                ..Default::default()
            },
        );
    }
    set
}

fn manifest() -> ThreadSnapshotManifest {
    ThreadSnapshotManifest {
        account_partition_roots: vec![[1; 32], [2; 32]],
        normal_message_partition_roots: vec![[3; 32]],
        high_priority_message_partition_roots: vec![[4; 32]],
    }
}

#[test]
fn snapshot_schedule_matches_balanced_reference_and_covers_every_partition_once(
) -> anyhow::Result<()> {
    for p in 0..38 {
        for n in 1..18 {
            let mut first_wave = std::collections::BTreeSet::new();
            let mut owners = vec![0; p];
            for r in 0..n {
                let actual: Vec<_> = partition_publication_order(p, r, n)?.collect();
                let primary: Vec<_> = (0..p).filter(|index| index % n == r).collect();
                for &index in &primary {
                    owners[index] += 1;
                }
                let expected: Vec<_> = if p == 0 {
                    Vec::new()
                } else {
                    let cycle_len = p / publisher_gcd(p, n);
                    (0..p)
                        .map(|column| (r + column % cycle_len * n + column / cycle_len) % p)
                        .collect()
                };
                if p > 0 {
                    first_wave.insert(actual[0]);
                }
                assert_eq!(actual, expected, "P={p}, N={n}, r={r}");
                for (position, &index) in actual.iter().enumerate() {
                    assert_eq!(partition_publication_position(p, r, n, index)?, position);
                }
                assert_eq!(&actual[..primary.len()], primary);
                let mut sorted = actual.clone();
                sorted.sort_unstable();
                assert_eq!(sorted, (0..p).collect::<Vec<_>>());
                // Disjoint primary sequences cover all indexes within ceil(P/N) waves.
                assert!(primary.len() <= p.div_ceil(n));
            }
            assert!(owners.iter().all(|count| *count == 1));
            if n >= p {
                assert_eq!(first_wave, (0..p).collect());
            }
        }
    }
    Ok(())
}

#[test]
fn snapshot_schedule_ranks_frozen_bk_members_independently_of_insertion_and_signer_index(
) -> anyhow::Result<()> {
    let manifest = manifest();
    assert_eq!(manifest_partition_count(&manifest), 4);
    let expected = rank_snapshot_publishers(&bk_set(false), &manifest)?;
    assert_eq!(expected, rank_snapshot_publishers(&bk_set(true), &manifest)?);
    let mut reindexed = BlockKeeperSet::new();
    for (i, node) in expected.iter().enumerate() {
        reindexed.insert(
            i as u16,
            BlockKeeperData { owner_address: node.clone().into(), ..Default::default() },
        );
    }
    assert_eq!(expected, rank_snapshot_publishers(&reindexed, &manifest)?);
    let mut epochs = bk_set(false);
    epochs.insert(
        999,
        BlockKeeperData { owner_address: AccountIdentifier::new([1; 32]), ..Default::default() },
    );
    assert_eq!(expected, rank_snapshot_publishers(&epochs, &manifest)?);
    // Removing the additional epoch must not remove the still-committed node
    // from scheduling, even if the BK reverse lookup no longer lists it.
    epochs.remove_signer(&999);
    assert_eq!(expected, rank_snapshot_publishers(&epochs, &manifest)?);
    let hash = snapshot_manifest_hash(&manifest);
    for id in &expected {
        let mut input = PUBLISHER_RANK_DOMAIN.to_vec();
        input.extend_from_slice(&hash);
        input.extend_from_slice(AccountIdentifier::from(id.clone()).as_slice());
        assert_eq!(publisher_rank_key(&hash, id), *blake3::hash(&input).as_bytes());
        let mut wrong = PUBLISHER_RANK_DOMAIN.to_vec();
        wrong.extend_from_slice(&hash);
        wrong.extend_from_slice(id.to_string().as_bytes());
        assert_ne!(publisher_rank_key(&hash, id), *blake3::hash(&wrong).as_bytes());
    }
    assert!(expected.windows(2).all(|pair| (publisher_rank_key(&hash, &pair[0]), &pair[0])
        < (publisher_rank_key(&hash, &pair[1]), &pair[1])));
    let mut changed = manifest.clone();
    changed.account_partition_roots[0][0] ^= 1;
    assert_ne!(
        publisher_rank_key(&snapshot_manifest_hash(&changed), &expected[0]),
        publisher_rank_key(&hash, &expected[0])
    );
    Ok(())
}

#[test]
fn snapshot_schedule_hash_ties_break_by_canonical_node_bytes() {
    let tied =
        vec![([8; 32], node(255)), ([8; 32], node(1)), ([8; 32], node(128)), ([7; 32], node(9))];
    assert_eq!(sort_publisher_ranks(tied), vec![node(9), node(1), node(128), node(255)]);
}

#[test]
fn balanced_snapshot_schedule_is_a_permutation_with_balanced_every_column_prefix(
) -> anyhow::Result<()> {
    for p in 0..49 {
        for n in 1..33 {
            let rows = (0..n)
                .map(|rank| {
                    balanced_partition_publication_order(p, rank, n)
                        .map(|order| order.collect::<Vec<_>>())
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            for (rank, row) in rows.iter().enumerate() {
                let order = balanced_partition_publication_order(p, rank, n)?;
                assert_eq!(order.clone().collect::<Vec<_>>(), *row);
                assert_eq!(order.collect::<Vec<_>>(), *row);
                let mut sorted = row.clone();
                sorted.sort_unstable();
                assert_eq!(sorted, (0..p).collect::<Vec<_>>(), "P={p}, N={n}, rank={rank}");
            }
            let mut copies = vec![0; p];
            for column in 0..p {
                for row in &rows {
                    copies[row[column]] += 1;
                }
                let total = (column + 1) * n;
                assert!(
                    copies.iter().all(|&count| count == total / p || count == total.div_ceil(p)),
                    "P={p}, N={n}, column={column}, copies={copies:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn balanced_snapshot_schedule_covers_every_partition_at_failure_lower_bound() -> anyhow::Result<()>
{
    // Exhaust actual absent-node subsets, not just failures at selected ranks.
    for p in 1..19 {
        for n in 1..9 {
            let rows = (0..n)
                .map(|rank| {
                    balanced_partition_publication_order(p, rank, n)
                        .map(|order| order.collect::<Vec<_>>())
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            for absent in 0u32..(1 << n) {
                let f = absent.count_ones() as usize;
                if f == n {
                    continue;
                }
                let columns = (p * (f + 1)).div_ceil(n);
                let mut covered = vec![false; p];
                for (rank, row) in rows.iter().enumerate() {
                    if absent & (1 << rank) == 0 {
                        for &part in &row[..columns] {
                            covered[part] = true;
                        }
                    }
                }
                assert!(covered.into_iter().all(|ready| ready), "P={p}, N={n}, absent={absent:b}");
            }
        }
    }
    // The current snapshot: four distinct copies after seven columns guarantee
    // coverage with any three absent publishers (10%, rounded conservatively).
    let mut publishers = [0u32; 39];
    for rank in 0..24 {
        for index in balanced_partition_publication_order(39, rank, 24)?.take(7) {
            publishers[index] |= 1 << rank;
        }
    }
    assert!(publishers.iter().all(|bits| bits.count_ones() >= 4));
    Ok(())
}

#[test]
fn balanced_snapshot_schedule_validates_inputs_and_is_lazy_at_machine_limits() -> anyhow::Result<()>
{
    assert!(balanced_partition_publication_order(4, 0, 0).is_err());
    assert!(balanced_partition_publication_order(4, 3, 3).is_err());
    assert_eq!(balanced_partition_publication_order(0, 3, 4)?.count(), 0);
    assert_eq!(
        balanced_partition_publication_order(usize::MAX, 0, 1)?.take(3).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        balanced_partition_publication_order(usize::MAX, usize::MAX - 1, usize::MAX)?
            .take(3)
            .collect::<Vec<_>>(),
        vec![usize::MAX - 1, 0, 1]
    );
    assert_eq!(
        balanced_partition_publication_order(3, usize::MAX - 1, usize::MAX)?.collect::<Vec<_>>(),
        vec![2, 0, 1]
    );
    assert_eq!(
        balanced_partition_publication_order(6, 0, 4)?.collect::<Vec<_>>(),
        vec![0, 4, 2, 1, 5, 3]
    );
    // Coprime counts, equal counts, more publishers than partitions and the
    // non-coprime production case are covered by the prefix/permutation tests.
    Ok(())
}

#[test]
fn balanced_snapshot_schedule_last_export_missing_publishers() -> anyhow::Result<()> {
    // Frozen rank -> bk number for d3ce1cf7…: 38 account partitions + one
    // normal-message partition. The four importing nodes were not exporting.
    let ranked =
        [7, 18, 20, 9, 6, 1, 4, 19, 21, 13, 23, 14, 3, 15, 2, 5, 10, 11, 0, 8, 17, 16, 22, 12];
    let mut first_available = [usize::MAX; 39];
    let mut old_available = [usize::MAX; 39];
    for (rank, node) in ranked.iter().enumerate() {
        let order: Vec<_> = balanced_partition_publication_order(39, rank, ranked.len())?.collect();
        println!(
            "bk{node:<2} {}",
            order.iter().map(|index| format!("{index:2}")).collect::<Vec<_>>().join(" ")
        );
        if [2, 20, 21, 22].contains(node) {
            continue;
        }
        for (column, index) in order.into_iter().enumerate() {
            first_available[index] = first_available[index].min(column + 1);
        }
        for (column, index) in
            legacy_partition_publication_order(39, rank, ranked.len())?.enumerate()
        {
            old_available[index] = old_available[index].min(column + 1);
        }
    }
    assert_eq!(first_available[..38].iter().max(), Some(&3));
    assert_eq!(first_available[38], 4);
    assert_eq!(old_available[..38].iter().max(), Some(&10));
    assert_eq!(old_available[38], 16);
    Ok(())
}

#[test]
fn snapshot_schedule_balanced_inverse_roundtrips_machine_sized_positions() -> anyhow::Result<()> {
    for p in [1, 6, 39, usize::MAX / 2 + 1, usize::MAX - 1, usize::MAX] {
        for n in [1, 3, 24, usize::MAX - 1, usize::MAX] {
            let cycle_len = p / publisher_gcd(p, n);
            for rank in [0, n / 2, n - 1] {
                for column in [0, p / 2, p - 1] {
                    // Independent closed-form reference uses a wider product,
                    // while the iterator itself uses overflow-free additions.
                    let step = (column % cycle_len) as u128 * n as u128 % p as u128;
                    let index =
                        ((rank % p) as u128 + step + (column / cycle_len) as u128) % p as u128;
                    assert_eq!(
                        partition_publication_position(p, rank, n, index as usize)?,
                        column,
                        "P={p}, N={n}, rank={rank}, column={column}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn snapshot_schedule_retains_frozen_ranks_after_peer_failure_and_remains_lazy() -> anyhow::Result<()>
{
    let ranked = rank_snapshot_publishers(&bk_set(false), &manifest())?;
    // The consumer may select live candidates, but must not rerank that subset.
    for (r, _survivor) in ranked.iter().enumerate().filter(|(r, _)| r % 3 != 0) {
        let mut order = partition_publication_order(23, r, ranked.len())?;
        let clone = order.clone();
        assert_eq!(clone.collect::<Vec<_>>(), order.by_ref().collect::<Vec<_>>());
        let mut all = partition_publication_order(23, r, ranked.len())?.collect::<Vec<_>>();
        all.sort_unstable();
        assert_eq!(all, (0..23).collect::<Vec<_>>());
    }
    // Huge counts do not allocate a partition-sized array or bitmap, and cyclic
    // wrap / primary stepping do not overflow machine-sized indexes.
    assert_eq!(
        partition_publication_order(usize::MAX, 0, 1)?.take(3).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        partition_publication_order(usize::MAX, usize::MAX - 1, usize::MAX)?
            .take(3)
            .collect::<Vec<_>>(),
        vec![usize::MAX - 1, 0, 1]
    );
    assert_eq!(
        partition_publication_order(3, usize::MAX - 1, usize::MAX)?.collect::<Vec<_>>(),
        vec![2, 0, 1]
    );
    Ok(())
}

#[test]
fn snapshot_schedule_rejects_empty_publishers_and_invalid_ranks_but_allows_empty_manifests() {
    assert!(rank_snapshot_publishers(&BlockKeeperSet::new(), &manifest()).is_err());
    assert!(partition_publication_order(4, 0, 0).is_err());
    assert!(partition_publication_order(4, 3, 3).is_err());
    assert_eq!(partition_publication_order(0, 0, 1).unwrap().count(), 0);
    assert_eq!(partition_publication_order(0, 3, 4).unwrap().count(), 0);
    assert_eq!(manifest_partition_count(&ThreadSnapshotManifest::default()), 0);
    assert_eq!(
        rank_snapshot_publishers(&bk_set(false), &ThreadSnapshotManifest::default()).unwrap().len(),
        12
    );
}

#[test]
fn snapshot_schedule_fixed_rank_and_order_vector() -> anyhow::Result<()> {
    let manifest = manifest();
    let ranked = rank_snapshot_publishers(&bk_set(false), &manifest)?;
    let rank_seeds: Vec<_> =
        ranked.iter().map(|id| AccountIdentifier::from(id.clone()).as_array()[0]).collect();
    let hash = snapshot_manifest_hash(&manifest);
    assert_eq!(
        (hex::encode(publisher_rank_key(&hash, &node(1))), rank_seeds),
        (
            "9ffc090d18498f93e983d960bf511a34476c5621521926987b2ba9e6cf82f789".to_string(),
            vec![9, 11, 12, 3, 10, 1, 5, 2, 8, 7, 4, 6],
        )
    );
    assert_eq!(
        partition_publication_order(10, 1, 3)?.collect::<Vec<_>>(),
        vec![1, 4, 7, 0, 3, 6, 9, 2, 5, 8]
    );
    Ok(())
}
