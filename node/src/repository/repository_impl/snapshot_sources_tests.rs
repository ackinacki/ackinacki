use node_types::AccountIdentifier;

use super::super::snapshot_schedule::partition_publication_order;
use super::*;
use crate::block_keeper_system::BlockKeeperData;

fn fixture() -> (ThreadSnapshotManifest, BlockKeeperSet, Vec<NodeIdentifier>) {
    let manifest = ThreadSnapshotManifest {
        account_partition_roots: vec![[1; 32], [2; 32], [3; 32]],
        normal_message_partition_roots: vec![[4; 32]],
        high_priority_message_partition_roots: vec![[5; 32]],
    };
    let mut bk = BlockKeeperSet::new();
    for i in 1..=7u8 {
        bk.insert(
            i as u16,
            BlockKeeperData {
                owner_address: AccountIdentifier::new([i; 32]),
                ..Default::default()
            },
        );
    }
    let ranked = rank_snapshot_publishers(&bk, &manifest).unwrap();
    (manifest, bk, ranked)
}

fn endpoint(rank: usize) -> Url {
    Url::parse(&format!("http://node-{rank}.invalid/v2/storage/")).unwrap()
}

#[test]
fn snapshot_sources_match_publisher_order_and_ignore_peer_discovery_order() -> anyhow::Result<()> {
    let (manifest, bk, ranked) = fixture();
    let known: Vec<_> =
        ranked.iter().enumerate().map(|(rank, node)| (node.clone(), endpoint(rank))).collect();
    let plan = SnapshotSourcePlan::new(&manifest, &bk, known.clone())?;
    let reverse = SnapshotSourcePlan::new(&manifest, &bk, known.into_iter().rev())?;
    for index in 0..5 {
        let mut reference: Vec<_> = ranked
            .iter()
            .enumerate()
            .map(|(rank, _)| {
                let position = partition_publication_order(5, rank, ranked.len())
                    .unwrap()
                    .position(|part| part == index)
                    .unwrap();
                (position, rank)
            })
            .collect();
        reference.sort_unstable();
        let expected: Vec<_> = reference
            .into_iter()
            .map(|(_, rank)| {
                endpoint(rank)
                    .join(&hex::encode(manifest.partition_blob_id(index).unwrap()))
                    .unwrap()
            })
            .collect();
        let actual = plan.partition_sources(index)?;
        assert_eq!(actual, expected);
        assert_eq!(actual, reverse.partition_sources(index)?);
        // At least the primary owner prepares this partition in its first wave.
        assert!(actual[0].as_str().starts_with(endpoint(index).as_str()));
    }
    assert!(plan.partition_sources(5).is_err());
    Ok(())
}

#[test]
fn snapshot_sources_missing_nodes_keep_full_committed_ranks_and_replica_preferences(
) -> anyhow::Result<()> {
    let (manifest, bk, ranked) = fixture();
    let known = vec![
        (ranked[2].clone(), endpoint(2)),
        (ranked[5].clone(), endpoint(5)),
        (AccountIdentifier::new([99; 32]).into(), endpoint(99)),
    ];
    let plan = SnapshotSourcePlan::new(&manifest, &bk, known)?;
    assert_eq!(plan.publishers.len(), ranked.len());
    for index in 0..5 {
        let mut reference: Vec<_> = [2, 5]
            .into_iter()
            .map(|rank| {
                (
                    partition_publication_order(5, rank, 7)
                        .unwrap()
                        .position(|part| part == index)
                        .unwrap(),
                    rank,
                )
            })
            .collect();
        reference.sort_unstable();
        assert_eq!(
            plan.partition_sources(index)?,
            reference
                .into_iter()
                .map(|(_, rank)| endpoint(rank)
                    .join(&hex::encode(manifest.partition_blob_id(index).unwrap()))
                    .unwrap())
                .collect::<Vec<_>>()
        );
    }
    let empty = SnapshotSourcePlan::new(&manifest, &bk, [])?;
    assert!(empty.partition_sources(0)?.is_empty());
    Ok(())
}

#[test]
fn snapshot_sources_multiple_endpoints_deduplicate_and_reject_ambiguous_owners(
) -> anyhow::Result<()> {
    let (manifest, bk, ranked) = fixture();
    let alternate = Url::parse("https://alternate.invalid/v2/storage/")?;
    let plan = SnapshotSourcePlan::new(
        &manifest,
        &bk,
        [
            (ranked[0].clone(), endpoint(0)),
            (ranked[0].clone(), alternate.clone()),
            (ranked[0].clone(), endpoint(0)),
        ],
    )?;
    assert_eq!(plan.partition_sources(0)?.len(), 2);
    assert!(SnapshotSourcePlan::new(
        &manifest,
        &bk,
        [(ranked[0].clone(), endpoint(0)), (ranked[1].clone(), endpoint(0)),]
    )
    .is_err());
    for bad in [
        "file:///cache/",
        "http://node.invalid/storage",
        "http://user:pass@node.invalid/",
        "http://node.invalid/?query",
        "http://node.invalid/#fragment",
    ] {
        assert!(SnapshotSourcePlan::new(&manifest, &bk, [(ranked[0].clone(), Url::parse(bad)?)])
            .is_err());
    }
    Ok(())
}

#[test]
fn snapshot_sources_inverse_schedule_handles_huge_indexes_without_iteration() -> anyhow::Result<()>
{
    for index in [0, 1, usize::MAX / 2, usize::MAX - 1] {
        assert_eq!(partition_publication_position(usize::MAX, 0, 1, index)?, index);
        assert_eq!(partition_publication_position(usize::MAX, index, usize::MAX, index)?, 0);
        assert!(
            partition_publication_position(usize::MAX, usize::MAX - 1, usize::MAX, index)?
                < usize::MAX
        );
    }
    assert!(partition_publication_position(0, 0, 1, 0).is_err());
    assert!(partition_publication_position(1, 0, 0, 0).is_err());
    Ok(())
}
