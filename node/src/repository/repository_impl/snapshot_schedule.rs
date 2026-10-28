//! Pure scheduling over a frozen committed BK set. This does not publish files.
use std::collections::BTreeSet;

use node_types::AccountIdentifier;

use super::snapshot_manifest::snapshot_manifest_hash;
use super::snapshot_manifest::ThreadSnapshotManifest;
use crate::block_keeper_system::BlockKeeperSet;
use crate::node::NodeIdentifier;

const PUBLISHER_RANK_DOMAIN: &[u8] = b"ACKI_NACKI_SNAPSHOT_PUBLISHER_RANK_V1";

/// Canonical index space: accounts, normal inboxes, then high-priority inboxes.
/// The primary blob is not a manifest member and has no partition index.
pub(crate) fn manifest_partition_count(manifest: &ThreadSnapshotManifest) -> usize {
    manifest.account_partition_roots.len()
        + manifest.normal_message_partition_roots.len()
        + manifest.high_priority_message_partition_roots.len()
}

/// Call once with the publication block's committed BK set, not a filtered list
/// of live peers. Store/reuse the result for this publication. Signer indexes,
/// set insertion order and subsequent topology changes do not affect node rank.
pub(crate) fn rank_snapshot_publishers(
    committed_bk_set: &BlockKeeperSet,
    manifest: &ThreadSnapshotManifest,
) -> anyhow::Result<Vec<NodeIdentifier>> {
    // Rank physical nodes, not signer entries/epochs. Derive membership from
    // authoritative BK entries rather than a mutable reverse-lookup cache.
    let publishers: BTreeSet<_> = committed_bk_set.iter().map(|keeper| keeper.node_id()).collect();
    anyhow::ensure!(!publishers.is_empty(), "snapshot publisher set is empty");
    let hash = snapshot_manifest_hash(manifest);
    let ranked =
        publishers.into_iter().map(|node| (publisher_rank_key(&hash, &node), node)).collect();
    Ok(sort_publisher_ranks(ranked))
}

fn publisher_rank_key(manifest_hash: &[u8; 32], node: &NodeIdentifier) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(PUBLISHER_RANK_DOMAIN);
    hasher.update(manifest_hash);
    // NodeIdentifier's serde representation is a string. The rank input is
    // instead exactly the underlying canonical 32 bytes, without framing.
    hasher.update(AccountIdentifier::from(node.clone()).as_slice());
    *hasher.finalize().as_bytes()
}

fn sort_publisher_ranks(mut ranked: Vec<([u8; 32], NodeIdentifier)>) -> Vec<NodeIdentifier> {
    // NodeIdentifier's Ord is the underlying AccountIdentifier's raw byte order,
    // which also supplies the required collision tie-breaker.
    ranked.sort_unstable();
    ranked.into_iter().map(|(_, node)| node).collect()
}

/// Run only AFTER publishing primary. Publication and source selection share
/// the balanced schedule over the same frozen, manifest-ranked publisher set.
pub(crate) fn partition_publication_order(
    partition_count: usize,
    publisher_rank: usize,
    publisher_count: usize,
) -> anyhow::Result<impl Iterator<Item = usize> + Clone> {
    balanced_partition_publication_order(partition_count, publisher_rank, publisher_count)
}

/// Previous schedule retained only as a regression reference for coverage.
#[cfg(test)]
fn legacy_partition_publication_order(
    partition_count: usize,
    publisher_rank: usize,
    publisher_count: usize,
) -> anyhow::Result<impl Iterator<Item = usize> + Clone> {
    anyhow::ensure!(publisher_count > 0, "snapshot publisher set is empty");
    anyhow::ensure!(
        publisher_rank < publisher_count,
        "snapshot publisher rank is outside the committed set"
    );
    let primary = (publisher_rank..partition_count).step_by(publisher_count);
    let start = if partition_count == 0 { 0 } else { publisher_rank % partition_count };
    let tail = partition_count - start;
    // With one publisher the primary phase already covers the entire manifest.
    let replica_count = if publisher_count == 1 { 0 } else { partition_count };
    let replicas = (0..replica_count)
        .map(move |offset| {
            // Modular addition without overflowing for very large index spaces.
            if offset < tail {
                start + offset
            } else {
                offset - tail
            }
        })
        .filter(move |index| index % publisher_count != publisher_rank);
    Ok(primary.chain(replicas))
}

/// Balanced schedule shared by publication and source selection.
/// Uses the same frozen, manifest-ranked BK set as the production schedule.
///
/// Let P be the partition count, N the publisher count and g = gcd(P, N).
/// Each row first strides by N modulo P for P/g steps, then shifts its starting
/// point by one and repeats for the next residue class. The g disjoint cycles
/// make every publisher's row a permutation of all P partitions, even when P
/// and N are not coprime. No live-peer data or random state participates.
///
/// Across all publishers, every prefix of k columns assigns each partition
/// either floor(k*N/P) or ceil(k*N/P) distinct publishers. Consequently the
/// lower bound ceil(P*(f+1)/N) columns suffices against ANY f missing publishers
/// (f < N). This balances coverage, not actual export duration or peer speed.
/// The iterator uses constant space and avoids overflowing index arithmetic.
pub(crate) fn balanced_partition_publication_order(
    partition_count: usize,
    publisher_rank: usize,
    publisher_count: usize,
) -> anyhow::Result<impl Iterator<Item = usize> + Clone> {
    anyhow::ensure!(publisher_count > 0, "snapshot publisher set is empty");
    anyhow::ensure!(
        publisher_rank < publisher_count,
        "snapshot publisher rank is outside the committed set"
    );
    // The empty iterator never yields the placeholder modulus's partition.
    let modulus = partition_count.max(1);
    let cycle_len = modulus / publisher_gcd(modulus, publisher_count);
    let stride = publisher_count % modulus;
    let start = publisher_rank % modulus;
    let add = move |a: usize, b: usize| {
        if a >= modulus - b {
            a - (modulus - b)
        } else {
            a + b
        }
    };
    let (mut current, mut within_cycle, mut cycle) = (start, 0, 0);
    Ok((0..partition_count).map(move |ordinal| {
        let result = current;
        if ordinal < partition_count - 1 {
            within_cycle += 1;
            if within_cycle == cycle_len {
                within_cycle = 0;
                cycle += 1;
                current = add(start, cycle);
            } else {
                current = add(current, stride);
            }
        }
        result
    }))
}

fn publisher_gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Invert the balanced schedule arithmetically, without walking or allocating
/// a publisher's permutation. The residue identifies the cycle; a modular
/// inverse identifies its column. Euclid costs O(log P), not O(P) per source.
pub(crate) fn partition_publication_position(
    partition_count: usize,
    publisher_rank: usize,
    publisher_count: usize,
    index: usize,
) -> anyhow::Result<usize> {
    anyhow::ensure!(
        publisher_count > 0 && publisher_rank < publisher_count,
        "invalid snapshot publisher rank/count"
    );
    anyhow::ensure!(index < partition_count, "partition index outside manifest");
    let start = publisher_rank % partition_count;
    let delta = if index >= start { index - start } else { partition_count - (start - index) };
    let gcd = publisher_gcd(partition_count, publisher_count);
    let cycle_len = partition_count / gcd;
    let cycle = delta % gcd;
    if cycle_len == 1 {
        return Ok(cycle);
    }
    // N/g and P/g are coprime. i128 coefficients accommodate every supported
    // usize (up to 64 bits); u128 keeps modular multiplication overflow-free.
    let (mut remainder, mut next_remainder) =
        (cycle_len as i128, (publisher_count / gcd % cycle_len) as i128);
    let (mut coefficient, mut next_coefficient) = (0i128, 1i128);
    while next_remainder != 0 {
        let quotient = remainder / next_remainder;
        (remainder, next_remainder) = (next_remainder, remainder % next_remainder);
        (coefficient, next_coefficient) =
            (next_coefficient, coefficient - quotient * next_coefficient);
    }
    let inverse = coefficient.rem_euclid(cycle_len as i128) as u128;
    let within = ((delta / gcd) as u128 * inverse % cycle_len as u128) as usize;
    Ok(cycle * cycle_len + within)
}

#[cfg(test)]
#[path = "snapshot_schedule_tests.rs"]
mod tests;
