use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use serde::Deserialize;
use serde::Serialize;

use crate::node::SignerIndex;
use crate::types::BlockIdentifier;

type DescendantsChainLength = usize;

#[derive(Clone, Serialize, Deserialize, PartialEq)]
struct Stats {
    min_attestations_threshold: usize,
    signers: HashSet<SignerIndex>,
    descendants_chain_length: DescendantsChainLength,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockStatistics {
    window_size: usize,
    incomplete_statistics: HashMap<BlockIdentifier, Stats>,
    complete_statistics: HashMap<BlockIdentifier, DescendantsChainLength>,
    dequeue_order: VecDeque<BlockIdentifier>,
    // dynamic parameter \beta
    median_descendants_chain_length_to_meet_threshold: usize,
}

impl BlockStatistics {
    pub fn zero(
        initial_window_size: usize,
        preset_median_descendants_chain_length_to_meet_threshold: usize,
    ) -> Self {
        Self {
            window_size: initial_window_size,
            incomplete_statistics: HashMap::new(),
            complete_statistics: HashMap::new(),
            dequeue_order: VecDeque::new(),
            median_descendants_chain_length_to_meet_threshold:
                preset_median_descendants_chain_length_to_meet_threshold,
        }
    }

    pub fn median_descendants_chain_length_to_meet_threshold(&self) -> usize {
        self.median_descendants_chain_length_to_meet_threshold
    }

    pub fn next(
        // Intentionally consuming
        mut self,
        block_identifier: BlockIdentifier,
        // Note: expected number of signers * 2/3
        block_min_attestations_threshold: usize,
        attestations: Vec<(BlockIdentifier, HashSet<SignerIndex>)>,
        // It is possible to override window size.
        // Must be propagated from the common section.
        new_window_size: Option<usize>,
    ) -> Self {
        // Update in-place all incomplete block statistics
        self.incomplete_statistics.retain(|_, e| {
            e.descendants_chain_length += 1;
            true
        });
        // Add an empty stats for the new block.
        self.incomplete_statistics.insert(
            block_identifier.clone(),
            Stats {
                min_attestations_threshold: block_min_attestations_threshold,
                signers: HashSet::new(),
                descendants_chain_length: 0,
            },
        );
        self.dequeue_order.push_back(block_identifier);

        for attestation in attestations.into_iter() {
            let (attestation_target, signers) = attestation;
            let Some(mut stats) = self.incomplete_statistics.remove(&attestation_target) else {
                continue;
            };
            stats.signers.extend(signers.into_iter());
            if stats.signers.len() >= stats.min_attestations_threshold {
                self.complete_statistics.insert(attestation_target, stats.descendants_chain_length);
            } else {
                self.incomplete_statistics.insert(attestation_target, stats);
            }
        }

        if let Some(window) = new_window_size {
            self.window_size = window;
        }

        while self.complete_statistics.len() > self.window_size {
            let eviction_target = self.dequeue_order.pop_front().unwrap();
            self.complete_statistics.remove(&eviction_target);
            self.incomplete_statistics.remove(&eviction_target);
        }

        if self.window_size < self.complete_statistics.len() {
            // No stat updates
            // Not absolutely right, but quick and works in absolutely most cases.
            return self;
        }

        let mut histogramm = BTreeMap::<DescendantsChainLength, usize>::new();
        let mut histogramm_incompletes = BTreeMap::<DescendantsChainLength, usize>::new();
        for blocks_count in self.complete_statistics.values() {
            histogramm.entry(*blocks_count).and_modify(|counter| *counter += 1).or_insert(1);
        }

        for stats in self.incomplete_statistics.values() {
            histogramm_incompletes
                .entry(stats.descendants_chain_length)
                .and_modify(|counter| *counter += 1)
                .or_insert(1);
        }

        let mut checkpoints = vec![];
        checkpoints.extend(histogramm.keys().cloned());
        checkpoints.extend(histogramm_incompletes.keys().cloned());
        checkpoints.sort();
        checkpoints.dedup();
        let mut left = 0;
        let mut right = self.dequeue_order.len();
        let mut median_descendants_chain_length_to_meet_threshold = None;
        for checkpoint in checkpoints.into_iter() {
            if let Some(complete_stats) = histogramm.get(&checkpoint) {
                left += complete_stats;
                right -= complete_stats;
            }
            if let Some(incomplete_stats) = histogramm_incompletes.get(&checkpoint) {
                right -= incomplete_stats;
            }
            if left >= right {
                median_descendants_chain_length_to_meet_threshold = Some(checkpoint);
                break;
            }
        }
        if left + right >= self.window_size {
            if let Some(median_descendants_chain_length_to_meet_threshold) =
                median_descendants_chain_length_to_meet_threshold
            {
                self.median_descendants_chain_length_to_meet_threshold =
                    median_descendants_chain_length_to_meet_threshold;
            }
        }
        self
    }

    pub fn attestations_watched(&self) -> HashSet<BlockIdentifier> {
        self.dequeue_order.clone().into_iter().collect()
    }
}

impl std::fmt::Debug for BlockStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut stats = vec![];
        for id in self.dequeue_order.iter() {
            stats.push({
                if let Some(stats) = self.incomplete_statistics.get(id) {
                    let value = stats.descendants_chain_length;
                    format!(">{value}")
                } else {
                    let value = self.complete_statistics.get(id).unwrap();
                    format!("{value}")
                }
            });
        }
        let stats = stats.join("; ");
        write!(
            f,
            "BlockStatistics(window size: {}; median_descendants_chain_length_to_meet_threshold: {}; details: {})",
            self.window_size, self.median_descendants_chain_length_to_meet_threshold,
            stats
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_NUMBER_OF_SIGNERS: usize = 5;

    fn into_id(i: u32) -> BlockIdentifier {
        let mut id_as_slice = [0u8; 32];
        id_as_slice[..4].clone_from_slice(&(i + 1).to_be_bytes());
        BlockIdentifier::from(tvm_types::UInt256::from(id_as_slice))
    }

    #[test]
    fn example() {
        let mut stats = BlockStatistics::zero(5, 3);
        stats.append(1, vec![]);
        stats.append(2, vec![(1, vec![1, 2, 3])]);
        stats.append(3, vec![(1, vec![3, 4]), (2, vec![1, 2, 3, 4, 5, 6])]);
        stats.append(4, vec![(3, vec![1, 2])]);
        stats.append(5, vec![(1, vec![6])]);
        stats.append(6, vec![(4, vec![3, 4, 5, 6])]);
        stats.append(
            7,
            vec![
                (3, vec![3, 5, 6]),
                (4, vec![1, 2, 3, 4, 5, 6]),
                (5, vec![1, 2, 3, 5, 6]),
                (6, vec![2, 3, 4, 5, 6]),
            ],
        );
        assert!(stats.median_descendants_chain_length_to_meet_threshold() == 2);
    }

    trait TestAppend {
        fn append(&mut self, id: u32, attestations: Vec<(u32, Vec<SignerIndex>)>);
    }

    impl TestAppend for BlockStatistics {
        fn append(&mut self, block_id: u32, attestations: Vec<(u32, Vec<SignerIndex>)>) {
            println!("Adding next block: <{block_id}>. Attestations -> {attestations:?}");
            let next = self.clone().next(
                into_id(block_id),
                EXPECTED_NUMBER_OF_SIGNERS,
                attestations
                    .into_iter()
                    .map(|e| {
                        let (id, signers) = e;
                        (into_id(id), HashSet::from_iter(signers))
                    })
                    .collect(),
                None,
            );
            println!("Resulting statistics: {next:?}");
            println!("-------------------------");
            *self = next;
        }
    }
}
