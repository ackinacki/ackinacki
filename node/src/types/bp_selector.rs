use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::block_keeper_system::BlockKeeperSet;
use crate::node::NodeIdentifier;
use crate::types::BlockIdentifier;

#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder, PartialEq)]
pub struct ProducerSelector {
    // Block id with last BK set change
    rng_seed_block_id: BlockIdentifier,
    // Shuffled BK set offset to find BP
    index: usize,
}

impl ProducerSelector {
    pub fn get_producer_node_id(&self, bk_set: &BlockKeeperSet) -> NodeIdentifier {
        let mut sorted_node_id_list = bk_set.iter_node_ids().collect::<Vec<_>>();
        let mut rng = SmallRng::from_seed(self.rng_seed_block_id.clone().as_rng_seed());
        sorted_node_id_list.shuffle(&mut rng);
        assert!(self.index < sorted_node_id_list.len(), "Producer selector index out of bounds");
        sorted_node_id_list
            .get(self.index)
            .expect("Producer index out of bounds")
            .to_owned()
            .clone()
    }

    pub fn is_node_bp(&self, bk_set: &BlockKeeperSet, node_id: &NodeIdentifier) -> bool {
        &self.get_producer_node_id(bk_set) == node_id
    }

    pub fn check_whether_this_node_is_bp_based_on_bk_set_and_index_offset(
        &self,
        bk_set: &BlockKeeperSet,
        node_id: &NodeIdentifier,
        offset: usize,
    ) -> bool {
        match self.get_distance_from_bp(bk_set, node_id) {
            Some(distance) => distance == (offset % bk_set.len()),
            None => false,
        }
    }

    pub fn get_distance_from_bp(
        &self,
        bk_set: &BlockKeeperSet,
        node_id: &NodeIdentifier,
    ) -> Option<usize> {
        let mut sorted_node_id_list = bk_set.iter_node_ids().collect::<Vec<_>>();
        let total_bk_cnt = sorted_node_id_list.len();
        let mut rng = SmallRng::from_seed(self.rng_seed_block_id.clone().as_rng_seed());
        sorted_node_id_list.shuffle(&mut rng);
        if let Some((position, _)) =
            sorted_node_id_list.into_iter().find_position(|id| *id == node_id)
        {
            Some(if position >= self.index {
                position - self.index
            } else {
                total_bk_cnt - self.index + position
            })
        } else {
            None
        }
    }

    pub fn move_index(self, diff: usize, bk_set_size: usize) -> Self {
        Self { rng_seed_block_id: self.rng_seed_block_id, index: (self.index + diff) % bk_set_size }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::str::FromStr;

    use tvm_types::AccountId;

    use crate::block_keeper_system::BlockKeeperData;
    use crate::block_keeper_system::BlockKeeperSet;
    use crate::node::SignerIndex;
    use crate::types::bp_selector::ProducerSelector;
    use crate::types::AccountAddress;
    use crate::types::BlockIdentifier;

    #[test]
    fn test_get_distance() {
        let mut bk_set = BlockKeeperSet::new();
        for i in 0..100 {
            let acc_id_str =
                format!("00000000000000000000000000000000000000000000000000000000{:08x}", i);
            bk_set.insert(
                i as SignerIndex,
                BlockKeeperData {
                    owner_address: AccountAddress(AccountId::from_str(&acc_id_str).unwrap()),
                    ..Default::default()
                },
            )
        }
        let producer_selector =
            ProducerSelector { rng_seed_block_id: BlockIdentifier::default(), index: 0 };
        let producer_node_id = producer_selector.get_producer_node_id(&bk_set);
        println!("Producer node ID: {}", producer_node_id);
        let mut distances_set: HashSet<usize> = HashSet::from_iter(0..100usize);
        for node_id in bk_set.iter_node_ids() {
            let dist = producer_selector.get_distance_from_bp(&bk_set, node_id);
            assert!(dist.is_some());
            assert!(distances_set.remove(&dist.unwrap()));
        }
        assert!(distances_set.is_empty());
    }

    #[test]
    fn test_move() {
        let mut bk_set = BlockKeeperSet::new();
        for i in 0..100 {
            let acc_id_str =
                format!("00000000000000000000000000000000000000000000000000000000{:08x}", i);
            bk_set.insert(
                i as SignerIndex,
                BlockKeeperData {
                    owner_address: AccountAddress(AccountId::from_str(&acc_id_str).unwrap()),
                    ..Default::default()
                },
            )
        }
        let mut producer_selector =
            ProducerSelector { rng_seed_block_id: BlockIdentifier::default(), index: 0 };
        let mut producer_node_id = producer_selector.get_producer_node_id(&bk_set);
        for _i in 0..1000 {
            producer_selector = producer_selector.move_index(1, 100);
            let new_producer_node_id = producer_selector.get_producer_node_id(&bk_set);
            assert_ne!(new_producer_node_id, producer_node_id);
            producer_node_id = new_producer_node_id;
        }
    }

    #[test]
    fn test_is_node_bp_based_on_bk_set_and_index_offset() {
        let mut bk_set = BlockKeeperSet::new();
        for i in 0..100 {
            let acc_id_str =
                format!("00000000000000000000000000000000000000000000000000000000{:08x}", i);
            bk_set.insert(
                i as SignerIndex,
                BlockKeeperData {
                    owner_address: AccountAddress(AccountId::from_str(&acc_id_str).unwrap()),
                    ..Default::default()
                },
            )
        }
        let producer_selector =
            ProducerSelector { rng_seed_block_id: BlockIdentifier::default(), index: 0 };
        let nodes_set = bk_set.iter_node_ids().cloned().collect::<Vec<_>>();
        for node_id in nodes_set {
            for i in 0..=100 {
                assert_ne!(i, 100);
                if producer_selector.check_whether_this_node_is_bp_based_on_bk_set_and_index_offset(
                    &bk_set, &node_id, i,
                ) {
                    break;
                }
            }
        }
    }
}
