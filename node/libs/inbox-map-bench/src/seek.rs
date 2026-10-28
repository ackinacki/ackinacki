// Exercise the production traversal in the standalone benchmark as well.
#[path = "../../../src/types/thread_message_queue/seek.rs"]
mod production;
pub use production::CyclicIter;
pub use production::SeekIter;

#[cfg(test)]
mod tests {
    use multi_map::MapKey;
    use multi_map::MapKeyPath;
    use multi_map::MultiMapAug;

    use super::*;

    #[test]
    fn seek_and_single_cyclic_pass_match_reference_including_partial_prefix_views() {
        let mut map = MultiMapAug::new();
        let mut keys = Vec::new();
        for index in 0..128usize {
            let key = MapKey(*blake3::hash(&(index as u64).to_le_bytes()).as_bytes());
            keys.push(key);
            map = map.update_single(&key, Some(crate::inbox(index, 0)));
        }
        let removed = keys[31];
        map = map.update_single(&removed, None);
        let mut probes = vec![MapKey([0; 32]), MapKey([255; 32]), removed];
        for key in &keys {
            probes.push(*key);
            let mut missing = key.0;
            missing[31] ^= 1;
            probes.push(MapKey(missing));
        }
        for bits in [0, 1, 4, 7, 8, 129, 255, 256] {
            let (_, branch) = map.split(MapKeyPath { prefix: keys[63], len: bits });
            for source in [&map, &branch] {
                let all: Vec<_> = source.iter().map(|(key, _)| key).collect();
                assert_eq!(
                    SeekIter::new(source, None).map(|(key, _)| key).collect::<Vec<_>>(),
                    all
                );
                for target in &probes {
                    let expected: Vec<_> =
                        all.iter().copied().filter(|key| key >= target).collect();
                    let found: Vec<_> =
                        SeekIter::new(source, Some(*target)).map(|(key, _)| key).collect();
                    assert_eq!(
                        found, expected,
                        "seek mismatch at prefix {bits}, target {target:?}"
                    );
                    let expected_cycle: Vec<_> = all
                        .iter()
                        .copied()
                        .filter(|key| key >= target)
                        .chain(all.iter().copied().filter(|key| key < target))
                        .collect();
                    let mut cyclic = CyclicIter::new(source, Some(*target));
                    let found: Vec<_> =
                        cyclic.by_ref().take(all.len() + 1).map(|(key, _)| key).collect();
                    assert_eq!(found, expected_cycle, "cycle mismatch at prefix {bits}");
                    assert!(cyclic.next().is_none());
                    assert!(cyclic.next().is_none());
                }
            }
        }
        let empty = MultiMapAug::new();
        assert!(SeekIter::<std::sync::Arc<crate::Inbox>>::new(&empty, Some(removed))
            .next()
            .is_none());
        assert!(CyclicIter::<std::sync::Arc<crate::Inbox>>::new(&empty, Some(removed))
            .next()
            .is_none());
    }
}
