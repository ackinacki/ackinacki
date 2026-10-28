//! Cursor prototype: read-only experiments; production queue code is untouched.
use multi_map::MapKey;
use multi_map::MultiMapAug;

use super::*;
use crate::seek::CyclicIter;
use crate::seek::SeekIter;

#[derive(Clone)]
struct KeyDapp {
    messages: MultiMapAug<Arc<Inbox>, ()>,
    cursor: Option<MapKey>,
    len: usize,
}

impl Blake3Hashable for KeyDapp {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.messages.root.hash());
    }
}

impl MultiMapValue for KeyDapp {
    fn write_value<W: std::io::Write>(&self, _writer: &mut W) -> anyhow::Result<()> {
        anyhow::bail!("benchmark-only value")
    }

    fn read_value<R: std::io::Read>(_reader: &mut R) -> anyhow::Result<Self> {
        anyhow::bail!("benchmark-only value")
    }
}

struct KeyState {
    messages: MultiMapAug<Arc<KeyDapp>, ()>,
    cursor: Option<MapKey>,
    len: usize,
}

fn account_key(route: &AccountRouting) -> MapKey {
    MapKey(*route.account_id().as_array())
}
fn dapp_key(id: &DAppIdentifier) -> MapKey {
    MapKey(*id.as_array())
}

impl KeyState {
    fn build(count: usize, dapps: usize) -> Self {
        let mut messages = MultiMapAug::new();
        let len = count / dapps;
        for d in 0..dapps {
            let mut accounts = MultiMapAug::new();
            for index in d * len..(d + 1) * len {
                accounts = accounts.update_single(
                    &account_key(&routing(index, count, dapps)),
                    Some(inbox(index, 0)),
                );
            }
            let cursor = SeekIter::new(&accounts, None).nth(len / 3).map(|(key, _)| key);
            messages = messages.update_single(
                &dapp_key(&dapp(d)),
                Some(Arc::new(KeyDapp { messages: accounts, cursor, len })),
            );
        }
        let cursor = SeekIter::new(&messages, None).nth(dapps / 3).map(|(key, _)| key);
        Self { messages, cursor, len: dapps }
    }

    fn from_indexed(state: &MerkleState) -> Self {
        let mut messages = MultiMapAug::new();
        for (id, dapp) in state.iter() {
            let len = dapp.messages.len();
            let cursor = dapp.messages.get_index(len / 3).map(account_key);
            let value = Arc::new(KeyDapp { messages: dapp.messages.raw().clone(), cursor, len });
            messages = messages.update_single(&dapp_key(id), Some(value));
        }
        assert_eq!(messages.root.hash(), state.root_hash().0);
        Self { messages, cursor: state.get_index(state.len() / 3).map(dapp_key), len: state.len() }
    }

    fn get(&self, id: &DAppIdentifier) -> &KeyDapp {
        &multi_map::ops::get_leaf(&self.messages.root, &self.messages.root_path, &dapp_key(id))
            .unwrap()
            .value
    }
}

fn checksum(current: u64, inbox: &Inbox) -> u64 {
    current.rotate_left(7) ^ u64::from_le_bytes(inbox.added[..8].try_into().unwrap())
}

// Exact inbox-selection pattern of ThreadMessageQueueIterator, excluding the
// common MessagesRange clone and DB/producer work on both sides.
fn indexed_walk(state: &MerkleState, limit: usize) -> (usize, u64) {
    let mut dapp_offset = 0;
    let mut account_offset = 0;
    let mut seen = 0;
    let mut sum = 0;
    while dapp_offset < state.len() && seen < limit {
        let id = state.get_index((state.len() / 3 + dapp_offset) % state.len()).unwrap();
        let dapp = state.get(id).unwrap();
        if account_offset >= dapp.messages.len() {
            dapp_offset += 1;
            account_offset = 0;
            continue;
        }
        let index = (dapp.messages.len() / 3 + account_offset) % dapp.messages.len();
        let route = dapp.messages.get_index(index).unwrap();
        account_offset += 1;
        sum = checksum(sum, dapp.messages.get(route).unwrap());
        seen += 1;
    }
    (seen, sum)
}

fn keyed_walk(state: &KeyState, limit: usize) -> (usize, u64) {
    let mut seen = 0;
    let mut sum = 0;
    for (_, dapp) in CyclicIter::new(&state.messages, state.cursor) {
        for (_, inbox) in CyclicIter::new(&dapp.messages, dapp.cursor) {
            sum = checksum(sum, inbox);
            seen += 1;
            if seen == limit {
                return (seen, sum);
            }
        }
    }
    (seen, sum)
}

fn timed<T>(f: impl FnOnce() -> T) -> (f64, T) {
    let start = Instant::now();
    let result = black_box(f());
    (start.elapsed().as_secs_f64() * 1000.0, result)
}

fn compare_walks(indexed: &MerkleState, keyed: &KeyState, count: usize, rounds: usize) {
    for limit in [32usize, 4096, count] {
        let limit = limit.min(count);
        let expected = indexed_walk(indexed, limit);
        assert_eq!(keyed_walk(keyed, limit), expected);
        let repetitions = if limit <= 32 { 100 } else { 1 };
        let mut old = Vec::new();
        let mut new = Vec::new();
        for round in 0..rounds {
            for side in [round % 2, 1 - round % 2] {
                let (ms, ()) = timed(|| {
                    for _ in 0..repetitions {
                        let result = if side == 0 {
                            indexed_walk(indexed, limit)
                        } else {
                            keyed_walk(keyed, limit)
                        };
                        assert_eq!(black_box(result), expected);
                    }
                });
                if side == 0 {
                    old.push(ms / repetitions as f64);
                } else {
                    new.push(ms / repetitions as f64);
                }
            }
        }
        println!(
            "WALK inboxes={limit} indexed_ms={:.6} keyed_ms={:.6}",
            median(&mut old),
            median(&mut new)
        );
    }
}

// Both inputs describe exactly the same existing starting leaf. In indexed
// storage the cursor is already numeric; in keyed storage it is already a key.
struct Probe {
    dapp_index: usize,
    account_index: usize,
    dapp_key: MapKey,
    account_key: MapKey,
}

fn compare_seeks(indexed: &MerkleState, keyed: &KeyState, rounds: usize) {
    let probes: Vec<_> = (0..4096)
        .map(|i| {
            let dapp_index = i * 7919 % indexed.len();
            let id = indexed.get_index(dapp_index).unwrap();
            let dapp = indexed.get(id).unwrap();
            let account_index = i * 104729 % dapp.messages.len();
            Probe {
                dapp_index,
                account_index,
                dapp_key: dapp_key(id),
                account_key: account_key(dapp.messages.get_index(account_index).unwrap()),
            }
        })
        .collect();
    let mut old = Vec::new();
    let mut new = Vec::new();
    let mut expected = None;
    for round in 0..rounds {
        for side in [round % 2, 1 - round % 2] {
            let (ms, sum) = timed(|| {
                let mut sum = 0;
                for probe in &probes {
                    let inbox = if side == 0 {
                        let id = indexed.get_index(black_box(probe.dapp_index)).unwrap();
                        let dapp = indexed.get(id).unwrap();
                        let key = dapp.messages.get_index(probe.account_index).unwrap();
                        dapp.messages.get(key).unwrap()
                    } else {
                        let (_, dapp) =
                            SeekIter::new(&keyed.messages, Some(black_box(probe.dapp_key)))
                                .next()
                                .unwrap();
                        SeekIter::new(&dapp.messages, Some(probe.account_key)).next().unwrap().1
                    };
                    sum = checksum(sum, inbox);
                }
                sum
            });
            if let Some(expected) = expected {
                assert_eq!(sum, expected);
            } else {
                expected = Some(sum);
            }
            if side == 0 {
                old.push(ms * 1e6 / probes.len() as f64);
            } else {
                new.push(ms * 1e6 / probes.len() as f64);
            }
        }
    }
    println!("SEEK two_levels indexed_ns={:.1} keyed_ns={:.1}", median(&mut old), median(&mut new));
}

fn compare_advance(indexed: &MerkleState, keyed: &KeyState, rounds: usize) {
    let touched = indexed.len().min(32);
    let old_dapps: Vec<_> = (0..touched)
        .map(|i| {
            indexed
                .get(indexed.get_index((indexed.len() / 3 + i) % indexed.len()).unwrap())
                .unwrap()
                .as_ref()
        })
        .collect();
    let new_dapps: Vec<_> = (0..touched)
        .map(|i| keyed.get(indexed.get_index((indexed.len() / 3 + i) % indexed.len()).unwrap()))
        .collect();
    let k = if touched == 1 { 32 } else { 1 };
    // Validate the resulting key positions, including cyclic wrap.
    for (old, new) in old_dapps.iter().zip(&new_dapps) {
        let pos = (old.messages.len() / 3 + k) % old.messages.len();
        assert_eq!(
            account_key(old.messages.get_index(pos).unwrap()),
            CyclicIter::new(&new.messages, new.cursor).nth(k % new.len).unwrap().0
        );
    }
    let dapp_pos = (indexed.len() / 3 + touched) % indexed.len();
    assert_eq!(
        dapp_key(indexed.get_index(dapp_pos).unwrap()),
        CyclicIter::new(&keyed.messages, keyed.cursor).nth(touched % keyed.len).unwrap().0
    );
    let mut old = Vec::new();
    let mut new = Vec::new();
    for round in 0..rounds {
        for side in [round % 2, 1 - round % 2] {
            let (ms, ()) = timed(|| {
                for _ in 0..1000 {
                    if side == 0 {
                        for dapp in &old_dapps {
                            black_box(
                                (black_box(dapp.messages.len() / 3) + black_box(k))
                                    % dapp.messages.len(),
                            );
                        }
                        black_box(
                            (black_box(indexed.len() / 3) + black_box(touched)) % indexed.len(),
                        );
                    } else {
                        for dapp in &new_dapps {
                            black_box(
                                CyclicIter::new(&dapp.messages, dapp.cursor)
                                    .nth(black_box(k) % dapp.len)
                                    .unwrap()
                                    .0,
                            );
                        }
                        black_box(
                            CyclicIter::new(&keyed.messages, keyed.cursor)
                                .nth(black_box(touched) % keyed.len)
                                .unwrap()
                                .0,
                        );
                    }
                }
            });
            if side == 0 {
                old.push(ms);
            } else {
                new.push(ms);
            }
        }
    }
    // 1000 repetitions: total milliseconds numerically equal microseconds/block.
    println!("ADVANCE touched_dapps={touched} accounts_per_dapp={k} indexed_us={:.4} keyed_us={:.4} metadata_persistence=excluded", median(&mut old), median(&mut new));
}

pub fn run(args: &[String]) {
    if args.first().is_some_and(|arg| arg == "keyed") {
        run_keyed(&args[1..]);
        return;
    }
    assert_eq!(args.len(), 3, "usage: inbox-map-bench cursor count dapps rounds");
    let count = args[0].parse::<usize>().unwrap();
    let dapps = args[1].parse::<usize>().unwrap();
    let rounds = args[2].parse::<usize>().unwrap();
    assert!(count > 0 && dapps > 0 && count.is_multiple_of(dapps) && rounds > 0);
    assert!(dapps <= 10000, "indexed baseline would overallocate: use at most 10000 DApps");
    let before = live_bytes();
    let indexed = MerkleState::build(count, dapps);
    let indexed_bytes = live_bytes().saturating_sub(before);
    let keyed = KeyState::from_indexed(&indexed);
    println!(
        "CURSOR_FIXTURE count={count} dapps={dapps} accounts_per_dapp={} rounds={rounds}",
        count / dapps
    );
    compare_walks(&indexed, &keyed, count, rounds);
    compare_seeks(&indexed, &keyed, rounds);
    compare_advance(&indexed, &keyed, rounds);
    drop(indexed);
    println!(
        "MEMORY indexed_mib={:.3} keyed_mib={:.3}",
        indexed_bytes as f64 / 1048576.0,
        live_bytes().saturating_sub(before) as f64 / 1048576.0
    );
    black_box(keyed);
}

fn run_keyed(args: &[String]) {
    assert_eq!(args.len(), 3, "usage: inbox-map-bench cursor keyed count dapps rounds");
    let count: usize = args[0].parse().unwrap();
    let dapps: usize = args[1].parse().unwrap();
    let rounds: usize = args[2].parse().unwrap();
    assert!(count > 0 && dapps > 0 && count.is_multiple_of(dapps) && rounds > 0);
    let before = live_bytes();
    let (build_ms, state) = timed(|| KeyState::build(count, dapps));
    println!(
        "KEYED_ONLY count={count} dapps={dapps} rounds={rounds} build_s={:.3} live_mib={:.3}",
        build_ms / 1000.0,
        live_bytes().saturating_sub(before) as f64 / 1048576.0
    );
    for limit in [32usize.min(count), 4096.min(count), count] {
        let expected = keyed_walk(&state, limit);
        assert_eq!(expected.0, limit);
        let mut samples = Vec::new();
        for _ in 0..rounds {
            let (ms, result) = timed(|| keyed_walk(&state, limit));
            assert_eq!(result, expected);
            samples.push(ms);
        }
        println!("WALK inboxes={limit} keyed_ms={:.6}", median(&mut samples));
    }

    let chain_before = live_bytes();
    let mut chain = vec![state.messages.clone()];
    let mut samples = Vec::new();
    for step in 0..10 {
        let updates: Vec<_> = (0..32)
            .map(|i| {
                let index = ((step * 32 + i) * 7919) % count;
                (routing(index, count, dapps), inbox(index, step + 1))
            })
            .collect();
        let (ms, next) = timed(|| {
            let mut next = chain.last().unwrap().clone();
            for (route, value) in &updates {
                let key = dapp_key(route.dapp_id());
                let mut dapp = multi_map::ops::get_leaf(&next.root, &next.root_path, &key)
                    .unwrap()
                    .value
                    .as_ref()
                    .clone();
                dapp.messages =
                    dapp.messages.update_single(&account_key(route), Some(value.clone()));
                next = next.update_single(&key, Some(Arc::new(dapp)));
            }
            next
        });
        for (route, value) in &updates {
            let dapp =
                &multi_map::ops::get_leaf(&next.root, &next.root_path, &dapp_key(route.dapp_id()))
                    .unwrap()
                    .value;
            let actual = &multi_map::ops::get_leaf(
                &dapp.messages.root,
                &dapp.messages.root_path,
                &account_key(route),
            )
            .unwrap()
            .value;
            assert_eq!(actual.added, value.added);
        }
        chain.push(next);
        samples.push(ms);
    }
    for index in 0..32 {
        let index = index * 7919 % count;
        let route = routing(index, count, dapps);
        let original = state.get(route.dapp_id());
        assert_eq!(
            multi_map::ops::get_leaf(
                &original.messages.root,
                &original.messages.root_path,
                &account_key(&route)
            )
            .unwrap()
            .value
            .added,
            inbox(index, 0).added
        );
    }
    println!("RETAINED successors=10 changes_per_state=32 update_median_ms={:.6} extra_live_mib={:.3} l1_updates=single metadata_persistence=excluded", median(&mut samples), live_bytes().saturating_sub(chain_before) as f64 / 1048576.0);
    black_box((&state, &chain));
}

#[cfg(test)]
mod standalone_tests {
    use super::*;

    #[test]
    fn direct_build_matches_indexed_fixture() {
        for (count, dapps) in [(100, 1), (100, 10), (100, 100)] {
            let indexed = MerkleState::build(count, dapps);
            let direct = KeyState::build(count, dapps);
            assert_eq!(direct.messages.root.hash(), indexed.root_hash().0);
            assert_eq!(keyed_walk(&direct, count), indexed_walk(&indexed, count));
        }
    }
}
