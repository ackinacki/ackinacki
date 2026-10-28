//! Storage-only comparison of the historical indexed queue layouts.
//! No node dependency, VM work, message payload allocation or codec timing.
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use account_inbox::range::MessagesRange;
use multi_map::MultiMapValue;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;
use serde::Deserialize;
use serde::Serialize;

// Frozen indexed baseline, retained for comparisons with key-based traversal.
#[allow(dead_code)]
mod maps {
    include!("indexed_maps.rs");

    impl<K: QueueMapKey, V: MultiMapValue> QueueMap<K, V> {
        pub(crate) fn raw(&self) -> &MultiMapAug<V, ()> {
            &self.map
        }
    }
}
mod cursor_bench;
#[allow(dead_code)]
mod order_set;
mod seek;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct MessageKey {
    hash: [u8; 32],
    prefix: u64,
    workchain: i32,
}

/// Same field shapes as AccountInbox, using synthetic message IDs.
/// The fixture uses one compacted message per inbox (no per-message payload).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Inbox {
    messages: MessagesRange<MessageKey, Arc<[u8; 32]>>,
    added: [u8; 32],
    consumed: [u8; 32],
}

impl Blake3Hashable for Inbox {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.added);
        hasher.update(&self.consumed);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MerkleDapp {
    messages: maps::QueueMap<AccountRouting, Arc<Inbox>>,
    cursor: usize,
}

impl Blake3Hashable for MerkleDapp {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.messages.root_hash().0);
    }
}

macro_rules! wire_value {
    ($value:ty) => {
        impl MultiMapValue for $value {
            fn write_value<W: std::io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
                Ok(bincode::serialize_into(writer, self)?)
            }

            fn read_value<R: std::io::Read>(reader: &mut R) -> anyhow::Result<Self> {
                Ok(bincode::deserialize_from(reader)?)
            }
        }
    };
}
wire_value!(Inbox);
wire_value!(MerkleDapp);

#[derive(Clone)]
struct LegacyDapp {
    messages: BTreeMap<AccountRouting, Arc<Inbox>>,
    order: order_set::OrderedSet<AccountRouting>,
    cursor: usize,
}

#[derive(Clone)]
struct LegacyState {
    messages: BTreeMap<DAppIdentifier, Arc<LegacyDapp>>,
    order: order_set::OrderedSet<DAppIdentifier>,
    cursor: usize,
}

type MerkleState = maps::QueueMap<DAppIdentifier, Arc<MerkleDapp>>;
type Updates = Vec<(AccountRouting, Option<Arc<Inbox>>)>;

fn dapp(index: usize) -> DAppIdentifier {
    if index == 0 {
        DAppIdentifier::default()
    } else {
        DAppIdentifier::new(
            *blake3::hash(&[b"dapp".as_slice(), &(index as u64).to_le_bytes()].concat()).as_bytes(),
        )
    }
}

fn routing(index: usize, count: usize, dapps: usize) -> AccountRouting {
    let dapp_index = if index < count { index / (count / dapps) } else { (index - count) % dapps };
    AccountIdentifier::new(*blake3::hash(&(index as u64).to_le_bytes()).as_bytes())
        .routing(dapp(dapp_index))
}

fn inbox(index: usize, version: usize) -> Arc<Inbox> {
    let hash =
        *blake3::hash(&[(index as u64).to_le_bytes(), (version as u64).to_le_bytes()].concat())
            .as_bytes();
    let key = MessageKey { hash, prefix: index as u64, workchain: 0 };
    let mut messages = MessagesRange::empty();
    messages.set_compacted_history(Some(key.clone()..=key));
    Arc::new(Inbox { messages, added: hash, consumed: [0; 32] })
}

trait State: Clone {
    fn build(count: usize, dapps: usize) -> Self;
    fn apply(&mut self, updates: &Updates);
    fn apply_batch(&mut self, updates: &Updates) {
        self.apply(updates);
    }
    fn get(&self, routing: &AccountRouting) -> Option<&Arc<Inbox>>;
    fn len(&self) -> usize;
}

impl State for LegacyState {
    fn build(count: usize, dapps: usize) -> Self {
        let per_dapp = count / dapps;
        let mut result =
            Self { messages: BTreeMap::new(), order: order_set::OrderedSet::new(), cursor: 0 };
        for dapp_index in 0..dapps {
            let mut queue = LegacyDapp {
                messages: BTreeMap::new(),
                order: order_set::OrderedSet::new(),
                cursor: 0,
            };
            for index in dapp_index * per_dapp..(dapp_index + 1) * per_dapp {
                let key = routing(index, count, dapps);
                queue.messages.insert(key, inbox(index, 0));
                queue.order.insert(key);
            }
            result.messages.insert(dapp(dapp_index), Arc::new(queue));
            result.order.insert(dapp(dapp_index));
        }
        result
    }

    fn apply(&mut self, updates: &Updates) {
        for (key, value) in updates {
            let queue = Arc::make_mut(self.messages.get_mut(key.dapp_id()).unwrap());
            match value {
                Some(value) => {
                    queue.messages.insert(*key, value.clone());
                    if !queue.order.contains(key) {
                        queue.order.insert(*key);
                    }
                }
                None => {
                    queue.messages.remove(key);
                    queue.order.remove(key);
                }
            }
            queue.cursor =
                if queue.messages.is_empty() { 0 } else { queue.cursor % queue.messages.len() };
            if queue.messages.is_empty() {
                self.messages.remove(key.dapp_id());
                self.order.remove(key.dapp_id());
            }
        }
        self.cursor = if self.order.is_empty() { 0 } else { self.cursor % self.order.len() };
    }

    fn get(&self, routing: &AccountRouting) -> Option<&Arc<Inbox>> {
        self.messages.get(routing.dapp_id())?.messages.get(routing)
    }

    fn len(&self) -> usize {
        self.messages.values().map(|dapp| dapp.messages.len()).sum()
    }
}

impl State for MerkleState {
    fn build(count: usize, dapps: usize) -> Self {
        let per_dapp = count / dapps;
        let mut result = Self::new();
        for dapp_index in 0..dapps {
            let mut messages = maps::QueueMap::new();
            for index in dapp_index * per_dapp..(dapp_index + 1) * per_dapp {
                messages.update_single(routing(index, count, dapps), Some(inbox(index, 0)));
            }
            result.insert(dapp(dapp_index), Arc::new(MerkleDapp { messages, cursor: 0 }));
        }
        result
    }

    fn apply(&mut self, updates: &Updates) {
        apply_merkle(self, updates, false);
    }

    fn get(&self, routing: &AccountRouting) -> Option<&Arc<Inbox>> {
        self.get(routing.dapp_id())?.messages.get(routing)
    }

    fn apply_batch(&mut self, updates: &Updates) {
        apply_merkle(self, updates, true);
    }

    fn len(&self) -> usize {
        self.values().map(|dapp| dapp.messages.len()).sum()
    }
}

// Same grouping and one COW per touched DApp as production update_inboxes.
fn apply_merkle(state: &mut MerkleState, updates: &Updates, batch_l2: bool) {
    let mut groups: HashMap<DAppIdentifier, Updates> = HashMap::new();
    for (key, value) in updates {
        groups.entry(*key.dapp_id()).or_default().push((*key, value.clone()));
    }
    let mut roots = Vec::with_capacity(groups.len());
    for (id, updates) in groups {
        let mut queue = state.get(&id).unwrap().clone();
        let inner = Arc::make_mut(&mut queue);
        if batch_l2 {
            inner.messages.update_batch(updates);
        } else {
            for (key, value) in updates {
                inner.messages.update_single(key, value);
            }
        }
        inner.cursor =
            if inner.messages.is_empty() { 0 } else { inner.cursor % inner.messages.len() };
        let value = if inner.messages.is_empty() { None } else { Some(queue) };
        roots.push((id, value));
    }
    state.update_batch(roots);
}

// Darwin allocator's live bytes: does not include freed pages retained by malloc.
// The command's /usr/bin/time -l separately reports process peak RSS.
#[cfg(target_os = "macos")]
fn live_bytes() -> usize {
    #[repr(C)]
    struct Statistics {
        blocks: u32,
        bytes: usize,
        max_bytes: usize,
        allocated: usize,
    }
    unsafe extern "C" {
        fn malloc_zone_statistics(zone: *mut std::ffi::c_void, stats: *mut Statistics);
    }
    let mut stats = Statistics { blocks: 0, bytes: 0, max_bytes: 0, allocated: 0 };
    unsafe { malloc_zone_statistics(std::ptr::null_mut(), &mut stats) };
    stats.bytes
}

#[cfg(not(target_os = "macos"))]
fn live_bytes() -> usize {
    0
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn run<S: State>(
    mode: &str,
    count: usize,
    changed: usize,
    depth: usize,
    rounds: usize,
    batch_l2: bool,
    dapps: usize,
) {
    let before = live_bytes();
    let start = Instant::now();
    let base = S::build(count, dapps);
    let build = start.elapsed().as_secs_f64();
    let base_bytes = live_bytes();
    println!(
        "fixture count={count} dapps={dapps} accounts_per_dapp={} inbox_size={} build_s={build:.3} base_live_mib={:.3}",
        count / dapps,
        std::mem::size_of::<Inbox>(),
        (base_bytes - before) as f64 / 1048576.0
    );

    let start = Instant::now();
    let clone_iterations = (1_000_000 / dapps).clamp(3, 100_000);
    for _ in 0..clone_iterations {
        black_box(base.clone());
    }
    println!("clone_drop_ns={:.1}", start.elapsed().as_nanos() as f64 / clone_iterations as f64);

    let mut totals = Vec::new();
    let mut drops = Vec::new();
    let mut steps = Vec::new();
    let mut deltas = Vec::new();
    for round in 0..rounds {
        // Key generation and new inbox allocation are outside timed sections.
        let changes: Vec<Updates> = (0..depth)
            .map(|step| {
                (0..changed)
                    .map(|i| {
                        let index = if mode == "insert" {
                            count + step * changed + i
                        } else {
                            ((step * changed + i) * 7919) % count
                        };
                        let value =
                            (mode != "delete").then(|| inbox(index, 1 + round * depth + step));
                        (routing(index, count, dapps), value)
                    })
                    .collect()
            })
            .collect();
        let before_chain = live_bytes();
        let mut chain = Vec::with_capacity(depth + 1);
        let mut retained_by_step = Vec::with_capacity(depth);
        chain.push(base.clone());
        let mut total_ms = 0.0;
        for updates in &changes {
            let start = Instant::now();
            let mut next = chain.last().unwrap().clone();
            if batch_l2 {
                next.apply_batch(updates);
            } else {
                next.apply(updates);
            }
            let ms = start.elapsed().as_secs_f64() * 1000.0;
            chain.push(next);
            total_ms += ms;
            steps.push(ms);
            retained_by_step.push(live_bytes().saturating_sub(before_chain) as f64 / 1048576.0);
        }
        let delta = live_bytes().saturating_sub(before_chain) as f64 / 1048576.0;
        // Check every version against its own update and the untouched base.
        for step in 0..depth {
            let state = &chain[step + 1];
            let expected_len = match mode {
                "insert" => count + (step + 1) * changed,
                "delete" => count - (step + 1) * changed,
                _ => count,
            };
            assert_eq!(State::len(state), expected_len);
            for (key, value) in &changes[step] {
                assert_eq!(
                    State::get(state, key).map(|inbox| inbox.added),
                    value.as_ref().map(|inbox| inbox.added)
                );
                assert_eq!(State::get(&base, key).is_some(), mode != "insert");
                if mode == "update" {
                    assert_ne!(
                        State::get(&base, key).unwrap().added,
                        value.as_ref().unwrap().added
                    );
                }
            }
        }
        println!("round={} chain_ms={total_ms:.3} retained_delta_mib={delta:.3}", round + 1);
        if round == rounds / 2 {
            for (step, extra_mib) in retained_by_step.iter().enumerate() {
                if step == 0 || step == 4 || step + 1 == depth {
                    println!("retained_successors={} extra_live_mib={extra_mib:.3}", step + 1);
                }
            }
        }
        let start = Instant::now();
        drop(chain);
        let drop_ms = start.elapsed().as_secs_f64() * 1000.0;
        totals.push(total_ms);
        drops.push(drop_ms);
        deltas.push(delta);
    }
    let chain_ms = median(&mut totals);
    let step_ms = median(&mut steps);
    let drop_ms = median(&mut drops);
    let delta_mib = median(&mut deltas);
    println!("RESULT mode={mode} batch_l2={batch_l2} dapps={dapps} n={count} changed={changed} depth={depth} rounds={rounds} step_ms={step_ms:.6} chain_ms={chain_ms:.3} drop_ms={drop_ms:.3} retained_delta_mib={delta_mib:.3}");
    black_box(base);
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|arg| arg == "cursor") {
        cursor_bench::run(&args[2..]);
        return;
    }
    assert!(args.len() >= 7, "usage: inbox-map-bench legacy|merkle update|insert|delete count changed_per_block depth rounds [--batch-l2] [--dapps=1000]");
    let mut batch_l2 = false;
    let mut dapps = 1usize;
    for flag in &args[7..] {
        if flag == "--batch-l2" {
            assert_eq!(args[1], "merkle");
            batch_l2 = true;
        } else if let Some(value) = flag.strip_prefix("--dapps=") {
            dapps = value.parse().expect("invalid DApp count");
        } else {
            panic!("unknown option: {flag}");
        }
    }
    let mode = &args[2];
    assert!(["update", "insert", "delete"].contains(&mode.as_str()));
    let count = args[3].parse::<usize>().unwrap();
    assert!(dapps > 0 && count >= dapps && count % dapps == 0);
    let changed = args[4].parse::<usize>().unwrap();
    let depth = args[5].parse::<usize>().unwrap();
    let rounds = args[6].parse::<usize>().unwrap();
    assert!(rounds > 0 && depth > 0 && changed > 0 && changed * depth < count);
    match args[1].as_str() {
        "legacy" => run::<LegacyState>(mode, count, changed, depth, rounds, batch_l2, dapps),
        "merkle" => run::<MerkleState>(mode, count, changed, depth, rounds, batch_l2, dapps),
        _ => panic!("unknown implementation"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deleting_the_only_inbox_removes_its_dapp_but_keeps_previous_states() {
        let merkle = MerkleState::build(100, 100);
        let legacy = LegacyState::build(100, 100);
        let mut new = merkle.clone();
        let mut old = legacy.clone();
        State::apply(&mut new, &vec![(routing(0, 100, 100), None)]);
        State::apply(&mut old, &vec![(routing(0, 100, 100), None)]);
        assert!(new.get(&dapp(0)).is_none());
        assert!(!old.messages.contains_key(&dapp(0)));
        assert_eq!(State::len(&new), 99);
        assert_eq!(State::len(&old), 99);
        assert_eq!(State::len(&merkle), 100);
        assert_eq!(State::len(&legacy), 100);
    }

    #[test]
    fn balanced_layout_and_retained_states_match_legacy() {
        let merkle = MerkleState::build(100, 10);
        let legacy = LegacyState::build(100, 10);
        assert_eq!(merkle.len(), 10);
        for (id, queue) in merkle.iter() {
            assert_eq!(queue.messages.len(), 10);
            assert!(queue.messages.keys().all(|key| key.dapp_id() == id));
        }
        let mut new_chain = vec![merkle];
        let mut old_chain = vec![legacy];
        for step in 0..10 {
            let key = routing(step * 10, 100, 10);
            let added = routing(100 + step, 100, 10);
            let updates = vec![(key, None), (added, Some(inbox(100 + step, 1)))];
            let mut new = new_chain.last().unwrap().clone();
            let mut old = old_chain.last().unwrap().clone();
            State::apply(&mut new, &updates);
            State::apply(&mut old, &updates);
            new_chain.push(new);
            old_chain.push(old);
        }
        assert_eq!(new_chain.len(), 11);
        for (version, (new, old)) in new_chain.iter().zip(&old_chain).enumerate() {
            assert_eq!(State::len(new), 100);
            assert_eq!(State::len(old), 100);
            for index in 0..110 {
                let key = routing(index, 100, 10);
                let expected = if index < 100 {
                    index % 10 != 0 || index / 10 >= version
                } else {
                    index - 100 < version
                };
                assert_eq!(State::get(new, &key).is_some(), expected);
                assert_eq!(State::get(old, &key).is_some(), expected);
                assert_eq!(
                    State::get(new, &key).map(|value| value.added),
                    State::get(old, &key).map(|value| value.added)
                );
            }
        }
    }
}
