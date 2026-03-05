// use account_map::btreemap::DAppBTreeMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::Criterion;
use rand::rngs::StdRng;
use rand::Rng;
use rand::RngCore;
use rand::SeedableRng;
use trie_map::hashmap::TrieHashMap;
use trie_map::MapKey;
use trie_map::MapRepository;

const N_INSERT: usize = 500_000;
const N_UPDATE_KEYS: usize = 1_000;
const N_UPDATES: usize = 100_000;
const UPDATE_BATCH: usize = 30;

// const N_INSERT: usize = 5_000_000;
// const N_UPDATE_KEYS: usize = 10_000;
// const N_UPDATES: usize = 1_000_000;
// const UPDATE_BATCH: usize = 300;
//
struct BenchData {
    /// 5M key/value pairs for initial filling and reuse.
    insert_pairs: Vec<(MapKey, [u8; 32])>,
    /// Indices into the first N_UPDATE_KEYS for updates.
    update_key_indices: Vec<usize>,
}

/// Increment [u8; 32] as a big-endian uint256.
fn inc_u256_be(v: &mut [u8; 32]) {
    for i in (0..32).rev() {
        let (new, carry) = v[i].overflowing_add(1);
        v[i] = new;
        if !carry {
            break;
        }
    }
}

fn generate_bench_data() -> BenchData {
    let mut rng = StdRng::seed_from_u64(0xD0E0_D0E0_DEAD_BEEF);

    let mut insert_pairs = Vec::with_capacity(N_INSERT);
    for _ in 0..N_INSERT {
        let mut k = [0u8; 32];
        let mut v = [0u8; 32];
        rng.fill_bytes(&mut k);
        rng.fill_bytes(&mut v);
        insert_pairs.push((MapKey(k), v));
    }

    // Updates will only touch the first N_UPDATE_KEYS keys.
    let mut update_key_indices = Vec::with_capacity(N_UPDATES);
    for _ in 0..N_UPDATES {
        let idx = rng.gen_range(0..N_UPDATE_KEYS);
        update_key_indices.push(idx);
    }

    BenchData { insert_pairs, update_key_indices }
}

// Bench: initial fill with 5M entries
//

fn bench_fill_for<M>(c: &mut Criterion, name: &str, data: &BenchData)
where
    M: MapRepository<Value = [u8; 32]> + Default + Clone + 'static,
{
    c.bench_function(name, |b| {
        b.iter_batched(
            || M::default(),
            |repo| {
                let mut map = M::new_map();
                for chunk in data.insert_pairs.chunks(1000) {
                    let chunk = &chunk.iter().map(|(k, v)| (*k, Some(*v))).collect::<Vec<_>>();
                    map = repo.map_update(&map, chunk);
                }
                black_box((repo, map));
            },
            BatchSize::LargeInput,
        );
    });
}

// Bench: 1M updates on 10k keys, batched by 300,
// cloning the map after each batch (clone cost included).
//

fn bench_updates_batched_clone_for<M>(c: &mut Criterion, name: &str, data: &BenchData)
where
    M: MapRepository<Value = [u8; 32]> + Default + Clone + 'static,
{
    c.bench_function(name, |b| {
        b.iter_batched(
            // Setup: build a fresh map and an array of current values for the 10k keys.
            || {
                let repo = M::default();
                let mut map = M::new_map();
                let mut current_values = Vec::with_capacity(N_UPDATE_KEYS);
                let mut batch = Vec::with_capacity(N_UPDATES);
                // We use the first N_UPDATE_KEYS pairs for initial state.
                for i in 0..N_UPDATE_KEYS {
                    let (key, value) = data.insert_pairs[i];
                    current_values.push(value);
                    batch.push((key, Some(value)));
                }
                map = repo.map_update(&map, &batch);
                (repo, map, current_values)
            },
            // Body: 1_000_000 updates, in batches of 300, clone after each batch.
            |(repo, mut map, mut current_values)| {
                for chunk in data.update_key_indices.chunks(UPDATE_BATCH) {
                    let mut batch = Vec::with_capacity(UPDATE_BATCH);
                    // Apply batch of updates
                    for &idx in chunk {
                        // Get current value, increment as uint256, write back
                        let mut val = current_values[idx];
                        inc_u256_be(&mut val);
                        current_values[idx] = val;

                        let (key, _) = data.insert_pairs[idx];
                        batch.push((key, Some(val)));
                    }
                    map = repo.map_update(&map, &batch);
                }

                black_box(map);
            },
            BatchSize::LargeInput,
        );
    });
}

fn benchmark_map<Map: MapRepository<Value = [u8; 32]> + Default + Clone + 'static>(
    name: &str,
    c: &mut Criterion,
    data: &BenchData,
) {
    bench_fill_for::<Map>(c, &format!("{name} initial fill"), data);
    bench_updates_batched_clone_for::<Map>(c, &format!("{name} batched update and clone"), data);
}

fn criterion_benchmark(c: &mut Criterion) {
    let data = generate_bench_data();
    benchmark_map::<TrieHashMap<[u8; 32]>>("hash", c, &data);
    // benchmark_map::<DAppBTreeMap>("btree", c, &data);
    // benchmark_map::<DAppPsmtMap>("psmt", c, &data);
}

fn config() -> Criterion {
    Criterion::default().without_plots().sample_size(10).warm_up_time(Duration::from_secs(1))
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
