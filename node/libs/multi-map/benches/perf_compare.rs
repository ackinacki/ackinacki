use std::hint::black_box;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::Criterion;
use multi_map::MultiMapRepository;
use rand::rngs::StdRng;
use rand::Rng;
use rand::RngCore;
use rand::SeedableRng;
use trie_map::trie::smt::TrieMapRepository;
use trie_map::MapKey;
use trie_map::MapKeyPath;
use trie_map::MapRepository;

const N_KEYS: usize = 100_000;
const N_UPDATE_BATCH: usize = 100;
const N_BATCHES: usize = 100;
const N_GET_KEYS: usize = 10_000;

struct BenchData {
    keys: Vec<MapKey>,
    values: Vec<[u8; 32]>,
}

fn generate_data() -> BenchData {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_CAFE_BABE);
    let mut keys = Vec::with_capacity(N_KEYS);
    let mut values = Vec::with_capacity(N_KEYS);
    for _ in 0..N_KEYS {
        let mut k = [0u8; 32];
        let mut v = [0u8; 32];
        rng.fill_bytes(&mut k);
        rng.fill_bytes(&mut v);
        keys.push(MapKey(k));
        values.push(v);
    }
    BenchData { keys, values }
}

fn build_map<R: MapRepository<Value = [u8; 32]> + Default>(data: &BenchData) -> (R, R::MapRef) {
    let repo = R::default();
    let mut map = R::new_map();
    for chunk in data.keys.iter().zip(data.values.iter()).collect::<Vec<_>>().chunks(1000) {
        let updates: Vec<_> = chunk.iter().map(|(k, v)| (**k, Some(**v))).collect();
        map = repo.map_update(&map, &updates);
    }
    (repo, map)
}

fn bench_bulk_insert<R: MapRepository<Value = [u8; 32]> + Default>(
    c: &mut Criterion,
    name: &str,
    data: &BenchData,
) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || R::default(),
            |repo| {
                let mut map = R::new_map();
                for chunk in
                    data.keys.iter().zip(data.values.iter()).collect::<Vec<_>>().chunks(1000)
                {
                    let updates: Vec<_> = chunk.iter().map(|(k, v)| (**k, Some(**v))).collect();
                    map = repo.map_update(&map, &updates);
                }
                black_box(map);
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_get<R: MapRepository<Value = [u8; 32]> + Default>(
    c: &mut Criterion,
    name: &str,
    data: &BenchData,
) {
    let (repo, map) = build_map::<R>(data);

    c.bench_function(name, |b| {
        b.iter(|| {
            for i in 0..N_GET_KEYS {
                black_box(repo.map_get(&map, &data.keys[i]));
            }
        });
    });
}

fn bench_incremental_updates<R: MapRepository<Value = [u8; 32]> + Default>(
    c: &mut Criterion,
    name: &str,
    data: &BenchData,
) {
    let (repo, map) = build_map::<R>(data);

    c.bench_function(name, |b| {
        let mut rng = StdRng::seed_from_u64(0x1234_5678);
        b.iter_batched(
            || {
                let batch_updates: Vec<Vec<(MapKey, Option<[u8; 32]>)>> = (0..N_BATCHES)
                    .map(|_| {
                        (0..N_UPDATE_BATCH)
                            .map(|_| {
                                let idx = rng.gen_range(0..N_KEYS);
                                let mut v = [0u8; 32];
                                rng.fill_bytes(&mut v);
                                (data.keys[idx], Some(v))
                            })
                            .collect()
                    })
                    .collect();
                (map.clone(), batch_updates)
            },
            |(mut m, batches)| {
                for batch in &batches {
                    m = repo.map_update(&m, batch);
                }
                black_box(m);
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_split_merge<R: MapRepository<Value = [u8; 32]> + Default>(
    c: &mut Criterion,
    name: &str,
    data: &BenchData,
) {
    let (repo, map) = build_map::<R>(data);

    let pfx = data.keys[500].0;
    let path = MapKeyPath { prefix: MapKey(pfx), len: 8 };

    c.bench_function(name, |b| {
        b.iter(|| {
            let (a, b_part) = repo.map_split(&map, path);
            let merged = repo.merge(&a, &b_part);
            black_box(merged);
        });
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let data = generate_data();

    bench_bulk_insert::<TrieMapRepository<[u8; 32]>>(c, "trie-map bulk insert", &data);
    bench_bulk_insert::<MultiMapRepository<[u8; 32]>>(c, "multi-map bulk insert", &data);

    bench_get::<TrieMapRepository<[u8; 32]>>(c, "trie-map get 10k", &data);
    bench_get::<MultiMapRepository<[u8; 32]>>(c, "multi-map get 10k", &data);

    bench_incremental_updates::<TrieMapRepository<[u8; 32]>>(c, "trie-map 100x100 updates", &data);
    bench_incremental_updates::<MultiMapRepository<[u8; 32]>>(
        c,
        "multi-map 100x100 updates",
        &data,
    );

    bench_split_merge::<TrieMapRepository<[u8; 32]>>(c, "trie-map split+merge", &data);
    bench_split_merge::<MultiMapRepository<[u8; 32]>>(c, "multi-map split+merge", &data);
}

fn config() -> Criterion {
    Criterion::default()
        .without_plots()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
