#![allow(unused)]

use std::time::Duration;

use account_state::DurableThreadAccountsRepository;
use account_state::FsAccountsStore;
use account_state::FsCompositeThreadAccounts;
use account_state::ThreadAccountUpdate;
use account_state::ThreadAccounts;
use account_state::ThreadAccountsBuilder;
use account_state::ThreadAccountsRepository;
use account_state::ThreadStateAccount;
use account_state::ThreadStateRef;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use tempfile::TempDir;
use tvm_block::MsgAddressInt;
use tvm_block::ShardAccount;
use tvm_types::AccountId;

type Repo = DurableThreadAccountsRepository<FsAccountsStore>;

type NodeAccounts = FsCompositeThreadAccounts;
type NodeRepo = <NodeAccounts as ThreadAccounts>::Repository;

// ==================== Data Generators ====================

fn new_u256(name: &str, seed: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(name.as_bytes());
    hasher.update(&seed.to_be_bytes());
    hasher.finalize().into()
}

fn make_dapp_id(seed: u64) -> DAppIdentifier {
    DAppIdentifier::new(new_u256("dapp", seed))
}

fn make_account_id(dapp_seed: u64, acc_seed: u64) -> AccountIdentifier {
    let combined = dapp_seed.wrapping_mul(100_000).wrapping_add(acc_seed);
    AccountIdentifier::new(new_u256("account", combined))
}

fn make_routing(dapp_seed: u64, acc_seed: u64) -> AccountRouting {
    AccountRouting::new(make_dapp_id(dapp_seed), make_account_id(dapp_seed, acc_seed))
}

fn make_state_account(routing: &AccountRouting, seed: u64) -> ThreadStateAccount {
    let id = routing.account_id();
    let tvm_acc = tvm_block::Account::with_address(
        MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
            .unwrap(),
    );
    let tvm_shard_acc = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", seed).into(),
        seed,
        Some(routing.dapp_id().as_array().into()),
    )
    .unwrap();
    ThreadStateAccount::from(tvm_shard_acc)
}

/// Builds a data cell tree of approximately `size` bytes using deterministic content.
/// Uses a 4-ary tree to stay well within the TVM cell depth limit of 2048.
fn build_data_cell(seed: u64, size: usize) -> tvm_types::Cell {
    use tvm_types::BuilderData;

    let chunk_size = 127; // max bytes per cell (~1023 bits)
    let num_cells = size.div_ceil(chunk_size);

    // Build leaf cells
    let mut cells: Vec<tvm_types::Cell> = Vec::with_capacity(num_cells);
    for i in 0..num_cells {
        let offset = i * chunk_size;
        let this_chunk = (size - offset).min(chunk_size);
        let mut builder = BuilderData::new();
        let bytes: Vec<u8> =
            (0..this_chunk).map(|j| ((seed as usize + offset + j) % 256) as u8).collect();
        builder.append_raw(&bytes, this_chunk * 8).unwrap();
        cells.push(builder.into_cell().unwrap());
    }

    // Build tree bottom-up: each internal node holds up to 4 child references
    while cells.len() > 1 {
        let mut next_level = Vec::new();
        for chunk in cells.chunks(4) {
            let mut builder = BuilderData::new();
            for child in chunk {
                builder.checked_append_reference(child.clone()).unwrap();
            }
            next_level.push(builder.into_cell().unwrap());
        }
        cells = next_level;
    }

    cells.into_iter().next().unwrap_or_default()
}

/// Create a TVM account with approximately `data_size` bytes of data.
fn make_sized_state_account(
    routing: &AccountRouting,
    seed: u64,
    data_size: usize,
) -> ThreadStateAccount {
    use tvm_block::*;
    use tvm_types::BuilderData;

    let id = routing.account_id();
    let address =
        MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
            .unwrap();

    let data_cell = build_data_cell(seed, data_size);

    let mut code_builder = BuilderData::new();
    code_builder.append_raw(&[0xDE, 0xAD, 0xBE, 0xEF], 32).unwrap();
    let code_cell = code_builder.into_cell().unwrap();

    let state_init =
        StateInit { code: Some(code_cell), data: Some(data_cell), ..Default::default() };
    let storage = AccountStorage::active_by_init_code_hash(
        0,
        CurrencyCollection::with_grams(1_000_000_000),
        state_init,
        true,
    );
    let storage_stat = StorageInfo::with_values(0, None);
    let mut tvm_acc = Account::with_storage(&address, &storage_stat, &storage);
    tvm_acc.update_storage_stat().unwrap();

    let shard_acc = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", seed).into(),
        seed,
        Some(routing.dapp_id().as_array().into()),
    )
    .unwrap();

    ThreadStateAccount::from(shard_acc)
}

fn empty_shard_accounts() -> tvm_block::ShardAccounts {
    tvm_block::ShardStateUnsplit::default()
        .read_accounts()
        .expect("failed to create empty ShardAccounts")
}

// ==================== Repo Builders ====================

/// Benchmark-only repo: DurableThreadAccountsRepository + FsAccountsStore (simple FS).
fn make_repo(dir: &TempDir) -> Repo {
    let base = dir.path();
    std::fs::create_dir_all(base.join("accounts")).unwrap();

    let accounts = FsAccountsStore::new(base.join("accounts")).unwrap();
    DurableThreadAccountsRepository::new(base.to_path_buf(), accounts).unwrap()
}

/// Production node repo: FsCompositeThreadAccounts with FsOptAccountsStore,
/// same as `NodeThreadAccounts::new_repository(path).build()` in bin/node.rs,
/// with apply_to_durable=true to exercise the durable update path.
fn make_node_repo(dir: &TempDir) -> NodeRepo {
    NodeAccounts::new_repository(dir.path().to_path_buf())
        .set_apply_to_durable(true)
        .set_accounts_aerospike_store("127.0.0.1:3000", "test", "test1")
        .build()
        .unwrap()
}

// ==================== Update Generators ====================

/// Generate all-insert updates: d DApps x a accounts each.
fn make_insert_updates(
    d: usize,
    a: usize,
    redirect: bool,
    seed_offset: u64,
) -> Vec<(AccountRouting, ThreadAccountUpdate)> {
    let mut updates = Vec::with_capacity(d * a);
    for di in 0..d {
        for ai in 0..a {
            let routing = make_routing(di as u64, seed_offset + ai as u64);
            let acc = make_state_account(&routing, seed_offset + ai as u64);
            let update = if redirect {
                ThreadAccountUpdate::UpdateOrInsert(acc.with_redirect().unwrap())
            } else {
                ThreadAccountUpdate::UpdateOrInsert(acc)
            };
            updates.push((routing, update));
        }
    }
    updates
}

/// Generate mixed insert/delete updates.
/// Deletes target accounts prepopulated with seed_offset=0.
/// New inserts use seed_offset=10_000 to avoid collisions.
fn make_mixed_updates(
    d: usize,
    a: usize,
    delete_fraction: f64,
) -> Vec<(AccountRouting, ThreadAccountUpdate)> {
    let delete_per_dapp = (a as f64 * delete_fraction).round() as usize;
    let insert_per_dapp = a - delete_per_dapp;
    let new_seed_offset = 10_000u64;

    let mut updates = Vec::with_capacity(d * a);
    for di in 0..d {
        for ai in 0..delete_per_dapp {
            let routing = make_routing(di as u64, ai as u64);
            updates.push((routing, ThreadAccountUpdate::Remove));
        }
        for ai in 0..insert_per_dapp {
            let seed = new_seed_offset + ai as u64;
            let routing = make_routing(di as u64, seed);
            let acc = make_state_account(&routing, seed);
            updates.push((routing, ThreadAccountUpdate::UpdateOrInsert(acc)));
        }
    }
    updates
}

/// Generate updates with mixed payload types (real accounts vs redirect stubs).
fn make_payload_updates(
    d: usize,
    a: usize,
    redirect_fraction: f64,
) -> Vec<(AccountRouting, ThreadAccountUpdate)> {
    let redirect_per_dapp = (a as f64 * redirect_fraction).round() as usize;
    let mut updates = Vec::with_capacity(d * a);
    for di in 0..d {
        for ai in 0..a {
            let routing = make_routing(di as u64, ai as u64);
            let acc = make_state_account(&routing, ai as u64);
            let update = if ai < redirect_per_dapp {
                ThreadAccountUpdate::UpdateOrInsert(acc.with_redirect().unwrap())
            } else {
                ThreadAccountUpdate::UpdateOrInsert(acc)
            };
            updates.push((routing, update));
        }
    }
    updates
}

/// Generate sized account data as (routing, account) pairs for the node builder path.
fn make_sized_accounts(
    d: usize,
    a: usize,
    data_size: usize,
    seed_offset: u64,
) -> Vec<(AccountRouting, ThreadStateAccount)> {
    let mut accounts = Vec::with_capacity(d * a);
    for di in 0..d {
        for ai in 0..a {
            let routing = make_routing(di as u64, seed_offset + ai as u64);
            let acc = make_sized_state_account(&routing, seed_offset + ai as u64, data_size);
            accounts.push((routing, acc));
        }
    }
    accounts
}

/// Generate all-insert updates with accounts of a specific data size.
fn make_sized_insert_updates(
    d: usize,
    a: usize,
    data_size: usize,
    seed_offset: u64,
) -> Vec<(AccountRouting, ThreadAccountUpdate)> {
    let mut updates = Vec::with_capacity(d * a);
    for di in 0..d {
        for ai in 0..a {
            let routing = make_routing(di as u64, seed_offset + ai as u64);
            let acc = make_sized_state_account(&routing, seed_offset + ai as u64, data_size);
            updates.push((routing, ThreadAccountUpdate::UpdateOrInsert(acc)));
        }
    }
    updates
}

/// Prepopulate the repo with d DApps x a_per_dapp accounts.
fn prepopulate(repo: &Repo, d: usize, a_per_dapp: usize) -> ThreadStateRef {
    let state = Repo::new_state();
    let shard_accounts = empty_shard_accounts();
    let updates = make_insert_updates(d, a_per_dapp, false, 0);
    repo.state_update(&shard_accounts, &state, &updates).unwrap()
}

// ==================== Benchmark Groups ====================

/// Scale benchmarks: vary D (DApps) x A (accounts per DApp), all inserts.
/// Isolates whether cost scales with DApp count (outer trie) or account count (inner trie).
fn bench_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_maps/scale");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));

    for &(d, a) in
        &[(1, 1), (1, 10), (1, 100), (1, 1000), (10, 10), (10, 100), (100, 10), (100, 100)]
    {
        group.throughput(Throughput::Elements((d * a) as u64));
        group.bench_with_input(
            BenchmarkId::new("DxA", format!("{d}x{a}")),
            &(d, a),
            |b, &(d, a)| {
                let dir = TempDir::new().unwrap();
                let repo = make_repo(&dir);
                let state = Repo::new_state();
                let shard_accounts = empty_shard_accounts();
                let updates = make_insert_updates(d, a, false, 0);

                b.iter(|| repo.state_update(&shard_accounts, &state, &updates).unwrap());
            },
        );
    }
    group.finish();
}

/// Delete-ratio benchmarks: fix D=10, A=100; vary delete fraction.
/// Tests whether delete-heavy workloads are slower due to map_get per deleted entry.
fn bench_delete_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_maps/delete_ratio");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    let d = 10usize;
    let a = 100usize;

    for &pct in &[0u32, 25, 50, 75, 100] {
        group.throughput(Throughput::Elements((d * a) as u64));
        group.bench_with_input(BenchmarkId::new("delete_pct", pct), &pct, |b, &pct| {
            let dir = TempDir::new().unwrap();
            let repo = make_repo(&dir);
            let state = prepopulate(&repo, d, a);
            let shard_accounts = empty_shard_accounts();
            let updates = make_mixed_updates(d, a, pct as f64 / 100.0);

            b.iter(|| repo.state_update(&shard_accounts, &state, &updates).unwrap());
        });
    }
    group.finish();
}

/// Trie-density benchmarks: fix D=10, A=100 updates; vary pre-existing accounts per DApp.
/// Tests whether dense tries are slower due to more CoW node copies on mutation.
fn bench_trie_density(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_maps/trie_density");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    let d = 1usize;
    let a = 1000usize;

    for &pre_existing in &[0usize, 500, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements((d * a) as u64));
        group.bench_with_input(
            BenchmarkId::new("pre_existing", pre_existing),
            &pre_existing,
            |b, &pre_existing| {
                let dir = TempDir::new().unwrap();
                let repo = make_repo(&dir);
                let state = if pre_existing > 0 {
                    prepopulate(&repo, d, pre_existing)
                } else {
                    Repo::new_state()
                };
                let shard_accounts = empty_shard_accounts();
                let updates = make_insert_updates(d, a, false, pre_existing as u64);

                b.iter(|| repo.state_update(&shard_accounts, &state, &updates).unwrap());
            },
        );
    }
    group.finish();
}

/// Payload-type benchmarks: fix D=10, A=100; vary account payload type.
/// Tests whether redirect stubs are faster because accounts.update is skipped.
fn bench_payload_type(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_maps/payload_type");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    let d = 10usize;
    let a = 100usize;

    for &(name, redirect_frac) in
        &[("real_accounts", 0.0), ("redirect_stubs", 1.0), ("mixed_50_50", 0.5)]
    {
        group.throughput(Throughput::Elements((d * a) as u64));
        group.bench_function(name, |b| {
            let dir = TempDir::new().unwrap();
            let repo = make_repo(&dir);
            let state = Repo::new_state();
            let shard_accounts = empty_shard_accounts();
            let updates = make_payload_updates(d, a, redirect_frac);

            b.iter(|| repo.state_update(&shard_accounts, &state, &updates).unwrap());
        });
    }
    group.finish();
}

/// Account-size benchmarks: fix D=10, A=100; vary account data size.
/// Isolates the cost of serializing and writing larger account payloads.
fn bench_account_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_maps/account_size");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    let d = 10usize;
    let a = 100usize;

    for &(label, size) in &[
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
        ("200KB", 200 * 1024),
        ("500KB", 500 * 1024),
        ("1MB", 1024 * 1024),
    ] {
        group.throughput(Throughput::Elements((d * a) as u64));
        group.bench_function(label, |b| {
            let dir = TempDir::new().unwrap();
            let repo = make_repo(&dir);
            let state = Repo::new_state();
            let shard_accounts = empty_shard_accounts();
            let updates = make_sized_insert_updates(d, a, size, 0);

            b.iter(|| repo.state_update(&shard_accounts, &state, &updates).unwrap());
        });
    }
    group.finish();
}

/// Compares current benchmark setup (DurableThreadAccountsRepository + FsAccountsStore)
/// vs the production node setup (CompositeThreadAccountRepository + FsOptAccountsStore
/// + builder pattern), matching the initialization in bin/node.rs:
///   NodeThreadAccounts::new_repository(path)
///   .set_accounts_aerospike_store(...)  // Aerospike not available in bench
///   .build()
///   We use FsOptAccountsStore (the non-Aerospike fallback) with apply_to_durable=true
///   and the full builder path: state_builder() -> insert_account() -> build().
fn bench_vs_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_maps/bench_vs_node");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    let d = 10usize;
    let a = 100usize;

    for &(label, size) in &[
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
        ("200KB", 200 * 1024),
        ("500KB", 500 * 1024),
        ("1MB", 1024 * 1024),
    ] {
        group.throughput(Throughput::Elements((d * a) as u64));

        // Current benchmark: DurableThreadAccountsRepository + FsAccountsStore, direct state_update
        group.bench_function(BenchmarkId::new("bench_direct", label), |b| {
            let dir = TempDir::new().unwrap();
            let repo = make_repo(&dir);
            let state = Repo::new_state();
            let shard_accounts = empty_shard_accounts();
            let updates = make_sized_insert_updates(d, a, size, 0);

            b.iter(|| repo.state_update(&shard_accounts, &state, &updates).unwrap());
        });

        // Production node path: CompositeThreadAccountRepository + FsOptAccountsStore,
        // using state_builder() -> insert_account() -> build() (full composite pipeline)
        group.bench_function(BenchmarkId::new("node_builder", label), |b| {
            let dir = TempDir::new().unwrap();
            let repo = make_node_repo(&dir);
            let state = NodeRepo::new_state();
            let accounts = make_sized_accounts(d, a, size, 0);

            b.iter(|| {
                let mut builder = repo.state_builder(&state);
                for (routing, acc) in &accounts {
                    builder.insert_account(routing, acc);
                }
                builder.build(None).unwrap()
            });
        });
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .without_plots()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
}

// criterion_group! {
//     name = benches;
//     config = config();
//     targets = bench_scale, bench_delete_ratio, bench_trie_density, bench_payload_type, bench_account_size, bench_vs_node
// }
criterion_group! {
    name = benches;
    config = config();
    targets = bench_trie_density,
}
criterion_main!(benches);
