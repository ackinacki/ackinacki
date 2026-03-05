use std::env;
use std::process::Command;
use std::sync::LazyLock;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use node_types::AccountHash;

use super::aerospike::AerospikeAccountsConfig;
use super::aerospike::AerospikeAccountsStore;
use super::aerospike::DEFAULT_NUM_WRITE_THREADS;
use super::AccountsRepository;
use crate::ThreadAccount;

const CONTAINER_NAME: &str = "aerospike-account-state-test";
const IMAGE: &str = "aerospike/aerospike-server:8.1.0.1";
const NAMESPACE: &str = "node";

static AEROSPIKE: LazyLock<String> = LazyLock::new(|| {
    let address =
        env::var("AEROSPIKE_SOCKET_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    if env::var("AEROSPIKE_EXTERNAL").is_ok() {
        return address;
    }

    // Remove stale container if exists
    let _ = Command::new("docker").args(["rm", "-f", CONTAINER_NAME]).output();

    // Start container
    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-p",
            "3000-3002:3000-3002",
            "-e",
            &format!("NAMESPACE={NAMESPACE}"),
            IMAGE,
        ])
        .output()
        .expect("Failed to start Aerospike container. Is Docker running?");

    assert!(
        output.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );

    // Wait for readiness: verify with an actual write+read round-trip
    for attempt in 1..=30 {
        let check = Command::new("docker")
            .args([
                "exec",
                CONTAINER_NAME,
                "asinfo",
                "-h",
                "127.0.0.1",
                "-p",
                "3000",
                "-v",
                &format!("namespace/{NAMESPACE}"),
            ])
            .output();

        if let Ok(out) = check {
            if out.status.success() && !out.stdout.is_empty() {
                // Namespace responds — verify with an actual client round-trip
                let client_policy = aerospike::ClientPolicy::default();
                if let Ok(client) = aerospike::Client::new(&client_policy, &address) {
                    let key = aerospike::as_key!(NAMESPACE, "readiness", "probe");
                    let bin = aerospike::as_bin!("v", 1);
                    if client.put(&aerospike::WritePolicy::default(), &key, &[bin]).is_ok() {
                        let _ = client.delete(&aerospike::WritePolicy::default(), &key);
                        eprintln!("Aerospike ready (attempt {attempt}/30)");
                        return address;
                    }
                }
                eprintln!("Namespace ready but client not yet operational ({attempt}/30)");
            }
        }
        eprintln!("Waiting for Aerospike... ({attempt}/30)");
        thread::sleep(Duration::from_secs(1));
    }

    panic!("Aerospike did not become ready after 30 attempts");
});

fn ensure_aerospike() -> String {
    AEROSPIKE.clone()
}

fn make_account_hash(seed: u8) -> AccountHash {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    AccountHash::new(bytes)
}

fn make_store_with_set(num_write_threads: usize, set: &str) -> AerospikeAccountsStore {
    let address = ensure_aerospike();
    AerospikeAccountsStore::new(AerospikeAccountsConfig {
        address,
        namespace: NAMESPACE.to_string(),
        set: set.to_string(),
        num_write_threads,
    })
    .expect("Failed to create AerospikeAccountsStore")
}

fn make_store(num_write_threads: usize) -> AerospikeAccountsStore {
    make_store_with_set(num_write_threads, &format!("test-acc-{num_write_threads}"))
}

#[test]
#[ignore]
fn test_sequential_update_and_read() {
    let store = make_store(0);
    let accounts: Vec<_> =
        (0..10).map(|i| (make_account_hash(i), Some(ThreadAccount::default()))).collect();

    store.update(&accounts).expect("Sequential update failed");

    for (hash, _) in &accounts {
        let result = store.get(hash).expect("Read failed");
        assert!(result.is_some(), "Account not found after sequential update");
    }
}

#[test]
#[ignore]
fn test_parallel_update_and_read() {
    let store = make_store(DEFAULT_NUM_WRITE_THREADS);
    let accounts: Vec<_> =
        (10..20).map(|i| (make_account_hash(i), Some(ThreadAccount::default()))).collect();

    store.update(&accounts).expect("Parallel update failed");

    for (hash, _) in &accounts {
        let result = store.get(hash).expect("Read failed");
        assert!(result.is_some(), "Account not found after parallel update");
    }
}

#[test]
#[ignore]
fn test_parallel_update_large_batch() {
    let store = make_store(4);
    let accounts: Vec<_> = (0..100u16)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = (i >> 8) as u8;
            bytes[1] = (i & 0xff) as u8;
            bytes[31] = 0xAA; // marker to avoid collision with other tests
            (AccountHash::new(bytes), Some(ThreadAccount::default()))
        })
        .collect();

    store.update(&accounts).expect("Parallel update of 100 accounts failed");

    for (hash, _) in &accounts {
        let result = store.get(hash).expect("Read failed");
        assert!(result.is_some(), "Account not found after large batch update");
    }
}

#[test]
#[ignore]
fn test_update_with_deletes() {
    let store = make_store(DEFAULT_NUM_WRITE_THREADS);
    let hash = make_account_hash(200);

    // Insert
    store.update(&[(hash, Some(ThreadAccount::default()))]).expect("Insert failed");
    assert!(store.get(&hash).expect("Read failed").is_some());

    // Delete (None value)
    store.update(&[(hash, None)]).expect("Delete failed");
    assert!(store.get(&hash).expect("Read failed").is_none());
}

#[test]
#[ignore]
fn test_parallel_update_mixed_insert_and_delete() {
    let store = make_store(DEFAULT_NUM_WRITE_THREADS);

    // Insert accounts first
    let hashes: Vec<_> = (220..230).map(make_account_hash).collect();
    let inserts: Vec<_> = hashes.iter().map(|h| (*h, Some(ThreadAccount::default()))).collect();
    store.update(&inserts).expect("Initial insert failed");

    // Mixed batch: delete even-indexed, insert new odd-indexed
    let mixed: Vec<_> = hashes
        .iter()
        .enumerate()
        .map(|(i, h)| {
            if i % 2 == 0 {
                (*h, None) // delete
            } else {
                (*h, Some(ThreadAccount::default())) // re-insert
            }
        })
        .collect();
    store.update(&mixed).expect("Mixed parallel update failed");

    for (i, hash) in hashes.iter().enumerate() {
        let result = store.get(hash).expect("Read failed");
        if i % 2 == 0 {
            assert!(result.is_none(), "Even-indexed account should be deleted");
        } else {
            assert!(result.is_some(), "Odd-indexed account should exist");
        }
    }
}

#[test]
#[ignore]
fn test_commit_flushes_to_aerospike() {
    let set_name = "test-commit-flush";

    // Store 1: update + commit
    let store1 = make_store_with_set(DEFAULT_NUM_WRITE_THREADS, set_name);
    let accounts: Vec<_> =
        (30..35).map(|i| (make_account_hash(i), Some(ThreadAccount::default()))).collect();

    store1.update(&accounts).expect("Update failed");
    store1.commit().expect("Commit failed");

    // Store 2: fresh instance with empty cache, same Aerospike set
    let store2 = make_store_with_set(DEFAULT_NUM_WRITE_THREADS, set_name);

    // Reads should hit Aerospike (cache miss) and find the data
    for (hash, _) in &accounts {
        let result = store2.get(hash).expect("Read from second store failed");
        assert!(
            result.is_some(),
            "Account not found in Aerospike after commit — flush did not persist data",
        );
    }
}

#[test]
#[ignore]
fn test_get_cache_hit_and_aerospike_miss() {
    let set_name = "test-cache-hit-miss";

    // Store 1: insert + commit to persist data in Aerospike
    let store1 = make_store_with_set(DEFAULT_NUM_WRITE_THREADS, set_name);
    let aerospike_hash = make_account_hash(40);
    store1.update(&[(aerospike_hash, Some(ThreadAccount::default()))]).expect("Update failed");
    store1.commit().expect("Commit failed");

    // Store 2: fresh cache
    let store2 = make_store_with_set(DEFAULT_NUM_WRITE_THREADS, set_name);

    // Insert a different account only into store2's cache (no commit)
    let cache_hash = make_account_hash(41);
    store2.update(&[(cache_hash, Some(ThreadAccount::default()))]).expect("Update failed");

    // cache_hash: cache hit — served from in-memory cache
    let cached = store2.get(&cache_hash).expect("Cache read failed");
    assert!(cached.is_some(), "Account should be in cache");

    // aerospike_hash: cache miss — served from Aerospike
    let from_as = store2.get(&aerospike_hash).expect("Aerospike read failed");
    assert!(from_as.is_some(), "Account should be readable from Aerospike on cache miss");

    // Non-existent hash: cache miss + Aerospike miss — returns None
    let missing_hash = make_account_hash(42);
    let missing = store2.get(&missing_hash).expect("Read failed");
    assert!(missing.is_none(), "Non-existent account should return None");
}

#[test]
#[ignore]
fn test_cache_eviction_after_flush() {
    let set_name = "test-cache-eviction";
    let store = make_store_with_set(DEFAULT_NUM_WRITE_THREADS, set_name);
    let accounts: Vec<_> =
        (50..55).map(|i| (make_account_hash(i), Some(ThreadAccount::default()))).collect();

    // update() populates cache
    store.update(&accounts).expect("Update failed");
    assert_eq!(store.cache_len(), 5, "Cache should have 5 entries after update");

    // commit() flushes to Aerospike and evicts cache
    store.commit().expect("Commit failed");
    assert_eq!(store.cache_len(), 0, "Cache should be empty after successful flush");

    // Data should still be readable from Aerospike on cache miss
    for (hash, _) in &accounts {
        let result = store.get(hash).expect("Read after eviction failed");
        assert!(result.is_some(), "Account should be readable from Aerospike after cache eviction");
    }
}

#[test]
#[ignore]
fn test_generation_guard_preserves_newer_cache() {
    let set_name = "test-gen-guard";
    let store = make_store_with_set(DEFAULT_NUM_WRITE_THREADS, set_name);
    let hash = make_account_hash(60);

    // First update (v1)
    store.update(&[(hash, Some(ThreadAccount::default()))]).expect("Update v1 failed");
    store.commit().expect("Commit v1 failed");

    // Second update (v2) — happens after flush, so cache has a newer generation
    store.update(&[(hash, Some(ThreadAccount::default()))]).expect("Update v2 failed");
    assert_eq!(store.cache_len(), 1, "Cache should have 1 entry for v2");

    // Commit v2
    store.commit().expect("Commit v2 failed");
    assert_eq!(store.cache_len(), 0, "Cache should be empty after flushing v2");

    // Verify data is still readable
    let result = store.get(&hash).expect("Read failed");
    assert!(result.is_some(), "Account should be readable from Aerospike");
}

#[test]
#[ignore]
fn test_benchmark_sequential_vs_parallel() {
    let batch_size = 200u16;
    let runs = 3;

    let make_accounts = |offset: u16| -> Vec<(AccountHash, Option<ThreadAccount>)> {
        (0..batch_size)
            .map(|i| {
                let n = offset + i;
                let mut bytes = [0u8; 32];
                bytes[0] = (n >> 8) as u8;
                bytes[1] = (n & 0xff) as u8;
                bytes[31] = 0xBB;
                (AccountHash::new(bytes), Some(ThreadAccount::default()))
            })
            .collect()
    };

    // Sequential (num_write_threads = 0)
    let store_seq = make_store(0);
    let mut seq_total = Duration::ZERO;
    for run in 0..runs {
        let accounts = make_accounts(run * batch_size);
        let start = Instant::now();
        store_seq.update(&accounts).expect("Sequential update failed");
        seq_total += start.elapsed();
    }
    let seq_avg = seq_total / runs as u32;

    // Parallel configurations
    let thread_counts = [10, 50];
    eprintln!("\n=== Benchmark: {batch_size} accounts, {runs} runs ===");
    eprintln!("Sequential (0 threads):  avg {seq_avg:?}");

    for threads in thread_counts {
        let store_par = make_store(threads);
        let mut par_total = Duration::ZERO;
        for run in 0..runs {
            let accounts = make_accounts((threads as u16) * 1000 + run * batch_size);
            let start = Instant::now();
            store_par.update(&accounts).expect("Parallel update failed");
            par_total += start.elapsed();
        }
        let par_avg = par_total / runs as u32;
        let speedup = seq_avg.as_secs_f64() / par_avg.as_secs_f64();
        eprintln!("Parallel ({threads:>2} threads):  avg {par_avg:?}  speedup: {speedup:.1}x");
    }
}
