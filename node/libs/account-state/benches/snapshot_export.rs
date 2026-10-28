//! Compare production scan export with ordered batch-get partition export.
//!
//! `cargo bench -p account-state --bench snapshot_export -- --address 127.0.0.1:3300
//! --accounts 100000 --thread-accounts 10000 --partition-mib 16`
//!
//! Use an isolated Aerospike instance (namespace `test`). Normally creates fresh
//! randomly prefixed archive sets and cleans only those account sets after use.
//! --reuse-prefix plus --reuse-scan-map reads an existing benchmark archive and
//! only the map header of its old scan output, without seeding or resetting data.
//! Remove the disposable instance after use to remove residual set metadata.
//! Without --address, runs an in-memory
//! correctness smoke test, NOT an Aerospike performance measurement.
//!
//! Timings include production body validation and wire encoding into a counting
//! sink; no account output is stored in memory or written to disk. Fixture
//! creation, partition planning, compression, HTTP and primary/message blobs
//! are excluded. Runs all scan rounds first, then all ordered export rounds.
//! First-ready measures partition encoding completion, not file publication.
//! No concurrent updates: the initialized archive is an immutable pin equivalent.
//! Caches are NOT reset; this is a fixed-order comparison, not cold-cache A/B.
//! `--mode legacy` exercises production legacy export, including rebuilding and
//! rewriting its account map. `RUST_LOG=monit=info` reports stage times.
//! `--mode scan` is the current scan exporter without legacy conversion, NOT an
//! old node binary; comparing them isolates conversion, not the full regression.
//! Production exporters perform their own hash, size and coverage checks; this
//! benchmark does not re-import output because the sink discards its bytes.
//! --read-only adds a storage-only comparison: scan checks full routing keys
//! against the Merkle map; ordered reads keys in map order. Neither decodes,
//! validates, hashes or serializes account bodies. Bytes are storage payload
//! bytes (including the archive's serialization envelope), not export bytes.
use std::fs::File;
use std::hint::black_box;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use account_state::write_account_partition_checked;
use account_state::AccountCapacity;
use account_state::AccountInfo;
use account_state::AccountPartition;
use account_state::AerospikeKVConfig;
use account_state::AerospikeKVStore;
use account_state::ArchiveStateStore;
use account_state::ArchiveStoreConfig;
use account_state::DurableThreadAccountsRepository;
use account_state::DurableThreadAccountsState;
use account_state::KVStore;
use account_state::PartitionReadLimits;
use account_state::ThreadAccount;
use account_state::ThreadAccountMap;
use clap::Parser;
use clap::ValueEnum;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;
use rayon::prelude::*;
use tvm_types::BuilderData;

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Both,
    Scan,
    Ordered,
    /// Production legacy scan export, including rebuilding the old account map.
    Legacy,
}

#[derive(Parser)]
struct Args {
    /// Omit for an in-memory correctness smoke test.
    #[arg(long)]
    address: Option<String>,
    /// Reuse an existing benchmark archive without seeding or cleaning its data.
    #[arg(long, requires_all = ["address", "reuse_scan_map"])]
    reuse_prefix: Option<String>,
    /// Read only the Merkle-map header from a previous scan export file.
    #[arg(long, requires = "reuse_prefix")]
    reuse_scan_map: Option<PathBuf>,
    #[arg(long, default_value = "test")]
    namespace: String,
    /// Total accounts across the selected and background threads.
    #[arg(long, default_value_t = 200)]
    accounts: usize,
    #[arg(long, default_value_t = 100)]
    thread_accounts: usize,
    #[arg(long, default_value_t = 9)]
    background_threads: usize,
    #[arg(long, default_value_t = 10)]
    dapps: usize,
    #[arg(long, default_value_t = 1)]
    partition_mib: u64,
    /// Runs per mode: all scan rounds first, then all ordered rounds.
    #[arg(long, default_value_t = 1)]
    rounds: usize,
    /// Select one mode to allow restarting Aerospike between mode groups.
    #[arg(long, value_enum, default_value = "both")]
    mode: Mode,
    /// Only read storage: no account validation or export encoding.
    #[arg(long)]
    read_only: bool,
    #[arg(long, default_value_t = 1024)]
    min_bytes: usize,
    #[arg(long, default_value_t = 16384)]
    max_bytes: usize,
    /// Zero disables rare 2 MiB raw-data outliers (BOCs will be larger).
    #[arg(long, default_value_t = 0)]
    oversized_every: usize,
    #[arg(long, default_value_t = 256)]
    batch_accounts: usize,
    #[arg(long, default_value_t = 8)]
    batch_mib: u64,
    #[arg(long, default_value_t = 8)]
    in_flight: usize,
    /// Storage readers, independent of the total outstanding-batch limit.
    #[arg(long, default_value_t = 4)]
    read_workers: usize,
    /// Account validators, independent of storage readers and queue depth.
    #[arg(long, default_value_t = 1)]
    validation_workers: usize,
}

fn hash(domain: &str, index: usize) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain.as_bytes());
    hasher.update(&(index as u64).to_le_bytes());
    hasher.finalize().into()
}

fn make_account(index: usize, args: &Args) -> anyhow::Result<(AccountRouting, ThreadAccount)> {
    let routing = AccountRouting::new(
        DAppIdentifier::new(hash("dapp", index % args.dapps)),
        AccountIdentifier::new(hash("account", index)),
    );
    let random = u64::from_le_bytes(hash("size", index)[..8].try_into()?);
    let size = if args.oversized_every > 0 && index.is_multiple_of(args.oversized_every) {
        2 * 1024 * 1024
    } else {
        args.min_bytes + (random % (args.max_bytes - args.min_bytes + 1) as u64) as usize
    };
    let mut random = blake3::Hasher::new();
    random.update(&hash("payload", index));
    let mut random = random.finalize_xof();
    let mut cells = Vec::new();
    for start in (0..size).step_by(127) {
        let mut bytes = vec![0; (size - start).min(127)];
        random.fill(&mut bytes);
        let mut cell = BuilderData::new();
        cell.append_raw(&bytes, bytes.len() * 8).map_err(|e| anyhow::anyhow!("{e}"))?;
        cells.push(cell.into_cell().map_err(|e| anyhow::anyhow!("{e}"))?);
    }
    // A balanced cell tree, not a long snake that would exceed TVM depth limits.
    while cells.len() > 1 {
        cells = cells
            .chunks(4)
            .map(|children| {
                let mut parent = BuilderData::new();
                for child in children {
                    parent
                        .checked_append_reference(child.clone())
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                parent.into_cell().map_err(|e| anyhow::anyhow!("{e}"))
            })
            .collect::<anyhow::Result<_>>()?;
    }
    let address = tvm_block::MsgAddressInt::with_standart(
        None,
        0,
        tvm_types::AccountId::from_raw(routing.account_id().as_slice().to_vec(), 256),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let storage = tvm_block::AccountStorage::active_by_init_code_hash(
        0,
        tvm_block::CurrencyCollection::with_grams(1_000_000_000),
        tvm_block::StateInit { data: cells.pop(), ..Default::default() },
        true,
    );
    let mut account = tvm_block::Account::with_storage(
        &address,
        &tvm_block::StorageInfo::with_values(0, None),
        &storage,
    );
    account.update_storage_stat().map_err(|e| anyhow::anyhow!("{e}"))?;
    let shard = tvm_block::ShardAccount::with_params(
        &account,
        hash("transaction", index).into(),
        index as u64,
        Some(routing.dapp_id().as_array().into()),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok((routing, ThreadAccount::from_tvm(shard)))
}

fn populate(archive: &ArchiveStateStore, args: &Args) -> anyhow::Result<ThreadAccountMap> {
    let mut map = ThreadAccountMap::new();
    let background = args.accounts - args.thread_accounts;
    for thread in 0..=args.background_threads {
        archive.ensure_thread(
            &ThreadIdentifier::new(&BlockIdentifier::default(), u16::try_from(thread)?),
            &BlockIdentifier::default(),
        )?;
    }
    // Only this invocation's fresh random prefix is touched. Initialization
    // requires registered threads in Uninitialized, not ensure_thread's Idle.
    archive.reset()?;
    for thread in 0..=args.background_threads {
        let count = if thread == 0 {
            args.thread_accounts
        } else {
            background / args.background_threads
                + usize::from(thread <= background % args.background_threads)
        };
        if count == 0 {
            continue;
        }
        let start = if thread == 0 {
            0
        } else {
            args.thread_accounts
                + (thread - 1) * (background / args.background_threads)
                + (thread - 1).min(background % args.background_threads)
        };
        let thread_id = ThreadIdentifier::new(&BlockIdentifier::default(), u16::try_from(thread)?);
        archive.thread_init_from_entries(
            &thread_id,
            BlockIdentifier::default(),
            (start..start + count).map(|index| {
                let (routing, account) = make_account(index, args)?;
                if thread == 0 {
                    map = map.update(&[(
                        routing,
                        Some((
                            AccountInfo::AccountHash(account.account_hash()),
                            AccountCapacity::new(account.canonical_size()),
                        )),
                    )]);
                }
                Ok((routing, account))
            }),
        )?;
        if thread == 0 || thread.is_multiple_of(32) || thread == args.background_threads {
            println!("SEEDED thread={thread} accounts={count}");
        }
    }
    Ok(map)
}

/// Discard output while preserving seek-back header accounting for scan export.
/// Unlike Cursor<Vec<u8>>, memory use does not grow with snapshot size.
#[derive(Default)]
struct CountingSink {
    position: u64,
    length: u64,
}

impl Write for CountingSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let len = black_box(bytes).len();
        self.position = self
            .position
            .checked_add(len as u64)
            .ok_or_else(|| std::io::Error::other("sink position overflow"))?;
        self.length = self.length.max(self.position);
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for CountingSink {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.position = match position {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => self.length.checked_add_signed(offset),
        }
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid sink seek")
        })?;
        Ok(self.position)
    }
}

fn check_sink() -> anyhow::Result<()> {
    let mut sink = CountingSink::default();
    sink.write_all(&[0; 16])?;
    sink.seek(SeekFrom::Start(0))?;
    sink.write_all(&[1; 8])?;
    anyhow::ensure!(
        sink.length == 16 && sink.stream_position()? == 8,
        "header rewrite must not increase output length"
    );
    anyhow::ensure!(sink.seek(SeekFrom::End(-17)).is_err(), "negative seek accepted");
    sink.seek(SeekFrom::End(0))?;
    sink.write_all(&[2])?;
    anyhow::ensure!(sink.length == 17, "append accounting mismatch");
    Ok(())
}

#[derive(Clone, Copy)]
struct Timing {
    first: f64,
    total: f64,
    bytes: u64,
}

fn scan_export(
    repo: &DurableThreadAccountsRepository,
    map: &ThreadAccountMap,
    legacy: bool,
) -> anyhow::Result<Timing> {
    let state = DurableThreadAccountsState::with_map(map.clone());
    let start = Instant::now();
    let mut sink = CountingSink::default();
    if legacy {
        repo.export_snapshot_v1_to_writer_checked(&state, &mut sink, &|| false)?;
    } else {
        repo.export_durable_snapshot_to_writer(&state, &mut sink)?;
    }
    let total = start.elapsed().as_secs_f64();
    Ok(Timing { first: total, total, bytes: sink.length })
}

fn ordered_export(
    repo: &DurableThreadAccountsRepository,
    parts: &[AccountPartition],
    limits: PartitionReadLimits,
) -> anyhow::Result<Timing> {
    let start = Instant::now();
    let mut result = Timing { first: 0.0, total: 0.0, bytes: 0 };
    for (index, part) in parts.iter().enumerate() {
        let mut sink = BufWriter::with_capacity(1024 * 1024, CountingSink::default());
        write_account_partition_checked(repo, &part.map, &mut sink, limits, &|| false)?;
        sink.flush()?;
        result.bytes += sink.get_ref().length;
        if index == 0 {
            result.first = start.elapsed().as_secs_f64();
        }
    }
    result.total = start.elapsed().as_secs_f64();
    Ok(result)
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

// Archive keys contain the complete routing, so membership needs no body decode.
fn storage_key(routing: &AccountRouting) -> Vec<u8> {
    [routing.dapp_id().as_slice(), routing.account_id().as_slice()].concat()
}

fn scan_read(kv: &KVStore, set: &str, map: &ThreadAccountMap) -> anyhow::Result<Timing> {
    let start = Instant::now();
    let mut bytes = 0;
    let mut accounts = 0;
    kv.enumerate_checked(set, &|| false, &mut |record| {
        anyhow::ensure!(record.key.len() == 64, "invalid archive routing key");
        let routing = AccountRouting::new(
            DAppIdentifier::new(record.key[..32].try_into()?),
            AccountIdentifier::new(record.key[32..].try_into()?),
        );
        if map.get(&routing).is_some() {
            bytes += record.data.len() as u64;
            accounts += 1;
            black_box(record.data.as_slice());
        }
        Ok(())
    })?;
    let total = start.elapsed().as_secs_f64();
    anyhow::ensure!(accounts == map.iter().count(), "scan read coverage mismatch");
    Ok(Timing { first: total, total, bytes })
}

fn ordered_read(
    kv: &KVStore,
    set: &str,
    parts: &[AccountPartition],
    limits: PartitionReadLimits,
) -> anyhow::Result<Timing> {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(limits.read_workers).build()?;
    let start = Instant::now();
    let mut result = Timing { first: 0.0, total: 0.0, bytes: 0 };
    for (index, part) in parts.iter().enumerate() {
        let mut entries = part.map.iter().peekable();
        while entries.peek().is_some() {
            let mut window = Vec::with_capacity(limits.in_flight_batches);
            for _ in 0..limits.in_flight_batches {
                let mut keys = Vec::with_capacity(limits.accounts_per_batch);
                let mut bytes = 0;
                while let Some((routing, _)) = entries.peek() {
                    let size = part.map.account_capacity(routing).unwrap().bytes();
                    if !keys.is_empty()
                        && (keys.len() >= limits.accounts_per_batch
                            || bytes + size > limits.canonical_bytes_per_batch)
                    {
                        break;
                    }
                    bytes += size;
                    keys.push(storage_key(routing));
                    entries.next();
                }
                if keys.is_empty() {
                    break;
                }
                window.push(keys);
            }
            // Collect preserves batch order; at most one bounded window is held.
            let batches: Vec<_> =
                pool.install(|| window.par_iter().map(|keys| kv.get(set, keys)).collect());
            for (keys, batch) in window.iter().zip(batches) {
                let batch = batch?;
                anyhow::ensure!(batch.len() == keys.len(), "batch read coverage mismatch");
                for record in batch {
                    let record =
                        record.ok_or_else(|| anyhow::anyhow!("missing archive account"))?;
                    result.bytes += record.data.len() as u64;
                    black_box(record.data.as_slice());
                }
            }
        }
        if index == 0 {
            result.first = start.elapsed().as_secs_f64();
        }
    }
    result.total = start.elapsed().as_secs_f64();
    Ok(result)
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    // Cargo appends --bench for harness=false executables.
    let args = Args::parse_from(std::env::args().filter(|arg| arg != "--bench" && arg != "--test"));
    anyhow::ensure!(
        !matches!(args.mode, Mode::Legacy) || !args.read_only,
        "legacy mode measures validated export, not read-only storage"
    );
    anyhow::ensure!(
        args.accounts >= args.thread_accounts
            && args.thread_accounts > 0
            && (args.background_threads > 0 || args.accounts == args.thread_accounts)
            && args.dapps > 0
            && args.rounds > 0
            && args.min_bytes > 0
            && args.max_bytes >= args.min_bytes
            && args.max_bytes - args.min_bytes < usize::MAX
            && u16::try_from(args.background_threads).is_ok(),
        "invalid dataset/round parameters"
    );
    let target = args
        .partition_mib
        .checked_mul(1024 * 1024)
        .filter(|x| *x > 0)
        .ok_or_else(|| anyhow::anyhow!("invalid partition size"))?;
    let limits = PartitionReadLimits {
        accounts_per_batch: args.batch_accounts,
        canonical_bytes_per_batch: args
            .batch_mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| anyhow::anyhow!("batch size overflow"))?,
        in_flight_batches: args.in_flight,
        read_workers: args.read_workers,
        validation_workers: args.validation_workers,
    };
    anyhow::ensure!(
        limits.accounts_per_batch > 0
            && limits.canonical_bytes_per_batch > 0
            && limits.in_flight_batches > 0
            && limits.read_workers > 0
            && limits.validation_workers > 0,
        "invalid batch limits"
    );
    check_sink()?;
    let files = tempfile::tempdir()?;
    let prefix = args.reuse_prefix.clone().unwrap_or_else(|| {
        format!("export_{}", files.path().file_name().unwrap().to_string_lossy().replace('.', "_"))
    });
    anyhow::ensure!(prefix.starts_with("export_"), "only benchmark archive prefixes may be reused");
    let kv = match &args.address {
        Some(address) => KVStore::Aerospike(AerospikeKVStore::new_v2(AerospikeKVConfig {
            address: address.clone(),
            namespace: args.namespace.clone(),
            num_write_threads: 4,
            metrics: None,
        })?),
        None => KVStore::InMemory(Default::default()),
    };
    let archive = ArchiveStateStore::new(
        kv.clone(),
        ArchiveStoreConfig {
            aerospike_address: args.address.clone(),
            node_id: prefix.clone(),
            write_parallelism: 4,
        },
    )?;
    println!("FIXTURE backend={} prefix={prefix} accounts={} selected={} background_threads={} cache=uncontrolled compression=none output=counting-sink rounds={} order=all-scan-then-all-ordered",
        if args.address.is_some() { "aerospike-v2" } else { "memory-smoke-only" },
        args.accounts, args.thread_accounts, args.background_threads, args.rounds);
    println!("LIMITS batch_accounts={} batch_bytes={} in_flight={} read_workers={} validation_workers={} rayon_threads={} scan_workers_env={} scan_queue_env={}",
        limits.accounts_per_batch, limits.canonical_bytes_per_batch, limits.in_flight_batches,
        limits.read_workers, limits.validation_workers,
        rayon::current_num_threads(),
        std::env::var("SNAPSHOT_EXPORT_VALIDATION_WORKERS").unwrap_or_else(|_| "default".into()),
        std::env::var("SNAPSHOT_EXPORT_VALIDATION_QUEUE_BATCHES").unwrap_or_else(|_| "default".into()));
    let start = Instant::now();
    let map = if let Some(path) = &args.reuse_scan_map {
        let mut source = BufReader::new(File::open(path)?);
        let mut length = [0; 8];
        source.read_exact(&mut length)?;
        let mut header = source.take(u64::from_le_bytes(length));
        let map = ThreadAccountMap::read(&mut header)?;
        anyhow::ensure!(header.limit() == 0, "scan map header length mismatch");
        anyhow::ensure!(
            map.iter().count() == args.thread_accounts,
            "reused map account count does not match --thread-accounts"
        );
        println!(
            "REUSED prefix={prefix} map_header={} account_bodies_not_loaded=true",
            path.display()
        );
        map
    } else {
        populate(&archive, &args)?
    };
    let parts = map.partition_by_capacity(target).map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let merged =
        ThreadAccountMap::merge_partitions(&parts).map_err(|e| anyhow::anyhow!("{e:?}"))?;
    anyhow::ensure!(
        merged.hash() == map.hash() && merged.capacity() == map.capacity(),
        "partition reconstruction mismatch"
    );
    println!(
        "DATASET setup_s={:.3} canonical_bytes={} partitions={} first_partition_bytes={}",
        start.elapsed().as_secs_f64(),
        map.capacity().bytes(),
        parts.len(),
        parts[0].capacity
    );
    let repo =
        DurableThreadAccountsRepository::new(files.path().join("durable"), archive.clone(), None)?;
    let pinned = archive.pin_current_account_sets();
    let read_set = format!("{prefix}_accounts_A_e{}", pinned.epoch());
    println!(
        "WORKLOAD read_only={} read_set={read_set} byte_unit={}",
        args.read_only,
        if args.read_only { "storage-payload" } else { "export" }
    );
    let modes: &[&str] = match args.mode {
        Mode::Both => &["scan_full_thread", "ordered_all_partitions"],
        Mode::Scan => &["scan_full_thread"],
        Mode::Ordered => &["ordered_all_partitions"],
        Mode::Legacy => &["legacy_full_thread"],
    };
    for &mode in modes {
        let mut samples = Vec::with_capacity(args.rounds);
        for round in 1..=args.rounds {
            let result = if args.read_only && mode == "scan_full_thread" {
                scan_read(&kv, &read_set, &map)?
            } else if args.read_only {
                ordered_read(&kv, &read_set, &parts, limits)?
            } else if mode == "scan_full_thread" {
                scan_export(&repo, &map, false)?
            } else if mode == "legacy_full_thread" {
                scan_export(&repo, &map, true)?
            } else {
                ordered_export(&repo, &parts, limits)?
            };
            println!(
                "RESULT mode={mode} read_only={} round={round} first_ready_s={:.6} total_s={:.6} bytes={}",
                args.read_only, result.first, result.total, result.bytes
            );
            samples.push(result);
        }
        println!(
            "MEDIAN mode={mode} first_ready_s={:.6} total_s={:.6}",
            median(samples.iter().map(|s| s.first).collect()),
            median(samples.iter().map(|s| s.total).collect())
        );
    }
    drop(pinned);
    drop(repo);
    // This is the generated fixture prefix, never an operator-supplied archive.
    if args.reuse_prefix.is_none() {
        archive.reset()?;
    }
    Ok(())
}
