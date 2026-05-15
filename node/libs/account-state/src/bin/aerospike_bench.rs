#![allow(clippy::disallowed_methods)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use account_state::AerospikeKVConfig;
use account_state::AerospikeKVStore;
use account_state::KVRecord;
use aerospike::as_bin;
use aerospike::as_key;
use aerospike::Client;
use aerospike::ClientPolicy;
use aerospike::Key;
use aerospike::WritePolicy;
use clap::Parser;
use rayon::ThreadPoolBuilder;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct Args {
    /// Aerospike host
    #[arg(long, default_value = "127.0.0.1:3000")]
    address: String,

    /// Namespace
    #[arg(long, default_value = "test")]
    namespace: String,

    /// Set name
    #[arg(long, default_value = "bench")]
    set: String,

    /// Number of worker threads
    #[arg(long, default_value_t = 50)]
    threads: usize,

    /// Total number of writes
    #[arg(long, default_value_t = 100_000)]
    total_ops: u64,

    /// Payload size in bytes
    #[arg(long, default_value_t = 5_000)]
    payload_size: usize,

    /// Print intermediate stats every N seconds
    #[arg(long, default_value_t = 1)]
    report_every_sec: u64,

    #[arg(long, default_value_t = 10)]
    duration_sec: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    with_kv(args)
}

fn with_kv(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let store = AerospikeKVStore::new(AerospikeKVConfig {
        address: args.address.clone(),
        namespace: args.namespace.clone(),
        num_write_threads: 48,
        metrics: None,
    })?;
    let t = Instant::now();
    let record_count = 100;
    let value_count = 500;
    for i in 0..record_count {
        let mut records = Vec::with_capacity(value_count);
        for j in 0..value_count {
            records.push(KVRecord {
                key: (i * value_count + j).to_string().into_bytes(),
                data: vec![0x12; args.payload_size],
                generation: 0,
                data_epoch: None,
            });
        }
        println!("putting {} records", i);
        store.put(&args.set, records, false)?;
    }
    let elapsed = t.elapsed().as_secs_f64();
    let total_done = record_count * value_count;
    let ops_per_sec = total_done as f64 / elapsed;
    let mb_per_sec = ops_per_sec * (args.payload_size as f64) / (1024.0 * 1024.0);

    println!();
    println!("=== final ===");
    println!("successful ops : {}", total_done);
    println!("elapsed sec    : {:.3}", elapsed);
    println!("ops/sec        : {:.0}", ops_per_sec);
    println!("throughput     : {:.2} MB/s", mb_per_sec);

    Ok(())
}

fn _with_rayon(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let policy = ClientPolicy::default();
    let client = Arc::new(Client::new(&policy, &args.address)?);

    let write_policy = Arc::new(WritePolicy::default());

    let done = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let bytes_per_op = args.payload_size as f64;

    let pool = ThreadPoolBuilder::new().num_threads(args.threads).build()?;

    let start = Instant::now();

    thread::scope(|scope| {
        {
            let done = Arc::clone(&done);
            let failed = Arc::clone(&failed);
            let stop = Arc::clone(&stop);
            let report_every_sec = args.report_every_sec;

            scope.spawn(move || {
                let mut last_done = 0u64;
                let mut last_t = Instant::now();

                while !stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(report_every_sec));

                    let current_done = done.load(Ordering::Relaxed);
                    let current_failed = failed.load(Ordering::Relaxed);

                    let dt = last_t.elapsed().as_secs_f64();
                    let delta_done = current_done - last_done;
                    let instant_ops = delta_done as f64 / dt;
                    let instant_mb = instant_ops * bytes_per_op / (1024.0 * 1024.0);

                    println!(
                        "progress: done={} failed={} ops/sec={:.0} throughput={:.2} MB/s",
                        current_done, current_failed, instant_ops, instant_mb
                    );

                    last_done = current_done;
                    last_t = Instant::now();
                }
            });
        }

        {
            let client = Arc::clone(&client);
            let write_policy = Arc::clone(&write_policy);
            let done = Arc::clone(&done);
            let failed = Arc::clone(&failed);
            let stop = Arc::clone(&stop);
            let namespace = args.namespace.clone();
            let set = args.set.clone();
            let payload_size = args.payload_size;
            let duration_sec = args.duration_sec;
            let threads = args.threads;

            scope.spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(duration_sec);

                pool.scope(|s| {
                    for tid in 0..threads {
                        let client = Arc::clone(&client);
                        let write_policy = Arc::clone(&write_policy);
                        let done = Arc::clone(&done);
                        let failed = Arc::clone(&failed);
                        let namespace = namespace.clone();
                        let set = set.clone();

                        s.spawn(move |_| {
                            let payload = vec![b'x'; payload_size];
                            let mut i = 0u64;

                            while Instant::now() < deadline {
                                let key: Key = as_key!(
                                    namespace.as_str(),
                                    set.as_str(),
                                    format!("t{tid}-{}", i % 10)
                                );

                                let bins = vec![
                                    as_bin!("thread", tid as i64),
                                    as_bin!("seq", i as i64),
                                    as_bin!("payload", payload.clone()),
                                ];

                                match client.put(&write_policy, &key, &bins) {
                                    Ok(_) => {
                                        done.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(_) => {
                                        failed.fetch_add(1, Ordering::Relaxed);
                                    }
                                }

                                i += 1;
                            }
                        });
                    }
                });

                stop.store(true, Ordering::Relaxed);
            });
        }
    });

    let elapsed = start.elapsed().as_secs_f64();
    let total_done = done.load(Ordering::Relaxed);
    let total_failed = failed.load(Ordering::Relaxed);
    let ops_per_sec = total_done as f64 / elapsed;
    let mb_per_sec = ops_per_sec * (args.payload_size as f64) / (1024.0 * 1024.0);

    println!();
    println!("=== final ===");
    println!("successful ops : {}", total_done);
    println!("failed ops     : {}", total_failed);
    println!("elapsed sec    : {:.3}", elapsed);
    println!("ops/sec        : {:.0}", ops_per_sec);
    println!("throughput     : {:.2} MB/s", mb_per_sec);

    Ok(())
}

fn _with_std(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let policy = ClientPolicy::default();
    let client = Arc::new(Client::new(&policy, &args.address)?);

    let write_policy = Arc::new(WritePolicy::default());

    let done = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let bytes_per_op = args.payload_size as f64;
    let deadline = Instant::now() + Duration::from_secs(args.duration_sec);

    let mut handles = Vec::with_capacity(args.threads + 1);

    {
        let done = Arc::clone(&done);
        let failed = Arc::clone(&failed);
        let stop = Arc::clone(&stop);
        let report_every_sec = args.report_every_sec;

        handles.push(thread::spawn(move || {
            let mut last_done = 0u64;
            let mut last_t = Instant::now();

            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(report_every_sec));

                let current_done = done.load(Ordering::Relaxed);
                let current_failed = failed.load(Ordering::Relaxed);

                let dt = last_t.elapsed().as_secs_f64();
                let delta_done = current_done - last_done;
                let instant_ops = delta_done as f64 / dt;
                let instant_mb = instant_ops * bytes_per_op / (1024.0 * 1024.0);

                println!(
                    "progress: done={} failed={} ops/sec={:.0} throughput={:.2} MB/s",
                    current_done, current_failed, instant_ops, instant_mb
                );

                last_done = current_done;
                last_t = Instant::now();
            }
        }));
    }

    for tid in 0..args.threads {
        let client = Arc::clone(&client);
        let write_policy = Arc::clone(&write_policy);
        let done = Arc::clone(&done);
        let failed = Arc::clone(&failed);
        let namespace = args.namespace.clone();
        let set = args.set.clone();
        let payload_size = args.payload_size;

        handles.push(thread::spawn(move || {
            let payload = vec![b'x'; payload_size];
            let mut i = 0u64;

            while Instant::now() < deadline {
                let key: Key =
                    as_key!(namespace.as_str(), set.as_str(), format!("t{tid}-{}", i % 10));

                let bins = vec![
                    as_bin!("thread", tid as i64),
                    as_bin!("seq", i as i64),
                    as_bin!("payload", payload.clone()),
                ];

                match client.put(&write_policy, &key, &bins) {
                    Ok(_) => {
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        println!("error: {:?}", err);
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }

                i += 1;
            }
        }));
    }

    let start = Instant::now();

    for handle in handles.drain(1..) {
        handle.join().expect("worker thread panicked");
    }

    stop.store(true, Ordering::Relaxed);

    if let Some(report_handle) = handles.pop() {
        report_handle.join().expect("reporter thread panicked");
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_done = done.load(Ordering::Relaxed);
    let total_failed = failed.load(Ordering::Relaxed);
    let ops_per_sec = total_done as f64 / elapsed;
    let mb_per_sec = ops_per_sec * bytes_per_op / (1024.0 * 1024.0);

    println!();
    println!("=== final ===");
    println!("successful ops : {}", total_done);
    println!("failed ops     : {}", total_failed);
    println!("elapsed sec    : {:.3}", elapsed);
    println!("ops/sec        : {:.0}", ops_per_sec);
    println!("throughput     : {:.2} MB/s", mb_per_sec);

    Ok(())
}
