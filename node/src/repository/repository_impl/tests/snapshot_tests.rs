use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use account_state::AccountInfo;
use account_state::ThreadAccountsRepository;
use account_state::ThreadAccountsState;
use anyhow::ensure;
use anyhow::Context;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use versioned_struct::Transitioning;

use super::super::AncestorBlockData;
use super::super::BlockStateRepository;
use super::super::RepositoryImpl;
use super::super::ThreadSnapshot;
use super::super::ThreadSnapshotHeader;
use super::super::STREAMED_THREAD_SNAPSHOT_MAGIC;
use super::finalized_blocks_storage;
use super::mock_bk_set_updates_tx;
use super::ZEROSTATE;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::config::config_read::ConfigRead;
use crate::config::GlobalConfig;
use crate::helper::get_temp_file_path;
use crate::multithreading::routing::service::RoutingService;
use crate::node::services::block_processor::service::MAX_ATTESTATION_TARGET_BETA;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BLOCK_STATISTICS_INITIAL_WINDOW_SIZE;
use crate::node::services::sync::snapshot_compression::COMPRESSED_SNAPSHOT_MAGIC;
use crate::node::services::sync::snapshot_compression::ZSTD_COMPRESSION_LEVEL;
use crate::node::shared_services::SharedServices;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefData;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::storage::MessageDurableStorage;
use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;
use crate::types::ThreadsTable;
use crate::utilities::guarded::Guarded;
use crate::utilities::FixedSizeHashSet;
use crate::versioning::ProtocolVersion;
use crate::zerostate::ZeroState;

const DURABLE_BENCH_CONTAINER: &str = "durable-snapshot-bench-aerospike";
const DURABLE_BENCH_DEFAULT_PORT: &str = "4000";
const DURABLE_BENCH_DEFAULT_SNAPSHOT_PATH: &str =
    "/Volumes/x5/logs/testnet/169719b1a9d0dae48750b55d0fc235f4ad5f08a50ceed8c9053ce84990b818ee";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchSnapshotFormat {
    Auto,
    Node,
    Raw,
}

struct DurableBenchEnv {
    root: PathBuf,
    repo_dir: PathBuf,
    durable_dir: PathBuf,
    export_path: PathBuf,
    aerospike_data_dir: PathBuf,
    aerospike_config_dir: PathBuf,
    aerospike_address: String,
    aerospike_namespace: String,
    aerospike_set_prefix: String,
}

fn durable_bench_root() -> PathBuf {
    if let Some(root) = std::env::var_os("DURABLE_BENCH_ROOT") {
        return PathBuf::from(root);
    }
    let target_dir = std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("node crate must have repository parent")
            .join("target")
    });
    target_dir.join("durable-snapshot-bench")
}

fn durable_bench_env() -> DurableBenchEnv {
    let root = durable_bench_root();
    let repo_dir = root.join("repo");
    let durable_dir = repo_dir.join("durable");
    let export_path = std::env::var_os("EXPORT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("exported.snapshot"));
    let port = std::env::var("AEROSPIKE_HOST_PORT")
        .unwrap_or_else(|_| DURABLE_BENCH_DEFAULT_PORT.to_string());
    DurableBenchEnv {
        aerospike_data_dir: root.join("aerospike"),
        aerospike_config_dir: root.join("aerospike-config"),
        aerospike_address: format!("127.0.0.1:{port}"),
        aerospike_namespace: std::env::var("AEROSPIKE_NAMESPACE")
            .unwrap_or_else(|_| "node".to_string()),
        aerospike_set_prefix: std::env::var("DURABLE_SET_PREFIX")
            .unwrap_or_else(|_| "durable_snapshot_bench".to_string()),
        root,
        repo_dir,
        durable_dir,
        export_path,
    }
}

fn run_command(mut command: Command, description: &str) -> anyhow::Result<String> {
    let output = command.output().with_context(|| format!("Failed to execute {description}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "{description} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn docker_rm_container() -> anyhow::Result<()> {
    let _ = Command::new("docker")
        .args(["rm", "-f", DURABLE_BENCH_CONTAINER])
        .output()
        .context("Failed to execute docker rm")?;
    Ok(())
}

fn docker_container_running() -> anyhow::Result<bool> {
    let output = Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", DURABLE_BENCH_CONTAINER])
        .output()
        .context("Failed to execute docker inspect")?;
    if !output.status.success() {
        return Ok(false);
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
}

fn docker_container_exists() -> anyhow::Result<bool> {
    let output = Command::new("docker")
        .args(["inspect", DURABLE_BENCH_CONTAINER])
        .output()
        .context("Failed to execute docker inspect")?;
    Ok(output.status.success())
}

fn wait_for_aerospike(env: &DurableBenchEnv) -> anyhow::Result<()> {
    for attempt in 1..=60 {
        let info_ready = Command::new("docker")
            .args([
                "exec",
                DURABLE_BENCH_CONTAINER,
                "asinfo",
                "-h",
                "127.0.0.1",
                "-p",
                "4000",
                "-v",
                "build",
            ])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        let client_ready = if info_ready {
            let probe = ThreadAccountsRepository::builder(env.root.join("aerospike-probe"))
                .set_accounts_aerospike_store(
                    &env.aerospike_address,
                    &env.aerospike_namespace,
                    "durable_snapshot_bench_probe",
                )
                .build();
            let _ = std::fs::remove_dir_all(env.root.join("aerospike-probe"));
            probe.is_ok()
        } else {
            false
        };
        if client_ready {
            println!("Aerospike ready after attempt {attempt}/60");
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    let mut logs = Command::new("docker");
    logs.args(["logs", DURABLE_BENCH_CONTAINER]);
    let logs = logs.output().ok();
    anyhow::bail!(
        "Aerospike did not become ready\n{}",
        logs.map(|out| String::from_utf8_lossy(&out.stderr).to_string()).unwrap_or_default()
    );
}

fn write_bench_aerospike_config(env: &DurableBenchEnv) -> anyhow::Result<()> {
    std::fs::create_dir_all(&env.aerospike_config_dir)?;
    let filesize =
        std::env::var("DURABLE_BENCH_AEROSPIKE_FILESIZE").unwrap_or_else(|_| "150G".to_string());
    let config = r#"# Aerospike database configuration file
service {
	cluster-name acki-nacki-node
}

logging {
	console {
		context any info
	}
}

network {
	service {
		address any
		access-address 127.0.0.1
		port 4000
	}

	heartbeat {
		mode mesh
		address local
		port 4002
		interval 150
		timeout 10
	}

	fabric {
		address local
		port 4001
	}
}

namespace node {
	replication-factor 1
	storage-engine device {
		file /opt/aerospike/data/node.dat
		filesize __FILESIZE__
		read-page-cache true
	}
}
"#
    .replace("__FILESIZE__", &filesize);
    std::fs::write(env.aerospike_config_dir.join("aerospike.conf"), config)?;
    Ok(())
}

fn start_bench_aerospike(env: &DurableBenchEnv, clean: bool) -> anyhow::Result<()> {
    if clean {
        docker_rm_container()?;
        let _ = std::fs::remove_dir_all(&env.aerospike_data_dir);
        let _ = std::fs::remove_dir_all(&env.aerospike_config_dir);
    } else if docker_container_running()? {
        wait_for_aerospike(env)?;
        return Ok(());
    } else if docker_container_exists()? {
        let mut start = Command::new("docker");
        start.args(["start", DURABLE_BENCH_CONTAINER]);
        run_command(start, "docker start durable benchmark Aerospike")?;
        wait_for_aerospike(env)?;
        return Ok(());
    }

    std::fs::create_dir_all(&env.aerospike_data_dir)?;
    write_bench_aerospike_config(env)?;
    let image = std::env::var("AEROSPIKE_IMAGE")
        .unwrap_or_else(|_| "aerospike/aerospike-server:8.1.0.1".to_string());
    let port = env
        .aerospike_address
        .rsplit_once(':')
        .map(|(_, port)| port)
        .unwrap_or(DURABLE_BENCH_DEFAULT_PORT);

    let mut run = Command::new("docker");
    run.args([
        "run",
        "-d",
        "--name",
        DURABLE_BENCH_CONTAINER,
        "-p",
        &format!("{port}:4000"),
        "-v",
        &format!("{}:/opt/aerospike/etc", env.aerospike_config_dir.display()),
        "-v",
        &format!("{}:/opt/aerospike/data", env.aerospike_data_dir.display()),
        &image,
        "--config-file",
        "/opt/aerospike/etc/aerospike.conf",
    ]);
    run_command(run, "docker run durable benchmark Aerospike")?;
    wait_for_aerospike(env)
}

fn clean_durable_bench(env: &DurableBenchEnv) -> anyhow::Result<()> {
    docker_rm_container()?;
    let _ = std::fs::remove_dir_all(&env.repo_dir);
    let _ = std::fs::remove_dir_all(&env.aerospike_data_dir);
    let _ = std::fs::remove_file(&env.export_path);
    std::fs::create_dir_all(&env.repo_dir)?;
    Ok(())
}

fn build_bench_repository(env: &DurableBenchEnv) -> anyhow::Result<RepositoryImpl> {
    let block_state_repository = BlockStateRepository::test(env.repo_dir.join("block-state"));
    let accounts_repository = AccountsRepository::new(env.repo_dir.join("accounts"), Some(0), 1);
    let builder = ThreadAccountsRepository::builder(&env.durable_dir).set_accounts_aerospike_store(
        &env.aerospike_address,
        &env.aerospike_namespace,
        &env.aerospike_set_prefix,
    );
    let thread_accounts_repository = match std::env::var("DURABLE_AEROSPIKE_BACKEND").as_deref() {
        Ok("v2") => ThreadAccountsRepository::builder(&env.durable_dir)
            .set_accounts_aerospike2_store(
                &env.aerospike_address,
                &env.aerospike_namespace,
                &env.aerospike_set_prefix,
            )
            .build()?,
        _ => builder.build()?,
    };
    RepositoryImpl::new(
        env.repo_dir.join("repository"),
        Some(PathBuf::from(ZEROSTATE)),
        1,
        SharedServices::test_start(RoutingService::stub().0, u32::MAX),
        Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
        false,
        block_state_repository,
        None,
        accounts_repository,
        thread_accounts_repository,
        MessageDurableStorage::mem(),
        finalized_blocks_storage(),
        mock_bk_set_updates_tx(),
        ConfigRead::new(
            ProtocolVersion::parse("None").unwrap(),
            GlobalConfig::default(),
            None,
            None,
        ),
    )
}

fn bench_ancestor_blocks_data(
    starting_block_id: &BlockIdentifier,
    repository: &RepositoryImpl,
) -> anyhow::Result<Vec<AncestorBlockData>> {
    let mut history = vec![];
    let history_length = std::cmp::max(
        std::cmp::max(
            2 * MAX_ATTESTATION_TARGET_BETA + MAX_ATTESTATION_TARGET_BETA,
            2 * MAX_ATTESTATION_TARGET_BETA + 2,
        ),
        BLOCK_STATISTICS_INITIAL_WINDOW_SIZE,
    ) + 1;
    let mut shared_services = repository.shared_services.clone();
    let mut cursor = *starting_block_id;
    for _ in 0..history_length {
        if cursor == BlockIdentifier::default() {
            break;
        }
        let block_state = repository.block_state_repository.get(&cursor)?;
        let (
            Some(block_seq_no),
            Some(thread_identifier),
            Some(bk_set),
            Some(future_bk_set),
            Some(envelope_hash),
            Some(parent_block_identifier),
            Some(block_version_state),
        ) = block_state.guarded(|e| {
            (
                *e.block_seq_no(),
                *e.thread_identifier(),
                e.bk_set().clone(),
                e.future_bk_set().clone(),
                e.envelope_hash().clone(),
                *e.parent_block_identifier(),
                e.block_version_state().clone(),
            )
        })
        else {
            anyhow::bail!("Failed to get ancestor block data for {cursor:?}");
        };
        let cross_thread_ref_data = shared_services
            .exec(|e| -> anyhow::Result<CrossThreadRefData> {
                e.cross_thread_ref_data_service.get_cross_thread_ref_data(&cursor)
            })
            .unwrap_or_else(|e| {
                println!(
                    "export: ancestor cross_thread_ref_data missing block={} fallback=true error={e}",
                    cursor.to_hex_string(),
                );
                CrossThreadRefData::builder()
                    .block_identifier(cursor)
                    .block_seq_no(block_seq_no)
                    .block_thread_identifier(thread_identifier)
                    .outbound_messages(HashMap::new())
                    .outbound_accounts(HashMap::new())
                    .threads_table(ThreadsTable::default())
                    .parent_block_identifier(parent_block_identifier)
                    .block_refs(vec![])
                    .build()
            });
        history.push(
            AncestorBlockData::builder()
                .block_identifier(cursor)
                .block_seq_no(block_seq_no)
                .thread_identifier(thread_identifier)
                .cross_thread_ref_data(cross_thread_ref_data)
                .bk_set(bk_set.deref().clone())
                .future_bk_set(future_bk_set.deref().clone())
                .envelope_hash(envelope_hash)
                .parent_block_identifier(parent_block_identifier)
                .block_version_state(block_version_state)
                .build(),
        );
        cursor = parent_block_identifier;
    }
    Ok(history)
}

fn ensure_zerostate_threads(repository: &RepositoryImpl) -> anyhow::Result<()> {
    let zerostate = ZeroState::load_from_file(PathBuf::from(ZEROSTATE))?;
    for tid in zerostate.list_threads() {
        let block_id = *zerostate.state(tid)?.get_block_id();
        repository.thread_accounts_repository.ensure_thread(tid, &block_id, false)?;
    }
    Ok(())
}

fn parse_bench_snapshot_format() -> anyhow::Result<BenchSnapshotFormat> {
    Ok(match std::env::var("SNAPSHOT_FORMAT").unwrap_or_else(|_| "auto".to_string()).as_str() {
        "auto" => BenchSnapshotFormat::Auto,
        "node" => BenchSnapshotFormat::Node,
        "raw" => BenchSnapshotFormat::Raw,
        other => anyhow::bail!("Unsupported SNAPSHOT_FORMAT={other}; expected auto|node|raw"),
    })
}

fn detect_snapshot_format(path: &Path) -> anyhow::Result<BenchSnapshotFormat> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; COMPRESSED_SNAPSHOT_MAGIC.len()];
    match file.read_exact(&mut magic) {
        Ok(()) if &magic == COMPRESSED_SNAPSHOT_MAGIC => return Ok(BenchSnapshotFormat::Node),
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            anyhow::bail!("Snapshot file is too short: {}", path.display())
        }
        Err(err) => return Err(err.into()),
    }

    let mut probe = magic.to_vec();
    while probe.len() < STREAMED_THREAD_SNAPSHOT_MAGIC.len() {
        let mut byte = [0u8; 1];
        if file.read(&mut byte)? == 0 {
            break;
        }
        probe.push(byte[0]);
    }
    if probe.as_slice() == STREAMED_THREAD_SNAPSHOT_MAGIC {
        Ok(BenchSnapshotFormat::Node)
    } else {
        Ok(BenchSnapshotFormat::Raw)
    }
}

fn configured_snapshot_format(path: &Path) -> anyhow::Result<BenchSnapshotFormat> {
    match parse_bench_snapshot_format()? {
        BenchSnapshotFormat::Auto => detect_snapshot_format(path),
        format => Ok(format),
    }
}

fn raw_snapshot_ids() -> anyhow::Result<(ThreadIdentifier, BlockIdentifier)> {
    let thread_id = std::env::var("THREAD_ID")
        .ok()
        .map(ThreadIdentifier::try_from)
        .transpose()
        .context("Failed to parse THREAD_ID")?
        .unwrap_or_default();
    let block_id = std::env::var("BLOCK_ID")
        .context("BLOCK_ID is required for raw durable snapshots")?
        .parse::<BlockIdentifier>()
        .context("Failed to parse BLOCK_ID")?;
    Ok((thread_id, block_id))
}

fn open_node_snapshot_reader(path: &Path) -> anyhow::Result<Box<dyn Read>> {
    let mut snapshot_file = File::open(path)?;
    let mut magic = [0u8; COMPRESSED_SNAPSHOT_MAGIC.len()];
    match snapshot_file.read_exact(&mut magic) {
        Ok(()) if &magic == COMPRESSED_SNAPSHOT_MAGIC => {
            Ok(Box::new(zstd::Decoder::new(snapshot_file)?))
        }
        Ok(()) => Ok(Box::new(Cursor::new(magic.to_vec()).chain(snapshot_file))),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            anyhow::bail!("Snapshot file is too short: {}", path.display())
        }
        Err(err) => Err(err.into()),
    }
}

fn read_snapshot_finalized_block(
    reader: &mut dyn Read,
) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
    let mut probe = Vec::with_capacity(STREAMED_THREAD_SNAPSHOT_MAGIC.len());
    let mut byte = [0u8; 1];
    while probe.len() < STREAMED_THREAD_SNAPSHOT_MAGIC.len() {
        match reader.read(&mut byte)? {
            0 => break,
            _ => probe.push(byte[0]),
        }
    }

    if probe.as_slice() == STREAMED_THREAD_SNAPSHOT_MAGIC {
        let header: super::super::StreamedThreadSnapshotHeader =
            bincode::deserialize_from(&mut *reader)?;
        let mut prefix_bytes = vec![0u8; header.prefix_len as usize];
        reader.read_exact(&mut prefix_bytes)?;
        let snapshot = super::super::deserialize_streamed_thread_snapshot_prefix(&prefix_bytes)?;
        return Ok(Some(snapshot.finalized_block));
    }

    let mut legacy_bytes = probe;
    reader.read_to_end(&mut legacy_bytes)?;
    let snapshot = ThreadSnapshot::deserialize_data_compat(&legacy_bytes)?.0;
    Ok(Some(snapshot.finalized_block().clone()))
}

fn load_bench_snapshot_finalized_block(
    block_id: &BlockIdentifier,
) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
    let Some(snapshot_path) = std::env::var_os("SNAPSHOT_PATH").map(PathBuf::from).or_else(|| {
        Some(PathBuf::from(DURABLE_BENCH_DEFAULT_SNAPSHOT_PATH)).filter(|p| p.is_file())
    }) else {
        return Ok(None);
    };
    let mut reader = open_node_snapshot_reader(&snapshot_path)?;
    let Some(block) = read_snapshot_finalized_block(&mut *reader)? else {
        return Ok(None);
    };
    if block.data().identifier() == *block_id {
        Ok(Some(block))
    } else {
        println!(
            "export: snapshot finalized block mismatch expected={} actual={} snapshot={}",
            block_id.to_hex_string(),
            block.data().identifier().to_hex_string(),
            snapshot_path.display(),
        );
        Ok(None)
    }
}

fn write_archive_map_file(
    env: &DurableBenchEnv,
    repository: &RepositoryImpl,
    state: &ThreadAccountsState,
    thread_id: &ThreadIdentifier,
    block_id: &BlockIdentifier,
) -> anyhow::Result<()> {
    let maps_dir = env.durable_dir.join("maps");
    std::fs::create_dir_all(&maps_dir)?;
    let path = maps_dir.join(format!("{}_{}", block_id.to_hex_string(), thread_id.to_hex_string()));
    let tmp = path.with_extension("tmp");
    {
        let file = File::create(&tmp)?;
        let mut writer = std::io::BufWriter::with_capacity(512 * 1024, file);
        repository
            .thread_accounts_repository
            .durable_map_repo()
            .map_repo()
            .map_write(state.durable.map(), &mut writer)?;
        writer.flush()?;
        writer.into_inner()?.sync_all()?;
    }
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn count_durable_map_entries(
    repository: &RepositoryImpl,
    state: &ThreadAccountsState,
) -> (usize, usize) {
    let mut vm = 0usize;
    let mut redirects = 0usize;
    for (_, info) in repository.thread_accounts_repository.durable_map_iter(&state.durable) {
        match info {
            AccountInfo::VmAccountHash(_) => vm += 1,
            AccountInfo::Redirect(_) => redirects += 1,
        }
    }
    (vm, redirects)
}

#[test]
#[ignore = "requires SNAPSHOT_PATH and starts a dedicated Aerospike Docker container"]
fn test_repository_impl_import_durable_snapshot_archive() -> anyhow::Result<()> {
    let total_started = Instant::now();
    let snapshot_path = std::env::var_os("SNAPSHOT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DURABLE_BENCH_DEFAULT_SNAPSHOT_PATH));
    ensure!(snapshot_path.is_file(), "Snapshot file does not exist: {}", snapshot_path.display());
    let env = durable_bench_env();
    let format = configured_snapshot_format(&snapshot_path)?;

    let started = Instant::now();
    clean_durable_bench(&env)?;
    start_bench_aerospike(&env, true)?;
    println!("import: clean_start elapsed={:?}", started.elapsed());

    let started = Instant::now();
    let mut repository = build_bench_repository(&env)?;
    ensure_zerostate_threads(&repository)?;
    println!("import: repository_create elapsed={:?}", started.elapsed());

    let (thread_id, block_id, restored_state) = match format {
        BenchSnapshotFormat::Node => {
            let started = Instant::now();
            let mut reader = open_node_snapshot_reader(&snapshot_path)?;
            let skipped_attestation_ids = Arc::new(Mutex::new(HashSet::default()));
            repository.set_state_from_snapshot_reader(
                &mut reader,
                &ThreadIdentifier::default(),
                skipped_attestation_ids,
            )?;
            println!("import: repository_sync_import elapsed={:?}", started.elapsed());

            let (block_id, _) = repository
                .select_thread_last_finalized_block(&ThreadIdentifier::default())?
                .ok_or_else(|| anyhow::anyhow!("No finalized block after node snapshot import"))?;
            let state = repository
                .get_full_optimistic_state_no_cache(&block_id, &ThreadIdentifier::default(), None)?
                .ok_or_else(|| anyhow::anyhow!("No optimistic state after node snapshot import"))?;
            (ThreadIdentifier::default(), block_id, Arc::unwrap_or_clone(state).shard_state.0)
        }
        BenchSnapshotFormat::Raw => {
            let (thread_id, block_id) = raw_snapshot_ids()?;
            let started = Instant::now();
            repository.thread_accounts_repository.reset_archive()?;
            repository.thread_accounts_repository.reset_accumulator()?;
            let mut file = File::open(&snapshot_path)?;
            let durable = repository
                .thread_accounts_repository
                .import_durable_snapshot_from_reader(&mut file, &thread_id, &block_id)?;
            let mut state = ThreadAccountsRepository::new_state();
            state.durable = durable;
            repository.thread_accounts_repository.state_save(&block_id, &state)?;
            println!("import: repository_raw_durable_import elapsed={:?}", started.elapsed());
            (thread_id, block_id, state)
        }
        BenchSnapshotFormat::Auto => unreachable!("auto format must be resolved before import"),
    };

    let started = Instant::now();
    write_archive_map_file(&env, &repository, &restored_state, &thread_id, &block_id)?;
    println!("import: archive_map_write elapsed={:?}", started.elapsed());

    let optimistic_map = env
        .durable_dir
        .join(format!("_optimistic_state/{}.accounts_map", block_id.to_hex_string()));
    ensure!(
        optimistic_map.is_file(),
        "Optimistic map was not written: {}",
        optimistic_map.display()
    );
    let archive_map = env.durable_dir.join(format!(
        "maps/{}_{}",
        block_id.to_hex_string(),
        thread_id.to_hex_string()
    ));
    ensure!(archive_map.is_file(), "Archive map was not written: {}", archive_map.display());

    let (vm_accounts, redirect_accounts) = count_durable_map_entries(&repository, &restored_state);
    println!(
        "import: done format={format:?} thread={thread_id:?} block={} vm_accounts={vm_accounts} redirects={redirect_accounts} root={} aerospike={} namespace={} set_prefix={} total={:?}",
        block_id.to_hex_string(),
        env.root.display(),
        env.aerospike_address,
        env.aerospike_namespace,
        env.aerospike_set_prefix,
        total_started.elapsed(),
    );
    Ok(())
}

#[test]
#[ignore = "requires imported benchmark repository/archive from test_repository_impl_import_durable_snapshot_archive"]
fn test_repository_impl_export_durable_snapshot_archive() -> anyhow::Result<()> {
    let total_started = Instant::now();
    let env = durable_bench_env();
    start_bench_aerospike(&env, false)?;

    let started = Instant::now();
    let repository = build_bench_repository(&env)?;
    println!("export: repository_create elapsed={:?}", started.elapsed());

    let started = Instant::now();
    let thread_id = ThreadIdentifier::default();
    let (block_id, _) =
        repository.select_thread_last_finalized_block(&thread_id)?.ok_or_else(|| {
            anyhow::anyhow!(
                "No finalized block in benchmark repository; production snapshot export needs \
                 full finalized state, not raw durable-only state"
            )
        })?;
    let full_state = repository
        .get_full_optimistic_state_no_cache(&block_id, &thread_id, None)?
        .ok_or_else(|| anyhow::anyhow!("No optimistic state for block {}", block_id))?;
    let state = Arc::unwrap_or_clone(full_state);
    println!("export: state_load elapsed={:?}", started.elapsed());

    if let Some(parent) = env.export_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let started = Instant::now();
    let parent_dir = env.export_path.parent().ok_or_else(|| {
        anyhow::anyhow!("Export path has no parent: {}", env.export_path.display())
    })?;
    let db_messages = state
        .messages
        .iter(&repository.message_db)
        .map(|range| range.remaining_messages_from_db().unwrap_or_default())
        .collect::<Vec<_>>();
    let db_messages_total: usize = db_messages.iter().map(Vec::len).sum();
    println!("export: db_messages count={db_messages_total} elapsed={:?}", started.elapsed());

    ensure!(
        repository
            .thread_accounts_repository()
            .flush_pending_and_wait_for_drain(Duration::from_secs(60)),
        "Timed out waiting for durable account accumulator to drain before snapshot export",
    );

    let durable_started = Instant::now();
    let mut durable_state_snapshot = tempfile::NamedTempFile::new_in(parent_dir)?;
    repository.thread_accounts_repository().export_durable_snapshot_to_writer(
        &state.shard_state.0,
        durable_state_snapshot.as_file_mut(),
    )?;
    durable_state_snapshot.as_file_mut().sync_all()?;
    let durable_bytes = durable_state_snapshot.as_file().metadata()?.len();
    println!(
        "export: durable_temp bytes={durable_bytes} path={} elapsed={:?}",
        durable_state_snapshot.path().display(),
        durable_started.elapsed(),
    );

    let snapshot_started = Instant::now();
    let serialized_state = bincode::serialize(&state)?;
    let ancestor_blocks_data = bench_ancestor_blocks_data(&block_id, &repository)?;
    let finalized_block = match repository.get_finalized_block(&block_id)? {
        Some(block) => block,
        None => {
            println!(
                "export: finalized block cache miss block={} loading_from_disk=true",
                block_id.to_hex_string(),
            );
            let block =
                match RepositoryImpl::load_block(&repository.get_blocks_dir_path(), &block_id)? {
                    Some(block) => Some(block),
                    None => load_bench_snapshot_finalized_block(&block_id)?,
                };
            if let Some(block) = block {
                Arc::new(block)
            } else {
                anyhow::bail!(
                    "No finalized block envelope for {}; rerun import benchmark with current code \
                     or set SNAPSHOT_PATH to the production snapshot used for import",
                    block_id
                );
            }
        }
    };

    let block_state = repository.block_state_repository.get(&block_id)?;
    let (
        Some(bk_set),
        Some(finalized_block_stats),
        Some(attestation_target),
        Some(producer_selector),
        Some(block_height),
        Some(prefinalization_proof),
        Some(future_bk_set),
        Some(descendant_bk_set),
        Some(descendant_future_bk_set),
        Some(ancestor_blocks_finalization_checkpoints),
        Some(finalizes_blocks),
        Some(parent_id),
        Some(block_protocol_version_state),
    ) = block_state.guarded(|e| {
        (
            e.bk_set().clone(),
            e.block_stats().clone(),
            *e.attestation_target(),
            e.producer_selector_data().clone(),
            *e.block_height(),
            e.prefinalization_proof().clone(),
            e.future_bk_set().clone(),
            e.descendant_bk_set().clone(),
            e.descendant_future_bk_set().clone(),
            e.ancestor_blocks_finalization_checkpoints().clone(),
            e.finalizes_blocks().clone(),
            *e.parent_block_identifier(),
            e.block_version_state().clone(),
        )
    })
    else {
        anyhow::bail!("Failed to get block data for production snapshot export");
    };
    let parent_block_state = repository.block_state_repository.get(&parent_id)?;
    let Some(parent_ancestor_blocks_finalization_checkpoints) =
        parent_block_state.guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
    else {
        anyhow::bail!("Failed to get parent block data for production snapshot export");
    };
    let shared_thread_state = ThreadSnapshot::builder()
        .optimistic_state(serialized_state)
        .ancestor_blocks_data(ancestor_blocks_data)
        .db_messages(db_messages)
        .finalized_block(finalized_block.deref().clone())
        .bk_set(bk_set.deref().clone())
        .future_bk_set(future_bk_set.deref().clone())
        .finalized_block_stats(finalized_block_stats)
        .attestation_target(attestation_target)
        .producer_selector(producer_selector)
        .block_height(block_height)
        .prefinalization_proof(prefinalization_proof)
        .descendant_bk_set(descendant_bk_set.deref().clone())
        .descendant_future_bk_set(descendant_future_bk_set.deref().clone())
        .ancestor_blocks_finalization_checkpoints(ancestor_blocks_finalization_checkpoints)
        .finalizes_blocks(finalizes_blocks)
        .parent_ancestor_blocks_finalization_checkpoints(
            parent_ancestor_blocks_finalization_checkpoints,
        )
        .block_protocol_version_state(block_protocol_version_state)
        .history_data_snapshot(Default::default())
        .durable_state_snapshot(None)
        .finalization_chain(vec![])
        .build();

    let header = ThreadSnapshotHeader {
        block_id: shared_thread_state.finalized_block().data().identifier(),
        thread_id: *shared_thread_state.finalized_block().data().common_section().thread_id(),
        seq_no: shared_thread_state.finalized_block().data().seq_no(),
        round: *shared_thread_state.finalized_block().data().common_section().round(),
    };

    let tmp_file_path = get_temp_file_path(parent_dir);
    let mut tmp_file = File::create(&tmp_file_path)?;
    tmp_file.write_all(COMPRESSED_SNAPSHOT_MAGIC)?;
    let mut encoder = zstd::Encoder::new(tmp_file, ZSTD_COMPRESSION_LEVEL)?;
    super::super::write_streamed_thread_snapshot_to_writer(
        &shared_thread_state,
        Some(durable_state_snapshot.path()),
        &mut encoder,
    )?;
    let tmp_file = encoder.finish()?;
    tmp_file.sync_all()?;
    std::fs::rename(&tmp_file_path, &env.export_path)?;

    let header_path = RepositoryImpl::snapshot_header_path(&env.export_path);
    if !std::fs::exists(&header_path)? {
        let header_bytes = bincode::serialize(&header)?;
        std::fs::write(&header_path, header_bytes)?;
    }
    println!(
        "export: production_snapshot path={} elapsed={:?}",
        env.export_path.display(),
        snapshot_started.elapsed(),
    );
    let export_elapsed = started.elapsed();
    let output_bytes = std::fs::metadata(&env.export_path)?.len();
    let (vm_accounts, redirect_accounts) =
        count_durable_map_entries(&repository, &state.shard_state.0);

    println!(
        "export: done thread={thread_id:?} block={} vm_accounts={vm_accounts} redirects={redirect_accounts} bytes={output_bytes} output={} export_elapsed={export_elapsed:?} total={:?}",
        block_id.to_hex_string(),
        env.export_path.display(),
        total_started.elapsed(),
    );
    Ok(())
}
