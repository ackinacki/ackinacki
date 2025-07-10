// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use ::node::block::producer::process::TVMBlockProducerProcess;
use ::node::bls::GoshBLS;
use ::node::helper::bm_license_loader::load_bm_license_contract_pubkey;
use ::node::helper::init_tracing;
use ::node::helper::key_handling::key_pairs_from_file;
use ::node::message::WrappedMessage;
use ::node::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use ::node::node::NetworkMessage;
use ::node::node::Node;
use ::node::repository::optimistic_state::OptimisticState;
use ::node::repository::repository_impl::FinalizedBlockStorage;
use ::node::repository::repository_impl::RepositoryImpl;
use anyhow::bail;
use clap::Parser;
use database::documents_db::DocumentsDb;
use database::sqlite::sqlite_helper;
use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::sqlite_helper::SqliteHelperConfig;
// use transport_layer::wtransport::WTransport;
use gossip::GossipConfig;
use http_server::BlockKeeperSetUpdate;
use http_server::ResolvingResult;
use message_router::message_router::MessageRouter;
use message_router::message_router::MessageRouterConfig;
use message_router::read_keys_from_file;
use network::config::NetworkConfig;
use network::network::BasicNetwork;
use network::network::PeerData;
use network::resolver::sign_gossip_node;
use network::resolver::WatchGossipConfig;
use node::block_keeper_system::BlockKeeperSet;
use node::config::load_blockchain_config;
use node::config::load_config_from_file;
use node::external_messages::ExternalMessagesThreadState;
use node::helper::bp_resolver::BPResolverImpl;
use node::helper::metrics::Metrics;
use node::helper::shutdown_tracing;
use node::message_storage::MessageDurableStorage;
use node::multithreading::routing::service::Command;
use node::multithreading::routing::service::RoutingService;
use node::node::block_request_service::BlockRequestService;
use node::node::block_state::repository::BlockStateRepository;
use node::node::block_state::state::AttestationsTarget;
use node::node::services::attestations_target::service::AttestationsTargetService;
use node::node::services::block_processor::service::BlockProcessorService;
use node::node::services::block_processor::service::SecurityGuarantee;
use node::node::services::db_serializer::DBSerializeService;
use node::node::services::send_attestations::AttestationSendService;
use node::node::services::send_attestations::AttestationSendServiceHandler;
use node::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use node::node::services::sync::ExternalFileSharesBased;
use node::node::services::sync::FileSavingService;
use node::node::services::validation::feedback::AckiNackiSend;
use node::node::services::validation::service::ValidationService;
use node::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use node::node::NodeIdentifier;
use node::protocol::authority_switch;
use node::protocol::authority_switch::action_lock::Authority;
use node::repository::accounts::AccountsRepository;
use node::repository::load_saved_blocks::SavedBlocksLoader;
use node::repository::Repository;
use node::services::blob_sync;
use node::types::bp_selector::ProducerSelector;
use node::types::calculate_hash;
use node::types::BlockHeight;
use node::types::BlockIdentifier;
use node::types::BlockSeqNo;
use node::types::CollectedAttestations;
use node::types::ThreadIdentifier;
use node::utilities::thread_spawn_critical::SpawnCritical;
use node::utilities::FixedSizeHashSet;
use node::zerostate::ZeroState;
use parking_lot::Mutex;
use rand::prelude::SeedableRng;
use rand::prelude::SmallRng;
use signal_hook::consts::SIGHUP;
use signal_hook::consts::SIGINT;
use signal_hook::consts::SIGTERM;
use signal_hook::iterator::Signals;
use telemetry_utils::mpsc::instrumented_channel;
use tokio::task::JoinHandle;
use transport_layer::msquic::MsQuicTransport;
use transport_layer::TlsCertCache;
use tvm_block::Serializable;
use tvm_types::base64_encode;
use tvm_types::AccountId;

// const ALIVE_NODES_WAIT_TIMEOUT_MILLIS: u64 = 100;
const MINIMUM_NUMBER_OF_CORES: usize = 8;
const DEFAULT_NACK_SIZE_CACHE: usize = 1000;

lazy_static::lazy_static!(
    static ref LONG_VERSION: String = format!("{}\nBUILD_GIT_BRANCH={}\nBUILD_GIT_COMMIT={}\nBUILD_GIT_DATE={}\nBUILD_TIME={}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_GIT_BRANCH"),
        env!("BUILD_GIT_COMMIT"),
        env!("BUILD_GIT_DATE"),
        env!("BUILD_TIME"),
    );
);

/// Acki-Nacki Node
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config_path: PathBuf,
}

#[cfg(feature = "rayon_affinity")]
fn init_rayon() {
    let cores = core_affinity::get_core_ids().expect("Unable to get core IDs");
    // Build Rayon thread pool and set affinity for each thread
    rayon::ThreadPoolBuilder::new()
        .num_threads(cores.len())
        .thread_name(|thread_index| format!("rayon{}", thread_index))
        .start_handler(move |thread_index| {
            let core_id = cores[thread_index % cores.len()];
            core_affinity::set_for_current(core_id);
        })
        .build_global()
        .unwrap();
}

fn main() -> Result<(), std::io::Error> {
    eprintln!("Starting Acki-Nacki Node version: {}", *LONG_VERSION);
    debug_used_features();
    if cfg!(debug_assertions) {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    #[cfg(feature = "rayon_affinity")]
    init_rayon();

    // unsafe { backtrace_on_stack_overflow::enable() };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(100 * 1024 * 1024)
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(async { tokio_main().await });

    exit(0);
}

async fn tokio_main() {
    let args = Args::parse();
    let metrics = init_tracing();
    tracing::info!("Tracing and metrics initialized");

    #[cfg(feature = "misbehave")]
    tracing::warn!("Node run with --feature misbehave set");

    let exit_code = match execute(args, metrics).await {
        Ok(_) => 0,
        Err(err) => {
            tracing::error!("{err:?}");
            1
        }
    };
    shutdown_tracing();
    exit(exit_code);
}

fn bk_set_update(
    seq_no: u32,
    current: Option<&BlockKeeperSet>,
    future: Option<&BlockKeeperSet>,
) -> BlockKeeperSetUpdate {
    BlockKeeperSetUpdate {
        seq_no,
        current: collect_node_id_owner_pk(current),
        future: collect_node_id_owner_pk(future),
    }
}

fn collect_node_id_owner_pk(bk_set: Option<&BlockKeeperSet>) -> Vec<(String, [u8; 32])> {
    let Some(bk_set) = bk_set else {
        return vec![];
    };
    bk_set
        .iter_node_ids()
        .filter_map(|node_id| {
            bk_set.get_by_node_id(node_id).map(|x| (node_id.to_string(), x.owner_pubkey))
        })
        .collect()
}

#[allow(clippy::await_holding_lock)]
async fn execute(args: Args, metrics: Option<Metrics>) -> anyhow::Result<()> {
    tracing::info!("Starting network");

    tracing::info!("Loading config");
    let tls_cert_cache = TlsCertCache::new()?;
    let config = load_config_from_file(&args.config_path)?.ensure_min_cpu(MINIMUM_NUMBER_OF_CORES);
    let network_config = config.network_config(Some(tls_cert_cache.clone()))?;
    let gossip_config = config.gossip_config()?;
    tracing::info!("Loaded config");

    tracing::info!("Node config: {}", serde_json::to_string_pretty(&config)?);
    tracing::info!("Gossip seeds expanded: {:?}", gossip_config.seeds);
    tracing::info!("Gossip advertise addr: {:?}", gossip_config.advertise_addr);

    let zerostate =
        ZeroState::load_from_file(&config.local.zerostate_path).expect("Failed to open zerostate");
    let bk_set = zerostate.get_block_keeper_set()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let initial_bk_set_update = bk_set_update(0, Some(&bk_set), None);
    let (bk_set_update_async_tx, bk_set_update_async_rx) =
        tokio::sync::watch::channel(initial_bk_set_update.clone());
    let (watch_gossip_config_tx, watch_gossip_config_rx) =
        tokio::sync::watch::channel(WatchGossipConfig { trusted_pubkeys: vec![] });
    let (config_tx, config_rx) = tokio::sync::watch::channel(config.clone());
    let (gossip_config_tx, gossip_config_rx) = tokio::sync::watch::channel(gossip_config);
    let (network_config_tx, network_config_rx) = tokio::sync::watch::channel(network_config);
    tokio::spawn(dispatch_hot_reload(
        tls_cert_cache.clone(),
        shutdown_rx.clone(),
        config_rx,
        bk_set_update_async_rx.clone(),
        network_config_tx,
        gossip_config_tx,
        watch_gossip_config_tx,
    ));
    let (gossip_handle, gossip_rest_handle) =
        gossip::run(shutdown_rx, gossip_config_rx, chitchat::transport::UdpTransport).await?;

    let gossip_listen_addr_clone = config.network.gossip_listen_addr;
    let gossip_advertise_addr =
        config.network.gossip_advertise_addr.unwrap_or(gossip_listen_addr_clone);
    tracing::info!("Gossip advertise addr: {:?}", gossip_advertise_addr);

    let gossip_node = config.gossip_peer()?;
    gossip_handle
        .with_chitchat(|c| {
            gossip_node.set_to(c.self_node_state());
            c.self_node_state().set(
                node::node::services::sync::GOSSIP_API_ADVERTISE_ADDR_KEY,
                config.network.api_advertise_addr.to_string(),
            );
            if let Ok(Some(key)) = transport_layer::resolve_signing_key(
                config.network.my_ed_key_secret.clone(),
                config.network.my_ed_key_path.clone(),
            ) {
                sign_gossip_node(c.self_node_state(), key);
            }
        })
        .await;

    let network = BasicNetwork::new(shutdown_tx, network_config_rx, MsQuicTransport::default());
    let chitchat = gossip_handle.chitchat();

    let (ext_messages_sender, ext_messages_receiver) = instrumented_channel(
        metrics.as_ref().map(|x| x.node.clone()),
        node::helper::metrics::INBOUND_EXT_CHANNEL,
    );
    let (direct_tx, broadcast_tx, incoming_rx, nodes_rx) = network
        .start(
            watch_gossip_config_rx,
            metrics.as_ref().map(|m| m.net.clone()),
            metrics.as_ref().map(|m| m.node.clone()),
            config.local.node_id.clone(),
            false,
            chitchat.clone(),
        )
        .await?;

    let bp_thread_count = Arc::<AtomicI32>::default();
    let node_metrics = metrics.as_ref().map(|m| m.node.clone());
    let (raw_block_sender, raw_block_receiver) =
        instrumented_channel(node_metrics.clone(), node::helper::metrics::RAW_BLOCK_CHANNEL);

    let block_manager_listen_addr = config.network.block_manager_listen_addr;
    let nodes_rx_clone = nodes_rx.clone();
    let block_manager_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        transport_layer::server::LiteServer::new(block_manager_listen_addr)
            .start(raw_block_receiver, move |node_id: tvm_types::AccountId| {
                let node_addr = nodes_rx_clone
                    .borrow()
                    .get(&(node_id.into()))
                    .map(|x| x.peer_addr.ip().to_string());

                node_addr
            })
            .await?;
        Ok(())
    });

    if cfg!(feature = "fail-fast") {
        let orig_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            tracing::trace!("{:?}", panic_info);
            orig_hook(panic_info);
            std::process::exit(100);
        }));
    }

    let sqlite_helper_config = SqliteHelperConfig::new(
        std::env::var("SQLITE_DATA_DIR")
            .unwrap_or(sqlite_helper::SQLITE_DATA_DIR.to_string())
            .into(),
        None,
    );

    let (sqlite_helper, writer_join_handle) = if !cfg!(feature = "disable_db_write") {
        let (sqlite_helper, writer_join_handle) =
            SqliteHelper::from_config(sqlite_helper_config)
                .map_err(|e| anyhow::format_err!("Failed to create sqlite helper: {e}"))?;

        let sqlite_helper: Arc<dyn DocumentsDb> = Arc::new(sqlite_helper);
        (Some(sqlite_helper), writer_join_handle)
    } else {
        (
            None,
            std::thread::Builder::new().name("sqlite_stub".to_string()).spawn(std::thread::park)?,
        )
    };

    let zerostate_path = Some(config.local.zerostate_path.clone());

    tracing::trace!(
        "config.global.min_time_between_state_publish_directives={:?}",
        config.global.min_time_between_state_publish_directives
    );
    let keys_map = key_pairs_from_file::<GoshBLS>(&config.local.key_path);
    let bls_keys_map = Arc::new(Mutex::new(keys_map));
    let bls_keys_map_clone = bls_keys_map.clone();

    // node should sync with other nodes, but if there are
    // no alive nodes, node should wait
    // TODO: fix. single thread implementation
    // let (block_id, _) = repository.select_thread_last_finalized_block(&ThreadIdentifier::new(0))?;
    // if block_id == BlockIdentifier::default() {
    //     loop {
    //         // TODO: improve this code. Do not check length, check that all vals present.
    //
    //         if let Ok(true) =
    //             network_config.alive_nodes(false).await.map(|v| {
    //                 tracing::trace!(
    //                 "[synchronizing] Waiting for sync with other nodes: other_nodes_cnt={nodes_cnt} alive_cnt={}", v.len()
    //             );
    //                 v.len() >= nodes_cnt
    //             })
    //         {
    //             break;
    //         }
    //         sleep(Duration::from_millis(ALIVE_NODES_WAIT_TIMEOUT_MILLIS)).await;
    //     }
    // }

    let seed_map = key_pairs_from_file::<GoshBLS>(&config.local.block_keeper_seed_path);
    let secret_seed = seed_map.values().last().unwrap().clone().0;
    let block_keeper_rng = SmallRng::from_seed(secret_seed.take_as_seed());

    let config_clone = config.clone();
    // let mut node_execute_handlers = JoinSet::new();
    // TODO: check that inner_service_loop is active
    let (routing, routing_rx, _inner_service_loop, _inner_ext_messages_loop) = RoutingService::new(
        incoming_rx,
        ext_messages_receiver,
        metrics.as_ref().map(|x| x.node.clone()),
        metrics.as_ref().map(|x| x.net.clone()),
    );

    let repo_path = PathBuf::from("./data");

    let mut node_shared_services = node::node::shared_services::SharedServices::start(
        routing.clone(),
        repo_path.clone(),
        metrics.as_ref().map(|m| m.node.clone()),
        config.global.thread_load_threshold,
        config.global.thread_load_window_size,
        config.local.rate_limit_on_incoming_block_req,
        config.global.thread_count_soft_limit,
    );
    let blob_sync_service =
        blob_sync::external_fileshares_based::ExternalFileSharesBased::builder()
            .local_storage_share_base_path(config.local.external_state_share_local_base_dir.clone())
            .build()
            .start(metrics.as_ref().map(|m| m.node.clone()))
            .expect("Blob sync service start");
    let nack_set_cache = Arc::new(Mutex::new(FixedSizeHashSet::new(DEFAULT_NACK_SIZE_CACHE)));
    let block_state_repo = BlockStateRepository::new(repo_path.clone().join("blocks-states"));
    let block_id = BlockIdentifier::default();
    let state = block_state_repo.get(&block_id)?;
    let mut state_in = state.lock();
    if !state_in.is_stored() {
        state_in.set_thread_identifier(ThreadIdentifier::default())?;
        state_in.set_parent_block_identifier(block_id.clone())?;
        let first_node_id = bk_set.iter_node_ids().next().unwrap().clone();
        state_in.set_producer(first_node_id)?;
        state_in.set_block_seq_no(BlockSeqNo::default())?;
        state_in.set_block_time_ms(0)?;
        state_in.set_common_checks_passed()?;
        state_in.set_finalized()?;
        state_in.set_prefinalized()?;
        state_in.set_applied()?;
        state_in.set_signatures_verified()?;
        state_in.set_stored_zero_state()?;
        let bk_set = Arc::new(bk_set.clone());
        state_in.set_bk_set(bk_set.clone())?;
        state_in.set_descendant_bk_set(bk_set)?;
        state_in.set_future_bk_set(Arc::new(BlockKeeperSet::new()))?;
        state_in.set_descendant_future_bk_set(Arc::new(BlockKeeperSet::new()))?;
        state_in.set_block_stats(BlockStatistics::zero(15, 3))?;
        state_in.set_initial_attestations_target(AttestationsTarget {
            descendant_generations: 3,
            main_attestations_target: 0,
            fallback_attestations_target: 0,
        })?;
        state_in.set_producer_selector_data(
            ProducerSelector::builder()
                .rng_seed_block_id(BlockIdentifier::default())
                .index(0)
                .build(),
        )?;
        state_in.set_ancestor_blocks_finalization_distances(HashMap::new())?;
        state_in.set_block_height(
            BlockHeight::builder().thread_identifier(ThreadIdentifier::default()).height(0).build(),
        )?;
        state_in.add_proposed_in_round(0)?;
    }
    drop(state_in);
    let accounts_repo = AccountsRepository::new(
        repo_path.clone(),
        Some(config.local.unload_after),
        config.global.save_state_frequency,
    );
    let message_db: MessageDurableStorage =
        MessageDurableStorage::new(config.local.message_storage_path.clone())?;
    let repository_blocks = Arc::new(Mutex::new(FinalizedBlockStorage::new(
        1_usize + TryInto::<usize>::try_into(config.global.save_state_frequency * 2).unwrap(),
    )));

    let (bk_set_update_tx, bk_set_update_rx) =
        instrumented_channel(node_metrics.clone(), node::helper::metrics::BK_SET_UPDATE_CHANNEL);

    let mut repository = RepositoryImpl::new(
        repo_path.clone(),
        zerostate_path.clone(),
        config.local.state_cache_size,
        node_shared_services.clone(),
        Arc::clone(&nack_set_cache),
        true, // This option can't be turned off, need fixes
        block_state_repo.clone(),
        metrics.as_ref().map(|m| m.node.clone()),
        accounts_repo,
        message_db.clone(),
        repository_blocks,
        bk_set_update_tx.clone(),
    );
    let unprocessed_blocks = repository
        .load_saved_blocks(&block_state_repo)
        .map_err(|e| {
            tracing::trace!("load_saved_blocks error: {e:?}");
            e
        })
        .expect("Failed to init repository");

    #[cfg(feature = "deadlock-detection")]
    let deadlock_detection_handle =
        std::thread::Builder::new().name("Deadlock detection".to_string()).spawn(move || {
            tracing::trace!("Spawn deadlock detector");
            loop {
                std::thread::sleep(Duration::from_secs(10));
                let deadlocks = ::parking_lot::deadlock::check_deadlock();
                if deadlocks.is_empty() {
                    continue;
                }

                println!("{} deadlocks detected", deadlocks.len());
                for (i, threads) in deadlocks.iter().enumerate() {
                    println!("Deadlock #{i}");
                    for t in threads {
                        println!("Thread Id {:#?}", t.thread_id());
                        println!("{:#?}", t.backtrace());
                    }
                }
            }
        })?;

    let zerostate_threads: Vec<ThreadIdentifier> = zerostate.list_threads().cloned().collect();

    for thread_id in &zerostate_threads {
        let (last_finalized_id, _) =
            repository.select_thread_last_finalized_block(thread_id)?.unwrap();
        tracing::trace!(
            "init thread: thread_id={:?} last_finalized_id={:?}",
            thread_id,
            last_finalized_id
        );
        node_shared_services.exec(|services| {
            // services.dependency_tracking.init_thread(*thread_id, BlockIdentifier::default());
            // TODO: check if we have to pass all threads in set
            services.threads_tracking.init_thread(
                last_finalized_id.clone(),
                HashSet::from_iter(vec![*thread_id].into_iter()),
                &mut (&mut services.router, &mut services.load_balancing),
            );
            // TODO: the same must happen after a node sync.
            services.thread_sync.on_block_finalized(&last_finalized_id, thread_id).unwrap();
        });
    }
    let ackinackisender = AckiNackiSend::builder()
        .node_id(config.local.node_id.clone())
        .bls_keys_map(bls_keys_map.clone())
        .ack_network_direct_tx(direct_tx.clone())
        .nack_network_broadcast_tx(broadcast_tx.clone())
        .build();

    let authority = Arc::new(Mutex::new(
        Authority::builder()
        .data_dir(repo_path.join("action-locks"))
        .block_state_repository(block_state_repo.clone())
        .block_repository(repository.clone())
        .node_identifier(config.local.node_id.clone())
        .bls_keys_map(bls_keys_map.clone())
        // TODO: make it restored from disk
        .action_lock(HashMap::new())
        .network_direct_tx(direct_tx.clone())
        .block_producers(HashMap::new())
        .bp_production_count(bp_thread_count.clone())
        .build(),
    ));

    let validation_service = ValidationService::new(
        &config.local.blockchain_config_path,
        repository.clone(),
        config.clone(),
        node_shared_services.clone(),
        block_state_repo.clone(),
        ackinackisender.clone(),
        metrics.as_ref().map(|m| m.node.clone()),
        message_db.clone(),
        authority.clone(),
    )
    .expect("Failed to create validation process");

    let repository_clone = repository.clone();

    let heartbeat_thread_join_handle = {
        // This is a simple hack to allow time based triggers to work.
        // Without this hack it will go stale once it runs out of all
        // messages in the system (it will wait for repo changes forever).
        let blocks_repo = block_state_repo.clone();
        let heartbeat_rate = std::time::Duration::from_millis(300);
        std::thread::Builder::new().name("heartbeat".to_string()).spawn(move || loop {
            std::thread::sleep(heartbeat_rate);
            blocks_repo.touch();
        })?
    };
    let mut chain_pulse_bind = authority_switch::chain_pulse_monitor::bind(authority.clone());
    let chain_pulse_monitor = chain_pulse_bind.monitor();

    let node_metrics = metrics.as_ref().map(|m| m.node.clone());
    let node_metrics_clone = node_metrics.clone();
    let notifications = Arc::new(AtomicU32::new(0));
    let stop_tx_vec = Arc::new(Mutex::new(vec![]));
    let stop_tx_vec_clone = stop_tx_vec.clone();
    let stop_result_rx_vec = Arc::new(Mutex::new(vec![]));
    let stop_result_rx_vec_clone = stop_result_rx_vec.clone();
    let (routing, _inner_service_thread) = RoutingService::start(
        (routing, routing_rx),
        move |parent_block_id,
              thread_id,
              thread_receiver,
              thread_sender,
              feedback_sender,
              ext_messages_rx| {
            tracing::trace!("start node for thread: {thread_id:?}");

            let block_collection = UnfinalizedCandidateBlockCollection::new(
                unprocessed_blocks.get(thread_id).cloned().unwrap_or_default().into_iter(),
                notifications.clone(),
            );

            let mut repository = repository_clone.clone();
            // HACK!
            if parent_block_id.is_some()
                && parent_block_id.as_ref().unwrap() != &BlockIdentifier::default()
            {
                repository.init_thread(thread_id, parent_block_id.as_ref().unwrap())?;
            }
            // END OF HACK
            let producer_election_rng = {
                // Here is the problem!
                // It takes the wrong block id.
                // should take a parent block of the thread instead.
                // Yet it requires an explanation why it was done like that
                let last_block_id =
                    repository.get_latest_block_id_with_producer_group_change(thread_id)?;
                let mut seed_bytes = last_block_id.as_ref().to_vec();
                seed_bytes.extend_from_slice(thread_id.as_ref());
                let seed = calculate_hash(&seed_bytes)?;
                SmallRng::from_seed(seed)
            };
            let external_messages = ExternalMessagesThreadState::builder()
                .with_thread_id(*thread_id)
                .with_report_metrics(node_metrics.clone())
                .with_cache_size(config.local.ext_messages_cache_size)
                .build()?;

            let external_messages_clone = external_messages.clone();
            let ext_msg_receiver = std::thread::Builder::new()
                .name("Ext message receiver".to_string())
                .spawn_critical(move || {
                    loop {
                        match ext_messages_rx.recv() {
                            Ok(message) => {
                                external_messages_clone.push_external_messages(&[message])?;
                            }
                            Err(e) => {
                                tracing::error!("Ext message receiver received an error: {e:?}");
                                break;
                            }
                        }
                    }
                    Ok(())
                })?;

            let (db_sender, db_service) = if let Some(sqlite_helper) = sqlite_helper.clone() {
                let (db_sender, db_receiver) = std::sync::mpsc::channel();
                (Some(db_sender), Some(DBSerializeService::new(sqlite_helper, db_receiver)?))
            } else {
                (None, None)
            };

            let file_saving_service = FileSavingService::builder()
                .root_path(config.local.external_state_share_local_base_dir.clone())
                .repository(repository.clone())
                .block_state_repository(block_state_repo.clone())
                .shared_services(node_shared_services.clone())
                .message_db(message_db.clone())
                .build();

            let mut sync_state_service = ExternalFileSharesBased::new(
                blob_sync_service.interface(),
                file_saving_service,
                chitchat.clone(),
            );
            sync_state_service.static_storages = config.network.static_storages.clone();
            sync_state_service.max_download_tries = config.network.shared_state_max_download_tries;
            sync_state_service.retry_download_timeout = std::time::Duration::from_millis(
                config.network.shared_state_retry_download_timeout_millis,
            );
            sync_state_service.download_deadline_timeout = config.global.node_joining_timeout;
            let production_process = TVMBlockProducerProcess::builder()
                .metrics(node_metrics.clone())
                .node_config(config.clone())
                .repository(repository.clone())
                .block_keeper_epoch_code_hash(config.global.block_keeper_epoch_code_hash.clone())
                .block_keeper_preepoch_code_hash(
                    config.global.block_keeper_preepoch_code_hash.clone(),
                )
                .producer_node_id(config.local.node_id.clone())
                .blockchain_config(Arc::new(load_blockchain_config(
                    &config.local.blockchain_config_path,
                )?))
                .parallelization_level(config.local.parallelization_level)
                .archive_sender(db_sender.clone())
                .shared_services(node_shared_services.clone())
                .block_produce_timeout(Arc::new(Mutex::new(Duration::from_millis(
                    config.global.time_to_produce_block_millis,
                ))))
                .thread_count_soft_limit(config.global.thread_count_soft_limit)
                .share_service(Some(sync_state_service.clone()))
                .build();

            let attestation_sender_service = AttestationSendService::builder()
                .pulse_timeout(std::time::Duration::from_millis(
                    config.global.time_to_produce_block_millis,
                ))
                .resend_attestation_timeout(config.global.attestation_resend_timeout)
                .node_id(config.local.node_id.clone())
                .thread_id(*thread_id)
                .bls_keys_map(bls_keys_map.clone())
                .block_state_repository(block_state_repo.clone())
                .network_direct_tx(direct_tx.clone())
                .metrics(node_metrics.clone())
                .authority(authority.clone())
                .build();
            let last_block_attestations = Arc::new(Mutex::new(CollectedAttestations::default()));

            let skipped_attestation_ids = Arc::new(Mutex::new(HashSet::new()));
            let block_gap = Arc::new(AtomicU32::new(0));
            let attestation_send_service = AttestationSendServiceHandler::new(
                attestation_sender_service,
                repository.clone(),
                last_block_attestations.clone(),
                block_state_repo.clone(),
                block_collection.clone(),
            );
            let chain_pulse_monitor = chain_pulse_monitor.clone();
            chain_pulse_monitor
                .send(ChainPulseEvent::start_thread(*thread_id, block_collection.clone()))
                .unwrap();
            chain_pulse_monitor.send(ChainPulseEvent::block_finalized(*thread_id)).unwrap();
            let block_processing_service = BlockProcessorService::new(
                SecurityGuarantee::from_chance_of_successful_attack(
                    config.global.chance_of_successful_attack,
                ),
                config.local.node_id.clone(),
                std::time::Duration::from_millis(config.global.time_to_produce_block_millis),
                config.global.save_state_frequency,
                bls_keys_map.clone(),
                *thread_id,
                block_state_repo.clone(),
                repository.clone(),
                node_shared_services.clone(),
                nack_set_cache.clone(),
                direct_tx.clone(),
                broadcast_tx.clone(),
                db_sender,
                skipped_attestation_ids.clone(),
                block_gap.clone(),
                validation_service.interface(),
                sync_state_service.clone(),
                ackinackisender.clone(),
                chain_pulse_monitor,
                block_collection.clone(),
            );

            // TODO: save blk_req_join_handle
            let (blk_req_tx, _blk_req_join_handle) = BlockRequestService::start(
                config.clone(),
                node_shared_services.clone(),
                repository.clone(),
                block_state_repo.clone(),
                direct_tx.clone(),
                node_metrics.clone(),
                block_collection.clone(),
            )?;

            let (stop_tx, stop_rx) = std::sync::mpsc::channel();
            {
                stop_tx_vec_clone.lock().push(stop_tx);
            }
            let (stop_result_tx, stop_result_rx) = std::sync::mpsc::channel();
            {
                stop_result_rx_vec_clone.lock().push(stop_result_rx);
            }
            let node = Node::new(
                node_shared_services.clone(),
                sync_state_service,
                production_process,
                repository.clone(),
                thread_receiver,
                broadcast_tx.clone(),
                direct_tx.clone(),
                raw_block_sender.clone(),
                bls_keys_map.clone(),
                config.clone(),
                block_keeper_rng.clone(),
                producer_election_rng.clone(),
                *thread_id,
                feedback_sender,
                parent_block_id.is_some(),
                block_state_repo.clone(),
                block_processing_service,
                // attestations_target_service:
                AttestationsTargetService::builder()
                    .repository(repository.clone())
                    .block_state_repository(block_state_repo.clone())
                    .build(),
                validation_service.interface(),
                skipped_attestation_ids,
                // block_gap,
                node_metrics_clone.clone(),
                thread_sender,
                external_messages,
                message_db.clone(),
                last_block_attestations,
                bp_thread_count.clone(),
                db_service,
                // Channel (sender) for block requests
                blk_req_tx.clone(),
                attestation_send_service,
                ext_msg_receiver,
                authority.clone(),
                block_collection.clone(),
                stop_rx,
                stop_result_tx,
            );

            Ok(node)
            // let thread_id_clone = *thread_id;
            // node_execute_handlers.spawn_blocking(move || (node.execute(), thread_id_clone));
        },
    );

    // TODO: need to start routing execution and track its status
    //    let router_execute_handler: JoinHandle<anyhow::Result<()>> =
    //        tokio::task::spawn_blocking(move || network_message_router.execute());
    for thread_id in zerostate_threads {
        tracing::trace!("Send start thread message for thread from zs: {thread_id:?}");
        routing.cmd_sender.send(Command::StartThread((thread_id, BlockIdentifier::default())))?;
    }

    tracing::info!("Adding routes");

    let signals_join_handle = {
        let mut signals = Signals::new([SIGHUP, SIGINT, SIGTERM])?;
        let blk_key_path = config_clone.local.key_path.clone();
        let config_path = args.config_path.clone();
        std::thread::Builder::new().name("signal handler".to_string()).spawn(move || {
            for sig in signals.forever() {
                tracing::info!("Received signal {:?}", sig);
                match sig {
                    SIGHUP => {
                        let new_key_map = key_pairs_from_file::<GoshBLS>(&blk_key_path);
                        tracing::trace!(
                            "Insert key pair, pubkeys: {:?}",
                            new_key_map.keys().collect::<Vec<_>>()
                        );
                        let mut keys_map = bls_keys_map_clone.lock();
                        *keys_map = new_key_map;
                        http_server::update_ext_message_auth_flag_from_files();
                        match load_config_from_file(&config_path) {
                            Ok(config) => {
                                config_tx.send_replace(config);
                            }
                            Err(err) => {
                                tracing::error!("Failed to load config from file: {err:?}");
                            }
                        }
                    }
                    SIGTERM | SIGINT => {
                        break;
                    }
                    _ => {}
                }
            }
        })?
    };

    std::thread::Builder::new()
        .name("BK set update handler".to_string())
        .spawn(move || {
            tracing::info!("BK set update handler started");
            let mut bk_set = initial_bk_set_update;
            while let Ok(update) = bk_set_update_rx.recv() {
                let new_bk_set = bk_set_update(
                    update.seq_no,
                    update.current.as_ref().map(|x| x.as_ref()),
                    update.future.as_ref().map(|x| x.as_ref()),
                );
                if new_bk_set != bk_set {
                    tracing::trace!("new bk set update: {:?}", new_bk_set);
                    bk_set = new_bk_set;
                    bk_set_update_async_tx.send_replace(bk_set.clone());
                }
            }
            tracing::info!("BK set update handler stopped");
        })
        .expect("Failed to spawn BK set updates handler");

    let config = config_clone;
    let repo_clone = repository.clone();

    let mut nodes_rx_clone = nodes_rx.clone();
    let http_server_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let default_thread = ThreadIdentifier::default();
        // Sync required by a bound in `salvo::Handler`
        let repo = Arc::new(Mutex::new(repo_clone));
        let repo_clone = repo.clone();

        let server = http_server::WebServer::new(
            config.network.api_addr,
            config.local.external_state_share_local_base_dir,
            ext_messages_sender,
            |msg: tvm_block::Message, thread: [u8; 34]| into_external_message(msg, thread.into()),
            {
                let repo = Arc::clone(&repo);
                let node_id = config.local.node_id.clone();
                move |thread_id| resolve_bp(thread_id.into(), &repo, &mut nodes_rx_clone, &node_id)
            },
            {
                let shard_state = match repo.lock().last_finalized_optimistic_state(&default_thread)
                {
                    Some(mut state) => state.get_shard_state().as_ref().clone(),
                    None => anyhow::bail!("Shard state not found"),
                };

                move |bm_license_addr| match load_bm_license_contract_pubkey(
                    shard_state.clone(),
                    bm_license_addr,
                ) {
                    Ok(pubkey) => pubkey,
                    Err(e) => {
                        tracing::error!("Failed to load pubkey for BMLicense: {e}");
                        None
                    }
                }
            },
            // This closure resolves account addresses to BOC
            move |account_address| {
                let repo = repo_clone.lock();

                let mut state = repo
                    .last_finalized_optimistic_state(&ThreadIdentifier::default())
                    .ok_or_else(|| anyhow::anyhow!("Shard state not found"))?;

                let acc_id = AccountId::from_string(&account_address)
                    .map_err(|_| anyhow::anyhow!("Invalid account address"))?;

                let thread_id = state
                    .get_thread_for_account(&acc_id)
                    .map_err(|_| anyhow::anyhow!("Account not found"))?;

                let shard_state = repo
                    .last_finalized_optimistic_state(&thread_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!("Shard state for thread_id {thread_id} not found")
                    })?
                    .get_shard_state()
                    .clone();

                let accounts = shard_state
                    .read_accounts()
                    .map_err(|e| anyhow::anyhow!("Can't read accounts from shard state: {e}"))?;

                let mut acc = accounts
                    .account(&acc_id)
                    .map_err(|e| anyhow::anyhow!("Can't find account in shard state: {e}"))?
                    .ok_or_else(|| anyhow::anyhow!("Can't find account in shard state"))?;

                if acc.is_external() {
                    let acc_id: tvm_types::UInt256 = acc_id
                        .clone()
                        .try_into()
                        .map_err(|e| anyhow::anyhow!("Failed to convert address: {e}"))?;

                    let root = match state.cached_accounts.get(&acc_id) {
                        Some((_, acc_root)) => acc_root.clone(),
                        None => repo.accounts_repository().load_account(
                            &acc_id,
                            acc.last_trans_hash(),
                            acc.last_trans_lt(),
                        )?,
                    };
                    if root.repr_hash() != acc.account_cell().repr_hash() {
                        bail!("External account cell hash mismatch");
                    }
                    acc.set_account_cell(root);
                }

                let account = acc
                    .read_account()
                    .and_then(|acc| acc.as_struct())
                    .map_err(|e| anyhow::anyhow!("{e}"))?;

                let boc = account.write_to_bytes().map_err(|e| anyhow::anyhow!("{e}"))?;
                Ok(base64_encode(&boc))
            },
            metrics.as_ref().map(|x| x.routing.clone()),
        );
        let _ = server.run(bk_set_update_async_rx).await;
        anyhow::bail!("HTTP server supposed to work forever");
    });

    if let Ok(bind_to) = std::env::var("MESSAGE_ROUTER") {
        tracing::trace!("start message router");
        let bp_resolver =
            BPResolverImpl::new(nodes_rx.clone(), Arc::new(Mutex::new(repository.clone())));
        let config = MessageRouterConfig {
            bp_resolver: Arc::new(Mutex::new(bp_resolver)),
            license_addr: std::env::var("BM_LICENSE_ADDRESS").ok(),
            keys: std::env::var("BM_KEYS_FILE")
                .ok()
                .and_then(|path| read_keys_from_file(&path).ok()),
        };
        let _ = MessageRouter::new(bind_to, config);
    }

    let wrapped_writer_join_handle = tokio::task::spawn_blocking(move || writer_join_handle.join());
    let wrapped_signals_join_handle =
        tokio::task::spawn_blocking(move || signals_join_handle.join());
    let wrapped_heartbeat_thread_join_handle =
        tokio::task::spawn_blocking(move || heartbeat_thread_join_handle.join());

    // let wrapped_blk_req_join_handle =
    //     tokio::task::spawn_blocking(move || blk_req_join_handle.join());

    let wrapped_deadlock_detector = tokio::task::spawn_blocking(move || {
        #[cfg(feature = "deadlock-detection")]
        deadlock_detection_handle.join()?;
        #[cfg(not(feature = "deadlock-detection"))]
        std::thread::park();
        Ok::<(), Box<dyn Any + Send + 'static>>(())
    });

    let result = tokio::select! {
        res = wrapped_writer_join_handle => {
            match res {
                Ok(_) =>{
                    unreachable!("record writer thread never returns")
                }
                Err(error) => {
                    anyhow::bail!("record writer thread failed with error: {error}");
                }
            }
        },
        res = wrapped_signals_join_handle => {
             match res {
                Ok(_) => {
                    // unreachable!("sigint handler thread never returns")

                    tracing::trace!("Start shutdown");
                    for stop_tx in stop_tx_vec.lock().iter() {
                        let _ = stop_tx.send(());
                    }
                    repository.dump_state();

                    // Note: vec of rx can be locked because we don't expect new threads to start
                    // after shutdown.
                    let result_rx_vec = stop_result_rx_vec.lock();
                    for rx in result_rx_vec.iter() {
                        let _ = rx.recv();
                    }

                    Ok(())
                }
                Err(error) => {
                    anyhow::bail!("sigint handler thread failed with error: {error}");
                }
            }
        },
        res = wrapped_heartbeat_thread_join_handle => {
            match res {
                Ok(_) =>{ unreachable!("heartbeat never returns") }
                Err(error) => {
                    anyhow::bail!("heartbeat handler thread failed with error: {error}");
                }
            }
        },
        // res = wrapped_blk_req_join_handle => {
        //     match res {
        //         Ok(Ok(_)) =>{ unreachable!("Block request service never returns") }
        //         Ok(Err(error)) =>{
        //             anyhow::bail!("Block request service failed with error: {:?}", error);
        //          }
        //         Err(error) => {
        //             anyhow::bail!("Block request service thread failed with error: {error}");
        //         }
        //     }
        // },
        v = http_server_handle => {
            anyhow::bail!("http_server failed: {v:?}");
        },
        v = block_manager_handle => {
            anyhow::bail!("lite node failed: {v:?}");
        },
        v = gossip_rest_handle => {
            anyhow::bail!("gossip rest failed: {v:?}");
        },
        v = wrapped_deadlock_detector => {
            match v {
                Ok(_) => {
                    Ok(())
                }
                Err(error) => {
                    anyhow::bail!("record writer thread failed with error: {error}");
                }
            }
        },
    };

    // Note: reachable on SIGTERM
    drop(chain_pulse_bind);
    result
}

fn into_external_message(
    message: tvm_block::Message,
    thread_id: ThreadIdentifier,
) -> anyhow::Result<NetworkMessage> {
    anyhow::ensure!(!message.is_internal(), "An issue with the Message content");
    let message = WrappedMessage { message };
    Ok(NetworkMessage::ExternalMessage((message, thread_id)))
}

fn resolve_bp(
    thread_id: ThreadIdentifier,
    repo: &Mutex<RepositoryImpl>,
    nodes_rx: &mut tokio::sync::watch::Receiver<HashMap<NodeIdentifier, PeerData>>,
    node_id: &NodeIdentifier,
) -> ResolvingResult {
    let bp_map = repo.lock().get_nodes_by_threads();
    tracing::debug!(target: "http_server", "bp resolver: map={:?}", bp_map);

    let Some(Some(bp_id)) = bp_map.get(&thread_id) else {
        return ResolvingResult::new(false, vec![]);
    };

    tracing::debug!(target: "http_server", "resolver: bp_id={:?}", bp_id);

    let list = nodes_rx
        .borrow()
        .get(bp_id)
        .map(|peer| match &peer.bk_api_socket {
            Some(socket) => socket.to_string(),
            None => peer.peer_addr.to_string(),
        })
        .map_or_else(Vec::new, |addr| vec![addr]);

    ResolvingResult::new(node_id == bp_id, list)
}

fn debug_used_features() {
    eprintln!("Enabled features:");
    if cfg!(feature = "tvm_tracing") {
        eprintln!("  tvm_tracing");
    }
    if cfg!(feature = "timing") {
        eprintln!("  timing");
    }
    if cfg!(feature = "allow-dappid-thread-split") {
        eprintln!("  allow-dappid-thread-split");
    }
    if cfg!(feature = "allow-threads-merge") {
        eprintln!("  allow-threads-merge");
    }
    if std::env::var("NODE_VERBOSE").is_ok() {
        eprintln!("  NODE_VERBOSE");
    }
    if let Ok(otel_endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        eprintln!("  OTEL_EXPORTER_OTLP_ENDPOINT={otel_endpoint}");
    }
    if let Ok(otel_service_name) = std::env::var("OTEL_SERVICE_NAME") {
        eprintln!("  OTEL_SERVICE_NAME={otel_service_name}");
    }
}

async fn dispatch_hot_reload(
    tls_cert_cache: TlsCertCache,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<node::config::Config>,
    mut bk_set_rx: tokio::sync::watch::Receiver<BlockKeeperSetUpdate>,
    network_config_tx: tokio::sync::watch::Sender<NetworkConfig>,
    gossip_config_tx: tokio::sync::watch::Sender<GossipConfig>,
    watch_gossip_config_tx: tokio::sync::watch::Sender<WatchGossipConfig>,
) {
    let mut bk_set_update = bk_set_rx.borrow().clone();
    let mut config = config_rx.borrow().clone();
    tracing::trace!(
        "Hot reload initial node config: {}",
        serde_json::to_string(&config).unwrap_or_default()
    );
    tracing::trace!(
        "Hot reload initial bk_set: {}",
        serde_json::to_string(&bk_set_update).unwrap_or_default()
    );
    loop {
        match config.network_config(Some(tls_cert_cache.clone())) {
            Ok(mut network_config) => {
                network_config.credential.trusted_ed_pubkeys =
                    HashSet::<transport_layer::VerifyingKey>::from_iter(
                        bk_set_update
                            .current
                            .iter()
                            .map(|x| &x.1)
                            .chain(bk_set_update.future.iter().map(|x| &x.1))
                            .filter_map(|x| transport_layer::VerifyingKey::from_bytes(x).ok())
                            .chain(network_config.credential.trusted_ed_pubkeys.iter().cloned()),
                    )
                    .into_iter()
                    .collect::<Vec<_>>();
                watch_gossip_config_tx.send_replace(WatchGossipConfig {
                    trusted_pubkeys: network_config.credential.trusted_ed_pubkeys.clone(),
                });
                network_config_tx.send_replace(network_config);
            }
            Err(err) => {
                tracing::error!("Failed to split network config: {err}");
            }
        }
        match config_rx.borrow().gossip_config() {
            Ok(gossip_config) => {
                gossip_config_tx.send_replace(gossip_config);
            }
            Err(err) => {
                tracing::error!("Failed to split gossip config: {err}");
            }
        }
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                tracing::trace!("Hot reload: shutdown");
                break;
            },
            sender = config_rx.changed() => if sender.is_ok() {
                config = config_rx.borrow().clone();
                tracing::trace!(
                    "Hot reload changed node config: {}",
                    serde_json::to_string(&config).unwrap_or_default()
                );
            } else {
                break;
            },
            sender = bk_set_rx.changed() => if sender.is_ok() {
                bk_set_update = bk_set_rx.borrow().clone();
                tracing::trace!(
                    "Hot reload changed bk_set: {}",
                    serde_json::to_string(&bk_set_update).unwrap_or_default()
                );
            } else {
                break;
            }
        }
    }
}
