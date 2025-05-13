// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

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
use ::node::helper::init_tracing;
use ::node::helper::key_handling::key_pairs_from_file;
use ::node::message::WrappedMessage;
use ::node::node::NetworkMessage;
use ::node::node::Node;
use ::node::repository::repository_impl::FinalizedBlockStorage;
use ::node::repository::repository_impl::RepositoryImpl;
use clap::Parser;
use database::documents_db::DocumentsDb;
use database::sqlite::sqlite_helper;
use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::sqlite_helper::SqliteHelperConfig;
use http_server::ResolvingResult;
use message_router::message_router::MessageRouter;
use network::config::NetworkConfig;
use network::network::BasicNetwork;
use network::pub_sub::CertFile;
use network::pub_sub::CertStore;
use network::pub_sub::PrivateKeyFile;
use network::resolver::GossipPeer;
use network::socket_addr::ToOneSocketAddr;
use network::try_socket_addr_from_url;
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
use node::node::block_state::load_unprocessed_blocks::UnprocessedBlocksLoader;
use node::node::block_state::repository::BlockStateRepository;
use node::node::block_state::state::AttestationsTarget;
use node::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use node::node::services::attestations_target::service::AttestationsTargetService;
use node::node::services::block_processor::service::BlockProcessorService;
use node::node::services::block_processor::service::SecurityGuarantee;
use node::node::services::db_serializer::DBSerializeService;
use node::node::services::fork_resolution::service::ForkResolutionService;
use node::node::services::send_attestations::AttestationSendService;
use node::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use node::node::services::sync::ExternalFileSharesBased;
use node::node::services::validation::feedback::AckiNackiSend;
use node::node::services::validation::service::ValidationService;
use node::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use node::repository::Repository;
use node::services::blob_sync;
use node::types::bp_selector::ProducerSelector;
use node::types::calculate_hash;
use node::types::BlockIdentifier;
use node::types::BlockSeqNo;
use node::types::CollectedAttestations;
use node::types::ThreadIdentifier;
use node::utilities::guarded::Guarded;
use node::utilities::guarded::GuardedMut;
use node::utilities::FixedSizeHashSet;
use node::zerostate::ZeroState;
use parking_lot::Mutex;
use rand::prelude::SeedableRng;
use rand::prelude::SmallRng;
use signal_hook::consts::SIGINT;
use signal_hook::consts::SIGTERM;
use signal_hook::iterator::Signals;
use telemetry_utils::mpsc::instrumented_channel;
use tokio::task::JoinHandle;

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

#[allow(clippy::await_holding_lock)]
async fn execute(args: Args, metrics: Option<Metrics>) -> anyhow::Result<()> {
    tracing::info!("Starting network");
    let config = load_config_from_file(&args.config_path)?.ensure_min_cpu(MINIMUM_NUMBER_OF_CORES);
    tracing::info!("Node config: {}", serde_json::to_string_pretty(&config)?);

    let seeds = config.network.get_gossip_seeds();
    tracing::info!("Gossip seeds expanded: {:?}", seeds);

    let gossip_listen_addr_clone = config.network.gossip_listen_addr.clone();
    let gossip_advertise_addr =
        config.network.gossip_advertise_addr.clone().unwrap_or(gossip_listen_addr_clone);
    tracing::info!("Gossip advertise addr: {:?}", gossip_advertise_addr);

    let (gossip_handle, gossip_rest_handle) = gossip::run(
        config.network.gossip_listen_addr.try_to_socket_addr()?,
        gossip_advertise_addr.try_to_socket_addr()?,
        seeds,
        config.network.chitchat_cluster_id.clone(),
    )
    .await?;

    let gossip_node = GossipPeer::new(
        config.local.node_id.clone(),
        config.network.node_advertise_addr.to_socket_addr(),
        config.network.proxies.clone(),
        config.network.bm_api_socket.clone(),
        config.network.bk_api_socket.clone(),
    );
    gossip_handle
        .with_chitchat(|c| {
            gossip_node.set_to(c.self_node_state());
        })
        .await;

    tracing::info!("Loading config");
    let network_config = {
        NetworkConfig::new(
            config.network.bind.try_to_socket_addr()?,
            CertFile::try_new(&config.network.my_cert)?,
            PrivateKeyFile::try_new(&config.network.my_key)?,
            CertStore::try_new(&config.network.peer_certs)?,
            config.network.subscribe.clone(),
            config.network.proxies.clone(),
        )
    };
    tracing::info!("Loaded config");
    let network = BasicNetwork::from(network_config.clone());
    let chitchat = gossip_handle.chitchat();

    let (ext_messages_sender, ext_messages_receiver) = instrumented_channel(
        metrics.as_ref().map(|x| x.node.clone()),
        node::helper::metrics::INBOUND_EXT_CHANNEL,
    );
    let (direct_tx, broadcast_tx, incoming_rx, nodes_rx) = network
        .start(
            metrics.as_ref().map(|m| m.net.clone()),
            metrics.as_ref().map(|m| m.node.clone()),
            config.local.node_id.clone(),
            chitchat,
        )
        .await?;

    let bp_thread_count = Arc::<AtomicI32>::default();
    let node_metrics = metrics.as_ref().map(|m| m.node.clone());
    let (raw_block_sender, raw_block_receiver) =
        instrumented_channel(node_metrics.clone(), node::helper::metrics::RAW_BLOCK_CHANNEL);

    let block_manager_listen_addr =
        config.network.block_manager_listen_addr.try_to_socket_addr()?;
    let mut nodes_rx_clone = nodes_rx.clone();
    let block_manager_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        transport_layer::server::LiteServer::new(block_manager_listen_addr)
            .start(raw_block_receiver, move |node_id: tvm_types::AccountId| {
                let node_addr = nodes_rx_clone
                    .borrow_and_update()
                    .get(&(node_id.into()))
                    .and_then(|peer_data| try_socket_addr_from_url(&peer_data.peer_url))
                    .map(|addr| addr.ip().to_string());

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

    let (sqlite_helper, writer_join_handle) = SqliteHelper::from_config(sqlite_helper_config)
        .map_err(|e| anyhow::format_err!("Failed to create sqlite helper: {e}"))?;

    let sqlite_helper: Arc<dyn DocumentsDb> = Arc::new(sqlite_helper);

    let zerostate_path = Some(config.local.zerostate_path.clone());

    tracing::trace!(
        "config.global.min_time_between_state_publish_directives={:?}",
        config.global.min_time_between_state_publish_directives
    );
    let keys_map = key_pairs_from_file::<GoshBLS>(&config.local.key_path);
    let bls_keys_map = Arc::new(Mutex::new(keys_map));
    let bls_keys_map_clone = bls_keys_map.clone();

    let zerostate =
        ZeroState::load_from_file(&config.local.zerostate_path).expect("Failed to open zerostate");
    let bk_set = zerostate.get_block_keeper_set()?;

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
        state_in.set_applied()?;
        state_in.set_signatures_verified()?;
        state_in.set_stored()?;
        let bk_set = Arc::new(bk_set);
        state_in.set_bk_set(bk_set.clone())?;
        state_in.set_descendant_bk_set(bk_set)?;
        state_in.set_block_stats(BlockStatistics::zero(15, 3))?;
        state_in.set_initial_attestations_target(AttestationsTarget {
            descendant_generations: 3,
            count: 0,
        })?;
        state_in.set_producer_selector_data(
            ProducerSelector::builder()
                .rng_seed_block_id(BlockIdentifier::default())
                .index(0)
                .build(),
        )?;
    }
    drop(state_in);
    let message_db: MessageDurableStorage =
        MessageDurableStorage::new(config.local.message_storage_path.clone())?;
    let repository_blocks = Arc::new(Mutex::new(FinalizedBlockStorage::new(
        1_usize + TryInto::<usize>::try_into(config.global.save_state_frequency * 2).unwrap(),
    )));
    let block_collection = UnfinalizedCandidateBlockCollection::new(
        Vec::new().into_iter(),
        Arc::new(AtomicU32::new(0)),
    );

    let repository = RepositoryImpl::new(
        repo_path.clone(),
        zerostate_path.clone(),
        config.local.state_cache_size,
        node_shared_services.clone(),
        Arc::clone(&nack_set_cache),
        config.local.split_state,
        block_state_repo.clone(),
        metrics.as_ref().map(|m| m.node.clone()),
        message_db.clone(),
        block_collection,
        repository_blocks,
    );

    let (blk_req_tx, blk_req_join_handle) = BlockRequestService::start(
        config.clone(),
        node_shared_services.clone(),
        repository.clone(),
        block_state_repo.clone(),
        direct_tx.clone(),
        node_metrics.clone(),
    )?;

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

    let validation_service = ValidationService::new(
        &config.local.blockchain_config_path,
        repository.clone(),
        config.clone(),
        node_shared_services.clone(),
        block_state_repo.clone(),
        AckiNackiSend::builder()
            .node_id(config.local.node_id.clone())
            .bls_keys_map(bls_keys_map.clone())
            .ack_network_direct_tx(direct_tx.clone())
            .nack_network_broadcast_tx(broadcast_tx.clone())
            .build(),
        metrics.as_ref().map(|m| m.node.clone()),
        message_db.clone(),
    )
    .expect("Failed to create validation process");

    let repository_clone = repository.clone();
    // TODO: check that inner_service_thread is active
    assert!(
        !config.network.static_storages.is_empty(),
        "Must have access to state sharing services"
    );

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

    let (bk_set_updates_tx, bk_set_updates_rx) =
        instrumented_channel(node_metrics.clone(), node::helper::metrics::BK_SET_UPDATE_CHANNEL);

    let mut unprocessed_blocks = block_state_repo.load_unprocessed_blocks()?;
    let threads: Vec<ThreadIdentifier> = unprocessed_blocks.keys().cloned().collect();
    for thread_id in threads {
        let Some((_last_finalized, last_finalized_seq_no)) =
            repository.select_thread_last_finalized_block(&thread_id)?
        else {
            continue;
        };

        let unprocessed_blocks_filtered = unprocessed_blocks.get_mut(&thread_id).unwrap();
        for block_state in unprocessed_blocks_filtered.iter() {
            use node::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocksSelectError::*;
            match block_state_repo
                .select_unfinalized_ancestor_blocks(block_state, last_finalized_seq_no)
            {
                Err(InvalidatedParent(ref mut chain)) | Err(BlockSeqNoCutoff(ref mut chain)) => {
                    for block in chain {
                        block.guarded_mut(|e| e.set_invalidated())?;
                    }
                }
                Err(FailedToLoadBlockState) => {
                    panic!(
                        "Failed to load ancestor block state: {}",
                        block_state.block_identifier()
                    );
                }
                _ => {}
            }
        }
        unprocessed_blocks_filtered
            .retain(|block_state| block_state.guarded(|e| !e.is_invalidated()));
    }

    let node_metrics = metrics.as_ref().map(|m| m.node.clone());
    let node_metrics_clone = node_metrics.clone();
    let (routing, _inner_service_thread) = RoutingService::start(
        (routing, routing_rx),
        bk_set_updates_tx,
        move |parent_block_id,
              thread_id,
              thread_receiver,
              thread_sender,
              feedback_sender,
              bk_set_updates_tx| {
            tracing::trace!("start node for thread: {thread_id:?}");
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
                .with_external_messages_file_path(
                    repo_path
                        .join("external-messages")
                        .join(format!("thread-{:x}", thread_id))
                        .join("data"),
                )
                .with_block_progress_data_dir(
                    repo_path
                        .join("external-messages")
                        .join(format!("thread-{:x}", thread_id))
                        .join("progress"),
                )
                .with_thread_id(*thread_id)
                .with_report_metrics(node_metrics.clone())
                .build()?;
            external_messages
                .set_progress_to_last_known(&parent_block_id.clone().unwrap_or_default())?;

            let (db_sender, db_receiver) = std::sync::mpsc::channel();
            let db_service = DBSerializeService::new(sqlite_helper.clone(), db_receiver)?;
            let block_states = unprocessed_blocks.remove(thread_id).unwrap_or_default();
            let unprocessed_blocks = block_states.into_iter().filter_map(|state| match repository
                .get_block(state.block_identifier())
            {
                Ok(Some(envelope)) => Some((state, envelope)),
                _ => None,
            });
            let unprocessed_blocks_cache = UnfinalizedCandidateBlockCollection::new(
                unprocessed_blocks,
                block_state_repo.clone_notifications_arc(),
            );
            repository.set_unprocessed_blocks_cache(unprocessed_blocks_cache);

            let production_process = TVMBlockProducerProcess::builder()
                .metrics(node_metrics.clone())
                .node_config(config.clone())
                .repository(repository.clone())
                .block_keeper_epoch_code_hash(config.global.block_keeper_epoch_code_hash.clone())
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
                .build();

            let mut sync_state_service =
                ExternalFileSharesBased::new(blob_sync_service.interface());
            sync_state_service.static_storages = config.network.static_storages.clone();
            sync_state_service.max_download_tries = config.network.shared_state_max_download_tries;
            sync_state_service.retry_download_timeout = std::time::Duration::from_millis(
                config.network.shared_state_retry_download_timeout_millis,
            );
            sync_state_service.download_deadline_timeout = config.global.node_joining_timeout;
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
                .build();
            let last_block_attestations = Arc::new(Mutex::new(CollectedAttestations::default()));

            let skipped_attestation_ids = Arc::new(Mutex::new(HashSet::new()));
            let block_gap = Arc::new(AtomicU32::new(0));
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
                bk_set_updates_tx,
                block_gap.clone(),
                external_messages.clone(),
                last_block_attestations.clone(),
                attestation_sender_service,
                validation_service.interface(),
            );

            let fork_resolution_service = ForkResolutionService::builder()
                .thread_identifier(*thread_id)
                .block_state_repository(block_state_repo.clone())
                .repository(repository.clone())
                .build();

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
                fork_resolution_service,
                block_gap,
                node_metrics_clone.clone(),
                thread_sender,
                external_messages,
                message_db.clone(),
                last_block_attestations,
                bp_thread_count.clone(),
                db_service,
                // Channel (sender) for block requests
                blk_req_tx.clone(),
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

    let sigint_join_handle = {
        let mut signals = Signals::new([SIGINT, SIGTERM])?;
        let blk_key_path = config_clone.local.key_path.clone();
        std::thread::Builder::new().name("signal handler".to_string()).spawn(move || {
            for sig in signals.forever() {
                println!("Received signal {:?}", sig);
                eprintln!("Received signal {:?}", sig);
                match sig {
                    SIGINT => {
                        let new_key_map = key_pairs_from_file::<GoshBLS>(&blk_key_path);
                        tracing::trace!(
                            "Insert key pair, pubkeys: {:?}",
                            new_key_map.keys().collect::<Vec<_>>()
                        );
                        let mut keys_map = bls_keys_map_clone.lock();
                        *keys_map = new_key_map;
                    }
                    SIGTERM => {
                        break;
                    }
                    _ => {}
                }
            }
        })?
    };

    let config = config_clone;
    let repo_clone = repository.clone();
    let mut nodes_rx_clone = nodes_rx.clone();
    let http_server_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        // Sync required by a bound in `salvo::Handler`
        let repo = Arc::new(Mutex::new(repo_clone));
        let server = http_server::WebServer::new(
            config.network.api_addr,
            config.local.external_state_share_local_base_dir,
            ext_messages_sender,
            |msg: tvm_block::Message, thread: [u8; 34]| into_external_message(msg, thread.into()),
            move |thread_id| {
                let bp_id_for_thread_map = {
                    let guarded_repo = repo.lock();
                    guarded_repo.get_nodes_by_threads()
                };
                tracing::debug!(target: "http_server", "bp resolver: map={:?}", bp_id_for_thread_map);

                let Some(Some(bp_id)) = bp_id_for_thread_map.get(&thread_id.into()) else {
                    return ResolvingResult::new(false, vec![]);
                };
                tracing::debug!(target: "http_server", "resolver: bp_id={:?}", bp_id);
                let list = nodes_rx_clone
                    .borrow_and_update()
                    .get(bp_id)
                    .and_then(|peer_data| match &peer_data.bk_api_socket {
                        Some(bk_api_socket) => Some(bk_api_socket.to_string()),
                        _ => try_socket_addr_from_url(&peer_data.peer_url)
                            .map(|url| url.ip().to_string()),
                    })
                    .map_or_else(Vec::new, |bp| vec![bp]);

                ResolvingResult::new(config.local.node_id == *bp_id, list)
            },
            metrics.as_ref().map(|x| x.routing.clone()),
        );
        let _ = server.run(bk_set_updates_rx).await;
        anyhow::bail!("HTTP server supposed to work forever");
    });

    if let Ok(bind_to) = std::env::var("MESSAGE_ROUTER") {
        tracing::trace!("start message router");
        let bp_resolver = BPResolverImpl::new(nodes_rx.clone(), Arc::new(Mutex::new(repository)));
        let _ = MessageRouter::new(bind_to, Arc::new(Mutex::new(bp_resolver)));
    }

    let wrapped_writer_join_handle = tokio::task::spawn_blocking(move || writer_join_handle.join());
    let wrapped_sigint_join_handle = tokio::task::spawn_blocking(move || sigint_join_handle.join());
    let wrapped_heartbeat_thread_join_handle =
        tokio::task::spawn_blocking(move || heartbeat_thread_join_handle.join());

    let wrapped_blk_req_join_handle =
        tokio::task::spawn_blocking(move || blk_req_join_handle.join());

    tokio::select! {
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
        res = wrapped_sigint_join_handle => {
             match res {
                Ok(_) => {
                    // unreachable!("sigint handler thread never returns")

                    tracing::trace!("Start shutdown");
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
        res = wrapped_blk_req_join_handle => {
            match res {
                Ok(Ok(_)) =>{ unreachable!("Block request service never returns") }
                Ok(Err(error)) =>{
                    anyhow::bail!("Block request service failed with error: {:?}", error);
                 }
                Err(error) => {
                    anyhow::bail!("Block request service thread failed with error: {error}");
                }
            }
        },
        v = http_server_handle => {
            anyhow::bail!("http_server failed: {v:?}");
        },
        v = block_manager_handle => {
            anyhow::bail!("lite node failed: {v:?}");
        },
        v = gossip_rest_handle => {
            anyhow::bail!("gossip rest failed: {v:?}");
        }
    }

    // Note: reachable on SIGTERM
}

fn into_external_message(
    message: tvm_block::Message,
    thread_id: ThreadIdentifier,
) -> anyhow::Result<NetworkMessage> {
    anyhow::ensure!(!message.is_internal(), "An issue with the Message content");
    let message = WrappedMessage { message };
    Ok(NetworkMessage::ExternalMessage((message, thread_id)))
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
    if cfg!(feature = "nack-on-block-verification-fail") {
        eprintln!("  nack-on-block-verification-fail");
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
