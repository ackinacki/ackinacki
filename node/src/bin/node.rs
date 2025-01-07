// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;

use ::node::block::producer::process::TVMBlockProducerProcess;
use ::node::bls::GoshBLS;
use ::node::helper::init_tracing;
use ::node::helper::key_handling::key_pair_from_file;
use ::node::message::WrappedMessage;
use ::node::node::NetworkMessage;
use ::node::node::Node;
use ::node::node::NodeIdentifier;
use ::node::repository::repository_impl::RepositoryImpl;
use clap::Parser;
use database::documents_db::DocumentsDb;
use database::sqlite::sqlite_helper;
use database::sqlite::sqlite_helper::SqliteHelper;
use message_router::message_router::MessageRouter;
use network::config::NetworkConfig;
use network::network::BasicNetwork;
use network::socket_addr::ToOneSocketAddr;
use node::block::keeper::process::TVMBlockKeeperProcess;
use node::config::load_config_from_file;
use node::helper::bp_resolver::BPResolverImpl;
use node::helper::shutdown_tracing;
use node::multithreading::routing::service::Command;
use node::multithreading::routing::service::RoutingService;
use node::node::attestation_processor::AttestationProcessorImpl;
use node::node::services::sync::ExternalFileSharesBased;
use node::repository::Repository;
use node::services::blob_sync;
use node::types::calculate_hash;
use node::types::BlockIdentifier;
use node::types::ThreadIdentifier;
use node::utilities::FixedSizeHashSet;
use node::zerostate::ZeroState;
use parking_lot::Mutex;
use rand::prelude::SeedableRng;
use rand::prelude::SmallRng;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
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

fn main() -> Result<(), std::io::Error> {
    eprintln!("Starting Acki-Nacki Node version: {}", *LONG_VERSION);
    if cfg!(debug_assertions) {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

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
    init_tracing();
    tracing::info!("Tracing initialized");

    let exit_code = match execute(args).await {
        Ok(_) => 0,
        Err(err) => {
            tracing::error!("{err}");
            1
        }
    };
    shutdown_tracing();
    exit(exit_code);
}

async fn execute(args: Args) -> anyhow::Result<()> {
    tracing::info!("Starting network");
    let config = load_config_from_file(&args.config_path)?.ensure_min_cpu(MINIMUM_NUMBER_OF_CORES);
    tracing::info!("Node config: {}", serde_json::to_string_pretty(&config)?);

    let seeds = config.network.get_gossip_seeds();
    tracing::info!("Gossip seeds expanded: {:?}", seeds);

    let gossip_listen_addr_clone = config.network.gossip_listen_addr.clone();
    let gossip_advertise_addr =
        config.network.gossip_advertise_addr.clone().unwrap_or(gossip_listen_addr_clone);
    tracing::info!("Gossip advertise addr: {:?}", gossip_advertise_addr);

    let gossip_handle = gossip::run(
        config.network.gossip_listen_addr.try_to_socket_addr()?,
        gossip_advertise_addr.try_to_socket_addr()?,
        seeds,
    )
    .await?;

    let node_advertise_addr = config.network.node_advertise_addr.to_socket_addr();
    let node_id = NodeIdentifier::from(config.local.node_id);
    let public_endpoint = config.network.public_endpoint.clone();

    gossip_handle
        .with_chitchat(|c| {
            c.self_node_state().set("node_advertise_addr", node_advertise_addr);
            c.self_node_state().set("node_id", node_id);
            if let Some(endpoint) = &public_endpoint {
                c.self_node_state().set("public_endpoint", endpoint);
            }
        })
        .await;

    let network_config =
        { NetworkConfig::new(config.network.bind.try_to_socket_addr()?, gossip_handle.chitchat()) };
    let network = BasicNetwork::from(network_config.clone());
    let (incoming_messages_sender, incoming_messages_receiver) = std::sync::mpsc::channel();
    let (broadcast_sender, single_sender) =
        network.start(incoming_messages_sender.clone(), config.network.send_buffer_size).await?;

    let (raw_block_sender, raw_block_receiver) = std::sync::mpsc::channel();

    let block_manager_listen_addr =
        config.network.block_manager_listen_addr.try_to_socket_addr()?;
    let block_manager_handler: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        transport_layer::server::LiteServer::new(block_manager_listen_addr)
            .start(raw_block_receiver)
            .await?;
        Ok(())
    });

    let sqlite_helper_config = json!({
        "data_dir": std::env::var("SQLITE_DATA_DIR").unwrap_or(sqlite_helper::SQLITE_DATA_DIR.into())
    })
    .to_string();
    let sqlite_helper_raw = SqliteHelper::from_config(&sqlite_helper_config)
        .map_err(|e| anyhow::format_err!("Failed to create sqlite helper: {e}"))?;
    let sqlite_helper: Option<Arc<dyn DocumentsDb>> = Some(Arc::new(sqlite_helper_raw.clone()));

    let zerostate_path = Some(config.local.zerostate_path.clone());

    tracing::trace!(
        "config.global.min_time_between_state_publish_directives={:?}",
        config.global.min_time_between_state_publish_directives
    );
    let (pubkey, secret) = key_pair_from_file::<GoshBLS>(config.local.key_path.clone());

    let zerostate =
        ZeroState::load_from_file(&config.local.zerostate_path).expect("Failed to open zerostate");
    let bk_sets = zerostate.get_block_keeper_sets()?;
    let block_keeper_sets = bk_sets;

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

    let (_, secret_seed) =
        key_pair_from_file::<GoshBLS>(config.local.block_keeper_seed_path.clone());
    let block_keeper_rng = SmallRng::from_seed(secret_seed.take_as_seed());

    let config_clone = config.clone();
    // let mut node_execute_handlers = JoinSet::new();
    // TODO: check that inner_service_loop is active
    let (routing, routing_rx, _inner_service_loop) =
        RoutingService::new(incoming_messages_receiver);
    let repo_path = PathBuf::from("./data");

    let mut node_shared_services =
        node::node::shared_services::SharedServices::start(routing.clone(), repo_path.clone());
    let blob_sync_service =
        blob_sync::external_fileshares_based::ExternalFileSharesBased::builder()
            .local_storage_share_base_path(config.local.external_state_share_local_base_dir.clone())
            .build()
            .start()
            .expect("Blob sync service start");
    let nack_set_cache = Arc::new(Mutex::new(FixedSizeHashSet::new(DEFAULT_NACK_SIZE_CACHE)));
    let repository = RepositoryImpl::new(
        repo_path.clone(),
        zerostate_path.clone(),
        (config.global.finalization_delay_to_stop * 2) as usize,
        node_shared_services.clone(),
        block_keeper_sets.clone(),
        Arc::clone(&nack_set_cache),
    );
    let zerostate_threads: Vec<ThreadIdentifier> = zerostate.list_threads().cloned().collect();

    for thread_id in &zerostate_threads {
        node_shared_services.exec(|services| {
            // services.dependency_tracking.init_thread(*thread_id, BlockIdentifier::default());
            // TODO: check if we have to pass all threads in set
            services.threads_tracking.init_thread(
                BlockIdentifier::default(),
                HashSet::from_iter(vec![*thread_id].into_iter()),
                &mut (&mut services.router, &mut services.load_balancing),
            );
            // TODO: the same must happen after a node sync.
            services
                .thread_sync
                .on_block_finalized(&BlockIdentifier::default(), thread_id)
                .unwrap();
        });
    }
    let repository_clone = repository.clone();
    // TODO: check that inner_service_thread is active
    assert!(
        !config.network.static_storages.is_empty(),
        "Must have access to state sharing services"
    );
    let (routing, _inner_service_thread) = RoutingService::start(
        (routing, routing_rx),
        move |parent_block_id, thread_id, thread_receiver| {
            let blockchain_config_path = &config.local.blockchain_config_path;
            tracing::trace!("start node for thread: {thread_id:?}");

            let mut repository = repository_clone.clone();
            // HACK!
            if parent_block_id.is_some()
                && parent_block_id.as_ref().unwrap() != &BlockIdentifier::default()
            {
                repository.init_thread(
                    thread_id,
                    parent_block_id.as_ref().unwrap(),
                    block_keeper_sets.clone(),
                    Arc::clone(&nack_set_cache),
                )?;
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
            // TODO: seems we have a problem here
            let new_blocks_from_other_threads = repository.add_thread_buffer(*thread_id);
            let production_process = TVMBlockProducerProcess::new(
                config.clone(),
                repository.clone(),
                sqlite_helper.clone(),
                new_blocks_from_other_threads,
                node_shared_services.clone(),
            )
            .expect("Failed to create production process");

            let validation_process = TVMBlockKeeperProcess::new(
                blockchain_config_path,
                repository.clone(),
                config.clone(),
                sqlite_helper.clone(),
                parent_block_id.clone(),
                *thread_id,
                node_shared_services.clone(),
                block_keeper_sets.clone(),
                Arc::clone(&nack_set_cache),
            )
            .expect("Failed to create validation process");

            let block_processor = AttestationProcessorImpl::new(
                repository.clone(),
                block_keeper_sets.clone(),
                *thread_id,
            );
            let mut sync_state_service =
                ExternalFileSharesBased::new(blob_sync_service.interface());
            sync_state_service.static_storages = config.network.static_storages.clone();
            sync_state_service.max_download_tries = config.network.shared_state_max_download_tries;
            sync_state_service.retry_download_timeout = std::time::Duration::from_millis(
                config.network.shared_state_retry_download_timeout_millis,
            );
            sync_state_service.download_deadline_timeout = config.global.node_joining_timeout;

            let node = Node::new(
                node_shared_services.clone(),
                sync_state_service,
                production_process,
                validation_process,
                repository.clone(),
                thread_receiver,
                broadcast_sender.clone(),
                single_sender.clone(),
                raw_block_sender.clone(),
                pubkey.clone(),
                secret.clone(),
                config.clone(),
                block_processor,
                block_keeper_sets.clone(),
                block_keeper_rng.clone(),
                producer_election_rng.clone(),
                *thread_id,
                parent_block_id.is_some(),
                Arc::clone(&nack_set_cache),
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
        routing.control.send(Command::StartThread((thread_id, BlockIdentifier::default())))?;
    }

    tracing::info!("Adding routes");

    // TODO: implement this function
    let get_block_fn = {
        // let repository = repository.clone();
        |id: Vec<u8>| -> anyhow::Result<Vec<u8>> {
            let mut res = Vec::from(0x_DE_AD_BE_EF_u32.to_be_bytes());
            res.extend(id);
            Ok(res)
        }

        // match repository.get_block(&BlockIdentifier::from(UInt256::from(id))) {
        //     Ok(Some(block)) => Ok(todo!()),
        //     Ok(None) => Err(anyhow::format_err!("Block not found")),
        //     Err(e) => Err(anyhow::format_err!("{e}")),
        // }
    };

    let config = config_clone;
    let http_server_handler: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let server = http_server::WebServer::new(
            config.network.api_addr,
            config.local.external_state_share_local_base_dir,
            Arc::new(get_block_fn),
            incoming_messages_sender,
            into_external_message,
        );
        server.run().await.await;
        anyhow::bail!("HTTP server supposed to work forever");
    });

    if let Ok(bind_to) = std::env::var("MESSAGE_ROUTER") {
        tracing::trace!("start message router");
        let bp_resolver = BPResolverImpl::new(network_config, repository);
        let _ = MessageRouter::new(bind_to, Arc::new(Mutex::new(bp_resolver)));
    }

    tokio::select! {
            v = http_server_handler => {
                tracing::error!("http_server failed: {v:?}");
                v??
            },
    //        v = node_execute_handlers.join_next() => {
    //            tracing::error!("node failed: {v:?}");
    //        },
            v = block_manager_handler => {
                tracing::error!("lite node failed: {v:?}");
                v??
            },
    //        v = router_execute_handler => {
    //            tracing::error!("network message router failed: {v:?}");
    //            v??
    //        },
        }

    Ok(())
}

fn into_external_message<BLS, TAck, TNack, TAttestation, TNodeIdentifier>(
    message: tvm_block::Message,
) -> anyhow::Result<NetworkMessage<BLS, TAck, TNack, TAttestation, WrappedMessage, TNodeIdentifier>>
where
    BLS: node::bls::BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    anyhow::ensure!(!message.is_internal(), "An issue with the Message content");
    let message = WrappedMessage { message };
    Ok(NetworkMessage::ExternalMessage(message))
}
