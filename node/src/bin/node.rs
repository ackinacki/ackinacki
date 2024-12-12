// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

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
use itertools::Itertools;
use message_router::message_router::MessageRouter;
use network::config::NetworkConfig;
use network::network::BasicNetwork;
use network::socket_addr::ToOneSocketAddr;
use node::block::keeper::process::TVMBlockKeeperProcess;
use node::config::load_config_from_file;
use node::helper::bp_resolver::BPResolverImpl;
use node::multithreading::node_message_router::NetworkMessageRouter;
use node::multithreading::thread_synchrinization_service::ThreadSyncService;
use node::node::attestation_processor::AttestationProcessorImpl;
use node::node::services::sync::ExternalFileSharesBased;
use node::repository::Repository;
use node::zerostate::ZeroState;
use parking_lot::Mutex;
use rand::prelude::SeedableRng;
use rand::prelude::SmallRng;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

// const ALIVE_NODES_WAIT_TIMEOUT_MILLIS: u64 = 100;
const MINIMUM_NUMBER_OF_CORES: usize = 8;

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
    // unsafe { backtrace_on_stack_overflow::enable() };
    init_tracing();

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

    if let Err(err) = execute(args).await {
        tracing::error!("{err}");
        exit(1);
    };
    exit(0);
}

async fn execute(args: Args) -> anyhow::Result<()> {
    tracing::info!("Starting network");
    let mut config = load_config_from_file(&args.config_path)?;

    let cpu_cnt = num_cpus::get();
    tracing::trace!("Number of cpu cores: {cpu_cnt}");
    assert!(
        cpu_cnt >= MINIMUM_NUMBER_OF_CORES,
        "Number of CPU cores is less than minimum: {cpu_cnt} < {MINIMUM_NUMBER_OF_CORES}"
    );
    tracing::trace!("Set parallelization level to number of cpu cores: {cpu_cnt}");
    config.local.parallelization_level = cpu_cnt;

    tracing::info!("Node config: {}", serde_json::to_string_pretty(&config)?);

    let seeds = config
        .network
        .gossip_seeds
        .iter()
        .map(|s| {
            s.try_to_socket_addr().map_err(|e| {
                tracing::error!(
                    "Failed to convert gossip seed {} to SocketAddr, skip it ({})",
                    s,
                    e
                );
                e
            })
        })
        .filter_map(|res| res.map(|socket| socket.to_string()).ok())
        .collect_vec();
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

    let blockchain_config_path = &config.local.blockchain_config_path;

    let sqlite_helper_config = json!({
        "data_dir": std::env::var("SQLITE_DATA_DIR").unwrap_or(sqlite_helper::SQLITE_DATA_DIR.into())
    })
    .to_string();
    let sqlite_helper_raw = SqliteHelper::from_config(&sqlite_helper_config)
        .map_err(|e| anyhow::format_err!("Failed to create sqlite helper: {e}"))?;
    let sqlite_helper: Option<Arc<dyn DocumentsDb>> = Some(Arc::new(sqlite_helper_raw.clone()));

    let zerostate_path = Some(config.local.zerostate_path.clone());

    let repository = RepositoryImpl::new(
        PathBuf::from("./data"),
        zerostate_path,
        (config.global.finalization_delay_to_stop * 2) as usize,
    );

    tracing::trace!(
        "config.global.min_time_between_state_publish_directives={:?}",
        config.global.min_time_between_state_publish_directives
    );
    let (pubkey, secret) = key_pair_from_file::<GoshBLS>(config.local.key_path.clone());

    let zerostate =
        ZeroState::load_from_file(&config.local.zerostate_path).expect("Failed to open zerostate");
    let bk_sets = zerostate.get_block_keeper_sets()?;
    let block_keeper_sets = Arc::new(Mutex::new(bk_sets));

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

    let (thread_sync_tx, thread_sync_rx) = std::sync::mpsc::channel();
    let mut thread_sync_router =
        ThreadSyncService::builder().common_receiver(thread_sync_rx).build();

    let mut network_message_router = NetworkMessageRouter::new(incoming_messages_receiver);
    let mut node_execute_handlers = JoinSet::new();

    for thread_id in zerostate.get_threads_table().list_threads() {
        tracing::trace!("start node for thread: {thread_id:?}");
        let thread_receiver = network_message_router.add_sender(*thread_id);
        let thread_sync_buffer = thread_sync_router.add_thread(*thread_id);
        let last_block_id = repository.get_latest_block_id_with_producer_group_change(
            &zerostate.state(thread_id)?.thread_id,
        )?;
        let seed_bytes: [u8; 32] = last_block_id.as_ref().try_into()?;
        let producer_election_rng = SmallRng::from_seed(seed_bytes);

        let production_process = TVMBlockProducerProcess::new(
            config.clone(),
            repository.clone(),
            sqlite_helper.clone(),
            thread_sync_buffer,
        )
        .expect("Failed to create production process");

        let sync_state_service = ExternalFileSharesBased::builder()
            .local_storage_share_base_path(config.local.external_state_share_local_base_dir.clone())
            .static_storages(config.network.static_storages.clone())
            .timeout(config.global.node_joining_timeout)
            .build();
        let validation_process = TVMBlockKeeperProcess::new(
            blockchain_config_path,
            repository.clone(),
            config.clone(),
            sqlite_helper.clone(),
            *thread_id,
            thread_sync_tx.clone(),
            zerostate.state(thread_id)?.threads_table.clone(),
        )
        .expect("Failed to create validation process");

        let block_processor = AttestationProcessorImpl::new(
            repository.clone(),
            block_keeper_sets.clone(),
            *thread_id,
        );

        let mut node = Node::new(
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
            zerostate.state(thread_id)?.threads_table.clone(),
            *thread_id,
        );
        let thread_id_clone = *thread_id;
        node_execute_handlers.spawn_blocking(move || (node.execute(), thread_id_clone));
    }

    let thread_sync_router_handler: JoinHandle<anyhow::Result<()>> =
        tokio::task::spawn_blocking(move || thread_sync_router.execute());

    let router_execute_handler: JoinHandle<anyhow::Result<()>> =
        tokio::task::spawn_blocking(move || network_message_router.execute());

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
        v = node_execute_handlers.join_next() => {
            tracing::error!("node failed: {v:?}");
        },
        v = block_manager_handler => {
            tracing::error!("lite node failed: {v:?}");
            v??
        },
        v = router_execute_handler => {
            tracing::error!("network message router failed: {v:?}");
            v??
        },
        v = thread_sync_router_handler => {
            tracing::error!("thread sync router failed: {v:?}");
            v??
        }
    };

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
