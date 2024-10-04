// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use ::node::block::producer::process::TVMBlockProducerProcess;
use ::node::bls::GoshBLS;
use ::node::database::documents_db::DocumentsDb;
use ::node::database::sqlite_helper;
use ::node::database::sqlite_helper::SqliteHelper;
use ::node::helper::init_tracing;
use ::node::helper::key_handling::key_pair_from_file;
use ::node::message::WrappedMessage;
use ::node::node::NetworkMessage;
use ::node::node::Node;
use ::node::node::NodeIdentifier;
use ::node::repository::repository_impl::RepositoryImpl;
use clap::Parser;
use itertools::Itertools;
use message_router::message_router::MessageRouter;
use network::config::NetworkConfig;
use network::network::BasicNetwork;
use network::socket_addr::ToOneSocketAddr;
use node::block::keeper::process::TVMBlockKeeperProcess;
use node::block::WrappedUInt256;
use node::config::load_config_from_file;
use node::helper::bp_resolver::BPResolverImpl;
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
use tokio::time::sleep;

const ALIVE_NODES_WAIT_TIMEOUT_MILLIS: u64 = 100;
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

    let mut network_config =
        { NetworkConfig::new(config.network.bind.try_to_socket_addr()?, gossip_handle.chitchat()) };
    let network = BasicNetwork::from(network_config.clone());
    let (incoming_messages_sender, incoming_messages_receiver) = std::sync::mpsc::channel();
    let (broadcast_sender, single_sender) =
        network.start(incoming_messages_sender.clone(), config.network.send_buffer_size).await?;

    let (raw_block_sender, raw_block_receiver) = std::sync::mpsc::channel();

    let lite_server_listen_addr = config.network.lite_server_listen_addr.try_to_socket_addr()?;
    let litenode_handler: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        block_manager::server::LiteServer::new(lite_server_listen_addr)
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

    let production_process =
        TVMBlockProducerProcess::new(config.clone(), repository.clone(), sqlite_helper.clone())
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
        sqlite_helper,
    )
    .expect("Failed to create validation process");

    tracing::trace!(
        "config.global.min_time_between_state_publish_directives={:?}",
        config.global.min_time_between_state_publish_directives
    );
    let (pubkey, secret) = key_pair_from_file::<GoshBLS>(config.local.key_path.clone());

    let block_keeper_ring_pubkeys = ZeroState::load_from_file(&config.local.zerostate_path)
        .expect("Failed to open zerostate")
        .block_keeper_set;
    let nodes_cnt = block_keeper_ring_pubkeys.len();
    let block_keeper_ring_pubkeys = Arc::new(Mutex::new(block_keeper_ring_pubkeys));
    let block_processor =
        AttestationProcessorImpl::new(repository.clone(), block_keeper_ring_pubkeys.clone());

    // node should sync with other nodes, but if there are
    // no alive nodes, node should wait
    let (block_id, _) = repository.select_thread_last_finalized_block(&0)?;
    if block_id == WrappedUInt256::default() {
        loop {
            // TODO: improve this code. Do not check length, check that all vals present.

            if let Ok(true) =
                network_config.alive_nodes(false).await.map(|v| {
                    tracing::trace!(
                    "[synchronizing] Waiting for sync with other nodes: other_nodes_cnt={nodes_cnt} alive_cnt={}", v.len()
                );
                    v.len() >= nodes_cnt
                })
            {
                break;
            }
            sleep(Duration::from_millis(ALIVE_NODES_WAIT_TIMEOUT_MILLIS)).await;
        }
    }

    let (_, secret_seed) =
        key_pair_from_file::<GoshBLS>(config.local.block_keeper_seed_path.clone());
    let block_keeper_rng = SmallRng::from_seed(secret_seed.take_as_seed());

    let mut node = Node::new(
        sync_state_service,
        production_process,
        validation_process,
        repository,
        incoming_messages_receiver,
        broadcast_sender,
        single_sender,
        raw_block_sender,
        pubkey,
        secret,
        config.clone(),
        block_processor,
        block_keeper_ring_pubkeys,
        block_keeper_rng,
    );

    tracing::info!("Adding routes");

    let http_server_handler: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let server = http_server::WebServer::new(
            config.network.api_addr,
            config.local.external_state_share_local_base_dir,
            incoming_messages_sender,
            into_external_message,
        );
        server.run().await.await;
        anyhow::bail!("HTTP server supposed to work forever");
    });

    if let Ok(bind_to) = std::env::var("MESSAGE_ROUTER") {
        tracing::trace!("start message router");
        let bp_resolver = BPResolverImpl::new(network_config, sqlite_helper_raw);
        let _ = MessageRouter::new(bind_to, Arc::new(Mutex::new(bp_resolver)));
    }

    let node_execute_handler: JoinHandle<anyhow::Result<()>> =
        tokio::task::spawn_blocking(move || node.execute());

    tokio::select! {
        v = http_server_handler => {
            tracing::trace!("http_server failed: {v:?}");
            v??
        },
        v = node_execute_handler => {
            tracing::trace!("node failed: {v:?}");
            v??
        },
        v = litenode_handler => {
            tracing::trace!("lite node failed: {v:?}");
            v??
        },
    };

    Ok(())
}

fn into_external_message<
    BLS,
    TBlock,
    TAck,
    TNack,
    TAttestation,
    TBlockIdentifier,
    TBlockSeqNo,
    TNodeIdentifier,
>(
    message: tvm_block::Message,
) -> anyhow::Result<
    NetworkMessage<
        BLS,
        TBlock,
        TAck,
        TNack,
        TAttestation,
        WrappedMessage,
        TBlockIdentifier,
        TBlockSeqNo,
        TNodeIdentifier,
    >,
>
where
    BLS: node::bls::BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlock: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    anyhow::ensure!(!message.is_internal(), "An issue with the Message content");
    let message = WrappedMessage { message };
    Ok(NetworkMessage::ExternalMessage(message))
}
