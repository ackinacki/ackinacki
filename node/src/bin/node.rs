// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::num::NonZero;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use ::node::block::producer::process::TVMBlockProducerProcess;
use ::node::bls::GoshBLS;
use ::node::helper::init_tracing;
use ::node::helper::key_handling::key_pairs_from_file;
use ::node::message::WrappedMessage;
use ::node::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use ::node::node::NetworkMessage;
use ::node::node::Node;
use ::node::protocol::authority_switch::round_time::RoundTime;
use ::node::repository::optimistic_state::OptimisticState;
use ::node::repository::repository_impl::FinalizedBlockStorage;
use ::node::repository::repository_impl::RepositoryImpl;
use clap::Parser;
use ext_messages_auth::auth::AccountRequest;
use gossip::GossipConfig;
use http_server::ApiBkSet;
use http_server::ResolvingResult;
use message_router::message_router::MessageRouter;
use message_router::message_router::MessageRouterConfig;
use message_router::read_keys_from_file;
use network::config::NetworkConfig;
use network::network::BasicNetwork;
use network::network::PeerData;
use network::resolver::sign_gossip_node;
use network::resolver::WatchGossipConfig;
use node::block::producer::wasm::WasmNodeCache;
use node::block_keeper_system::BlockKeeperSet;
use node::bls::envelope::BLSSignedEnvelope;
use node::bls::envelope::Envelope;
use node::config::load_blockchain_config;
use node::config::load_config_from_file;
use node::creditconfig::abi::DAPP_CONFIG_TVC;
use node::creditconfig::dappconfig::calculate_dapp_config_address;
use node::creditconfig::dappconfig::decode_dapp_config_data;
use node::external_messages::ExternalMessagesThreadState;
use node::helper::account_boc_loader::get_account_from_shard_state;
use node::helper::bp_resolver::BPResolverImpl;
use node::helper::calc_file_hash;
use node::helper::metrics::Metrics;
use node::helper::metrics::BLOCK_STATE_SAVE_CHANNEL;
use node::helper::metrics::OPTIMISTIC_STATE_SAVE_CHANNEL;
use node::helper::shutdown_tracing;
use node::helper::start_shutdown;
use node::helper::SHUTDOWN_FLAG;
use node::multithreading::routing::service::Command;
use node::multithreading::routing::service::RoutingService;
use node::node::block_request_service::BlockRequestService;
use node::node::block_state::attestation_target_checkpoints::AncestorBlocksFinalizationCheckpoints;
use node::node::block_state::repository::BlockState;
use node::node::block_state::repository::BlockStateRepository;
use node::node::block_state::start_state_save_service;
use node::node::block_state::state::AttestationTarget;
use node::node::block_state::state::AttestationTargets;
use node::node::services::attestations_target::service::AttestationTargetsService;
use node::node::services::authority_switch::AuthoritySwitchService;
use node::node::services::block_processor::service::BlockProcessorService;
use node::node::services::block_processor::service::SecurityGuarantee;
use node::node::services::block_processor::service::MAX_ATTESTATION_TARGET_BETA;
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
use node::protocol::authority_switch::action_lock::ActionLockCollection;
use node::protocol::authority_switch::action_lock::Authority;
use node::protocol::authority_switch::find_last_prefinalized::find_last_prefinalized;
use node::repository::accounts::AccountsRepository;
use node::repository::load_saved_blocks::SavedBlocksLoader;
use node::repository::start_optimistic_state_save_service;
use node::repository::Repository;
use node::services::blob_sync;
use node::services::cross_thread_ref_data_availability_synchronization::CrossThreadRefDataAvailabilitySynchronizationService;
use node::storage::ActionLockStorage;
use node::storage::AerospikeStore;
use node::storage::CachedStore;
use node::storage::CrossRefStorage;
use node::storage::LruSizedCache;
use node::storage::MessageDurableStorage;
use node::storage::DEFAULT_AEROSPIKE_MESSAGE_CACHE_MAX_ENTRIES;
use node::types::bp_selector::ProducerSelector;
use node::types::calculate_hash;
use node::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;
use node::types::AccountAddress;
use node::types::AckiNackiBlock;
use node::types::BlockHeight;
use node::types::BlockIdentifier;
use node::types::BlockSeqNo;
use node::types::CollectedAttestations;
use node::types::DAppIdentifier;
use node::types::ThreadIdentifier;
use node::utilities::guarded::Guarded;
use node::utilities::guarded::GuardedMut;
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
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_block::StateInit;
use tvm_types::base64_encode;
use tvm_types::UInt256;

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
    #[arg(long)]
    #[arg(default_value = "true")]
    clear_missing_block_locks: bool,
    #[arg(long)]
    #[arg(default_value = "true")]
    check_zerostate_validity: bool,
}

#[cfg(feature = "rayon_affinity")]
fn init_rayon() {
    let cores = core_affinity::get_core_ids().expect("Unable to get core IDs");
    let custom_stack_size = 16 * 1024 * 1024;
    // Build Rayon thread pool and set affinity for each thread
    rayon::ThreadPoolBuilder::new()
        .num_threads(cores.len())
        .thread_name(|thread_index| format!("rayon{thread_index}"))
        .stack_size(custom_stack_size)
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
    let (metrics, tracing_guard) = init_tracing();
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
    shutdown_tracing(tracing_guard);
    exit(exit_code);
}

fn verify_zerostate(zs: &ZeroState, message_db: &MessageDurableStorage) -> anyhow::Result<()> {
    tracing::trace!("Verifying ZeroState");
    let mut messages = HashSet::new();
    for state in zs.states().values() {
        let messages_queue = state.messages().clone();
        let acc_iter = messages_queue.iter(message_db);
        for account_messages in acc_iter {
            for msg in account_messages {
                let (message, _) =
                    msg.map_err(|e| anyhow::format_err!("Failed to unpack message: {e:?}"))?;
                let tvm_message = message.message.clone();
                if !messages.insert(
                    tvm_message
                        .hash()
                        .map_err(|e| anyhow::format_err!("Failed to calc message hash: {e}"))?,
                ) {
                    let hash = tvm_message.hash().map(|x| x.to_hex_string()).unwrap_or_default();
                    let src =
                        tvm_message.src().map(|x| x.address().to_hex_string()).unwrap_or_default();
                    let dst =
                        tvm_message.dst().map(|x| x.address().to_hex_string()).unwrap_or_default();
                    return Err(anyhow::format_err!(
                        "Duplicated message in zerostate: {hash}, src: {src}, dst: {dst}"
                    ));
                }
            }
        }
    }

    let base_config_stateinit = StateInit::construct_from_bytes(DAPP_CONFIG_TVC)
        .map_err(|e| anyhow::format_err!("Failed to construct DAPP config tvc: {e}"))?;

    let addr = calculate_dapp_config_address(
        DAppIdentifier(AccountAddress(UInt256::ZERO)),
        base_config_stateinit.clone(),
    )
    .map_err(|e| anyhow::format_err!("Failed to calculate DAPP config address: {e}"))?;
    let account_addr = AccountAddress(addr);
    tracing::trace!("zero config address: {:?}", account_addr);
    let zero_thread_state = zs.state(&ThreadIdentifier::default())?;
    let accounts = zero_thread_state
        .get_shard_state()
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read accounts: {e}"))?;

    match accounts.account(&account_addr.clone().into()) {
        Ok(Some(acc)) => {
            tracing::trace!(target: "builder", "account found");
            let acc_d = acc
                .read_account()
                .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?
                .as_struct()
                .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?;
            let data = decode_dapp_config_data(&acc_d)?;
            match data {
                Some(configdata) if configdata.is_unlimit => {
                    // OK - continue
                }
                Some(_) => {
                    return Err(anyhow::format_err!(
                        "Incorrect unlimit in 0 DappConfig: expected true, got false"
                    ));
                }
                None => {
                    return Err(anyhow::format_err!(
                        "No config data found for DAppIdentifier::ZERO"
                    ));
                }
            }
        }
        Ok(None) => {
            return Err(anyhow::format_err!(
                "Account for DAppIdentifier::ZERO not found at address: {:?}",
                account_addr
            ));
        }
        Err(e) => {
            return Err(anyhow::format_err!("Failed to get account for DAppIdentifier::ZERO: {e}"));
        }
    }
    // Note: DAppIdentifier::ONE config is not always deployed in test networks. Skip check (Could be returned under condition)

    // let addr = calculate_dapp_config_address(
    //     DAppIdentifier(AccountAddress(UInt256::from_be_bytes(&[1]))),
    //     base_config_stateinit,
    // )
    // .map_err(|e| anyhow::format_err!("Failed to calculate DAPP config address: {e}"))?;
    // let account_addr = AccountAddress(addr);
    // tracing::trace!("DAppIdentifier::ONE config address: {:?}", account_addr);
    //
    // match accounts.account(&account_addr.clone().into()) {
    //     Ok(Some(acc)) => {
    //         tracing::trace!(target: "builder", "account found");
    //         let acc_d = acc
    //             .read_account()
    //             .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?
    //             .as_struct()
    //             .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?;
    //         let data = decode_dapp_config_data(&acc_d)?;
    //         match data {
    //             Some(configdata) if configdata.is_unlimit => {
    //                 // OK - continue
    //             }
    //             Some(_) => {
    //                 return Err(anyhow::format_err!(
    //                     "Incorrect unlimit in 1 DappConfig: expected true, got false"
    //                 ));
    //             }
    //             None => {
    //                 return Err(anyhow::format_err!(
    //                     "No config data found for DAppIdentifier::ONE"
    //                 ));
    //             }
    //         }
    //     }
    //     Ok(None) => {
    //         return Err(anyhow::format_err!(
    //             "Account for DAppIdentifier::ONE not found at address: {:?}",
    //             account_addr
    //         ));
    //     }
    //     Err(e) => {
    //         return Err(anyhow::format_err!("Failed to get account for DAppIdentifier::ONE: {e}"));
    //     }
    // }
    Ok(())
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
    tracing::info!("Clear missing block locks: {}", args.clear_missing_block_locks);

    let node_metrics = metrics.as_ref().map(|m| m.node.clone());
    let socket_address =
        std::env::var("AEROSPIKE_SOCKET_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let set_prefix = std::env::var("AEROSPIKE_SET_PREFIX").unwrap_or_else(|_| "node".to_string());

    let durable_store = AerospikeStore::new(socket_address, node_metrics.clone())?;
    let durable_set = |suffix: &str| format!("{set_prefix}-{suffix}");

    let num_cached_entries = std::env::var("AEROSPIKE_CACHE_MESSAGE_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_AEROSPIKE_MESSAGE_CACHE_MAX_ENTRIES);

    let cache = LruSizedCache::new(num_cached_entries);
    let cached_store = CachedStore::new(durable_store.clone(), cache);

    let message_db = MessageDurableStorage::new(cached_store, &durable_set("msg"));
    let crossref_db = CrossRefStorage::new(durable_store.clone(), &durable_set("ref"));
    let action_lock_db = ActionLockStorage::new(durable_store.clone(), &durable_set("lck"));

    let zerostate =
        ZeroState::load_from_file(&config.local.zerostate_path).expect("Failed to open zerostate");

    let zerostate_hash =
        calc_file_hash(&config.local.zerostate_path).expect("Failed to calculate zerostate hash");
    tracing::info!(target: "monit", "zerostate hash: {zerostate_hash}");

    if args.check_zerostate_validity {
        verify_zerostate(&zerostate, &message_db)?;
    }
    let bk_set = if let Some(bk_set_update_path) = &config.local.bk_set_update_path {
        serde_json::from_slice::<ApiBkSet>(&std::fs::read(bk_set_update_path)?)?
    } else {
        ApiBkSet { seq_no: 0, current: (&zerostate.get_block_keeper_set()?).into(), future: vec![] }
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (bk_set_update_async_tx, bk_set_update_async_rx) =
        tokio::sync::watch::channel(bk_set.clone());
    let (watch_gossip_config_tx, watch_gossip_config_rx) =
        tokio::sync::watch::channel(WatchGossipConfig {
            max_nodes_with_same_id: config.network.max_nodes_with_same_id as usize,
            trusted_pubkeys: HashSet::default(),
        });
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
            if let Ok(keys) = transport_layer::resolve_signing_keys(
                &config.network.my_ed_key_secret,
                &config.network.my_ed_key_path,
            ) {
                if let Some(key) = keys.first() {
                    sign_gossip_node(c.self_node_state(), key.clone());
                }
            }
        })
        .await;

    let network = BasicNetwork::new(shutdown_tx, network_config_rx, MsQuicTransport::default());
    let chitchat = gossip_handle.chitchat();

    let wasm_cache = WasmNodeCache::new()?;

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
            config.network.node_advertise_addr,
            false,
            chitchat.clone(),
        )
        .await?;

    let bp_thread_count = Arc::<AtomicI32>::default();
    let (raw_block_sender, raw_block_receiver) = instrumented_channel::<(NodeIdentifier, Vec<u8>)>(
        node_metrics.clone(),
        node::helper::metrics::RAW_BLOCK_CHANNEL,
    );

    let block_manager_listen_addr = config.network.block_manager_listen_addr;
    let nodes_rx_clone = nodes_rx.clone();
    let block_manager_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        transport_layer::server::LiteServer::new(block_manager_listen_addr)
            .start(raw_block_receiver, move |node_id| {
                let node_addr = nodes_rx_clone
                    .borrow()
                    .get(&node_id)
                    .map(|x| x.first().map(|x| x.peer_addr.ip().to_string()).unwrap_or_default());
                node_addr
            })
            .await?;
        Ok(())
    });

    if cfg!(feature = "fail-fast") {
        let orig_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            if let Some(location) = panic_info.location() {
                eprintln!("panic occurred in file '{}'", location.file());
            } else {
                eprintln!("panic occurred but can't get location information...");
            }
            eprintln!("{panic_info:?}");
            orig_hook(panic_info);
            std::process::exit(100);
        }));
    }

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
                        ext_messages_auth::auth::update_ext_message_auth_flag_from_files();
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

    // let mut node_execute_handlers = JoinSet::new();
    // TODO: check that inner_service_loop is active
    let (routing, routing_rx, _inner_service_loop, _inner_ext_messages_loop) = RoutingService::new(
        incoming_rx,
        ext_messages_receiver,
        metrics.as_ref().map(|x| x.node.clone()),
        metrics.as_ref().map(|x| x.net.clone()),
    );

    let repo_path = PathBuf::from("./data");
    let node_cross_thread_ref_data_availability_synchronization_service =
        CrossThreadRefDataAvailabilitySynchronizationService::new(
            metrics.as_ref().map(|m| m.node.clone()),
        )
        .unwrap();

    let mut node_shared_services = node::node::shared_services::SharedServices::start(
        routing.clone(),
        repo_path.clone(),
        metrics.as_ref().map(|m| m.node.clone()),
        config.global.thread_load_threshold,
        config.global.thread_load_window_size,
        config.local.rate_limit_on_incoming_block_req,
        config.global.thread_count_soft_limit,
        crossref_db,
    );
    let blob_sync_service =
        blob_sync::external_fileshares_based::ExternalFileSharesBased::builder()
            .local_storage_share_base_path(config.local.external_state_share_local_base_dir.clone())
            .build()
            .start(metrics.as_ref().map(|m| m.node.clone()))
            .expect("Blob sync service start");
    let nack_set_cache = Arc::new(Mutex::new(FixedSizeHashSet::new(DEFAULT_NACK_SIZE_CACHE)));
    let (state_save_tx, state_save_rx) =
        instrumented_channel(node_metrics.clone(), BLOCK_STATE_SAVE_CHANNEL);
    let state_save_service = std::thread::Builder::new()
        .name("State save service".to_string())
        .spawn_critical(move || start_state_save_service(state_save_rx))?;
    let block_state_repo =
        BlockStateRepository::new(repo_path.clone().join("blocks-states"), Arc::new(state_save_tx));

    let block_id = BlockIdentifier::default();
    let state = block_state_repo.get(&block_id)?;
    state.guarded_mut(|state_in| -> anyhow::Result<()> {
        if !state_in.is_stored() {
            state_in.set_thread_identifier(ThreadIdentifier::default())?;
            let bk_set = BlockKeeperSet::from(bk_set.current.clone());
            let first_node_id = bk_set.iter_node_ids().next().unwrap().clone();
            state_in.set_producer(first_node_id)?;
            state_in.set_block_seq_no(BlockSeqNo::default())?;
            state_in.set_block_time_ms(0)?;
            state_in.set_common_checks_passed()?;
            state_in.set_finalized()?;
            // state_in.set_prefinalized()?;
            state_in.set_ancestors(vec![])?;
            state_in.set_applied(
                Instant::now() - Duration::from_millis(330),
                Instant::now() - Duration::from_millis(330),
            )?;
            state_in.set_signatures_verified()?;
            state_in.set_stored_zero_state()?;
            let bk_set = Arc::new(bk_set.clone());
            state_in.set_bk_set(bk_set.clone())?;
            state_in.set_descendant_bk_set(bk_set)?;
            state_in.set_future_bk_set(Arc::new(BlockKeeperSet::new()))?;
            state_in.set_descendant_future_bk_set(Arc::new(BlockKeeperSet::new()))?;
            state_in.set_block_stats(BlockStatistics::zero(
                NonZero::new(15).unwrap(),
                NonZero::new(3).unwrap(),
            ))?;
            state_in.set_attestation_target(
                AttestationTargets::builder()
                    .primary(
                        AttestationTarget::builder()
                            .generation_deadline(3)
                            .required_attestation_count(0)
                            .build(),
                    )
                    .fallback(
                        AttestationTarget::builder()
                            .generation_deadline(7)
                            .required_attestation_count(0)
                            .build(),
                    )
                    .build(),
            )?;
            state_in.set_producer_selector_data(
                ProducerSelector::builder()
                    .rng_seed_block_id(BlockIdentifier::default())
                    .index(0)
                    .build(),
            )?;
            state_in.set_ancestor_blocks_finalization_checkpoints(
                AncestorBlocksFinalizationCheckpoints::builder()
                    .primary(HashMap::new())
                    .fallback(HashMap::new())
                    .build(),
            )?;
            state_in.set_block_height(
                BlockHeight::builder()
                    .thread_identifier(ThreadIdentifier::default())
                    .height(0)
                    .build(),
            )?;
            state_in.set_block_round(0)?;
        }
        Ok(())
    })?;
    drop(state);
    let accounts_repo = AccountsRepository::new(
        repo_path.clone(),
        config.local.unload_after,
        config.global.save_state_frequency,
    );
    let finalized_block_storage_size =
        1_usize + TryInto::<usize>::try_into(config.global.save_state_frequency * 2).unwrap();
    let repository_blocks =
        Arc::new(Mutex::new(FinalizedBlockStorage::new(finalized_block_storage_size)));

    let (bk_set_update_tx, bk_set_update_rx) =
        instrumented_channel(node_metrics.clone(), node::helper::metrics::BK_SET_UPDATE_CHANNEL);

    let mut repository = RepositoryImpl::new(
        repo_path.clone(),
        zerostate_path.clone(),
        config.local.state_cache_size,
        node_shared_services.clone(),
        Arc::clone(&nack_set_cache),
        config.local.unload_after.is_some(),
        block_state_repo.clone(),
        metrics.as_ref().map(|m| m.node.clone()),
        accounts_repo,
        message_db.clone(),
        repository_blocks,
        bk_set_update_tx,
    );

    let (optimistic_save_tx, optimistic_save_rx) =
        instrumented_channel(node_metrics.clone(), OPTIMISTIC_STATE_SAVE_CHANNEL);
    let repository_clone = repository.clone();
    let optimistic_state_service = std::thread::Builder::new()
        .name("Optimistic state save service".to_string())
        .spawn_critical(move || {
            start_optimistic_state_save_service(repository_clone, optimistic_save_rx)
        })?;

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

    let action_lock_collections = if args.clear_missing_block_locks {
        clear_missing_block_locks(
            &repository,
            &block_state_repo,
            &unprocessed_blocks,
            action_lock_db.clone(),
            repo_path.join("action-locks"),
        )?
    } else {
        HashMap::new()
    };

    let ackinackisender = AckiNackiSend::builder()
        .node_id(config.local.node_id.clone())
        .bls_keys_map(bls_keys_map.clone())
        .ack_network_direct_tx(direct_tx.clone())
        .nack_network_broadcast_tx(broadcast_tx.clone())
        .build();

    let authority = Arc::new(Mutex::new(
        Authority::builder()
        .round_buckets(RoundTime::linear(
            // min round time
            Duration::from_millis(config.global.round_min_time_millis),
            // step
            Duration::from_millis(config.global.round_step_millis),
            // max round time: 30 sec
            Duration::from_millis(config.global.round_max_time_millis),
        ))
        .data_dir(repo_path.join("action-locks"))
        .block_state_repository(block_state_repo.clone())
        .block_repository(repository.clone())
        .node_identifier(config.local.node_id.clone())
        .bls_keys_map(bls_keys_map.clone())
        // TODO: make it restored from disk
        // .action_lock(HashMap::new())
        .network_direct_tx(direct_tx.clone())
        // .block_producers(HashMap::new())
        .bp_production_count(bp_thread_count.clone())
        .network_broadcast_tx(broadcast_tx.clone())
        .node_joining_timeout(config.global.node_joining_timeout)
        .action_lock_db(action_lock_db)
        .max_lookback_block_height_distance(finalized_block_storage_size)
        .self_addr(config.network.node_advertise_addr)
        .action_lock_collections(action_lock_collections)
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
        wasm_cache.clone(),
        message_db.clone(),
        authority.clone(),
    )
    .expect("Failed to create validation process");

    let repository_clone = repository.clone();
    let (heartbeat_channel_tx, heartbeat_channel_rx) = std::sync::mpsc::channel();
    let heartbeat_thread_join_handle = {
        // This is a simple hack to allow time based triggers to work.
        // Without this hack it will go stale once it runs out of all
        // messages in the system (it will wait for repo changes forever).
        let mut blocks_repo = block_state_repo.clone();
        let heartbeat_rate = std::time::Duration::from_millis(300);
        std::thread::Builder::new().name("heartbeat".to_string()).spawn(move || {
            use std::sync::mpsc::TryRecvError;
            let mut attestation_notifications: Vec<Arc<Mutex<CollectedAttestations>>> = vec![];
            'outer: loop {
                std::thread::sleep(heartbeat_rate);
                blocks_repo.touch();
                for attestations in attestation_notifications.iter_mut() {
                    attestations.guarded_mut(|e| e.touch());
                }
                'inner: loop {
                    match heartbeat_channel_rx.try_recv() {
                        Ok(notifications) => attestation_notifications.push(notifications),
                        Err(TryRecvError::Empty) => break 'inner,
                        Err(TryRecvError::Disconnected) => break 'outer,
                    }
                }
            }
        })?
    };
    let mut chain_pulse_bind = authority_switch::chain_pulse_monitor::bind(authority.clone());
    let chain_pulse_monitor = chain_pulse_bind.monitor();
    let stalled_threads = chain_pulse_bind.stalled_threads();

    let node_metrics = metrics.as_ref().map(|m| m.node.clone());
    let node_metrics_clone = node_metrics.clone();
    let stop_result_rx_vec = Arc::new(Mutex::new(vec![]));
    let stop_result_rx_vec_clone = stop_result_rx_vec.clone();
    let bk_set_update_async_rx_clone = bk_set_update_async_rx.clone();
    let (routing, _inner_service_thread) = RoutingService::start(
        (routing, routing_rx),
        metrics.as_ref().map(|m| m.node.clone()),
        move |parent_block_id,
              thread_id,
              thread_receiver,
              thread_authority_receiver,
              thread_sender,
              thread_authority_sender,
              feedback_sender,
              ext_messages_rx| {
            tracing::trace!("start node for thread: {thread_id:?}");

            let block_collection = UnfinalizedCandidateBlockCollection::new(
                unprocessed_blocks.get(thread_id).cloned().unwrap_or_default().into_iter(),
            );

            let mut repository = repository_clone.clone();
            repository
                .unfinalized_blocks()
                .guarded_mut(|e| e.insert(*thread_id, block_collection.clone()));
            // HACK!
            if let Some(ref parent_block_id) = parent_block_id {
                if parent_block_id != &BlockIdentifier::default() {
                    repository.init_thread(thread_id, parent_block_id)?;
                }
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
                .with_feedback_sender(feedback_sender.clone())
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
                bk_set_update_async_rx_clone.clone(),
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
                .shared_services(node_shared_services.clone())
                .block_produce_timeout(Arc::new(Mutex::new(Duration::from_millis(
                    config.global.time_to_produce_block_millis,
                ))))
                .thread_count_soft_limit(config.global.thread_count_soft_limit)
                .share_service(Some(sync_state_service.clone()))
                .wasm_cache(wasm_cache.clone())
                .save_optimistic_service_sender(optimistic_save_tx.clone())
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
            let _ = heartbeat_channel_tx.send(Arc::clone(&last_block_attestations));

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
            match chain_pulse_monitor
                .send(ChainPulseEvent::start_thread(*thread_id, block_collection.clone()))
            {
                Ok(()) => {}
                _ => {
                    if SHUTDOWN_FLAG.get() != Some(&true) {
                        anyhow::bail!("Failed to send start thread message");
                    }
                }
            }
            let block_height = if let Some(parent_block_id) = parent_block_id.as_ref() {
                let parent_state = block_state_repo
                    .get(parent_block_id)
                    .expect("Failed to get block state of the block that has started a thread");
                let block_height = parent_state
                    .guarded(|e| *e.block_height())
                    .expect("Block that starts a thread must have a block height set");
                Some(block_height)
            } else {
                None
            };
            match chain_pulse_monitor
                .send(ChainPulseEvent::block_finalized(*thread_id, block_height))
            {
                Ok(()) => {}
                _ => {
                    if SHUTDOWN_FLAG.get() != Some(&true) {
                        anyhow::bail!("Failed to send block finalized message");
                    }
                }
            }
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
                skipped_attestation_ids.clone(),
                block_gap.clone(),
                validation_service.interface(),
                sync_state_service.clone(),
                ackinackisender.clone(),
                chain_pulse_monitor.clone(),
                block_collection.clone(),
                node_cross_thread_ref_data_availability_synchronization_service.interface(),
                optimistic_save_tx.clone(),
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

            let (stop_result_tx, stop_result_rx) = std::sync::mpsc::channel();
            {
                stop_result_rx_vec_clone.lock().push(stop_result_rx);
            }

            let self_node_tx_clone = thread_sender.clone();
            let direct_tx_clone = direct_tx.clone();
            let block_collection_clone = block_collection.clone();
            let thread_authority = authority.guarded_mut(|e| e.get_thread_authority(thread_id));
            let block_state_repo_clone = block_state_repo.clone();
            let broadcast_tx_clone = broadcast_tx.clone();
            let chain_pulse_monitor_clone = chain_pulse_monitor.clone();
            let thread_id_clone = *thread_id;
            let authority_handler = std::thread::Builder::new()
                .name(format!("AuthoritySwitchService for {thread_id_clone:?}"))
                .spawn_critical(move || {
                    let mut authority_service = AuthoritySwitchService::builder()
                        .self_addr(config.network.node_advertise_addr)
                        .rx(thread_authority_receiver)
                        .self_node_tx(self_node_tx_clone)
                        .network_direct_tx(direct_tx_clone)
                        .thread_id(thread_id_clone)
                        .unprocessed_blocks_cache(block_collection_clone)
                        .thread_authority(thread_authority)
                        .network_broadcast_tx(broadcast_tx_clone)
                        .block_state_repository(block_state_repo_clone)
                        .chain_pulse_monitor(chain_pulse_monitor_clone)
                        .sync_timeout_duration(
                            config.global.min_time_between_state_publish_directives,
                        )
                        .build();
                    authority_service.run()
                })
                .unwrap();

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
                AttestationTargetsService::builder()
                    .block_state_repository(block_state_repo.clone())
                    .build(),
                validation_service.interface(),
                skipped_attestation_ids,
                // block_gap,
                node_metrics_clone.clone(),
                thread_sender.clone(),
                external_messages,
                message_db.clone(),
                last_block_attestations,
                bp_thread_count.clone(),
                // Channel (sender) for block requests
                blk_req_tx.clone(),
                attestation_send_service,
                ext_msg_receiver,
                authority.clone(),
                block_collection.clone(),
                stop_result_tx,
                stalled_threads.clone(),
                chain_pulse_monitor.clone(),
                authority_handler,
                thread_authority_sender,
                optimistic_save_tx.clone(),
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

    let repo_clone = repository.clone();
    let repo = Arc::new(Mutex::new(repo_clone));

    let (account_request_tx, mut account_request_rx) =
        tokio::sync::mpsc::channel::<AccountRequest>(100);

    let account_request_handle = tokio::spawn(async move {
        while let Some(AccountRequest { address, response }) = account_request_rx.recv().await {
            tracing::trace!(target: "node", "incoming account ({address}) request");
            let result = get_account_from_shard_state(repo.clone(), &address)
                .map(|(acc, _dapp_id)| Some(acc));

            tracing::trace!(target: "node", "incoming account ({address}) request result: {result:?}");

            let _ = response.send(result);
        }
    });

    std::thread::Builder::new()
        .name("BK set update handler".to_string())
        .spawn(move || {
            tracing::info!("BK set update handler started");
            let mut bk_set = bk_set;
            while let Ok(update) = bk_set_update_rx.recv() {
                if bk_set.update(&ApiBkSet::from(update)) {
                    tracing::trace!("new bk set update: {:?}", bk_set);
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
        // Sync required by a bound in `salvo::Handler`
        let repo_clone_0 = Arc::new(Mutex::new(repo_clone));
        let repo_clone_1 = repo_clone_0.clone();
        let server = http_server::WebServer::new(
            config.network.api_addr,
            config.local.external_state_share_local_base_dir,
            ext_messages_sender,
            account_request_tx,
            |msg: tvm_block::Message, thread: [u8; 34]| into_external_message(msg, thread.into()),
            {
                let repo = repo_clone_0.clone();
                let node_id = config.local.node_id.clone();
                move |thread_id| resolve_bp(thread_id.into(), &repo, &mut nodes_rx_clone, &node_id)
            },
            // This closure resolves account addresses to tuple: (BOC, Option<dapp_id_as_hex_string>)
            move |address| {
                tracing::trace!(target: "http_server", "Start get_boc_by_addr");
                let result = get_account_from_shard_state(repo_clone_0.clone(), &address);
                tracing::trace!(target: "http_server", "get_boc_by_addr result: {result:?}");
                let (account, dapp_id) = result?;
                let boc = account.write_to_bytes().map_err(|e| anyhow::anyhow!("{e}"))?;
                let tuple = (
                    base64_encode(&boc), //
                    dapp_id.map(|id| id.as_hex_string()),
                );
                Ok(tuple)
            },
            // This closure returns last seq_no for default thread
            move || {
                let block_seq_no = *(repo_clone_1
                    .lock()
                    .last_finalized_optimistic_state(&ThreadIdentifier::default())
                    .ok_or_else(|| anyhow::anyhow!("Shard state not found"))?
                    .get_block_seq_no());

                Ok(block_seq_no.into())
            },
            Some(config.local.node_wallet_pubkey),
            config.local.signing_keys,
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
            owner_wallet_pubkey: None,
            signing_keys: std::env::var("BM_ISSUER_KEYS_FILE")
                .ok()
                .and_then(|path| read_keys_from_file(&path).ok()),
        };
        MessageRouter::new(bind_to, config).run();
    }

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

    let state_save_service_join_handle =
        tokio::task::spawn_blocking(move || state_save_service.join());
    let optimistic_save_service_join_handle =
        tokio::task::spawn_blocking(move || optimistic_state_service.join());

    let result = tokio::select! {
        res = wrapped_signals_join_handle => {
             match res {
                Ok(_) => {
                    // unreachable!("sigint handler thread never returns")

                    tracing::info!(target: "monit", "Start shutdown");

                    start_shutdown();

                    repository.dump_state();

                    // Note: vec of rx can be locked because we don't expect new threads to start
                    // after shutdown.
                    let result_rx_vec = stop_result_rx_vec.lock();
                    for rx in result_rx_vec.iter() {
                        let _ = rx.recv();
                    }
                    tracing::info!(target: "monit", "Shutdown finished");
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
        v = account_request_handle => {
            anyhow::bail!("AccountRequest resolver failed: {v:?}");
        },
        v = state_save_service_join_handle => {
            anyhow::bail!("State saving service failed: {v:?}");
        },
        v = optimistic_save_service_join_handle => {
            anyhow::bail!("Optimistic state saving service failed: {v:?}");
        }
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
    nodes_rx: &mut tokio::sync::watch::Receiver<HashMap<NodeIdentifier, Vec<PeerData>>>,
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
        .and_then(|peers| peers.first())
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
    if cfg!(feature = "messages_db") {
        eprintln!("  messages_db");
    }
    if cfg!(feature = "disable_db_for_messages") {
        eprintln!("  disable_db_for_messages");
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
    mut bk_set_rx: tokio::sync::watch::Receiver<ApiBkSet>,
    network_config_tx: tokio::sync::watch::Sender<NetworkConfig>,
    gossip_config_tx: tokio::sync::watch::Sender<GossipConfig>,
    watch_gossip_config_tx: tokio::sync::watch::Sender<WatchGossipConfig>,
) {
    let mut bk_set = bk_set_rx.borrow().clone();
    let mut config = config_rx.borrow().clone();
    tracing::trace!(
        "Hot reload initial node config: {}",
        serde_json::to_string(&config).unwrap_or_default()
    );
    tracing::trace!(
        "Hot reload initial bk_set: {}",
        serde_json::to_string(&bk_set).unwrap_or_default()
    );
    loop {
        match config.network_config(Some(tls_cert_cache.clone())) {
            Ok(mut network_config) => {
                network_config.credential.trusted_pubkeys =
                    HashSet::<transport_layer::VerifyingKey>::from_iter(
                        bk_set
                            .current
                            .iter()
                            .map(|x| &x.owner_pubkey.0)
                            .chain(bk_set.future.iter().map(|x| &x.owner_pubkey.0))
                            .filter_map(|x| transport_layer::VerifyingKey::from_bytes(x).ok())
                            .chain(network_config.credential.trusted_pubkeys.iter().cloned())
                            .chain(network_config.credential.my_cert_pubkeys().unwrap_or_default()),
                    );
                watch_gossip_config_tx.send_replace(WatchGossipConfig {
                    max_nodes_with_same_id: config.network.max_nodes_with_same_id as usize,
                    trusted_pubkeys: network_config.credential.trusted_pubkeys.clone(),
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
                if bk_set.update(&bk_set_rx.borrow()) {
                    tracing::trace!(
                        "Hot reload changed bk_set: {}",
                        serde_json::to_string(&bk_set).unwrap_or_default()
                    );
                }
            } else {
                break;
            }
        }
    }
}

fn clear_missing_block_locks(
    repository_impl: &RepositoryImpl,
    block_state_repository: &BlockStateRepository,
    unfinalized_blocks: &HashMap<
        ThreadIdentifier,
        Vec<(BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>,
    >,
    action_lock_db: ActionLockStorage,
    action_lock_data_dir: PathBuf,
) -> anyhow::Result<HashMap<ThreadIdentifier, ActionLockCollection>> {
    let mut result = HashMap::new();
    for (thread_id, _metadata) in repository_impl.get_all_metadata().guarded(|e| e.clone()) {
        let mut action_lock_collection = ActionLockCollection::new(
            action_lock_data_dir.join(format!("{thread_id:?}")),
            action_lock_db.clone(),
        );
        let last_prefinalized =
            find_last_prefinalized(&thread_id, repository_impl, block_state_repository)?;
        let (prefinalized_block_height, children) = last_prefinalized.guarded(|e| {
            (
                (*e.block_height()).expect("Prefinalized block must have block height set"),
                e.known_children_for_all_threads(),
            )
        });

        let mut cur_thread_heights = HashSet::new();
        let mut cursor = prefinalized_block_height;
        let check_distance = MAX_ATTESTATION_TARGET_BETA * 2;
        for _ in 0..check_distance {
            cur_thread_heights.insert(cursor);
            cursor = cursor.next(&thread_id);
        }

        let mut children = VecDeque::from_iter(children);
        for _ in 0..check_distance {
            let mut next_children = VecDeque::new();
            let mut same_thread_child_height = None;
            for child in children.iter() {
                let child_state = block_state_repository.get(child)?;
                let (block_height, all_children) = child_state
                    .guarded(|e| (*e.block_height(), e.known_children_for_all_threads()));
                if let Some(height) = block_height {
                    if prefinalized_block_height.signed_distance_to(&height).is_some() {
                        // Child is from the same thread, save height and child to check lock after
                        same_thread_child_height = Some(height);
                        next_children.extend(all_children);
                    } else {
                        // Child from the other thread, check block but not parse children
                        if let Some(child_thread_id) =
                            child_state.guarded(|e| *e.thread_identifier())
                        {
                            if !unfinalized_blocks
                                .get(&child_thread_id)
                                .map(|v| {
                                    v.iter().any(|(_, block)| block.data().identifier() == *child)
                                })
                                .unwrap_or(false)
                            {
                                tracing::trace!(
                                    "clear_missing_block_locks: remove lock for: {height:?}"
                                );
                                action_lock_collection.remove(&height);
                            }
                        }
                    }
                }
            }
            if let Some(height) = same_thread_child_height {
                let do_remove = if let Some(lock) =
                    action_lock_collection.get(&height, block_state_repository)
                {
                    tracing::trace!("clear_missing_block_locks: found lock: {lock:?}");
                    if let Some((_, locked_block_ref)) = lock.locked_block() {
                        let block_id = locked_block_ref.block_identifier();
                        !unfinalized_blocks
                            .get(&thread_id)
                            .map(|v| {
                                v.iter().any(|(_, block)| block.data().identifier() == *block_id)
                            })
                            .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                };
                if do_remove {
                    cur_thread_heights.remove(&height);
                    tracing::trace!("clear_missing_block_locks: remove lock for: {height:?}");
                    action_lock_collection.remove(&height);
                }
            }
            if next_children.is_empty() {
                break;
            }
            children = next_children;
        }

        for height in cur_thread_heights {
            if let Some(lock) = action_lock_collection.get(&height, block_state_repository) {
                tracing::trace!("clear_missing_block_locks: found lock: {lock:?}");
                if let Some((_, locked_block_ref)) = lock.locked_block() {
                    let block_id = locked_block_ref.block_identifier();
                    if !unfinalized_blocks
                        .get(&thread_id)
                        .map(|v| v.iter().any(|(_, block)| block.data().identifier() == *block_id))
                        .unwrap_or(false)
                    {
                        tracing::trace!("clear_missing_block_locks: remove lock: {lock:?}");
                        action_lock_collection.remove(&height);
                    }
                }
            }
        }

        result.insert(thread_id, action_lock_collection);
    }
    Ok(result)
}
