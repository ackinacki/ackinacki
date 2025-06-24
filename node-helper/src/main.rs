use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clap::ArgAction;
use clap::Parser;
use clap::Subcommand;
use gosh_bls_lib::bls::gen_bls_key_pair;
use gosh_bls_lib::serde_bls::BLSKeyPair;
use network::parse_publisher_addr;
use network::try_parse_socket_addr;
use node::bls::gosh_bls::PubKey;
use node::bls::gosh_bls::Secret;
use node::bls::GoshBLS;
use node::config::load_config_from_file;
use node::config::save_config_to_file;
use node::config::GlobalConfig;
use node::config::NetworkConfig;
use node::config::NodeConfig;
use node::helper::key_handling::key_pairs_from_file;
use node::node::NodeIdentifier;
use node::types::RndSeed;
use serde_json::json;
use tvm_client::ClientConfig;
use tvm_client::ClientContext;

const EPOCH_CODE_HASH_FILE_PATH: &str = "./contracts/bksystem/BlockKeeperEpochContract.code.hash";

#[derive(Parser, Debug)]
#[command(author, about, long_about = None)]
struct Args {
    /// Subcommand
    #[command(subcommand)]
    command: Commands,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
enum Commands {
    /// Set up AckiNacki node config
    Config(Config),
    /// Generate BLS key pair
    Bls(Bls),
    GenKeys(GenKeys),
}

#[derive(Parser, Debug)]
struct Bls {
    /// Path where to store BLS key pair
    #[arg(long)]
    path: Option<PathBuf>,

    /// Flag that indicates that helper should remove old file, before generating new BLS key pair
    /// If not set, new key will be appended to file
    #[clap(short, long, action=ArgAction::SetTrue, default_value = "false", requires("path"))]
    remove_old: bool,

    /// Quiet execution. Do not print new pubkey
    #[clap(short, long, action=ArgAction::SetTrue, default_value = "false", requires("path"))]
    quiet: bool,
}

#[derive(Parser, Debug)]
struct GenKeys {
    /// Path where to store key pair
    #[arg(long)]
    path: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct Config {
    /// Path to the config file
    #[arg(short, long, required = true)]
    config_file_path: PathBuf,

    /// Create default config if config is invalid or does not exist
    #[clap(short, long, action=ArgAction::SetTrue, default_value = "false")]
    default: bool,

    /// Node id should be specified as 64-len hex string with keeper wallet address.
    #[arg(long, env)]
    node_id: Option<String>,

    /// Blockchain config path
    #[arg(long, env)]
    blockchain_config: Option<PathBuf>,

    /// Path to file with node key pair
    #[arg(long, env)]
    keys_path: Option<String>,

    /// Node socket to listen on (QUIC UDP)
    #[arg(long, env)]
    pub bind: Option<SocketAddr>,

    /// Node address to advertise (QUIC UDP)
    #[arg(long, env)]
    #[arg(value_parser = parse_node_addr)]
    pub node_advertise_addr: Option<SocketAddr>,

    /// Gossip UDP socket (e.g., 127.0.0.1:10000)
    #[arg(long, env)]
    pub gossip_listen_addr: Option<SocketAddr>,

    /// Gossip advertise address (e.g., hostname:port or ip:port)
    #[arg(long, env)]
    #[arg(value_parser = parse_gossip_addr)]
    pub gossip_advertise_addr: Option<SocketAddr>,

    /// Gossip seed nodes addresses (e.g., hostname:port or ip:port)
    #[arg(long, env)]
    #[arg(long, env, value_delimiter = ',', value_parser = parse_gossip_addr)]
    pub gossip_seeds: Option<Vec<SocketAddr>>,

    #[arg(long, env)]
    pub block_manager_listen_addr: Option<SocketAddr>,

    /// All static stores urls-bases (e.g. "https://example.com/storage/")
    #[arg(long, env, value_delimiter = ',')]
    pub static_storages: Option<Vec<url::Url>>,

    /// Socket address for SDK API
    #[arg(long, env)]
    pub api_addr: Option<String>,

    /// Path to zerostate file
    #[arg(long, env)]
    pub zerostate_path: Option<PathBuf>,

    /// Local shared path where to store files for sync.
    #[arg(long, env)]
    pub external_state_share_local_base_dir: Option<PathBuf>,

    #[arg(long, env)]
    pub network_send_buffer_size: Option<usize>,

    #[arg(long, env)]
    #[arg(value_parser = parse_duration::parse)]
    pub min_time_between_state_publish_directives: Option<Duration>,

    #[arg(long, env)]
    pub bm_api_socket: Option<SocketAddr>,

    #[arg(long, env)]
    pub bk_api_socket: Option<SocketAddr>,

    #[arg(long, env)]
    pub parallelization_level: Option<usize>,

    #[arg(long, env)]
    #[arg(value_parser = parse_duration::parse)]
    pub node_joining_timeout: Option<Duration>,

    #[arg(long, env)]
    pub block_keeper_seed_path: Option<String>,

    #[arg(long)]
    pub producer_change_gap_size: Option<usize>,

    /// Number of max tries to download shared state
    #[arg(long)]
    pub shared_state_max_download_tries: Option<u8>,

    /// Retry timeout for shared state download
    #[arg(long)]
    pub shared_state_retry_download_timeout_millis: Option<u64>,

    /// Comma separated files and directories with network TLS certificates
    #[arg(long)]
    pub network_peer_certs: Option<String>,

    /// The name of the TLS cert file used for auth.
    #[arg(long)]
    pub network_my_cert: Option<PathBuf>,

    /// The name of the TLS key file used for auth.
    #[arg(long)]
    pub network_my_key: Option<PathBuf>,

    /// Predefined subscriptions to peers.
    #[arg(long)]
    pub network_subscribe: Option<String>,

    /// Proxy list to propagate via gossip.
    #[arg(long)]
    pub network_proxies: Option<String>,

    /// Chitchat cluster id for gossip
    #[arg(long)]
    pub chitchat_cluster_id: Option<String>,

    /// Number of blocks after which the account is unloaded from shard state.
    #[arg(long)]
    pub unload_after: Option<u32>,

    /// Thread load (aggregated number of messages in a queue to start splitting a thread) threshold for split
    #[arg(long)]
    pub thread_load_threshold: Option<usize>,

    /// Thread load window size, which is used to calculate thread load
    #[arg(long)]
    pub thread_load_window_size: Option<usize>,

    /// Maximum of threads
    #[arg(long)]
    pub thread_count_soft_limit: Option<usize>,

    /// State cache size in local repository
    #[arg(long)]
    pub state_cache_size: Option<usize>,

    /// Path to the local message durable storage
    #[arg(long)]
    pub message_storage_path: Option<PathBuf>,

    /// Epoch contract code hash
    #[arg(long, env)]
    pub block_keeper_epoch_code_hash: Option<String>,
}

const DEFAULT_NODE_PORT: u16 = 8500;
const DEFAULT_GOSSIP_PORT: u16 = 10000;
fn parse_node_addr(s: &str) -> Result<SocketAddr, String> {
    try_parse_socket_addr(s, DEFAULT_NODE_PORT).map_err(|err| err.to_string())
}

fn parse_gossip_addr(s: &str) -> Result<SocketAddr, String> {
    try_parse_socket_addr(s, DEFAULT_GOSSIP_PORT).map_err(|err| err.to_string())
}

fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    match args.command {
        Commands::Config(config_cmd) => {
            let mut config = match load_config_from_file(&config_cmd.config_file_path) {
                Ok(config) => config,
                Err(e) => {
                    if config_cmd.default {
                        println!("Failed to open config, create a default one");
                        let Some(node_id) = config_cmd.node_id.clone() else {
                            eprintln!("node_id must be specified for default config");
                            exit(2);
                        };
                        let Some(cluster_id) = config_cmd.chitchat_cluster_id.clone() else {
                            eprintln!("chitchat_cluster_id must be specified for default config");
                            exit(2);
                        };
                        let Some(node_advertise_addr) = config_cmd.node_advertise_addr else {
                            eprintln!("node_advertise_addr must be specified for default config");
                            exit(2);
                        };
                        let Some(api_addr) = config_cmd.api_addr.clone() else {
                            eprintln!("api_addr must be specified for default config");
                            exit(2);
                        };
                        let local = NodeConfig::builder()
                            .node_id(NodeIdentifier::from_str(&node_id).expect("Invalid node ID"))
                            .build();
                        let network_config = NetworkConfig::builder()
                            .chitchat_cluster_id(cluster_id)
                            .node_advertise_addr(node_advertise_addr)
                            .api_addr(api_addr)
                            .build();

                        node::config::Config {
                            global: GlobalConfig::default(),
                            network: network_config,
                            local,
                        }
                    } else {
                        eprint!("Error: {e}");
                        exit(1);
                    }
                }
            };

            if let Some(node_id) = config_cmd.node_id {
                config.local.node_id = NodeIdentifier::from_str(&node_id)
                    .map_err(|err| anyhow::anyhow!("Invalid node_id [{node_id}]: {err}"))?;
            }

            if let Some(blockchain_config) = config_cmd.blockchain_config {
                config.local.blockchain_config_path = blockchain_config;
            }

            if let Some(keys_path) = config_cmd.keys_path {
                config.local.key_path = keys_path;
            }

            if let Some(zerostate_path) = config_cmd.zerostate_path {
                config.local.zerostate_path = zerostate_path;
            }

            if let Some(external_state_share_local_base_dir) =
                config_cmd.external_state_share_local_base_dir
            {
                config.local.external_state_share_local_base_dir =
                    external_state_share_local_base_dir;
            }

            if let Some(bind) = config_cmd.bind {
                config.network.bind = bind;
            }

            if let Some(node_advertise_addr) = config_cmd.node_advertise_addr {
                config.network.node_advertise_addr = node_advertise_addr;
            }

            if let Some(gossip_listen_addr) = config_cmd.gossip_listen_addr {
                config.network.gossip_listen_addr = gossip_listen_addr;
            }

            if let Some(gossip_advertise_addr) = config_cmd.gossip_advertise_addr {
                config.network.gossip_advertise_addr = Some(gossip_advertise_addr);
            }

            if let Some(gossip_seeds) = config_cmd.gossip_seeds {
                config.network.gossip_seeds = gossip_seeds;
            }

            if let Some(block_manager_listen_addr) = config_cmd.block_manager_listen_addr {
                config.network.block_manager_listen_addr = block_manager_listen_addr;
            }

            if let Some(static_storages) = config_cmd.static_storages {
                config.network.static_storages = static_storages;
            }

            if let Some(api_addr) = config_cmd.api_addr {
                config.network.api_addr = api_addr;
            }

            if let Some(network_send_buffer_size) = config_cmd.network_send_buffer_size {
                config.network.send_buffer_size = network_send_buffer_size;
            }

            if let Some(min_time_between_state_publish_directives) =
                config_cmd.min_time_between_state_publish_directives
            {
                config.global.min_time_between_state_publish_directives =
                    min_time_between_state_publish_directives;
            }

            if let Some(node_joining_timeout) = config_cmd.node_joining_timeout {
                config.global.node_joining_timeout = node_joining_timeout;
            }

            if let Some(bm_api_socket) = config_cmd.bm_api_socket {
                config.network.bm_api_socket = Some(bm_api_socket);
            }

            if let Some(bk_api_socket) = config_cmd.bk_api_socket {
                config.network.bk_api_socket = Some(bk_api_socket);
            }

            if let Some(parallelization_level) = config_cmd.parallelization_level {
                config.local.parallelization_level = parallelization_level;
            }

            if let Some(block_keeper_epoch_code_hash) = config_cmd.block_keeper_epoch_code_hash {
                config.global.block_keeper_epoch_code_hash =
                    block_keeper_epoch_code_hash.trim_start_matches("0x").to_string();
            } else if let Ok(code_hash) = std::fs::read_to_string(EPOCH_CODE_HASH_FILE_PATH) {
                config.global.block_keeper_epoch_code_hash =
                    code_hash.trim_start_matches("0x").to_string();
            }

            if let Some(block_keeper_seed_path) = config_cmd.block_keeper_seed_path {
                config.local.block_keeper_seed_path = block_keeper_seed_path;
            }

            if let Some(producer_change_gap_size) = config_cmd.producer_change_gap_size {
                config.global.producer_change_gap_size = producer_change_gap_size;
            }

            if let Some(shared_state_max_download_tries) =
                config_cmd.shared_state_max_download_tries
            {
                config.network.shared_state_max_download_tries = shared_state_max_download_tries;
            }

            if let Some(shared_state_retry_download_timeout_millis) =
                config_cmd.shared_state_retry_download_timeout_millis
            {
                config.network.shared_state_retry_download_timeout_millis =
                    shared_state_retry_download_timeout_millis;
            }

            if let Some(certs) = config_cmd.network_peer_certs {
                config.network.peer_certs = certs.split(',').map(PathBuf::from).collect();
            }

            if let Some(cert) = config_cmd.network_my_cert {
                config.network.my_cert = cert;
            }
            if let Some(key) = config_cmd.network_my_key {
                config.network.my_key = key;
            }

            if let Some(subscribe) = config_cmd.network_subscribe {
                config.network.subscribe = subscribe
                    .split(',')
                    .filter_map(
                        |x| if !x.is_empty() { Some(parse_publisher_addr(x)) } else { None },
                    )
                    .map(|x| x.map(|x| vec![x]))
                    .collect::<Result<_, _>>()?;
            }

            if let Some(proxies) = config_cmd.network_proxies {
                config.network.proxies = proxies
                    .split(',')
                    .filter_map(
                        |x| if !x.is_empty() { Some(parse_publisher_addr(x)) } else { None },
                    )
                    .collect::<Result<_, _>>()?;
            }

            if let Some(cluster_id) = config_cmd.chitchat_cluster_id {
                config.network.chitchat_cluster_id = cluster_id;
            }

            if let Some(unload_after) = config_cmd.unload_after {
                config.local.unload_after = Some(unload_after);
            }

            if let Some(thread_load_threshold) = config_cmd.thread_load_threshold {
                config.global.thread_load_threshold = thread_load_threshold;
            }

            if let Some(thread_load_window_size) = config_cmd.thread_load_window_size {
                config.global.thread_load_window_size = thread_load_window_size;
            }

            if let Some(thread_count_soft_limit) = config_cmd.thread_count_soft_limit {
                config.global.thread_count_soft_limit = thread_count_soft_limit;
            }

            if let Some(state_cache_size) = config_cmd.state_cache_size {
                config.local.state_cache_size = state_cache_size;
            }

            if let Some(message_storage_path) = config_cmd.message_storage_path {
                config.local.message_storage_path = message_storage_path;
            }

            save_config_to_file(&config, &config_cmd.config_file_path)
        }
        Commands::Bls(bls_cmd) => {
            let keypair = BLSKeyPair::from(gen_bls_key_pair()?);
            let rng_seed = RndSeed::from(gen_bls_key_pair()?.1);
            if let Some(path) = bls_cmd.path {
                if !bls_cmd.quiet {
                    let pubkey = json!({"pubkey": format!("{}", hex::encode(keypair.public))});
                    println!("{}", serde_json::to_string_pretty(&pubkey)?);
                }
                let mut bls_keys_map = if bls_cmd.remove_old {
                    let _ = std::fs::remove_file(&path);
                    HashMap::new()
                } else if std::fs::exists(&path)? {
                    key_pairs_from_file::<GoshBLS>(path.to_str().unwrap())
                } else {
                    HashMap::new()
                };
                bls_keys_map
                    .insert(PubKey::from(keypair.public), (Secret::from(keypair.secret), rng_seed));
                save_keys_map_to_file(path, bls_keys_map)
            } else {
                println!("{}", keypair.to_string()?);
                Ok(())
            }
        }
        Commands::GenKeys(gen_key_cmd) => {
            let client = Arc::new(
                ClientContext::new(ClientConfig::default())
                    .map_err(|e| anyhow::format_err!("failed to create sdk client: {}", e))?,
            );
            let key_pair = tvm_client::crypto::generate_random_sign_keys(client)
                .map_err(|e| anyhow::format_err!("failed to generate keys: {}", e))?;
            let keys_json = serde_json::to_string_pretty(&key_pair)
                .map_err(|e| anyhow::format_err!("failed to serialize the keypair: {}", e))?;
            if let Some(keys_path) = gen_key_cmd.path {
                std::fs::write(keys_path, &keys_json)
                    .map_err(|e| anyhow::format_err!("failed to create file with keys: {}", e))?;
            } else {
                println!("{}", keys_json);
            }

            Ok(())
        }
    }
}

fn save_keys_map_to_file(
    path: PathBuf,
    keys_map: HashMap<PubKey, (Secret, RndSeed)>,
) -> anyhow::Result<()> {
    let mut keys_vec = vec![];
    for (pubkey, (secret, rnd_seed)) in keys_map {
        let mut json_map = serde_json::Map::new();
        json_map.insert("public".to_string(), json!(hex::encode(pubkey.as_ref())));
        json_map.insert("secret".to_string(), json!(hex::encode(secret.take_as_seed())));
        json_map.insert("rnd".to_string(), json!(hex::encode(rnd_seed.as_ref())));
        keys_vec.push(json_map);
    }
    Ok(std::fs::write(path, serde_json::to_string_pretty(&keys_vec)?)?)
}
