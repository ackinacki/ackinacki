use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use clap::ArgAction;
use clap::Parser;
use clap::Subcommand;
use gosh_bls_lib::bls::gen_bls_key_pair;
use gosh_bls_lib::serde_bls::BLSKeyPair;
use network::socket_addr::StringSocketAddr;
use node::config::load_config_from_file;
use node::config::save_config_to_file;
use node::node::NodeIdentifier;
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

    /// This node id
    #[arg(long, env)]
    node_id: Option<NodeIdentifier>,

    /// Blockchain config path
    #[arg(long, env)]
    blockchain_config: Option<PathBuf>,

    /// Path to file with node key pair
    #[arg(long, env)]
    keys_path: Option<String>,

    /// Node socket to listen on (QUIC UDP)
    #[arg(long, env)]
    pub bind: Option<StringSocketAddr>,

    /// Node address to advertise (QUIC UDP)
    #[arg(long, env)]
    pub node_advertise_addr: Option<StringSocketAddr>,

    /// Gossip UDP socket (e.g., 127.0.0.1:10000)
    #[arg(long, env)]
    pub gossip_listen_addr: Option<StringSocketAddr>,

    /// Gossip advertise address (e.g., hostname:port or ip:port)
    #[arg(long, env)]
    pub gossip_advertise_addr: Option<StringSocketAddr>,

    /// Gossip seed nodes addresses (e.g., hostname:port or ip:port)
    #[arg(long, env, value_delimiter = ',')]
    pub gossip_seeds: Option<Vec<StringSocketAddr>>,

    #[arg(long, env)]
    pub block_manager_listen_addr: Option<StringSocketAddr>,

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
    pub public_endpoint: Option<String>,

    #[arg(long, env)]
    pub parallelization_level: Option<usize>,

    #[arg(long, env)]
    #[arg(value_parser = parse_duration::parse)]
    pub node_joining_timeout: Option<Duration>,

    #[arg(long, env)]
    pub block_keeper_seed_path: Option<String>,

    #[arg(long)]
    pub producer_change_gap_size: Option<usize>,

    /// Number of signatures, required for block acceptance.
    #[arg(long)]
    pub min_signatures_cnt_for_acceptance: Option<usize>,
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
                        node::config::Config::default()
                    } else {
                        eprint!("Error: {e}");
                        exit(1);
                    }
                }
            };

            if let Some(node_id) = config_cmd.node_id {
                config.local.node_id = node_id;
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

            if let Some(public_endpoint) = config_cmd.public_endpoint {
                config.network.public_endpoint = Some(public_endpoint);
            }

            if let Some(parallelization_level) = config_cmd.parallelization_level {
                config.local.parallelization_level = parallelization_level;
            }

            if let Ok(code_hash) = std::fs::read_to_string(EPOCH_CODE_HASH_FILE_PATH) {
                config.global.block_keeper_epoch_code_hash =
                    code_hash.trim_start_matches("0x").to_string();
            }

            if let Some(block_keeper_seed_path) = config_cmd.block_keeper_seed_path {
                config.local.block_keeper_seed_path = block_keeper_seed_path;
            }

            if let Some(producer_change_gap_size) = config_cmd.producer_change_gap_size {
                config.global.producer_change_gap_size = producer_change_gap_size;
            }

            if let Some(min_signatures_cnt_for_acceptance) =
                config_cmd.min_signatures_cnt_for_acceptance
            {
                config.global.min_signatures_cnt_for_acceptance = min_signatures_cnt_for_acceptance;
            }

            save_config_to_file(&config, &config_cmd.config_file_path)
        }
        Commands::Bls(bls_cmd) => {
            let keypair = BLSKeyPair::from(gen_bls_key_pair()?);
            if let Some(path) = bls_cmd.path {
                keypair.save_to_file(&path)
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
