// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
mod blockchain_config;
mod network_config;
mod serde_config;
#[cfg(test)]
mod test;
mod validations;
use std::path::PathBuf;
use std::time::Duration;

pub use blockchain_config::*;
use network::pub_sub::CertFile;
use network::pub_sub::CertStore;
use network::pub_sub::PrivateKeyFile;
use network::resolver::GossipPeer;
pub use network_config::NetworkConfig;
use serde::Deserialize;
use serde::Serialize;
pub use serde_config::load_config_from_file;
pub use serde_config::save_config_to_file;
use transport_layer::TlsCertCache;
use typed_builder::TypedBuilder;

use crate::node::NodeIdentifier;
use crate::types::BlockSeqNo;

// TODO: These settings should be moved onchain.
/// Global node config, including block producer and synchronization settings.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GlobalConfig {
    /// Duration of one iteration of producing cycle in milliseconds.
    /// Defaults to 330
    pub time_to_produce_block_millis: u64,

    /// Maximum verification duration for one block.
    /// Defaults to 440 (330 * 4 / 3)
    pub time_to_verify_block_millis: u64,

    /// Maximum execution duration of one transaction production in milliseconds.
    /// Defaults to None
    pub time_to_produce_transaction_millis: Option<u64>,

    /// Maximum execution duration of one transaction verification in milliseconds.
    /// Defaults to None
    pub time_to_verify_transaction_millis: Option<u64>,

    /// Maximum execution duration of one transaction verification in milliseconds.
    /// Applied to transactions that was aborted with ExceptionCode::ExecutionTimeout.
    /// Defaults to Some(time_to_produce_transaction_millis * 0.9) is set in ensure_execution_timeouts
    pub time_to_verify_transaction_aborted_with_execution_timeout_millis: Option<u64>,

    /// Timeout between attestation resend.
    pub attestation_resend_timeout: Duration,

    /// Difference between the seq no of the incoming block and the seq no of
    /// the last saved block, which causes the node synchronization process
    /// to start. Defaults to 20
    pub need_synchronization_block_diff: <BlockSeqNo as std::ops::Sub>::Output,

    /// Minimal time between publishing state.
    /// Defaults to 600 seconds
    pub min_time_between_state_publish_directives: Duration,

    /// Block gap size that causes block producer rotation.
    /// Defaults to 6
    pub producer_change_gap_size: usize,

    /// Timeout between consecutive NodeJoining messages sending.
    /// Defaults to 60 seconds
    pub node_joining_timeout: Duration,

    /// Block gap before sharing the state on sync.
    /// Defaults to 32
    pub sync_gap: u64,

    /// Delay in milliseconds which node waits after receiving block it can't
    /// apply before switching to sync mode.
    /// Defaults to 500
    pub sync_delay_milliseconds: u128,

    /// Save optimistic state frequency (every N'th block)
    /// Defaults to 200
    pub save_state_frequency: u32,

    /// Block keeper epoch code hash
    pub block_keeper_epoch_code_hash: String,

    /// Block keeper preepoch code hash
    pub block_keeper_preepoch_code_hash: String,

    /// Expected maximum number of threads.
    /// Note: it can grow over this value for some time on the running network.
    pub thread_count_soft_limit: usize,

    /// Thread load (aggregated number of messages in a queue to start splitting a thread) threshold for split
    pub thread_load_threshold: usize,

    /// Thread load window size, which is used to calculate thread load
    pub thread_load_window_size: usize,

    /// Change for a successfull attack
    pub chance_of_successful_attack: f64,
}

/// Node interaction settings
#[derive(Serialize, Deserialize, Debug, Clone, TypedBuilder)]
pub struct NodeConfig {
    /// Identifier of the current node.
    pub node_id: NodeIdentifier,

    /// Path to the file with blockchain config.
    #[builder(default = PathBuf::from("blockchain_config.json"))]
    pub blockchain_config_path: PathBuf,

    /// Path to the file with BLS key pair.
    #[builder(default = "block_keeper.keys.json".to_string())]
    pub key_path: String,

    /// Path to the file with block keeper seed key.
    #[builder(default = "block_keeper.keys.json".to_string())]
    pub block_keeper_seed_path: String,

    /// Path to zerostate file.
    #[builder(default = PathBuf::from("zerostate"))]
    pub zerostate_path: PathBuf,

    /// Local directory path which will be shared to other nodes.
    #[builder(default = PathBuf::from("/tmp"))]
    pub external_state_share_local_base_dir: PathBuf,

    /// Level of block production parallelization.
    #[builder(default = 20)]
    pub parallelization_level: usize,

    /// Block cache size in local repository
    #[builder(default = 20)]
    pub block_cache_size: usize,

    /// State cache size in local repository
    #[builder(default = 10)]
    pub state_cache_size: usize,

    /// Number of blocks after which the account is unloaded from shard state.
    #[builder(default = 10)]
    pub unload_after: u32,
    /// Path for message durable storage.
    #[builder(default = PathBuf::from("./message_storage/db"))]
    pub message_storage_path: PathBuf,

    /// Limit of calls to the on_incoming_block_request function per second
    #[builder(default = u32::MAX)]
    pub rate_limit_on_incoming_block_req: u32,

    /// Ext messages cache size
    #[builder(default = 1000)]
    pub ext_messages_cache_size: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// Global config
    #[serde(default)]
    pub global: GlobalConfig,

    /// Network config
    pub network: NetworkConfig,

    /// Local config
    pub local: NodeConfig,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            time_to_produce_block_millis: 330,
            time_to_verify_block_millis: 330 * 4 / 3,
            time_to_produce_transaction_millis: None,
            time_to_verify_transaction_millis: None,
            time_to_verify_transaction_aborted_with_execution_timeout_millis: None,
            need_synchronization_block_diff: 20,
            min_time_between_state_publish_directives: Duration::from_secs(600),
            attestation_resend_timeout: Duration::from_secs(3),
            producer_change_gap_size: 6,
            node_joining_timeout: Duration::from_secs(300),
            sync_gap: 32,
            sync_delay_milliseconds: 500,
            save_state_frequency: 200,
            block_keeper_epoch_code_hash:
                "7929933699d89b37b22b4c35632408197d3ba3f54e95af8f8ef04fc1962f203b".to_string(),
            block_keeper_preepoch_code_hash:
                "e3ae3234925316215a2310cb687ec65ed4a68aae285d038c73aa851c216ec894".to_string(),
            thread_count_soft_limit: 100,
            thread_load_window_size: 100,
            thread_load_threshold: 5000,
            chance_of_successful_attack: 0.000000001_f64,
        }
    }
}

impl Config {
    pub fn gossip_config(&self) -> anyhow::Result<gossip::GossipConfig> {
        Ok(gossip::GossipConfig {
            listen_addr: self.network.gossip_listen_addr,
            advertise_addr: self.network.gossip_advertise_addr,
            seeds: self.network.gossip_seeds.clone(),
            cluster_id: self.network.chitchat_cluster_id.clone(),
        })
    }

    pub fn gossip_peer(&self) -> anyhow::Result<GossipPeer<NodeIdentifier>> {
        GossipPeer::new(
            self.local.node_id.clone(),
            self.network.node_advertise_addr,
            self.network.proxies.clone(),
            self.network.bm_api_socket,
            self.network.bk_api_socket,
            transport_layer::resolve_signing_key(
                self.network.my_ed_key_secret.clone(),
                self.network.my_ed_key_path.clone(),
            )?,
        )
    }

    pub fn network_config(
        &self,
        tls_cert_cache: Option<TlsCertCache>,
    ) -> anyhow::Result<network::config::NetworkConfig> {
        network::config::NetworkConfig::new(
            self.network.bind,
            CertFile::try_new(&self.network.my_cert)?,
            PrivateKeyFile::try_new(&self.network.my_key)?,
            transport_layer::resolve_signing_key(
                self.network.my_ed_key_secret.clone(),
                self.network.my_ed_key_path.clone(),
            )?,
            CertStore::try_new(&self.network.peer_certs)?,
            self.network.peer_ed_pubkeys.clone(),
            self.network.subscribe.clone(),
            self.network.proxies.clone(),
            tls_cert_cache,
        )
    }
}

#[cfg(test)]
impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            node_id: NodeIdentifier::some_id(),
            blockchain_config_path: PathBuf::from("blockchain_config.json"),
            key_path: "block_keeper.keys.json".to_string(),
            zerostate_path: PathBuf::from("zerostate"),
            external_state_share_local_base_dir: PathBuf::from("/tmp"),
            parallelization_level: 20,
            block_keeper_seed_path: "block_keeper.keys.json".to_string(),
            block_cache_size: 20,
            state_cache_size: 10,
            unload_after: 10,
            message_storage_path: PathBuf::from("./message_storage/db"),
            rate_limit_on_incoming_block_req: u32::MAX,
            ext_messages_cache_size: 200,
        }
    }
}

pub fn must_save_state_on_seq_no(
    seq_no: BlockSeqNo,
    parent_seq_no: Option<BlockSeqNo>,
    save_state_frequency: u32,
) -> bool {
    let seq_no = u32::from(seq_no);
    if let Some(parent_seq_no) = parent_seq_no {
        (u32::from(parent_seq_no) / save_state_frequency) != (seq_no / save_state_frequency)
    } else {
        seq_no % save_state_frequency == 0
    }
}
