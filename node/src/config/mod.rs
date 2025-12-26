// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
mod blockchain_config;
pub mod config_read;
mod network_config;
mod serde_config;
#[cfg(test)]
mod test;
mod validations;

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

pub use blockchain_config::*;
use gossip::gossip_peer::GossipPeer;
use gossip::GossipReloadConfig;
use network::pub_sub::CertFile;
use network::pub_sub::CertStore;
use network::pub_sub::PrivateKeyFile;
use network::resolver::WatchGossipConfig;
use network::topology::NetEndpoint;
use network::topology::NetPeer;
pub use network_config::NetworkConfig;
use serde::Deserialize;
use serde::Serialize;
pub use serde_config::load_config_from_file;
pub use serde_config::save_config_to_file;
use transport_layer::TlsCertCache;
use typed_builder::TypedBuilder;

use crate::node::NodeIdentifier;
use crate::types::BlockSeqNo;

const DEFAULT_ENGINE_VERSION: &str = "1.0.1";
const DEFAULT_GOSSIP_VERSION: &str = "0";

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

    /// Chance of a successful attack
    pub chance_of_successful_attack: f64,

    /// BP rotation round parameters
    pub round_min_time_millis: u64,
    pub round_step_millis: u64,
    pub round_max_time_millis: u64,

    ///  Security parameter. Minimal time after the last finalization to enable Synchronization start
    pub time_to_enable_sync_finalized: Duration,

    /// TVM engine version
    pub engine_version: semver::Version,

    /// Gossip protocol version
    pub gossip_version: u16,
}

/// Node interaction settings
#[derive(Serialize, Deserialize, Debug, Clone, TypedBuilder, PartialEq)]
pub struct NodeConfig {
    /// Identifier of the current node.
    pub node_id: NodeIdentifier,

    /// Path to the file with blockchain config.
    /// Config is deprecated (it is unused and will be removed soon)
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

    /// Optional path to init bk set. If omitted, uses bk set from zerostate.
    /// Bk set file format must match `/v2/bk_set_update` API output.
    #[builder(default)]
    pub bk_set_update_path: Option<PathBuf>,

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
    #[builder(default = None)]
    pub unload_after: Option<u32>,

    /// Limit of calls to the on_incoming_block_request function per second
    #[builder(default = u32::MAX)]
    pub rate_limit_on_incoming_block_req: u32,

    /// Ext messages cache size
    #[builder(default = 1000)]
    pub ext_messages_cache_size: usize,

    /// BlockKeeper node owner wallet pubkey
    #[builder(default = "".to_string())]
    pub node_wallet_pubkey: String,

    /// Path to file with keys pair for signing the authorization token of incoming external messages
    /// Required for direct sending external messages via node
    #[builder(default = None)]
    pub signing_keys: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// Network config
    pub network: NetworkConfig,

    /// Local config
    pub local: NodeConfig,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        let time_to_verify_block_millis = 330 * 4 / 3;
        #[cfg(feature = "low_verification_time")]
        let time_to_verify_block_millis = 33 * 4 / 3;
        Self {
            time_to_produce_block_millis: 330,
            time_to_verify_block_millis,
            time_to_produce_transaction_millis: None,
            time_to_verify_transaction_millis: None,
            time_to_verify_transaction_aborted_with_execution_timeout_millis: None,
            need_synchronization_block_diff: 20,
            min_time_between_state_publish_directives: Duration::from_secs(600),
            attestation_resend_timeout: Duration::from_secs(3),
            producer_change_gap_size: 6,
            node_joining_timeout: Duration::from_secs(300),
            sync_gap: 64,
            sync_delay_milliseconds: 500,
            save_state_frequency: 200,
            block_keeper_epoch_code_hash:
                "2a7dd92c9d5617625bf1fbba27fc56cdebe8e3dfa844ecce0cf824818ece41a4".to_string(),
            block_keeper_preepoch_code_hash:
                "cd342c55f7e669738889c85858e0b065f61641d8e683b5ddd1ce5d3fe981e92a".to_string(),
            thread_count_soft_limit: 100,
            thread_load_window_size: 100,
            thread_load_threshold: 5000,
            chance_of_successful_attack: 0.000000001_f64,
            round_min_time_millis: 10000,
            round_step_millis: 1000,
            round_max_time_millis: 30000,
            time_to_enable_sync_finalized: Duration::from_secs(1200),
            engine_version: DEFAULT_ENGINE_VERSION.parse().unwrap(),
            gossip_version: DEFAULT_GOSSIP_VERSION.parse().unwrap(),
        }
    }
}

impl Config {
    pub fn gossip_config(&self) -> gossip::GossipConfig {
        gossip::GossipConfig {
            listen_addr: self.network.gossip_listen_addr,
            advertise_addr: self.network.gossip_advertise_addr,
            seeds: self.network.gossip_seeds.clone().into(),
            cluster_id: self.network.chitchat_cluster_id.clone(),
            peer_ttl_seconds: self.network.gossip_peer_ttl_seconds,
        }
    }

    pub fn gossip_reload_config(&self) -> anyhow::Result<GossipReloadConfig<NodeIdentifier>> {
        Ok(GossipReloadConfig {
            gossip_config: self.gossip_config(),
            my_ed_key_secret: self.network.my_ed_key_secret.clone(),
            my_ed_key_path: self.network.my_ed_key_path.clone(),
            peer_config: Some(self.gossip_peer()?),
        })
    }

    pub fn gossip_peer(&self) -> anyhow::Result<GossipPeer<NodeIdentifier>> {
        GossipPeer {
            id: self.local.node_id.clone(),
            node_protocol_addr: self.network.node_advertise_addr,
            proxies: self.network.proxies.clone().into(),
            bm_api_addr: self.network.bm_api_socket,
            bk_api_addr_deprecated: self.network.bk_api_socket,
            bk_api_url_for_storage_sync: Some(self.network.api_advertise_addr.clone()),
            bk_api_host_port: self.network.bk_api_host_port.clone(),
            pubkey_signature: None,
        }
        .signed(&transport_layer::resolve_signing_keys(
            &self.network.my_ed_key_secret,
            &self.network.my_ed_key_path,
        )?)
    }

    pub fn endpoint(&self) -> NetEndpoint<NodeIdentifier> {
        NetEndpoint::Peer(NetPeer::with_id_and_addr(
            self.local.node_id.clone(),
            self.network.node_advertise_addr,
        ))
    }

    pub fn watch_gossip_config(
        &self,
        trusted_pubkeys: HashSet<transport_layer::VerifyingKey>,
    ) -> WatchGossipConfig<NodeIdentifier> {
        WatchGossipConfig {
            endpoint: self.endpoint(),
            max_nodes_with_same_id: self.network.max_nodes_with_same_id as usize,
            override_subscribe: self.override_subscribe(),
            trusted_pubkeys,
            peer_ttl_seconds: self.network.gossip_peer_ttl_seconds,
        }
    }

    pub fn override_subscribe(&self) -> Vec<NetEndpoint<NodeIdentifier>> {
        if !self.network.subscribe.is_empty() {
            self.network.subscribe.iter().map(|x| NetEndpoint::Proxy(x.clone())).collect()
        } else if !self.network.proxies.is_empty() {
            vec![NetEndpoint::Proxy(self.network.proxies.clone())]
        } else {
            Default::default()
        }
    }

    pub fn network_config(
        &self,
        tls_cert_cache: Option<TlsCertCache>,
    ) -> anyhow::Result<network::config::NetworkConfig> {
        network::config::NetworkConfig::new(
            self.network.bind,
            self.network.direct_send_mode.clone(),
            CertFile::try_new(&self.network.my_cert)?,
            PrivateKeyFile::try_new(&self.network.my_key)?,
            &transport_layer::resolve_signing_keys(
                &self.network.my_ed_key_secret,
                &self.network.my_ed_key_path,
            )?,
            CertStore::try_new(&self.network.peer_certs)?,
            HashSet::from_iter(self.network.peer_ed_pubkeys.clone()),
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
            bk_set_update_path: None,
            external_state_share_local_base_dir: PathBuf::from("/tmp"),
            parallelization_level: 20,
            block_keeper_seed_path: "block_keeper.keys.json".to_string(),
            block_cache_size: 20,
            state_cache_size: 10,
            unload_after: None,
            rate_limit_on_incoming_block_req: u32::MAX,
            ext_messages_cache_size: 200,
            node_wallet_pubkey: "some_public_key".to_string(),
            signing_keys: None,
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
