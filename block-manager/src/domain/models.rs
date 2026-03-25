use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use message_router::message_router::MessageRouter;
use message_router::KeyPair;
use parking_lot::RwLock;
use transport_layer::HostPort;

pub const DEFAULT_BP_PORT: u16 = 8500;

pub enum WorkerCommand {
    Data(Vec<u8>),
    RotateDb,
    Shutdown(mpsc::Sender<()>),
}

pub struct AppConfig {
    pub default_bp: HostPort,
    pub rest_api: SocketAddr,
    pub subscribe_sockets: Vec<SocketAddr>,
    pub sqlite_path: String,
    pub owner_wallet_pubkey: Option<String>,
    pub signing_keys: Option<KeyPair>,
    pub bk_api_token: String,
    pub config_path: Option<PathBuf>,
    pub bk_api_endpoints: Vec<HostPort>,
}

pub struct AppState {
    pub bk_api_pool: BkApiPool,
    pub bk_api_token: String,
    pub message_router: Arc<MessageRouter>,
    pub last_block_gen_utime: AtomicU64,
}

/// Pool of BK API endpoints with failover support.
/// On error, tries the next endpoint and promotes the first successful one to default.
pub struct BkApiPool {
    endpoints: RwLock<Vec<HostPort>>,
    default_idx: AtomicUsize,
}

impl BkApiPool {
    pub fn new(endpoints: Vec<HostPort>) -> Self {
        Self { endpoints: RwLock::new(endpoints), default_idx: AtomicUsize::new(0) }
    }

    /// Update the list of endpoints (e.g. on SIGHUP config reload).
    pub fn update_endpoints(&self, new_endpoints: Vec<HostPort>) {
        let mut eps = self.endpoints.write();
        *eps = new_endpoints;
        self.default_idx.store(0, Ordering::Relaxed);
    }

    /// Returns the ordered list of endpoints to try, starting from the current default.
    pub fn endpoints_to_try(&self) -> Vec<HostPort> {
        let eps = self.endpoints.read();
        if eps.is_empty() {
            return vec![];
        }
        let idx = self.default_idx.load(Ordering::Relaxed) % eps.len();
        let mut result = Vec::with_capacity(eps.len());
        for i in 0..eps.len() {
            result.push(eps[(idx + i) % eps.len()].clone());
        }
        result
    }

    /// Promote the given endpoint to be the new default.
    pub fn promote(&self, endpoint: &HostPort) {
        let eps = self.endpoints.read();
        if let Some(pos) = eps.iter().position(|e| e == endpoint) {
            self.default_idx.store(pos, Ordering::Relaxed);
        }
    }
}

pub trait UpdatableBPResolver: BPResolver {
    fn upsert(&self, thread_id: String, bp_list: Vec<HostPort>) -> anyhow::Result<()>;
}
