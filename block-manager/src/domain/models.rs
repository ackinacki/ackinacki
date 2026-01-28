use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use message_router::message_router::MessageRouter;
use message_router::KeyPair;
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
    pub subscribe_socket: SocketAddr,
    pub sqlite_path: String,
    pub owner_wallet_pubkey: Option<String>,
    pub signing_keys: Option<KeyPair>,
    pub bk_api_token: String,
}

pub struct AppState {
    pub default_bp: HostPort,
    pub bk_api_token: String,
    pub message_router: Arc<MessageRouter>,
    pub last_block_gen_utime: AtomicU64,
}

pub trait UpdatableBPResolver: BPResolver {
    fn upsert(&self, thread_id: String, bp_list: Vec<HostPort>) -> anyhow::Result<()>;
}
