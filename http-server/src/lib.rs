use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::Sender;

use api::blocks::GetBlockFn;
use rcgen::CertifiedKey;
use salvo::conn::rustls::Keycert;
use salvo::conn::rustls::RustlsConfig;
use salvo::prelude::*;
use tvm_block::Message;

mod api;

#[derive(Clone)]
pub struct WebServer<T, F> {
    pub addr: String,
    pub local_storage_dir: PathBuf,
    pub get_block_by_id: GetBlockFn,
    pub incoming_message_sender: Sender<T>,
    pub into_external_message: F,
}

impl<T, F> WebServer<T, F>
where
    T: Send + Sync + Clone + 'static + std::fmt::Debug,
    F: Send + Sync + Clone + 'static + Fn(Message) -> anyhow::Result<T>,
{
    pub fn new(
        addr: impl AsRef<str>,
        local_storage_dir: impl AsRef<Path>,
        get_block_by_id: GetBlockFn,
        incoming_message_sender: Sender<T>,
        into_external_message: F,
    ) -> Self {
        Self {
            addr: addr.as_ref().to_string(),
            local_storage_dir: local_storage_dir.as_ref().to_path_buf(),
            get_block_by_id,
            incoming_message_sender,
            into_external_message,
        }
    }

    pub fn route(self) -> Router {
        let storage_latest_router = Router::with_path("storage_latest")
            .get(api::StorageLatestHandler::new(self.local_storage_dir.clone()));
        let storage_router = Router::with_path("storage/<**path>")
            .get(StaticDir::new([&self.local_storage_dir]).auto_list(true));

        let topics_router =
            Router::with_path("topics/requests").post(api::TopicsRequestsHandler::<T, F>::new());

        // TODO: not implemented yet
        let blocks_router = Router::with_path("blocks/<id>")
            .get(api::BlocksBlockHandler::new(self.get_block_by_id.clone()));

        Router::new() //
            .hoop(Logger::new())
            .hoop(affix_state::inject(self.clone()))
            .push(storage_latest_router)
            .push(storage_router)
            .push(topics_router)
            .push(blocks_router)
    }

    #[must_use = "server run must be awaited twice (first await is to prepare run call)"]
    pub async fn run(self) -> impl Future<Output = ()> {
        let rustls_config = rustls_config();

        let quinn_listener = QuinnListener::new(
            rustls_config.clone().build_quinn_config().expect("QUIC quinn config"),
            self.addr.clone(),
        );
        // TODO: turn SSL back when it's ready
        // let tcp_listener = TcpListener::new(self.addr.clone()).rustls(rustls_config);
        let tcp_listener = TcpListener::new(self.addr.clone());

        // TODO: maybe use try_bind?
        let acceptor = tcp_listener.join(quinn_listener).bind().await;

        tracing::info!("Start HTTP server on {}", &self.addr);
        Server::new(acceptor).serve(Service::new(self.route()))
    }
}

pub fn rustls_config() -> RustlsConfig {
    // generate self-signed keys
    let CertifiedKey { cert, key_pair } = rcgen::generate_simple_self_signed([
        "0.0.0.0".into(),
        "127.0.0.1".into(),
        "::1".into(),
        "localhost".into(),
    ])
    .expect("generate self-signed certs");

    let keycert = Keycert::new().cert(cert.pem()).key(key_pair.serialize_pem());
    RustlsConfig::new(keycert)
}
