// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::Sender;

use api::blocks::GetBlockFn;
pub use api::ext_messages::ExtMsgFeedback;
pub use api::ext_messages::FeedbackError;
pub use api::ext_messages::FeedbackErrorCode;
use rcgen::CertifiedKey;
use salvo::conn::rustls::Keycert;
use salvo::conn::rustls::RustlsConfig;
use salvo::prelude::*;
use tokio::sync::oneshot;
use tvm_block::Message;

mod api;

#[derive(Clone)]
pub struct WebServer<TMessage, TMsgConverter, TBPResolver> {
    pub addr: String,
    pub local_storage_dir: PathBuf,
    pub get_block_by_id: GetBlockFn,
    pub incoming_message_sender: Sender<(TMessage, Option<oneshot::Sender<ExtMsgFeedback>>)>,
    pub into_external_message: TMsgConverter,
    pub bp_resolver: TBPResolver,
}

impl<TMessage, TMsgConverter, TBPResolver> WebServer<TMessage, TMsgConverter, TBPResolver>
where
    TMessage: Send + Sync + Clone + 'static + std::fmt::Debug,
    TMsgConverter:
        Send + Sync + Clone + 'static + Fn(Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Send + Sync + Clone + 'static + FnMut([u8; 34]) -> Option<std::net::SocketAddr>,
{
    pub fn new(
        addr: impl AsRef<str>,
        local_storage_dir: impl AsRef<Path>,
        get_block_by_id: GetBlockFn,
        incoming_message_sender: Sender<(TMessage, Option<oneshot::Sender<ExtMsgFeedback>>)>,
        into_external_message: TMsgConverter,
        bp_resolver: TBPResolver,
    ) -> Self {
        Self {
            addr: addr.as_ref().to_string(),
            local_storage_dir: local_storage_dir.as_ref().to_path_buf(),
            get_block_by_id,
            incoming_message_sender,
            into_external_message,
            bp_resolver,
        }
    }

    pub fn route(self) -> Router {
        // Returns latest shard state
        let storage_latest_router = Router::with_path("storage_latest")
            .get(api::StorageLatestHandler::new(self.local_storage_dir.clone()));
        // Returns selected shard state
        let storage_router = Router::with_path("storage/<**path>")
            .get(StaticDir::new([&self.local_storage_dir]).auto_list(true));

        // Process inbound external messages
        //
        // JSON: [{
        //         "id": String,
        //         "boc": String,
        //         "expire"?: Int
        //       }]
        let ext_messages_router = Router::with_path("messages").post(
            api::ext_messages::v1::ExtMessagesHandler::<TMessage, TMsgConverter, TBPResolver>::new(
            ),
        );
        let ext_messages_router_v2 = Router::with_path("messages").post(
            api::ext_messages::v2::ExtMessagesHandler::<TMessage, TMsgConverter, TBPResolver>::new(
            ),
        );

        // TODO: not implemented yet
        // Returns block by id
        let blocks_router = Router::with_path("blocks/<id>")
            .get(api::BlocksBlockHandler::new(self.get_block_by_id.clone()));

        let router_v1 = Router::with_path("v1")
            .push(storage_latest_router)
            .push(storage_router)
            .push(ext_messages_router)
            .push(blocks_router);

        let router_v2 = Router::with_path("v2").push(ext_messages_router_v2);

        Router::new() //
            .hoop(salvo::logging::Logger::new())
            .hoop(affix_state::inject(self.clone()))
            .path("bk")
            .push(router_v1)
            .push(router_v2)
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
