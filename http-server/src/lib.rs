// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

pub use api::ext_messages::ExtMsgError;
pub use api::ext_messages::ExtMsgErrorData;
pub use api::ext_messages::ExtMsgFeedback;
pub use api::ext_messages::ExtMsgFeedbackList;
pub use api::ext_messages::ExtMsgResponse;
pub use api::ext_messages::FeedbackError;
pub use api::ext_messages::FeedbackErrorCode;
pub use api::ext_messages::ResolvingResult;
pub use api::BlockKeeperSetUpdate;
use metrics::RoutingMetrics;
use rcgen::CertifiedKey;
use salvo::conn::rustls::Keycert;
use salvo::conn::rustls::RustlsConfig;
use salvo::prelude::*;
use telemetry_utils::mpsc::InstrumentedReceiver;
use telemetry_utils::mpsc::InstrumentedSender;
use tokio::sync::oneshot;
use tvm_block::Message;

use crate::api::BkSetSnapshot;

mod api;
pub mod metrics;

#[derive(Clone)]
pub struct WebServer<TMessage, TMsgConverter, TBPResolver> {
    pub addr: String,
    pub local_storage_dir: PathBuf,
    pub incoming_message_sender:
        InstrumentedSender<(TMessage, Option<oneshot::Sender<ExtMsgFeedback>>)>,
    pub bk_set: Arc<parking_lot::RwLock<BkSetSnapshot>>,
    pub into_external_message: TMsgConverter,
    pub bp_resolver: TBPResolver,
    pub metrics: Option<RoutingMetrics>,
}

impl<TMessage, TMsgConverter, TBPResolver> WebServer<TMessage, TMsgConverter, TBPResolver>
where
    TMessage: Send + Sync + Clone + 'static + std::fmt::Debug,
    TMsgConverter:
        Send + Sync + Clone + 'static + Fn(Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Send + Sync + Clone + 'static + FnMut([u8; 34]) -> ResolvingResult,
{
    pub fn new(
        addr: impl AsRef<str>,
        local_storage_dir: impl AsRef<Path>,
        incoming_message_sender: InstrumentedSender<(
            TMessage,
            Option<oneshot::Sender<ExtMsgFeedback>>,
        )>,
        into_external_message: TMsgConverter,
        bp_resolver: TBPResolver,
        metrics: Option<RoutingMetrics>,
    ) -> Self {
        Self {
            addr: addr.as_ref().to_string(),
            local_storage_dir: local_storage_dir.as_ref().to_path_buf(),
            incoming_message_sender,
            into_external_message,
            bp_resolver,
            bk_set: Arc::new(parking_lot::RwLock::new(BkSetSnapshot::new())),
            metrics,
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

        // curl -v -H "If-Modified-Since: Wed, 22 Jan 2025 06:56:02 GMT" localhost:11001/bk/v1/bk_set
        let bk_set_router = Router::with_path("bk_set").get(api::BkSetHandler::<
            TMessage,
            TMsgConverter,
            TBPResolver,
        >::new());

        let router_v1 = Router::with_path("v1")
            .push(storage_latest_router)
            .push(storage_router)
            .push(ext_messages_router)
            .push(bk_set_router);

        let router_v2 = Router::with_path("v2").push(ext_messages_router_v2);

        Router::new() //
            .hoop(Logger::new())
            .hoop(affix_state::inject(self.clone()))
            .path("bk")
            .push(router_v1)
            .push(router_v2)
    }

    #[must_use = "server run must be awaited twice (first await is to prepare run call)"]
    pub async fn run(self, bk_set_updates_rx: InstrumentedReceiver<BlockKeeperSetUpdate>) {
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

        let bk_set = self.bk_set.clone();
        let bk_set_update_task = std::thread::Builder::new()
            .name("BK set update handler".to_string())
            .spawn(move || {
                tracing::info!("BK set update handler started");
                while let Ok(update) = bk_set_updates_rx.recv() {
                    bk_set.write().update(update)
                }
                tracing::info!("BK set update handler stopped");
            })
            .expect("Failed to spawn BK set updates handler");

        tracing::info!("Start HTTP server on {}", &self.addr);
        Server::new(acceptor).serve(Service::new(self.route())).await;
        match bk_set_update_task.join() {
            Ok(_) => tracing::info!("BK set update handler stopped"),
            Err(_) => tracing::error!("BK set update handler stopped with error"),
        }
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
