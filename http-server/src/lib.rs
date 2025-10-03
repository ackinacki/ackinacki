// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

// pub use api::ext_messages::token::EXT_MESSAGE_AUTH_REQUIRED;
pub use api::ext_messages::ExtMsgError;
pub use api::ext_messages::ExtMsgErrorData;
pub use api::ext_messages::ExtMsgFeedback;
pub use api::ext_messages::ExtMsgFeedbackList;
pub use api::ext_messages::ExtMsgResponse;
pub use api::ext_messages::FeedbackError;
pub use api::ext_messages::FeedbackErrorCode;
pub use api::ext_messages::ResolvingResult;
pub use api::ApiBk;
pub use api::ApiBkSet;
pub use api::ApiBkStatus;
pub use api::ApiPubKey;
pub use api::ApiUInt256;
pub use api::BkSetSummary;
pub use api::BkSetSummaryResult;
pub use api::BkSummary;
use ext_messages_auth::auth::AccountRequest;
use ext_messages_auth::auth::Token;
use ext_messages_auth::read_keys_from_file;
use ext_messages_auth::KeyPair;
use metrics::RoutingMetrics;
use rcgen::CertifiedKey;
use salvo::conn::rustls::Keycert;
use salvo::conn::rustls::RustlsConfig;
use salvo::prelude::*;
use telemetry_utils::mpsc::InstrumentedSender;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tvm_block::Message;

use crate::api::ext_messages::render_error;
use crate::api::ext_messages::ExternalMessage;
use crate::api::ext_messages::IncomingExternalMessage;
use crate::api::ApiBkSetSnapshot;
use crate::api::BkSetSummarySnapshot;

mod api;
mod helpers;
pub mod metrics;

const AUTH_HEADER: &str = "authorization";
const PASS_UNAUTHORIZED_KEY: &str = "pass_unauthorized";
const AUTHORIZED_BY_BK_KEY: &str = "authorized_by_bk_token";

#[derive(Clone)]
pub struct WebServer<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter> {
    pub addr: String,
    pub local_storage_dir: PathBuf,
    pub incoming_message_sender:
        InstrumentedSender<(TMessage, Option<oneshot::Sender<ExtMsgFeedback>>)>,
    pub signing_pubkey_request_senber: mpsc::Sender<AccountRequest>,
    pub bk_set_summary: Arc<parking_lot::RwLock<BkSetSummarySnapshot>>,
    pub bk_set: Arc<parking_lot::RwLock<ApiBkSetSnapshot>>,
    pub into_external_message: TMsgConverter,
    pub bp_resolver: TBPResolver,
    pub get_boc_by_addr: TBocByAddrGetter,
    pub get_default_thread_seqno: TSeqnoGetter,
    pub owner_wallet_pubkey: Option<String>,
    pub signing_keys: Option<KeyPair>,
    pub metrics: Option<RoutingMetrics>,
}

impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
    WebServer<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
where
    TMessage: Send + Sync + Clone + 'static + std::fmt::Debug,
    TMsgConverter:
        Send + Sync + Clone + 'static + Fn(Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Send + Sync + Clone + 'static + FnMut([u8; 34]) -> ResolvingResult,
    TBocByAddrGetter:
        Send + Sync + Clone + 'static + Fn(String) -> anyhow::Result<(String, Option<String>)>,
    TSeqnoGetter: Send + Sync + Clone + 'static + Fn() -> anyhow::Result<u32>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        addr: impl AsRef<str>,
        local_storage_dir: impl AsRef<Path>,
        incoming_message_sender: InstrumentedSender<(
            TMessage,
            Option<oneshot::Sender<ExtMsgFeedback>>,
        )>,
        signing_pubkey_request_senber: mpsc::Sender<AccountRequest>,
        into_external_message: TMsgConverter,
        bp_resolver: TBPResolver,
        get_boc_by_addr: TBocByAddrGetter,
        get_default_thread_seqno: TSeqnoGetter,
        owner_wallet_pubkey: Option<String>,
        signing_keys_path: Option<String>,
        metrics: Option<RoutingMetrics>,
    ) -> Self {
        let signing_keys =
            signing_keys_path.as_ref().and_then(|path| read_keys_from_file(path).ok());
        Self {
            addr: addr.as_ref().to_string(),
            local_storage_dir: local_storage_dir.as_ref().to_path_buf(),
            incoming_message_sender,
            signing_pubkey_request_senber,
            into_external_message,
            bp_resolver,
            bk_set_summary: Arc::new(parking_lot::RwLock::new(BkSetSummarySnapshot::new())),
            bk_set: Arc::new(parking_lot::RwLock::new(ApiBkSetSnapshot::new())),
            get_boc_by_addr,
            get_default_thread_seqno,
            owner_wallet_pubkey,
            signing_keys,
            metrics,
        }
    }

    pub fn route(self) -> Router {
        // Returns latest shard state
        let storage_latest_router = Router::with_path("storage_latest")
            .get(api::StorageLatestHandler::new(self.local_storage_dir.clone()));
        // Returns selected shard state
        let storage_router = Router::with_path("storage/{*path}")
            .hoop(report_block_request)
            .get(StaticDir::new([&self.local_storage_dir]).auto_list(true));

        let bk_set_router = Router::with_path("bk_set").get(api::BkSetSummaryHandler::<
            TMessage,
            TMsgConverter,
            TBPResolver,
            TBocByAddrGetter,
            TSeqnoGetter,
        >::new());

        let bk_set_update_router = Router::with_path("bk_set_update").get(api::ApiBkSetHandler::<
            TMessage,
            TMsgConverter,
            TBPResolver,
            TBocByAddrGetter,
            TSeqnoGetter,
        >::new());

        let router_ext_messages = Router::with_path("messages")
            .hoop(pass_unauthorized)
            .hoop(auth)
            .hoop(validate_ext_message)
            .post(api::ext_messages::v2::ExtMessagesHandler::<
                TMessage,
                TMsgConverter,
                TBPResolver,
                TBocByAddrGetter,
                TSeqnoGetter,
            >::new());

        let router_account =
            Router::with_path("account").hoop(auth).get(api::BocByAddressHandler::<
                TMessage,
                TMsgConverter,
                TBPResolver,
                TBocByAddrGetter,
                TSeqnoGetter,
            >::new());

        let router_seqno =
            Router::with_path("default_thread_seqno").hoop(auth).get(api::LastSeqnoHandler::<
                TMessage,
                TMsgConverter,
                TBPResolver,
                TBocByAddrGetter,
                TSeqnoGetter,
            >::new());

        // Routes:
        // v2/bk_set
        // v2/messages
        // v2/account?address=<address>
        // v2/default_thread_seqno
        Router::new()
            .hoop(Logger::new())
            .hoop(affix_state::inject(self.clone()))
            .hoop(affix_state::inject(self.metrics.clone()))
            .push(
                Router::new()
                    .path("v2")
                    .push(router_account)
                    .push(router_ext_messages)
                    .push(bk_set_router)
                    .push(bk_set_update_router)
                    .push(router_seqno)
                    .push(storage_latest_router)
                    .push(storage_router),
            )
    }

    #[must_use = "server run must be awaited twice (first await is to prepare run call)"]
    pub async fn run(self, mut bk_set_rx: tokio::sync::watch::Receiver<ApiBkSet>) {
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

        let shared_bk_set = self.bk_set.clone();
        let shared_bk_set_summary = self.bk_set_summary.clone();
        let bk_set_task = tokio::spawn(async move {
            tracing::info!("BK set handler started");
            let mut bk_set = bk_set_rx.borrow().clone();
            shared_bk_set.write().replace(bk_set.clone());
            shared_bk_set_summary.write().replace(BkSetSummary::new(&bk_set));
            while bk_set_rx.changed().await.is_ok() {
                if bk_set.update(&bk_set_rx.borrow_and_update()) {
                    shared_bk_set.write().replace(bk_set.clone());
                    shared_bk_set_summary.write().replace(BkSetSummary::new(&bk_set));
                }
            }
            tracing::info!("BK set handler stopped");
        });

        tracing::info!("Start HTTP server on {}", &self.addr);
        Server::new(acceptor).serve(Service::new(self.route())).await;
        match bk_set_task.await {
            Ok(_) => tracing::info!("BK set update handler stopped"),
            Err(_) => tracing::error!("BK set update handler stopped with error"),
        }
    }

    pub fn issue_token(&self) -> Option<Token> {
        if let Some(keys) = &self.signing_keys {
            if let Some(issuer_pubkey) = &self.owner_wallet_pubkey {
                Token::new(
                    &keys.secret,
                    ext_messages_auth::auth::TokenIssuer::Bk(issuer_pubkey.to_string()),
                )
                .ok()
            } else {
                None
            }
        } else {
            None
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

#[handler]
pub async fn report_block_request(
    req: &mut Request,
    res: &mut Response,
    depot: &mut Depot,
    ctrl: &mut FlowCtrl,
) {
    if let Ok(Some(metrics)) = depot.obtain::<Option<RoutingMetrics>>() {
        metrics.report_state_request();
    };
    ctrl.call_next(req, depot, res).await;
}

#[handler]
pub async fn pass_unauthorized(
    req: &mut Request,
    res: &mut Response,
    depot: &mut Depot,
    ctrl: &mut FlowCtrl,
) {
    depot.insert(PASS_UNAUTHORIZED_KEY, true);
    ctrl.call_next(req, depot, res).await;
}

// This is authentication middleware. By default, if std::env::var `AUTH_TOKEN` is not set, access is denied.
#[handler]
pub async fn auth(req: &mut Request, res: &mut Response, depot: &mut Depot, ctrl: &mut FlowCtrl) {
    let token = std::env::var("AUTH_TOKEN").ok();

    let authorized = token.as_ref().is_some_and(|token| {
        req.headers()
            .get(AUTH_HEADER)
            .and_then(|auth_header| auth_header.to_str().ok())
            .is_some_and(|auth_str| auth_str == format!("Bearer {token}"))
    });

    let pass_unauth = depot.get::<bool>(PASS_UNAUTHORIZED_KEY).copied().unwrap_or(false);

    if authorized {
        depot.insert(AUTHORIZED_BY_BK_KEY, true);
    }

    if authorized || pass_unauth {
        ctrl.call_next(req, depot, res).await;
    } else {
        res.status_code(StatusCode::UNAUTHORIZED);
        res.render("Unauthorized");
    }
}

#[handler]
async fn validate_ext_message(
    req: &mut Request,
    res: &mut Response,
    depot: &mut Depot,
    ctrl: &mut FlowCtrl,
) {
    let Ok(incomings) = req.parse_json::<Vec<IncomingExternalMessage>>().await else {
        return render_error(res, StatusCode::BAD_REQUEST, "Invalid request body", None);
    };

    if incomings.is_empty() {
        return render_error(res, StatusCode::BAD_REQUEST, "Empty request", None);
    }

    let Ok(ext_msg): Result<ExternalMessage, _> = (&incomings[0]).try_into() else {
        let msg = format!("Error parsing message (msg_id={:?})", incomings[0].id());
        tracing::warn!(target: "http_server", msg);
        return render_error(res, StatusCode::BAD_REQUEST, &msg, None);
    };

    if !ext_msg.is_dst_exists() {
        return render_error(res, StatusCode::BAD_REQUEST, "Invalid destination", None);
    }

    depot.insert("message", ext_msg);

    ctrl.call_next(req, depot, res).await;
}
