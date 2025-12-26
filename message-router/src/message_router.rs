// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web;
use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use ext_messages_auth::auth::Token;
use parking_lot::Mutex;

use crate::bp_resolver::BPResolver;
use crate::defaults::DEFAULT_BK_API_MESSAGES_PATH;
use crate::defaults::DEFAULT_BM_API_MESSAGES_PATH;
use crate::KeyPair;

lazy_static::lazy_static!(
    static ref NODE_URL_PATH: String = std::env::var("NODE_URL_PATH")
        .unwrap_or(DEFAULT_BK_API_MESSAGES_PATH.into());

    static ref ROUTER_URL_PATH: String = std::env::var("ROUTER_URL_PATH")
        .unwrap_or(DEFAULT_BM_API_MESSAGES_PATH.into());
);

pub struct MessageRouterConfig {
    pub bp_resolver: Arc<Mutex<dyn BPResolver>>,
    pub owner_wallet_pubkey: Option<String>,
    pub signing_keys: Option<KeyPair>,
}

#[derive(Clone)]
pub struct MessageRouter {
    pub bind: SocketAddr,
    pub owner_wallet_pubkey: Option<String>,
    pub signing_keys: Option<KeyPair>,
    pub bp_resolver: Arc<Mutex<dyn BPResolver>>,
}

impl Display for MessageRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageRouter bind={}", self.bind)
    }
}

impl MessageRouter {
    pub fn new(bind: String, config: MessageRouterConfig) -> Self {
        let bind = bind.to_socket_addrs().expect("msg").next().unwrap();
        let owner_wallet_pubkey = config.owner_wallet_pubkey;
        let signing_keys = config.signing_keys;
        let bp_resolver = config.bp_resolver;
        let message_router = Self { bind, owner_wallet_pubkey, signing_keys, bp_resolver };

        tracing::info!("Starting MessageRouter: {message_router}");

        message_router
    }

    pub fn config(cfg: &mut web::ServiceConfig, message_router: Arc<MessageRouter>) {
        cfg.service(
            // Process inbound external messages and forward its to the BP
            //
            // JSON: [{
            //     "id": String,
            //     "body": String,
            //     "expireAt"?: Int,
            //     "threadId"?: String,
            //     "ext_message_token": {
            //         "unsigned": String,
            //         "signature": String,
            //         "issuer": {
            //              "bm": String,
            //              or
            //              "bk": String,
            //          }
            //     }
            // }]
            web::resource(ROUTER_URL_PATH.to_string())
                .route(web::post().to(move |node_requests| {
                    let message_router = message_router.clone();
                    actix_ext_messages_handler(node_requests, message_router)
                }))
                .route(web::head().to(HttpResponse::MethodNotAllowed)),
        );
    }

    pub fn dump(&self) {
        tracing::debug!("msg_router: {}", self);
    }
}

impl MessageRouter {
    pub fn issue_token(&self) -> Option<Token> {
        if let Some(keys) = &self.signing_keys {
            if let Some(issuer_pubkey) = &self.owner_wallet_pubkey {
                Token::new(
                    &keys.secret,
                    ext_messages_auth::auth::TokenIssuer::Bm(issuer_pubkey.to_string()),
                )
                .ok()
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn run(&self) {
        let message_router = Arc::new(self.clone());
        tracing::info!("Starting MessageRouter");
        let _ = std::thread::Builder::new()
            .name("Message router".into())
            .spawn(move || {
                if let Err(e) = run(message_router) {
                    tracing::error!("Message router exited with error: {:?}", e);
                }
            })
            .expect("Failed to spawn actix");
    }
}

#[actix_web::main]
async fn run(message_router: Arc<MessageRouter>) -> anyhow::Result<()> {
    let bind = message_router.bind;
    HttpServer::new(move || {
        let message_router = message_router.clone();
        App::new()
            .wrap(middleware::DefaultHeaders::new().add(("Content-type", "application/json")))
            .configure(move |cfg| MessageRouter::config(cfg, message_router))
            .wrap(Logger::default())
    })
    .bind(bind)?
    .run()
    .await
    .map_err(anyhow::Error::from)?;

    tracing::info!(target: "message_router", "MessageRouter is running: bind={bind}");

    Ok(())
}

async fn actix_ext_messages_handler(
    node_requests: web::Json<serde_json::Value>,
    message_router: Arc<MessageRouter>,
) -> actix_web::Result<web::Json<serde_json::Value>> {
    match crate::process_ext_messages::run(node_requests.into_inner(), message_router).await {
        Ok(value) => Ok(web::Json(value)),
        Err(e) => {
            tracing::error!("process_ext_messages_inner failed: {:?}", e);
            Err(actix_web::error::ErrorInternalServerError(e))
        }
    }
}
