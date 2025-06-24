// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Display;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web;
use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use parking_lot::Mutex;
use serde_json::json;
use telemetry_utils::now_ms;

use crate::base64_id_decode;
use crate::bp_resolver::BPResolver;
use crate::defaults::DEFAULT_BK_API_TIMEOUT;
use crate::defaults::DEFAULT_NODE_URL_PATH;
use crate::defaults::DEFAULT_NODE_URL_PROTO;
use crate::defaults::DEFAULT_URL_PATH;
use crate::token::Token;
use crate::KeyPair;

lazy_static::lazy_static!(
    static ref NODE_URL_PATH: String = std::env::var("NODE_URL_PATH")
        .unwrap_or(DEFAULT_NODE_URL_PATH.into());

    static ref ROUTER_URL_PATH: String = std::env::var("ROUTER_URL_PATH")
        .unwrap_or(DEFAULT_URL_PATH.into());
);

pub struct MessageRouterConfig {
    pub bp_resolver: Arc<Mutex<dyn BPResolver>>,
    pub license_addr: Option<String>,
    pub keys: Option<KeyPair>,
}

#[derive(Clone, Debug)]
pub struct MessageRouter {
    pub bind: SocketAddr,
    pub license_addr: Option<String>,
    pub keys: Option<KeyPair>,
}

impl Display for MessageRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageRouter bind={}", self.bind)
    }
}

impl MessageRouter {
    pub fn new(bind: String, config: MessageRouterConfig) -> Self {
        let bind = bind.to_socket_addrs().expect("msg").next().unwrap();
        let license_addr = config.license_addr;
        let keys = config.keys;
        let message_router = Self { bind, license_addr, keys };

        tracing::info!("Starting MessageRouter: {message_router:?}");

        {
            let message_router = Arc::new(message_router.clone());
            let bp_resolver = Arc::clone(&config.bp_resolver);
            let _ = std::thread::Builder::new()
                .name("Message router".into())
                .spawn(move || {
                    if let Err(e) = Self::run(bind, bp_resolver, message_router) {
                        tracing::error!("Message router exited with error: {:?}", e);
                    }
                })
                .expect("Failed to spawn actix");
        }

        message_router
    }

    #[actix_web::main]
    async fn run(
        bind: SocketAddr,
        bp_resolver: Arc<Mutex<dyn BPResolver>>,
        message_router: Arc<MessageRouter>,
    ) -> anyhow::Result<()> {
        HttpServer::new(move || {
            let bp_resolver_clone = bp_resolver.clone();
            let message_router = message_router.clone();
            App::new()
                .wrap(middleware::DefaultHeaders::new().add(("Content-type", "application/json")))
                .configure(move |cfg| Self::config(cfg, bp_resolver_clone, message_router))
                .wrap(Logger::default())
        })
        .bind(bind)?
        .run()
        .await
        .map_err(anyhow::Error::from)?;

        tracing::info!(target: "message_router", "MessageRouter is running: bind={bind}");

        Ok(())
    }

    pub fn config(
        cfg: &mut web::ServiceConfig,
        bp_resolver: Arc<Mutex<dyn BPResolver>>,
        message_router: Arc<MessageRouter>,
    ) {
        cfg.service(
            // Process inbound external messages and forward its to the BP
            //
            // JSON: [{
            //     "id": String,
            //     "body": String,
            //     "expireAt"?: Int,
            //     "threadId"?: String,
            //     "bm_token": {
            //         "value": String,
            //         "expire_at": Int,
            //     }
            // }]
            web::resource(ROUTER_URL_PATH.to_string())
                .route(web::post().to(move |node_requests| {
                    let resolver_clone = bp_resolver.clone();
                    let message_router = message_router.clone();
                    Self::process_ext_messages(node_requests, resolver_clone, message_router)
                }))
                .route(web::head().to(HttpResponse::MethodNotAllowed)),
        );
    }

    async fn process_ext_messages(
        node_requests: web::Json<serde_json::Value>,
        bp_resolver: Arc<Mutex<dyn BPResolver>>,
        message_router: Arc<MessageRouter>,
    ) -> actix_web::Result<web::Json<serde_json::Value>> {
        let Some(nrs) = node_requests.as_array() else {
            tracing::error!(target: "message_router", "bad request: {node_requests}");
            let error = serde_json::json!(http_server::ExtMsgResponse::new_with_error(
                "BAD_REQUEST".into(),
                "Incorrect request".into(),
                None,
            ));
            return Ok(web::Json(error));
        };
        let thread_id = nrs
            .first()
            .and_then(|f| f.get("thread_id"))
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or(String::from(
                "00000000000000000000000000000000000000000000000000000000000000000000",
            ));

        let ids: HashMap<serde_json::Value, String> = nrs
            .iter()
            .filter_map(|nr| base64_id_decode(&nr["id"]).ok().map(|id| (nr["id"].clone(), id)))
            .collect();

        tracing::info!(target: "message_router", "Ext messages received: {:?}", nrs.iter().map(|nr| format!("{}", nr["id"])));

        let recipients = bp_resolver.lock().resolve(Some(thread_id.clone()));
        tracing::trace!(target: "message_router", "Resolved BPs (thread={:?}): {:?}", thread_id, recipients);
        let mut result = serde_json::json!({});
        if recipients.is_empty() {
            let error = serde_json::json!(http_server::ExtMsgResponse::new_with_error(
                "INTERNAL_ERROR".into(),
                "Failed to obtain any Block Producer addresses".into(),
                None,
            ));
            return Ok(web::Json(error));
        }

        let mut nrs = nrs.clone();
        let bm_token = json!(message_router.create_token());
        tracing::debug!(target: "message_router", "add token to request: {:?}", bm_token);
        if let Some(license_addr) = &message_router.license_addr {
            let bm_license = serde_json::Value::String(license_addr.to_string());
            for nr in &mut nrs {
                nr["bm_token"] = bm_token.clone();
                nr["bm_license"] = bm_license.clone();
            }
        }

        let client =
            awc::Client::builder().timeout(Duration::from_secs(DEFAULT_BK_API_TIMEOUT)).finish();
        for recipient in recipients {
            let url = construct_url(recipient);
            tracing::info!(target: "message_router", "Forwarding requests to: {url}");
            result = match client
                .post(&url)
                .insert_header(("X-EXT-MSG-SENT", now_ms().to_string()))
                .send_json(&nrs)
                .await
            {
                Ok(mut response) => {
                    let body = response.body().await;
                    let recipient = recipient.ip().to_string();
                    tracing::info!(target: "message_router", "response body (src={}): {:?}", recipient, body);
                    match body {
                        Ok(b) => {
                            let s = std::str::from_utf8(&b)?;
                            let mut response = serde_json::from_str::<serde_json::Value>(s)?;
                            if message_router.license_addr.is_some() {
                                response["block_manager"] = json!({
                                    "license_address": message_router.license_addr,
                                    "token": json!(message_router.create_token()),
                                });
                                tracing::info!(target: "message_router", "add token to response: {:?}", response["block_manager"]);
                            }
                            return Ok(web::Json(response));
                        }
                        Err(err) => {
                            tracing::error!(target: "message_router", "redirection to {url} failed: {err}");
                            let err_data = http_server::ExtMsgErrorData::new(
                                vec![recipient],
                                ids.get(&nrs[0]["id"]).unwrap().to_string(),
                                None,
                                Some(thread_id.clone()),
                            );
                            serde_json::json!(http_server::ExtMsgResponse::new_with_error(
                                "INTERNAL_ERROR".into(),
                                format!(
                                    "Failed to parse the response from the Block Producer: {err}"
                                ),
                                Some(err_data),
                            ))
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(target: "message_router", "redirection to {url} failed: {err}");
                    let err_data = http_server::ExtMsgErrorData::new(
                        vec![recipient.ip().to_string()],
                        ids.get(&nrs[0]["id"]).unwrap().to_string(),
                        None,
                        Some(thread_id.clone()),
                    );
                    serde_json::json!(http_server::ExtMsgResponse::new_with_error(
                        "INTERNAL_ERROR".into(),
                        format!("The message redirection to the Block Producer has failed: {err}"),
                        Some(err_data),
                    ))
                }
            }
        }

        Ok(web::Json(result))
    }

    pub fn dump(&self) {
        tracing::debug!("msg_router: {}", self);
    }
}

impl MessageRouter {
    pub fn create_token(&self) -> Option<Token> {
        if let Some(keys) = &self.keys {
            Token::new_token(keys).ok()
        } else {
            None
        }
    }
}

fn construct_url(host: SocketAddr) -> String {
    format!("{DEFAULT_NODE_URL_PROTO}://{}:{}{}", host.ip(), host.port(), *NODE_URL_PATH)
}
