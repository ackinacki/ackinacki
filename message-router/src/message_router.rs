// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
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
use telemetry_utils::now_ms;

use crate::base64_id_decode;
use crate::bp_resolver::BPResolver;
use crate::defaults::DEFAULT_BK_API_TIMEOUT;
use crate::defaults::DEFAULT_NODE_URL_PATH;
use crate::defaults::DEFAULT_NODE_URL_PORT;
use crate::defaults::DEFAULT_NODE_URL_PROTO;
use crate::defaults::DEFAULT_URL_PATH;

lazy_static::lazy_static!(
    static ref NODE_URL_PATH: String = std::env::var("NODE_URL_PATH")
        .unwrap_or(DEFAULT_NODE_URL_PATH.into());

    static ref ROUTER_URL_PATH: String = std::env::var("ROUTER_URL_PATH")
        .unwrap_or(DEFAULT_URL_PATH.into());
);

pub struct MessageRouter {
    pub bind: SocketAddr,
}

impl Display for MessageRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageRouter bind={}", self.bind)
    }
}

impl MessageRouter {
    pub fn new(bind_to: String, bp_resolver: Arc<Mutex<dyn BPResolver>>) -> Self {
        let bind = bind_to.to_socket_addrs().expect("Incorrect address").next().unwrap();

        let bind_clone = bind;
        let _ = std::thread::Builder::new()
            .name("Message router".into())
            .spawn(move || {
                let _ = Self::run(bind_clone, bp_resolver);
            })
            .expect("Failed to spawn actix");

        Self { bind }
    }

    #[actix_web::main]
    async fn run(bind: SocketAddr, bp_resolver: Arc<Mutex<dyn BPResolver>>) -> anyhow::Result<()> {
        HttpServer::new(move || {
            let bp_resolver_clone = bp_resolver.clone();
            App::new()
                .wrap(middleware::DefaultHeaders::new().add(("Content-type", "application/json")))
                .configure(move |cfg| Self::config(cfg, bp_resolver_clone))
                .wrap(Logger::default())
        })
        .bind(bind)?
        .run()
        .await
        .map_err(anyhow::Error::from)?;

        tracing::info!(target: "message_router", "MessageRouter is running: bind={bind}");

        Ok(())
    }

    fn config(cfg: &mut web::ServiceConfig, bp_resolver: Arc<Mutex<dyn BPResolver>>) {
        cfg.service(
            // Process inbound external messages and forward its to the BP
            //
            // JSON: [{
            //         "id": String,
            //         "body": String,
            //         "expireAt"?: Int,
            //         "threadId"?: String
            //       }]
            web::resource(ROUTER_URL_PATH.to_string())
                .route(web::post().to(move |node_requests| {
                    let resolver_clone = bp_resolver.clone();
                    Self::process_ext_messages(node_requests, resolver_clone)
                }))
                .route(web::head().to(HttpResponse::MethodNotAllowed)),
        );
    }

    async fn process_ext_messages(
        node_requests: web::Json<serde_json::Value>,
        bp_resolver: Arc<Mutex<dyn BPResolver>>,
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

        tracing::info!(target: "message_router", "Ext messages received: {:?}", ids.iter().map(|(k, v)| format!("{v} ({k})")));

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
        let client =
            awc::Client::builder().timeout(Duration::from_secs(DEFAULT_BK_API_TIMEOUT)).finish();
        for recipient in recipients {
            let url = construct_url(recipient);
            tracing::info!(target: "message_router", "Forwarding requests to: {url}");
            result = match client
                .post(&url)
                .insert_header(("X-EXT-MSG-SENT", now_ms().to_string()))
                .send_json(&node_requests)
                .await
            {
                Ok(mut response) => {
                    let body = response.body().await;
                    let recipient = recipient.ip().to_string();
                    tracing::info!(target: "message_router", "response body (src={}): {:?}", recipient, body);
                    match body {
                        Ok(b) => {
                            let s = std::str::from_utf8(&b)?;
                            let response = serde_json::from_str::<serde_json::Value>(s)?;
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

fn construct_url(host: SocketAddr) -> String {
    format!("{DEFAULT_NODE_URL_PROTO}://{}:{}{}", host.ip(), DEFAULT_NODE_URL_PORT, *NODE_URL_PATH)
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::MessageRouter;
    use crate::bp_resolver::BPResolver;

    struct BPResolverHelper;

    impl BPResolver for BPResolverHelper {
        fn resolve(&mut self, _thread_id: Option<String>) -> Vec<std::net::SocketAddr> {
            vec!["0.0.0.0:8600".parse().unwrap()]
        }
    }

    #[test]
    #[ignore]
    fn test_running() {
        env_logger::builder().format_timestamp(None).init();

        let msg_router =
            MessageRouter::new("127.0.0.1:8700".into(), Arc::new(Mutex::new(BPResolverHelper)));
        tracing::debug!("{}", msg_router);

        loop {
            let _ = 0u8;
        }
    }

    #[test]
    fn test_bp_resolver() {
        let mut bp_resolver = BPResolverHelper;
        let bp_socket_addr = bp_resolver.resolve(None);

        assert_eq!(bp_socket_addr, vec!["0.0.0.0:8600".parse().unwrap()]);
    }
}
