// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use actix_web::middleware::Logger;
use actix_web::web;
use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use parking_lot::Mutex;

use crate::bp_resolver::BPResolver;
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
        let bind = bind_to.to_socket_addrs().expect("Incorrect address: {bind_to}").next().unwrap();

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
    ) -> actix_web::Result<String> {
        tracing::trace!(target: "message_router", "Requests received: {}", node_requests);
        let recipient = bp_resolver.lock().resolve(None);
        tracing::trace!(target: "message_router", "Resolved BP: {:?}", recipient);
        let result = if let Some(recipient) = recipient {
            let url = construct_url(recipient);
            tracing::info!(target: "message_router", "Forwarding requests to: {url}");
            let client = awc::Client::default();
            match client.post(url).send_json(&node_requests).await {
                Ok(_) => "Ok",
                Err(err) => {
                    tracing::error!(target: "message_router", "redirect error: {err}");
                    "Error"
                }
            }
        } else {
            tracing::warn!(target: "message_router", "BP not found");
            "Ok. Nothing to do"
        };

        Ok(result.to_string())
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
    use crate::bp_resolver::BPResolver;

    struct BPResolverHelper;

    impl BPResolver for BPResolverHelper {
        fn resolve(&mut self, _node_id: Option<i32>) -> Option<std::net::SocketAddr> {
            Some("0.0.0.0:8600".parse().unwrap())
        }
    }

    #[test]
    fn test_bp_resolver() {
        let mut bp_resolver = BPResolverHelper;
        let bp_socket_addr = bp_resolver.resolve(None);

        assert_eq!(bp_socket_addr, Some("0.0.0.0:8600".parse().unwrap()));
    }
}
