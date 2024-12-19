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
            // Process inbound external messages and forward its to the BP
            //
            // JSON: [{
            //         "id": String,
            //         "boc": String,
            //         "expire"?: Int
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
    ) -> actix_web::Result<String> {
        tracing::trace!(target: "message_router", "Requests received: {}", node_requests);
        let recipients = bp_resolver.lock().resolve();
        tracing::trace!(target: "message_router", "Resolved BPs: {:?}", recipients);
        let mut result = "Ok. Nothing to do";
        for recipient in recipients {
            let url = construct_url(recipient);
            tracing::info!(target: "message_router", "Forwarding requests to: {url}");
            let client = awc::Client::default();
            result = match client.post(&url).send_json(&node_requests).await {
                Ok(_) => "Ok",
                Err(err) => {
                    tracing::error!(target: "message_router", "redirect to {url} error: {err}");
                    "Error"
                }
            }
        }

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
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::MessageRouter;
    use crate::bp_resolver::BPResolver;

    struct BPResolverHelper;

    impl BPResolver for BPResolverHelper {
        fn resolve(&mut self) -> Vec<std::net::SocketAddr> {
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
        let bp_socket_addr = bp_resolver.resolve();

        assert_eq!(bp_socket_addr, vec!["0.0.0.0:8600".parse().unwrap()]);
    }
}
