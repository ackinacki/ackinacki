// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use actix_web::test;
use actix_web::web;
use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use message_router::message_router::MessageRouter;
use message_router::KeyPair;
use message_router::MockBPResolver;
use message_router::DEFAULT_BK_API_MESSAGES_PATH;
use message_router::DEFAULT_BM_API_MESSAGES_PATH;
use parking_lot::Mutex;
use serde_json::json;
use serde_json::Value;
use transport_layer::HostPort;

lazy_static::lazy_static!(
    static ref LICENSE_ADDRESS: Option<String> = std::env::var("LICENSE_ADDRESS").ok();
);

async fn actix_ext_messages_handler(
    node_requests: web::Json<serde_json::Value>,
    message_router: Arc<MessageRouter>,
) -> actix_web::Result<web::Json<serde_json::Value>> {
    match message_router::process_ext_messages::run(node_requests.into_inner(), message_router)
        .await
    {
        Ok(value) => {
            let v =
                serde_json::to_value(value).map_err(actix_web::error::ErrorInternalServerError)?;
            Ok(web::Json(v))
        }
        Err(e) => {
            tracing::error!("process_ext_messages_inner failed: {:?}", e);
            Err(actix_web::error::ErrorInternalServerError(e))
        }
    }
}

fn message_router_config(cfg: &mut web::ServiceConfig, message_router: Arc<MessageRouter>) {
    cfg.service(
        // Process inbound external messages and forward its to the BP
        web::resource(DEFAULT_BM_API_MESSAGES_PATH.to_string())
            .route(web::post().to(move |node_requests| {
                let message_router = message_router.clone();
                actix_ext_messages_handler(node_requests, message_router)
            }))
            .route(web::head().to(HttpResponse::MethodNotAllowed)),
    );
}

fn mock_block_producer_server() -> HostPort {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind mock server");
    let addr = HostPort::from(listener.local_addr().unwrap());

    let _ = std::thread::Builder::new().name("Mocked_BK_API".into()).spawn(move || {
        let server = HttpServer::new(|| {
            App::new().route(DEFAULT_BK_API_MESSAGES_PATH, web::post().to(|| async {
                HttpResponse::Ok().json(serde_json::json!({
                    "result": {
                        "message_hash": "e01f2741c00d1439b55ac1637a0f18994c8ce4957efbcaf4be8b59a5b7b1ebb2",
                        "block_hash": "019a26d3f408e55ea6f687a4a98b9083cd731f030e7915f2ec60871e5509a7a6",
                        "tx_hash": "25d7ace13a6b288a564cd6447b2d2b9f914d41db1ac3c9ff17e96871636ec328",
                        "ext_out_msgs": [],
                        "aborted": false,
                        "exit_code": 0,
                        "producers": ["127.0.0.1:11002"],
                        "current_time": "1746544513335",
                        "thread_id": "00000000000000000000000000000000000000000000000000000000000000000000",
                    },
                    "error": null
                }))
            }))
        })
        .listen(listener)
        .expect("Failed to start test HTTP server")
        .run();

        let _ = actix_web::rt::System::new().block_on(server);
    });

    std::thread::sleep(Duration::from_millis(200));

    addr
}

fn get_keys(v: &serde_json::Value) -> std::collections::HashSet<String> {
    v.as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default()
}

#[actix_rt::test]
async fn test_process_ext_messages_no_bp() {
    let mut mock = MockBPResolver::new();
    mock.expect_resolve().return_const(vec![]);
    let resolver = Arc::new(Mutex::new(mock));

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let message_router = MessageRouter {
        bind,
        owner_wallet_pubkey: None,
        signing_keys: None,
        bp_resolver: resolver,
    };

    let app = test::init_service(
        App::new().configure(|cfg| message_router_config(cfg, Arc::new(message_router))),
    )
    .await;

    let req = test::TestRequest::post()
        .uri(DEFAULT_BM_API_MESSAGES_PATH)
        .set_json(json!([{
            "id": "aGVsbG8=",
            "body": "test"
        }]))
        .to_request();

    let resp: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(resp["result"], Value::Null);
    assert_eq!(
        resp["error"],
        json!({
            "code": "INTERNAL_ERROR",
            "data": null,
            "message": "Failed to obtain any Block Producer addresses",
        })
    );
}

#[actix_rt::test]
async fn test_process_ext_messages_invalid_json() {
    let mut mock = MockBPResolver::new();
    mock.expect_resolve().return_const(vec![]);
    let resolver = Arc::new(Mutex::new(mock));

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let message_router = MessageRouter {
        bind,
        owner_wallet_pubkey: None,
        signing_keys: None,
        bp_resolver: resolver,
    };

    let app = test::init_service(
        App::new().configure(|cfg| message_router_config(cfg, Arc::new(message_router))),
    )
    .await;

    let req = test::TestRequest::post()
        .uri(DEFAULT_BM_API_MESSAGES_PATH)
        .set_json(json!({"bad": "data"}))
        .to_request();

    let resp: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(resp["result"], Value::Null);
    assert_eq!(
        resp["error"],
        json!({
            "code": "BAD_REQUEST",
            "data": null,
            "message": "Incorrect request",
        })
    );
}

#[actix_rt::test]
async fn test_process_ext_messages_redirection_failed() {
    let mut mock = MockBPResolver::new();
    let addr = "127.0.0.1:18600".parse().expect("failed to parse socket address");
    mock.expect_resolve().return_const(vec![addr]);
    let resolver = Arc::new(Mutex::new(mock));

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let message_router = MessageRouter {
        bind,
        owner_wallet_pubkey: None,
        signing_keys: None,
        bp_resolver: resolver,
    };

    let app = test::init_service(
        App::new().configure(|cfg| message_router_config(cfg, Arc::new(message_router))),
    )
    .await;

    let req = test::TestRequest::post()
        .uri(DEFAULT_BM_API_MESSAGES_PATH)
        .set_json(json!([{
            "id": "aGVsbG8=",
            "body": "test"
        }]))
        .to_request();

    let resp: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(resp["result"], Value::Null);
    assert_eq!(resp["error"]["code"], "INTERNAL_ERROR");
    assert_eq!(
        get_keys(&resp["error"]["data"]),
        ["current_time", "exit_code", "message_hash", "producers", "thread_id"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .starts_with("The message redirection to the Block Producer has failed"));
}

#[actix_rt::test]
async fn test_process_ext_messages_successful_forward() {
    let bp_addr = mock_block_producer_server();

    let mut mock = MockBPResolver::new();
    mock.expect_resolve().return_const(vec![bp_addr.clone()]);
    let resolver = Arc::new(Mutex::new(mock));

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let owner_wallet_pubkey =
        "e2c9d4be54d342d3f0e6394a7738fc39b93d4fe3fdba317aa699f7305566de2b".to_string();
    let keys = KeyPair {
        public: "61024bc92e0828896274d28cfaab60163e8c576299292008f8be67e7d6513a79".into(),
        secret: "2297a2052530b36c952441e34e1e298314d830849d2ac819ddd8e59b116b8674".into(),
    };
    let message_router = MessageRouter {
        bind,
        owner_wallet_pubkey: Some(owner_wallet_pubkey.clone()),
        signing_keys: Some(keys),
        bp_resolver: resolver,
    };
    let app = test::init_service(
        App::new().configure(|cfg| message_router_config(cfg, Arc::new(message_router))),
    )
    .await;

    let req = test::TestRequest::post()
        .uri(DEFAULT_BM_API_MESSAGES_PATH)
        .set_json(json!([{
            "id": "aGVsbG8=",
            "body": "test body"
        }]))
        .to_request();

    let resp: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(resp["error"], Value::Null);
    assert_eq!(
        get_keys(&resp["result"]),
        [
            "aborted",
            "block_hash",
            "current_time",
            "exit_code",
            "ext_out_msgs",
            "message_hash",
            "producers",
            "thread_id",
            "tx_hash"
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    );
    assert_eq!(
        get_keys(&resp["ext_message_token"]),
        ["issuer", "unsigned", "signature"].iter().map(|s| s.to_string()).collect()
    );
    assert_eq!(resp["ext_message_token"]["issuer"], json!({ "bm": owner_wallet_pubkey }));
}
