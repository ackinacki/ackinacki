use std::net::SocketAddr;
use std::sync::Arc;

use message_router::process_ext_messages;
use reqwest::Client;
use salvo::conn::TcpListener;
use salvo::logging::Logger;
use salvo::prelude::*;
use salvo::Listener;
use salvo::Router;
use salvo::Server;
use tvm_types::AccountId;

use crate::application::usecases;
use crate::domain::models::AppState;

// REST API server (get accounts, external_messages, health checks)
pub async fn run(
    bind_socker: SocketAddr,
    app_state: Arc<AppState>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let acceptor = TcpListener::new(bind_socker).try_bind().await?;
    let server = Server::new(acceptor);
    let router = Router::new()
        .path("v2")
        .hoop(Logger::new())
        .hoop(log_traceparent)
        .hoop(affix_state::inject(app_state))
        .push(Router::with_path("account").get(boc_by_address))
        .push(Router::with_path("readiness").get(readiness))
        .push(Router::with_path("messages").post(ext_message));

    Ok(tokio::spawn(async move {
        server.serve(router).await;
    }))
}

#[handler]
async fn ext_message(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Ok(state) = depot.obtain::<Arc<AppState>>() else {
        tracing::error!("Can't obtain internal state");
        render_error(res, StatusCode::INTERNAL_SERVER_ERROR, "Internal error");
        return;
    };

    let message_router = state.message_router.clone();

    let body = match req.parse_json::<serde_json::Value>().await {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("Failed to parse request body: {}", e);
            return render_error(res, StatusCode::BAD_REQUEST, "Invalid JSON body");
        }
    };

    process_ext_messages::run(body, message_router)
        .await
        .map(|response| {
            res.status_code(StatusCode::OK);
            res.render(Json(response));
        })
        .unwrap_or_else(|e| {
            tracing::error!("Failed to process external messages: {}", e);
            render_error(res, StatusCode::INTERNAL_SERVER_ERROR, "Internal server error");
        });
}

#[handler]
async fn log_traceparent(
    req: &mut Request,
    res: &mut Response,
    depot: &mut Depot,
    ctrl: &mut FlowCtrl,
) {
    if let Some(traceparent) = req.headers().get("traceparent") {
        match traceparent.to_str() {
            Ok(value) => tracing::debug!(id = value, "Incoming request traceparent"),
            Err(_) => {
                tracing::debug!(id = ?traceparent, "Incoming request traceparent (non-UTF8)")
            }
        }
    }

    ctrl.call_next(req, depot, res).await;
}

#[handler]
async fn boc_by_address(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Ok(state) = depot.obtain::<Arc<AppState>>() else {
        tracing::error!("Can't obtain internal state");
        render_error(res, StatusCode::INTERNAL_SERVER_ERROR, "Internal error");
        return;
    };
    let address = match req.query::<String>("address") {
        Some(addr) if !addr.trim().is_empty() => addr.trim_start_matches("0:").to_string(),
        _ => {
            return render_error(res, StatusCode::BAD_REQUEST, "Address parameter required");
        }
    };
    // Check if address is valid
    if AccountId::from_string(&address).is_err() {
        return render_error(res, StatusCode::BAD_REQUEST, "Invalid address");
    }

    // Try endpoints with failover
    let endpoints = state.bk_api_pool.endpoints_to_try();
    if endpoints.is_empty() {
        render_error(res, StatusCode::BAD_GATEWAY, "No BK API endpoints configured");
        return;
    }

    let client = Client::new();
    let mut last_error = None;

    for endpoint in &endpoints {
        let url = format!("http://{endpoint}/v2/account?address={address}");
        let resp = client.get(&url).bearer_auth(&state.bk_api_token).send().await;

        match resp {
            Ok(original_resp) if original_resp.status().is_server_error() => {
                tracing::warn!("BK {endpoint} returned {}, trying next", original_resp.status());
                last_error = Some(format!("BK {endpoint} returned {}", original_resp.status()));
                continue;
            }
            Ok(original_resp) => {
                state.bk_api_pool.promote(endpoint);
                res.status_code(original_resp.status());
                match original_resp.bytes().await {
                    Ok(body) => {
                        if let Err(e) = res.write_body(body) {
                            tracing::error!("Can't write body: {}", e);
                            render_error(
                                res,
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "Internal server error: failed to write body",
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to read response body from {endpoint}: {e}");
                        render_error(
                            res,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Internal server error: failed to read response body",
                        );
                    }
                }
                return;
            }
            Err(e) => {
                tracing::warn!("BK {endpoint} request failed: {e}, trying next");
                last_error = Some(format!("BK {endpoint}: {e}"));
                continue;
            }
        }
    }

    // All endpoints failed
    let err_msg = last_error.unwrap_or_else(|| "all endpoints failed".to_string());
    tracing::error!("All BK API endpoints failed. Last error: {err_msg}");
    render_error(res, StatusCode::BAD_GATEWAY, "All BK API endpoints failed");
}

#[handler]
async fn readiness(_req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Ok(state) = depot.obtain::<Arc<AppState>>() else {
        tracing::error!("Can't obtain internal state");
        render_error(res, StatusCode::INTERNAL_SERVER_ERROR, "Internal error");
        return;
    };

    let output = usecases::readiness::exec(state);

    res.status_code(if output.is_ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE });
    res.render(output.reason);
}

fn render_error(res: &mut Response, status_code: StatusCode, text: &str) {
    res.status_code(status_code);
    res.render(text.to_owned());
}
