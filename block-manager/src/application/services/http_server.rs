use std::net::SocketAddr;
use std::sync::Arc;

use message_router::process_ext_messages;
use reqwest::Client;
use salvo::conn::TcpListener;
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

    // Construct request to node
    let base_url = format!("http://{}/v2/account", state.default_bp);
    let resp = Client::new()
        .get(format!("{base_url}?address={address}"))
        .bearer_auth(state.bk_api_token.clone())
        .send()
        .await;

    match resp {
        Ok(original_resp) => {
            // Skip headers forwarding
            // Forward status code
            res.status_code(original_resp.status());

            // Forward response body
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
                    tracing::error!("Failed to read response body: {}", e);
                    render_error(
                        res,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal server error: failed to read response body",
                    )
                }
            }
        }
        Err(e) => {
            tracing::error!("API request to the default BK failed: {}", e);
            render_error(res, StatusCode::BAD_GATEWAY, "API request failed");
        }
    }
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
