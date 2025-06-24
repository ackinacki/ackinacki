use std::net::SocketAddr;

use http_server::auth;
use salvo::prelude::*;
use tvm_types::AccountId;

use crate::license_root::build_fetch_boc_request;

#[derive(Debug, Clone)]
pub struct AppState {
    pub default_bp: SocketAddr,
}

pub fn rest_api_router(default_bp: SocketAddr) -> Router {
    let app_state = AppState { default_bp };
    Router::new()
        .path("bm")
        .hoop(auth)
        .hoop(affix_state::inject(app_state))
        .push(Router::with_path("account").get(boc_by_address))
}

#[handler]
async fn boc_by_address(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Ok(state) = depot.obtain::<AppState>() else {
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

    let boc_request = build_fetch_boc_request(state.default_bp, &address);
    let resp = boc_request.send().await;

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

fn render_error(res: &mut Response, status_code: StatusCode, text: &str) {
    res.status_code(status_code);
    res.render(text.to_owned());
}
