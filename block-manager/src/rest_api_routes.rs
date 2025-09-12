// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::atomic::Ordering;
use std::sync::Arc;

use message_router::process_ext_messages;
use salvo::prelude::*;
use telemetry_utils::now_ms;
use tvm_types::AccountId;

use crate::bm_contract_root::build_fetch_boc_request;
use crate::executor::AppState;

const HEALTH_GEN_UTIME_DIFF_SEC: u64 = 5;

pub fn rest_api_router(app_state: Arc<AppState>) -> Router {
    Router::new()
        .path("v2")
        // No auth required
        // .hoop(auth)
        .hoop(affix_state::inject(app_state))
        .push(Router::with_path("account").get(boc_by_address))
        .push(Router::with_path("readiness").get(readiness))
        .push(Router::with_path("messages").post(route_message))
}

#[handler]
async fn route_message(req: &mut Request, depot: &mut Depot, res: &mut Response) {
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

#[handler]
async fn readiness(_req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Ok(state) = depot.obtain::<Arc<AppState>>() else {
        tracing::error!("Can't obtain internal state");
        render_error(res, StatusCode::INTERNAL_SERVER_ERROR, "Internal error");
        return;
    };

    let last_block_gen_utime = state.last_block_gen_utime.load(Ordering::Relaxed);
    let now = now_ms();
    let diff = now.saturating_sub(last_block_gen_utime);
    let ready = diff < HEALTH_GEN_UTIME_DIFF_SEC * 1000;
    res.status_code(if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE });
    res.render(format!(
        "ready = {ready}, diff = {diff}, now = {now}, last_block_gen_utime = {last_block_gen_utime}"
    ));
}

fn render_error(res: &mut Response, status_code: StatusCode, text: &str) {
    res.status_code(status_code);
    res.render(text.to_owned());
}
