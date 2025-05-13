// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;

use salvo::prelude::*;
use salvo::Depot;
use salvo::Response;
use serde_json::Value;
use telemetry_utils::millis_from_now;
use tokio::sync::oneshot;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;
use tvm_types::base64_decode;

use super::ExtMsgErrorData;
use super::ResolvingResult;
use crate::api::ext_messages::ExtMsgError;
use crate::api::ext_messages::ExtMsgResponse;
use crate::api::ext_messages::ThreadIdentifier;
use crate::WebServer;

pub struct ExtMessagesHandler<TMesssage, TMsgConverter, TBPResolver>(
    PhantomData<TMesssage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
);

impl<TMessage, TMsgConverter, TBPResolver>
    ExtMessagesHandler<TMessage, TMsgConverter, TBPResolver>
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver> Handler
    for ExtMessagesHandler<TMessage, TMsgConverter, TBPResolver>
where
    TMessage: Clone + Send + Sync + 'static + std::fmt::Debug,
    TMsgConverter: Clone
        + Send
        + Sync
        + 'static
        + Fn(tvm_block::Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Clone + Send + Sync + 'static + FnMut([u8; 34]) -> ResolvingResult,
{
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        tracing::info!(target: "http_server", "Rest service: request got!");

        let Ok(mut web_server) = depot.obtain::<WebServer<TMessage, TMsgConverter, TBPResolver>>()
        else {
            return render_error(
                res,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Web Server state not found",
            );
        };

        let Ok(body) = req.parse_json::<Value>().await else {
            tracing::warn!(target: "http_server", "Error parsing request's body");
            return render_error(res, StatusCode::BAD_REQUEST, "Invalid request body");
        };

        let Some(records) = body.as_array().filter(|r| !r.is_empty()) else {
            return render_error(res, StatusCode::BAD_REQUEST, "Empty request");
        };

        let web_server_mut = &mut web_server;
        let mut resolver = web_server_mut.bp_resolver.clone();
        // API v2 accepts only one message at a time
        let record = &records[0];
        let msg_id =
            record.as_object().and_then(|record| record.get("id")).and_then(|val| val.as_str());

        let body =
            record.as_object().and_then(|record| record.get("body")).and_then(|val| val.as_str());

        let (Some(key), Some(value)) = (msg_id, body) else {
            return render_error(res, StatusCode::BAD_REQUEST, "Missing message id or body");
        };

        tracing::trace!(target: "http_server", "Process request record: {msg_id:?}");

        let message = match parse_message(key, value) {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!(target: "http_server", "Error parsing message (msg_id:?): {}", err);
                return render_error(
                    res,
                    StatusCode::BAD_REQUEST,
                    format!("Error parsing message: {}", err).as_str(),
                );
            }
        };

        let convert = &web_server.into_external_message;
        let thread_id_str = record.get("thread_id").and_then(Value::as_str);
        let thread_id = thread_id_str
            .map_or_else(ThreadIdentifier::default, |s| {
                s.to_string().try_into().unwrap_or_default()
            })
            .into();

        let resolving_result = resolver(thread_id);
        tracing::trace!(
            target: "http_server",
            "Resolved BPs: {:?} for thread {:?}",
            resolving_result,
            thread_id_str,
        );
        if !resolving_result.i_am_bp {
            let message_hash = message.hash().map(|h| h.to_hex_string()).unwrap_or("".to_string());
            res.status_code(StatusCode::BAD_REQUEST);
            render_error_response(
                res,
                "WRONG_PRODUCER",
                Some("Resend message to the active Block Producer"),
                Some(ExtMsgErrorData::new(
                    resolving_result.active_bp,
                    message_hash,
                    None,
                    thread_id_str.map(|s| s.to_string()),
                )),
            );
            return;
        };
        let wrapped_message: TMessage = match convert(message, thread_id) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(target: "http_server", "Error queue message. Message was not accepted: {}", e);
                return render_error(
                    res,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Message was not accepted",
                );
            }
        };

        if let Some(started) = extract_ext_msg_sent_time(req.headers()) {
            match millis_from_now(started) {
                Ok(millis) => {
                    web_server
                        .metrics
                        .as_ref()
                        .inspect(|m| m.report_ext_msg_delivery_duration(millis));
                }
                Err(e) => {
                    tracing::error!(target: "http_server", "Error calculating message delivery duration: {}", e);
                }
            }
        } else {
            tracing::trace!(target: "http_server", "X-EXT-MSG-SENT header is missing");
        }

        tracing::trace!(target: "http_server", "Process request send message: {:?}", wrapped_message);
        let (feedback_sender, feedback_receiver) = oneshot::channel();

        if let Err(e) = web_server
            .incoming_message_sender
            .clone()
            .send((wrapped_message, Some(feedback_sender)))
        {
            tracing::warn!(target: "http_server", "Error queue message: {}", e);
            return render_error(
                res,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Node does not accept messages",
            );
        }
        match feedback_receiver.await {
            Ok(feedback) => {
                tracing::trace!(target: "http_server", "Process request send message result Ok(): {}", feedback);
                let producers = match feedback.thread_id {
                    Some(thread_id) => resolver(thread_id).active_bp,
                    _ => vec![],
                };
                let mut result: ExtMsgResponse = feedback.into();
                result.set_producers(producers);
                tracing::trace!(target: "http_server", "Response message: {:?}", result);
                res.status_code(StatusCode::OK);
                res.render(Json(result));
                return;
            }
            Err(e) => {
                tracing::warn!(target: "http_server", "Error queue message: {}", e);
                return render_error(
                    res,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "The status of message execution is unknown",
                );
            }
        }
    }
}

fn parse_message(id: &str, message_b64: &str) -> Result<Message, String> {
    tracing::trace!(target: "http_server", "parse_message {id}");
    let message_bytes = base64_decode(message_b64)
        .map_err(|e| format!("Error decoding base64-encoded message: {}", e))?;

    let message_cell = tvm_types::boc::read_single_root_boc(message_bytes).map_err(|e| {
        tracing::error!(target: "http_server", "Error deserializing message: {}", e);
        format!("Error deserializing message: {}", e)
    })?;

    Message::construct_from_cell(message_cell)
        .map_err(|e| format!("Error parsing message's cells tree: {}", e))
}

fn render_error_response(
    res: &mut Response,
    code: &str,
    message: Option<&str>,
    data: Option<ExtMsgErrorData>,
) {
    res.render(Json(ExtMsgResponse {
        result: None,
        error: Some(ExtMsgError {
            code: code.to_string(),
            message: message.unwrap_or(code).to_string(),
            data,
        }),
    }));
}

fn render_error(res: &mut Response, status: StatusCode, message: &str) {
    res.status_code(status);
    res.render(Json(ExtMsgResponse {
        result: None,
        error: Some(ExtMsgError {
            code: status.to_string(),
            message: message.to_string(),
            data: None,
        }),
    }));
}

fn extract_ext_msg_sent_time(headers: &salvo::http::HeaderMap) -> Option<u64> {
    headers.get("X-EXT-MSG-SENT")?.to_str().ok()?.parse().ok()
}
