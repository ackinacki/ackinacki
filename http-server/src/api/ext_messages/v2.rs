// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;

use salvo::prelude::*;
use salvo::Depot;
use salvo::Response;
use serde_json::Value;
use tokio::sync::oneshot;
use tvm_block::Deserializable;
use tvm_block::Message;
use tvm_types::base64_decode;
use tvm_types::UInt256;

use super::ExtMsgErrorData;
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
    TBPResolver: Clone + Send + Sync + 'static + FnMut([u8; 34]) -> Option<std::net::SocketAddr>,
{
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        tracing::info!(target: "http_server", "Rest service: request got!");

        let Ok(mut web_server_state) =
            depot.obtain::<WebServer<TMessage, TMsgConverter, TBPResolver>>()
        else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            render_error_response(
                res,
                "INTERNAL_SERVER_ERROR",
                Some("Web Server state is not found"),
                None,
            );
            return;
        };

        if let Ok(body) = req.parse_json::<Value>().await {
            if let Some(records) = body.as_array() {
                tracing::trace!(target: "http_server", "Process request records len: {}", records.len());
                // API v2 accepts only one message at a time
                let records = &mut records.clone();
                records.drain(1..);
                for record in records {
                    let msg_id = record
                        .as_object()
                        .and_then(|record| record.get("id"))
                        .and_then(|val| val.as_str());

                    let boc = record
                        .as_object()
                        .and_then(|record| record.get("boc"))
                        .and_then(|val| val.as_str());
                    if let Some(key) = msg_id {
                        if let Some(value) = boc {
                            tracing::trace!(target: "http_server", "Process request record");
                            let message = match parse_message(key, value) {
                                Ok(m) => m,
                                Err(err) => {
                                    tracing::warn!(target: "http_server", "Error parsing message: {}", err);

                                    res.status_code(StatusCode::BAD_REQUEST);
                                    render_error_response(
                                        res,
                                        "BAD_REQUEST",
                                        Some(format!("Error parsing message: {}", err).as_str()),
                                        None,
                                    );
                                    return;
                                }
                            };
                            let convert = &web_server_state.into_external_message;
                            let thread_id = record
                                .as_object()
                                .and_then(|record| record.get("thread_id"))
                                .and_then(|val| val.as_str())
                                .map_or(ThreadIdentifier::default(), |s| {
                                    s.to_string()
                                        .try_into()
                                        .ok()
                                        .unwrap_or(ThreadIdentifier::default())
                                });
                            let wrapped_message: TMessage = match convert(message, thread_id.into())
                            {
                                Ok(e) => e,
                                Err(e) => {
                                    tracing::warn!(target: "http_server", "Error queue message. Message was not accepted: {}", e);

                                    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                                    render_error_response(
                                        res,
                                        "INTERNAL_SERVER_ERROR",
                                        Some("Message was not accepted"),
                                        None,
                                    );
                                    return;
                                }
                            };
                            tracing::trace!(target: "http_server", "Process request send message: {:?}", wrapped_message);
                            let (feedback_sender, feedback_receiver) = oneshot::channel();
                            let external_message = (wrapped_message, Some(feedback_sender));
                            match web_server_state.incoming_message_sender.send(external_message) {
                                Ok(()) => match feedback_receiver.await {
                                    Ok(resp) => {
                                        tracing::trace!(target: "http_server", "Process request send message result Ok(): {:?}", resp);
                                        let producers = match resp.thread_id {
                                            Some(thread_id) => {
                                                let web_server_mut = &mut web_server_state;
                                                let mut resolver =
                                                    web_server_mut.bp_resolver.clone();
                                                resolver(thread_id)
                                                    .map_or(vec![], |addr| vec![addr.to_string()])
                                            }
                                            _ => vec![],
                                        };
                                        let mut result: ExtMsgResponse = resp.into();
                                        result.set_producers(producers);
                                        tracing::trace!(target: "http_server", "Response message: {:?}", result);
                                        res.status_code(StatusCode::OK);
                                        res.render(Json(result));
                                        return;
                                    }
                                    Err(e) => {
                                        tracing::warn!(target: "http_server", "Error queue message: {}", e);
                                        res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                                        render_error_response(
                                            res,
                                            "INTERNAL_SERVER_ERROR",
                                            Some("The status of message execution is unknown"),
                                            None,
                                        );
                                        return;
                                    }
                                },
                                Err(e) => {
                                    tracing::warn!(target: "http_server", "Error queue message: {}", e);
                                    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                                    render_error_response(
                                        res,
                                        "INTERNAL_SERVER_ERROR",
                                        Some("Node does not accept messages"),
                                        None,
                                    );
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }

        tracing::warn!(target: "http_server", "Error parsing request's body");
        res.status_code(StatusCode::BAD_REQUEST);
        render_error_response(res, "BAD_REQUEST", Some("Error parsing request's body"), None);
    }
}

fn parse_message(id_b64: &str, message_b64: &str) -> Result<Message, String> {
    tracing::trace!(target: "http_server", "parse_message {id_b64}");
    let message_bytes = base64_decode(message_b64)
        .map_err(|error| format!("Error decoding base64-encoded message: {}", error))?;

    let id_bytes = base64_decode(id_b64)
        .map_err(|error| format!("Error decoding base64-encoded message's id: {}", error))?;

    let id = UInt256::from_be_bytes(&id_bytes);

    let message_cell = tvm_types::boc::read_single_root_boc(message_bytes).map_err(|error| {
        tracing::error!(target: "http_server", "Error deserializing message: {}", error);
        format!("Error deserializing message: {}", error)
    })?;

    if message_cell.repr_hash() != id {
        return Err("Error: calculated message's hash doesn't correspond given key".to_string());
    }

    Message::construct_from_cell(message_cell)
        .map_err(|error| format!("Error parsing message's cells tree: {}", error))
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
