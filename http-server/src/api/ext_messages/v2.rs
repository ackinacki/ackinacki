// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;
use std::time::Instant;

use ext_messages_auth::auth::is_auth_required;
use ext_messages_auth::auth::TokenVerificationResult;
use salvo::prelude::*;
use salvo::Depot;
use salvo::Response;
use telemetry_utils::millis_from_now;
use tokio::sync::oneshot;

use super::ExtMsgErrorData;
use super::ResolvingResult;
use crate::api::ext_messages::render_error;
use crate::api::ext_messages::render_error_response;
use crate::api::ext_messages::ExtMsgResponse;
use crate::helpers::extract_ext_msg_sent_time;
use crate::ExternalMessage;
use crate::WebServer;

pub struct ExtMessagesHandler<TMesssage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>(
    PhantomData<TMesssage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
    PhantomData<TBocByAddrGetter>,
    PhantomData<TSeqnoGetter>,
);

impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
    ExtMessagesHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter> Handler
    for ExtMessagesHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
where
    TMessage: Clone + Send + Sync + 'static + std::fmt::Debug,
    TMsgConverter: Clone
        + Send
        + Sync
        + 'static
        + Fn(tvm_block::Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Clone + Send + Sync + 'static + FnMut([u8; 34]) -> ResolvingResult,
    TBocByAddrGetter:
        Clone + Send + Sync + 'static + Fn(String) -> anyhow::Result<(String, Option<String>)>,
    TSeqnoGetter: Clone + Send + Sync + 'static + Fn() -> anyhow::Result<u32>,
{
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        tracing::info!(target: "http_server", "Rest service: request got!");
        let moment = Instant::now();
        let Ok(mut web_server) = depot.obtain::<WebServer<
            TMessage,
            TMsgConverter,
            TBPResolver,
            TBocByAddrGetter,
            TSeqnoGetter,
        >>() else {
            return render_error(
                res,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Web Server state not found",
                None,
            );
        };

        let message = depot.get::<ExternalMessage>("message").unwrap();
        tracing::trace!("processing ext message: {message:?}");

        let authorized_by_bearer_token =
            depot.get::<bool>(crate::AUTHORIZED_BY_BK_KEY).copied().unwrap_or(false);

        if is_auth_required() && !authorized_by_bearer_token {
            tracing::debug!("Ext message authorization required");
            let Some(ref token) = message.ext_message_token else {
                tracing::debug!("Ext message authorization failed: token not found");
                return render_error_response(
                    res,
                    "BAD_TOKEN",
                    Some("Ext message auth token not found"),
                    None,
                    None,
                );
            };

            let account_request_tx = web_server.signing_pubkey_request_senber.clone();
            match token.authorize(account_request_tx).await {
                TokenVerificationResult::Ok => {
                    tracing::debug!("Token verification passed");
                }
                TokenVerificationResult::TokenMalformed => {
                    tracing::debug!("Token verification failed: malformed");
                    return render_error_response(
                        res,
                        "BAD_TOKEN",
                        Some("BM token is malformed"),
                        None,
                        None,
                    );
                }
                TokenVerificationResult::InvalidSignature => {
                    tracing::debug!("Token verification failed: invalid signature");
                    return render_error_response(
                        res,
                        "INVALID_SIGNATURE",
                        Some("BM signature validation failed"),
                        None,
                        None,
                    );
                }
                TokenVerificationResult::Expired => {
                    tracing::debug!("Token verification failed: expired");
                    return render_error_response(
                        res,
                        "TOKEN_EXPIRED",
                        Some("BM token expired"),
                        None,
                        None,
                    );
                }
            }
        }

        tracing::debug!(target: "http_server", "Incomming ext message: {message:?}");

        let bk_auth_token = if authorized_by_bearer_token {
            let bk_auth_token = web_server.issue_token();
            tracing::debug!(target: "http_server", "Issued bk token: {bk_auth_token:?}");
            bk_auth_token
        } else {
            None
        };

        let web_server_mut = &mut web_server;
        let mut resolver = web_server_mut.bp_resolver.clone();
        let resolving_result = resolver(message.thread_id().into());
        tracing::trace!(
            target: "http_server",
            "Resolved BPs: {:?} for thread {:?}",
            resolving_result,
            message.thread_id(),
        );
        if !resolving_result.i_am_bp {
            let message_hash = message.hash();
            render_error_response(
                res,
                "WRONG_PRODUCER",
                Some("Resend message to the active Block Producer"),
                Some(ExtMsgErrorData::new(
                    resolving_result.active_bp,
                    message_hash,
                    None,
                    Some(format!("{:?}", message.thread_id())),
                )),
                bk_auth_token,
            );
            return;
        };

        let convert = &web_server.into_external_message;
        let wrapped_message: TMessage = match convert(
            message.tvm_message(),
            message.thread_id().into(),
        ) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(target: "http_server", "Error queue message. Message was not accepted: {}", e);
                return render_error(
                    res,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Message was not accepted",
                    bk_auth_token,
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
            web_server.metrics.as_ref().inspect(|m| {
                m.report_ext_msg_processing_duration(
                    moment.elapsed().as_millis() as u64,
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                )
            });
            return render_error(
                res,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Node does not accept messages",
                bk_auth_token,
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
                web_server.metrics.as_ref().inspect(|m| {
                    m.report_ext_msg_processing_duration(
                        moment.elapsed().as_millis() as u64,
                        StatusCode::OK.as_u16(),
                    )
                });
                return;
            }
            Err(e) => {
                tracing::warn!(target: "http_server", "Error queue message: {}", e);
                web_server.metrics.as_ref().inspect(|m| {
                    m.report_ext_msg_processing_duration(
                        moment.elapsed().as_millis() as u64,
                        StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    )
                });
                return render_error(
                    res,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "The status of message execution is unknown",
                    bk_auth_token,
                );
            }
        }
    }
}
