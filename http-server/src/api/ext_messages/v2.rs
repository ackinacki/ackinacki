// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;
use std::time::Instant;

use ext_messages_auth::auth::is_auth_required;
use ext_messages_auth::auth::TokenVerificationResult;
use node_types::ThreadIdentifier;
use salvo::prelude::*;
use salvo::Depot;
use salvo::Response;
use telemetry_utils::millis_from_now;
use tokio::sync::oneshot;

use super::ExtMsgErrorData;
use super::ResolvingResult;
use crate::api::ext_messages::message::NotQueuedExtMessage;
use crate::api::ext_messages::render_error;
use crate::api::ext_messages::render_error_response;
use crate::api::ext_messages::ExtMsgResponse;
use crate::helpers::extract_ext_msg_sent_time;
use crate::WebServer;

pub struct ExtMessagesHandler<TBPResolver, TSeqnoGetter>(
    PhantomData<TBPResolver>,
    PhantomData<TSeqnoGetter>,
);

impl<TBPResolver, TSeqnoGetter> ExtMessagesHandler<TBPResolver, TSeqnoGetter> {
    pub fn new() -> Self {
        Self(PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TBPResolver, TSeqnoGetter> Handler for ExtMessagesHandler<TBPResolver, TSeqnoGetter>
where
    TBPResolver: Clone + Send + Sync + 'static + FnMut(ThreadIdentifier) -> ResolvingResult,
    TSeqnoGetter: Clone + Send + Sync + 'static + Fn() -> anyhow::Result<u32>,
{
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let moment = Instant::now();
        let Ok(web_server) = depot.obtain::<WebServer<TBPResolver, TSeqnoGetter>>() else {
            return render_error(
                res,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Web Server state not found",
                None,
            );
        };

        let message = match depot.get::<NotQueuedExtMessage>("message") {
            Ok(m) => m,
            Err(_) => {
                tracing::debug!(target: "http_server", "External message not found or has wrong type in depot");
                return render_error(
                    res,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "External message has been lost",
                    None,
                );
            }
        };

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

            let account_request_tx = web_server.account_request_sender.clone();
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
                TokenVerificationResult::UnauthorizedIssuer => {
                    tracing::debug!(
                        "Token verification failed: unauthorized issuer (no delegated licenses)"
                    );
                    return render_error_response(
                        res,
                        "UNAUTHORIZED_ISSUER",
                        Some("Missing delegated licenses"),
                        None,
                        None,
                    );
                }
                TokenVerificationResult::UnknownIssuer => {
                    tracing::debug!("Token verification failed: unknown issuer");
                    return render_error_response(
                        res,
                        "UNKNOWN_ISSUER",
                        Some("Invalid issuer"),
                        None,
                        None,
                    );
                }
                TokenVerificationResult::IssuerResolutionFailed => {
                    tracing::debug!("Token verification failed: issuer resolution error");
                    return render_error_response(
                        res,
                        "ISSUER_RESOLUTION_FAILED",
                        Some("Failed to resolve token issuer"),
                        None,
                        None,
                    );
                }
                TokenVerificationResult::MissingSigningKey => {
                    tracing::debug!("Token verification failed: issuer wallet has no signing key");
                    return render_error_response(
                        res,
                        "MISSING_SIGNING_KEY",
                        Some("Issuer wallet has no signing key configured"),
                        None,
                        None,
                    );
                }
            }
        }

        let bk_auth_token = if authorized_by_bearer_token {
            let bk_auth_token = web_server.issue_token();
            tracing::debug!(target: "http_server", "Issued bk token: {bk_auth_token:?}");
            bk_auth_token
        } else {
            None
        };

        // Check that current BP is right one for the message thread_id
        let mut resolver = web_server.bp_resolver.clone();

        let resolving_result = resolver(message.thread_id());
        tracing::trace!(
            target: "http_server",
            "Resolved BPs: {:?} for thread {:?}",
            resolving_result,
            message.thread_id(),
        );
        if !resolving_result.i_am_bp {
            render_error_response(
                res,
                "WRONG_PRODUCER",
                Some("Resend message to the active Block Producer"),
                Some(ExtMsgErrorData::new(
                    resolving_result.active_bp,
                    message.hash().to_hex_string(),
                    None,
                    // Plain hex (LowerHex) — round-trips through
                    // ThreadIdentifier::try_from on the receiving node.
                    // Debug / Display add prefixes ("ThreadIdentifier<..>",
                    // "<T:..>") that break hex::decode on retry.
                    Some(format!("{:x}", message.thread_id())),
                )),
                bk_auth_token,
            );
            return;
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

        let (feedback_sender, feedback_receiver) = oneshot::channel();

        if let Err(e) =
            web_server.incoming_message_sender.clone().send((message.clone(), feedback_sender))
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

                web_server.metrics.as_ref().inspect(|m| {
                    m.report_ext_msg_processing_duration(
                        moment.elapsed().as_millis() as u64,
                        StatusCode::OK.as_u16(),
                    );
                    let err_type =
                        feedback.error.as_ref().map(|x| x.code.to_string()).unwrap_or_default();

                    m.report_ext_msg_feedback(&err_type);
                });

                let mut result: ExtMsgResponse = feedback.into();
                result.set_producers(producers);
                tracing::trace!(target: "http_server", "Response message: {:?}", result);

                res.status_code(StatusCode::OK);
                res.render(Json(result));

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
