// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::Context;
use async_graphql::ErrorExtensions;
use async_graphql::FieldResult;
use async_graphql::InputObject;
use async_graphql::Object;
use async_graphql::SimpleObject;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use serde::Deserialize;

use super::error::PostReqError;

mod fwd_ext_messages;
use fwd_ext_messages::fwd_to_bk;

#[derive(InputObject)]
/// Request with external inbound message.
pub struct Request {
    id: Option<String>,
    body: Option<String>,
    expire_at: Option<f64>,
}

#[derive(InputObject)]
/// Request with external inbound message.
pub struct ExtMessage {
    id: Option<String>,
    body: Option<String>,
    expire_at: Option<f64>,
    thread_id: Option<String>,
}

#[derive(serde::Serialize, Clone)]
struct NodeRequest {
    id: String,
    boc: String,
    expire: Option<f64>,
    thread_id: Option<String>,
}

impl From<ExtMessage> for NodeRequest {
    fn from(msg: ExtMessage) -> Self {
        NodeRequest {
            id: msg.id.unwrap(),
            boc: msg.body.unwrap(),
            expire: msg.expire_at,
            thread_id: msg.thread_id,
        }
    }
}

#[derive(SimpleObject, Deserialize, Debug, Default)]
#[graphql(rename_fields = "snake_case")]
pub struct SendMessageResponse {
    message_hash: Option<String>,
    block_hash: Option<String>,
    tx_hash: Option<String>,
    aborted: Option<bool>,
    tvm_exit_code: Option<i32>,
    producers: Vec<String>,
    current_time: Option<String>,
    thread_id: Option<String>,
}

pub type NodeUrl = String;

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Post external inbound messages to blockchain node.
    async fn post_requests<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        #[graphql(desc = "List of message requests")] requests: Option<Vec<Option<Request>>>,
    ) -> FieldResult<Option<Vec<Option<String>>>> {
        tracing::trace!("Processing post request...");
        if requests.is_none() {
            return Ok(None);
        }
        let mut ids: Vec<Option<String>> = Vec::new();
        let records: Vec<NodeRequest> = requests
            .unwrap()
            .iter()
            .filter_map(|r| match r {
                Some(Request { id: Some(id), body: Some(body), expire_at }) => {
                    ids.push(Some(id.into()));
                    Some(NodeRequest {
                        id: id.into(),
                        boc: body.into(),
                        expire: *expire_at,
                        thread_id: None,
                    })
                }
                _ => None,
            })
            .collect();

        let result = fwd_to_node(ctx.data::<NodeUrl>()?, records).await;
        if let Err(err) = result {
            tracing::error!("Failed to forward requests: {err}");
            return Err(PostReqError::InternalError(
                "Failed to forward requests to node".to_string(),
            )
            .extend());
        }

        Ok(Some(ids))
    }

    /// Post external inbound message to blockchain node.
    async fn send_message<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        #[graphql(desc = "List of message requests")] message: Option<ExtMessage>,
    ) -> FieldResult<Option<SendMessageResponse>> {
        tracing::trace!("Processing post request...");
        if message.is_none() {
            return Ok(None);
        }

        fwd_to_bk(ctx.data::<NodeUrl>()?, message.unwrap().into()).await
    }
}

async fn fwd_to_node(url: &str, messages: Vec<NodeRequest>) -> anyhow::Result<()> {
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::CACHE_CONTROL, HeaderValue::from_str("no-cache").unwrap());
    headers.insert(reqwest::header::ORIGIN, HeaderValue::from_str("same-origin").unwrap());
    headers
        .insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers.insert(reqwest::header::ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let client = reqwest::Client::builder().default_headers(headers).build()?;

    tracing::debug!("Forwarding to: {url}");
    let response = client.post(url).json(&serde_json::json!(&messages)).send().await?;
    tracing::debug!("Forward response code: {}", response.status());

    Ok(())
}
