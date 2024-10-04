// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::Context;
use async_graphql::ErrorExtensions;
use async_graphql::FieldResult;
use async_graphql::InputObject;
use async_graphql::Object;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;

use super::error::PostReqError;

#[derive(InputObject)]
/// Request with external inbound message.
pub struct Request {
    id: Option<String>,
    body: Option<String>,
    expire_at: Option<f64>,
}

pub type NodeUrl = String;

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Post external inbound message to blockchain node.
    async fn post_requests<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        #[graphql(desc = "List of message requests")] requests: Option<Vec<Option<Request>>>,
    ) -> FieldResult<Option<Vec<Option<String>>>> {
        log::trace!("Processing post request...");
        if requests.is_none() {
            return Ok(None);
        }
        let mut ids: Vec<Option<String>> = Vec::new();
        let records: Vec<NodeRequest> = requests
            .unwrap()
            .iter()
            .filter_map(|r| match r {
                Some(Request { id: Some(id), body: Some(body), .. }) => {
                    ids.push(Some(id.into()));
                    Some(NodeRequest { key: id.into(), value: body.into() })
                }
                _ => None,
            })
            .collect();

        let result = fwd_to_node(ctx.data::<NodeUrl>()?, records).await;
        if let Err(err) = result {
            log::error!("Failed to forward requests: {err}");
            return Err(PostReqError::InternalError(
                "Failed to forward requests to node".to_string(),
            )
            .extend());
        }

        Ok(Some(ids))
    }
}

#[derive(serde::Serialize)]
struct NodeRequest {
    key: String,
    value: String,
}

async fn fwd_to_node(url: &str, requests: Vec<NodeRequest>) -> anyhow::Result<()> {
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::CACHE_CONTROL, HeaderValue::from_str("no-cache").unwrap());
    headers.insert(reqwest::header::ORIGIN, HeaderValue::from_str("same-origin").unwrap());
    headers
        .insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers.insert(reqwest::header::ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let client = reqwest::Client::builder().default_headers(headers).build()?;

    let response = client.post(url).json(&serde_json::json!({"records": &requests})).send().await?;
    log::debug!("Forward response code: {}", response.status());

    Ok(())
}
