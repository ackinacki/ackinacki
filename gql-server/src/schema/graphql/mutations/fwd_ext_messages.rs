// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;

use async_graphql::ErrorExtensions;
use async_graphql::Value;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::StatusCode;
use serde::Deserialize;

use super::ExtMessage;
use super::SendMessageResponse;
use crate::defaults::BK_API_TIMEOUT;

#[derive(Debug, Default, Deserialize)]
struct SendMessageError {
    code: String,
    message: String,
    data: Option<Value>,
}

impl std::fmt::Display for SendMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SendMessageError {}

impl ErrorExtensions for SendMessageError {
    fn extend(&self) -> async_graphql::Error {
        async_graphql::Error::new(format!("{}", self)).extend_with(|_, e| {
            e.set("code", self.code.clone());
            if let Some(data) = &self.data {
                e.set("details", (*data).clone());
            }
        })
    }
}

pub(crate) async fn fwd_to_bk(
    url: &str,
    message: ExtMessage,
) -> async_graphql::Result<Option<SendMessageResponse>> {
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::CACHE_CONTROL, HeaderValue::from_str("no-cache").unwrap());
    headers.insert(reqwest::header::ORIGIN, HeaderValue::from_str("same-origin").unwrap());
    headers
        .insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers.insert(reqwest::header::ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(BK_API_TIMEOUT))
        .build()?;

    tracing::debug!("fwd_to_bk(): Forwarding (id: {:?}) to: {url}", message.id);

    match client.post(url).json(&serde_json::json!(vec![&message])).send().await {
        Ok(response) => {
            tracing::debug!("Forward response code: {}", response.status());
            if response.status() != StatusCode::OK {
                let error = SendMessageError {
                    code: "INTERNAL_ERROR".into(),
                    message: format!(
                        "The message redirection to the BM API has failed: {}",
                        response.status()
                    ),
                    ..Default::default()
                };
                return Err(error.extend());
            }
            let body = response.bytes().await;
            tracing::trace!("response body: {:?}", body);
            match body {
                Ok(b) => {
                    let s = std::str::from_utf8(&b)?;
                    let value = serde_json::from_str::<serde_json::Value>(s)?;
                    if value["result"].is_object() {
                        let mut result =
                            serde_json::from_value::<SendMessageResponse>(value["result"].clone())?;
                        // support deprecated field `tvm_exit_code`
                        result.tvm_exit_code = result.exit_code;
                        Ok(Some(result))
                    } else {
                        let error =
                            serde_json::from_value::<SendMessageError>(value["error"].clone())?;
                        Err(error.extend())
                    }
                }
                Err(_) => {
                    let error = SendMessageError {
                        code: "INTERNAL_ERROR".to_string(),
                        message: "Failed to parse response from the Block Producer".to_string(),
                        data: None,
                    };
                    Err(error.extend())
                }
            }
        }
        Err(err) => {
            tracing::error!("forward to {url} error: {err}");
            let error = SendMessageError {
                message: "Error forwarding the message to the Block Producer".to_string(),
                code: "INTERNAL_ERROR".to_string(),
                data: None,
            };
            Err(error.extend())
        }
    }
}
