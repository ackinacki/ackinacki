// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::borrow::Cow;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::Bytes;

pub mod v1;
pub mod v2;

#[serde_as]
#[derive(Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ThreadIdentifier(#[serde_as(as = "Bytes")] [u8; 34]);

impl Default for ThreadIdentifier {
    fn default() -> Self {
        Self([0; 34])
    }
}

impl From<[u8; 34]> for ThreadIdentifier {
    fn from(array: [u8; 34]) -> Self {
        Self(array)
    }
}

impl TryFrom<std::string::String> for ThreadIdentifier {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match hex::decode(value) {
            Ok(array) => {
                let boxed_slice = array.into_boxed_slice();
                let boxed_array: Box<[u8; 34]> = match boxed_slice.try_into() {
                    Ok(array) => array,
                    Err(e) => anyhow::bail!("Expected a Vec of length 34 but it was {}", e.len()),
                };
                Ok(Self(*boxed_array))
            }
            Err(_) => anyhow::bail!("Failed to convert to ThreadIdentifier"),
        }
    }
}

impl From<ThreadIdentifier> for [u8; 34] {
    fn from(val: ThreadIdentifier) -> Self {
        val.0
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ExtMsgResponse {
    result: Option<ExtMsgResult>,
    error: Option<ExtMsgError>,
}

impl ExtMsgResponse {
    pub fn new_with_error(code: String, message: String, data: Option<ExtMsgErrorData>) -> Self {
        Self { result: None, error: Some(ExtMsgError::new(code, message, data)) }
    }

    fn set_producers(&mut self, producers: Vec<String>) {
        if let Some(mut result) = self.result.take() {
            result.producers = producers.clone();
            self.result = Some(result);
        }
        if let Some(mut error) = self.error.take() {
            error.set_producers(producers);
            self.error = Some(error);
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct ExtMsgResult {
    message_hash: String,
    block_hash: String,
    tx_hash: String,
    aborted: bool, /* We still need this field as some aborted transactions from ExtInMsg are still included into block */
    tvm_exit_code: i32, /* To see the exit code of aborted transaction included into block. 0 for success */
    producers: Vec<String>, // ip of block producers, [0] - active one
    current_time: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct ExtMsgErrorData {
    producers: Vec<String>,
    message_hash: String,
    tvm_exit_code: Option<i32>,
    current_time: String,
    thread_id: Option<String>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ExtMsgError {
    code: String,
    message: String,
    data: Option<ExtMsgErrorData>,
}

impl ExtMsgError {
    fn new(code: String, message: String, data: Option<ExtMsgErrorData>) -> Self {
        Self { code, message, data }
    }

    fn set_producers(&mut self, producers: Vec<String>) {
        if let Some(mut data) = self.data.take() {
            data.producers = producers;
            self.data = Some(data);
        }
    }
}

#[derive(Clone, Debug)]
pub enum FeedbackErrorCode {
    Ok,
    TvmError,
    MessageExpired,
    TooManyRequestsInQueue,
    NotActiveProducer,
    DuplicateMessage,
    ThreadMismatch,
    InternalError,
}

impl FeedbackErrorCode {
    fn to_string(&self) -> Cow<'static, str> {
        match self {
            FeedbackErrorCode::Ok => Cow::Borrowed("Ok"),
            FeedbackErrorCode::TvmError => Cow::Borrowed("TVM_ERROR"),
            FeedbackErrorCode::MessageExpired => Cow::Borrowed("MESSAGE_EXPIRED"),
            FeedbackErrorCode::TooManyRequestsInQueue => {
                Cow::Borrowed("TOO_MANY_REQUESTS_IN_QUEUE")
            }
            FeedbackErrorCode::NotActiveProducer => Cow::Borrowed("WRONG_PRODUCER"),
            FeedbackErrorCode::DuplicateMessage => Cow::Borrowed("DUPLICATE_MESSAGE"),
            FeedbackErrorCode::ThreadMismatch => Cow::Borrowed("THREAD_MISMATCH"),
            FeedbackErrorCode::InternalError => Cow::Borrowed("INTERNAL_ERROR"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FeedbackError {
    pub code: FeedbackErrorCode,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ExtMsgFeedback {
    pub message_hash: String,
    pub tx_hash: Option<String>,
    pub block_hash: Option<String>,
    pub aborted: bool,
    pub tvm_exit_code: i32,
    pub thread_id: Option<[u8; 34]>,
    pub error: Option<FeedbackError>,
}

impl From<ExtMsgFeedback> for ExtMsgResponse {
    fn from(feedback: ExtMsgFeedback) -> Self {
        tracing::trace!(target: "http_server", "Converting feedback to response...");
        let mut response = ExtMsgResponse { ..Default::default() };
        tracing::trace!(target: "http_server", "Dummy response ok");

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        tracing::trace!(target: "http_server", "Current time: {current_time}");
        tracing::trace!(target: "http_server", "Processing by exit code: {}", feedback.tvm_exit_code);
        match feedback.tvm_exit_code {
            0 => {
                response.result = Some(ExtMsgResult {
                    message_hash: feedback.message_hash,
                    block_hash: feedback.block_hash.unwrap(),
                    tx_hash: feedback.tx_hash.unwrap(),
                    aborted: false,
                    current_time,
                    tvm_exit_code: 0,
                    producers: vec![],
                });
            }
            _ => {
                let feedback_error = feedback.error.unwrap();
                // let (code, message) = match feedback.error {
                //     Some(_) => (),
                //     None => ("INTERNAL_ERROR".to_string(), "")
                // };
                let error = ExtMsgError {
                    code: feedback_error.code.to_string().into(),
                    message: match feedback_error.message {
                        Some(msg) => msg,
                        None => "Unknown error".to_string(),
                    },
                    data: Some(ExtMsgErrorData {
                        message_hash: feedback.message_hash,
                        current_time,
                        tvm_exit_code: Some(feedback.tvm_exit_code),
                        producers: vec![],
                        thread_id: feedback.thread_id.map(hex::encode),
                    }),
                };
                response.error = Some(error);
            }
        }

        response
    }
}
