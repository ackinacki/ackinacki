// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::borrow::Cow;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use ext_messages_auth::auth::Token;
pub(crate) use message::ExternalMessage;
pub(crate) use message::IncomingExternalMessage;
use salvo::http::StatusCode;
use salvo::writing::Json;
use salvo::Response;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::Bytes;
use tvm_types::write_boc;
use tvm_types::SliceData;

mod message;
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

impl Debug for ThreadIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", hex::encode(self.0))
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ExtMsgResponse {
    result: Option<ExtMsgResult>,
    error: Option<ExtMsgError>,
    ext_message_token: Option<Token>,
}

impl ExtMsgResponse {
    pub fn new_with_error(code: String, message: String, data: Option<ExtMsgErrorData>) -> Self {
        Self {
            result: None,
            error: Some(ExtMsgError::new(code, message, data)),
            ext_message_token: None,
        }
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

impl From<ExtMsgFeedback> for ExtMsgResponse {
    fn from(feedback: ExtMsgFeedback) -> Self {
        tracing::trace!(target: "http_server", "Converting feedback to response...");

        let current_time = current_time_millis(None).to_string();
        let block_hash = feedback.block_hash.unwrap_or("None".to_string());
        let tx_hash = feedback.tx_hash.unwrap_or("None".to_string());

        if feedback.error.is_none() && feedback.exit_code == 0 {
            let ext_out_msgs = feedback
                .ext_out_msgs
                .into_iter()
                .filter_map(|body| write_boc(&body.cell()).ok().map(tvm_types::base64_encode))
                .collect();

            ExtMsgResponse {
                result: Some(ExtMsgResult {
                    message_hash: feedback.message_hash,
                    block_hash,
                    tx_hash,
                    state_timestamp: feedback.now,
                    ext_out_msgs,
                    aborted: feedback.aborted,
                    current_time,
                    exit_code: 0,
                    producers: vec![],
                    thread_id: feedback.thread_id.map(hex::encode),
                }),
                ..Default::default()
            }
        } else {
            let feedback_error: FeedbackError = feedback.error.unwrap();
            ExtMsgResponse {
                error: Some(ExtMsgError {
                    code: feedback_error.code.to_string().into(),
                    message: feedback_error.message.unwrap_or_else(|| "Unknown error".to_string()),
                    data: Some(ExtMsgErrorData::new(
                        vec![],
                        feedback.message_hash,
                        Some(feedback.exit_code),
                        feedback.thread_id.map(hex::encode),
                    )),
                }),
                ..Default::default()
            }
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct ExtMsgResult {
    message_hash: String,
    block_hash: String,
    tx_hash: String,
    state_timestamp: Option<u32>,
    ext_out_msgs: Vec<String>,
    aborted: bool, /* We still need this field as some aborted transactions from ExtInMsg are still included into block */
    exit_code: i32, /* To see the exit code of aborted transaction included into block. 0 for success */
    producers: Vec<String>, // ip of block producers, [0] - active one
    current_time: String,
    thread_id: Option<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct ExtMsgErrorData {
    producers: Vec<String>,
    message_hash: String,
    exit_code: Option<i32>,
    current_time: String,
    thread_id: Option<String>,
}

impl ExtMsgErrorData {
    pub fn new(
        producers: Vec<String>,
        message_hash: String,
        exit_code: Option<i32>,
        thread_id: Option<String>,
    ) -> Self {
        Self {
            producers,
            message_hash,
            exit_code,
            current_time: current_time_millis(None).to_string(),
            thread_id,
        }
    }
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
    ComputeSkipped,
    QueueOverflow,
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
            FeedbackErrorCode::ComputeSkipped => Cow::Borrowed("COMPUTE_SKIPPED"),
            FeedbackErrorCode::QueueOverflow => Cow::Borrowed("QUEUE_OVERFLOW"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FeedbackError {
    pub code: FeedbackErrorCode,
    pub message: Option<String>,
}

impl Display for FeedbackError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("FeedbackError");

        debug.field("code", &self.code);

        if let Some(message) = &self.message {
            debug.field("message", message);
        }

        debug.finish()
    }
}

#[derive(Clone, Default)]
pub struct ExtMsgFeedback {
    pub message_hash: String,
    pub tx_hash: Option<String>,
    pub block_hash: Option<String>,
    pub now: Option<u32>,
    pub aborted: bool,
    pub exit_code: i32,
    pub thread_id: Option<[u8; 34]>,
    pub error: Option<FeedbackError>,
    pub ext_out_msgs: Vec<SliceData>,
}

impl Display for ExtMsgFeedback {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("ExtMsgFeedback");

        debug.field("message_hash", &self.message_hash);

        if let Some(tx_hash) = &self.tx_hash {
            debug.field("tx_hash", tx_hash);
        }

        if let Some(block_hash) = &self.block_hash {
            debug.field("block_hash", block_hash);
        }

        debug.field("aborted", &self.aborted);
        debug.field("exit_code", &self.exit_code);

        if let Some(thread_id) = &self.thread_id {
            debug.field("thread_id", &hex::encode(thread_id));
        }

        if !self.ext_out_msgs.is_empty() {
            debug.field("ext_out_msgs", &self.ext_out_msgs.len());
        }

        if let Some(error) = &self.error {
            debug.field("error", error);
        }

        debug.finish()
    }
}

#[derive(Clone)]
pub struct ExtMsgFeedbackList(pub Vec<ExtMsgFeedback>);

impl Default for ExtMsgFeedbackList {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtMsgFeedbackList {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn push(&mut self, feedback: ExtMsgFeedback) {
        self.0.push(feedback);
    }
}

impl Display for ExtMsgFeedbackList {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut list = f.debug_list();
        for feedback in &self.0 {
            list.entry(&format_args!("{feedback}"));
        }
        list.finish()
    }
}

#[derive(Debug)]
pub struct ResolvingResult {
    i_am_bp: bool,
    active_bp: Vec<String>,
}

impl ResolvingResult {
    pub fn new(i_am_bp: bool, active_bp: Vec<String>) -> Self {
        Self { i_am_bp, active_bp }
    }
}

fn current_time_millis(ttl: Option<u64>) -> u128 {
    let now = std::time::SystemTime::now();
    let ttl = ttl.unwrap_or_default();
    let future = now + std::time::Duration::from_secs(ttl);
    future.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_millis()
}

pub(crate) fn render_error_response(
    res: &mut Response,
    code: &str,
    message: Option<&str>,
    data: Option<ExtMsgErrorData>,
    ext_message_token: Option<Token>,
) {
    res.status_code(StatusCode::BAD_REQUEST);
    res.render(Json(ExtMsgResponse {
        result: None,
        error: Some(ExtMsgError {
            code: code.to_string(),
            message: message.unwrap_or(code).to_string(),
            data,
        }),
        ext_message_token,
    }));
}

pub(crate) fn render_error(
    res: &mut Response,
    status: StatusCode,
    message: &str,
    ext_message_token: Option<Token>,
) {
    res.status_code(status);
    res.render(Json(ExtMsgResponse {
        result: None,
        error: Some(ExtMsgError {
            code: status.to_string(),
            message: message.to_string(),
            data: None,
        }),
        ext_message_token,
    }));
}
