// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::Error;
use async_graphql::ErrorExtensions;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum PostReqError {
    // ExceededSize,
    // Expired,
    InvalidPaginationArgs(String),
    InternalError(String),
}

impl ErrorExtensions for PostReqError {
    fn extend(&self) -> Error {
        Error::new(format!("{self:?}")).extend_with(|_, e| e.set("code", "INTERNAL_SERVER_ERROR"))
    }
}
