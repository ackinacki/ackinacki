// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::EdgeNameType;
use async_graphql::OutputType;

use super::query::PaginationArgs;
use crate::schema::graphql::message::Message;
use crate::schema::graphql::Transaction;

pub(crate) type BlockchainMessage = Message;
pub(crate) type BlockchainTransaction = Transaction;

#[derive(Clone)]
pub struct BlockchainTransactionsQueryArgs {
    pub min_balance_delta: Option<String>,
    pub max_balance_delta: Option<String>,
    pub code_hash: Option<String>,
    pub pagination: PaginationArgs,
}

pub(crate) struct BlockchainTransactionsEdge;

impl EdgeNameType for BlockchainTransactionsEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainTransactionsEdge".to_string()
    }
}

pub(crate) struct BlockchainTransactionsConnection;

impl ConnectionNameType for BlockchainTransactionsConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainTransactionsConnection".to_string()
    }
}
