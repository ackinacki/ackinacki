// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::EdgeNameType;
use async_graphql::OutputType;

use super::account::BlockchainMasterSeqNoFilter;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql_ext::Block;

pub(crate) type BlockchainBlock = Block;

#[derive(Clone)]
pub struct BlockchainBlocksQueryArgs {
    pub block_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
    pub min_tr_count: Option<i32>,
    pub max_tr_count: Option<i32>,
    pub pagination: PaginationArgs,
}

pub(crate) struct BlockchainBlocksEdge;

impl EdgeNameType for BlockchainBlocksEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainBlocksEdge".to_string()
    }
}

pub(crate) struct BlockchainBlocksConnection;

impl ConnectionNameType for BlockchainBlocksConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainBlocksConnection".to_string()
    }
}
