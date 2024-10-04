// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::EdgeNameType;
use async_graphql::OutputType;

use super::account::BlockchainMasterSeqNoFilter;
use super::query::PaginateDirection;
use super::query::QueryArgs;
use crate::schema::graphql::Block;

pub(crate) type BlockchainBlock = Block;

#[derive(Clone)]
pub struct BlockchainBlocksQueryArgs {
    pub block_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
    pub min_tr_count: Option<i32>,
    pub max_tr_count: Option<i32>,
    pub first: Option<usize>,
    pub after: Option<String>,
    pub last: Option<usize>,
    pub before: Option<String>,
}

impl QueryArgs for BlockchainBlocksQueryArgs {
    fn get_limit(&self) -> usize {
        1 + if let Some(first) = self.first {
            first
        } else if let Some(last) = self.last {
            last
        } else {
            crate::defaults::QUERY_BATCH_SIZE as usize
        }
    }

    fn get_direction(&self) -> PaginateDirection {
        if self.last.is_some() || self.before.is_some() {
            PaginateDirection::Backward
        } else {
            PaginateDirection::Forward
        }
    }
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
