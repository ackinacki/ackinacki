// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::Edge;
use async_graphql::connection::EdgeNameType;
use async_graphql::connection::EmptyFields;
use async_graphql::types::connection::query;
use async_graphql::types::connection::Connection;
use async_graphql::Context;
use async_graphql::Enum;
use async_graphql::InputObject;
use async_graphql::Object;
use async_graphql::OutputType;
use sqlx::SqlitePool;

use crate::client::BlockchainClient;
use crate::schema::db;
use crate::schema::db::message::PaginateDirection;
use crate::schema::graphql::account::Account;
use crate::schema::graphql::blockchain_api::calc_prev_next_markers;
use crate::schema::graphql::message::Message;

type BlockchainAccount = Account;
type BlockchainMessage = Message;

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
#[graphql(rename_items = "PascalCase")]
pub enum BlockchainMessageTypeFilterEnum {
    /// External inbound
    ExtIn,
    /// External outbound
    ExtOut,
    /// Internal inbound
    IntIn,
    /// Internal outbound
    IntOut,
}

impl From<BlockchainMessageTypeFilterEnum> for u8 {
    fn from(value: BlockchainMessageTypeFilterEnum) -> Self {
        match value {
            BlockchainMessageTypeFilterEnum::ExtIn => 1,
            BlockchainMessageTypeFilterEnum::ExtOut => 2,
            BlockchainMessageTypeFilterEnum::IntIn | BlockchainMessageTypeFilterEnum::IntOut => 0,
        }
    }
}

#[derive(Clone, InputObject)]
pub struct BlockchainMasterSeqNoFilter {
    pub start: Option<i32>,
    pub end: Option<i32>,
}

struct BlockchainMessageEdge;

impl EdgeNameType for BlockchainMessageEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainMessageEdge".to_string()
    }
}

struct BlockchainMessagesConnection;

impl ConnectionNameType for BlockchainMessagesConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainMessagesConnection".to_string()
    }
}

#[derive(Clone)]
pub struct BlockchainAccountQuery<'a> {
    pub ctx: &'a Context<'a>,
    pub address: String,
}

#[Object]
impl BlockchainAccountQuery<'_> {
    /// Account information (e.g. boc).
    pub async fn info(
        &self,
        #[graphql(
            desc = "Optional block hash. If byBlock is specified then the account info will be returned from the shard state defined by the specified block. Otherwise the account info will be returned from the last known shard state.",
            default
        )]
        _by_block: Option<String>,
    ) -> Option<BlockchainAccount> {
        let pool = self.ctx.data::<SqlitePool>().unwrap();
        db::Account::by_address(pool, Some(self.address.clone())).await.unwrap().map(|db_account| {
            let account_id = db_account.id.clone();
            let mut account: Account = db_account.into();
            account.id = format!("account/{account_id}");
            account
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// This node could be used for a cursor-based pagination of account
    /// messages.
    pub async fn messages(
        &self,
        #[graphql(
            name = "allow_latest_inconsistent_data",
            desc = "By default there is special latency added for the fetched recent data (several seconds) to ensure impossibility of inserts before the latest fetched cursor (data consistency, for reliable pagination). It is possible to disable this guarantee and to reduce the latency of realtime data by setting this flag to true."
        )]
        allow_latest_inconsistent_data: Option<bool>,
        #[graphql(name = "master_seq_no_range")] block_seq_no_range: Option<
            BlockchainMasterSeqNoFilter,
        >,
        #[graphql(
            name = "counterparties",
            desc = "Filter messages by counterparties (max - 5 counterparties)."
        )]
        counterparties: Option<Vec<String>>,
        #[graphql(
            name = "msg_type",
            desc = "Filter messages by type (multiple are allowed, all messages if not specified)."
        )]
        msg_type: Option<Vec<BlockchainMessageTypeFilterEnum>>,
        #[graphql(
            name = "min_value",
            desc = "Optional filter by min value (unoptimized, query could be dropped by timeout)."
        )]
        min_value: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
        #[graphql(
            desc = "Defines query scope. If true then query performed on a maximum time range supported by the cloud. If false then query performed on a recent time range supported by the cloud. You can find an actual information about time ranges on evercloud documentation."
        )]
        _archive: Option<bool>,
    ) -> Option<
        Connection<
            String,
            BlockchainMessage,
            EmptyFields,
            EmptyFields,
            BlockchainMessagesConnection,
            BlockchainMessageEdge,
        >,
    > {
        query(after, before, first, last, |after, before, first, last| async move {
            log::debug!(
                "first={:?}, after={:?}, last={:?}, before={:?}",
                first,
                after,
                last,
                before
            );

            // let directions = match msg_type {
            //     None => (true, true),
            //     Some(req_types) => req_types.into_iter().fold((false, false), |mut acc,
            // t| {         match t {
            //             BlockchainMessageTypeFilterEnum::ExtIn
            //             | BlockchainMessageTypeFilterEnum::IntIn => acc.0 = true,
            //             BlockchainMessageTypeFilterEnum::ExtOut
            //             | BlockchainMessageTypeFilterEnum::IntOut => acc.1 = true,
            //         }
            //         acc
            //     }),
            // };

            // let where_clause = {
            //     let mut where_ops = vec![];
            //     if directions.0 {
            //         where_ops.push(format!("dst={:?}", self.address));
            //     }
            //     if directions.1 {
            //         where_ops.push(format!("src={:?}", self.address));
            //     }
            //     match where_ops.len() {
            //         0 => "".to_string(),
            //         _ => format!("WHERE ({})", where_ops.join(" OR ")),
            //     }
            // };

            // let limit = if let Some(first) = first {
            //     first + 1
            // } else if let Some(last) = last {
            //     last + 1
            // } else {
            //     defaults::QUERY_BATCH_SIZE as usize
            // };
            // let mut messages = db::Message::list(
            //     self.ctx.data::<SqlitePool>().unwrap(),
            //     where_clause,
            //     "".to_string(),
            //     Some(limit).to_int(),
            // )
            // .await
            // .unwrap();

            let msg_type = match msg_type {
                Some(list) if list.is_empty() => None,
                _ => msg_type,
            };
            let args = db::AccountMessagesQueryArgs::new(
                allow_latest_inconsistent_data,
                block_seq_no_range,
                counterparties,
                msg_type,
                min_value,
                first,
                after.clone(),
                last,
                before.clone(),
            );
            let direction = args.get_direction();

            let limit = args.get_limit();

            let mut messages = db::Message::account_messages(
                self.ctx.data::<SqlitePool>().unwrap(),
                self.ctx.data::<BlockchainClient>().unwrap().clone(),
                self.address.clone(),
                args,
            )
            .await?;

            let (has_previous_page, has_next_page) =
                calc_prev_next_markers(after, before, first, last, messages.len());
            log::debug!("has_previous_page={:?}, after={:?}", has_previous_page, has_next_page);

            let mut connection: Connection<
                String,
                Message,
                EmptyFields,
                EmptyFields,
                BlockchainMessagesConnection,
                BlockchainMessageEdge,
            > = Connection::new(has_previous_page, has_next_page);

            if messages.len() >= limit {
                match direction {
                    PaginateDirection::Forward => messages.truncate(messages.len() - 1),
                    PaginateDirection::Backward => {
                        messages.drain(0..1);
                    }
                }
            }
            connection.edges.extend(messages.into_iter().map(|msg| {
                let msg: BlockchainMessage = msg.into();
                let cursor = msg.dst_chain_order.clone().unwrap();
                let edge: Edge<String, Message, EmptyFields, BlockchainMessageEdge> =
                    Edge::with_additional_fields(cursor, msg, EmptyFields);
                edge
            }));
            Ok::<_, async_graphql::Error>(connection)
        })
        .await
        .ok()
    }
}
