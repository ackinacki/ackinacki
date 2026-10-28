// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

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
use tvm_client::ClientContext;

use super::connection_node_selection;
use super::populate_message_tree;
use super::populate_transaction_tree;
use super::selected_node_fields_at;
use super::transactions::BlockchainTransaction;
use super::MAX_RELATION_DEPTH;
use crate::schema::db;
use crate::schema::db::transaction::AccountTransactionsQueryArgs;
use crate::schema::db::DBConnector;
use crate::schema::graphql::account::Account;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql::transaction::Transaction;
use crate::schema::graphql_ext::account::AccountEventEdge;
use crate::schema::graphql_ext::account::AccountEventsConnection;
use crate::schema::graphql_ext::blockchain_api::transactions::BlockchainTransactionsConnection;
use crate::schema::graphql_ext::blockchain_api::transactions::BlockchainTransactionsEdge;
use crate::schema::graphql_ext::events::Event;
use crate::schema::graphql_ext::is_cold_storage_enabled;
use crate::schema::graphql_ext::message::Message;
use crate::schema::graphql_ext::strip_cold_pruned_transaction_fields;

type BlockchainAccount = Account;
type BlockchainMessage = Message;

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
#[graphql(rename_items = "PascalCase")]
pub enum BlockchainMessageTypeFilterEnum {
    /// External inbound
    ExtIn,
    /// External outbound — any version: `ExtOut` (msg_type 2) and
    /// `ExtOutMsgInfoV2` (msg_type 4)
    ExtOut,
    /// External outbound v2 only (`ExtOutMsgInfoV2`, msg_type 4)
    ExtOutV2,
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
            BlockchainMessageTypeFilterEnum::ExtOutV2 => 4,
            BlockchainMessageTypeFilterEnum::IntIn | BlockchainMessageTypeFilterEnum::IntOut => 0,
        }
    }
}

#[derive(Clone, InputObject)]
pub struct BlockchainMasterSeqNoFilter {
    pub start: Option<i32>,
    pub end: Option<i32>,
}

pub struct BlockchainMessageEdge;

impl EdgeNameType for BlockchainMessageEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainMessageEdge".to_string()
    }
}

pub struct BlockchainMessagesConnection;

impl ConnectionNameType for BlockchainMessagesConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainMessagesConnection".to_string()
    }
}

pub struct BlockchainAccountEdge;

impl EdgeNameType for BlockchainAccountEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainAccountEdge".to_string()
    }
}

pub struct BlockchainAccountsConnection;

impl ConnectionNameType for BlockchainAccountsConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainAccountsConnection".to_string()
    }
}

#[derive(Clone)]
pub struct BlockchainAccountQuery<'a> {
    pub ctx: &'a Context<'a>,
    pub account_id: String,
    pub dapp_id: String,
    pub archive_address: String,
    pub preloaded: Option<db::Account>,
}

pub(super) fn archive_address_from_account_id(account_id: &str) -> String {
    format!("0:{account_id}")
}

pub(super) fn account_id_from_archive_address(address: &str) -> String {
    address.split_once(':').map(|(_, account_id)| account_id).unwrap_or(address).to_string()
}

#[Object]
impl BlockchainAccountQuery<'_> {
    /// Account information (e.g. boc).
    pub(crate) async fn info(
        &self,
        #[graphql(
            desc = "Optional block hash. If byBlock is specified then the account info will be returned from the shard state defined by the specified block. Otherwise the account info will be returned from the last known shard state.",
            default
        )]
        _by_block: Option<String>,
    ) -> async_graphql::Result<Option<BlockchainAccount>> {
        if let Some(preloaded) = &self.preloaded {
            return Ok(Some(preloaded.clone().into()));
        }
        let db_connector = self.ctx.data::<Arc<DBConnector>>()?;
        let client = self.ctx.data::<Arc<ClientContext>>()?;

        Ok(db::Account::by_account_id(
            db_connector,
            client,
            self.account_id.clone(),
            self.dapp_id.clone(),
        )
        .await?
        .map(|db_account| db_account.into()))
    }

    /// This node could be used for a cursor-based pagination of account
    /// events (aka outgoing external messages).
    pub async fn events(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Optional destination address filter.")] dst: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
    ) -> async_graphql::Result<
        Option<
            Connection<
                String,
                Event,
                EmptyFields,
                EmptyFields,
                AccountEventsConnection,
                AccountEventEdge,
            >,
        >,
    > {
        Ok(Some(
            query(after, before, first, last, |after, before, first, last| async move {
                tracing::debug!(
                    "first={:?}, after={:?}, last={:?}, before={:?}",
                    first,
                    after,
                    last,
                    before
                );

                let pagination = PaginationArgs { first, after, last, before };
                let projection = db::Message::event_projection_for_fields(selected_node_fields_at(
                    self.ctx,
                    &["account", "events"],
                ));
                let mut messages = db::Message::account_events(
                    ctx.data::<Arc<DBConnector>>().unwrap(),
                    &projection,
                    self.archive_address.clone(),
                    dst,
                    &pagination,
                )
                .await?;

                let (has_previous_page, has_next_page) =
                    pagination.get_bound_markers(messages.len());
                tracing::debug!(
                    "has_previous_page={:?}, after={:?}",
                    has_previous_page,
                    has_next_page
                );

                let mut connection: Connection<
                    String,
                    Event,
                    EmptyFields,
                    EmptyFields,
                    AccountEventsConnection,
                    AccountEventEdge,
                > = Connection::new(has_previous_page, has_next_page);

                pagination.shrink_portion(&mut messages);

                let mut edges: Vec<Edge<String, Event, EmptyFields, AccountEventEdge>> = vec![];
                for message in messages {
                    let event: Event = message.into();
                    let cursor = event.msg_chain_order.clone().unwrap();
                    let edge: Edge<String, Event, EmptyFields, AccountEventEdge> =
                        Edge::with_additional_fields(cursor, event, EmptyFields);
                    edges.push(edge);
                }
                connection.edges.extend(edges);
                Ok::<_, async_graphql::Error>(connection)
            })
            .await
            .map_err(crate::schema::db::connector::map_db_error)?,
        ))
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
            desc = "Filter messages by counterparties (max - 5 counterparties). This field is mutually exclusive with 'msg_type=ExtOut'.",
            deprecation = "This argument is deprecated and will be removed in a future release."
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
    ) -> async_graphql::Result<
        Option<
            Connection<
                String,
                BlockchainMessage,
                EmptyFields,
                EmptyFields,
                BlockchainMessagesConnection,
                BlockchainMessageEdge,
            >,
        >,
    > {
        if let Some(ref msg_types) = msg_type {
            let has_external_outbound = msg_types
                .contains(&BlockchainMessageTypeFilterEnum::ExtOut)
                || msg_types.contains(&BlockchainMessageTypeFilterEnum::ExtOutV2);
            if has_external_outbound {
                if let Some(ref cps) = counterparties {
                    if !cps.is_empty() {
                        return Err(async_graphql::Error::new(
                            "Invalid input: `counterparties` parameter must be null when `msg_type` includes an external outbound type (`ExtOut`/`ExtOutV2`)."
                        ));
                    }
                }
            }
        }

        // On cold-storage servers only external outbound messages are stored: `ExtOut`
        // (msg_type 2) and `ExtOutV2` (msg_type 3). Any filter for inbound or internal
        // messages cannot be satisfied and is rejected rather than silently returning
        // nothing.
        if is_cold_storage_enabled(self.ctx) {
            if let Some(ref msg_types) = msg_type {
                let has_unavailable = msg_types.iter().any(|t| {
                    !matches!(
                        t,
                        BlockchainMessageTypeFilterEnum::ExtOut
                            | BlockchainMessageTypeFilterEnum::ExtOutV2
                    )
                });
                if has_unavailable {
                    return Err(async_graphql::Error::new(
                        "Only external outbound messages are available on cold-storage servers.",
                    ));
                }
            }
        }

        Ok(Some(
            query(after, before, first, last, |after, before, first, last| async move {
                tracing::debug!(
                    "first={:?}, after={:?}, last={:?}, before={:?}",
                    first,
                    after,
                    last,
                    before
                );

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
                    PaginationArgs { first, after, last, before },
                );
                let selected_fields = selected_node_fields_at(self.ctx, &["account", "messages"]);
                let projection = db::Message::connection_projection_for_fields(
                    selected_fields.clone(),
                    &args.cursor_field(),
                );
                let mut messages = db::Message::account_messages(
                    self.ctx.data::<Arc<DBConnector>>().unwrap(),
                    &projection,
                    self.archive_address.clone(),
                    &args,
                )
                .await?;

                let (has_previous_page, has_next_page) =
                    args.pagination.get_bound_markers(messages.len());
                tracing::debug!(
                    "has_previous_page={:?}, after={:?}",
                    has_previous_page,
                    has_next_page
                );

                let mut connection: Connection<
                    String,
                    Message,
                    EmptyFields,
                    EmptyFields,
                    BlockchainMessagesConnection,
                    BlockchainMessageEdge,
                > = Connection::new(has_previous_page, has_next_page);

                args.pagination.shrink_portion(&mut messages);

                let mut edges: Vec<Edge<String, Message, EmptyFields, BlockchainMessageEdge>> =
                    vec![];
                for message in messages {
                    let mut message: BlockchainMessage = message.into();
                    populate_message_tree(
                        self.ctx,
                        &mut message,
                        connection_node_selection(
                            self.ctx.look_ahead().field("account").field("messages"),
                        ),
                        MAX_RELATION_DEPTH,
                    )
                    .await?;
                    let cursor = match args.cursor_field() {
                        db::MessageCursorField::Dst => message
                            .dst_chain_order
                            .clone()
                            .or(message.src_chain_order.clone())
                            .unwrap(),
                        db::MessageCursorField::Src => message
                            .src_chain_order
                            .clone()
                            .or(message.dst_chain_order.clone())
                            .unwrap(),
                        db::MessageCursorField::Coalesced => message
                            .dst_chain_order
                            .clone()
                            .or(message.src_chain_order.clone())
                            .unwrap(),
                    };
                    let edge: Edge<String, Message, EmptyFields, BlockchainMessageEdge> =
                        Edge::with_additional_fields(cursor, message, EmptyFields);
                    edges.push(edge);
                }
                connection.edges.extend(edges);
                Ok::<_, async_graphql::Error>(connection)
            })
            .await
            .map_err(crate::schema::db::connector::map_db_error)?,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    /// This node could be used for a cursor-based pagination of account transactions.
    pub async fn transactions(
        &self,
        #[graphql(
            name = "allow_latest_inconsistent_data",
            desc = "By default there is special latency added for the fetched recent data (several seconds) to ensure impossibility of inserts before the latest fetched cursor (data consistency, for reliable pagination). It is possible to disable this guarantee and to reduce the latency of realtime data by setting this flag to true."
        )]
        allow_latest_inconsistent_data: Option<bool>,
        #[graphql(name = "master_seq_no_range")] block_seq_no_range: Option<
            BlockchainMasterSeqNoFilter,
        >,
        aborted: Option<bool>,
        #[graphql(
            name = "min_balance_delta",
            desc = "Optional filter by min balance_delta (unoptimized, query could be dropped by timeout)."
        )]
        min_balance_delta: Option<String>,
        #[graphql(
            name = "max_balance_delta",
            desc = "Optional filter by max balance_delta (unoptimized, query could be dropped by timeout)."
        )]
        max_balance_delta: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
        #[graphql(
            desc = "Defines query scope. If true then query performed on a maximum time range supported by the cloud. If false then query performed on a recent time range supported by the cloud. You can find an actual information about time ranges on evercloud documentation."
        )]
        _archive: Option<bool>,
    ) -> async_graphql::Result<
        Option<
            Connection<
                String,
                BlockchainTransaction,
                EmptyFields,
                EmptyFields,
                BlockchainTransactionsConnection,
                BlockchainTransactionsEdge,
            >,
        >,
    > {
        Ok(Some(
            query(after, before, first, last, |after, before, first, last| async move {
                tracing::debug!(
                    "first={:?}, after={:?}, last={:?}, before={:?}",
                    first,
                    after,
                    last,
                    before
                );

                let args = AccountTransactionsQueryArgs::new(
                    allow_latest_inconsistent_data,
                    block_seq_no_range,
                    aborted,
                    min_balance_delta,
                    max_balance_delta,
                    PaginationArgs { first, after, last, before },
                );

                let projection = db::Transaction::connection_projection_for_fields(
                    strip_cold_pruned_transaction_fields(
                        self.ctx,
                        selected_node_fields_at(self.ctx, &["account", "transactions"]),
                    ),
                );
                let mut transactions: Vec<db::Transaction> = db::Transaction::account_transactions(
                    self.ctx.data::<Arc<DBConnector>>().unwrap(),
                    &projection,
                    self.archive_address.clone(),
                    &args,
                )
                .await?;

                let (has_previous_page, has_next_page) =
                    args.pagination.get_bound_markers(transactions.len());
                tracing::debug!(
                    "has_previous_page={:?}, after={:?}",
                    has_previous_page,
                    has_next_page
                );

                let mut connection: Connection<
                    String,
                    Transaction,
                    EmptyFields,
                    EmptyFields,
                    BlockchainTransactionsConnection,
                    BlockchainTransactionsEdge,
                > = Connection::new(has_previous_page, has_next_page);

                args.pagination.shrink_portion(&mut transactions);

                let mut edges = vec![];
                for transaction in transactions {
                    let mut transaction: BlockchainTransaction = transaction.into();
                    populate_transaction_tree(
                        self.ctx,
                        &mut transaction,
                        connection_node_selection(
                            self.ctx.look_ahead().field("account").field("transactions"),
                        ),
                        MAX_RELATION_DEPTH,
                    )
                    .await?;
                    let cursor = transaction.chain_order.clone();
                    let edge: Edge<String, Transaction, EmptyFields, BlockchainTransactionsEdge> =
                        Edge::with_additional_fields(cursor, transaction, EmptyFields);
                    edges.push(edge);
                }
                connection.edges.extend(edges);
                Ok::<_, async_graphql::Error>(connection)
            })
            .await
            .map_err(crate::schema::db::connector::map_db_error)?,
        ))
    }
}
