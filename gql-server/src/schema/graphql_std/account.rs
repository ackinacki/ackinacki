// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use async_graphql::connection::query;
use async_graphql::connection::Connection;
use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::Edge;
use async_graphql::connection::EdgeNameType;
use async_graphql::connection::EmptyFields;
use async_graphql::Context;
use async_graphql::Object;
use async_graphql::OutputType;
use async_graphql::SimpleObject;
use tvm_client::ClientContext;

use crate::schema::db;
use crate::schema::db::DBConnector;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql_std::events::Event;

struct AccountEventEdge;

impl EdgeNameType for AccountEventEdge {
    fn type_name<T: OutputType>() -> String {
        "AccountEventEdge".to_string()
    }
}

struct AccountEventsConnection;

impl ConnectionNameType for AccountEventsConnection {
    fn type_name<T: OutputType>() -> String {
        "AccountEventsConnection".to_string()
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(rename_fields = "snake_case")]
pub(crate) struct Account {
    pub id: String,
    /// Bag of cells with the account struct encoded as base64.
    boc: Option<String>,
    dapp_id: Option<String>,
}

impl From<db::Account> for Account {
    fn from(acc: db::Account) -> Self {
        let boc = acc.boc.map(tvm_types::base64_encode);
        Self { id: acc.id.clone(), boc, dapp_id: acc.dapp_id }
    }
}

#[derive(Clone)]
pub struct AccountQuery {
    pub address: String,
    pub preloaded: Option<db::Account>,
}

#[Object]
impl AccountQuery {
    /// Account information.
    pub async fn info(&self, ctx: &Context<'_>) -> Option<Account> {
        if let Some(preloaded) = &self.preloaded {
            return Some(preloaded.clone().into());
        }

        let db_connector = ctx.data::<Arc<DBConnector>>().unwrap();
        let client = ctx.data::<Arc<ClientContext>>().unwrap();

        db::Account::by_address(db_connector, client, Some(self.address.clone()))
            .await
            .unwrap()
            .map(|db_account| db_account.into())
    }

    /// This node could be used for a cursor-based pagination of account
    /// events (aka outgoing external messages).
    pub async fn events(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
    ) -> Option<
        Connection<
            String,
            Event,
            EmptyFields,
            EmptyFields,
            AccountEventsConnection,
            AccountEventEdge,
        >,
    > {
        query(after, before, first, last, |after, before, first, last| async move {
            tracing::debug!(
                "first={:?}, after={:?}, last={:?}, before={:?}",
                first,
                after,
                last,
                before
            );

            let pagination = PaginationArgs { first, after, last, before };
            let mut messages = db::Message::account_events(
                ctx.data::<Arc<DBConnector>>().unwrap(),
                self.address.clone(),
                &pagination,
            )
            .await?;

            let (has_previous_page, has_next_page) = pagination.get_bound_markers(messages.len());
            tracing::debug!("has_previous_page={:?}, after={:?}", has_previous_page, has_next_page);

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
        .ok()
    }
}
