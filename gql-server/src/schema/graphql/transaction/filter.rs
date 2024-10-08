// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::InputObject;
use serde::Serialize;

use crate::schema::graphql::filter::OptIntFilter;
use crate::schema::graphql::filter::OptStringFilter;
use crate::schema::graphql::filter::WhereOp;

#[derive(InputObject, Debug, Serialize)]
#[graphql(rename_fields = "snake_case")]
pub(crate) struct TransactionFilter {
    id: OptStringFilter,
    account_addr: OptStringFilter,
    block_id: OptStringFilter,
    end_status: OptIntFilter,
    orig_status: OptIntFilter,
    or: Option<Box<TransactionFilter>>,
}

impl WhereOp for TransactionFilter {}
