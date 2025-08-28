// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::InputObject;
use serde::Serialize;

use crate::schema::graphql::filter::OptFloatFilter;
use crate::schema::graphql::filter::OptIntFilter;
use crate::schema::graphql::filter::OptStringFilter;
use crate::schema::graphql::filter::WhereOp;

#[derive(InputObject, Debug, Serialize)]
#[graphql(rename_fields = "snake_case")]
pub struct MessageFilter {
    id: OptStringFilter,
    created_at: OptFloatFilter,
    dst: OptStringFilter,
    msg_type: OptIntFilter,
    src: OptStringFilter,
    status: OptIntFilter,
    or: Option<Box<MessageFilter>>,
}

impl WhereOp for MessageFilter {}
