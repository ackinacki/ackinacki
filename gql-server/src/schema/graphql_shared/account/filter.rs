// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::InputObject;
use serde::Serialize;

// use struct_fields_macro_derive::FieldList;
use crate::schema::graphql::filter::{
    // OptFloatFilter,
    // OptIntFilter,
    OptStringFilter,
    WhereOp,
};

#[derive(InputObject, Debug, Serialize, Default)]
#[graphql(rename_fields = "snake_case")]
pub struct AccountFilter {
    id: OptStringFilter,
    dapp_id: OptStringFilter,
    // acc_type: OptIntFilter,
    // last_paid: OptFloatFilter,
    #[graphql(name = "OR")]
    or: Option<Box<AccountFilter>>,
}

impl WhereOp for AccountFilter {}

#[cfg(test)]
pub mod tests {
    use super::AccountFilter;
    use crate::schema::graphql::filter::StringFilter;
    use crate::schema::graphql::filter::WhereOp;

    #[test]
    fn test_account_filter() {
        let af = AccountFilter {
            id: Some(StringFilter { eq: Some("-1:555...555".to_string()), ..Default::default() }),
            ..Default::default()
        };

        assert_eq!(af.to_where().unwrap(), format!("WHERE id = {:?}", "-1:555...555"));
    }
}
