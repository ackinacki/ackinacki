// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::ComplexObject;
use async_graphql::SimpleObject;

use super::formats::BigIntFormat;
use crate::helpers::format_big_int_dec;

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex)]
pub struct OtherCurrency {
    pub currency: Option<f64>,
    #[graphql(skip)]
    pub value: Option<String>,
}

#[ComplexObject]
impl OtherCurrency {
    async fn value(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int_dec(self.value.clone(), format)
    }
}
