// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::EdgeNameType;
use async_graphql::ComplexObject;
use async_graphql::Context;
use async_graphql::OutputType;
use async_graphql::SimpleObject;
use faster_hex::hex_string;

use super::attestations::BlockAttestation;
use crate::schema::db::DBConnector;
use crate::schema::graphql::query::PaginationArgs;

#[derive(Clone)]
pub struct BlockchainBkSetUpdatesQueryArgs {
    pub pagination: PaginationArgs,
    pub thread_id: Option<String>,
    pub height_start: Option<u64>,
    pub height_end: Option<u64>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct BlockchainBkSetUpdate {
    pub block_id: String,
    pub bk_set_update: String,
    pub chain_order: String,
    pub height: Option<u64>,
    pub thread_id: Option<String>,
}

impl TryFrom<crate::schema::db::bk_set_update::BkSetUpdate> for BlockchainBkSetUpdate {
    type Error = anyhow::Error;

    fn try_from(value: crate::schema::db::bk_set_update::BkSetUpdate) -> Result<Self, Self::Error> {
        Ok(Self {
            block_id: value.block_id,
            bk_set_update: hex_string(&value.bk_set_update),
            chain_order: value.chain_order,
            height: value.height.and_then(|v| {
                let arr: [u8; 8] = v.try_into().ok()?;
                Some(u64::from_be_bytes(arr))
            }),
            thread_id: value.thread_id,
        })
    }
}

#[ComplexObject]
impl BlockchainBkSetUpdate {
    async fn attestations(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<BlockAttestation>> {
        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        let rows = crate::schema::db::attestation::Attestation::by_source_block_id(
            db_connector,
            &self.block_id,
        )
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        rows.into_iter()
            .map(BlockAttestation::try_from_db)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }
}

pub(crate) struct BlockchainBkSetUpdatesEdge;

impl EdgeNameType for BlockchainBkSetUpdatesEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainBkSetUpdatesEdge".to_string()
    }
}

pub(crate) struct BlockchainBkSetUpdatesConnection;

impl ConnectionNameType for BlockchainBkSetUpdatesConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainBkSetUpdatesConnection".to_string()
    }
}
