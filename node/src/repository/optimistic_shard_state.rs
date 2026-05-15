// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;

use account_state::ThreadAccountsRepository;
use account_state::ThreadAccountsState;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde_with::DeserializeAs;
use serde_with::SerializeAs;
use tvm_types::Cell;

use super::tvm_cell_serde::CellFormat;

#[derive(Clone, Debug)]
pub struct OptimisticShardState(pub ThreadAccountsState);

impl Default for OptimisticShardState {
    fn default() -> Self {
        Self(ThreadAccountsRepository::new_state())
    }
}

impl Serialize for OptimisticShardState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::Error;
        let cell = ThreadAccountsRepository::state_to_tvm_cell(&self.0)
            .map_err(|err| S::Error::custom(format!("Failed to serialize shard state: {}", err)))?;
        <CellFormat as SerializeAs<Cell>>::serialize_as(&cell, serializer)
    }
}

impl<'de> Deserialize<'de> for OptimisticShardState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let cell: Cell = <CellFormat as DeserializeAs<Cell>>::deserialize_as(deserializer)?;
        Ok(Self(
            ThreadAccountsRepository::state_with_tvm_cell_and_empty_durable_state(cell).map_err(
                |err| D::Error::custom(format!("Failed to deserialize shard state: {}", err)),
            )?,
        ))
    }
}
