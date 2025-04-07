// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde_with::DeserializeAs;
use serde_with::SerializeAs;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_block::ShardStateUnsplit;
use tvm_types::Cell;
use typed_builder::TypedBuilder;

use super::tvm_cell_serde::CellFormat;

#[derive(Clone, TypedBuilder)]
pub struct OptimisticShardState {
    pub(crate) shard_state: Option<Arc<ShardStateUnsplit>>,
    pub(crate) shard_state_cell: Option<Cell>,
}

impl Debug for OptimisticShardState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimisticShardState")
            .field("shard_state", &self.shard_state.is_some())
            .field("shard_state_cell", &self.shard_state_cell.is_some())
            .finish()
    }
}

impl Default for OptimisticShardState {
    fn default() -> Self {
        Self { shard_state: Some(Arc::new(ShardStateUnsplit::default())), shard_state_cell: None }
    }
}

impl From<Arc<ShardStateUnsplit>> for OptimisticShardState {
    fn from(value: Arc<ShardStateUnsplit>) -> Self {
        Self { shard_state: Some(value), shard_state_cell: None }
    }
}

impl From<(ShardStateUnsplit, Cell)> for OptimisticShardState {
    fn from(value: (ShardStateUnsplit, Cell)) -> Self {
        let (shard_state, shard_state_root_cell) = value;
        Self {
            shard_state: Some(Arc::new(shard_state)),
            shard_state_cell: Some(shard_state_root_cell),
        }
    }
}

impl From<(Arc<ShardStateUnsplit>, Cell)> for OptimisticShardState {
    fn from(value: (Arc<ShardStateUnsplit>, Cell)) -> Self {
        let (shard_state, shard_state_root_cell) = value;
        Self { shard_state: Some(shard_state), shard_state_cell: Some(shard_state_root_cell) }
    }
}

impl From<Cell> for OptimisticShardState {
    fn from(value: Cell) -> Self {
        Self { shard_state: None, shard_state_cell: Some(value) }
    }
}

impl From<ShardStateUnsplit> for OptimisticShardState {
    fn from(value: ShardStateUnsplit) -> Self {
        Self { shard_state: Some(Arc::new(value)), shard_state_cell: None }
    }
}

impl Serialize for OptimisticShardState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(cell) = &self.shard_state_cell {
            <CellFormat as SerializeAs<Cell>>::serialize_as(cell, serializer)
        } else {
            let cell = self
                .shard_state
                .clone()
                .expect("Either state or root cell must exist")
                .serialize()
                .expect("Failed to serialize shard state");
            <CellFormat as SerializeAs<Cell>>::serialize_as(&cell, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for OptimisticShardState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let cell: Cell = <CellFormat as DeserializeAs<Cell>>::deserialize_as(deserializer)?;
        Ok(Self::from(cell))
    }
}

impl OptimisticShardState {
    pub fn into_cell(&mut self) -> Cell {
        if self.shard_state_cell.is_none() {
            let cell = self
                .shard_state
                .clone()
                .expect("Either state or root cell must exist")
                .serialize()
                .expect("Failed to serialize shard state");
            self.shard_state_cell = Some(cell);
        }
        self.shard_state_cell.clone().unwrap()
    }

    pub fn into_shard_state(&mut self) -> Arc<ShardStateUnsplit> {
        if self.shard_state.is_none() {
            assert!(self.shard_state_cell.is_some());
            let cell = self.shard_state_cell.clone().unwrap();
            let shard_state = Arc::new(
                ShardStateUnsplit::construct_from_cell(cell)
                    .expect("Failed to deserialize shard state from cell"),
            );
            self.shard_state = Some(shard_state.clone());
        }
        self.shard_state.clone().unwrap()
    }
}
