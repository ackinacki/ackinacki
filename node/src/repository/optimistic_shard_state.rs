// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use parking_lot::Mutex;
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
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[derive(Clone)]
pub enum OptimisticShardStateRepresentation {
    Both(Arc<ShardStateUnsplit>, Arc<Cell>),
    AsShardState(Arc<ShardStateUnsplit>),
    AsCell(Arc<Cell>),
}

impl AllowGuardedMut for OptimisticShardStateRepresentation {}

#[derive(TypedBuilder)]
pub struct OptimisticShardState {
    data: Arc<Mutex<OptimisticShardStateRepresentation>>,
}

impl Clone for OptimisticShardState {
    fn clone(&self) -> Self {
        let data_clone = self.data.guarded(|v| v.clone());
        Self { data: Arc::new(Mutex::new(data_clone)) }
    }
}

impl Debug for OptimisticShardState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (is_shard_state, is_cell) = match *self.data.lock() {
            OptimisticShardStateRepresentation::Both(_, _) => (true, true),
            OptimisticShardStateRepresentation::AsShardState(_) => (true, false),
            OptimisticShardStateRepresentation::AsCell(_) => (false, true),
        };
        f.debug_struct("OptimisticShardState")
            .field("shard_state", &is_shard_state)
            .field("shard_state_cell", &is_cell)
            .finish()
    }
}

impl Default for OptimisticShardState {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(OptimisticShardStateRepresentation::AsShardState(Arc::new(
                ShardStateUnsplit::default(),
            )))),
        }
    }
}

impl From<Arc<ShardStateUnsplit>> for OptimisticShardState {
    fn from(value: Arc<ShardStateUnsplit>) -> Self {
        Self { data: Arc::new(Mutex::new(OptimisticShardStateRepresentation::AsShardState(value))) }
    }
}

impl From<(ShardStateUnsplit, Cell)> for OptimisticShardState {
    fn from(value: (ShardStateUnsplit, Cell)) -> Self {
        let (shard_state, shard_state_root_cell) = value;
        Self {
            data: Arc::new(Mutex::new(OptimisticShardStateRepresentation::Both(
                Arc::new(shard_state),
                Arc::new(shard_state_root_cell),
            ))),
        }
    }
}

impl From<(Arc<ShardStateUnsplit>, Cell)> for OptimisticShardState {
    fn from(value: (Arc<ShardStateUnsplit>, Cell)) -> Self {
        let (shard_state, shard_state_root_cell) = value;
        Self {
            data: Arc::new(Mutex::new(OptimisticShardStateRepresentation::Both(
                shard_state,
                Arc::new(shard_state_root_cell),
            ))),
        }
    }
}

impl From<Cell> for OptimisticShardState {
    fn from(shard_state_root_cell: Cell) -> Self {
        Self {
            data: Arc::new(Mutex::new(OptimisticShardStateRepresentation::AsCell(Arc::new(
                shard_state_root_cell,
            )))),
        }
    }
}

impl From<ShardStateUnsplit> for OptimisticShardState {
    fn from(value: ShardStateUnsplit) -> Self {
        Self {
            data: Arc::new(Mutex::new(OptimisticShardStateRepresentation::AsShardState(Arc::new(
                value,
            )))),
        }
    }
}

impl Serialize for OptimisticShardState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.data.guarded(|e| match e {
            OptimisticShardStateRepresentation::Both(_, root_cell)
            | OptimisticShardStateRepresentation::AsCell(root_cell) => {
                <CellFormat as SerializeAs<Cell>>::serialize_as(root_cell.as_ref(), serializer)
            }
            OptimisticShardStateRepresentation::AsShardState(shard_state) => {
                let cell =
                    shard_state.clone().serialize().expect("Failed to serialize shard state");
                <CellFormat as SerializeAs<Cell>>::serialize_as(&cell, serializer)
            }
        })
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
    pub fn make_an_independent_copy(&self) -> Self {
        OptimisticShardState { data: Arc::new(Mutex::new(self.data.lock().clone())) }
    }

    pub fn into_cell(&self) -> Arc<Cell> {
        self.data.guarded_mut(|e| match e {
            OptimisticShardStateRepresentation::Both(_, root_cell)
            | OptimisticShardStateRepresentation::AsCell(root_cell) => Arc::clone(root_cell),
            OptimisticShardStateRepresentation::AsShardState(shard_state) => {
                let root_cell = Arc::new(
                    shard_state.clone().serialize().expect("Failed to serialize shard state"),
                );
                *e = OptimisticShardStateRepresentation::Both(
                    Arc::clone(shard_state),
                    Arc::clone(&root_cell),
                );
                root_cell
            }
        })
    }

    pub fn into_shard_state(&self) -> Arc<ShardStateUnsplit> {
        self.data.guarded_mut(|e| match e {
            OptimisticShardStateRepresentation::Both(shard_state, _)
            | OptimisticShardStateRepresentation::AsShardState(shard_state) => {
                tracing::trace!("take existing shard state");
                Arc::clone(shard_state)
            }
            OptimisticShardStateRepresentation::AsCell(root_cell) => {
                let cell = Arc::make_mut(root_cell).clone();
                tracing::trace!("deser shard state from cell start");
                let shard_state = Arc::new(
                    ShardStateUnsplit::construct_from_cell(cell)
                        .expect("Failed to deserialize shard state from cell"),
                );
                tracing::trace!("deser shard state from cell finish");
                *e = OptimisticShardStateRepresentation::Both(
                    Arc::clone(&shard_state),
                    Arc::clone(root_cell),
                );
                shard_state
            }
        })
    }

    pub fn clone_representation(&self) -> OptimisticShardStateRepresentation {
        self.data.guarded(|e| e.clone())
    }
}
