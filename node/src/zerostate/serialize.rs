// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use crate::zerostate::ZeroState;

pub(super) mod legacy_states {
    use std::collections::HashMap;

    use node_types::ThreadIdentifier;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    use crate::repository::optimistic_state::OptimisticStateImpl;
    use crate::repository::repository_impl::snapshot_v1::State;

    pub fn serialize<S: Serializer>(
        states: &HashMap<ThreadIdentifier, OptimisticStateImpl>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        states
            .iter()
            .map(|(id, state)| (*id, State::from(state.clone())))
            .collect::<HashMap<_, _>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashMap<ThreadIdentifier, OptimisticStateImpl>, D::Error> {
        HashMap::<ThreadIdentifier, State>::deserialize(deserializer)?
            .into_iter()
            .map(|(id, legacy)| {
                // A zerostate has no separate message archive. Inline messages
                // seed fresh commitments; external history is an invalid input.
                let state =
                    legacy.into_current(&mut HashMap::new()).map_err(serde::de::Error::custom)?;
                if id != state.thread_id {
                    return Err(serde::de::Error::custom("Zerostate thread ID mismatch"));
                }
                Ok((id, state))
            })
            .collect()
    }
}

impl ZeroState {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        let zs: ZeroState = bincode::deserialize(&bytes).map_err(|e| {
            anyhow::format_err!("Failed to load zerostate from {:?}: {e}", path.as_ref())
        })?;
        Ok(zs)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let bytes = bincode::serialize(&self)?;
        std::fs::write(path.as_ref(), bytes).map_err(|e| {
            anyhow::format_err!("Failed to save zerostate to file {:?}: {e}", path.as_ref())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use node_types::ThreadIdentifier;
    use serde::Serialize;

    use super::*;
    use crate::block_keeper_system::BlockKeeperSet;
    use crate::repository::optimistic_state::OptimisticStateImpl;
    use crate::repository::repository_impl::snapshot_v1::State;

    // The pre-upgrade envelope has no format marker and embeds legacy queues.
    #[derive(Serialize)]
    struct LegacyZeroState {
        expires_at: u64,
        states: HashMap<ThreadIdentifier, State>,
        block_keeper_set: HashMap<ThreadIdentifier, BlockKeeperSet>,
    }

    #[test]
    fn zerostate_preserves_legacy_wire_layout() -> anyhow::Result<()> {
        let thread = ThreadIdentifier::default();
        let mut state = OptimisticStateImpl::zero();
        state.thread_id = thread;
        let legacy = bincode::serialize(&LegacyZeroState {
            expires_at: 123456,
            states: HashMap::from([(thread, State::from(state))]),
            block_keeper_set: HashMap::new(),
        })?;
        let loaded: ZeroState = bincode::deserialize(&legacy)?;
        assert_eq!(loaded.expires_at(), 123456);
        assert_eq!(loaded.states().len(), 1);
        assert_eq!(loaded.states()[&thread].thread_id, thread);
        assert_eq!(bincode::serialize(&loaded)?, legacy);
        Ok(())
    }

    #[test]
    #[ignore = "requires LEGACY_ZEROSTATE_PATH"]
    fn load_deployed_legacy_zerostate() -> anyhow::Result<()> {
        let path = std::env::var("LEGACY_ZEROSTATE_PATH")?;
        let loaded = ZeroState::load_from_file(path)?;
        anyhow::ensure!(!loaded.states().is_empty(), "Empty zerostate");
        anyhow::ensure!(!loaded.get_block_keeper_set()?.is_empty(), "Empty BK set");
        Ok(())
    }
}
