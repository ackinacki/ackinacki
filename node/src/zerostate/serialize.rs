// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use bincode::Options;

const MAGIC: &[u8; 7] = b"ZEROSV2";

use crate::zerostate::ZeroState;

impl ZeroState {
    /// Load an execution base without modifying the source file or archive.
    /// Editing tools should use `load_from_file` to preserve file roundtrips.
    pub fn load_for_execution<P: AsRef<Path>>(
        path: P,
        accounts: &account_state::ThreadAccountsRepository,
    ) -> anyhow::Result<Self> {
        use crate::repository::optimistic_state::OptimisticState;

        let mut zs = Self::load_from_file(path)?;
        for (thread_id, state) in zs.states_mut() {
            let prepared = accounts.prepare_zerostate(thread_id, &state.get_shard_state())?;
            state.set_shard_state(prepared);
        }
        Ok(zs)
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        anyhow::ensure!(bytes.starts_with(MAGIC), "Unsupported zerostate format: ZEROSV2 required");
        let zs: ZeroState = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .reject_trailing_bytes()
            .deserialize(&bytes[MAGIC.len()..])
            .map_err(|e| {
                anyhow::format_err!("Failed to load zerostate from {:?}: {e}", path.as_ref())
            })?;
        for (id, state) in zs.states() {
            anyhow::ensure!(*id == state.thread_id, "Zerostate thread ID mismatch");
        }
        Ok(zs)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.states().values().all(|state| state.shard_state.0.durable.map().is_empty()),
            "Cannot save an execution zerostate: the file format stores only TVM accounts"
        );
        let mut bytes = MAGIC.to_vec();
        bytes.extend(bincode::serialize(self)?);
        std::fs::write(path.as_ref(), bytes).map_err(|e| {
            anyhow::format_err!("Failed to save zerostate to file {:?}: {e}", path.as_ref())
        })
    }
}

#[cfg(test)]
mod tests {
    use account_state::ThreadAccountsRepository;
    use node_types::ThreadIdentifier;

    use super::*;
    use crate::repository::optimistic_state::OptimisticStateImpl;

    #[test]
    fn execution_zerostate_preserves_accounts_and_cannot_be_saved() -> anyhow::Result<()> {
        use crate::repository::optimistic_state::OptimisticState;
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("zerostate");
        let accounts = ThreadAccountsRepository::builder(directory.path().join("archive"))
            .set_apply_to_durable(false)
            .build()?;
        let thread = ThreadIdentifier::default();
        let id = node_types::AccountIdentifier::new([1; 32]);
        let account = tvm_block::Account::with_address(tvm_block::MsgAddressInt::with_standart(
            None,
            0,
            tvm_types::AccountId::from_raw(id.as_slice().to_vec(), 256),
        )?);
        let shard = tvm_block::ShardAccount::with_params(&account, [1; 32].into(), 1, None)?;
        let mut state = OptimisticStateImpl::zero();
        let mut builder = accounts.state_builder(&thread, 0, &state.get_shard_state());
        builder.insert_account(&id.redirect(), &account_state::ThreadAccount::from_tvm(shard));
        state.set_shard_state(builder.build(None)?.new_state);
        let mut raw = ZeroState::default();
        raw.states_mut().insert(thread, state);
        raw.save_to_file(&input)?;
        let source = std::fs::read(&input)?;
        let raw = ZeroState::load_from_file(&input)?;
        let zs = ZeroState::load_for_execution(&input, &accounts)?;
        let mut count = 0;
        for (thread_id, raw_state) in raw.states() {
            let prepared = zs.state(thread_id)?.get_shard_state();
            raw_state.get_shard_state().tvm.iterate_accounts(|id, account| {
                let actual = accounts.state_account(&prepared, &id.redirect())?.unwrap();
                assert_eq!(actual.account_hash(), account.account_hash());
                count += 1;
                Ok(true)
            })?;
            prepared.tvm.iterate_accounts(|_, _| {
                anyhow::bail!("Execution zerostate retained a TVM account")
            })?;
            assert_eq!(raw_state.block_id, zs.state(thread_id)?.block_id);
        }
        assert!(count > 0, "Fixture must contain accounts");
        assert!(zs.save_to_file(&input).is_err());
        assert_eq!(std::fs::read(&input)?, source);
        let again = ZeroState::load_for_execution(&input, &accounts)?;
        for (thread_id, state) in zs.states() {
            assert_eq!(
                state.get_shard_state().durable.map().hash(),
                again.state(thread_id)?.get_shard_state().durable.map().hash(),
            );
        }
        Ok(())
    }

    #[test]
    fn current_zerostate_roundtrips_and_rejects_unversioned_or_trailing_bytes() -> anyhow::Result<()>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("zerostate");
        let mut state = ZeroState::default();
        state.states_mut().insert(ThreadIdentifier::default(), OptimisticStateImpl::zero());
        state.save_to_file(&path)?;
        assert_eq!(ZeroState::load_from_file(&path)?.states().len(), 1);
        let current = std::fs::read(&path)?;
        std::fs::write(&path, &current[MAGIC.len()..])?;
        assert!(ZeroState::load_from_file(&path).is_err());
        let mut trailing = current;
        trailing.push(1);
        std::fs::write(&path, trailing)?;
        assert!(ZeroState::load_from_file(&path).is_err());
        Ok(())
    }
}
