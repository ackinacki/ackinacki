use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use node_types::BlockIdentifier;
use parking_lot::RwLock;

use super::state::AckiNackiBlockState;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::guarded::TryGuardedMut;

pub enum StateSaveCommand {
    Save(Arc<BlockStateInner>),
    Shutdown,
}

pub struct BlockStateInner {
    pub(super) block_identifier: BlockIdentifier,
    pub(super) shared_access: RwLock<AckiNackiBlockState>,
}

impl Hash for BlockStateInner {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.block_identifier.hash(state)
    }
}

impl PartialEq for BlockStateInner {
    fn eq(&self, other: &Self) -> bool {
        self.block_identifier == other.block_identifier
    }
}

impl Eq for BlockStateInner {}

#[cfg(feature = "fail_on_long_lock")]
const MAX_LOCK_TIME_MS: u64 = 20;
const TRACE_LONG_BLOCK_STATE_LOCK_MS: u128 = 50;

impl Guarded<AckiNackiBlockState> for Arc<BlockStateInner> {
    #[track_caller]
    fn guarded<F, T>(&self, action: F) -> T
    where
        F: FnOnce(&AckiNackiBlockState) -> T,
    {
        let wait_started_at = std::time::Instant::now();
        let guard = self.shared_access.read();
        let wait_elapsed = wait_started_at.elapsed();
        #[cfg(feature = "fail_on_long_lock")]
        let start = std::time::Instant::now();
        let hold_started_at = std::time::Instant::now();
        let result = action(&guard);
        drop(guard);
        let hold_elapsed = hold_started_at.elapsed();
        if wait_elapsed.as_millis() > TRACE_LONG_BLOCK_STATE_LOCK_MS
            || hold_elapsed.as_millis() > TRACE_LONG_BLOCK_STATE_LOCK_MS
        {
            tracing::trace!(
                "block_state_lock timing: kind=read block_id={:?} wait_ms={} hold_ms={} caller={}",
                self.block_identifier,
                wait_elapsed.as_millis(),
                hold_elapsed.as_millis(),
                std::panic::Location::caller(),
            );
        }
        #[cfg(feature = "fail_on_long_lock")]
        if start.elapsed() > std::time::Duration::from_millis(MAX_LOCK_TIME_MS) {
            eprintln!("{:?}", std::backtrace::Backtrace::force_capture());
            eprintln!("Block state lock has taken too long");
            // panic!("Block state lock has taken too long");
        }
        result
    }
}

impl GuardedMut<AckiNackiBlockState> for Arc<BlockStateInner> {
    #[track_caller]
    fn guarded_mut<F, T>(&self, action: F) -> T
    where
        F: FnOnce(&mut AckiNackiBlockState) -> T,
    {
        let wait_started_at = std::time::Instant::now();
        let mut guard = self.shared_access.write();
        let wait_elapsed = wait_started_at.elapsed();
        #[cfg(feature = "fail_on_long_lock")]
        let start = std::time::Instant::now();
        let hold_started_at = std::time::Instant::now();
        let result = guard.inner_guarded_mut(action);
        drop(guard);
        let hold_elapsed = hold_started_at.elapsed();
        if wait_elapsed.as_millis() > TRACE_LONG_BLOCK_STATE_LOCK_MS
            || hold_elapsed.as_millis() > TRACE_LONG_BLOCK_STATE_LOCK_MS
        {
            tracing::trace!(
                "block_state_lock timing: kind=write block_id={:?} wait_ms={} hold_ms={} caller={}",
                self.block_identifier,
                wait_elapsed.as_millis(),
                hold_elapsed.as_millis(),
                std::panic::Location::caller(),
            );
        }
        #[cfg(feature = "fail_on_long_lock")]
        if start.elapsed() > std::time::Duration::from_millis(MAX_LOCK_TIME_MS) {
            eprintln!("{:?}", std::backtrace::Backtrace::force_capture());
            panic!("Block state lock has taken too long");
        }

        result
    }
}

impl TryGuardedMut<AckiNackiBlockState> for Arc<BlockStateInner> {
    #[track_caller]
    fn try_guarded_mut<F, T>(&self, action: F) -> Option<T>
    where
        F: FnOnce(&mut AckiNackiBlockState) -> T,
    {
        self.shared_access.try_write().map(|mut guard| guard.inner_guarded_mut(action))
    }
}
