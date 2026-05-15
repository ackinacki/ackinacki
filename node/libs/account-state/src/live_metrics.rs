#[cfg(not(feature = "disable-live-counting"))]
mod imp {
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    static LIVE_THREAD_ACCOUNTS: AtomicI64 = AtomicI64::new(0);
    static LIVE_THREAD_ACCOUNT_STATES: AtomicI64 = AtomicI64::new(0);
    static LIVE_PENDING_UPDATES: AtomicI64 = AtomicI64::new(0);
    static LIVE_ACCUMULATED_UPDATES: AtomicI64 = AtomicI64::new(0);
    static UNFINALIZED_POOL_ACTIVE: AtomicU64 = AtomicU64::new(0);
    static UNFINALIZED_POOL_DRAINING: AtomicU64 = AtomicU64::new(0);

    macro_rules! live_counter {
        ($guard:ident, $counter:ident, $getter:ident) => {
            #[derive(Debug, PartialEq, Eq)]
            pub struct $guard;

            impl $guard {
                pub fn new() -> Self {
                    $counter.fetch_add(1, Ordering::Relaxed);
                    Self
                }
            }

            impl Default for $guard {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl Clone for $guard {
                fn clone(&self) -> Self {
                    Self::new()
                }
            }

            impl Drop for $guard {
                fn drop(&mut self) {
                    $counter.fetch_sub(1, Ordering::Relaxed);
                }
            }

            pub fn $getter() -> i64 {
                $counter.load(Ordering::Relaxed)
            }
        };
    }

    live_counter!(LiveThreadAccountCounter, LIVE_THREAD_ACCOUNTS, live_thread_accounts);
    live_counter!(
        LiveThreadAccountsStateCounter,
        LIVE_THREAD_ACCOUNT_STATES,
        live_thread_account_states
    );
    live_counter!(LivePendingUpdateCounter, LIVE_PENDING_UPDATES, live_pending_updates);
    live_counter!(LiveAccumulatedUpdateCounter, LIVE_ACCUMULATED_UPDATES, live_accumulated_updates);

    pub fn set_unfinalized_pool_sizes(active: u64, draining: u64) {
        UNFINALIZED_POOL_ACTIVE.store(active, Ordering::Relaxed);
        UNFINALIZED_POOL_DRAINING.store(draining, Ordering::Relaxed);
    }

    pub fn unfinalized_pool_active_size() -> u64 {
        UNFINALIZED_POOL_ACTIVE.load(Ordering::Relaxed)
    }

    pub fn unfinalized_pool_draining_size() -> u64 {
        UNFINALIZED_POOL_DRAINING.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "disable-live-counting")]
mod imp {
    macro_rules! noop_counter {
        ($guard:ident, $getter:ident) => {
            #[derive(Debug, Default, Clone, PartialEq, Eq)]
            pub struct $guard;

            impl $guard {
                #[allow(dead_code)]
                pub fn new() -> Self {
                    Self
                }
            }

            pub fn $getter() -> i64 {
                0
            }
        };
    }

    noop_counter!(LiveThreadAccountCounter, live_thread_accounts);
    noop_counter!(LiveThreadAccountsStateCounter, live_thread_account_states);
    noop_counter!(LivePendingUpdateCounter, live_pending_updates);
    noop_counter!(LiveAccumulatedUpdateCounter, live_accumulated_updates);

    pub fn set_unfinalized_pool_sizes(_active: u64, _draining: u64) {}
    pub fn unfinalized_pool_active_size() -> u64 {
        0
    }
    pub fn unfinalized_pool_draining_size() -> u64 {
        0
    }
}

pub use imp::live_accumulated_updates;
pub use imp::live_pending_updates;
pub use imp::live_thread_account_states;
pub use imp::live_thread_accounts;
pub(crate) use imp::set_unfinalized_pool_sizes;
pub use imp::unfinalized_pool_active_size;
pub use imp::unfinalized_pool_draining_size;
pub(crate) use imp::LiveAccumulatedUpdateCounter;
pub(crate) use imp::LivePendingUpdateCounter;
pub(crate) use imp::LiveThreadAccountCounter;
pub(crate) use imp::LiveThreadAccountsStateCounter;
