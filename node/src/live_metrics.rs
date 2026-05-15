#[cfg(not(feature = "disable-live-counting"))]
mod imp {
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;

    static LIVE_ACKI_NACKI_BLOCKS: AtomicI64 = AtomicI64::new(0);
    static LIVE_OPTIMISTIC_STATE_IMPLS: AtomicI64 = AtomicI64::new(0);
    static LIVE_BLOCK_STATES: AtomicI64 = AtomicI64::new(0);

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

    live_counter!(LiveAckiNackiBlockCounter, LIVE_ACKI_NACKI_BLOCKS, live_acki_nacki_blocks);
    live_counter!(
        LiveOptimisticStateImplCounter,
        LIVE_OPTIMISTIC_STATE_IMPLS,
        live_optimistic_state_impls
    );
    live_counter!(LiveBlockStateCounter, LIVE_BLOCK_STATES, live_block_states);
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

    noop_counter!(LiveAckiNackiBlockCounter, live_acki_nacki_blocks);
    noop_counter!(LiveOptimisticStateImplCounter, live_optimistic_state_impls);
    noop_counter!(LiveBlockStateCounter, live_block_states);
}

pub use imp::live_acki_nacki_blocks;
pub use imp::live_block_states;
pub use imp::live_optimistic_state_impls;
pub(crate) use imp::LiveAckiNackiBlockCounter;
pub(crate) use imp::LiveBlockStateCounter;
pub(crate) use imp::LiveOptimisticStateImplCounter;
