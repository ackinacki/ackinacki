#[cfg(not(feature = "disable-live-counting"))]
mod imp {
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;

    static LIVE_NODES: AtomicI64 = AtomicI64::new(0);

    #[derive(Debug, PartialEq, Eq)]
    pub struct LiveMultiMapNodeCounter;

    impl LiveMultiMapNodeCounter {
        pub fn new() -> Self {
            LIVE_NODES.fetch_add(1, Ordering::Relaxed);
            Self
        }
    }

    impl Default for LiveMultiMapNodeCounter {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Clone for LiveMultiMapNodeCounter {
        fn clone(&self) -> Self {
            Self::new()
        }
    }

    impl Drop for LiveMultiMapNodeCounter {
        fn drop(&mut self) {
            LIVE_NODES.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn live_multimap_nodes() -> i64 {
        LIVE_NODES.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "disable-live-counting")]
mod imp {
    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct LiveMultiMapNodeCounter;

    impl LiveMultiMapNodeCounter {
        #[allow(dead_code)]
        pub fn new() -> Self {
            Self
        }
    }

    pub fn live_multimap_nodes() -> i64 {
        0
    }
}

pub use imp::live_multimap_nodes;
pub(crate) use imp::LiveMultiMapNodeCounter;
