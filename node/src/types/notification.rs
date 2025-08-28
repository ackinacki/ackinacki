use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

static ID_COUNTER: AtomicU32 = AtomicU32::new(1);

// We expect to have very few notifier objects
#[derive(Clone)]
pub struct Notification {
    id: u32,
    notifications: Arc<(parking_lot::Mutex<u32>, parking_lot::Condvar)>,
}

impl Notification {
    // Note: Do not implement Default trait for this type.
    // Number of objects of this type must be very limited
    // and adding Default::default implementation for it would confuse users.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { id: ID_COUNTER.fetch_add(1, Ordering::SeqCst), notifications: Default::default() }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn touch(&mut self) {
        let mut e = self.notifications.0.lock();
        *e = e.wrapping_add(1);
        self.notifications.1.notify_all();
    }

    pub fn stamp(&self) -> u32 {
        let value = self.notifications.0.lock();
        *value
    }

    pub fn wait_for_updates(&mut self, stamp: u32) {
        let mut notification = self.notifications.0.lock();
        self.notifications.1.wait_while(&mut notification, |e| *e == stamp);
    }

    pub fn wait_for_updates_timeout(&mut self, stamp: u32, timeout: Duration) {
        let mut notification = self.notifications.0.lock();
        self.notifications.1.wait_while_for(&mut notification, |e| *e == stamp, timeout);
    }
}
