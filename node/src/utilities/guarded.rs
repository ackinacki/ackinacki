use std::sync::Arc;

use parking_lot::Mutex;

// TODO: replace if similar fn exists in std
pub trait Guarded<Inner> {
    fn guarded<F, T>(&self, action: F) -> T
    where
        F: FnOnce(&Inner) -> T;
}

pub trait GuardedMut<Inner> {
    fn guarded_mut<F, T>(&self, action: F) -> T
    where
        F: FnOnce(&mut Inner) -> T;
}

impl<Inner> Guarded<Inner> for Arc<Mutex<Inner>> {
    fn guarded<F, T>(&self, action: F) -> T
    where
        F: FnOnce(&Inner) -> T,
    {
        let guard = self.lock();
        let result = action(&guard);
        drop(guard);
        result
    }
}

impl<Inner> GuardedMut<Inner> for Arc<Mutex<Inner>> {
    fn guarded_mut<F, T>(&self, action: F) -> T
    where
        F: FnOnce(&mut Inner) -> T,
    {
        let mut guard = self.lock();
        let result = action(&mut guard);
        drop(guard);
        result
    }
}
