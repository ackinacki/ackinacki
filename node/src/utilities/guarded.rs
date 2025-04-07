use std::sync::Arc;

use parking_lot::Mutex;

pub trait AllowGuardedMut {
    fn inner_guarded_mut<F, T>(&mut self, action: F) -> T
    where
        F: FnOnce(&mut Self) -> T,
    {
        action(self)
    }
}

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

pub trait TryGuardedMut<Inner> {
    fn try_guarded_mut<F, T>(&self, action: F) -> Option<T>
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

impl<Inner> GuardedMut<Inner> for Arc<Mutex<Inner>>
where
    Inner: AllowGuardedMut,
{
    fn guarded_mut<F, T>(&self, action: F) -> T
    where
        F: FnOnce(&mut Inner) -> T,
    {
        let mut guard = self.lock();
        let result = <Inner as AllowGuardedMut>::inner_guarded_mut(&mut guard, action);
        drop(guard);
        result
    }
}

impl<Inner> TryGuardedMut<Inner> for Arc<Mutex<Inner>>
where
    Inner: AllowGuardedMut,
{
    fn try_guarded_mut<F, T>(&self, action: F) -> Option<T>
    where
        F: FnOnce(&mut Inner) -> T,
    {
        self.try_lock().map(|mut guard| guard.inner_guarded_mut(action))
    }
}
