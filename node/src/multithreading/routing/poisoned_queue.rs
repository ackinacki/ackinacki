use std::collections::VecDeque;

// A set of messages that were not dispatched since there were
// no destination available at the time of dispatching.
// Note: super inefficient implementation. But quick to implement.
pub struct PoisonedQueue<T> {
    buffer: VecDeque<T>,
    capacity: usize,
}

impl<T> PoisonedQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self { buffer: VecDeque::with_capacity(capacity + 1), capacity }
    }

    pub fn push(&mut self, item: T) {
        self.buffer.push_back(item);
        if self.buffer.len() > self.capacity {
            let _evicted = self.buffer.pop_front();
        }
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.buffer.retain(f)
    }
}
