use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::types::ThreadIdentifier;

#[derive(
    Serialize, Deserialize, Getters, TypedBuilder, Hash, Eq, PartialEq, Clone, Copy, Debug,
)]
pub struct BlockHeight {
    thread_identifier: ThreadIdentifier,
    height: u64,
}

impl BlockHeight {
    pub fn next(&self, thread_identifier: &ThreadIdentifier) -> Self {
        let thread_identifier = *thread_identifier;
        if self.thread_identifier == thread_identifier {
            Self { thread_identifier, height: self.height + 1 }
        } else {
            Self { thread_identifier, height: 0 }
        }
    }

    pub fn signed_distance_to(&self, other: &BlockHeight) -> Option<i128> {
        if self.thread_identifier != other.thread_identifier {
            return None;
        }
        Some(other.height as i128 - self.height as i128)
    }
}
