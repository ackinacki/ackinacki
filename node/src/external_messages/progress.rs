use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Deserialize, Serialize, PartialEq, Getters, Debug)]
#[serde(transparent)]
pub struct Progress {
    last_processed_external_message_index: u32,
}

impl Progress {
    pub fn next(&self, offset_processed: usize) -> Self {
        Self {
            last_processed_external_message_index: self.last_processed_external_message_index
                + offset_processed as u32,
        }
    }

    pub(super) fn zero() -> Self {
        Self { last_processed_external_message_index: 0 }
    }
}
