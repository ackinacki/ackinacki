// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::cmp::Ordering;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Stamp {
    pub(crate) index: u64,
    pub(crate) timestamp: DateTime<Utc>,
}

impl PartialEq for Stamp {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for Stamp {}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Stamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index.cmp(&other.index)
    }
}
