// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;

use crate::message::Message;

#[cfg(test)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MessageStub {
    account_id: u64,
}

#[cfg(test)]
impl MessageStub {
    pub fn new(id: u64) -> Self {
        Self { account_id: id }
    }
}

#[cfg(test)]
impl Message for MessageStub {
    type AccountId = u64;

    //    fn is_internal(&self) -> bool {
    //        false
    //    }

    fn destination(&self) -> Self::AccountId {
        self.account_id
    }
}
