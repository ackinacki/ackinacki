// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[cfg(test)]
use crate::account::MockAccount;

#[cfg(test)]
impl Clone for MockAccount {
    fn clone(&self) -> Self {
        MockAccount::default()
    }

    fn clone_from(&mut self, _source: &Self) {
        *self = MockAccount::default();
    }
}
