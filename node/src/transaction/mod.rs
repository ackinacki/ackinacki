// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[cfg(test)]
use ::mockall::automock;

#[cfg_attr(test, automock)]
pub trait Transaction {
    fn is_internal(&self) -> bool;
}

impl Transaction for tvm_block::Transaction {
    fn is_internal(&self) -> bool {
        todo!()
    }
}
