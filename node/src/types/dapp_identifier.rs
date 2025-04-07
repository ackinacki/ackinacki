// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;
use tvm_block::Serializable;
use tvm_types::BuilderData;

use crate::types::AccountAddress;

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Default)]
pub struct DAppIdentifier(pub AccountAddress);

impl Debug for DAppIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::ops::BitAnd for &'_ DAppIdentifier {
    type Output = DAppIdentifier;

    fn bitand(self, rhs: Self) -> Self::Output {
        DAppIdentifier(&self.0 & &rhs.0)
    }
}

impl Serializable for DAppIdentifier {
    fn write_to(&self, cell: &mut BuilderData) -> tvm_types::Result<()> {
        self.0 .0.write_to(cell)
    }
}
