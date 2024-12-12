use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;

use super::blk_prev_info_format::BlkPrevInfoFormat;

#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlockInfo {
    #[serde_as(as = "BlkPrevInfoFormat")]
    inner: tvm_block::BlkPrevInfo,
}

impl std::ops::Deref for BlockInfo {
    type Target = tvm_block::BlkPrevInfo;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for BlockInfo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<tvm_block::BlkPrevInfo> for BlockInfo {
    fn from(value: tvm_block::BlkPrevInfo) -> Self {
        Self { inner: value }
    }
}

#[derive(Ord, PartialEq, Eq, PartialOrd, Serialize, Deserialize, Debug, Clone)]
pub struct BlockEndLT(pub u64);
