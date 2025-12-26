use tvm_client::abi::Error;

/// Wraps parsed ABI and TVC bytes of the smart contract.
/// Introduced to avoid tvm_abi functions that requires parsing ABI on each call.
/// Pattern to use this struct is:
/// ```rs
/// use crate::types::TvmContract;
/// use std::sync::LazyLock;
///
/// pub static EPOCH_CONTRACT: LazyLock<TvmContract> = LazyLock::new(|| {
///     TvmContract::new(
///         include_str!("../../../contracts/0.79.3_compiled/bksystem/BlockKeeperEpochContract.abi.json"),
///         include_bytes!("../../../contracts/0.79.3_compiled/bksystem/BlockKeeperEpochContract.tvc"),
///     )
/// });
/// ```
pub struct TvmContract {
    pub sdk_abi: tvm_client::abi::Abi,
    pub abi: tvm_abi::Contract,
    pub tvc: &'static [u8],
}

impl TvmContract {
    pub fn new(abi_json_str: &str, tvc: &'static [u8]) -> Self {
        let sdk_abi = tvm_client::abi::Abi::Json(abi_json_str.to_string());
        Self {
            sdk_abi,
            abi: tvm_abi::Contract::load(abi_json_str.as_bytes())
                .map_err(Error::invalid_json)
                .unwrap(),
            tvc,
        }
    }
}
