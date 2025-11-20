// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub static EPOCH_ABI: &str =
    include_str!("../../../contracts/0.79.3_compiled/bksystem/BlockKeeperEpochContract.abi.json");

pub static PREEPOCH_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/bksystem/BlockKeeperPreEpochContract.abi.json"
);

pub static BLOCK_KEEPER_COOLER_ABI: &str =
    include_str!("../../../contracts/0.79.3_compiled/bksystem/BlockKeeperCoolerContract.abi.json");
pub static BLOCK_KEEPER_WALLET_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json"
);
pub static BLOCK_KEEPER_WALLET_TVC: &[u8] = include_bytes!(
    "../../../contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.tvc"
);
pub static BLOCK_MANAGER_LICENSE_ABI: &str =
    include_str!("../../../contracts/0.79.3_compiled/bksystem/LicenseBM.abi.json");
