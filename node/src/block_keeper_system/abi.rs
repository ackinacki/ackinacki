// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub static EPOCH_ABI: &str =
    include_str!("../../../contracts/bksystem/BlockKeeperEpochContract.abi.json");

pub static PREEPOCH_ABI: &str =
    include_str!("../../../contracts/bksystem/BlockKeeperPreEpochContract.abi.json");

pub static BLOCK_KEEPER_WALLET_ABI: &str =
    include_str!("../../../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json");
pub static BLOCK_KEEPER_WALLET_TVC: &[u8] =
    include_bytes!("../../../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.tvc");
pub static BLOCK_MANAGER_LICENSE_ABI: &str =
    include_str!("../../../contracts/bksystem/LicenseBM.abi.json");
