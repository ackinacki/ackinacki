// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub static DAPP_CONFIG_ABI: &str =
    include_str!("../../../contracts/dappconfig/DappConfig.abi.json");
pub static DAPP_ROOT_ABI: &str = include_str!("../../../contracts/dappconfig/DappRoot.abi.json");
pub static DAPP_CONFIG_TVC: &[u8] = include_bytes!("../../../contracts/dappconfig/DappConfig.tvc");
