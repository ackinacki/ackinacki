// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub static DAPP_ROOT_ADDR: &str = include_str!("../../../contracts/dappconfig/DappRoot.addr");
pub static DAPP_CONFIG_ABI: &str =
    include_str!("../../../contracts/0.79.3_compiled/dappconfig/DappConfig.abi.json");
pub static DAPP_ROOT_ABI: &str =
    include_str!("../../../contracts/0.79.3_compiled/dappconfig/DappRoot.abi.json");
pub static DAPP_CONFIG_TVC: &[u8] =
    include_bytes!("../../../contracts/0.79.3_compiled/dappconfig/DappConfig.tvc");
