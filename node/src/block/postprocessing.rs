use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
#[cfg(feature = "bk_wallets_repair")]
use std::str::FromStr;
use std::sync::Arc;

use tracing::instrument;
#[cfg(feature = "bk_wallets_repair")]
use tvm_block::Account;
#[cfg(feature = "bk_wallets_repair")]
use tvm_block::Deserializable;
#[cfg(feature = "bk_wallets_repair")]
use tvm_block::Message;
use tvm_block::MsgAddrStd;
#[cfg(feature = "bk_wallets_repair")]
use tvm_block::MsgAddressInt;
#[cfg(feature = "bk_wallets_repair")]
use tvm_block::Serializable;
#[cfg(feature = "bk_wallets_repair")]
use tvm_block::ShardAccount;
#[cfg(feature = "bk_wallets_repair")]
use tvm_block::ShardAccounts;
use tvm_types::AccountId;
#[cfg(feature = "bk_wallets_repair")]
use tvm_types::UInt256;

#[cfg(feature = "bk_wallets_repair")]
use crate::block_keeper_system::abi::BLOCK_KEEPER_COOLER_ABI;
use crate::config::FIX_STAKE_BLOCK_SEQ_NO;
#[cfg(feature = "bk_wallets_repair")]
use crate::config::REPAIR_BK_WALLETS_BLOCK_SEQ_NO;
#[cfg(feature = "mirror_repair")]
use crate::config::REPAIR_MIRRORS_BLOCK_SEQ_NO;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;
use crate::types::AccountInbox;
use crate::types::AccountRouting;
use crate::types::BlockIdentifier;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_85EEE8FE56E3F77470A982870DE2DAD0014AA355AAD4A052C17468DF945C31D3: &[u8] = include_bytes!("./../../repair_accounts_data/85eee8fe56e3f77470a982870de2dad0014aa355aad4a052c17468df945c31d3_cc63be63625a164dbd9888be1ba5d3f2d0c0f19b634f3592ae605684ed7005f6");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_7ABB85F52411C0EE3B372996BC54526BCFB7FC232236101AD9B9248E6E046D77: &[u8] = include_bytes!("./../../repair_accounts_data/7abb85f52411c0ee3b372996bc54526bcfb7fc232236101ad9b9248e6e046d77_8ef1880f91b6807ae998dcbf390e408dcf8ade189725a7868ed7704d00b56289");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_C0A07D04D40F719E02914F05918B389229D6CB6499DA8DFE86201AE07FB7B8C9: &[u8] = include_bytes!("./../../repair_accounts_data/c0a07d04d40f719e02914f05918b389229d6cb6499da8dfe86201ae07fb7b8c9_339fb0e3b582028680804bd10221d6f1d73ac92950ec899f38197408eb505938");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_BB89CF85F1A5C8521F879E9BAD83257DB4C9036CE26951C0AE1AA930B6DD1486: &[u8] = include_bytes!("./../../repair_accounts_data/bb89cf85f1a5c8521f879e9bad83257db4c9036ce26951c0ae1aa930b6dd1486_149b1168f982a62c477d58d02827838062b2fe80129809979077434408c31c8d");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_3DB40895E2CE599E425F810E768E7FF005526FA40DDE248E124D147904E6CC2C: &[u8] = include_bytes!("./../../repair_accounts_data/3db40895e2ce599e425f810e768e7ff005526fa40dde248e124d147904e6cc2c_a38a9060afd8572498976ddd9e289c255a3fd92d984889097acde75e9f477a81");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_2018A2548CE0B107774BDB623DC4FA30CCF38E7269288C2093C555F416FAD797: &[u8] = include_bytes!("./../../repair_accounts_data/2018a2548ce0b107774bdb623dc4fa30ccf38e7269288c2093c555f416fad797_6ea18bb25709c6763fd6182ad270b04b58d60a10171fd766c262b464df765fc5");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_16E4BB3531A5600BF654D89ED13C33F6E6B1B06A9210A6B367ECCBEC3C36CF21: &[u8] = include_bytes!("./../../repair_accounts_data/16e4bb3531a5600bf654d89ed13c33f6e6b1b06a9210a6b367eccbec3c36cf21_3cfbf27e3c23b497936d891ebcf67133cc433adbabe493ebf6b342fa63e0c75a");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_8A5A1110E1C873ABCEA15A8AB74AB08D5514CDCA5E3B507179E4D0F5AD532193: &[u8] = include_bytes!("./../../repair_accounts_data/8a5a1110e1c873abcea15a8ab74ab08d5514cdca5e3b507179e4d0f5ad532193_831fd0a8f629bd5673d8c0938ce982defe0a1bb0d6ed4081d282d44d69e453c6");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_F9734BF55DE573030097D12C6B95AF6525DD1EE9E44A6419F78D183C3B79D394: &[u8] = include_bytes!("./../../repair_accounts_data/f9734bf55de573030097d12c6b95af6525dd1ee9e44a6419f78d183c3b79d394_23f1dfbe7019649612e9e5f2550a340bf87ccb1cd5c4f771d2d6c675452718de");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_FF0E03891282FED4AAD30B1B522E4B6F0EFC6FDA970BAB2940E18425ED1BAED6: &[u8] = include_bytes!("./../../repair_accounts_data/ff0e03891282fed4aad30b1b522e4b6f0efc6fda970bab2940e18425ed1baed6_3db2754f210f3d8fc4f696cbb0a14120cc4a848119fc0d193b51f4bd365b3eb4");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_52CA31E0FC400EE3EF8EACED777F9DF3D9B350221835CB0B7B7DAD59271D214E: &[u8] = include_bytes!("./../../repair_accounts_data/52ca31e0fc400ee3ef8eaced777f9df3d9b350221835cb0b7b7dad59271d214e_6218ef2d827095f7dedd2e37c134d8fe95542b24d5f47a0ed331138c976b2f31");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_D875DF9CF418106D1407F60BE189ADC9E7C0E6DB430AE945FFA37448EF46469F: &[u8] = include_bytes!("./../../repair_accounts_data/d875df9cf418106d1407f60be189adc9e7c0e6db430ae945ffa37448ef46469f_3227713c1fe1656f2aa429db5fe754b95f1bf326cab28e8d9105b6abab42e881");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_D84BDFA753950541BAFAC1E8137BBA14FA6AA55409D9EE33252D442B4E3E682A: &[u8] = include_bytes!("./../../repair_accounts_data/d84bdfa753950541bafac1e8137bba14fa6aa55409d9ee33252d442b4e3e682a_f0583771a50f286e88e7d7dfdbf2fb807aecf9f2c5d54985ad8da92197cc6857");
#[cfg(feature = "bk_wallets_repair")]
pub static ACCOUNT_DATA_1033EA894040ADBA26F13680690FAF3E05F74987610E4F5D50B22142333A8787: &[u8] = include_bytes!("./../../repair_accounts_data/1033ea894040adba26f13680690faf3e05f74987610e4f5d50b22142333a8787_2f0f1240171f5ca39ad87df6410bc0e821aa94469a85075cdc5e145c5add4038");
#[cfg(feature = "mirror_repair")]
pub static MIRROR_TVC: &[u8] =
    include_bytes!("../../../contracts/0.79.3_compiled/mvsystem/Mirror.tvc");

#[cfg(feature = "bk_wallets_repair")]
lazy_static::lazy_static!(
    static ref ACCOUNTS_TO_REPAIR: HashMap<AccountAddress, Account> = {
        let mut m = HashMap::new();
        m.insert(AccountAddress::from_str("85eee8fe56e3f77470a982870de2dad0014aa355aad4a052c17468df945c31d3").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_85EEE8FE56E3F77470A982870DE2DAD0014AA355AAD4A052C17468DF945C31D3).unwrap());
        m.insert(AccountAddress::from_str("7abb85f52411c0ee3b372996bc54526bcfb7fc232236101ad9b9248e6e046d77").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_7ABB85F52411C0EE3B372996BC54526BCFB7FC232236101AD9B9248E6E046D77).unwrap());
        m.insert(AccountAddress::from_str("c0a07d04d40f719e02914f05918b389229d6cb6499da8dfe86201ae07fb7b8c9").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_C0A07D04D40F719E02914F05918B389229D6CB6499DA8DFE86201AE07FB7B8C9).unwrap());
        m.insert(AccountAddress::from_str("bb89cf85f1a5c8521f879e9bad83257db4c9036ce26951c0ae1aa930b6dd1486").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_BB89CF85F1A5C8521F879E9BAD83257DB4C9036CE26951C0AE1AA930B6DD1486).unwrap());
        m.insert(AccountAddress::from_str("3db40895e2ce599e425f810e768e7ff005526fa40dde248e124d147904e6cc2c").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_3DB40895E2CE599E425F810E768E7FF005526FA40DDE248E124D147904E6CC2C).unwrap());
        m.insert(AccountAddress::from_str("2018a2548ce0b107774bdb623dc4fa30ccf38e7269288c2093c555f416fad797").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_2018A2548CE0B107774BDB623DC4FA30CCF38E7269288C2093C555F416FAD797).unwrap());
        m.insert(AccountAddress::from_str("16e4bb3531a5600bf654d89ed13c33f6e6b1b06a9210a6b367eccbec3c36cf21").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_16E4BB3531A5600BF654D89ED13C33F6E6B1B06A9210A6B367ECCBEC3C36CF21).unwrap());
        m.insert(AccountAddress::from_str("8a5a1110e1c873abcea15a8ab74ab08d5514cdca5e3b507179e4d0f5ad532193").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_8A5A1110E1C873ABCEA15A8AB74AB08D5514CDCA5E3B507179E4D0F5AD532193).unwrap());
        m.insert(AccountAddress::from_str("f9734bf55de573030097d12c6b95af6525dd1ee9e44a6419f78d183c3b79d394").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_F9734BF55DE573030097D12C6B95AF6525DD1EE9E44A6419F78D183C3B79D394).unwrap());
        m.insert(AccountAddress::from_str("ff0e03891282fed4aad30b1b522e4b6f0efc6fda970bab2940e18425ed1baed6").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_FF0E03891282FED4AAD30B1B522E4B6F0EFC6FDA970BAB2940E18425ED1BAED6).unwrap());
        m.insert(AccountAddress::from_str("52ca31e0fc400ee3ef8eaced777f9df3d9b350221835cb0b7b7dad59271d214e").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_52CA31E0FC400EE3EF8EACED777F9DF3D9B350221835CB0B7B7DAD59271D214E).unwrap());
        m.insert(AccountAddress::from_str("d875df9cf418106d1407f60be189adc9e7c0e6db430ae945ffa37448ef46469f").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_D875DF9CF418106D1407F60BE189ADC9E7C0E6DB430AE945FFA37448EF46469F).unwrap());
        m.insert(AccountAddress::from_str("d84bdfa753950541bafac1e8137bba14fa6aa55409d9ee33252d442b4e3e682a").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_D84BDFA753950541BAFAC1E8137BBA14FA6AA55409D9EE33252D442B4E3E682A).unwrap());
        m.insert(AccountAddress::from_str("1033ea894040adba26f13680690faf3e05f74987610e4f5d50b22142333a8787").unwrap(), Account::construct_from_bytes(ACCOUNT_DATA_1033EA894040ADBA26F13680690FAF3E05F74987610E4F5D50B22142333A8787).unwrap());
        m
    };
);

#[cfg(feature = "bk_wallets_repair")]
lazy_static::lazy_static!(
    static ref OLD_ACCOUNTS_HASHES: HashMap<AccountAddress, UInt256> = {
        let mut m = HashMap::new();
        m.insert(AccountAddress::from_str("85eee8fe56e3f77470a982870de2dad0014aa355aad4a052c17468df945c31d3").unwrap(), UInt256::from_str("cc63be63625a164dbd9888be1ba5d3f2d0c0f19b634f3592ae605684ed7005f6").unwrap());
        m.insert(AccountAddress::from_str("7abb85f52411c0ee3b372996bc54526bcfb7fc232236101ad9b9248e6e046d77").unwrap(), UInt256::from_str("8ef1880f91b6807ae998dcbf390e408dcf8ade189725a7868ed7704d00b56289").unwrap());
        m.insert(AccountAddress::from_str("c0a07d04d40f719e02914f05918b389229d6cb6499da8dfe86201ae07fb7b8c9").unwrap(), UInt256::from_str("339fb0e3b582028680804bd10221d6f1d73ac92950ec899f38197408eb505938").unwrap());
        m.insert(AccountAddress::from_str("bb89cf85f1a5c8521f879e9bad83257db4c9036ce26951c0ae1aa930b6dd1486").unwrap(), UInt256::from_str("149b1168f982a62c477d58d02827838062b2fe80129809979077434408c31c8d").unwrap());
        m.insert(AccountAddress::from_str("3db40895e2ce599e425f810e768e7ff005526fa40dde248e124d147904e6cc2c").unwrap(), UInt256::from_str("a38a9060afd8572498976ddd9e289c255a3fd92d984889097acde75e9f477a81").unwrap());
        m.insert(AccountAddress::from_str("2018a2548ce0b107774bdb623dc4fa30ccf38e7269288c2093c555f416fad797").unwrap(), UInt256::from_str("6ea18bb25709c6763fd6182ad270b04b58d60a10171fd766c262b464df765fc5").unwrap());
        m.insert(AccountAddress::from_str("16e4bb3531a5600bf654d89ed13c33f6e6b1b06a9210a6b367eccbec3c36cf21").unwrap(), UInt256::from_str("3cfbf27e3c23b497936d891ebcf67133cc433adbabe493ebf6b342fa63e0c75a").unwrap());
        m.insert(AccountAddress::from_str("8a5a1110e1c873abcea15a8ab74ab08d5514cdca5e3b507179e4d0f5ad532193").unwrap(), UInt256::from_str("831fd0a8f629bd5673d8c0938ce982defe0a1bb0d6ed4081d282d44d69e453c6").unwrap());
        m.insert(AccountAddress::from_str("f9734bf55de573030097d12c6b95af6525dd1ee9e44a6419f78d183c3b79d394").unwrap(), UInt256::from_str("23f1dfbe7019649612e9e5f2550a340bf87ccb1cd5c4f771d2d6c675452718de").unwrap());
        m.insert(AccountAddress::from_str("ff0e03891282fed4aad30b1b522e4b6f0efc6fda970bab2940e18425ed1baed6").unwrap(), UInt256::from_str("3db2754f210f3d8fc4f696cbb0a14120cc4a848119fc0d193b51f4bd365b3eb4").unwrap());
        m.insert(AccountAddress::from_str("52ca31e0fc400ee3ef8eaced777f9df3d9b350221835cb0b7b7dad59271d214e").unwrap(), UInt256::from_str("6218ef2d827095f7dedd2e37c134d8fe95542b24d5f47a0ed331138c976b2f31").unwrap());
        m.insert(AccountAddress::from_str("d875df9cf418106d1407f60be189adc9e7c0e6db430ae945ffa37448ef46469f").unwrap(), UInt256::from_str("3227713c1fe1656f2aa429db5fe754b95f1bf326cab28e8d9105b6abab42e881").unwrap());
        m.insert(AccountAddress::from_str("d84bdfa753950541bafac1e8137bba14fa6aa55409d9ee33252d442b4e3e682a").unwrap(), UInt256::from_str("f0583771a50f286e88e7d7dfdbf2fb807aecf9f2c5d54985ad8da92197cc6857").unwrap());
        m.insert(AccountAddress::from_str("1033ea894040adba26f13680690faf3e05f74987610e4f5d50b22142333a8787").unwrap(), UInt256::from_str("2f0f1240171f5ca39ad87df6410bc0e821aa94469a85075cdc5e145c5add4038").unwrap());
        m
    };
);

#[cfg(feature = "bk_wallets_repair")]
lazy_static::lazy_static!(
    static ref OLD_ACCOUNTS_COOLERS: HashMap<AccountAddress, MsgAddressInt> = {
        let mut m = HashMap::new();
        m.insert(AccountAddress::from_str("85eee8fe56e3f77470a982870de2dad0014aa355aad4a052c17468df945c31d3").unwrap(), MsgAddressInt::from_str("7df5b0ae40ecb3802944d4972327b2658f1f7b1f721372a809e32122b37519be").unwrap());
        m.insert(AccountAddress::from_str("7abb85f52411c0ee3b372996bc54526bcfb7fc232236101ad9b9248e6e046d77").unwrap(), MsgAddressInt::from_str("93ebf66e4775421b2dd4d9709eb7cdac1f9e27943af9be1de448d628db9ead5e").unwrap());
        m.insert(AccountAddress::from_str("c0a07d04d40f719e02914f05918b389229d6cb6499da8dfe86201ae07fb7b8c9").unwrap(), MsgAddressInt::from_str("8d7c313ec37a53bbd23c7628816765c596fa13eb428be8eea3bf3937f1d55377").unwrap());
        m.insert(AccountAddress::from_str("bb89cf85f1a5c8521f879e9bad83257db4c9036ce26951c0ae1aa930b6dd1486").unwrap(), MsgAddressInt::from_str("fc00ba1d707432bb3ff850fad0536e5197007eb4afdb932451e87fdee9ef20be").unwrap());
        m.insert(AccountAddress::from_str("3db40895e2ce599e425f810e768e7ff005526fa40dde248e124d147904e6cc2c").unwrap(), MsgAddressInt::from_str("e84a23b7bee882be2c249fdbb0451da6fdd3ffc8bc1c8ed344c9c34c5c1409d8").unwrap());
        m.insert(AccountAddress::from_str("2018a2548ce0b107774bdb623dc4fa30ccf38e7269288c2093c555f416fad797").unwrap(), MsgAddressInt::from_str("f90d09f70de9c68a038fa438f383025741308679c1b1bab3c288b73e294527ae").unwrap());
        m.insert(AccountAddress::from_str("16e4bb3531a5600bf654d89ed13c33f6e6b1b06a9210a6b367eccbec3c36cf21").unwrap(), MsgAddressInt::from_str("06b8a619779f770630fa97efb96b86e03aad5b08b6d0df689057569424ec91b1").unwrap());
        m.insert(AccountAddress::from_str("8a5a1110e1c873abcea15a8ab74ab08d5514cdca5e3b507179e4d0f5ad532193").unwrap(), MsgAddressInt::from_str("019275bd6c8e20e0bc6f83655a7b4be408739a2300001d472c883873beecddd7").unwrap());
        m.insert(AccountAddress::from_str("f9734bf55de573030097d12c6b95af6525dd1ee9e44a6419f78d183c3b79d394").unwrap(), MsgAddressInt::from_str("add8044995027e7a3739bbaf46d0f67a07105eb6f08614b8bbad475e576d7a9d").unwrap());
        m.insert(AccountAddress::from_str("ff0e03891282fed4aad30b1b522e4b6f0efc6fda970bab2940e18425ed1baed6").unwrap(), MsgAddressInt::from_str("d659c47f29be1739990cda435b3f527e05d2a2e7370ac1ba87738277e8ba661e").unwrap());
        m.insert(AccountAddress::from_str("52ca31e0fc400ee3ef8eaced777f9df3d9b350221835cb0b7b7dad59271d214e").unwrap(), MsgAddressInt::from_str("6e58d19af2c6e7ed297aeeb2b4286b4572daa29bec566156b862daae727c7667").unwrap());
        m.insert(AccountAddress::from_str("d875df9cf418106d1407f60be189adc9e7c0e6db430ae945ffa37448ef46469f").unwrap(), MsgAddressInt::from_str("b355a93edbc24b7d8dcad460bb1990351d39f101a88766f547ebc8993d3e271b").unwrap());
        m.insert(AccountAddress::from_str("d84bdfa753950541bafac1e8137bba14fa6aa55409d9ee33252d442b4e3e682a").unwrap(), MsgAddressInt::from_str("a753deebf773035f02ab5f910982b752d26269bbcac8a96ed7d9fc073d53ca2d").unwrap());
        m.insert(AccountAddress::from_str("1033ea894040adba26f13680690faf3e05f74987610e4f5d50b22142333a8787").unwrap(), MsgAddressInt::from_str("41d89b540d263bf718768da69b0bc6301736a849a458833092c25e12c8b1ed1d").unwrap());
        m
    };
);

#[cfg(feature = "bm_license_repair")]
lazy_static::lazy_static!(
    static ref ACCOUNTS_LICENSES: HashMap<AccountAddress, (MsgAddressInt, String)> = {
        let mut m = HashMap::new();
        m.insert(AccountAddress::from_str("681bf89ec5837add54b329f8c7f32a1b97e8fed2964b39dcde8b102976d8ca60").unwrap(), (MsgAddressInt::from_str("b14eecbc0f43aa7186167fdeb985fe00f886a0d8e4b30d07dfb83db1d2ca00f0").unwrap(), "0x64eb087f078c11369731fb3c6ae9cc989d7bffc55ec85ecc2385b3cd051bb900".to_string()));
        m.insert(AccountAddress::from_str("4060ad1a4a00cca47222246788f44d163cf94ff90c38aec70e754693b7bc536a").unwrap(), (MsgAddressInt::from_str("26f9191c1383573c122c97d91e982d897455aabcd4ae37d4860f56c91d4fa884").unwrap(), "0x13f2591b13a85c9c2fd3c2d043a988c90b0240b9eaa59de4ae6dc685ea273b86".to_string()));
        m.insert(AccountAddress::from_str("e6e3c95463ee65c50116ad939582156cdf2c1fc2b0316f6c8214960cb404b95a").unwrap(), (MsgAddressInt::from_str("e2b7f9144ee83d247a2eb83f5099691ee6a0e1b176581206cfbfe901d62f5724").unwrap(), "0x69a453fcd052d779d42255aa73a02b55fbbf74d298c7cbcd99890dc0d9968042".to_string()));
        m.insert(AccountAddress::from_str("0e4e5c47410d8d4e06e7be27f5a9f09e26d50852d2eaaa0c11a3d69552de0ef3").unwrap(), (MsgAddressInt::from_str("542ea7fd5a3413c3766a488a9cefff3215e36d77d518617bf6fbc1db660dd6e4").unwrap(), "0x2eaf65f32cd144943fb3e2e8412ccdd85b46f4734483dd7e42f7e532d54fd571".to_string()));
        m.insert(AccountAddress::from_str("acdfeb444ab154dfdb742f66cec0f10d5b33fa1046842118b11f070b4d46514d").unwrap(), (MsgAddressInt::from_str("f0779f07d84cae9fd2275e7fa26b56cc6a8d9e30bfe93b892c035384eacbcf91").unwrap(), "0x54cb46c63df248ea7b1c959017cf4aba3f0ae7bb6213bd0d8b22e83f4f119e5d".to_string()));
        m.insert(AccountAddress::from_str("0f1ace6db54840377395008a1456e08d9fd4dca4ad5045a14591bd983aeefe2d").unwrap(), (MsgAddressInt::from_str("ecacc3561b4ec562236a13dd495d4174b2af0f62bec774727427787c408bbe86").unwrap(), "0xba4bf55a5a74e22de3c6b9ba3960568380a42ad9831b4f74a34e2a8d343702f8".to_string()));
        m.insert(AccountAddress::from_str("5cbd0371ef9ae39ebf7cabbfecd5389bb34fe8cdcb150bb63e97817049859865").unwrap(), (MsgAddressInt::from_str("1e91b51ce621cfe314542c8e132ce943b126ddd8441973e9bbfccc302b996931").unwrap(), "0x413db96e31993269c8116427b88a84f0f934a300ca3dd97b52eeb9fe676c1d19".to_string()));
        m
    };
);

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn postprocess(
    mut initial_optimistic_state: OptimisticStateImpl,
    consumed_internal_messages: HashMap<AccountAddress, HashSet<MessageIdentifier>>,
    mut produced_internal_messages_to_the_current_thread: HashMap<
        AccountAddress,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    accounts_that_changed_their_dapp_id: HashMap<AccountRouting, Option<WrappedAccount>>,
    block_id: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    mut new_state: OptimisticShardState,
    mut produced_internal_messages_to_other_threads: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    block_info: BlockInfo,
    thread_id: ThreadIdentifier,
    threads_table: ThreadsTable,
    block_accounts: HashSet<AccountAddress>,
    accounts_repo: AccountsRepository,
    db: MessageDurableStorage,
    #[cfg(feature = "monitor-accounts-number")] updated_accounts_number: u64,
) -> anyhow::Result<(OptimisticStateImpl, CrossThreadRefData)> {
    #[cfg(feature = "bk_wallets_repair")]
    {
        if <u32>::from(block_seq_no) == REPAIR_BK_WALLETS_BLOCK_SEQ_NO {
            add_messages_to_repair_coolers(&mut produced_internal_messages_to_the_current_thread);
        }
    }

    #[cfg(feature = "bm_license_repair")]
    {
        if <u32>::from(block_seq_no) == FIX_STAKE_BLOCK_SEQ_NO {
            add_messages_to_repair_bm_license(
                &mut produced_internal_messages_to_the_current_thread,
            );
        }
    }

    // Prepare produced_internal_messages_to_the_current_thread
    for (addr, messages) in produced_internal_messages_to_the_current_thread.iter_mut() {
        let mut sorted = if let Some(consumed_messages) = consumed_internal_messages.get(addr) {
            let ids_set =
                HashSet::<MessageIdentifier>::from_iter(messages.iter().map(|(id, _)| id.clone()));
            let intersection: HashSet<&MessageIdentifier> =
                HashSet::from_iter(ids_set.intersection(consumed_messages));
            let mut consumed_part = vec![];
            messages.retain(|el| {
                if intersection.contains(&el.0) {
                    consumed_part.push((el.0.clone(), el.1.clone()));
                    false
                } else {
                    true
                }
            });
            consumed_part
        } else {
            vec![]
        };
        messages.sort_by(|a, b| a.1.cmp(&b.1));
        sorted.extend(messages.clone());
        *messages = sorted;
    }

    produced_internal_messages_to_other_threads
        .iter_mut()
        .for_each(|(_addr, messages)| messages.sort_by(|a, b| a.1.cmp(&b.1)));

    let mut new_thread_refs = initial_optimistic_state.thread_refs_state.clone();
    let current_thread_id = *initial_optimistic_state.get_thread_id();
    let current_thread_last_block = (current_thread_id, block_id.clone(), block_seq_no);
    new_thread_refs.update(current_thread_id, current_thread_last_block.clone());

    let mut removed_accounts: Vec<AccountAddress> = vec![];
    let mut outbound_accounts: HashMap<
        AccountRouting,
        (Option<WrappedAccount>, Option<AccountInbox>),
    > = HashMap::new();

    let messages = ThreadMessageQueueState::build_next()
        .with_initial_state(initial_optimistic_state.messages)
        .with_consumed_messages(consumed_internal_messages)
        .with_produced_messages(produced_internal_messages_to_the_current_thread)
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .with_db(db.clone())
        .build()?;
    initial_optimistic_state.messages = messages;

    for (routing, account) in &accounts_that_changed_their_dapp_id {
        // Note: we are using input state threads table to determine if this account should be in
        //  the descendant state. In case of thread split those accounts will be cropped into the
        // correct thread.
        if !initial_optimistic_state.does_routing_belong_to_the_state(routing) {
            removed_accounts.push(routing.1.clone());
            let account_inbox =
                initial_optimistic_state.messages.account_inbox(&routing.1).cloned();
            outbound_accounts.insert(routing.clone(), (account.clone(), account_inbox));
        }
    }
    if !outbound_accounts.is_empty() {
        let mut shard_state = new_state.into_shard_state().as_ref().clone();
        let mut shard_accounts = shard_state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read accounts from shard state: {e}"))?;
        for (account_routing, (_, _)) in &outbound_accounts {
            let account_id = account_routing.1 .0.clone();
            // let default_account_routing = AccountRouting(
            //     DAppIdentifier(account_routing.1.clone()),
            //     account_routing.1.clone(),
            // );
            // if initial_optimistic_state.does_routing_belong_to_the_state(&default_account_routing) {

            let acc_id: AccountId = account_id.clone().into();
            if shard_accounts
                .account(&acc_id)
                .map_err(|e| anyhow::format_err!("Failed to check account: {e}"))?
                .is_some()
            {
                tracing::debug!(target: "node", "replace account with redirect: {:?}", account_routing);
                shard_accounts.replace_with_redirect(&account_id).map_err(|e| {
                    anyhow::format_err!("Failed to insert stub to shard state: {e}")
                })?;
            }
            // }
        }
        shard_state
            .write_accounts(&shard_accounts)
            .map_err(|e| anyhow::format_err!("Failed to write accounts to shard state: {e}"))?;
        new_state = shard_state.into();
    }

    let messages = ThreadMessageQueueState::build_next()
        .with_initial_state(initial_optimistic_state.messages)
        .with_consumed_messages(HashMap::new())
        .with_produced_messages(HashMap::new())
        .with_removed_accounts(removed_accounts)
        .with_added_accounts(BTreeMap::new())
        .with_db(db.clone())
        .build()?;
    initial_optimistic_state.messages = messages;

    let mut changed_accounts = initial_optimistic_state.changed_accounts;
    let mut cached_accounts = initial_optimistic_state.cached_accounts;
    if let Some(unload_after) = accounts_repo.get_unload_after() {
        changed_accounts.extend(block_accounts.into_iter().map(|acc| (acc, block_seq_no)));
        cached_accounts.retain(|account_id, (seq_no, _)| {
            if *seq_no + accounts_repo.get_store_after() >= block_seq_no
                && !changed_accounts.contains_key(account_id)
            {
                true
            } else {
                tracing::trace!(
                    account_id = account_id.to_hex_string(),
                    "Removing account from cache"
                );
                false
            }
        });
        let mut shard_state = new_state.into_shard_state().as_ref().clone();
        let mut shard_accounts = shard_state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read accounts from shard state: {e}"))?;
        let mut deleted = Vec::new();
        for (account_id, seq_no) in std::mem::take(&mut changed_accounts) {
            if seq_no + unload_after <= block_seq_no {
                if let Some(mut account) =
                    shard_accounts.account(&(&account_id).into()).map_err(|e| {
                        anyhow::format_err!("Failed to read account from shard state: {e}")
                    })?
                {
                    tracing::trace!(
                        account_id = account_id.to_hex_string(),
                        "Unloading account from state"
                    );
                    let cell = account
                        .replace_with_external()
                        .map_err(|e| anyhow::format_err!("Failed to set account external: {e}"))?;
                    cached_accounts.insert(account_id.clone(), (block_seq_no, cell));
                    shard_accounts.insert(&account_id.0, &account).map_err(|e| {
                        anyhow::format_err!("Failed to insert account into shard state: {e}")
                    })?;
                } else {
                    deleted.push(account_id);
                }
            } else {
                changed_accounts.insert(account_id, seq_no);
            }
        }
        if !deleted.is_empty() {
            accounts_repo.accounts_deleted(&thread_id, deleted, block_info.prev1().unwrap().end_lt);
        }
        shard_state
            .write_accounts(&shard_accounts)
            .map_err(|e| anyhow::format_err!("Failed to write accounts: {e}"))?;
        new_state = shard_state.into();
    }

    #[cfg(feature = "bk_wallets_repair")]
    {
        if <u32>::from(block_seq_no) == REPAIR_BK_WALLETS_BLOCK_SEQ_NO {
            let mut shard_state = new_state.into_shard_state().as_ref().clone();
            let mut shard_accounts = shard_state.read_accounts().map_err(|e| {
                anyhow::format_err!("Failed to read accounts from shard state: {e}")
            })?;

            repair_accounts(&mut shard_accounts)?;

            shard_state
                .write_accounts(&shard_accounts)
                .map_err(|e| anyhow::format_err!("Failed to write accounts: {e}"))?;
            new_state = shard_state.into();
        }
    }

    #[cfg(feature = "mirror_repair")]
    {
        if <u32>::from(block_seq_no) == REPAIR_MIRRORS_BLOCK_SEQ_NO {
            let mut shard_state = new_state.into_shard_state().as_ref().clone();
            let mut shard_accounts = shard_state.read_accounts().map_err(|e| {
                anyhow::format_err!("Failed to read accounts from shard state: {e}")
            })?;

            repair_mirror_accounts(&mut shard_accounts)?;

            shard_state
                .write_accounts(&shard_accounts)
                .map_err(|e| anyhow::format_err!("Failed to write accounts: {e}"))?;
            new_state = shard_state.into();
        }
    }

    #[cfg(feature = "monitor-accounts-number")]
    let new_state = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id.clone())
        .shard_state(new_state)
        .messages(initial_optimistic_state.messages)
        .high_priority_messages(initial_optimistic_state.high_priority_messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped)
        .changed_accounts(changed_accounts)
        .cached_accounts(cached_accounts)
        .accounts_number(updated_accounts_number)
        .build();

    #[cfg(not(feature = "monitor-accounts-number"))]
    let new_state = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id.clone())
        .shard_state(new_state)
        .messages(initial_optimistic_state.messages)
        .high_priority_messages(initial_optimistic_state.high_priority_messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped)
        .changed_accounts(changed_accounts)
        .cached_accounts(cached_accounts)
        .build();

    let cross_thread_ref_data = CrossThreadRefData::builder()
        .block_identifier(block_id.clone())
        .block_seq_no(block_seq_no)
        .block_thread_identifier(thread_id)
        .outbound_messages(produced_internal_messages_to_other_threads)
        .outbound_accounts(outbound_accounts)
        .threads_table(threads_table.clone())
        .parent_block_identifier(initial_optimistic_state.block_id.clone())
        .block_refs(vec![]) // set up later
        .build();

    Ok((new_state, cross_thread_ref_data))
}

#[cfg(feature = "bk_wallets_repair")]
fn repair_accounts(accounts: &mut ShardAccounts) -> anyhow::Result<()> {
    for (account_address, awaited_hash) in OLD_ACCOUNTS_HASHES.iter() {
        let acc_id: AccountId = account_address.clone().into();
        let Some(shard_account) = accounts
            .account(&acc_id)
            .map_err(|e| anyhow::format_err!("Failed to read account from shard state: {e}"))?
        else {
            tracing::error!(
                "bk_wallets_repair: account was not found in state: {account_address:?}"
            );
            continue;
        };
        let old_account = shard_account
            .read_account()
            .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?
            .as_struct()
            .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?;
        let Some(old_data_hash) = old_account.get_data().map(|data| data.repr_hash()) else {
            tracing::error!("bk_wallets_repair: account has no data: {account_address:?}");
            continue;
        };
        if old_data_hash != *awaited_hash {
            tracing::error!(
                "bk_wallets_repair: account data has wrong hash: {account_address:?} - {} != {}",
                old_data_hash.to_hex_string(),
                awaited_hash.to_hex_string()
            );
            continue;
        }
        let Some(new_account) = ACCOUNTS_TO_REPAIR.get(account_address) else {
            tracing::error!("bk_wallets_repair: Failed to get new account");
            continue;
        };
        let Ok(new_account_cell) = new_account.serialize() else {
            tracing::error!("bk_wallets_repair: Failed to serialize new account");
            continue;
        };
        let new_shard_account = ShardAccount::with_account_root(
            new_account_cell,
            shard_account.last_trans_hash().clone(),
            shard_account.last_trans_lt(),
            shard_account.get_dapp_id().cloned(),
        );
        if let Err(e) = accounts.insert(&account_address.0, &new_shard_account) {
            tracing::error!("bk_wallets_repair: Failed to insert new shard account: {e}");
            continue;
        }
        tracing::error!("bk_wallets_repair: Success: {account_address:?}");
    }
    Ok(())
}

#[cfg(feature = "bk_wallets_repair")]
fn add_messages_to_repair_coolers(
    buffer: &mut HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
) {
    for (src, dest) in OLD_ACCOUNTS_COOLERS.iter() {
        let src = MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, 0, src.0.clone().into()));
        use tvm_block::CurrencyCollection;
        use tvm_block::Grams;
        use tvm_block::InternalMessageHeader;
        use tvm_types::SliceData;

        let msg_body = tvm_abi::encode_function_call(
            BLOCK_KEEPER_COOLER_ABI,
            "touch",
            None,
            "{}",
            true,
            None,
            None,
        )
        .expect("Failed to encode body");
        let cc = CurrencyCollection::from_grams(Grams::new(100_000_000).unwrap());
        let message_header = InternalMessageHeader::with_addresses(src.clone(), dest.clone(), cc);
        let body =
            SliceData::load_cell(msg_body.into_cell().expect("Failed serialize message body"))
                .expect("Failed to serialize message body");
        let message = Message::with_int_header_and_body(message_header, body);
        let wrapped_message = WrappedMessage { message };
        let message_id = MessageIdentifier::from(&wrapped_message);
        let destination: AccountAddress = dest.address().into();
        buffer.entry(destination).or_default().push((message_id, Arc::new(wrapped_message)));
    }
}

#[cfg(feature = "bm_license_repair")]
fn add_messages_to_repair_bm_license(
    buffer: &mut HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
) {
    for (src, (dest, wallet_owner_pubkey)) in ACCOUNTS_LICENSES.iter() {
        let src = MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, 0, src.0.clone().into()));
        use tvm_block::CurrencyCollection;
        use tvm_block::Grams;
        use tvm_block::InternalMessageHeader;
        use tvm_types::SliceData;

        use crate::block_keeper_system::abi::BLOCK_MANAGER_LICENSE_ABI;

        let parameters = format!(r#"{{"pubkey": "{wallet_owner_pubkey}"}}"#,);
        let msg_body = tvm_abi::encode_function_call(
            BLOCK_MANAGER_LICENSE_ABI,
            "acceptLicense",
            None,
            &parameters,
            true,
            None,
            None,
        )
        .expect("Failed to encode body");
        let cc = CurrencyCollection::from_grams(Grams::new(100_000_000).unwrap());
        let message_header = InternalMessageHeader::with_addresses(src.clone(), dest.clone(), cc);
        let body =
            SliceData::load_cell(msg_body.into_cell().expect("Failed serialize message body"))
                .expect("Failed to serialize message body");
        let message = Message::with_int_header_and_body(message_header, body);
        let wrapped_message = WrappedMessage { message };
        let message_id = MessageIdentifier::from(&wrapped_message);
        let destination: AccountAddress = dest.address().into();
        buffer.entry(destination).or_default().push((message_id, Arc::new(wrapped_message)));
    }
}

#[cfg(feature = "mirror_repair")]
fn repair_mirror_accounts(accounts: &mut ShardAccounts) -> anyhow::Result<()> {
    use tvm_block::StateInit;

    let mirror_stateinit = StateInit::construct_from_bytes(MIRROR_TVC)
        .map_err(|e| anyhow::format_err!("Failed to construct mirror tvc: {e}"))?;

    let mirror_code = mirror_stateinit
        .code()
        .ok_or_else(|| anyhow::format_err!("Mirror TVC doesn't contain code"))?
        .clone();

    for i in 1..=1000 {
        let hex_part = format!("2{i:063x}");
        let address = format!("0:{hex_part}");
        let mirror_addr = AccountAddress::from_str(&address)
            .map_err(|e| anyhow::format_err!("Failed to calculate mirror addr: {e}"))?;
        let acc_id: AccountId = mirror_addr.clone().into();
        let Some(shard_account) = accounts
            .account(&acc_id)
            .map_err(|e| anyhow::format_err!("Failed to read account from shard state: {e}"))?
        else {
            tracing::error!("mirror_repair: account was not found in state: {mirror_addr:?}");
            continue;
        };
        let mut old_account = shard_account
            .read_account()
            .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?
            .as_struct()
            .map_err(|e| anyhow::format_err!("Failed to read account from shard account: {e}"))?;
        if let Some(old_account_stateinit) = old_account.state_init_mut() {
            old_account_stateinit.set_code(mirror_code.clone());
        } else {
            tracing::error!("mirror_repair: Failed to get old stateinit account");
            continue;
        }
        let Ok(new_account_cell) = old_account.serialize() else {
            tracing::error!("mirror_repair: Failed to serialize new account");
            continue;
        };
        let new_shard_account = ShardAccount::with_account_root(
            new_account_cell,
            shard_account.last_trans_hash().clone(),
            shard_account.last_trans_lt(),
            shard_account.get_dapp_id().cloned(),
        );
        if let Err(e) = accounts.insert(&mirror_addr.0, &new_shard_account) {
            tracing::error!("mirror_repair: Failed to insert new shard account: {e}");
            continue;
        }
        tracing::error!("mirror_repair: Success: {mirror_addr:?}");
    }
    Ok(())
}
