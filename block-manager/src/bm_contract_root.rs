// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;

use reqwest::Client;
use reqwest::Error;
use reqwest::RequestBuilder;
use sdk_wrapper::Account;
use serde::Deserialize;
use tvm_client::ClientConfig;
use tvm_client::ClientContext;

const BM_CONTRACT_ROOT_ADDR: &str =
    "0:6666666666666666666666666666666666666666666666666666666666666666";
static BM_CONTRACT_ROOT_ABI: &str =
    include_str!("../../contracts/bksystem/BlockManagerContractRoot.abi.json");

static BK_API_TOKEN: LazyLock<String> = LazyLock::new(|| {
    std::env::var("BK_API_TOKEN").expect("BK Account API token (BK_API_TOKEN) not found")
});

#[derive(Deserialize, Debug)]
struct AddressResponse {
    boc: String,
}

#[derive(Deserialize)]
struct WalletAddress {
    wallet: Option<String>,
}

pub fn build_fetch_boc_request(host: SocketAddr, address: &str) -> RequestBuilder {
    let base_url = format!("http://{host}/v2/account");
    let url = format!("{base_url}?address={address}");
    let client = Client::new();
    client.get(&url).bearer_auth(BK_API_TOKEN.to_string())
}

pub async fn fetch_boc(host: SocketAddr, address: &str) -> Result<String, Error> {
    let request_builder = build_fetch_boc_request(host, address);
    let response = request_builder.send().await?;

    let address_data: AddressResponse = response.json().await?;

    Ok(address_data.boc)
}

pub async fn get_bm_owner_wallet_addr(
    api_host: SocketAddr,
    wallet_pubkey: &String,
) -> anyhow::Result<Option<String>> {
    let abi = tvm_client::abi::Abi::Json(BM_CONTRACT_ROOT_ABI.to_string());
    let bm_contract_root_addr = Some(BM_CONTRACT_ROOT_ADDR);

    let account = Account::try_new_with_abi(abi, None, None, bm_contract_root_addr).await?;

    let context = Arc::new(ClientContext::new(ClientConfig::default())?);

    let boc = fetch_boc(api_host, BM_CONTRACT_ROOT_ADDR).await?;

    let result = account
        .run_local(
            &context,
            "getAckiNackiBlockManagerNodeWalletAddress",
            Some(serde_json::json!({"pubkey": format!("0x{}", wallet_pubkey)})),
            Some(boc),
        )
        .await?;

    let owner_wallet_address = serde_json::from_value::<WalletAddress>(result)?;

    Ok(owner_wallet_address.wallet)
}
