// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;

use reqwest::Client;
use reqwest::Error;
use reqwest::RequestBuilder;
use serde::Deserialize;
use tvm_client::ClientConfig;
use tvm_client::ClientContext;

use crate::blockchain::Account;

const LICENSE_ROOT_ADDR: &str =
    "0:4444444444444444444444444444444444444444444444444444444444444444";
static LICENSE_ROOT_ABI: &str = include_str!("../../contracts/bksystem/LicenseRoot.abi.json");

static BK_ACCOUNT_API_TOKEN: LazyLock<String> = LazyLock::new(|| {
    std::env::var("BK_ACCOUNT_API_TOKEN")
        .expect("BK Account API token (BK_ACCOUNT_API_TOKEN) not found")
});

#[derive(Deserialize, Debug)]
struct AddressResponse {
    boc: String,
}

pub fn build_fetch_boc_request(host: SocketAddr, address: &str) -> RequestBuilder {
    let base_url = format!("http://{host}/v2/account");
    let url = format!("{base_url}?address={address}");
    let client = Client::new();
    client.get(&url).bearer_auth(BK_ACCOUNT_API_TOKEN.to_string())
}

pub async fn fetch_boc(host: SocketAddr, address: &str) -> Result<String, Error> {
    let request_builder = build_fetch_boc_request(host, address);
    let response = request_builder.send().await?;

    let address_data: AddressResponse = response.json().await?;

    Ok(address_data.boc)
}

pub async fn get_license_addr(
    api_host: SocketAddr,
    license_number: String,
) -> anyhow::Result<Option<String>> {
    let number = license_number.parse::<u128>()?;

    let abi = tvm_client::abi::Abi::Json(LICENSE_ROOT_ABI.to_string());
    let license_root_addr = Some(LICENSE_ROOT_ADDR);

    let account = Account::try_new_with_abi(abi, None, None, license_root_addr).await?;

    let context = Arc::new(ClientContext::new(ClientConfig::default())?);

    let boc = fetch_boc(api_host, LICENSE_ROOT_ADDR).await?;
    tracing::debug!("fetched boc. len={}", boc.len());

    let result = account
        .run_local(
            &context,
            "getLicenseBMAddress",
            Some(serde_json::json!({"num": number})),
            Some(boc),
        )
        .await?;

    let license_address =
        result.get("license_address").and_then(|s| s.as_str()).map(str::to_string);

    Ok(license_address)
}
