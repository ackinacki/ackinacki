// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::Cursor;
use std::io::Seek;
use std::io::Write;
use std::sync::Arc;

use serde_json::Value;
use tvm_client::abi::encode_message;
use tvm_client::abi::Abi;
use tvm_client::abi::CallSet;
use tvm_client::abi::ParamsOfEncodeMessage;
use tvm_client::abi::Signer;
use tvm_client::crypto::KeyPair;
use tvm_client::net::query_collection;
use tvm_client::net::ParamsOfQueryCollection;
use tvm_client::tvm::run_tvm;
use tvm_client::tvm::ParamsOfRunTvm;
use tvm_client::ClientContext;
use tvm_sdk::ContractImage;

use crate::helpers::read_file;

#[derive(Debug)]
pub struct Account {
    address: String, // AccountAddress
    dapp_id: Option<String>,
    keys: Option<KeyPair>,
    abi: Abi,
    _tvc: Option<Vec<u8>>,
}

impl std::fmt::Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Account {{ address: {}, dapp_id: {:?}, keys: {:?}, abi: <Abi> }}",
            self.address, self.dapp_id, self.keys
        )
    }
}

impl Account {
    #[allow(dead_code)]
    pub async fn try_new(
        abi_path: &str,
        tvc_path: Option<&str>,
        keys: Option<KeyPair>,
        address: Option<&str>,
    ) -> anyhow::Result<Self> {
        let abi = Self::load_abi(abi_path)?;
        let (address, tvc) = match tvc_path {
            Some(path) => {
                let mut tvc = read_file(path)?;
                Self::update_tvc(&mut tvc, abi.clone(), keys.clone())?;
                let address = Self::calc_address(&tvc).await?;
                (address, Some(tvc))
            }
            None => {
                let addr =
                    address.ok_or_else(|| anyhow::anyhow!("failed to get address"))?.to_string();
                (addr, None)
            }
        };

        Ok(Self { address, dapp_id: None, keys, abi, _tvc: tvc })
    }

    pub async fn try_new_with_abi(
        abi: Abi,
        tvc_path: Option<&str>,
        keys: Option<KeyPair>,
        address: Option<&str>,
    ) -> anyhow::Result<Self> {
        let (address, tvc) = match tvc_path {
            Some(path) => {
                let mut tvc = read_file(path)?;
                Self::update_tvc(&mut tvc, abi.clone(), keys.clone())?;
                let address = Self::calc_address(&tvc).await?;
                (address, Some(tvc))
            }
            None => {
                let addr =
                    address.ok_or_else(|| anyhow::anyhow!("failed to get address"))?.to_string();
                (addr, None)
            }
        };

        Ok(Self { address, dapp_id: None, keys, abi, _tvc: tvc })
    }

    pub fn address(&self) -> String {
        self.address.clone()
    }

    pub async fn run_local(
        &self,
        context: &Arc<ClientContext>,
        method: &str,
        params: Option<serde_json::Value>,
        boc: Option<String>,
    ) -> anyhow::Result<Value> {
        let account_boc = if boc.is_some() {
            boc
        } else {
            let filter = Some(serde_json::json!({
                "id": { "eq": self.address() }
            }));

            let query = query_collection(
                context.clone(),
                ParamsOfQueryCollection {
                    collection: "accounts".to_owned(),
                    filter,
                    result: "boc".to_owned(),
                    limit: Some(1),
                    order: None,
                },
            )
            .await
            .map(|r| r.result)?;

            if query.is_empty() {
                anyhow::bail!(
                    "account with address {} not found. Was trying to call {}",
                    self.address(),
                    method,
                );
            }
            query[0]["boc"].as_str().map(|s| s.to_string())
        };

        if account_boc.is_none() {
            anyhow::bail!("account with address {} does not contain boc", self.address(),);
        }

        let call_set = match params {
            Some(value) => CallSet::some_with_function_and_input(method, value),
            None => CallSet::some_with_function(method),
        };

        let encoded = encode_message(
            Arc::clone(context),
            ParamsOfEncodeMessage {
                abi: self.abi.clone(),
                address: Some(self.address()),
                call_set,
                signer: Signer::None,
                deploy_set: None,
                processing_try_index: None,
                signature_id: None,
            },
        )
        .await
        .map_err(|e| anyhow::format_err!("failed to encode message: {e}"))?;

        let result = run_tvm(
            context.clone(),
            ParamsOfRunTvm {
                message: encoded.message,
                account: account_boc.unwrap(),
                abi: Some(self.abi.clone()),
                boc_cache: None,
                execution_options: None,
                return_updated_account: None,
            },
        )
        .await
        .map(|r| r.decoded.unwrap())
        .map(|r| r.output.unwrap())
        .map_err(|e| anyhow::format_err!("run_local failed: {e}"))?;

        Ok(result)
    }

    pub async fn calc_address(tvc: &[u8]) -> anyhow::Result<String> {
        let tvc_cell = tvm_types::boc::read_single_root_boc(tvc).unwrap();
        let tvc_hash = tvc_cell.repr_hash();
        let address = format!("{}:{}", 0, tvc_hash.as_hex_string());
        Ok(address)
    }

    pub fn update_tvc(tvc_bytes: &mut [u8], abi: Abi, keys: Option<KeyPair>) -> anyhow::Result<()> {
        let Some(ref keys) = keys else {
            return Ok(());
        };

        let mut state_init = Cursor::new(tvc_bytes);
        let mut contract_image = ContractImage::from_state_init(&mut state_init)
            .map_err(|e| anyhow::anyhow!("unable to load contract image: {}", e))?;

        let init_data = Self::insert_pubkey_to_init_data(&keys.public, None)?;
        let abi_str = abi.json_string()?;
        contract_image
            .update_data(false, &init_data, &abi_str)
            .map_err(|e| anyhow::anyhow!("unable to update contract image data: {}", e))?;

        let vec_bytes = contract_image
            .serialize()
            .map_err(|e| anyhow::anyhow!("unable to serialize contract image: {}", e))?;
        state_init
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| anyhow::anyhow!("failed to access the tvc file: {}", e))?;
        state_init
            .write_all(&vec_bytes)
            .map_err(|e| anyhow::anyhow!("failed to update the tvc file: {}", e))?;
        Ok(())
    }

    pub fn insert_pubkey_to_init_data(
        pubkey: &str,
        opt_init_data: Option<&str>,
    ) -> anyhow::Result<String> {
        let str_init_data = opt_init_data.unwrap_or("{}");
        let mut init_data: Value = serde_json::from_str(str_init_data)?;

        let obj = init_data
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("init_data must be a JSON object"))?;

        obj.insert("_pubkey".to_string(), Value::String(format!("0x{pubkey}")));

        Ok(init_data.to_string())
    }

    pub fn load_abi(path: &str) -> anyhow::Result<Abi> {
        let abi_str_result = std::fs::read_to_string(path);
        if let Err(e) = abi_str_result {
            anyhow::bail!("Failed to read {path}: {e}");
        }
        let abi = Abi::Json(abi_str_result?);
        Ok(abi)
    }
}
