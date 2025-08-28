// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use ed25519_dalek::Signature;
use ed25519_dalek::Signer;
use ed25519_dalek::SigningKey;
use ed25519_dalek::Verifier;
use ed25519_dalek::VerifyingKey;
use hex::FromHex;
use hex::ToHex;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tvm_block::Serializable;

use crate::normalize_address;
use crate::now_plus_n_secs;
use crate::owner_wallet::decode_signing_pubkey;
use crate::root_contracts::BK_CONTRACT_ROOT_ABI;
use crate::root_contracts::BK_CONTRACT_ROOT_ADDR;
use crate::root_contracts::BM_CONTRACT_ROOT_ABI;
use crate::root_contracts::BM_CONTRACT_ROOT_ADDR;

lazy_static::lazy_static!(
    static ref TOKEN_TTL: u64 = std::env::var("TOKEN_TTL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300);
);

pub static EXT_MESSAGE_AUTH_REQUIRED: LazyLock<AtomicBool> = LazyLock::new(|| {
    let enabled = std::env::var("EXT_MESSAGE_AUTH_REQUIRED")
        .map(|v| matches!(v.to_lowercase().as_str(), "true" | "yes" | "1"))
        .unwrap_or(false);
    AtomicBool::new(enabled)
});

// token issuer -> (sign_key, expired_at)
static SIGN_KEY_CACHE: LazyLock<RwLock<HashMap<TokenIssuer, (String, Instant)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

const SIGN_KEY_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
struct WalletAddress {
    wallet: String,
}

pub struct AccountRequest {
    pub address: String,
    pub response: oneshot::Sender<anyhow::Result<Option<tvm_block::Account>>>,
}

// The token issuer for external message authorization.
// Determined by the public key of the BM or BK wallet owner (_owner_pubkey).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TokenIssuer {
    Bm(String),
    Bk(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Token {
    pub unsigned: String, // The time until which the token remains valid. UNIX timestamp in ms
    pub signature: String, // hex-encoded signature
    pub issuer: TokenIssuer,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TokenVerificationResult {
    Ok,
    TokenMalformed,
    InvalidSignature,
    Expired,
}

impl Token {
    pub fn new(secret: &str, issuer: TokenIssuer) -> Result<Self, anyhow::Error> {
        let unsigned = now_plus_n_secs(*TOKEN_TTL).to_string();
        let message_bytes = unsigned.as_bytes();

        let secret_bytes = <[u8; 32]>::from_hex(secret)?;
        let signing_key = SigningKey::from_bytes(&secret_bytes);

        let signature: Signature = signing_key.sign(message_bytes);

        Ok(Self { unsigned, signature: signature.to_bytes().encode_hex(), issuer })
    }

    pub fn verify(&self, signing_pubkey: Option<String>) -> TokenVerificationResult {
        tracing::trace!("verify token against signing pubkey: {signing_pubkey:?}");

        let signature_bytes = match <[u8; 64]>::from_hex(&self.signature) {
            Ok(bytes) => bytes,
            Err(_) => return TokenVerificationResult::TokenMalformed,
        };

        let Some(verifying_key_hex) = signing_pubkey else {
            return TokenVerificationResult::InvalidSignature;
        };

        let public_bytes = match <[u8; 32]>::from_hex(&verifying_key_hex) {
            Ok(bytes) => bytes,
            Err(_) => return TokenVerificationResult::TokenMalformed,
        };

        let verifying_key = match VerifyingKey::from_bytes(&public_bytes) {
            Ok(key) => key,
            Err(_) => return TokenVerificationResult::TokenMalformed,
        };

        let expiry = match self.unsigned.parse::<u64>() {
            Ok(ms) => ms,
            Err(_) => return TokenVerificationResult::TokenMalformed,
        };

        if now_plus_n_secs(0) > expiry.into() {
            return TokenVerificationResult::Expired;
        }

        match verifying_key
            .verify(self.unsigned.as_bytes(), &Signature::from_bytes(&signature_bytes))
        {
            Ok(_) => TokenVerificationResult::Ok,
            Err(_) => TokenVerificationResult::InvalidSignature,
        }
    }

    pub async fn authorize(
        &self,
        account_request_tx: mpsc::Sender<AccountRequest>,
    ) -> TokenVerificationResult {
        if !is_auth_required() {
            return TokenVerificationResult::Ok;
        }

        tracing::trace!("authorize token {self:?}");
        let Ok(signing_pubkey) = get_signing_pubkey(&self.issuer, account_request_tx).await else {
            tracing::trace!("Failed to get signing pubkey. Unauthorize");
            return TokenVerificationResult::InvalidSignature;
        };

        self.verify(signing_pubkey)
    }
}

async fn request_account(
    address: &str,
    account_request_tx: mpsc::Sender<AccountRequest>,
) -> anyhow::Result<Option<tvm_block::Account>> {
    let (response_tx, response_rx) = oneshot::channel();
    let request = AccountRequest { address: address.to_string(), response: response_tx };

    account_request_tx.send(request).await?;
    response_rx.await?
}

async fn get_signing_pubkey(
    issuer: &TokenIssuer,
    account_request_tx: mpsc::Sender<AccountRequest>,
) -> anyhow::Result<Option<String>> {
    tracing::trace!("request signing pubkey");
    if let Some((pubkey, expires_at)) = SIGN_KEY_CACHE.read().get(issuer) {
        if Instant::now() < *expires_at {
            tracing::trace!("SIGN_KEY_CACHE: cache hit for {issuer:?}");
            return Ok(Some(pubkey.clone()));
        }
    }

    let context = Arc::new(tvm_client::ClientContext::new(tvm_client::ClientConfig::default())?);

    let (abi_json, contract_addr, function_name, pubkey_str) = match issuer {
        TokenIssuer::Bk(ref pubkey) => (
            BK_CONTRACT_ROOT_ABI,
            BK_CONTRACT_ROOT_ADDR,
            "getAckiNackiBlockKeeperNodeWalletAddress",
            format!("0x{pubkey}"),
        ),
        TokenIssuer::Bm(pubkey) => (
            BM_CONTRACT_ROOT_ABI,
            BM_CONTRACT_ROOT_ADDR,
            "getAckiNackiBlockManagerNodeWalletAddress",
            format!("0x{pubkey}"),
        ),
    };

    let abi = tvm_client::abi::Abi::Json(abi_json.to_string());
    let contract =
        sdk_wrapper::Account::try_new_with_abi(abi, None, None, Some(contract_addr)).await?;

    let boc = if let Some(raw_account) =
        request_account(contract_addr, account_request_tx.clone()).await?
    {
        let boc_bytes = raw_account.write_to_bytes().map_err(|e| anyhow::anyhow!("{e}"))?;
        tvm_types::base64_encode(&boc_bytes)
    } else {
        tracing::trace!("Failed to get BOC of the root contract at {contract_addr}");
        return Ok(None);
    };

    let result = contract
        .run_local(
            &context,
            function_name,
            Some(serde_json::json!({ "pubkey": pubkey_str })),
            Some(boc),
        )
        .await?;

    let owner_wallet_address = serde_json::from_value::<WalletAddress>(result)?.wallet;
    let wallet_addr = match normalize_address(&owner_wallet_address) {
        Some(addr) => addr,
        None => {
            tracing::trace!("Failed to normalize wallet address");
            return Ok(None);
        }
    };

    let Some(wallet_account) = request_account(&wallet_addr, account_request_tx).await? else {
        tracing::trace!("Failed to get BOC of the wallet at {wallet_addr}");
        return Ok(None);
    };

    tracing::trace!("got account: {wallet_account:?}");
    let signing_key = decode_signing_pubkey(&wallet_account, issuer);
    tracing::trace!("got signing key: {signing_key:?}");

    if let Ok(Some(ref pubkey)) = signing_key {
        SIGN_KEY_CACHE
            .write()
            .insert(issuer.clone(), (pubkey.clone(), Instant::now() + SIGN_KEY_CACHE_TTL));
        tracing::trace!("SIGN_KEY_CACHE: cache update for {issuer:?}");
    }

    signing_key
}

pub fn is_auth_required() -> bool {
    EXT_MESSAGE_AUTH_REQUIRED.load(Ordering::Relaxed)
}

pub fn force_auth() {
    EXT_MESSAGE_AUTH_REQUIRED.store(true, Ordering::Relaxed);
}

pub fn disable_auth() {
    EXT_MESSAGE_AUTH_REQUIRED.store(false, Ordering::Relaxed);
}

pub fn update_ext_message_auth_flag_from_files() {
    let enabled = std::path::Path::new("/workdir/ext_message_auth.enable").exists();
    let disabled = std::path::Path::new("/workdir/ext_message_auth.disable").exists();

    if enabled {
        force_auth();
        tracing::info!("EXT_MESSAGE_AUTH_REQUIRED updated to true");
    } else if disabled {
        disable_auth();
        tracing::info!("EXT_MESSAGE_AUTH_REQUIRED updated to false");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::OnceLock;

    use hex::ToHex;

    use super::*;

    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn lock_test() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn dummy_signing_keypair() -> (String, String) {
        use ed25519_dalek::SigningKey;

        let signing = SigningKey::generate(&mut rand::rngs::OsRng);
        let verifying = signing.verifying_key();

        let secret_hex = signing.to_bytes().encode_hex();
        let public_hex = verifying.to_bytes().encode_hex();

        (secret_hex, public_hex)
    }

    #[tokio::test]
    async fn test_token_creation_and_validation() {
        let _guard = lock_test();

        force_auth();

        let (secret, public) = dummy_signing_keypair();

        let token = Token::new(&secret, TokenIssuer::Bm("bm_wallet".to_string()))
            .expect("Token should be created");

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::Ok);
    }

    #[tokio::test]
    async fn test_token_expired() {
        let _guard = lock_test();

        force_auth();

        let sign_pubkey =
            "128a5586045a9a3c300f99ef958d5536ab5d4fbaad6e3726321e87a071d4834c".to_string();

        let token = Token {
            unsigned: "1747082722439".to_string(), // a long time ago in a galaxy far, far away
            signature: "d1a57140c6f00acb88bc4921fa28c1800e929012d38e57fbb9ab982e2a480f2287f508b476c1e2ddb88dfd260a08bb3b0083bb902651bfe8e167a6147976eb0e".to_string(),
            issuer: TokenIssuer::Bk("bk_wallet".to_string()),
        };

        let result = token.verify(Some(sign_pubkey));
        assert_eq!(result, TokenVerificationResult::Expired);
    }

    #[tokio::test]
    async fn test_token_wrong_signature() {
        let _guard = lock_test();

        force_auth();

        let (secret, public) = dummy_signing_keypair();

        let mut token = Token::new(&secret, TokenIssuer::Bm("bm_wallet".to_string())).unwrap();

        token.signature = "00".repeat(64);

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::InvalidSignature);
    }

    #[tokio::test]
    async fn test_token_disabled_auth() {
        let _guard = lock_test();

        disable_auth();

        let (secret, public) = dummy_signing_keypair();

        let token = Token::new(&secret, TokenIssuer::Bk("bk_wallet".to_string())).unwrap();

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::Ok);
    }

    #[tokio::test]
    async fn test_malformed_token_signature() {
        let _guard = lock_test();

        force_auth();

        let (secret, public) = dummy_signing_keypair();

        let mut token = Token::new(&secret, TokenIssuer::Bm("bm_wallet".to_string())).unwrap();

        token.signature = "not-a-hex".to_string(); // incorrect signature format

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::TokenMalformed);
    }
}
