// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
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
use tvm_types::UInt256;

use crate::normalize_address;
use crate::now_plus_n_secs;
use crate::owner_wallet::decode_owner_wallet;
use crate::owner_wallet::OwnerWalletData;
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

// token issuer -> (sign_key, delegated_licenses_count, expired_at)
static SIGN_KEY_CACHE: LazyLock<RwLock<HashMap<TokenIssuer, (String, usize, Instant)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

const SIGN_KEY_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
struct WalletAddress {
    wallet: String,
}

#[derive(Debug)]
pub struct AccountRequest {
    pub address: String,
    pub response: oneshot::Sender<anyhow::Result<(tvm_block::Account, Option<UInt256>, u64)>>,
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
    UnauthorizedIssuer,
    UnknownIssuer,
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
        let result = get_wallet_data(&self.issuer, account_request_tx).await;

        let (signing_pubkey, license_count) = match result {
            Ok(wallet_data) => (wallet_data.pubkey, wallet_data.license_count),
            Err(e) if e.to_string() == "Unknown account" => {
                return TokenVerificationResult::UnknownIssuer
            }
            Err(e) => {
                tracing::trace!("Failed to process issuer wallet: {e}");
                return TokenVerificationResult::InvalidSignature;
            }
        };

        if license_count == 0 {
            return TokenVerificationResult::UnauthorizedIssuer;
        }

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
    response_rx.await?.map(|(acc, ..)| Some(acc))
}

async fn get_wallet_data(
    issuer: &TokenIssuer,
    account_request_tx: mpsc::Sender<AccountRequest>,
) -> anyhow::Result<OwnerWalletData> {
    tracing::trace!("request signing pubkey");
    if let Some((pubkey, license_count, expires_at)) = SIGN_KEY_CACHE.read().get(issuer) {
        if Instant::now() < *expires_at {
            tracing::trace!("SIGN_KEY_CACHE: cache hit for {issuer:?}");
            return Ok(OwnerWalletData {
                pubkey: Some(pubkey.to_string()),
                license_count: *license_count,
            });
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
        return Err(anyhow!("Account (owner wallet root) request failed"));
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
            return Err(anyhow!("Incorrect address"));
        }
    };

    tracing::trace!(wallet_address = %wallet_addr, "derived");

    let Some(wallet_account) = request_account(&wallet_addr, account_request_tx).await? else {
        tracing::trace!("Failed to get BOC of the wallet at {wallet_addr}");
        return Err(anyhow!("Account (owner wallet) request failed"));
    };

    tracing::trace!("got account: {wallet_account:?}");
    let issuer_wallet_data = decode_owner_wallet(&wallet_account, issuer);
    tracing::trace!("got signing key: {issuer_wallet_data:?}");

    if let Ok(OwnerWalletData { pubkey: Some(ref pubkey), license_count }) = issuer_wallet_data {
        SIGN_KEY_CACHE.write().insert(
            issuer.clone(),
            (pubkey.to_string(), license_count, Instant::now() + SIGN_KEY_CACHE_TTL),
        );
        tracing::trace!("SIGN_KEY_CACHE: cache update for {issuer:?}");
    }

    issuer_wallet_data
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
        tracing::debug!("EXT_MESSAGE_AUTH_REQUIRED updated to true");
    } else if disabled {
        disable_auth();
        tracing::debug!("EXT_MESSAGE_AUTH_REQUIRED updated to false");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use hex::ToHex;
    use sdk_wrapper::read_file;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex as TokioMutex;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tvm_block::Account;
    use tvm_block::Deserializable;

    use super::*;
    use crate::read_keys_from_file;
    use crate::KeyPair;

    const FAKE_BM_ISSUER_ADDR: &str =
        "40f11d13cf70edb0b4ac883a915c03ba333eceb49263e47ef0ba0415e9b023c5";
    const REAL_BM_ISSUER_ADDR: &str =
        "c3c0474d61fdd004960d1a5a320bc88549f73238cfa0a8fc6a00e15a72bbda19";
    const REAL_BK_ISSUER_ADDR: &str =
        "dc67ae73b647b399bb8293ef1b5c9bc9577baf036ad7ec5c49505efbae74ac67";

    static TEST_LOCK: OnceLock<TokioMutex<()>> = OnceLock::new();

    #[allow(unused)]
    fn init_tracing() {
        let test_filter = tracing_subscriber::EnvFilter::new("ext_messages_auth,sdk_wrapper");
        let _ = tracing_subscriber::registry()
            .with(
                test_filter,
                // tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "trace".into()),
            )
            .with(tracing_subscriber::fmt::layer())
            .try_init();
    }

    async fn lock_test() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_LOCK.get_or_init(|| TokioMutex::new(())).lock().await
    }

    fn dummy_signing_keypair() -> (String, String) {
        use ed25519_dalek::SigningKey;

        let signing = SigningKey::generate(&mut rand::thread_rng());
        let verifying = signing.verifying_key();

        let secret_hex = signing.to_bytes().encode_hex();
        let public_hex = verifying.to_bytes().encode_hex();

        (secret_hex, public_hex)
    }

    #[tokio::test]
    async fn test_token_creation_and_validation() {
        let _guard = lock_test().await;

        force_auth();

        let (secret, public) = dummy_signing_keypair();

        let token = Token::new(&secret, TokenIssuer::Bm("bm_wallet".to_string()))
            .expect("Token should be created");

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::Ok);
    }

    #[tokio::test]
    async fn test_token_expired() {
        let _guard = lock_test().await;

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
        let _guard = lock_test().await;

        force_auth();

        let (secret, public) = dummy_signing_keypair();

        let mut token = Token::new(&secret, TokenIssuer::Bm("bm_wallet".to_string())).unwrap();

        token.signature = "00".repeat(64);

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::InvalidSignature);
    }

    #[tokio::test]
    async fn test_token_disabled_auth() {
        let _guard = lock_test().await;

        disable_auth();

        let (secret, public) = dummy_signing_keypair();

        let token = Token::new(&secret, TokenIssuer::Bk("bk_wallet".to_string())).unwrap();

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::Ok);
    }

    #[tokio::test]
    async fn test_token_malformed_signature() {
        let _guard = lock_test().await;

        force_auth();

        let (secret, public) = dummy_signing_keypair();

        let mut token = Token::new(&secret, TokenIssuer::Bm("bm_wallet".to_string())).unwrap();

        token.signature = "not-a-hex".to_string(); // incorrect signature format

        let result = token.verify(Some(public));
        assert_eq!(result, TokenVerificationResult::TokenMalformed);
    }

    fn construct_response_acc(boc_file: &str) -> anyhow::Result<Account> {
        let boc = String::from_utf8(read_file(&format!("./fixtures/{boc_file}"))?)
            .map(|s| s.trim().to_owned())?;
        Account::construct_from_base64(&boc)
            .map_err(|e| anyhow::anyhow!("Failed to construct account from boc: {e}"))
    }

    async fn mock_account_request_handler(
        mut rx: mpsc::Receiver<AccountRequest>,
    ) -> anyhow::Result<()> {
        while let Some(request) = rx.recv().await {
            let account: Result<Account, anyhow::Error> = match request.address.as_str() {
                BK_CONTRACT_ROOT_ADDR => construct_response_acc("bk_root.boc.b64"),
                BM_CONTRACT_ROOT_ADDR => construct_response_acc("bm_root.boc.b64"),
                FAKE_BM_ISSUER_ADDR => construct_response_acc("fake_bm_issuer_40f11d13cf70edb0b4ac883a915c03ba333eceb49263e47ef0ba0415e9b023c5.boc.b64"),
                REAL_BM_ISSUER_ADDR => construct_response_acc("real_bm_issuer_c3c0474d61fdd004960d1a5a320bc88549f73238cfa0a8fc6a00e15a72bbda19.boc.b64"),
                REAL_BK_ISSUER_ADDR => construct_response_acc("real_bk_issuer_dc67ae73b647b399bb8293ef1b5c9bc9577baf036ad7ec5c49505efbae74ac67.boc.b64"),
                _ => Err(anyhow!("Unknown account")),
            };

            let account_response = account.map(|account| (account, None, 0));

            let _ = request.response.send(account_response);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_authorize_with_auth_not_required() {
        let _guard = lock_test().await;
        // init_tracing();
        disable_auth();

        let (secret, pubkey) = dummy_signing_keypair();
        let issuer = TokenIssuer::Bk(pubkey);
        let token = Token::new(&secret, issuer).unwrap();

        let (tx, _rx) = mpsc::channel(1);

        let result = token.authorize(tx).await;
        assert_eq!(result, TokenVerificationResult::Ok);
    }

    #[tokio::test]
    async fn test_authorize_with_non_existed_issuer() {
        let _guard = lock_test().await;
        // init_tracing();
        force_auth();

        let (secret, pubkey) = dummy_signing_keypair();
        let issuer = TokenIssuer::Bk(pubkey);

        let token = Token::new(&secret, issuer.clone()).unwrap();

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let _ = mock_account_request_handler(rx).await;
        });

        let result = token.authorize(tx).await;

        assert_eq!(result, TokenVerificationResult::UnknownIssuer);
    }

    #[tokio::test]
    async fn test_authorize_with_zero_licenses() -> anyhow::Result<()> {
        let _guard = lock_test().await;
        // init_tracing();
        force_auth();

        let KeyPair { public, secret } = read_keys_from_file("./fixtures/fake_bm_issuer_40f11d13cf70edb0b4ac883a915c03ba333eceb49263e47ef0ba0415e9b023c5.keys.json")
            .expect("keypair required");

        let issuer = TokenIssuer::Bm(public);
        let token = Token::new(&secret, issuer)?;

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let _ = mock_account_request_handler(rx).await;
        });

        let result = token.authorize(tx).await;

        assert_eq!(result, TokenVerificationResult::UnauthorizedIssuer);

        Ok(())
    }

    #[tokio::test]
    async fn test_authorize_with_delegated_license() -> anyhow::Result<()> {
        let _guard = lock_test().await;
        // init_tracing();
        force_auth();

        let KeyPair { public, secret } = read_keys_from_file("./fixtures/real_bm_issuer_c3c0474d61fdd004960d1a5a320bc88549f73238cfa0a8fc6a00e15a72bbda19.keys.json")
            .expect("keypair required");

        let issuer = TokenIssuer::Bm(public);
        let token = Token::new(&secret, issuer)?;

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let _ = mock_account_request_handler(rx).await;
        });

        let result = token.authorize(tx).await;

        assert_eq!(result, TokenVerificationResult::Ok);

        Ok(())
    }

    #[tokio::test]
    async fn test_authorize_with_cache_hit() -> anyhow::Result<()> {
        let _guard = lock_test().await;
        // init_tracing();
        force_auth();

        let (secret, pubkey) = dummy_signing_keypair();
        let issuer = TokenIssuer::Bk(pubkey.clone());

        SIGN_KEY_CACHE
            .write()
            .insert(issuer.clone(), (pubkey, 5, Instant::now() + Duration::from_secs(3600)));

        let token = Token::new(&secret, issuer)?;

        let (tx, _rx) = mpsc::channel(10);

        let result = token.authorize(tx).await;

        assert_eq!(result, TokenVerificationResult::Ok);

        Ok(())
    }

    #[tokio::test]
    async fn test_authorize_with_expired_cache() -> anyhow::Result<()> {
        let _guard = lock_test().await;
        // init_tracing();
        force_auth();

        let (secret, pubkey) = dummy_signing_keypair();
        let issuer = TokenIssuer::Bk(pubkey.clone());

        SIGN_KEY_CACHE
            .write()
            .insert(issuer.clone(), (pubkey, 5, Instant::now() - Duration::from_secs(1)));

        let token = Token::new(&secret, issuer)?;

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let _ = mock_account_request_handler(rx).await;
        });

        let result = token.authorize(tx).await;

        assert_eq!(result, TokenVerificationResult::UnknownIssuer);

        // SIGN_KEY_CACHE.write().clear();
        Ok(())
    }

    #[tokio::test]
    async fn test_authorize_with_concurrent_requests() -> anyhow::Result<()> {
        let _guard = lock_test().await;
        // init_tracing();
        force_auth();

        let KeyPair { public: pubkey, secret } = read_keys_from_file("./fixtures/real_bm_issuer_c3c0474d61fdd004960d1a5a320bc88549f73238cfa0a8fc6a00e15a72bbda19.keys.json")
            .expect("keypair required");
        let issuer = TokenIssuer::Bm(pubkey.clone());
        let token = Token::new(&secret, issuer)?;

        let (tx, rx) = mpsc::channel(50);

        tokio::spawn(async move {
            let _ = mock_account_request_handler(rx).await;
        });

        let mut handles = vec![];
        for _ in 0..50 {
            let token_clone = token.clone();
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move { token_clone.authorize(tx_clone).await });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(matches!(
                result,
                TokenVerificationResult::Ok | TokenVerificationResult::UnauthorizedIssuer
            ));
        }

        Ok(())
    }
}
