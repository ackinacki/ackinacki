// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::LazyLock;

use ed25519_dalek::Signature;
use ed25519_dalek::Verifier;
use ed25519_dalek::VerifyingKey;
use hex::FromHex;
use serde_json::Value;

pub static EXT_MESSAGE_AUTH_REQUIRED: LazyLock<AtomicBool> = LazyLock::new(|| {
    let enabled = std::env::var("EXT_MESSAGE_AUTH_REQUIRED")
        .map(|v| matches!(v.to_lowercase().as_str(), "true" | "yes" | "1"))
        .unwrap_or(false);
    AtomicBool::new(enabled)
});

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TokenVerificationResult {
    Ok,
    TokenMalformed,
    UnknownLicense,
    InvalidSignature,
    Expired,
}

pub fn verify_bm_token(value: &Value, expected_pubkey: Option<String>) -> TokenVerificationResult {
    if !is_auth_required() {
        tracing::debug!("token auth skipped");
        return TokenVerificationResult::Ok;
    }

    tracing::debug!("validating token...");

    let token = match value.get("bm_token") {
        Some(v) => v,
        None => return TokenVerificationResult::TokenMalformed,
    };

    let unsigned = match token.get("unsigned").and_then(Value::as_str) {
        Some(s) => s,
        None => return TokenVerificationResult::TokenMalformed,
    };

    let signature = match token.get("signature").and_then(Value::as_str) {
        Some(s) => s,
        None => return TokenVerificationResult::TokenMalformed,
    };

    let signature_bytes = match <[u8; 64]>::from_hex(signature) {
        Ok(bytes) => bytes,
        Err(_) => return TokenVerificationResult::TokenMalformed,
    };

    let verifying_key = match token.get("verifying_key").and_then(Value::as_str) {
        Some(s) => s,
        None => return TokenVerificationResult::TokenMalformed,
    };

    match expected_pubkey {
        Some(pubkey) if pubkey == verifying_key => (),
        _ => return TokenVerificationResult::UnknownLicense,
    }

    let public_bytes = match <[u8; 32]>::from_hex(verifying_key) {
        Ok(bytes) => bytes,
        Err(_) => return TokenVerificationResult::TokenMalformed,
    };

    let verifying_key = match VerifyingKey::from_bytes(&public_bytes) {
        Ok(key) => key,
        Err(_) => return TokenVerificationResult::TokenMalformed,
    };

    if let Ok(until) = unsigned.parse::<u64>() {
        if super::current_time_millis(None) > until.into() {
            return TokenVerificationResult::Expired;
        }
    } else {
        return TokenVerificationResult::TokenMalformed;
    }

    match verifying_key.verify(unsigned.as_bytes(), &Signature::from_bytes(&signature_bytes)) {
        Ok(_) => TokenVerificationResult::Ok,
        Err(_) => TokenVerificationResult::InvalidSignature,
    }
}

fn force_auth() {
    EXT_MESSAGE_AUTH_REQUIRED.store(true, Ordering::Relaxed);
}

fn disable_auth() {
    EXT_MESSAGE_AUTH_REQUIRED.store(false, Ordering::Relaxed);
}

pub fn is_auth_required() -> bool {
    EXT_MESSAGE_AUTH_REQUIRED.load(Ordering::Relaxed)
}

pub fn update_ext_message_auth_flag_from_files() {
    let enabled = std::path::PathBuf::from("/workdir/ext_message_auth.enable").exists();
    let disabled = std::path::PathBuf::from("/workdir/ext_message_auth.disable").exists();

    if enabled {
        force_auth();
        tracing::info!("EXT_MESSAGE_AUTH_REQUIRED updated to true");
    } else if disabled {
        disable_auth();
        tracing::info!("EXT_MESSAGE_AUTH_REQUIRED updated to false");
    };
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::OnceLock;

    use serde_json::json;

    use super::*;

    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    const BM_LICENSE_OWNER_PUBKEY: &str =
        "128a5586045a9a3c300f99ef958d5536ab5d4fbaad6e3726321e87a071d4834c";

    fn lock_test() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn valid_token() -> Value {
        json!({
            "bm_token": {
                "signature": "bdf3bbbfdc01bdaeedb20a378d58ff9a1fc3617eaddeb1e1313ad3ca2b8b20db1d33f61c9ae781e2c7cf2e318d9376d5ad7092616331168edd96ab7ce58df203",
                "unsigned": "159513482593073",
                "verifying_key": BM_LICENSE_OWNER_PUBKEY
            }
        })
    }

    fn expired_token() -> Value {
        json!({
            "bm_token": {
                "signature": "d1a57140c6f00acb88bc4921fa28c1800e929012d38e57fbb9ab982e2a480f2287f508b476c1e2ddb88dfd260a08bb3b0083bb902651bfe8e167a6147976eb0e",
                "unsigned": "1747082722439",
                "verifying_key": BM_LICENSE_OWNER_PUBKEY
            }
        })
    }

    #[test]
    fn test_valid_token() {
        let _lock = lock_test();

        let token = valid_token();

        disable_auth();
        assert_eq!(
            verify_bm_token(&token, Some(BM_LICENSE_OWNER_PUBKEY.to_string())),
            TokenVerificationResult::Ok
        );

        force_auth();
        assert_eq!(
            verify_bm_token(&token, Some(BM_LICENSE_OWNER_PUBKEY.to_string())),
            TokenVerificationResult::Ok
        );
    }

    #[test]
    fn test_invalid_signature_token() {
        let _lock = lock_test();

        let mut token = valid_token();
        token["bm_token"]["signature"] = serde_json::Value::String("00".repeat(64)); // invalid signature

        disable_auth();
        assert_eq!(
            verify_bm_token(&token, Some(BM_LICENSE_OWNER_PUBKEY.to_string())),
            TokenVerificationResult::Ok
        );

        force_auth();
        assert_eq!(
            verify_bm_token(&token, Some(BM_LICENSE_OWNER_PUBKEY.to_string())),
            TokenVerificationResult::InvalidSignature
        );
    }

    #[test]
    fn test_expired_token() {
        let _lock = lock_test();

        let token = expired_token();

        disable_auth();
        assert_eq!(
            verify_bm_token(&token, Some(BM_LICENSE_OWNER_PUBKEY.to_string())),
            TokenVerificationResult::Ok
        );

        force_auth();
        assert_eq!(
            verify_bm_token(&token, Some(BM_LICENSE_OWNER_PUBKEY.to_string())),
            TokenVerificationResult::Expired
        );
    }

    #[test]
    fn test_malformed_token() {
        let _lock = lock_test();

        let mut token = valid_token();
        token["bm_token"]["verifying_key"] = Value::Null;

        disable_auth();
        assert_eq!(verify_bm_token(&token, None), TokenVerificationResult::Ok);

        force_auth();
        assert_eq!(verify_bm_token(&token, None), TokenVerificationResult::TokenMalformed);
    }

    #[test]
    fn test_auth_required() {
        let _lock = lock_test();

        let empty = &Value::Null;

        disable_auth();
        assert_eq!(verify_bm_token(empty, None), TokenVerificationResult::Ok);

        force_auth();
        assert_eq!(verify_bm_token(empty, None), TokenVerificationResult::TokenMalformed);
    }

    #[test]
    fn test_unknown_license() {
        let _lock = lock_test();

        let token = valid_token();

        disable_auth();
        assert_eq!(verify_bm_token(&token, None), TokenVerificationResult::Ok);

        force_auth();
        let expected_pubkey =
            "e2c9d4be54d342d3f0e6394a7738fc39b93d4fe3fdba317aa699f7305566de2b".to_string();
        assert_eq!(
            verify_bm_token(&token, Some(expected_pubkey)),
            TokenVerificationResult::UnknownLicense
        );

        let expected_pubkey = BM_LICENSE_OWNER_PUBKEY.to_string();
        assert_eq!(verify_bm_token(&token, Some(expected_pubkey)), TokenVerificationResult::Ok);
    }
}
