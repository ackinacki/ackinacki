// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use parking_lot::RwLock;
use tvm_block::ShardStateUnsplit;
use tvm_types::AccountId;

use crate::block_keeper_system::bm_license::decode_bm_license_pubkey;

// TODO: refactor to eliminate the global variable
static BM_PUBKEY_CACHE: LazyLock<RwLock<HashMap<String, (String, Instant)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

const PUBKEY_TTL: Duration = Duration::from_secs(60);

pub fn load_bm_license_contract_pubkey(
    shard_state: ShardStateUnsplit,
    addr: String,
) -> anyhow::Result<Option<String>> {
    let Some(address) = normalize_address(&addr) else {
        tracing::debug!("Invalid account address: {addr}");
        return Ok(None);
    };

    if let Some((pubkey, expires_at)) = BM_PUBKEY_CACHE.read().get(&address) {
        if Instant::now() < *expires_at {
            tracing::trace!("BM_PUBKEY_CACHE: cache hit for {address}");
            return Ok(Some(pubkey.clone()));
        }
    }

    tracing::trace!("BM_PUBKEY_CACHE: cache miss for {address}");

    let account_id = AccountId::from_string(&address)
        .map_err(|e| anyhow::format_err!("Invalid account address {}: {}", address, e))?;
    let account = shard_state
        .read_accounts()
        .and_then(|accounts| accounts.account(&account_id))
        .map_err(|e| anyhow::format_err!("Failed to read accounts from shard state: {e}"))?
        .and_then(|shard_account| {
            shard_account
                .read_account()
                .and_then(|acc| acc.as_struct())
                .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))
                .ok()
        })
        .ok_or(anyhow::format_err!("Account not found or invalid"))?;

    let result = decode_bm_license_pubkey(&account)?;

    if let Some(ref pubkey) = result {
        BM_PUBKEY_CACHE.write().insert(addr, (pubkey.clone(), Instant::now() + PUBKEY_TTL));
        tracing::trace!("BM_PUBKEY_CACHE: cache update for {address}");
    }

    Ok(result)
}

fn normalize_address(s: &str) -> Option<String> {
    let trimmed = s.trim();

    let hex = match trimmed.rsplit_once(':') {
        Some((_, h)) => h,
        None => trimmed,
    };

    let hex = hex.trim();

    if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hex.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_address_plain() {
        let input = "8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800";
        assert_eq!(normalize_address(input), Some(input.to_string()));
    }

    #[test]
    fn test_valid_address_with_prefix() {
        let input = "0:8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800";
        let expected =
            "8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800".to_string();
        assert_eq!(normalize_address(input), Some(expected));
    }

    #[test]
    fn test_valid_address_with_colon_prefix() {
        let input = ":8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800";
        let expected =
            "8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800".to_string();
        assert_eq!(normalize_address(input), Some(expected));
    }

    #[test]
    fn test_invalid_too_short() {
        let input = "deadbeef";
        assert_eq!(normalize_address(input), None);
    }

    #[test]
    fn test_invalid_too_long() {
        let input = &("a".repeat(65));
        assert_eq!(normalize_address(input), None);
    }

    #[test]
    fn test_invalid_non_hex_chars() {
        let input = "-1:8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800";
        let expected =
            "8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800".to_string();
        assert_eq!(normalize_address(input), Some(expected));
    }

    #[test]
    fn test_whitespace_handling() {
        let input = "   0:8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800   ";
        let expected =
            "8e8dad0462a4d5c528e18251846f24bc5c04cd1871115fb1e9b00c9741f60800".to_string();
        assert_eq!(normalize_address(input), Some(expected));
    }
}
