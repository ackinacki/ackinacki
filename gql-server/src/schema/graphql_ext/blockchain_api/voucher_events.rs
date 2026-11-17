// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! `blockchain.voucherEvents`: RootPN `VoucherGenerated` events found by
//! `skUCommit` in the history proofs store.

use async_graphql::ErrorExtensions;

/// The `sk_u_commit` argument is not hex of at most 32 bytes.
pub const CODE_VOUCHER_EVENTS_BAD_ARGUMENT: &str = "VOUCHER_EVENTS_BAD_ARGUMENT";
/// This server has no history proofs store, so it cannot tell whether a
/// voucher exists: an error, not an empty list.
pub const CODE_HISTORY_STORE_UNAVAILABLE: &str = "HISTORY_STORE_UNAVAILABLE";

pub(super) fn error(code: &'static str, message: String) -> async_graphql::Error {
    async_graphql::Error::new(message).extend_with(|_, e| e.set("code", code))
}

/// `skUCommit` as 32 bytes: 1 to 64 hex digits in any letter case, with or
/// without `0x`, padded with zeros on the left (a uint256 written without its
/// leading zeros).
pub fn parse_sk_u_commit(value: &str) -> Result<[u8; 32], String> {
    let digits = value.strip_prefix("0x").or_else(|| value.strip_prefix("0X")).unwrap_or(value);
    if digits.is_empty() || digits.len() > 64 || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "sk_u_commit must be 1 to 64 hex digits, with or without 0x, got {value:?}"
        ));
    }
    let padded = format!("{digits:0>64}");
    let mut sk = [0u8; 32];
    faster_hex::hex_decode(padded.as_bytes(), &mut sk).map_err(|err| err.to_string())?;
    Ok(sk)
}

#[cfg(test)]
mod tests {
    use super::parse_sk_u_commit;

    #[test]
    fn parse_sk_u_commit_pads_and_ignores_case() {
        let mut one = [0u8; 32];
        one[31] = 1;
        assert_eq!(parse_sk_u_commit("1"), Ok(one));
        assert_eq!(parse_sk_u_commit("0x01"), Ok(one));
        assert_eq!(parse_sk_u_commit(&format!("0X{}", "AB".repeat(32))), Ok([0xab; 32]));
        for bad in ["", "0x", "0xg1", &"1".repeat(65), "-1"] {
            assert!(parse_sk_u_commit(bad).is_err(), "{bad:?}");
        }
    }
}
