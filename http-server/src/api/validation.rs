// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

/// Strictly parse a 64-character ASCII hex string.
///
/// Returns the lowercase-normalized value. Rejects any other length and any
/// non-hex character. The 66-char `0x`/`0X`/`0:` prefixed variants accepted
/// by `node-types::u8_32_from_str` are rejected here because we want strict
/// inputs on the HTTP boundary.
pub fn parse_hex32(value: &str, field_name: &str) -> Result<String, String> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("Invalid {field_name}: expected 64 hex characters without prefix"));
    }
    Ok(value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOWER: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const UPPER: &str = "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF";

    #[test]
    fn accepts_64_lowercase_hex() {
        assert_eq!(parse_hex32(LOWER, "account_id").unwrap(), LOWER);
    }

    #[test]
    fn normalizes_uppercase_to_lowercase() {
        assert_eq!(parse_hex32(UPPER, "account_id").unwrap(), LOWER);
    }

    #[test]
    fn rejects_63_chars() {
        let err = parse_hex32(&LOWER[..63], "dapp_id").unwrap_err();
        assert!(err.contains("dapp_id"));
        assert!(err.contains("64 hex"));
    }

    #[test]
    fn rejects_65_chars() {
        let s = format!("{LOWER}a");
        let err = parse_hex32(&s, "account_id").unwrap_err();
        assert!(err.contains("account_id"));
    }

    #[test]
    fn rejects_0x_prefix() {
        let s = format!("0x{LOWER}"); // 66 chars
        assert!(parse_hex32(&s, "account_id").is_err());
    }

    #[test]
    fn rejects_0_colon_prefix() {
        let s = format!("0:{LOWER}"); // 66 chars
        assert!(parse_hex32(&s, "account_id").is_err());
    }

    #[test]
    fn rejects_non_hex_character() {
        // 64 chars but contains 'g'
        let s = format!("g{}", &LOWER[1..]);
        assert!(parse_hex32(&s, "account_id").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_hex32("", "account_id").is_err());
    }
}
