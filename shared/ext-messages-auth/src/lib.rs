// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;

// todo: convert to auth service
pub mod auth;
mod owner_wallet;
pub mod root_contracts;

// todo prevent printing the secret key into the log
#[derive(Clone, Debug, Deserialize)]
pub struct KeyPair {
    pub public: String,
    pub secret: String,
}

fn now_plus_n_secs(ttl: u64) -> u128 {
    let now = std::time::SystemTime::now();
    let future = now + std::time::Duration::from_secs(ttl);
    future.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_millis()
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

pub fn read_keys_from_file(path: &str) -> Result<KeyPair, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let keys: KeyPair = serde_json::from_reader(reader)?;
    Ok(keys)
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
