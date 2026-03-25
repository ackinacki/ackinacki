// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use num::bigint::Sign;
use num::BigInt;
use num::Num;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tvm_block::Deserializable;
use tvm_block::ExtraCurrencyCollection;
use tvm_types::read_single_root_boc;

use crate::defaults::THREAD_ID_LENGTH;
use crate::schema::graphql::currency::OtherCurrency;
use crate::schema::graphql::formats::BigIntFormat;
use crate::schema::graphql_ext::QueryOrderBy;

pub(crate) trait ToBool {
    fn to_bool(&self) -> Option<bool>;
}

impl ToBool for Option<i64> {
    fn to_bool(&self) -> Option<bool> {
        match self {
            Some(value) => match value {
                0 => Some(false),
                1 => Some(true),
                _ => None,
            },
            _ => None,
        }
    }
}

pub(crate) trait ToInt {
    fn to_int(&self) -> Option<i32>;
}

impl ToInt for Option<i64> {
    fn to_int(&self) -> Option<i32> {
        match self {
            Some(value) => match *value <= i32::MAX as i64 {
                true => Some(*value as i32),
                false => {
                    tracing::warn!("failed to convert {value} into Int");
                    None
                }
            },
            _ => None,
        }
    }
}

impl ToInt for Option<usize> {
    fn to_int(&self) -> Option<i32> {
        match self {
            Some(value) => match *value <= i32::MAX as usize {
                true => Some(*value as i32),
                false => {
                    tracing::warn!("failed to convert {value} into Int");
                    None
                }
            },
            _ => None,
        }
    }
}

pub(crate) trait ToOptU64 {
    fn to_opt_u64(&self) -> Option<u64>;
}

impl ToOptU64 for Option<i64> {
    fn to_opt_u64(&self) -> Option<u64> {
        self.as_ref().map(|value| *value as u64)
    }
}

pub(crate) trait ToFloat {
    fn to_float(&self) -> Option<f64>;
}

impl ToFloat for Option<i64> {
    fn to_float(&self) -> Option<f64> {
        (*self).map(|v| v as f64)
    }
}

pub fn format_big_int(value: Option<String>, format: Option<BigIntFormat>) -> Option<String> {
    // value.as_ref()?;
    match value {
        Some(value) if !value.is_empty() => {
            let big_int = BigInt::from_str_radix(&value, 16).unwrap();
            let formatted = match format {
                Some(BigIntFormat::DEC) => big_int.to_string(),
                _ => {
                    let (sign, u_value) = big_int.into_parts();
                    if sign == Sign::Minus {
                        format!("-0x{}", u_value.to_str_radix(16))
                    } else {
                        format!("0x{}", u_value.to_str_radix(16))
                    }
                }
            };

            Some(formatted)
        }
        _ => None,
    }
}

pub fn format_big_int_dec(str: Option<String>, format: Option<BigIntFormat>) -> Option<String> {
    str.as_ref()?;

    let big_int = BigInt::from_str_radix(&str.unwrap(), 10).unwrap();
    let formatted = match format {
        Some(BigIntFormat::DEC) => big_int.to_string(),
        _ => {
            let (sign, u_value) = big_int.into_parts();
            if sign == Sign::Minus {
                format!("-0x{}", u_value.to_str_radix(16))
            } else {
                format!("0x{}", u_value.to_str_radix(16))
            }
        }
    };

    Some(formatted)
}

pub fn u64_to_string(value: u64) -> String {
    let mut string = format!("{value:x}");
    string.insert_str(0, &format!("{:x}", string.len() - 1));
    string
}

#[inline]
pub fn pad_thread_id(thread_id: String) -> String {
    if thread_id.len() >= THREAD_ID_LENGTH {
        thread_id
    } else {
        let mut padded = String::with_capacity(THREAD_ID_LENGTH);
        for _ in 0..(THREAD_ID_LENGTH - thread_id.len()) {
            padded.push('0');
        }
        padded.push_str(&thread_id);
        padded
    }
}

pub fn init_tracing() {
    // Init tracing
    let filter = match std::env::var("NODE_VERBOSE") {
        Ok(v) if !v.is_empty() => tracing_subscriber::filter::Targets::new()
            .with_target("gql_server", LevelFilter::TRACE)
            .with_target("data_loader", LevelFilter::TRACE)
            .with_target("blockchain_api", LevelFilter::TRACE)
            .with_target("sqlx", LevelFilter::OFF),
        _ => tracing_subscriber::filter::Targets::new()
            .with_target("gql_server", LevelFilter::INFO)
            .with_target("data_loader", LevelFilter::TRACE)
            .with_target("blockchain_api", LevelFilter::TRACE)
            .with_target("sqlx", LevelFilter::OFF),
    };
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_thread_ids(true)
                .with_ansi(false)
                .with_writer(std::io::stderr),
        )
        .with(filter)
        .init();
}

pub fn ecc_from_bytes(bytes: Option<Vec<u8>>) -> anyhow::Result<Option<Vec<OtherCurrency>>> {
    let other_currency = match bytes {
        Some(bytes) => {
            let cell = read_single_root_boc(bytes).map_err(|e| anyhow::format_err!("{e}"))?;
            let ecc = ExtraCurrencyCollection::construct_from_cell(cell)
                .map_err(|e| anyhow::format_err!("{e}"))?;
            let mut res = vec![];
            let _ = ecc.iterate_with_keys(|currency: u32, v| {
                res.push(OtherCurrency {
                    currency: Some(currency as f64),
                    value: Some(v.value().to_string()),
                });
                Ok(true)
            });
            Some(res)
        }
        _ => None,
    };

    Ok(other_currency)
}

pub fn query_order_by_str(order_by: Option<Vec<Option<QueryOrderBy>>>) -> String {
    if order_by.is_none() {
        return "".to_string();
    }

    let order_str = order_by
        .unwrap()
        .iter()
        .filter_map(|v| {
            let v = v.as_ref()?;
            let path = v.path.as_deref()?;

            let path = if path == "id" { "block_id" } else { path };
            Some(format!("{} {}", path, v.direction.unwrap()))
        })
        .collect::<Vec<_>>()
        .join(",");

    match order_str.len() {
        0 => "".to_string(),
        _ => format!(" ORDER BY {order_str} "),
    }
}

const HEX_TABLE: [u8; 16] = *b"0123456789abcdef";

/// Formats a `u64` value as an SQLite BLOB hex literal in the form `X'0123...abcd'`.
#[inline]
pub fn u64_to_hexed_blob_buf(v: u64) -> [u8; 19] {
    let mut buf = [0u8; 19];
    buf[0] = b'X';
    buf[1] = b'\'';

    let mut n = v;
    for i in (0..16).rev() {
        let nibble = (n & 0x0f) as usize;
        buf[2 + i] = HEX_TABLE[nibble];
        n >>= 4;
    }

    buf[18] = b'\'';
    buf
}

/// Formats a `u64` value as an SQLite BLOB hex literal string `X'0123...abcd'`.
#[inline]
pub fn u64_to_hexed_blob_literal(v: u64) -> String {
    let buf = u64_to_hexed_blob_buf(v);
    std::str::from_utf8(&buf).expect("u64 blob literal is valid UTF-8").to_string()
}

#[inline]
pub fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
pub mod tests {
    use crate::defaults::THREAD_ID_LENGTH;
    use crate::helpers::format_big_int;
    use crate::helpers::pad_thread_id;
    use crate::helpers::u64_to_hexed_blob_literal;

    #[test]
    fn test_format_big_int() {
        assert_eq!(
            format_big_int(Some("-f744df471cf".to_owned()), None),
            Some("-0xf744df471cf".to_owned())
        );

        assert_eq!(format_big_int(Some("000".to_owned()), None), Some("0x0".to_owned()));

        assert_eq!(
            format_big_int(Some("-f0c87d253161b4ed43".to_owned()), None),
            Some("-0xf0c87d253161b4ed43".to_owned())
        );

        assert_eq!(
            format_big_int(Some("0f3782dace9d900000".to_owned()), None),
            Some("0xf3782dace9d900000".to_owned())
        );
    }

    #[test]
    fn test_pad_thread_id() {
        let short = "thread-1".to_string();
        let padded = pad_thread_id(short.clone());
        assert_eq!(padded.len(), THREAD_ID_LENGTH);
        assert!(padded.ends_with(&short));

        let already_padded = "0".repeat(THREAD_ID_LENGTH);
        assert_eq!(pad_thread_id(already_padded.clone()), already_padded);
    }

    #[test]
    fn test_u64_to_hexed_blob_literal() {
        assert_eq!(u64_to_hexed_blob_literal(0), "X'0000000000000000'");
        assert_eq!(u64_to_hexed_blob_literal(1), "X'0000000000000001'");
        assert_eq!(u64_to_hexed_blob_literal(u64::MAX), "X'ffffffffffffffff'");
    }
}
