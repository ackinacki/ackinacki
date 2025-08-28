// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use async_graphql::futures_util::TryStreamExt;
use futures::future;
use futures::stream::FuturesUnordered;
use num::bigint::Sign;
use num::BigInt;
use num::Num;
use sqlx::SqlitePool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tvm_block::Deserializable;
use tvm_block::ExtraCurrencyCollection;
use tvm_types::read_single_root_boc;

use crate::schema::db;
use crate::schema::graphql::block::Block;
use crate::schema::graphql::currency::OtherCurrency;
use crate::schema::graphql::formats::BigIntFormat;
use crate::schema::graphql::message::InMsg;
use crate::schema::graphql::message::Message;
use crate::schema::graphql::transaction::Transaction;
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

pub async fn _load_trx_out_messages(
    pool: &SqlitePool,
    trx: &mut [Option<Transaction>],
) -> anyhow::Result<()> {
    let mut ids: Vec<String> = Vec::new();

    for t in trx.iter_mut() {
        let trx_id = &t.as_ref().unwrap().id;
        ids.push(format!("{:?}", trx_id.clone()));
    }

    let mut messages: HashMap<String, Vec<Option<Message>>> = HashMap::new();
    let sql = format!("SELECT * FROM messages WHERE transaction_id IN ({})", ids.join(","));
    let rows =
        sqlx::query_as(&sql).fetch(pool).map_ok(|m| m).try_collect::<Vec<db::Message>>().await?;

    for row in rows {
        let message: Message = row.clone().into();
        if let Some(ref tr_id) = row.transaction_id {
            if messages.contains_key(&tr_id.clone()) {
                if let Some(m_vec) = messages.get_mut(tr_id) {
                    m_vec.push(Some(message));
                }
            } else {
                messages.insert(tr_id.to_string(), vec![Some(message)]);
            }
        }
    }

    for t in trx.iter_mut().flatten() {
        if messages.contains_key(&t.id) {
            t.out_messages = Some(messages.get(&t.id).unwrap().to_vec());
        }
    }
    Ok(())
}

pub async fn _load_blocks_in_messages(
    pool: &SqlitePool,
    blocks: &mut [Option<Block>],
) -> anyhow::Result<()> {
    // let ids = blocks
    // .iter()
    // .map(|b| format!("{:?}", &b.as_ref().unwrap().id))
    // .collect::<Vec<String>>()
    // .join(",");
    //
    // let mut messages: HashMap<String, Vec<Message>> = HashMap::new();
    // let sql = format!(
    // "SELECT * FROM messages WHERE id IN (SELECT in_msg FROM transactions WHERE
    // block_id IN ({}))", ids
    // );
    // let rows = sqlx::query_as(&sql)
    // .fetch(pool)
    // .map_ok(|m| m)
    // .try_collect::<Vec<db::Message>>()
    // .await?;
    let tasks = blocks
        .iter()
        .map(|b| {
            let block = b.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                let sql = format!(
                    "SELECT * FROM messages WHERE id IN (SELECT in_msg FROM transactions WHERE block_id={:?})",
                    block.clone().unwrap().id
                );
                let _rows = sqlx::query_as(&sql)
                    .fetch(&pool)
                    .map_ok(|m: db::Message| m.into())
                    .try_collect::<Vec<InMsg>>()
                    .await;
                block.clone()
            })
        })
        .collect::<FuturesUnordered<_>>();

    let _res = future::join_all(tasks).await;

    Ok(())
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

#[cfg(test)]
pub mod tests {
    use crate::helpers::format_big_int;

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
}
