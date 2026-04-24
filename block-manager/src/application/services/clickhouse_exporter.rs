// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;

use database::documents_db::TxActivitySummary;

/// Row written to ClickHouse `main.tx_summary` table.
#[derive(Debug, Clone, clickhouse::Row, serde::Serialize)]
pub struct TransactionSummary {
    pub tx_id: String,
    pub block_time: u32,
    pub account_addr: String,
    pub tr_type: u8,
    pub aborted: bool,
    pub in_msg_type: u8,
    pub bounced: bool,
    pub total_fees: String,
}

impl From<TxActivitySummary> for TransactionSummary {
    fn from(s: TxActivitySummary) -> Self {
        Self {
            tx_id: s.tx_id,
            block_time: s.timestamp,
            account_addr: s.account_addr,
            tr_type: s.tr_type,
            aborted: s.aborted,
            in_msg_type: s.in_msg_type.unwrap_or(255), // 255 = unknown
            bounced: s.bounced,
            total_fees: s.total_fees,
        }
    }
}

const BATCH_SIZE: usize = 10_000;
const BATCH_TIMEOUT: Duration = Duration::from_secs(5);
const TABLE: &str = "tx_summary";

/// Runs the ClickHouse writer loop. Receives `TxActivitySummary` batches
/// from the block applier via `tokio::sync::mpsc` channel, converts and inserts
/// into ClickHouse in batches.
pub async fn run(
    mut rx: tokio::sync::mpsc::Receiver<Vec<TxActivitySummary>>,
    clickhouse_url: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
) -> anyhow::Result<()> {
    let mut client = clickhouse::Client::default().with_url(&clickhouse_url).with_database("main");
    if let Some(user) = clickhouse_user {
        client = client.with_user(user);
    }
    if let Some(password) = clickhouse_password {
        client = client.with_password(password);
    }

    tracing::info!("ClickHouse tx-summary exporter started, url={clickhouse_url}");

    let mut buffer: Vec<TransactionSummary> = Vec::with_capacity(BATCH_SIZE);

    loop {
        match tokio::time::timeout(BATCH_TIMEOUT, rx.recv()).await {
            Ok(Some(summaries)) => {
                buffer.extend(summaries.into_iter().map(TransactionSummary::from));
                // Drain any additional pending batches without blocking
                while let Ok(more) = rx.try_recv() {
                    buffer.extend(more.into_iter().map(TransactionSummary::from));
                }
            }
            Ok(None) => {
                tracing::info!("Activity channel closed, flushing remaining rows");
                if !buffer.is_empty() {
                    if let Err(err) = flush(&client, &mut buffer).await {
                        tracing::error!("Final flush failed: {err}");
                    }
                }
                return Ok(());
            }
            Err(_) => {
                // Timeout — flush whatever we have
            }
        }

        if !buffer.is_empty() {
            if let Err(err) = flush(&client, &mut buffer).await {
                tracing::error!("ClickHouse flush failed: {err}");
            }
        }
    }
}

async fn flush(
    client: &clickhouse::Client,
    buffer: &mut Vec<TransactionSummary>,
) -> anyhow::Result<()> {
    let count = buffer.len();
    let mut insert = client.insert::<TransactionSummary>(TABLE).await?;
    for row in buffer.iter() {
        insert.write(row).await?;
    }
    insert.end().await?;
    buffer.clear();
    tracing::debug!("Flushed {count} tx summary records to ClickHouse");
    Ok(())
}
