use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use futures::future::join_all;
use hex::FromHex;
use http_server::BkInfo;
use http_server::BkSetResult;

use crate::config::ProxyConfig;

fn get_bk_set_url(addr: SocketAddr) -> String {
    format!("http://{addr}/bk/v1/bk_set")
}

pub async fn run(
    config_rx: tokio::sync::watch::Receiver<ProxyConfig>,
    bk_set_tx: tokio::sync::watch::Sender<HashSet<transport_layer::VerifyingKey>>,
    client: reqwest::Client,
    bk_set_watch_interval_sec: u64,
) {
    let mut bk_set = HashSet::<transport_layer::VerifyingKey>::new();
    loop {
        let ProxyConfig { bk_addrs, .. } = config_rx.borrow().clone();

        let mut tasks: Vec<tokio::task::JoinHandle<anyhow::Result<BkSetResult>>> = Vec::new();

        for addr in bk_addrs {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                let request = client.get(get_bk_set_url(addr)).build()?;
                let response = client.execute(request).await?;
                Ok(response.json::<BkSetResult>().await?)
            }));
        }
        let results = join_all(tasks).await;

        let mut winner: Option<BkSetResult> = None;

        for res in results {
            match res {
                Ok(Ok(bk_result)) => {
                    // BkSet with a maximum seq_no will be the winner
                    winner = get_winner_bk_set(winner, bk_result);
                }
                Ok(Err(err)) => {
                    tracing::error!("Request returned error: {:?}", err);
                }
                Err(join_err) => {
                    tracing::error!("Task failed to join: {:?}", join_err);
                }
            }
        }

        fn bk_info_to_pubkey(info: BkInfo) -> Option<transport_layer::VerifyingKey> {
            let pubkey = <[u8; 32]>::from_hex(&info.node_owner_pk).ok()?;
            transport_layer::VerifyingKey::from_bytes(&pubkey).ok()
        }

        if let Some(winner) = winner {
            let new_bk_set = HashSet::<transport_layer::VerifyingKey>::from_iter(
                winner
                    .bk_set
                    .into_iter()
                    .filter_map(bk_info_to_pubkey)
                    .chain(winner.future_bk_set.into_iter().filter_map(bk_info_to_pubkey)),
            );
            if new_bk_set != bk_set {
                bk_set = new_bk_set;
                bk_set_tx.send_replace(bk_set.clone());
            }
        }
        tokio::time::sleep(Duration::from_secs(bk_set_watch_interval_sec)).await;
    }
}

// This function compares the current BkSetResult with a candidate and returns the one with the higher seq_no.
fn get_winner_bk_set(current: Option<BkSetResult>, candidate: BkSetResult) -> Option<BkSetResult> {
    match current {
        Some(current_bk_set) => {
            if candidate.seq_no > current_bk_set.seq_no {
                Some(candidate)
            } else {
                Some(current_bk_set)
            }
        }
        None => Some(candidate),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_winner_bk_set() {
        let current = None;
        let candidate = BkSetResult { seq_no: 1, ..Default::default() };
        assert_eq!(get_winner_bk_set(current, candidate).unwrap().seq_no, 1);

        let current = Some(BkSetResult { seq_no: 1, ..Default::default() });
        let candidate = BkSetResult { seq_no: 2, ..Default::default() };
        assert_eq!(get_winner_bk_set(current, candidate).unwrap().seq_no, 2);

        let current = Some(BkSetResult { seq_no: 3, ..Default::default() });
        let candidate = BkSetResult { seq_no: 2, ..Default::default() };
        assert_eq!(get_winner_bk_set(current, candidate).unwrap().seq_no, 3);
    }
}
