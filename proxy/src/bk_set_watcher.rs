use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use futures::future::join_all;
use http_server::ApiBk;
use http_server::ApiBkSet;
use transport_layer::pubkeys_info;

use crate::config::ProxyConfig;

fn get_bk_set_url(addr: SocketAddr) -> String {
    format!("http://{addr}/v2/bk_set_update")
}

pub async fn run(
    config_rx: tokio::sync::watch::Receiver<ProxyConfig>,
    bk_set_tx: tokio::sync::watch::Sender<HashSet<transport_layer::VerifyingKey>>,
    client: reqwest::Client,
    bk_set_watch_interval_sec: u64,
) {
    let mut bk_set = ApiBkSet { seq_no: 0, current: vec![], future: vec![] };
    let mut bk_owner_pubkeys = HashSet::<transport_layer::VerifyingKey>::new();
    loop {
        let ProxyConfig { bk_addrs, .. } = config_rx.borrow().clone();

        let mut tasks: Vec<tokio::task::JoinHandle<anyhow::Result<ApiBkSet>>> = Vec::new();

        for addr in bk_addrs {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                let request = client.get(get_bk_set_url(addr)).build()?;
                let response = client.execute(request).await?;
                Ok(response.json::<ApiBkSet>().await?)
            }));
        }
        let results = join_all(tasks).await;

        let mut winner: Option<ApiBkSet> = None;

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

        fn bk_owner_pubkey(bk: &ApiBk) -> Option<transport_layer::VerifyingKey> {
            transport_layer::VerifyingKey::from_bytes(&bk.owner_pubkey.0).ok()
        }

        if let Some(winner) = winner {
            if winner.seq_no >= bk_set.seq_no {
                tracing::info!(
                    "Update bk set to received {} from {}.",
                    winner.seq_no,
                    bk_set.seq_no
                );
            } else {
                tracing::info!(
                    "Skip received bk set {} because of less than {}.",
                    winner.seq_no,
                    bk_set.seq_no
                );
            }
            if bk_set.update(&winner) {
                let new_bk_owner_pubkeys = HashSet::<transport_layer::VerifyingKey>::from_iter(
                    bk_set
                        .current
                        .iter()
                        .filter_map(bk_owner_pubkey)
                        .chain(bk_set.future.iter().filter_map(bk_owner_pubkey)),
                );
                if new_bk_owner_pubkeys != bk_owner_pubkeys {
                    tracing::info!(
                        "Update trusted pubkeys to [{}] from [{}].",
                        pubkeys_info(&new_bk_owner_pubkeys, 4),
                        pubkeys_info(&bk_owner_pubkeys, 4)
                    );
                    bk_owner_pubkeys = new_bk_owner_pubkeys;
                    bk_set_tx.send_replace(bk_owner_pubkeys.clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(bk_set_watch_interval_sec)).await;
    }
}

// This function compares the current BkSetResult with a candidate and returns the one with the higher seq_no.
fn get_winner_bk_set(current: Option<ApiBkSet>, candidate: ApiBkSet) -> Option<ApiBkSet> {
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
        fn bk_set(seq_no: u64) -> ApiBkSet {
            ApiBkSet { seq_no, future: vec![], current: vec![] }
        }
        let candidate = bk_set(1);
        assert_eq!(get_winner_bk_set(current, candidate).unwrap().seq_no, 1);

        let current = Some(bk_set(1));
        let candidate = bk_set(2);
        assert_eq!(get_winner_bk_set(current, candidate).unwrap().seq_no, 2);

        let current = Some(bk_set(3));
        let candidate = bk_set(2);
        assert_eq!(get_winner_bk_set(current, candidate).unwrap().seq_no, 3);
    }
}
