use std::collections::HashMap;

use itertools::Itertools;
use tvm_types::UInt256;

use crate::resolver::blockchain::watch_blockchain;
use crate::resolver::blockchain::NodeDb;

fn node_db() -> NodeDb {
    NodeDb::new("/Users/michaelvlasov/dev/gosh/bm-archive.db")
}

#[tokio::test]
async fn test_proxy_list() {
    let self_peer_id = UInt256::with_array([1u8; 32]);
    let (subscribe_tx, subscribe_rx) = tokio::sync::watch::channel(Vec::new());
    let (peers_tx, _) = tokio::sync::watch::channel(HashMap::new());
    tokio::spawn(watch_blockchain(node_db(), node_db(), self_peer_id, subscribe_tx, peers_tx));
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let subscribe = subscribe_rx.borrow().clone();
    let info = subscribe
        .into_iter()
        .map(|x| x.subscribe_addrs().into_iter().map(|x| x.to_string()).join(","))
        .collect::<Vec<_>>();
    println!("{info:?}");
}
