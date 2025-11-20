pub mod watch;
pub mod watch_hot_reload;

use std::fmt::Display;
use std::str::FromStr;

use chitchat::ChitchatRef;
use gossip::gossip_peer::GossipPeer;
pub use watch::watch_gossip;
pub use watch::WatchGossipConfig;

pub(crate) fn collect_gossip_peers<PeerId: Display + FromStr<Err: Display>>(
    chitchat: &ChitchatRef,
    config: &WatchGossipConfig<PeerId>,
) -> (Vec<GossipPeer<PeerId>>, usize) {
    let chitchat = chitchat.lock();
    let mut peers = Vec::new();
    let mut live_nodes_len = 0;
    for chitchat_id in chitchat.live_nodes() {
        live_nodes_len += 1;
        let Some(peer) = chitchat.node_state(chitchat_id).and_then(GossipPeer::try_get_from) else {
            continue;
        };

        let is_trusted = if !config.trusted_pubkeys.is_empty() {
            peer.pubkey_signature
                .as_ref()
                .map(|(pub_key, _)| config.trusted_pubkeys.contains(pub_key))
                .unwrap_or_default()
        } else {
            true
        };
        if is_trusted {
            peers.push(peer);
        }
    }
    (peers, live_nodes_len)
}
