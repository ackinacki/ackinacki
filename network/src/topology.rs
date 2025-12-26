use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;

use ed25519_dalek::VerifyingKey;
use gossip::gossip_peer::GossipPeer;
use transport_layer::HostPort;
use url::Url;

use crate::config::SocketAddrSet;
use crate::resolver::WatchGossipConfig;

#[derive(Clone, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum NetEndpoint<PeerId> {
    Peer(NetPeer<PeerId>),
    Proxy(SocketAddrSet),
}

impl<PeerId> NetEndpoint<PeerId> {
    pub fn is_proxy(&self) -> bool {
        matches!(*self, NetEndpoint::Proxy(_))
    }

    pub fn subscribe_addrs(&self) -> SocketAddrSet {
        match self {
            NetEndpoint::Peer(peer) => SocketAddrSet::with_addr(peer.addr),
            NetEndpoint::Proxy(addrs) => addrs.clone(),
        }
    }
}

impl<PeerId: Display> Debug for NetEndpoint<PeerId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetEndpoint::Peer(peer) => {
                write!(f, "Peer(")?;
                peer.debug_inner(f)?;
                write!(f, ")")
            }
            NetEndpoint::Proxy(addrs) => write!(f, "Proxy({addrs:?})"),
        }
    }
}

#[derive(PartialEq, Eq, Clone)]
pub struct NetPeer<PeerId> {
    pub id: PeerId,
    pub addr: SocketAddr,
    pub bm_api_addr: Option<SocketAddr>,
    pub bk_api_host_port: Option<HostPort>,
    pub bk_api_url_for_storage_sync: Option<Url>,
    pub bk_api_addr_deprecated: Option<SocketAddr>,
    pub bk_owner_pubkey: VerifyingKey,
}

impl<PeerId: Display> NetPeer<PeerId> {
    pub fn with_id_and_addr(id: PeerId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            bm_api_addr: None,
            bk_api_host_port: None,
            bk_api_url_for_storage_sync: None,
            bk_api_addr_deprecated: None,
            bk_owner_pubkey: Default::default(),
        }
    }

    pub fn debug_inner(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}, {}, {}",
            self.id,
            self.addr,
            hex::encode(self.bk_owner_pubkey.as_bytes()).chars().take(6).collect::<String>()
        )?;
        if let Some(bm) = &self.bm_api_addr {
            write!(f, ", bm:{bm}")?;
        }
        if let Some(bk) = &self.bk_api_addr_deprecated {
            write!(f, ", bk:{bk}")?;
        }
        Ok(())
    }

    pub fn resolve_bk_host_port(&self, default_port: u16) -> HostPort {
        self.bk_api_host_port
            .clone()
            .or_else(|| self.bk_api_addr_deprecated.map(|x| x.into()))
            .unwrap_or_else(|| HostPort::new(self.addr.ip().to_string(), Some(default_port)))
    }
}

impl<PeerId: Display> Debug for NetPeer<PeerId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NetPeer(")?;
        self.debug_inner(f)?;
        write!(f, ")")
    }
}

impl<PeerId> From<GossipPeer<PeerId>> for NetPeer<PeerId> {
    fn from(value: GossipPeer<PeerId>) -> Self {
        Self {
            id: value.id,
            addr: value.node_protocol_addr,
            bm_api_addr: value.bm_api_addr,
            bk_api_host_port: value.bk_api_host_port,
            bk_api_url_for_storage_sync: value.bk_api_url_for_storage_sync,
            bk_api_addr_deprecated: value.bk_api_addr_deprecated,
            bk_owner_pubkey: value.pubkey_signature.map(|(pubkey, _)| pubkey).unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetTopology<PeerId: PartialEq + Eq + Debug + Display + Hash>(Arc<Topology<PeerId>>);

#[derive(Debug)]
struct Topology<PeerId: PartialEq + Eq + Debug + Display + Hash> {
    segments: Vec<NetSegment<PeerId>>,
    i_am_peer_behind_proxy: bool,
    my_subscribe_plan: Vec<NetEndpoint<PeerId>>,
    my_segment: Option<NetSegment<PeerId>>,
    peer_resolver: HashMap<PeerId, Vec<NetPeer<PeerId>>>,
}

#[derive(Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum NetSegment<PeerId: PartialEq + Eq + Hash> {
    SinglePeer(NetPeer<PeerId>),
    Proxied(SocketAddrSet, HashMap<PeerId, Vec<NetPeer<PeerId>>>),
}

impl<PeerId: Display + Debug + PartialEq + Eq + Hash> Debug for NetSegment<PeerId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetSegment::SinglePeer(peer) => {
                write!(f, "SinglePeer(")?;
                peer.debug_inner(f)?;
                write!(f, ")")
            }
            NetSegment::Proxied(addrs, peers) => {
                write!(f, "Proxied({addrs:?}, {peers:?})")
            }
        }
    }
}

impl<PeerId: Clone + PartialEq + Eq + Display + Debug + Hash> Default for NetTopology<PeerId> {
    fn default() -> Self {
        Self(Arc::new(Topology {
            i_am_peer_behind_proxy: false,
            segments: Default::default(),
            peer_resolver: Default::default(),
            my_subscribe_plan: Default::default(),
            my_segment: Default::default(),
        }))
    }
}

#[derive(Default)]
struct SegmentBuilder<PeerId> {
    proxy_addrs: SocketAddrSet,
    peers: Vec<NetPeer<PeerId>>,
}

impl<PeerId: Clone> SegmentBuilder<PeerId> {
    fn collect_segments(gossip_peers: &[GossipPeer<PeerId>]) -> Vec<Self> {
        let mut segments = Vec::new();
        let mut segment_index_by_proxy_addr = HashMap::new();
        for peer in gossip_peers {
            let mut proxy_addrs = peer.proxies.clone();
            let mut peers = vec![peer.clone().into()];
            if proxy_addrs.is_empty() {
                segments.push(SegmentBuilder { proxy_addrs: proxy_addrs.into(), peers });
            } else {
                // Find segments with intersected subscribe addrs
                let mut existing_segment_indexes = Vec::<usize>::new();
                for addr in &proxy_addrs {
                    if let Some(index) = segment_index_by_proxy_addr.get(addr) {
                        if !existing_segment_indexes.contains(index) {
                            existing_segment_indexes.push(*index);
                        }
                    }
                }

                let dst_index = if !existing_segment_indexes.is_empty() {
                    // Merge all found segments into first
                    for i in existing_segment_indexes.iter().skip(1) {
                        proxy_addrs.extend(mem::take(&mut segments[*i].proxy_addrs));
                        peers.extend(mem::take(&mut segments[*i].peers));
                    }
                    existing_segment_indexes[0]
                } else {
                    segments.len()
                };
                for addr in &proxy_addrs {
                    segment_index_by_proxy_addr.insert(*addr, dst_index);
                }
                if dst_index < segments.len() {
                    segments[dst_index].proxy_addrs.extend(proxy_addrs);
                    segments[dst_index].peers.extend(peers);
                } else {
                    segments.push(SegmentBuilder { proxy_addrs: proxy_addrs.into(), peers });
                }
            }
        }
        segments.retain(|x| !x.proxy_addrs.is_empty() || !x.peers.is_empty());
        segments
    }
}

impl<PeerId: Clone + PartialEq + Eq + Display + Debug + Hash> NetTopology<PeerId> {
    pub fn new(config: &WatchGossipConfig<PeerId>, peers: &[GossipPeer<PeerId>]) -> Self {
        Self(Arc::new(Topology::new(config, peers)))
    }

    pub fn try_resolve_endpoint(
        &self,
        is_proxy: bool,
        pubkeys: &[VerifyingKey],
    ) -> Option<NetEndpoint<PeerId>> {
        self.0.try_resolve_endpoint(is_proxy, pubkeys)
    }

    pub fn proxied_segment_contains_peer(
        &self,
        proxy_addrs: &SocketAddrSet,
        peer_id: &PeerId,
    ) -> bool {
        self.0.proxied_segment_contains_peer(proxy_addrs, peer_id)
    }

    pub fn endpoint_is_peer_from_my_segment(&self, remote_endpoint: &NetEndpoint<PeerId>) -> bool {
        self.0.endpoint_is_peer_from_my_segment(remote_endpoint)
    }

    pub fn resolve_peer(&self, peer_id: &PeerId) -> Option<&[NetPeer<PeerId>]> {
        self.0.peer_resolver.get(peer_id).map(|x| x.as_slice())
    }

    pub fn peer_resolver(&self) -> &HashMap<PeerId, Vec<NetPeer<PeerId>>> {
        &self.0.peer_resolver
    }

    pub fn my_subscribe_plan(&self) -> &Vec<NetEndpoint<PeerId>> {
        &self.0.my_subscribe_plan
    }

    pub fn i_am_peer_behind_proxy(&self) -> bool {
        self.0.i_am_peer_behind_proxy
    }
}

impl<PeerId: Clone + PartialEq + Eq + Display + Debug + Hash> Topology<PeerId> {
    fn new(config: &WatchGossipConfig<PeerId>, gossip_peers: &[GossipPeer<PeerId>]) -> Self {
        let segment_builders = SegmentBuilder::collect_segments(gossip_peers);
        let mut segments = Vec::new();
        for mut segment_builder in segment_builders {
            if segment_builder.proxy_addrs.is_empty() {
                if let Some(peer) = segment_builder.peers.pop() {
                    segments.push(NetSegment::SinglePeer(peer));
                }
            } else {
                let mut peers = HashMap::new();
                for peer in segment_builder.peers.into_iter() {
                    if let Some(existing) = peers.get_mut(&peer.id) {
                        sorted_insert_peer(existing, peer);
                    } else {
                        peers.insert(peer.id.clone(), vec![peer]);
                    }
                }
                segments.push(NetSegment::Proxied(segment_builder.proxy_addrs, peers));
            }
        }
        let peer_resolver = Self::build_peer_resolver(config, &segments);
        let my_subscribe_plan = if !config.override_subscribe.is_empty() {
            config.override_subscribe.clone()
        } else {
            Self::build_subscribe_plan(config, &segments)
        };
        let my_segment = Self::find_my_segment(config, &segments).cloned();
        let i_am_peer_behind_proxy = matches!(
            (&config.endpoint, &my_segment),
            (NetEndpoint::Peer(_), Some(NetSegment::Proxied(_, _)))
        );
        Self { i_am_peer_behind_proxy, segments, peer_resolver, my_subscribe_plan, my_segment }
    }

    fn try_resolve_endpoint(
        &self,
        remote_is_proxy: bool,
        remote_cert_pubkeys: &[VerifyingKey],
    ) -> Option<NetEndpoint<PeerId>> {
        fn find_peer<'p, PeerId>(
            peers: &'p HashMap<PeerId, Vec<NetPeer<PeerId>>>,
            pubkeys: &[VerifyingKey],
        ) -> Option<&'p NetPeer<PeerId>> {
            peers.values().flatten().find(|peer| pubkeys.contains(&peer.bk_owner_pubkey))
        }

        if remote_is_proxy {
            // find proxied segment containing peer with pubkey from cert
            self.segments.iter().find_map(|segment| match segment {
                NetSegment::Proxied(addrs, peers) => {
                    find_peer(peers, remote_cert_pubkeys).map(|_| NetEndpoint::Proxy(addrs.clone()))
                }
                _ => None,
            })
        } else {
            // find peer with pubkey from cert
            find_peer(&self.peer_resolver, remote_cert_pubkeys).cloned().map(NetEndpoint::Peer)
        }
    }

    fn endpoint_is_peer_from_my_segment(&self, remote_endpoint: &NetEndpoint<PeerId>) -> bool {
        let NetEndpoint::Peer(remote_peer) = remote_endpoint else {
            return false;
        };
        match &self.my_segment {
            Some(NetSegment::SinglePeer(my_segment)) => my_segment.id == remote_peer.id,
            Some(NetSegment::Proxied(_, my_segment_peers)) => {
                my_segment_peers.contains_key(&remote_peer.id)
            }
            _ => false,
        }
    }

    fn proxied_segment_contains_peer(&self, proxy_addrs: &SocketAddrSet, peer_id: &PeerId) -> bool {
        for segment in &self.segments {
            if let NetSegment::Proxied(addrs, peers) = segment {
                if addrs == proxy_addrs {
                    return peers.contains_key(peer_id);
                }
            }
        }
        false
    }

    fn build_peer_resolver(
        config: &WatchGossipConfig<PeerId>,
        segments: &[NetSegment<PeerId>],
    ) -> HashMap<PeerId, Vec<NetPeer<PeerId>>> {
        let mut peer_resolver = HashMap::new();
        let mut insert_peer = |peer: &NetPeer<PeerId>| {
            let resolved_peers = if let Some(existing) = peer_resolver.get_mut(&peer.id) {
                existing
            } else {
                peer_resolver.insert(peer.id.clone(), Vec::new());
                peer_resolver.get_mut(&peer.id).unwrap()
            };
            if resolved_peers.len() < config.max_nodes_with_same_id {
                sorted_insert_peer(resolved_peers, peer.clone());
            }
        };
        for segment in segments {
            match segment {
                NetSegment::SinglePeer(peer) => {
                    insert_peer(peer);
                }
                NetSegment::Proxied(_, peers) => {
                    for peers in peers.values().flatten() {
                        insert_peer(peers);
                    }
                }
            }
        }
        peer_resolver
    }

    fn find_my_segment<'s>(
        config: &WatchGossipConfig<PeerId>,
        segments: &'s [NetSegment<PeerId>],
    ) -> Option<&'s NetSegment<PeerId>> {
        for segment in segments {
            match (&config.endpoint, segment) {
                (NetEndpoint::Peer(my_peer), NetSegment::SinglePeer(segment_peer)) => {
                    if my_peer.id == segment_peer.id {
                        return Some(segment);
                    }
                }
                (NetEndpoint::Peer(my_peer), NetSegment::Proxied(_, segment_peers)) => {
                    if segment_peers.contains_key(&my_peer.id) {
                        return Some(segment);
                    }
                }
                (NetEndpoint::Proxy(_), NetSegment::SinglePeer(_)) => {}
                (
                    NetEndpoint::Proxy(my_proxy_addrs),
                    NetSegment::Proxied(segment_proxy_addrs, _),
                ) => {
                    if my_proxy_addrs.iter().any(|x| segment_proxy_addrs.contains(x)) {
                        return Some(segment);
                    }
                }
            }
        }
        None
    }

    fn build_subscribe_plan(
        config: &WatchGossipConfig<PeerId>,
        segments: &[NetSegment<PeerId>],
    ) -> Vec<NetEndpoint<PeerId>> {
        let mut subscribe = Vec::new();
        for segment in segments {
            match (&config.endpoint, segment) {
                // node to single node segment
                (NetEndpoint::Peer(my_peer), NetSegment::SinglePeer(other_peer)) => {
                    if my_peer.id != other_peer.id {
                        subscribe.push(NetEndpoint::Peer(other_peer.clone()));
                    }
                }
                // node to proxied segment
                (NetEndpoint::Peer(_), NetSegment::Proxied(proxy_addrs, _)) => {
                    subscribe.push(NetEndpoint::Proxy(proxy_addrs.clone()));
                }
                // proxy to single node segment
                (NetEndpoint::Proxy(_), NetSegment::SinglePeer(other_peer)) => {
                    subscribe.push(NetEndpoint::Peer(other_peer.clone()));
                }
                // proxy to proxied segment
                (
                    NetEndpoint::Proxy(my_addrs),
                    NetSegment::Proxied(segment_proxies, segment_peers),
                ) => {
                    let is_my_segment = my_addrs.iter().any(|x| segment_proxies.contains(x));
                    if is_my_segment {
                        for peer in segment_peers.values().flatten() {
                            subscribe.push(NetEndpoint::Peer(peer.clone()));
                        }
                    } else {
                        subscribe.push(NetEndpoint::Proxy(segment_proxies.clone()));
                    }
                }
            }
        }
        subscribe
    }
}

fn sorted_insert_peer<PeerId>(peers: &mut Vec<NetPeer<PeerId>>, peer: NetPeer<PeerId>) {
    if let Err(index) = peers.binary_search_by_key(&peer.addr, |x| x.addr) {
        peers.insert(index, peer);
    }
}

#[cfg(test)]
mod tests {
    use gossip::default_gossip_peer_ttl_seconds;

    use super::*;

    fn addr(i: u8) -> SocketAddr {
        ([0, 0, 0, i], 0).into()
    }

    fn addrs<const N: usize>(i: [u8; N]) -> SocketAddrSet {
        i.iter().cloned().map(addr).collect()
    }

    fn gossip_peer(
        id: usize,
        advertise_addr: SocketAddr,
        proxies: SocketAddrSet,
    ) -> GossipPeer<usize> {
        GossipPeer {
            id,
            node_protocol_addr: advertise_addr,
            proxies: proxies.into(),
            pubkey_signature: None,
            bk_api_addr_deprecated: None,
            bm_api_addr: None,
            bk_api_url_for_storage_sync: None,
            bk_api_host_port: None,
        }
    }

    fn peers<const N: usize>(
        peers: [(usize, SocketAddrSet); N],
    ) -> HashMap<usize, Vec<NetPeer<usize>>> {
        peers
            .into_iter()
            .map(|(id, addrs)| {
                (id, addrs.into_iter().map(|a| NetPeer::with_id_and_addr(id, a)).collect())
            })
            .collect()
    }

    fn proxy_seg(
        subscribe: SocketAddrSet,
        peers: HashMap<usize, Vec<NetPeer<usize>>>,
    ) -> NetSegment<usize> {
        NetSegment::Proxied(subscribe, peers)
    }

    fn peer_seg(id: usize, addr: SocketAddr) -> NetSegment<usize> {
        NetSegment::SinglePeer(NetPeer::with_id_and_addr(id, addr))
    }

    fn peer_endpoint(id: usize) -> NetEndpoint<usize> {
        NetEndpoint::Peer(NetPeer::with_id_and_addr(id, addr(id as u8)))
    }

    #[test]
    fn test_net_topology() {
        let config = WatchGossipConfig {
            endpoint: peer_endpoint(1),
            max_nodes_with_same_id: 100,
            override_subscribe: Default::default(),
            peer_ttl_seconds: default_gossip_peer_ttl_seconds(),
            trusted_pubkeys: Default::default(),
        };
        let topology = NetTopology::new(
            &config,
            &[
                gossip_peer(1, addr(1), addrs([100, 101])),
                gossip_peer(1, addr(2), addrs([100, 101])),
                gossip_peer(2, addr(3), addrs([100, 101])),
                gossip_peer(3, addr(4), addrs([102, 103])),
                gossip_peer(4, addr(5), addrs([])),
            ],
        );
        assert_eq!(
            topology.0.segments,
            vec![
                proxy_seg(addrs([100, 101]), peers([(1, addrs([1, 2])), (2, addrs([3]))])),
                proxy_seg(addrs([102, 103]), peers([(3, addrs([4]))])),
                peer_seg(4, addr(5))
            ]
        );

        let topology = NetTopology::new(
            &config,
            &[
                gossip_peer(2, addr(2), addrs([101, 102])),
                gossip_peer(1, addr(1), addrs([100, 101])),
                gossip_peer(3, addr(3), addrs([102, 103])),
                gossip_peer(4, addr(5), addrs([])),
            ],
        );
        assert_eq!(
            topology.0.segments,
            vec![
                proxy_seg(
                    addrs([100, 101, 102, 103]),
                    peers([(1, addrs([1])), (2, addrs([2])), (3, addrs([3]))])
                ),
                peer_seg(4, addr(5))
            ]
        );

        let topology = NetTopology::new(
            &config,
            &[
                gossip_peer(1, addr(1), addrs([101, 102])),
                gossip_peer(2, addr(2), addrs([103, 104])),
                gossip_peer(3, addr(3), addrs([100, 101, 104])),
                gossip_peer(4, addr(4), addrs([])),
            ],
        );
        assert_eq!(
            topology.0.segments,
            vec![
                proxy_seg(
                    addrs([100, 101, 102, 103, 104]),
                    peers([(1, addrs([1])), (2, addrs([2])), (3, addrs([3]))])
                ),
                peer_seg(4, addr(4))
            ]
        );

        let topology = NetTopology::new(
            &config,
            &[
                gossip_peer(1, addr(2), addrs([102, 101, 102])),
                gossip_peer(1, addr(1), addrs([103, 102])),
                gossip_peer(1, addr(2), addrs([100, 103, 101])),
                gossip_peer(4, addr(4), addrs([])),
            ],
        );
        assert_eq!(
            topology.0.segments,
            vec![
                proxy_seg(addrs([100, 101, 102, 103]), peers([(1, addrs([1, 2]))])),
                peer_seg(4, addr(4))
            ]
        );
    }
}
