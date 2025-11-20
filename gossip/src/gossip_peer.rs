use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;

use base64::Engine;
use chitchat::NodeState;
use ed25519_dalek::Signer;
use ed25519_dalek::Verifier;

use crate::bytes_to_sign;
use crate::pubkey_signature_to_string;
use crate::ADVERTISE_ADDR_KEY;
use crate::BK_API_SOCKET_KEY;
use crate::BM_API_SOCKET_KEY;
use crate::ID_KEY;
use crate::PROXIES_KEY;
use crate::PUBKEY_SIGNATURE_KEY;

#[derive(Clone, Debug, PartialEq)]
pub struct GossipPeer<PeerId> {
    pub id: PeerId,
    pub advertise_addr: SocketAddr,
    pub proxies: Vec<SocketAddr>,
    pub bm_api_socket: Option<SocketAddr>,
    pub bk_api_socket: Option<SocketAddr>,
    pub pubkey_signature: Option<(transport_layer::VerifyingKey, transport_layer::Signature)>,
}

impl<PeerId> GossipPeer<PeerId>
where
    PeerId: FromStr<Err: Display> + Display,
{
    pub fn new(
        id: PeerId,
        advertise_addr: SocketAddr,
        proxies: Vec<SocketAddr>,
        bm_api_socket: Option<SocketAddr>,
        bk_api_socket: Option<SocketAddr>,
        signing_keys: &[transport_layer::SigningKey],
    ) -> anyhow::Result<Self> {
        let mut peer = Self {
            id,
            advertise_addr,
            proxies,
            bm_api_socket,
            bk_api_socket,
            pubkey_signature: None,
        };
        if let Some(key) = signing_keys.first() {
            let signature =
                key.sign(&bytes_to_sign(peer.values().iter().map(|(k, v)| (*k, v.as_str()))));
            peer.pubkey_signature = Some((key.verifying_key(), signature));
        }
        Ok(peer)
    }

    fn values(&self) -> Vec<(&'static str, String)> {
        let mut values = vec![
            (ID_KEY, self.id.to_string()),
            (ADVERTISE_ADDR_KEY, self.advertise_addr.to_string()),
        ];
        if !self.proxies.is_empty() {
            if let Ok(proxies) = serde_json::to_string(&self.proxies) {
                values.push((PROXIES_KEY, proxies));
            }
        }
        if let Some(bm) = &self.bm_api_socket {
            values.push((BM_API_SOCKET_KEY, bm.to_string()));
        }
        if let Some(bk) = &self.bk_api_socket {
            values.push((BK_API_SOCKET_KEY, bk.to_string()));
        }
        if let Some((pubkey, signature)) = &self.pubkey_signature {
            values.push((PUBKEY_SIGNATURE_KEY, pubkey_signature_to_string(pubkey, signature)));
        }
        values
    }

    pub fn try_from_values<'kv>(
        values: impl Iterator<Item = (&'kv str, &'kv str)>,
    ) -> Option<Self> {
        let mut peer_id = Option::<PeerId>::None;
        let mut peer_advertise_addr = None;
        let mut peer_proxies = vec![];
        let mut peer_bm_api_socket = None;
        let mut peer_bk_api_socket = None;
        let mut peer_pubkey_signature = None;
        let values = values.collect::<Vec<_>>();
        for &(k, v) in &values {
            match k {
                ID_KEY => {
                    peer_id = Some(parse_value(ID_KEY, v)?);
                }
                ADVERTISE_ADDR_KEY => {
                    peer_advertise_addr = Some(parse_value(ADVERTISE_ADDR_KEY, v)?);
                }
                BM_API_SOCKET_KEY => {
                    peer_bm_api_socket = Some(parse_value(BM_API_SOCKET_KEY, v)?);
                }
                BK_API_SOCKET_KEY => {
                    peer_bk_api_socket = Some(parse_value(BK_API_SOCKET_KEY, v)?);
                }
                PROXIES_KEY => {
                    peer_proxies = serde_json::from_str(v)
                        .inspect_err(|err| {
                            tracing::warn!("Invalid value {v} for {}: {err}", PROXIES_KEY);
                        })
                        .unwrap_or_default()
                }
                PUBKEY_SIGNATURE_KEY => {
                    peer_pubkey_signature = Some(parse_pubkey_signature(v).ok()?);
                }
                _ => {}
            }
        }
        let peer_id = peer_id?;
        let Some(peer_advertise_addr) = peer_advertise_addr else {
            tracing::error!("Missing value for {}", ADVERTISE_ADDR_KEY);
            return None;
        };

        if let Some((key, signature)) = peer_pubkey_signature {
            key.verify(&bytes_to_sign(values.into_iter()), &signature).ok()?;
        }
        let peer = Self {
            id: peer_id,
            advertise_addr: peer_advertise_addr,
            proxies: peer_proxies,
            bm_api_socket: peer_bm_api_socket,
            bk_api_socket: peer_bk_api_socket,
            pubkey_signature: peer_pubkey_signature,
        };

        Some(peer)
    }

    pub fn try_get_from(node_state: &NodeState) -> Option<Self> {
        Self::try_from_values(node_state.key_values())
    }

    pub fn set_to(&self, node_state: &mut NodeState) {
        for (k, v) in self.values() {
            node_state.set(k, v);
        }
    }
}

fn parse_value<T: FromStr>(name: &str, value: &str) -> Option<T>
where
    T::Err: Display,
{
    T::from_str(value)
        .inspect_err(|err| {
            tracing::warn!("Invalid gossip value {value} for {name}: {err}");
        })
        .ok()
}

fn parse_pubkey_signature(
    s: &str,
) -> anyhow::Result<(transport_layer::VerifyingKey, transport_layer::Signature)> {
    let buf = base64::engine::general_purpose::STANDARD.decode(s)?;
    if buf.len() != 32 + 64 {
        anyhow::bail!("Invalid pubkey signature");
    }
    let pubkey = transport_layer::VerifyingKey::from_bytes(&(buf[..32].try_into()?))?;
    let signature = transport_layer::Signature::from_bytes(buf[32..].try_into()?);
    Ok((pubkey, signature))
}

impl<PeerId: Display> Display for GossipPeer<PeerId> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GossipPeer {{ id: {}, advertise_addr: {}, proxies: [{}], bm_api_socket: {}, bk_api_socket: {} }}",
            self.id,
            self.advertise_addr,
            self.proxies.iter().map(|addr| addr.to_string()).collect::<Vec<_>>().join(", "),
            self.bm_api_socket.as_ref().map_or("None".to_string(), |s| s.to_string()),
            self.bk_api_socket.as_ref().map_or("None".to_string(), |s| s.to_string()),
        )
    }
}

#[test]
fn test_signature() {
    let a =
        GossipPeer::new("1".to_string(), ([127, 0, 0, 1], 1234).into(), vec![], None, None, &[])
            .unwrap();
    let values = a.values();
    let b = GossipPeer::<String>::try_from_values(values.iter().map(|(k, v)| (*k, v.as_str())))
        .unwrap();
    assert_eq!(a.id, b.id);
    assert_eq!(a.advertise_addr, b.advertise_addr);
    assert_eq!(a.proxies, b.proxies);
    assert_eq!(a.bm_api_socket, b.bm_api_socket);
    assert_eq!(a.bk_api_socket, b.bk_api_socket);
    assert_eq!(a.pubkey_signature, b.pubkey_signature);

    let signing_key = transport_layer::SigningKey::generate(&mut rand::rngs::OsRng);
    let a = GossipPeer::new(
        "1".to_string(),
        ([127, 0, 0, 1], 1234).into(),
        vec![],
        None,
        None,
        std::slice::from_ref(&signing_key),
    )
    .unwrap();
    let mut values = a.values();
    let b = GossipPeer::<String>::try_from_values(values.iter().map(|(k, v)| (*k, v.as_str())))
        .unwrap();
    assert_eq!(a.id, b.id);
    assert_eq!(a.advertise_addr, b.advertise_addr);
    assert_eq!(a.proxies, b.proxies);
    assert_eq!(a.bm_api_socket, b.bm_api_socket);
    assert_eq!(a.bk_api_socket, b.bk_api_socket);
    assert_eq!(a.pubkey_signature, b.pubkey_signature);

    for (k, v) in &mut values {
        if *k == PUBKEY_SIGNATURE_KEY {
            *v = "invalid".to_string();
        }
    }
    assert!(GossipPeer::<String>::try_from_values(values.iter().map(|(k, v)| (*k, v.as_str())))
        .is_none());

    for (k, v) in &mut values {
        if *k == PUBKEY_SIGNATURE_KEY {
            let fake_signature = signing_key.sign("fake".as_bytes());
            *v = pubkey_signature_to_string(&signing_key.verifying_key(), &fake_signature);
        }
    }
    assert!(GossipPeer::<String>::try_from_values(values.iter().map(|(k, v)| (*k, v.as_str())))
        .is_none());
}
