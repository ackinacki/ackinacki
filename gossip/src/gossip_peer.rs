use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;

use base64::Engine;
use chitchat::NodeState;
use ed25519_dalek::Signer;
use ed25519_dalek::Verifier;
use itertools::Itertools;
use transport_layer::HostPort;
use url::Url;

const NODE_PROTOCOL_ADDR_KEY: &str = "node_advertise_addr";
const BK_API_HOST_PORT_KEY: &str = "bk_api_host_port";
const BK_API_URL_FOR_STORAGE_SYNC_KEY: &str = "api_advertise_addr";
const BK_API_ADDR_KEY: &str = "bk_api_socket";
const ID_KEY: &str = "node_id";
const PROXIES_KEY: &str = "node_proxies";
// pubkey_signature is base64 buf with (VerifyingKey([u8; 32]), Signature([u8; 64]))
const PUBKEY_SIGNATURE_KEY: &str = "pubkey_signature";

#[derive(Clone, Debug, PartialEq)]
pub struct GossipPeer<PeerId> {
    // Peer id. Usually node id.
    pub id: PeerId,
    // Node Protocol
    pub node_protocol_addr: SocketAddr,
    pub proxies: Vec<SocketAddr>,
    // BK API.
    pub bk_api_host_port: Option<HostPort>,
    // BK API for storage sync used by nodes inside docker compose
    pub bk_api_url_for_storage_sync: Option<Url>,
    // BK API. Deprecated, use bk_api_host_port
    pub bk_api_addr_deprecated: Option<SocketAddr>,
    pub pubkey_signature: Option<(transport_layer::VerifyingKey, transport_layer::Signature)>,
}

impl<PeerId> GossipPeer<PeerId>
where
    PeerId: FromStr<Err: Display> + Display,
{
    pub fn signed(mut self, signing_keys: &[transport_layer::SigningKey]) -> anyhow::Result<Self> {
        if let Some(key) = signing_keys.first() {
            let signature =
                key.sign(&bytes_to_sign(self.values().iter().map(|(k, v)| (*k, v.as_str()))));
            self.pubkey_signature = Some((key.verifying_key(), signature));
        }
        Ok(self)
    }

    fn values(&self) -> Vec<(&'static str, String)> {
        let mut values = vec![
            (ID_KEY, self.id.to_string()),
            (NODE_PROTOCOL_ADDR_KEY, self.node_protocol_addr.to_string()),
        ];
        if !self.proxies.is_empty() {
            if let Ok(proxies) = serde_json::to_string(&self.proxies) {
                values.push((PROXIES_KEY, proxies));
            }
        }
        if let Some(host_port) = &self.bk_api_host_port {
            values.push((BK_API_HOST_PORT_KEY, host_port.to_string()));
        }
        if let Some(url) = &self.bk_api_url_for_storage_sync {
            values.push((BK_API_URL_FOR_STORAGE_SYNC_KEY, url.to_string()));
        }
        if let Some(socket) = &self.bk_api_addr_deprecated {
            values.push((BK_API_ADDR_KEY, socket.to_string()));
        }
        if let Some((pubkey, signature)) = &self.pubkey_signature {
            values.push((PUBKEY_SIGNATURE_KEY, pubkey_signature_to_string(pubkey, signature)));
        }
        values
    }

    pub fn try_from_values<'kv>(
        values: impl Iterator<Item = (&'kv str, &'kv str)>,
    ) -> Option<Self> {
        let mut id = Option::<PeerId>::None;
        let mut node_protocol_addr = None;
        let mut proxies = vec![];
        let mut bk_api_url = None;
        let mut bk_api_url_for_storage_sync = None;
        let mut bk_api_addr = None;
        let mut pubkey_signature = None;
        let values = values.collect::<Vec<_>>();
        for &(k, v) in &values {
            match k {
                ID_KEY => {
                    id = Some(parse_value(ID_KEY, v)?);
                }
                NODE_PROTOCOL_ADDR_KEY => {
                    node_protocol_addr = Some(parse_value(NODE_PROTOCOL_ADDR_KEY, v)?);
                }

                BK_API_HOST_PORT_KEY => {
                    bk_api_url = Some(parse_value(BK_API_HOST_PORT_KEY, v)?);
                }
                BK_API_URL_FOR_STORAGE_SYNC_KEY => {
                    bk_api_url_for_storage_sync =
                        Some(parse_value(BK_API_URL_FOR_STORAGE_SYNC_KEY, v)?);
                }
                BK_API_ADDR_KEY => {
                    bk_api_addr = Some(parse_value(BK_API_ADDR_KEY, v)?);
                }
                PROXIES_KEY => {
                    proxies = serde_json::from_str(v)
                        .inspect_err(|err| {
                            tracing::warn!("Invalid value {v} for {}: {err}", PROXIES_KEY);
                        })
                        .unwrap_or_default()
                }
                PUBKEY_SIGNATURE_KEY => {
                    pubkey_signature = Some(parse_pubkey_signature(v).ok()?);
                }
                _ => {}
            }
        }
        let id = id?;
        let Some(node_protocol_addr) = node_protocol_addr else {
            tracing::error!("Missing value for {}", NODE_PROTOCOL_ADDR_KEY);
            return None;
        };

        if let Some((key, signature)) = pubkey_signature {
            key.verify(&bytes_to_sign(values.into_iter()), &signature).ok()?;
        }
        let peer = Self {
            id,
            node_protocol_addr,
            proxies,
            bk_api_host_port: bk_api_url,
            bk_api_url_for_storage_sync,
            bk_api_addr_deprecated: bk_api_addr,
            pubkey_signature,
        };

        Some(peer)
    }

    pub fn try_get_from(node_state: &NodeState) -> Option<Self> {
        Self::try_from_values(node_state.key_values())
    }

    pub fn update_node_state(
        &self,
        node_state: &mut NodeState,
        signing_key_secrets: &[String],
        signing_key_paths: &[String],
    ) {
        for (k, v) in self.values() {
            node_state.set(k, v);
        }
        if let Ok(keys) =
            transport_layer::resolve_signing_keys(signing_key_secrets, signing_key_paths)
        {
            if let Some(key) = keys.first() {
                sign_gossip_node(node_state, key.clone());
            }
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
            "GossipPeer {{ id: {}, advertise_addr: {}, proxies: [{}], bk_api_socket: {} }}",
            self.id,
            self.node_protocol_addr,
            self.proxies.iter().map(|addr| addr.to_string()).collect::<Vec<_>>().join(", "),
            self.bk_api_addr_deprecated.as_ref().map_or("None".to_string(), |s| s.to_string()),
        )
    }
}

fn sign_gossip_node(node_state: &mut NodeState, key: transport_layer::SigningKey) {
    let signature = key.sign(&bytes_to_sign(node_state.key_values()));
    node_state
        .set(PUBKEY_SIGNATURE_KEY, pubkey_signature_to_string(&key.verifying_key(), &signature));
}

#[test]
fn test_signature() {
    fn test_peer() -> GossipPeer<String> {
        GossipPeer {
            id: "1".to_string(),
            node_protocol_addr: ([127, 0, 0, 1], 1234).into(),
            proxies: vec![],
            bk_api_host_port: None,
            bk_api_url_for_storage_sync: None,
            bk_api_addr_deprecated: None,
            pubkey_signature: None,
        }
    }
    let a = test_peer();
    let values = a.values();
    let b = GossipPeer::<String>::try_from_values(values.iter().map(|(k, v)| (*k, v.as_str())))
        .unwrap();
    assert_eq!(a.id, b.id);
    assert_eq!(a.node_protocol_addr, b.node_protocol_addr);
    assert_eq!(a.proxies, b.proxies);
    assert_eq!(a.bk_api_addr_deprecated, b.bk_api_addr_deprecated);
    assert_eq!(a.pubkey_signature, b.pubkey_signature);

    let signing_key = transport_layer::SigningKey::generate(&mut rand::rngs::OsRng);
    let a = test_peer().signed(std::slice::from_ref(&signing_key)).unwrap();
    let mut values = a.values();
    let b = GossipPeer::<String>::try_from_values(values.iter().map(|(k, v)| (*k, v.as_str())))
        .unwrap();
    assert_eq!(a.id, b.id);
    assert_eq!(a.node_protocol_addr, b.node_protocol_addr);
    assert_eq!(a.proxies, b.proxies);
    assert_eq!(a.bk_api_addr_deprecated, b.bk_api_addr_deprecated);
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

fn pubkey_signature_to_string(
    pubkey: &transport_layer::VerifyingKey,
    signature: &transport_layer::Signature,
) -> String {
    let mut buf = [0u8; 32 + 64];
    buf[..32].copy_from_slice(&pubkey.as_bytes()[..]);
    buf[32..].copy_from_slice(&signature.to_bytes()[..]);
    base64::engine::general_purpose::STANDARD.encode(buf)
}

fn bytes_to_sign<'kv>(key_values: impl Iterator<Item = (&'kv str, &'kv str)>) -> Vec<u8> {
    let mut data = Vec::new();
    for (k, v) in key_values.sorted_by_key(|x| x.0) {
        if k != PUBKEY_SIGNATURE_KEY {
            data.extend_from_slice(k.as_bytes());
            data.push(0);
            data.extend_from_slice(v.as_bytes());
            data.push(0);
        }
    }
    data
}
