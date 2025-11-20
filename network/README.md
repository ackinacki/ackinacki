
# Network topology

Acki Nacki network divided into segments.

There are two typ of segments:
- Proxied: contains at least one proxy and unlimited number of nodes behind proxy.
- Single node: contains single node without proxy.

# Gossip

Each node register node with:
- `node_id` node id
- `node_proxies` comma separated socket addrs of proxies
- `node_advertise_addr` socket addr of node protocol
- `bk_api_socket`
- `bm_api_socket`
- `pubkey_signature` – base64 of (VerifyingKey([u8; 32]), Signature([u8; 64]))

Signature calculation:
- Iterate all fields except `pubkey_signature`.
- Sort fields by key.
- Make string with concatenated `{key}\0{value}\0`.
- Sign string utf-8 bytes.

Proxy has no this key/values.

# Quic connections

## ALPN

There are three ALPN variants for quic connections:
- "acki-nacki-direct" connection established by BK to send direct messages.
- "acki-nacki-subscription-from-node" connection established by BK to receive broadcast messages.
- "acki-nacki-subscription-from-proxy" connection established by proxy to receive broadcast messages.

## TLS certificates

Each participant must provide a valid TLS certificate.

TLS pubkey must be signed by BK owner key pair (Ed key pair of the BK's owner wallet). This signature and owner 
pubkey stored as special extension of certificate: 
- Proxy certificate must be signed with multiple BK's owner keys(at least one):
- BK certificate must be signed by this BK owner keys. At least one of the signers must be presented 
  in current or future BK set, otherwise certificate will be treated as invalid.

TLS certificate must contain valid alternative subject names field:
- BK must specify it's advertise address as it specified in gossip record.
- Proxy must specify addresses that was specified in `proxies` field of gossip records of the BKs (not all required 
  but at least one).

- If Node config contains `proxies`: subscribe to each address from this list.
- Otherwise scan gossip nodes:
  - Skip missing signature and untrusted pubkey (not in bk sets).
  - If the node has no proxies: subscribe to the `node_advertise_addr`
  - If the node has `proxies`
    - Aggregate all such nodes by network segments (defined by `proxies` field) 
    - Connect to a random proxy from each network segment
- If the config contains a `subscribe` section, BK will subscribe to all segments listed there.
  Each segment in the config is a string containing one or more socket addresses, separated by commas.
  BK will subscribe to one randomly chosen address from each segment.
  Example:
  ```yaml
  network:
    subscribe:
    - 127.0.0.1:8600,127.0.0.2:8600
    - 127.0.0.3:8600
    - 127.0.0.4:8600,127.0.0.5:8600,127.0.0.6:8600
  ```
  In this example, there are three segments:
  - The first segment has two publishing addresses — BK will subscribe to one randomly selected address. 
  - The second segment has only one publishing address — BK will subscribe to this address.
  - The third segment has three publishing addresses — BK will again subscribe to one randomly selected address.
  As a result, BK will subscribe to three publishers in total — one from each segment.

  If you build the config file using `node-helper`, you can specify the `network-subscribe` option,
  where segments are separated by `;` and addresses within each segment are separated by `,`.
  Example (equivalent to the previous YAML config):
  
  ```shell
  node-helper ... --network-subscribe '127.0.0.1:8600,127.0.0.2:8600;127.0.0.3:8600;127.0.0.4:8600,127.0.0.5:8600,127.0.0.6:8600'
  ```
  Note: The value must be enclosed in single quotes `'` because `;` is a command separator in most shells. 
- 
- If the config contains `proxies`, BK will subscribe to one random address from the provided list.

- Otherwise, BK will scan gossip nodes:
  - Skip nodes with missing signatures or untrusted public keys (not included in the BK sets).
  - If a node has no proxies, subscribe to its `node_advertise_addr`.
  - If a node has proxies, subscribe to the first accessible proxy.

## Proxy subscriptions

- If config contains `subscribe`: subscribes to each address from this list.
- Otherwise scan gossip nodes:
  - Skip missing signature and untrusted pubkeys (not in bk sets).
  - If the node has no proxies: subscribe to the `node_advertise_addr`.
  - If the `proxies` contain this proxy: subscribe to the `node_advertise_addr`.
  - If the `proxies` does not contain this proxy: 
    - Aggregate all such nodes by network segments (defined by `proxies` field) 
    - Connect to a random proxy from each network segment

# Proxy routing

Proxy uses the next rules to broadcast messages between sender and receiver:
- If sender is proxy and receiver is proxy then resending is disallowed.
- If sender is from external segment and receiver is from ,   
