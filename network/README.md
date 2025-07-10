
# Gossip/blockchain propagation

## Gossip

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

## Node Subscriptions

- If config contains `subscribe`: subscribe to each address from this list.
- If config contains `proxies`: subscribe to each address from this list.
- Otherwise scan gossip nodes:
  - Skip missing signature and untrusted pubkey (not in bk sets).
  - If the node has no proxies: subscribe to the `node_advertise_addr`.
  - If the node has proxies: subscribe to the first accessible proxy.

## Proxy subscriptions

- If config contains `subscribe`: subscribes to each address from this list.
- Otherwise scan gossip nodes:
  - Skip missing signature and untrusted pubkey (not in bk sets).
  - If the node has no proxies: subscribe to the `node_advertise_addr`.
  - If the `proxies` contain this proxy: subscribe to the `node_advertise_addr`.
  - If the `proxies` does not contain this proxy: subscribe to the first accessible address from this list.

## Tokio Tasks

- gossip
- pub-sub
  - listener
  - subscribe
  - connection-supervisor: for each connection
    - receiver: for outgoing connection
    - sender: for incoming connection
  - direct-sender
    - dispatcher
    - sender: for each outgoing connection
