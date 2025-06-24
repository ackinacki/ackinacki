
# Gossip/blockchain propagation

## Gossip

Each node register node with:
- `node_id` node id
- `node_proxies` comma separated socket addrs of proxies
- `node_advertise_addr` socket addr of node protocol
- `bk_api_socket`
- `bm_api_socket`

Proxy has no this key/values.

## Node Subscriptions

- If config contains `subscribe`: subscribe to each address from this list.
- If config contains `proxies`: subscribe to each address from this list.
- Otherwise scan gossip nodes:
  - If the node has no proxies: subscribe to the node's `node_advertise_addr`.
  - If the node has proxies: subscribe to the first accessible proxy.

## Proxy subscriptions

- If config contains `subscribe`: subscribes to each address from this list.
- Otherwise scan gossip nodes:
  - If the node has no proxies: subscribe to the node's `node_advertise_addr`.
  - If the node's proxies contain this proxy: subscribe to the peer's `node_advertise_addr`.
  - If the node's proxies do not contain this proxy: subscribe to the first accessible address from this list.

