
# Gossip/blockchain propagation

## Configs

Node (node.config.network.proxies):

node0 -> proxy0
node1 -> proxy0
node2 -> proxy1
node3 -> proxy1
node4 ->

## Subscribes

node0 -> proxy0
node1 -> proxy0
node2 -> proxy1
node3 -> proxy1
node4 -> proxy0, proxy1

proxy0 -> node0, node1, node4, proxy1
proxy1 -> node2, node3, node4, proxy0
