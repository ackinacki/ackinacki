# Acki Nacki helper tools

## node-helper

Node binary requires node config, which can be generated with a helper tool.
Example of node config file:

```yaml
global:
  require_minimum_blocks_to_finalize: 0
  require_minimum_time_milliseconds_to_finalize: 0
  time_to_produce_block_millis: 330
  finalization_delay_to_slow_down: 6
  slow_down_multiplier: 4
  finalization_delay_to_stop: 6
  need_synchronization_block_diff: 6
  min_time_between_state_publish_directives:
    secs: 20
    nanos: 0
  producer_group_size: 5
  producer_change_gap_size: 1000
  node_joining_timeout:
    secs: 10
    nanos: 0
  min_signatures_cnt_for_acceptance: 3
  sync_gap: 32
  sync_delay_milliseconds: 500
  save_state_frequency: 200
  block_keeper_epoch_code_hash: 603b7888daf8a20a411c99e0929c33e0615fd60fe7c55c49de03144aa564fcff
  gas_limit_for_special_transaction: 10000000
network:
  bind: 0.0.0.0:8500
  node_advertise_addr: node1:8500
  gossip_listen_addr: 0.0.0.0:10000
  gossip_advertise_addr: node1:10000
  gossip_seeds:
    - node0:10000
    - node1:10000
    - node2:10000
  lite_server_listen_addr: 127.0.0.1:11000
  static_storages:
    - http://host.docker.internal/storage/node-0/
    - http://host.docker.internal/storage/node-1/
    - http://host.docker.internal/storage/node-2/
    - http://host.docker.internal/storage/node-3/
    - http://host.docker.internal/storage/node-4/
  api_addr: 0.0.0.0:8600
  send_buffer_size: 1000
  bm_api_socket: http://127.0.0.1:81
  bk_api_socket: http://127.0.0.1:82
local:
  node_id: 1
  blockchain_config_path: config/blockchain.conf.json
  key_path: config/block_keeper1_bls.keys.json
  block_keeper_seed_path: config/block_keeper1_bls.keys.json
  zerostate_path: config/zerostate
  external_state_share_local_base_dir: /share
  parallelization_level: 20
```

Config definition:

```text
global:                                                                                             # Global node config, including block producer and synchronization settings
  require_minimum_blocks_to_finalize: 0                                                             # Number of child blocks that has to be accepted as main candidate before finalizing the block.
  require_minimum_time_milliseconds_to_finalize: 0                                                  # Time in milliseconds that is required to pass after block creation before finalization.
  time_to_produce_block_millis: 330                                                                 # Duration of one iteration of producing cycle in milliseconds.
  finalization_delay_to_slow_down: 6                                                                # Number of non-finalized blocks, after which the block producer slows down.
  slow_down_multiplier: 4                                                                           # Block producer slow down multiplier.
  finalization_delay_to_stop: 6                                                                     # Number of non-finalized blocks, after which the block producer stops.
  need_synchronization_block_diff: 6                                                                # Difference between the seq no of the incoming block and the seq no of the last applied block, which causes the node synchronization process to start.
  min_time_between_state_publish_directives:                                                        # Minimal time between publishing state.
    secs: 20
    nanos: 0
  producer_group_size: 5                                                                            # Number of nodes in producer group.
  producer_change_gap_size: 6                                                                       # Block gap size that causes block producer rotation.
  node_joining_timeout:                                                                             # Minimum timeout between sending NodeJoining messages.
    secs: 60
    nanos: 0
  min_signatures_cnt_for_acceptance: 3                                                              # Number of signatures that bloxk should collect to be considered as accepted by majority.
  sync_gap: 32                                                                                      # Block gap before sharing a state for a syncing node.
  sync_delay_milliseconds: 500                                                                      # Delay timeout before switching to synchronization mode.
  save_state_frequency: 200                                                                         # Save state to the local storage period in numver of blocks.
  block_keeper_epoch_code_hash: 603b7888daf8a20a411c99e0929c33e0615fd60fe7c55c49de03144aa564fcff    # Block keeper epoch contract code hash.
  gas_limit_for_special_transaction: 10000000                                                       # Gas limit for special transactions.
network:                                                                                            # Network settings
  bind: 0.0.0.0:8500                                                                                # Socket to listen other nodes messages (QUIC UDP).
  node_advertise_addr: node1:8500                                                                   # Public node socket address that will be advertised with gossip (QUIC UDP)
  gossip_listen_addr: 0.0.0.0:10000                                                                 # UDP socket address to listen gossip.
  gossip_advertise_addr: node1:10000                                                                # Gossip advertise socket address.
  gossip_seeds:                                                                                     # Gossip seed nodes socket addresses.
  - node0:10000
  - node1:10000
  - node2:10000
  lite_server_listen_addr: 127.0.0.1:11000                                                          
  static_storages:                                                                                  # Static storages urls (e.g. https://example.com/storage/)
  - http://host.docker.internal/storage/node-0/
  - http://host.docker.internal/storage/node-1/
  - http://host.docker.internal/storage/node-2/
  - http://host.docker.internal/storage/node-3/
  - http://host.docker.internal/storage/node-4/
  api_addr: 0.0.0.0:8600                                                                            # Socket address for SDK API.
  send_buffer_size: 1000                                                                            # Network send buffer size Defaults to 1000
  bm_api_socket: http://127.0.0.1:81                                                                # Public address for Block Manager API of this node
  bk_api_socket: http://127.0.0.1:82                                                                # Public endpoint for Block Keeper API of this node
local:                                                                                              # Node interaction settings
  node_id: 1                                                                                        # Identifier of the current node.
  blockchain_config_path: config/blockchain.conf.json                                               # Path to the file with blockchain config.
  key_path: config/block_keeper1_bls.keys.json                                                      # Path to the file with BLS key pair.
  block_keeper_seed_path: config/block_keeper1_bls.keys.json                                        # Path to the file with BLS key pair used for BK seed.
  zerostate_path: config/zerostate                                                                  # Path to zerostate file.
  external_state_share_local_base_dir: /share                                                       # Local directory path which will be shared to other nodes.
  parallelization_level: 20                                                                         # Parallelization level
```

When node loads config it tries to resolve addresses from config. It is useful when node is run in docker compose and
allows to use container name instead of ip address.
Resolve example for the config above:

```text
...
    "gossip_seeds": [
      "node0:10000",
      "node1:10000",
      "node2:10000"
    ],
...
2024-03-31T06:40:48.587894Z  INFO ThreadId(02) node: Gossip seeds expanded: ["172.19.0.7:10000", "172.19.0.10:10000", "172.19.0.9:10000"]
...
```

### install node-helper

Run in the root dir of acki-nacki repo

```bash
cargo install --path node-helper
```

### generate node config

Helper tool allows user to generate config file from scratch or update the existing one.
To create new config flag `--default` should be used.
All config arguments can be set in cli or env (cli arguments override env).

```bash
➜ node-helper config --help
Set up AckiNacki node config

Usage: node-helper config [OPTIONS] --config-file-path <CONFIG_FILE_PATH>

Options:
  -c, --config-file-path <CONFIG_FILE_PATH>
          Path to the config file
  -d, --default
          Create default config if config is invalid or does not exist
      --node-id <NODE_ID>
          This node id [env: NODE_ID=]
      --blockchain-config <BLOCKCHAIN_CONFIG>
          Blockchain config path [env: BLOCKCHAIN_CONFIG=]
      --keys-path <KEYS_PATH>
          Path to file with node key pair [env: KEYS_PATH=]
      --bind <BIND>
          Node socket to listen on (QUIC UDP) [env: BIND=]
      --node-advertise-addr <NODE_ADVERTISE_ADDR>
          Node address to advertise (QUIC UDP) [env: NODE_ADVERTISE_ADDR=]
      --gossip-listen-addr <GOSSIP_LISTEN_ADDR>
          Gossip UDP socket (e.g., 127.0.0.1:10000) [env: GOSSIP_LISTEN_ADDR=]
      --gossip-advertise-addr <GOSSIP_ADVERTISE_ADDR>
          Gossip advertise address (e.g., hostname:port or ip:port) [env: GOSSIP_ADVERTISE_ADDR=]
      --gossip-seeds <GOSSIP_SEEDS>
          Gossip seed nodes addresses (e.g., hostname:port or ip:port) [env: GOSSIP_SEEDS=]
      --lite-server-listen-addr <LITE_SERVER_LISTEN_ADDR>
          [env: LITE_SERVER_LISTEN_ADDR=]
      --static-storages <STATIC_STORAGES>
          All static stores urls-bases (e.g. "https://example.com/storage/") [env: STATIC_STORAGES=]
      --api-addr <API_ADDR>
          Socket address for SDK API [env: API_ADDR=]
      --zerostate-path <ZEROSTATE_PATH>
          Path to zerostate file [env: ZEROSTATE_PATH=]
      --external-state-share-local-base-dir <EXTERNAL_STATE_SHARE_LOCAL_BASE_DIR>
          Local shared path where to store files for sync [env: EXTERNAL_STATE_SHARE_LOCAL_BASE_DIR=]
      --network-send-buffer-size <NETWORK_SEND_BUFFER_SIZE>
          [env: NETWORK_SEND_BUFFER_SIZE=]
      --min-time-between-state-publish-directives <MIN_TIME_BETWEEN_STATE_PUBLISH_DIRECTIVES>
          [env: MIN_TIME_BETWEEN_STATE_PUBLISH_DIRECTIVES=]
      --bm_api_socket <BM_API_SOCKET>
          Public address of the Block Manager API [env: BM_API_SOCKET=]
      --bk_api_socket <BK_API_SOCKET>
          Public address of the Block Keeper API [env: BK_API_SOCKET=]
      --parallelization-level <PARALLELIZATION_LEVEL>
          [env: PARALLELIZATION_LEVEL=]
      --node-joining-timeout <NODE_JOINING_TIMEOUT>
          [env: NODE_JOINING_TIMEOUT=]
      --block-keeper-seed-path <BLOCK_KEEPER_SEED_PATH>
          [env: BLOCK_KEEPER_SEED_PATH=]
      --producer-change-gap-size <PRODUCER_CHANGE_GAP_SIZE>

  -h, --help
          Print help

```

Usage example:

```text
➜ node-helper config -d -c acki-nacki.conf.json --node-id 2             # Create default config with node_id set to `2`

➜ cat acki-nacki.conf.json | grep node_id                               # Check node_id in the config
    "node_id": 2,

➜ node-helper config -c acki-nacki.conf.json --node-id 3                # Change node_id to `3`
➜ cat acki-nacki.conf.json | grep node_id               
    "node_id": 3,

➜ NODE_ID=4 node-helper config -c acki-nacki.conf.json                  # Argument from env
➜ cat acki-nacki.conf.json | grep node_id             
    "node_id": 4,

➜ NODE_ID=0 node-helper config -c acki-nacki.conf.json --node-id 1      # Cli argument overrides env
➜ cat acki-nacki.conf.json | grep node_id                         
    "node_id": 1,
```

### Generate keys for node

node-helper can generate BLS and wallet keys:

```text
➜ node-helper bls --help   
Generate BLS key pair

Usage: node-helper bls [OPTIONS]

Options:
      --path <PATH>  Path where to store BLS key pair
  -h, --help         Print help

➜ node-helper gen-keys --help
Usage: node-helper gen-keys [OPTIONS]

Options:
      --path <PATH>  Path where to store key pair
  -h, --help         Print help

```

Usage:

Generate BLS key pair and print it:

```text
➜ node-helper bls
{
  "public": "b4a0f61cee384a4b7122f4ef60da781ff86fc205aa52080f1756358d603a415b814e3f23b6e8e86f6cb509faf6f513d7",
  "secret": "352cde0c395fbe01276c4ddbba35112ce92de2349c6a8d401fdf1e19c967c152"
}
```

Generate BLS key pair to file:

```text
➜ node-helper bls --path /tmp/bls.keys.json
```

Generate wallet key pair and print it:

```text
➜ node-helper gen-keys                     
{
  "public": "836cf25c5dc6a3997b948b97864e8a8ffe14653b4ac403217f61161c3116d0b9",
  "secret": "b15c3d4cf7df7d5b5faa0ac7ff86a3e92c12c26e86a998e5347c677a7c5c82b7"
}
```

Generate wallet keys to file:

```text
➜ node-helper gen-keys --path /tmp/master.keys.json
```

