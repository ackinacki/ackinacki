# Release Notes

All notable changes to this project will be documented in this file.

## [0.15.1] – 2026-05-01

### New / Improvements
- Added cursor pagination for `blockchain.events` GraphQL queries, including event `src` and `src_dapp_id` fields and a BM archive index on external outgoing message chain order
- Added `node_transaction_execution_time` histogram and suspicious transaction execution warnings for account read, execution, and save stages
- Added JoinHandle monitor service and `node_join_handle_monitor_buffer_size` metric for block production worker threads
- Added Ansible support for configuring Block Keeper external message queue size
- Removed obsolete transitioning protocol-state handling and historical block serialization paths
- Added attestations cache to use in BP

---

### Fixes
- Fixed block production shutdown so producer thread results are received without blocking indefinitely on thread join
- Fixed panic on Block Producer failing assumptions during block production
- Removed stale `bm-schema.db` during Block Manager upgrade cleanup
- Reduced noisy rate limiter logs in the Block Keeper TLS proxy
- Fixed bp prodcounter

---

## [0.15.0] – 2026-04-24

### New / Improvements
- Added runtime-tunable `gql-server` YAML config with `--config` / `GQL_CONFIG_FILE`, `SIGUSR1` reload for pool and SQLite settings, query timeouts, OTLP metrics for GraphQL and SQLite activity, and Ansible-managed q-server config in Block Manager deployments
- Added optional ClickHouse export in Block Manager for per-transaction activity summaries
- Added snapshot helper tools for bootstrapping zerostate contracts into `ThreadSnapshot` files and viewing snapshot protocol versions
- Added HTTP retry with exponential backoff for external message forwarding in `message-router`: retries on connection errors, timeouts, and server errors (429, 500, 502, 503, 504) with up to 3 attempts
- Added per-IP rate limiting in the BK TLS proxy with Cloudflare-aware client IP handling and configurable exempt ranges
- Added `ansible/block-manager-storage-maintenance.yaml` to rotate old Block Manager archive databases, reload `q_server_bm`, and prune aged archived files
- Stopped persisting external messages on Block Keeper nodes to reduce storage growth during block production
- Updated TVM SDK to `v2.24.20.an` and switched local external-message TVM execution in `ext-messages-auth` from `tvm_client` to `tvm_contracts`

### Fixes
- Fixed GraphQL cursor pagination and resolver batching in `gql-server`, reducing skipped pages, duplicate rows, and connection pool contention on message and transaction queries
- Fixed GraphQL `lt`, `prev_trans_lt`, and `last_trans_lt` formatting so logical times are decoded from sortable storage encoding before being returned
- Fixed `message-router` handling of hex-encoded `message_hash` identifiers
- Fixed Block Manager BP resolution to preserve configured Block Producer ports instead of forcing the legacy `8600` default
- Fixed startup from `ThreadSnapshot` when the BK set is provided separately
- Fixed BK deployment config generation to advertise `bk-api-host-port` without duplicating the API port
- Fixed BK TLS proxy Ansible runs under privilege escalation by fetching Cloudflare IP ranges on `localhost` without `become`
- Fixed startup on rustls-based builds by installing the default crypto provider in both `node` and `gql-server`

---

## [0.14.3] – 2026-03-25

### New / Improvements
- Added Block Manager multi-node mode with YAML-configured BK stream and API endpoint pools, parallel subscriptions, API failover, and runtime config reload support
- Added `bm-archive-processor` improvements including multithreaded XZ compression, leftover `.db` recovery, block gap reporting, `--post-upload` handling, and single-instance locking
- Added GraphQL support for `blockchain.bkSetUpdates(...)`, block and BK-set `attestations`, and `dst` filtering for `blockchain.account.events`
- Added BM archive migration `003-attestations_bk_set_update` with new tables and indexes required for archive merging and GraphQL queries
- Added snapshot helper tools for viewing or replacing `bk_set` in `ThreadSnapshot` files and clearing finalization checkpoints
- Added TLS availability checks to Block Keeper deployment and upgrade playbooks
- Updated TVM SDK to `v2.24.13`

---

## Fixes
- Fixed node synchronization so it can continue even when the node is temporarily unable to send the next round
- Fixed noisy `SyncFinalized` handling during `NodeJoining`
- Fixed `ThreadSnapshot` compatibility so nodes can decode snapshots produced from `main`
- Fixed GraphQL rollout compatibility by deprecating legacy account queries instead of removing them immediately
- Fixed Block Manager sync resilience by reconnecting across several BK nodes instead of depending on a single source

## [0.14.0] – 2026-03-05

### New / Improvements

- Added a historical layer with Merkle hash support to the block common section and block serialization flow
- Implemented block state transition logic for the node block state lifecycle
- Updated protocol version metadata: promoted a new active version and marked the previous version as retired
- Updated core Rust dependencies and synchronized SDK versions used in the runtime and tests
- Updated the BK Docker Compose template for the new release configuration
- Added threads table prefab support for block processing and state handling
- Enabled node startup with a BK set file in deployment playbooks
- Removed deprecated `RETIRED_VERSION` environment variable usage from node startup and deployment configurations
- Updated Accumulator blockchain settings, including token naming and DApp ID parameters
- Added block post-processing for Accumulator-related block handling in node validation and repository flows
- Updated `bm-archive-processor` grouping and output controls with configurable server match mode (`all`/`any`) and compression mode (`none`/`gzip`/`xz`)
- Added support for offloading thread account state to Aerospike durable storage, including state tooling updates
- Added DApp ID propagation for external messages in node processing and HTTP API payloads
- Added traceparent header logging in Block Manager and HTTP server request paths for distributed tracing
- Added gauge metric `node_attestation_tracking_collection_size` for attestation tracking observability
- Added history-proof flow under feature-gated block production and synchronization paths
- Updated SDK integration for USDC DApp ID repair checks in block post-processing

---

## Fixes

- Added strict validation for shard-state `account_address` length (exactly 32 bytes) in the account BOC loader
- Improved `message-router` reqwest diagnostics with explicit HTTP status codes and timeout/error context
- Fixed a potential node shutdown issue during the execution stop sequence
- Fixed snapshot synchronization persistence in repository/file-saving flow to avoid inconsistent sync state
- Fixed fast clean restart playbook behavior to preserve BK set during cleanup


## [0.13.4] – 2026-02-11

### Fixed
- BM staking: Removed exit command from block manager staking script to prevent bm_staking service fault

## [0.13.3] – 2026-02-06

### New / Improvements
- The GQL server supports reading data from additional database files residing in the same directory as the current active BM's database
- Added documentation about *Block Manager Service Management*

### Fixes
- Updated the cron job to send a `SIGHUP` signal to the q-server to re-read the list of `.db` files when it changes.
- Refactored union queries.

## [0.13.2] – 2026-01-28

### New / Improvements
- Added **delegated license** check in BM wallet so that only wallets with delegated licenses may receive rewards
- Extracted signal handling into a separate function and initialized it at the start of `tokio::main`
- Removed deprecated **port 8700** from code, docker-compose, and ansible
- Zero State is optional now and is not required during node deployment on Mainnet
- Added documentation about `BM staking` and `BK migration to the new Proxy`
- Quarantine + counter metric for unparsed blocks: now if any block can not be parsed it is added to quarantine folder and metric is emmited
- Histogram: `node_gas_used` to monitor network transactional load and percentage of  used gas in a block
- Counter: `node_gas_overflow_total` to monitor number of blocks with gas overflow
- Updated **TVM SDK to v2.24.9.an**

---

## Fixes
- Fixed unsafe `unwrap` handling in the Network module
- Fixed outgoing buffer size metric that was not dropped in some cases
- Forced synchronization when no state is present

## [0.13.1] - 2026-01-15

### Improvements
- Old protocol version migrations cleaned up

## [0.13.0] - 2025-12-18

### New
- Protocol version introduced
- Rolling network upgrade supported: when majority of nodes are updated to the new version the network protocol is automatically switched to the new version
- Mobile Verifiers Miner subsystem which provides on-chain support for mining and computation verification performed using the on-chain Bee Engine backend. The subsystem is application-agnostic and can be integrated into any application
- HTTPS supported for BM, BK APIs
- Graceful shutdown supported in BM
- BM upgrade scripts
- Support of Gosh provider in TLS wasm binary
- Preflight handler for graphql server
- Versioning introduced in BM
- Guide how to migrate BK to a another server

## [0.12.11] - 2025-12-09

### Fixed
- Chitchat: dead node reappearing
- Chitchat: panic on deserializing invalid message

## [0.12.10] - 2025-11-30

### Fixed 
- Added a critical log when the gossip cluster overflows (instead of the old panic).

## [0.12.9] - 2025-11-30

### Fixed
- Fixed panic in chitchat when cluster digest size is more than 65k.
- Fixed unnecessary chitchat restarting on every sighup (only when gossip params are changed).
- Fixed reusing chitchat id on chitchat restarting (always generate new id on every chitchat restart).


## [0.12.8] - 2025-11-25

### New 
- counter `missed_blocks`  based on block height
- gauge `node_network_planned_publisher_count`
- counter `node_network_added_connections`, (attr `remote_role`: `publisher`, `subscriber`, `direct_sender)`
- counter `node_network_removed_connections`, (attr `remote_role`: `publisher`, `subscriber`, `direct_sender)`
- add `monit` target to some network module logs
- counter `missed_blocks` that tracks the number of blocks received out of order (i.e., when a block’s height is not equal to the previous block height plus 1).
- histogram `block_processing_jitter` that measures the intervals between calls to the on_incoming_block_candidate() function.
- proxy metric `node_build_info` with attrs `version`, `commit`

### Fixed
- Node could not send a next round request after syncing on the stopped network
- Proxy did not report `node_network_gossip_peers`, `node_network_gossip_live_nodes` metrics

## [0.12.7] - 2025-11-24

### Improvements
- Added gossip peers TTL

## [0.12.6] - 2025-11-20

### Fixed

- Added config network.direct_send_mode ("direct", "broadcast", "both")

## [0.12.5] - 2025-11-20

### Fixed

- Network message decoding error and state synchronization error

## [0.12.4] - 2025-11-20

### New
- Added the `node_network_publisher_count` metric to display the number of nodes or proxies the Node is subscribed to (will receive blocks from) 
- `bm-archive-helper` tool that merges daily BM data, stores it in an archive and backups daily diffs to S3
- Messages sent directly from BKs to BP such as as attestations, ACKs, NACKs are now additionally relayed via proxies for better fault-tolerance of the network.

### Improvements
- `node_sync_status.sh` script now displays sync time difference in minutes instead of hours
- Added several network module logs to the `monit` target for better observability
- Updated thread lifecycle handling: previously, some threads did not exit cleanly on SIGTERM; added proper coordination to guarantee graceful termination
- Improved the stability of graceful-shutdown.yaml ansible task: tail 50000 lines instead of 5000 when searching for the `Shutdown finished` log entry
- Node does not stop syncing when receiving old or equal state

### Fixed
- Block Producer now restarts block production in case previously produced block had an invalid (outdated) configuration, i.e. BK set was updated, block version changed (coming soon)
- `node_network_subscriber_count` reported an incorrect number of subscribers
- BK Node didnt update its network data in Gossip after receiving SIGHUP
- Duplicate propagation of received blocks on nodes without proxies:  Proxy no longer resends data, received from other proxies, to nodes without proxies 
- If a BK Node updated its IP and it had already been a Block Producer before it happened, the connection broke and did not reconnect
- `outgoing_buffer_size` metric increased during disconnect but never decreased after reconnect

## [0.12.3] - 2025-11-13

### Fixed
- Added additional check during block prefinalization and invalidation of blocks of lower round to avoid having 2 prefinalized blocks at the same height. 
- Multifactor auth failed if the seed phrase was changed.

## [0.10.1] - 2025-10-03

### New
- State sync request metrics
- Block Manager staking scripts

### Improvements
- MV contract system updates

## [0.10.0] - 2025-10-01

### Improvements
- MV contract system updates

## [0.9.0] - 2025-10-01

### New
- Metrics: `finalized_block_attestations_cnt`, `node_block_req_recv`, `node_block_req_exec`, error kinds `load_blob_fail`, `load_blob_error`
- Log NACK reason

### Improvements
- Proxy docs and scripts improved
- Zerostate checks added
- Default log level in Proxy is `Info`

### Fixed
- Block verification issue

## [0.8.2] - 2025-09-25

### Fixed
- Finalization bug

## [0.7.7] - 2025-09-23

### New
- Ability to run several instances with the same NodeID (for upgrade purpose)

### Improvements
- MV system updates
- Staking updates

### Fixed
- BK signal handling
- Sync fixes
- Graceful shutdown fixes


## [0.7.6] - 2025-09-18

### New
- `:8600/v2/bk_set_update` BK endpoint with the full bk set info
- Ability to specify BK set in `NodeConfig.bk_set_update_path`
- BK and Proxy deployment scripts now download the latest bk set from the specified BK node and gracefully restart the node with this BK set

### Improvements
- DappId table removed
- Proxy paragraph updated in README.md
  
### Fixed
- Node runs out of ephemeral ports

## [0.7.5] - 2025-09-16

### Fixed
- Node join issues

### Improvements
- Forced entering into syncing state when requesting Node Join
- Staking improvements
- RUST_BACKTRACE removed

## [0.7.4] - 2025-09-12

### New
- `/readiness` endpoint on BM
- Ability to sign Proxy certificate with multiple BK keys from BK set
- Detached attestations
- Added ability to send direct replies without NodeID
- New metrics with prefinalized blocks and authority switch metrics
- GQL server returns account data in `query{blockchain{account{info}}}` and account data is now available in Explorer


### Improvements
- DEBUG traces turned off by default, export `NODE_VERBOSE=1` to enable them. INFO, ERROR logs enabled by default
- WAL2 support in BM
- Improvements in staking scripts
- BK ansible role refactored 


### Fixed
- Node join fixes
- Load of finalized block on start
- Allow BP stop on epoch end
- Attestatio target for reject
- `bk_set`, `future_bk_set` metrics fixed
- Generate attestation for an old block if needed


## [0.7.3] - 2025-09-04

### Improvements
- Staking improvemens

### Fixed
- SIGHUP signal was handled incorrectly when the node was not in sync 

## [0.7.2] - 2025-09-03

### Improvements
- New sync metrics
  
## [0.7.1] - 2025-09-02

### Improvements
- Ansible scripts: 
  - nginx removed from BK deployment
  - set BIND, API_ADDR, MESSAGE_ROUTER via variables

### Fixed
- Staking script: create a stake even if coolers exist
- Disconnect from peers that were removed from BK set
- BIND_GOSSIP_PORT was not propagated to GOSSIP_LISTEN_ADDR which caused gossip unavailability in case of multiple BK deployment

## [0.7.0] - 2025-08-29

### New
- External messages authorization on BK using a pubkey from BK set
- Block Manager database rotation
- Fallback protocol support
- Chain invalidation mechanism
- Store cross-reference data, internal messages, and action locks in Aerospike DB
- `get_account` BK endpoint now returns `{boc, dapp_id}`
- Account events now exposed in GQL API
- WASM binary added for Multifactor Wallet token validation

### Improvements
- `Mobile Verifiers` contract system updates
- Avoid config reload on BK set update
- Proxy role enhancements
- Block Manager scripts enhancements
- Reduced block state repository lock time on initial state load
- Zerostate verification logic added
- Optimistic state is saved via a separate service
- `last_seqno` metric added to BM 
- Outbound accounts metric added to BK
- Panic hook added to BM
- State saving performance improved
- Block apply disabled on BP

### Fixed
- Apply failure 
- Split state condition
  
## [0.6.2] - 2025-07-16 

### Improvements 
- External messages processing optimizations

## [0.6.1] - 2025-07-10

### Improvements
- Master keys renamed to Node Owner keys
- Staking scripts updated

## [0.6.0] - 2025-07-08

### New
- QUIC authentication by TLS certificate generated with node_owner keys from BK set
- Multifactor wallet with the support of Google authentication released

### Fixed
- OLTP errors in logs
- Order of internal messages
- Block time correction led to infinite block generation time
- A BK node that had already been a Producer couldn't become a Producer again
- Authority switch

## [0.5.3] - 2025-07-01

### Fixed
- Multiple blocks finalizing the same block

## [0.5.2] - 2025-07-01

### New
- Storage access URLs are advertised via Gossip
- A new parameter has been added to the WASM instruction to specify the path to the compiled Rust program.
- Updated staking scripts: gracefull shutdown added
  
### Improvements  
- External messages are processed in parallel
- Backpressure implemented for QUIC streams

## [0.5.1] - 2025-06-24

### New
- `bk\v2\account` and `bm\v2\account`  APIs to get account BOC from the BK/BM node
- `runwasm` instruction
- epoch length is now measured in seqno range
- proxy deployment scripts
- Node, Proxy, Block Manager log level set to Error

### Improvements
- Disable data retranslation between proxies
- Account storage improvements
- Gossip protocol improvements
- Network layer improvements

### Fixed
- Verification failures
- Possible deadlocks

## [0.5.0] - 2025-06-02

### New
- Staking scripts support continuous staking
- BM authorization on BK
- BK node uses `SIGHUB` to update its config
- Migrate BK,BM's network layer to `msquic` library
- Append only mode for the data retrieved from gossip
- Epoch hash argument in `node-helper`

### Improvements
- Multisig updates
- New metrics: `node_bk_set_size_gauge`, `node_unfinalized_blocks_queue` 

### Fixed
- Node sync fixes


## [0.4.1] - 2025-05-13

### New
- Block Manager contracts
- BK wallet licenses limit is increased to 10
- Propagate BK's public  IP/port over gossip

### Improvements
- Multisig: Refactored the `_getSendFlags` function
  Added `reqConfirms` validation in `submitUpdate`
  Updated the expired transactions cleanup mechanism
  Added a check to ensure owner public keys is not zero
- Stability and performance improvements

## [0.4.0] – 2025-04-05

### Improvements
- Stability and performance optimizations

## [0.3.8] – 2025-01-28

### Fixed
- Performance improvements and fixes 

## [0.3.7] – 2025-01-23
### New 
- Added `http://node/bk/v1/bk_set` endpoint

### Improvements
- Resend attestations on a fork
- producer selector with seed

## [0.3.6] – 2025-01-13
### New 
Feature: BLS key set adjustment
Feature: adjustable finalization parameters
Add and rework node services: block processing, attestations handling, validator, etc.
Proxy-direct-integration-without-contracts (#383)
* Added network config params `cert_dirs`, `key_file`, `publish_proxies`, `subscribe_proxies`,
* Added support for loading TLS root certs and TLS auth from PEM files.
* added proxy connections limit
* add getter for epoch address
* feature: send only a single attestation per child per thread

### Improvements
* Readme updated: release tags are specified for images
* Add md5sum and ls -la outputs (#390)
* Slow down and speed up production on lack of attestations (#391)
* Return save of cross thread ref data for produced block
* send single attestation for forks (#394)
* fork resolution (#395)
* Add signer index to stake request and supporting new BLS keys (#384)
* Add signer index to stake request
* Add supporting new BLS keys
* fork for equally spread sigs (#396)
* fork for equally spread sigs
* Node identifier simplification. Use bk wallet address for the node identification
* Feature/resend range (#398)
* re enable verification (#403)
* Sort blocks on bp restart (#413)
* http server clone sender (#414)
* attestation target priorities (#415)
* moving send attestations service
* ensure bls contains producerid
* Add new way to get NODE ID (#407)
* Lift up gossip handler to node.rs (#416)
* lift up gossip handler to node.rs

### Fixed
* Fixed sync on the running net,
* Fixed attestation sending
* assorted stabilization fixes
* fix thread split (#392)
* Fix sync with applying valid block
* Fix check for several threads
* Fixed all bin caching, added bin hash dump, manual test functionality and skip build if image exists (#393)
* Fixed check of blocks for reference + fixes


### Breaking changes
bls keys file format has changed to allow key set changes

## [0.3.4] – 2025-01-13

### New
- Getter for Signer Index address

## [0.3.3] – 2025-01-08

### New
- `bm/v2/messages` api with synchronous message processing

### Improvements
- Refactor block state markers and state storage organization
  
### Fixed
- Attestation sending on rotate

## [0.3.2] – 2024-12-30

### New
- Proxy service 

### Fixed
- Signer index integration
- Fixed some Shellnet bugs

## [0.3.1] – 2024-12-30

### New
- Slashing
- Add log rotate to public deployments BK and BM 
  
### Improved
- Change DNS name to IPs in storage 
- Adjust real network node parameters
- Tracing spans added

### Fixed
- Node join 
- Share state
  
## [0.3.0] – 2024-12-12

### New
- Block Manager deployment documentation and scripts
- Thread split supported
- `tvm-tracing` feature added to trace tvm execution results
- `allow-dappid-thread-split` feature added to enable possibility to split into threads inside Dapp ID
- Getter for `ProxyListCode` added
- Added node binaries of 2 new types into node image: 
  - with `tvm-tracing` feature enabled
  - with `allow-dappid-thread-split` and `tvm_tracing` features enabled 
  
 ### Improved

- BK API  root URL is now `bk/v1` with 1 endpoint `bk/v1/messages` 
  that can receive POST requests with external messages (previously `topic/requests`) 
- Message Router API root URL is now `bm/v1` with 1 endpoint `bm/v1/messages` 
  that can receive POST requests with external messages. 
  This renaming is a preparation step before moving this component into Block Manager in the next releases and deprecation of Message Router.


## [0.2.0] – 2024-11-28

### New

- Static Multithreading supported
- GraphQL API is supported in Block Manager (only block indexer, Accounts API is not implemented yet)
- Block Manager ansible scripts and documentation

## [0.1.2] – 2024-11-11

### Improved

Node protocol improvements

## [0.1.1] – 2024-10-15

### Improved

Node protocol improvements

## [0.1.0] – 2024-10-04

### New

Initial release
