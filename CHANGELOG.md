# Release Notes

All notable changes to this project will be documented in this file.

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
