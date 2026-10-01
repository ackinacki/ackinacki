# Release Notes

All notable changes to this project will be documented in this file.

## [0.19.3] – 2026-09-28

### Breaking Changes
- Moved the bridge contracts `eccUSDCBridge`, `DepositVoucher` and `EthBeaconLightClient` to https://github.com/gosh-sh/bridge (`contracts/an/`). `contracts/scripts/generate_zerostate.py` and the `tests/exchange` scripts now download them on every run from the commit pinned as `BRIDGE_COMMIT` in `contracts/scripts/bridge_contracts.py`, so they need HTTPS access to `raw.githubusercontent.com`. Without network access, set `BRIDGE_REPO` to a local clone of gosh-sh/bridge that contains the pinned commit. The downloaded files are git-ignored and overwritten on every run: to change a contract, commit the change to the bridge repository and move `BRIDGE_COMMIT`. `contracts/exchange/Makefile` is removed; rebuild the contracts in the bridge repository with `make -C contracts/an/exchange`. `config/zerostate` is regenerated with the contracts at version `1.4.0`.
- Bridge deposits now require an accepted source block: `finalizeDeposit` refuses a proof whose block hash is not in the bridge's accepted set (`ERR_UNKNOWN_BLOCK`, 224). The set is empty after a deployment or a code upgrade, so deposits fail until the owner adds hashes with `setAcceptedBlockHash` or deploys a light client with `deployLightClient`.
- The bridge now refuses a deposit or a withdrawal whose recipient is all zero bytes (`ERR_ZERO_RECIPIENT`, 230); previously the funds were lost.

### New / Improvements
- Added the `node_ext_msg_filtered` counter for external messages rejected by queue limits (`node_ext_msg_low_priority_filtered` still reports the low-priority subset) and the `node_ext_msg_rejected_not_block_producer` counter for messages rejected because the node is not the current block producer. All three carry the `thread` label.
- Reduced the node's default log volume: `http_server` now defaults to `WARN` and `ext_messages` to `INFO`, and detailed records moved to dedicated log targets. Update `RUST_LOG` filters that select the old targets or levels.
- Made the per-DApp external-message queue log opt-in: set `TEST_EXT_MESSAGES_QUEUE_LOG=true` (`1`, `yes` and `on` also work; disabled by default) to get the `ext_messages_queue_by_dapp` records back.
- Brought the bridge contracts `eccUSDCBridge`, `DepositVoucher` and `EthBeaconLightClient` to version `1.4.0`; they now require `pragma gosh-solidity >=0.80`.
- Added the `EthBeaconLightClient` contract: it verifies Ethereum sync-committee proofs and passes the block hash of each finalized checkpoint to the bridge. Only checkpoint blocks are accepted this way, because `submitAncestry` does not fit the per-transaction gas limit; deposits made in other blocks still need `setAcceptedBlockHash`.
- Added accepted-block management to `eccUSDCBridge`: `setLightClientCode`, `deployLightClient` (the light client can be deployed only by the bridge), `setAcceptedBlockHash`, `acceptBlockHashFromLightClient`, `forgetBlockHashFromLightClient`, the irreversible `disableOwnerAnchors` (leaves the light client as the only writer), and the getters `isAcceptedBlockHash`, `getLightClient` and `getAnchorConfig`.
- `contracts/scripts/generate_zerostate.py` now sets up the bridge account, including the light-client code, from the pinned bridge commit. `BRIDGE_ZS_L1_CHAIN_ID` and `BRIDGE_ZS_L1_BRIDGE` override the trusted L1 bridge of a generated zerostate (defaults: `11155111` and `0xCdFd6Cef70F68d0849310cD970F8ef8F8E4b4fdb`, Sepolia). `GiverV3.getUSDCBridgeData` is removed, so the giver's code hash changes in newly generated zerostates.

### Fixes
- Fixed load-test zerostate generation with `MV_MINERS_COUNT` placing `Miner` contracts over the `Mirror` accounts; generation now stops if a Miner address is duplicated or overlaps the Mirror range. Regenerate the load-test zerostate or snapshot before rerunning Mobile Verifiers load tests.
- Fixed `502` responses and high CPU load in the Block Keeper TLS proxy while its Block Keeper is the block producer.
- Starting with engine version `1.0.7`, unsigned external messages to Miner accounts with code hash `7ef9b5bcb1c0e33b339c8b42d53a33dbba8489c74826aec734cfb4fe845b3ae9` are rejected with the `UNSIGNED_MINER_MESSAGE` feedback error; retired `1.0.6` blocks retain the previous execution rules.
- Fixed token issuer wallet lookup failures being hidden by the default log filters: they are now logged at `WARN` on the `ext_messages_auth` target. A missing issuer account still returns `UNKNOWN_ISSUER` without a warning.
- Fixed graceful node shutdown occasionally exiting with status `101`.
- Fixed block validation keeping and verifying blocks whose height is below the latest finalized block of their thread.
- Fixed block production aborting when external messages address the same account both through its current dApp and through its redirect alias.
- Fixed finalization stalling while a node catches up after snapshot sync.

## [0.19.2] – 2026-09-15

### New / Improvements
- Changed node catch-up mode to start only after a sync snapshot is imported,
  apply only to the synced thread, and exit once that thread's unfinalized block
  queue and block receive-to-finalization delay are below the catch-up limits.
  Added the per-thread `node_catch_up_status` gauge.
- Sped up node catch-up after snapshot sync: while a node is replaying a large
  backlog it skips block validation work, suppresses sync snapshot sharing,
  stops scanning after the first blocked height, and removes several hot
  per-block trace logs.
- Added a node `ext_messages` DEBUG log for every received external message,
  including `message_id`, `destination_addr`, `dapp_id` and `function_id`.
- Added node external-message metrics: `node_ext_msg_processed_per_block`,
  `node_ext_msg_received`, `node_ext_msg_low_priority_received`,
  `node_ext_msg_low_priority_filtered`,
  `node_ext_msg_queue_low_priority_percentage` and
  `node_ext_msg_queue_low_priority_total_limit_percentage`.
- Added block-producer external-message execution metrics
  `node_ext_msg_high_priority_processed_per_block` and
  `node_ext_msg_low_priority_processed_per_block`.
- Updated TVM SDK/CLI dependency to the `v3.0.6.an` release
- Applied durable account Merkle updates in parallel for larger block-state batches, reducing node block-apply latency before attestation generation.
- Created durable account Merkle updates in parallel while producing blocks with larger account-update batches, reducing block generation latency.
- Added low-priority scheduling for external messages whose function id is in the built-in low-priority set. These messages stay under the existing total, per-DApp and per-account queue limits, but are selected only after normal-priority external messages.
- Added `ext_messages_low_priority_limit_percentage` (default `80`) to cap low-priority external messages at that share of each total, per-DApp and per-account external-message queue limit. `node-helper config` also accepts `--ext-messages-low-priority-limit-percentage` and `EXT_MESSAGES_LOW_PRIORITY_LIMIT_PERCENTAGE`.
- Changed the default external-message queue limits in the `block-keeper` Ansible role: `EXT_MESSAGES_TOTAL_LIMIT` 1000 → 1200, `EXT_MESSAGES_DAPP_LIMIT` 500 → 600.
- Added the optional node local config field `bk_set_changes_blocks_path`. When set, the node saves every finalized block that carries `block_keeper_set_changes` into that directory.
- `bm-archive-processor`: added `--daily-hook <CMD>` (runs a command, e.g. a trim script, against the daily DB before it is merged) and `--paranoid` (makes missing rows fatal in both merges and runs `PRAGMA quick_check` on the daily; without it no integrity check runs and missing rows are only logged — note that a `--paranoid` failure on the merge into the full DB leaves that day partially applied there, harmless to retry but failing every run until fixed); daily DBs are now written in `journal_mode=DELETE` so stock SQLite can open them — dailies uploaded to S3 before this change still need a WAL2-enabled SQLite; `--upload-later` / `--upload-only` decouple S3 uploads from processing through an `upload-queue/` directory; a failed archive group now makes the processor exit non-zero, so a wrapper that treated exit 0 as "day applied" sees the failure instead of losing the day
- `bm-archive-processor`: an archive group is now refused unless every incoming BM database is at the same schema version, and the full database must already exist — create and migrate `db/bm-archive.db` with `migration-tool` after a rotation, the processor no longer falls back to migrating each source itself. In exchange the per-source migration before a merge is gone (~54 hours off a seven-database day); when the full DB is ahead of the sources, the assembled daily is migrated once instead
- `migration-tool`: added `--no-journal` and `--no-check-constraints` for migrating cold archive copies; `CREATE INDEX` migrations now sort in 512 MiB runs — point `SQLITE_TMPDIR` at a volume with free space when migrating a multi-terabyte archive, or the root filesystem fills up

### Fixes
- Fixed node snapshot sync so once a downloaded snapshot is accepted, remaining
  pending or in-progress snapshot candidates are cancelled and cannot prune or
  overwrite the catch-up chain.
- Fixed node restart from shutdown so saved unfinalized blocks are relinked to
  their parents and reapplied over the last finalized durable state.
- Fixed block production external-message scheduling so a block producer tries
  to execute one low-priority external message after every four high-priority
  external messages when low-priority messages are available.
- Removed empty DApp entries from the node account state when the last account in that DApp is deleted.

## [0.19.1] – 2026-08-24

### New / Improvements
- Added a `src_dapp_id` filter to the GraphQL `blockchain.events` query, covering `ExtOut` and `ExtOutV2` with cursor pagination. Requires BM archive migration `009-events_src_dapp_id_msg_chain_order_index`; legacy `ExtOut` messages carry no `src_dapp_id` and never match the filter
- Added the `node_ext_msg_queue_size_by_dapp` gauge — external-message queue occupancy per DApp. Covers DApp ids `0`, `1`, `2` and `4`; override the set with `EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS`
- Quietened `contracts/scripts/generate_zerostate.py`: the helper commands it runs no longer echo their output
- Added the extra-currency movement of a transaction as `transaction.balance_delta_other` and `transaction.total_fees_other` — signed amounts, `null` when nothing moved. Requires BM archive migration `010-transaction_currency_deltas`; transactions archived earlier report `null`
- Showed the extra currencies in the explorer bundled with the `live` image, on message and transaction rows and details, labelled `ECC NACKL` / `ECC SHELL` / `ECC USDC` (an unnamed id as `ECC #<id>`); message rows show the value as a signed delta of the account being viewed
- Changed the default external-message queue limits in the `block-keeper` Ansible role: `EXT_MESSAGES_TOTAL_LIMIT` 250 → 1000, `EXT_MESSAGES_DAPP_LIMIT` 200 → 500, `EXT_MESSAGES_ACCOUNT_LIMIT` 100 → 5. A deployment needing a wider per-account allowance has to set the variable in its inventory

### Fixes
- Fixed the GraphQL `transaction` money fields being read as hexadecimal instead of decimal, which inflated every amount except `total_fees` — a `balance_delta` of `627700000` was served as `0x627700000`. Clients that compensated for the inflated numbers have to drop the workaround
- Fixed the explorer bundled with the `live` image rendering every outbound message of an account as incoming, and not showing an event's external destination address
- Fixed `eccUSDCBridge.initiateWithdrawal` (v1.3.1) accepting an empty `recipient`, which burned the tokens with no way to claim them; it is now refused with error `223` (`ERR_RECIPIENT_EMPTY`). `DepositVoucher` is versioned to 1.3.1 and a freshly generated zerostate carries the rebuilt contracts
- Fixed attestation delay calculation bug
- Increased block gas limit

## [0.19.0] – 2026-08-18

### Breaking Changes
- Replaced the `ext_messages_cache_size` node setting with three external-message queue limits — `ext_messages_total_limit`, `ext_messages_dapp_limit` and `ext_messages_account_limit` (also available as `node-helper config` flags and `EXT_MESSAGES_*_LIMIT` environment variables). A config still carrying the old setting keeps loading, but its value is silently ignored, so it has to be carried over by hand
- Bumped the default TVM engine version from `1.0.5` to `1.0.6`, changing the protocol version of the network
- Renamed the bridge contract `USDCBridge` to `eccUSDCBridge` (v1.3.0) and bound deposits to their source chain: the L1 `chainId` is now part of the deposit proof and of the `DepositVoucher` (v1.1.0) identity, and the verification key was rotated — proofs produced for the previous circuit are no longer accepted
- Removed the `code_hash` argument of the GraphQL `blockchain.transactions` query; it never worked and any query using it failed with a database error
- Dropped the transition path for the pre-0.18 protocol-version scheme, so an upgrade from a node older than 0.18 has to go through 0.18 first

### New / Improvements
- Made the external-message queue fair: messages are now tracked per DApp and per account and served in round-robin order under the three new limits, so one spamming DApp or account can no longer crowd everyone else out of the queue. Queue occupancy per DApp is reported in the node log
- Reported a missing destination DApp id (required since 0.16.3) as an explicit `BAD_REQUEST` / `destination dapp is not specified` in both the node API and the message-router, instead of a generic request-parsing or hex-format error
- Added a cold-storage mode to gql-server (`--cold-storage`, `GQL_COLD_STORAGE` or the `cold_storage` config option, off by default). On cold-storage servers the data that is no longer kept — `transaction.boc`, `transaction.in_message`, `message.dst_transaction` — is hidden from the schema and rejected when queried, and `blockchain.account.messages` serves external-outbound messages only
- Added the `CrossDapp` (`msg_type = 3`) and `ExtOutV2` (`msg_type = 4`) values to the GraphQL `MessageType` enum: events queries now include v2 external-outbound messages, the `ExtOut` filter covers both v1 and v2, and `ExtOutV2` selects v2 only
- Handled destruction of a pre-epoch contract: the Block Keeper leaving before its epoch starts is now published in the block as a block-keeper set change, so the set stays in sync with the BK system contracts
- Attached a quorum proof to an authority switch: the signed locks that formed the switch majority now travel with it and are verified by the receiving node, repeated requests from one signer count once, and a signer sending conflicting locks for the same round is discarded
- Added an expiration date to the zerostate — the node refuses to start on an expired one — and a `zerostate-helper state expires-at` command to set it
- Added an evaluation deployment kit (`ansible/eval-kit`) for standing up a network from scratch: deployment-kit generator, deploy playbooks, inventory template, sync-status check, an EVM migration guide and a WASM contract example
- Added an owner-managed allowlist of trusted L1 bridges to the bridge contract, so a deposit is accepted only from a known chain and contract and an L1 bridge can be rotated without downtime; deposits from an unknown source are rejected with a dedicated error, and a freshly generated zerostate is seeded with the current Sepolia bridge
- Added `UpdateCustodianMultisigWallet_v2` (v2.4.0), replacing the deprecated multisig wallet: multisig-confirmed code upgrades, lifecycle events for every transfer, custodian and code change, and getters over each request queue
- Gave the multisig wallet self-managed gas: it converts SHELL to keep its balance between the configured minimum and target, with the config changed under multisig and surviving a custodian change
- Sped up account writes to the archive on the accumulated-update path
- Restored the no-wipe testnet upgrade path and added a node restart/resync check after the upgrade
- Pinned the node, gql-server and block-manager images of the DEX end-to-end network to the build that generated its zerostate
- Updated the bundled Explorer for `dapp_id::account_id` addressing

### Fixes
- Fixed a gql-server crash when a message of the cross-DApp or v2 external-outbound type was returned
- Fixed a gql-server failure on a message with an unknown type or status, which used to abort the whole request and lose every other message in the response; such values are now returned as `null` names alongside their raw numeric value
- Fixed the Block Keeper BLS key restore step, which aborted the playbook when there was nothing to restore

## [0.18.1] – 2026-07-24

### New / Improvements
- Removed a long lock that stalled block finalization by moving repository metadata persistence off the finalization path into a dedicated background thread
- Reduced node log spam by moving verbose per-message tracing to dedicated, opt-in targets and emitting periodic aggregated summaries on hot paths instead of a line per event

### Fixes
- Fixed an attestation-aggregation race during finalization so aggregation targets the desired signature threshold and no longer drops locally produced block states before they are finalized
- Filter stale acks older than the finalization window before including them in a produced block
- Fixed block-producer broadcast timing and fallback-transition handling

## [0.18.0] – 2026-07-16

### New / Improvements
- Added a full on-chain DEX contract suite — `RootOracle`, `Oracle`, `OracleEventList`, `OrderBook`, `PMP`, `PrivateNote`, `Nullifier`, and `RootPN` — implementing Poseidon-based private notes, order books, oracle event lists, and voucher (claim) flows
- Added an AI inference market / model registry contract suite — `SuperRoot`, `RootModel`, `ModelRegistry`, `InferenceOrderBook`, and `TokenContract` — and enabled `SuperRoot` by default in the zerostate
- Renamed `TokenBridge` to `USDCBridge` throughout, finalized the ETH-deposit circuit (VKBLOB v2) verified on-chain, and required `tokenId == USDC_ECC_ID` in `initiateWithdrawal`/`finalizeDeposit`; `MVConfig` is now deployed under `MV_DAPP_ID` in the zerostate
- Reworked history-proof verification from a global layer scan into a per-block **history cursor** that is derived from the parent block and advanced as each block is applied, so blocks are now verified against their parent's cursor instead of shared global history data
- Implemented proof refs / L7 referenced-block commitment and wired the `CHKHISTPROOF` executor callback to the per-block history cursor
- Added the archive column `tracked_ext_out_messages_root`, stored it from `CommonSection` during block archive serialization, and exposed it on the GraphQL `Block` type
- Added support for the v2 external-outbound message header (`msg_type=4`): the SQLite archive stores it and GraphQL ext-out queries now match both v1 (`msg_type=2`) and v2 (`msg_type=4`)
- Restored in-flight external messages on production restart so messages erased on a producer restart can be re-queued instead of being lost
- Optimized the internal message queue processing in block production (#2267)
- Replaced `monotree` with a dense binary Merkle tree in heap layout for history/state hashing (#1927), and switched BK set commitment hashing to spec-compliant Poseidon (#2097)
- Added prebuilt bridge prover tooling — `bridge-event-halo2-prover`, `bridge-event-witness-builder`, `bridge-event-private-witness-export`, `bridge-prover-daemon`, `dex_data_exporter`, `sk-commit-tool`, and the `halo2-proover` binary — the `params/kzg_bn254_19.srs` trusted setup, and a `scripts/rebuild-bridge-bins.sh` reproducible-build script
- Reorganized the compiled contract artifacts into versioned folders (`contracts/0.79.3_compiled`, `0.80.0_compiled`, `0.81.0_compiled`) and updated all consumers (tests, `generate_zerostate.py`, `proof_helper`) to the new paths
- Added end-to-end DEX (`dexdo`) test infrastructure — Ansible playbooks, a Docker Compose stack, and CI test jobs for the DEX and the USDC bridge
- Updated the TVM SDK to `v3.0.4.an` and refreshed the bundled DEX/inference contracts and zerostate

### Fixes
- Fixed `message-router` to avoid a panic on an undecodable external-message id when block production fails (#2285)
- Fixed the attestations GraphQL API (#2243) and history/proof handling during version transition and node sync
- Fixed `proof_helper` to read higher-layer markers from the correct boundary block and replaced the deprecated GraphQL API queries with the new ones (#2117)
- Fixed `giver` compilation by removing a duplicate `getDataForAuthService`, and fixed private-note and DEX audit issues
- Pinned `axiom-eth` by rev and patched `poseidon-primitives` to the gosh-sh fork to stop `snark-verifier` stable-Rust drift; clarified the unlicensed `halo2_kzg_srs` git dependency as `MIT OR Apache-2.0` in `deny.toml` so `cargo deny check licenses` passes
- Resolved all `cargo clippy --workspace --all-targets -- -D warnings` findings (`for_kv_map`, `useless_borrows_in_formatting`, `collapsible_else_if`, `dead_code`)

---

## [0.17.0] – 2026-07-07

### New / Improvements
- Replaced the legacy block identifier with a Poseidon/SHA-256 Merkle-tree-based Acki Nacki block ID computed over 16 canonical leaves, and switched block serialization, verification, and history-proof flows to the new identifier scheme
- Updated TVM SDK from `v3.0.2.an` to `v3.0.3.an`
- Added new GraphQL `Block` fields: `block_id`, `block_merkle_tree_leaves`, `proof_block_refs`, `history_proofs`, `tracked_ext_out_messages_root`, and `tracked_ext_out_message_hashes` exposing Poseidon Merkle tree data and tracked external outbound messages
- Implemented strict ordering guarantees in the durable accumulator — deferred batches now drain in FIFO order and later batches queue behind any pending deferred work, preventing out-of-order state updates
- Hardened node shutdown: snapshot workers are cancelled via a token on shutdown, final flush is refused while deferred batches or snapshot pins remain, and the commit loop records fatal errors so drain-wait fails fast
- Implemented safer snapshot import with pre-download candidate checks (ImportNeeded / AlreadyCovered / TooClose / Invalidated), staged archive epochs for safe import, and epoch-pinned account sets to prevent races between import/export and truncation
- Improved internal message queue processing performance in block production
- Added Caddy authorization capabilities with configurable basic-auth and IP-allowlist matchers for BK API and node API endpoints
- Updated the bundled Explorer to support `dapp_id::account_id` addressing and removed the deprecated `boc` block field
- Made `mem.log.*` tracing targets opt-in through explicit `RUST_LOG` configuration instead of emitting them by default

---

### Fixes
- Fixed block production failure during durable account update when a redirect account had no DApp ID — redirect creation now uses the routing context as the source of truth instead of copying from the account body
- Fixed parent block ID resolution in the block producer
- Fixed Caddy port mapping to correctly expose BM and BK API ports
- Fixed a transitive dependency conflict where `cookie` crates pulled an incompatible `time` version, breaking fresh checkouts and CI builds
- Improved error handling in the message router for external messages with malformed ids

---

## [0.16.3] – 2026-06-16

### Breaking Changes
- Reworked the account and external-message HTTP APIs to use separate strict `account_id` and `dapp_id` fields: `GET /v2/account` now requires `account_id` and `dapp_id` (both 64-char unprefixed, lowercase-normalized hex) and rejects the legacy prefixed `address=0:...` parameter, while `POST /v2/messages` requests must include both `account_id` and `dapp_id` (the old optional `dst_dapp_id` is gone) and reject any message whose `account_id` does not match the BOC's destination
- Removed the `boc` field from the GraphQL `blockchain.block` type, replacing it with a `data` field that returns the zstd-compressed block body (clients reading `boc` must switch to `data` and decompress)

### New / Improvements
- Added zstd level-3 compression for persisted `blocks.data` and `transactions.boc` BLOBs in the SQLite archive (with raw-bytes fallback on compression/decompression failure) and dropped the redundant `blocks.boc` column via migration `007-drop_blocks_boc`, reducing Block Manager archive storage
- Reduced gql-server SQLite query load by building field-based `SELECT` projections that fetch only the GraphQL-requested columns plus the technical columns needed for pagination, ordering, deduplication, nested relation loading, and type conversion, instead of reading every column
- Added an optional `CREATE_NET` flag to the Block Keeper TLS proxy Ansible role that creates the `ackinacki-net` Docker network before composing the proxy up
- Parallelized snapshot import/export account-hash validation in the durable account repository across a configurable worker-thread pipeline (tunable via `SNAPSHOT_WRITTEN_HASH_WORKERS`/`SNAPSHOT_WRITTEN_HASH_QUEUE_BATCHES`), and added a `node_snapshot_import_time` gauge reporting per-thread snapshot import duration
- Added differentiated node shutdown modes driven by the received signal — `SIGTERM` triggers a Fast shutdown that gives in-flight snapshot workers up to 10s to finish before dumping state, while `SIGINT` triggers a Full shutdown that waits for all snapshot workers to complete — and made snapshot save/share cancellable via a cancel token checked during streaming writes
- Added a composite BM archive index `index_blocks_thread_chain_order` on `blocks(thread_id, chain_order)` (migration `006`) to speed up thread-filtered `blockchain.blocks(..., thread_id)` GraphQL pagination on large archives
- Removed the legacy `SyncFinalized` / `SyncFrom` and seq-no-anchored snapshot synchronization paths, keeping only height-anchored snapshot sync (incoming legacy `SyncFinalized` messages are now ignored, but the wire variant is retained for compatibility with older nodes)
- `bm-archive-processor` with `--post-upload delete` now also deletes the source files in `processed/` after a successful daily archive upload, reducing local disk growth
- Reworked the Caddy role to reach the Block Manager and its nginx over Docker service-discovery names on the shared `ackinacki-net` network (`block_manager`/`nginx_bm`) instead of host private-IP ports, removing the `HOST_PRIVATE_IP`/`NGINX_HOST`/`NGINX_PORT` env wiring and dropping the private-IP `80`/`443`/`8600` port bindings
- Refactored the live nginx config to use named `keepalive`-enabled upstreams (`block_manager`, `node_api`, `q_server`) with static `proxy_pass` targets instead of per-location Docker DNS resolver lookups, eliminating GraphQL/API restarts triggered by upstream re-resolution
- Added `depends_on` ordering so the Block Manager `nginx_bm` (and the docker test-gossip/test-proxy nginx services) start only after their `q_server`/`block_manager` upstreams, preventing nginx startup failures

---

### Fixes
- Fixed an underflow panic in optimistic-state cleanup logging by computing before/after counts under a single lock and using saturating subtraction for the removed-count log
- Fixed post-sync block validation to skip emitting NACKs for blocks that are already finalized, and to fail block building with a `BLOCK_HAS_MESSAGES_WITH_EQUAL_HASH` verify error (instead of an assertion panic) when an inbound message with a duplicate hash is encountered; also handles `AccountWasMoved` reroute/ignore cases when removing new messages
- Fixed `blockchain.bkSetUpdates` attestation queries to map database errors through the shared DB error mapper instead of surfacing raw error strings, and added the supporting `index_attestations_source_block_id` index
- Removed the `Upgrade`/`Connection: upgrade` proxy headers from live nginx locations so backend keepalive connections are no longer broken by spurious WebSocket-style connection upgrades
- Fixed Block Keeper graceful shutdown to force-remove the node container with `docker compose rm -f` when `docker compose stop` fails to terminate a lingering process
- Added a pre-task to the Ansible `deep-cleanup` play that probes root-filesystem health via `/var/tmp` and performs an emergency raw `rm -rf` of the data directories over SSH when the disk is full, so the module-based cleanup tasks can subsequently run

---

## [0.16.2] – 2026-05-21

### Fixes
- Fixed stale durable-account state after writes and removals by invalidating cached account bodies on writes and removing archived redirect stubs when deleting real DApp routings

---

## [0.16.1] – 2026-05-20

### Fixes
- Fixed block production slowdowns on large accounts by caching slow durable archive account reads by VM account hash and reusing cached account bodies only for the expected state-map hash
- Fixed an underflow panic in optimistic-state cleanup logging when the cache size changed between cleanup and the removed-count calculation

---

## [0.16.0] – 2026-05-13

### New / Improvements
- Added state v2 synchronization snapshots with height-based anchors and streamed `SNAP2V1` snapshot support
- Reworked durable account-state storage with archive snapshots, Aerospike-backed KV storage, and account-state metrics
- Added state snapshot loading and sharing flow for external file-share based synchronization
- Added Block Keeper deployment options for retained optimistic-state archive cleanup and optional retired config usage
- Added live-instance counters and observable gauges for `ThreadAccount`, `ThreadAccountsState`, `PendingUpdate`, `AccumulatedUpdate`, multi-map nodes, `AckiNackiBlock`, `OptimisticStateImpl`, and `BlockState`, plus `unfinalized_pool` active/draining size gauges and an `accumulator_deferred_batches` gauge
- Added block production timing histograms (`node_block_computation_time`, `node_block_serialization_time`, `node_tvm_block_serialization_time`, `node_block_tvm_apply_time`, `node_block_durable_apply_time`)

---

### Fixes
- Disabled core dump generation for Block Keeper containers
- Fixed hybrid BK-on-BM Caddy deployment to copy and use the dedicated BK TLS certificate files
- Fixed synchronization snapshot selection to prefer newer height or sequence anchors and keep usable downloaded snapshots during repeated `NodeJoining` broadcasts
- Fixed external messages being removed from the queue before execution completed, which caused unprocessed messages to be dropped instead of retained for retry
- Fixed snapshot pin leak that left the accumulator in `Requested`/`BoundaryReached` indefinitely when the snapshot worker never acquired the pin, causing the update loop to defer batches and grow RAM/swap; `request_snapshot_pin` now returns an RAII `PinRequestGuard` that auto-cancels on drop, and the legacy snapshot path acquires and releases the pin explicitly instead of leaving the request orphaned
- Fixed authority-switch sending unnecessary fallback and primary ancestor attestations from nodes that are not in the ancestor block's BK set

---

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
