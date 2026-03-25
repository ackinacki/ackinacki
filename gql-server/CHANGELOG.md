# gql-server Release Notes

All notable changes to `gql-server` are documented in this file.

## [0.6.0] - 2026-03-04

### Added
- Added `blockchain.bkSetUpdates(...)` query with cursor pagination.
- Added `attestations` field for `blockchain.blocks(...).edges.node`.
- Added nested `attestations` field for each `bkSetUpdates` node.
- Added attestation GraphQL model with `target_type` enum (`Primary`, `Fallback`) and decoded `signature_occurrences`.
- Added optional `dst` filter parameter to `blockchain.account.events` query to filter events by destination address.
- Added validation error when `msg_type` includes `ExtOut` and `counterparties` is non-empty in `blockchain.account.messages` query.
- Added composite database index `(src, dst, msg_chain_order)` on the messages table for faster event queries with destination filtering.

### Changed
- Renamed Blockchain API block thread argument from `threadId` to `thread_id` for `blockByHeight` and `blocks`.
- Deprecated root-level query fields `account` and `accounts`.
- Deprecated `blockchain.accounts` query field.
- Added DB-layer support for querying `bk_set_updates` and `attestations` across attached archive DBs with deduplication.

### Fixed
- Improved GraphQL integration test server startup to avoid flaky port collisions during parallel test runs.

## [0.4.0] - 2026-02-12

### Added
- Added `blockByHeight(thread_id: String, height: Int)` query field in the Blockchain API.
- Added `thread_id` filter for blocks queries in the Blockchain API.
- Added `events` query field to `blockchain.account`.
- Added `finalized_timestamp` query field in the Blockchain API.

### Changed
- Deprecated root-level query fields `blocks`, `messages`, and `transactions`.

### Removed
- Removed `store_events_only` feature flag.
- Removed legacy `graphql_std` schema modules.

### Fixed
- Fixed pagination behavior in shared GraphQL query handling.
- Fixed archive attachment discovery to keep only the newest DB files by timestamp (up to `MAX_ATTACHED_DB`).
