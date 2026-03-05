# gql-server Release Notes

All notable changes to `gql-server` are documented in this file.

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
