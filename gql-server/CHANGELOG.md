# gql-server Release Notes

All notable changes to `gql-server` are documented in this file.

## [0.7.0] - 2026-04-07

### Added
- Added `--config` CLI flag and `GQL_CONFIG_FILE` env var for loading runtime-tunable parameters from a YAML file. Added `config-default.yaml` with annotated default values.
- Added SIGUSR1-based config reload for `max_pool_connections`, `acquire_timeout_secs`, `sqlite_query_timeout_secs`, `sqlite_mmap_size`, `sqlite_cache_size`, and `max_attached_db` without restart. Histogram boundary changes are applied at startup only.
- Added read-optimized SQLite PRAGMAs applied to every new connection: `mmap_size`, `cache_size` (configurable), `temp_store = MEMORY`, `query_only = ON`.
- Added `acquire_timeout_secs` config parameter (default 5 s) — requests waiting longer for a free connection fail fast instead of blocking indefinitely.
- Added SQLite query timeout (default 2 s) via `sqlite3_progress_handler`. Queries exceeding the limit are interrupted and return a GraphQL error with `extensions.code = "TIMEOUT"`.
- Added OpenTelemetry (OTLP) metrics: SQLite connection pool gauges (`gql_sqlite_pool_size`, `gql_sqlite_pool_idle`), successful GraphQL query duration histogram (`gql_query_duration`), GraphQL error counter (`gql_query_errors_total`), SQLite query duration histogram (`gql_sqlite_query_duration`), and build info gauge.
- Added `src_transaction_id` field to the Message GraphQL type (ID of the transaction that produced the message).
- Added debug-level logging of SQL query execution time for every query.

### Breaking Changes
- Gated deprecated root-level query resolvers (`account`, `accounts`, `blocks`, `messages`, `transactions`) and `blockchain.accounts` are now controlled via `--deprecated-api` CLI flag, `GQL_DEPRECATED_API` env var, or `deprecated_api` YAML config option. When disabled (the default), the fields are hidden from introspection and return an error if queried directly. The `deprecated_api` config option supports hot-reload via SIGUSR1.

### Changed
- Deprecated `transaction_id` field on Message in favor of `src_transaction_id`.
- Deprecated `counterparties` argument on `blockchain.account.messages` query; it will be removed in a future release.
- Resolver error handling: replaced `.ok()` with proper error propagation (`?`) in all cursor-pagination resolvers. Database errors are no longer silently swallowed.

### Fixed
- Fixed `max_pool_connections` config being ignored: `DBConnector` always used the hardcoded default (15) instead of the configured value, causing pool size to reset on attachment updates or other pool recreation events.
- Fixed N+1 connection pool contention in `account.messages` resolver: `dst_transaction` lookups are now batched into a single `IN (...)` query, and `src_transaction`/`dst_transaction` loading uses a single `load_many` call instead of per-message `load_one` awaits.
- Fixed N+1 connection pool contention in `blockchain.transactions` resolver: `in_message` and `out_messages` are now fetched in a single batched `load_many` call instead of per-transaction calls inside the loop.
- Fixed message pagination cursor to use the correct chain order field matching the SQL `ORDER BY`, preventing skipped or duplicated rows during forward/backward pagination.
- Fixed `hasPreviousPage` / `hasNextPage` returning `false` when `after` / `before` cursors are present. Previously `hasPreviousPage` was always `false` for forward pagination (`first`/`after`) and `hasNextPage` was always `false` for backward pagination (`last`/`before`).

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
