# block-manager Release Notes

All notable changes to `block-manager` are documented in this file.

## [0.9.1]

### Added
- The maximum finalized block age that the readiness endpoint tolerates is now
  configurable via `--readiness-max-block-age-secs` /
  `READINESS_MAX_BLOCK_AGE_SECS` (allowed range: 1–60 seconds). The readiness
  response body now also reports the configured threshold alongside the measured
  block age, so a 503 can be diagnosed without inspecting the deployment config.

### Changed
- Raised the default readiness block-age threshold from 5 to 30 seconds, so
  normal block-production jitter no longer causes spurious 503 responses.
  Deployments that relied on the previous 5-second behaviour must set
  `READINESS_MAX_BLOCK_AGE_SECS=5` explicitly.

## [0.8.1]

### Fixed
- Improved error handling in the message router for external messages with
  malformed ids — such messages are now gracefully skipped with a warning
  instead of interrupting request processing.

## [0.8.0] - 2026-06-16

### Breaking Changes
- The `/v2/account` proxy endpoint now requires separate `account_id` and `dapp_id`
  query parameters (each validated as 64-char unprefixed hex) and no longer accepts
  the legacy prefixed `address=0:...` parameter; upstream Block Keeper requests are
  forwarded as `?account_id=...&dapp_id=...`.

### Changed
- Reduced BM archive storage by persisting `blocks.data` and `transactions.boc`
  BLOBs with zstd level-3 compression (raw-bytes fallback on compression or
  decompression failure) and dropping the redundant `blocks.boc` column via
  archive migration `007-drop_blocks_boc`.
- Added a composite archive index `index_blocks_thread_chain_order` on
  `blocks(thread_id, chain_order)` to speed up thread-filtered
  `blockchain.blocks(..., thread_id)` GraphQL pagination on large archives.
