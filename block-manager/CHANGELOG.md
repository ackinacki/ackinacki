# block-manager Release Notes

All notable changes to `block-manager` are documented in this file.

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
