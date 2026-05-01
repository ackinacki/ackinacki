# migration-tool Release Notes

All notable changes to `migration-tool` are documented in this file.

## Unreleased

### Added
- Added BM archive migration `005-events_msg_chain_order_index`:
  - creates partial index `index_messages_ext_out_msg_chain_order` on `messages(msg_chain_order)` for rows where `msg_type = 2`

## [0.4.0] - 2026-04-01

### Added
- Added BM archive migration `004-transaction_in_msg_index`:
  - creates index `index_transactions_in_msg` on `transactions(in_msg)`
  - creates composite index `index_transactions_addr_order` on `transactions(account_addr, chain_order DESC)`
  - creates composite index `index_messages_src_msg_order` on `messages(src, msg_chain_order DESC)`

### Changed
- Reordered composite index `idx_blocks_thread_height` from `(height, thread_id)` to `(thread_id, height)` for better query performance.

### Removed
- Dropped 5 redundant indexes that duplicate `UNIQUE` constraints or are prefixes of composite indexes:
  - `index_messages_msg_id` (duplicate of `UNIQUE(id)`)
  - `index_messages_src` (prefix of `(src, dst, msg_chain_order)` and `(src, msg_chain_order)`)
  - `index_transactions_transaction_id` (duplicate of `UNIQUE(id)`)
  - `index_blocks_block_id` (duplicate of `UNIQUE(id)`)
  - `idx_attestations_block_id` (prefix of `UNIQUE(block_id, target_type)`)

## [0.3.0] - 2026-02-24

### Added
- Added BM archive migration `003-attestations_bk_set_update`:
  - creates `bk_set_updates` table and indexes
  - creates `attestations` table and indexes
  - adds index `index_transactions_chain_order` on `transactions` table
  - adds composite index `(src, dst, msg_chain_order)` on `messages` table
- Added migration smoke-test coverage for version `3`:
  - migrate `v2 -> v3` and verify new objects are created
  - migrate `v3 -> v2` and verify new objects are removed
