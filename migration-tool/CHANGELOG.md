# migration-tool Release Notes

All notable changes to `migration-tool` are documented in this file.

## [0.6.0]

### Added
- Added BM archive migration `010-transaction_currency_deltas`: adds the
  `balance_delta_other` and `total_fees_other` TEXT columns to `transactions`.
  Both hold a JSON array ordered by currency id
  (`[{"currency":<u32>,"value":"<signed decimal>"}]`) and are `NULL` when the
  transaction moved no extra currency. Existing rows are not backfilled — the
  columns stay `NULL` for transactions archived before the upgrade
- Added BM archive migration `009-events_src_dapp_id_msg_chain_order_index`:
  - creates a partial composite index on `messages(src_dapp_id, msg_chain_order)`
    for event message types `2` and `4`
  - extends the existing event cursor index to cover both message types

## [0.5.0]

### Added
- Added BM archive migration `008-poseidon`:
  - adds the `block_merkle_leaves`, `history_proofs`, `tracked_ext_out_messages_root`,
    `tracked_ext_out_message_hashes` and `proof_block_refs` BLOB columns to `blocks`,
    with length checks on the fixed-size ones
- Added BM archive migration `007-drop_blocks_boc`:
  - drops the `blocks.boc` column, made redundant by the zstd-compressed `blocks.data`
- Added BM archive migration `006-attestations_source_block_id_index`:
  - creates index `index_attestations_source_block_id` on `attestations(source_block_id)`
  - creates composite index `index_blocks_thread_chain_order` on `blocks(thread_id, chain_order)` for faster `blockchain.blocks(..., thread_id)` pagination.
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
