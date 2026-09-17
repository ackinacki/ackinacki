# migration-tool Release Notes

All notable changes to `migration-tool` are documented in this file.

## [0.7.0] - 2026-09-15

### Added
- `--no-journal`: runs the migration with `journal_mode=DELETE` instead of the
  journal mode persisted in the database file (WAL/WAL2). Intended for cold
  archive copies; do not use on live block-manager databases. The pragma is
  applied only when a migration actually runs — an inspect-only invocation
  (`migration-tool -p <db>` without `--block-manager`) leaves the journal mode
  untouched.
- `--no-check-constraints`: on upgrade, applies a migration's `up-nocheck.sql`
  variant when it ships one (`008-poseidon` does) — the same DDL without CHECK
  constraints. Avoids a full-table `pragma_quick_check` scan per `ALTER TABLE
  ... ADD COLUMN ... CHECK` on multi-terabyte archives (hours → seconds). The
  resulting database is stamped with the same schema version but does not
  enforce the skipped CHECKs; use only on cold copies whose rows were already
  validated at insert time. The tool refuses to run if the migration directory
  it is about to apply does not carry the expected version number.

### Changed
- Migrations run with a 1 GiB page cache. The bundled SQLite is a plain
  `bundled` build (`SQLITE_TEMP_STORE=1`), so index sorts already spill to
  disk; what the cache changes is the size of the sorter's in-memory runs
  (SQLite caps them at 512 MiB), which turns a `CREATE INDEX` over a
  multi-billion-row table from thousands of tiny merge passes into a few large
  ones. Temporary files land in `SQLITE_TMPDIR`: point it at a volume with free
  space when migrating a multi-terabyte archive, or the root filesystem fills
  up.

### Fixed
- `--no-journal` switches the journal mode only when a migration actually runs,
  and fails if SQLite refuses the switch. Previously an inspection or an
  idempotent re-run on an up-to-date database rewrote its journal mode, and a
  refused switch went unnoticed.
- A failed `user_version` read (locked, truncated or non-SQLite file) now aborts
  with an explicit error instead of being treated as schema version 0 and
  migrated from scratch.

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
