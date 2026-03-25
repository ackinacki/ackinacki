# migration-tool Release Notes

All notable changes to `migration-tool` are documented in this file.

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

