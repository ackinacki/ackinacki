# block-manager Release Notes

All notable changes to `block-manager` are documented in this file.

## [0.11.0] – 2026-11-16

### Added
- History proofs store `bm-history-proofs.db` next to `bm-archive.db` in
  `SQLITE_PATH`. For thread 0 it keeps the proof fields of every block, the
  bridge `WithdrawalInitiated` and RootPN `VoucherGenerated` events, BK set
  updates and the attestations of key and rotation blocks that the bridge and
  DEX provers need. The file is never rotated, is not part of the daily
  archives and grows by about 45 MB a day. It starts empty on upgrade and fills
  from live blocks only. Back it up with
  `sqlite3 bm-history-proofs.db "VACUUM INTO '<backup>'"`, not by copying the
  file. Specification: `specs/block-manager/history_proofs_store.md`.
- `block-manager history-proofs gaps`, `export` and `import` find and close the
  gaps that every restart of block-manager or of its node leaves in the store,
  by copying height ranges from another block-manager you run. Exit codes: `0`
  complete, `1` heights still missing, `3` bad data, `2` any other error.
- Flags `--bridge-event-routing` / `BRIDGE_EVENT_ROUTING`,
  `--bridge-event-dst` / `BRIDGE_EVENT_DST`, `--voucher-event-routing` /
  `VOUCHER_EVENT_ROUTING` and `--voucher-event-dst` / `VOUCHER_EVENT_DST` select
  the stored events; the defaults are the Exchange (`eccUSDCBridge`) and RootPN
  accounts. Use the same values on every block-manager: `import` refuses a file
  exported with other ones.
- Archive table `latest_bk_set` with the exact BK set of thread 0, polled from
  the BK `/v2/bk_set` every `--bk-set-poll-interval-secs` /
  `BK_SET_POLL_INTERVAL_SECS` seconds (default `60`, range 1–3600). Requires
  the archive migration `011-latest_bk_set` and BK nodes that serve the new
  `/v2/bk_set` format: upgrade the BK nodes first. Sets of 300 or more BKs are
  not stored. Specification: `specs/block-manager/latest_bk_set.md`.
- Metrics: `bm_latest_bk_set_seqno`; `bm_errors` kinds `bk_set_poll`,
  `bk_set_invalid`, `bk_set_stalled`, `bk_set_evicted`, `bk_set_anchor`,
  `bk_set_commitment`, `bk_set_store`, `bk_set_restore`; the `bm_history_*`
  metrics of the store.

### Changed
- The Ansible role adds a Docker healthcheck to `block_manager` and starts
  `q_server_bm` only after it passes. Set `BM_HEALTHCHECK_START_PERIOD`
  (default `1h`) above the longest startup migration.
- The Ansible playbooks `upgrade-bm.yaml` and `block-manager-upgrading.yaml`
  upgrade one block-manager at a time (`bm_update_batch_size` now defaults to
  `1`) and close its store gaps from the other block-managers of the inventory
  before the next one stops (`CLOSE_HISTORY_GAPS`, `bm_history_sources`). The
  hosts and the Ansible controller need `rsync`.

## [0.10.1] – 2026-10-25

### Changed
- Moved incoming-request `traceparent` diagnostics to the `http_server` logging target, so operators can filter these DEBUG records independently from the rest of `block_manager`.

## [0.10.0]

### Added
- The BM archive now records `src_dapp_id` for external outbound v2
  (`ExtOutMsgInfoV2`, `msg_type = 4`) messages, taken from the message header, so
  events can be attributed to and filtered by the dApp that emitted them. Legacy
  `ExtOut` (`msg_type = 2`) messages carry no such header field and keep a `NULL`
  `src_dapp_id`.
- Added archive migration `009-events_src_dapp_id_msg_chain_order_index`, which
  creates the partial composite index
  `index_messages_events_src_dapp_id_msg_chain_order` on
  `messages(src_dapp_id, msg_chain_order)` for event message types, and widens the
  existing event cursor index `index_messages_ext_out_msg_chain_order` from
  `msg_type = 2` to `msg_type IN (2, 4)` so that v2 events are covered by it too.

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

## [0.9.0]

### Added
- Added archive support for the external outbound v2 message header
  (`ExtOutMsgInfoV2`), introduced in tvm-sdk `v3.0.4.an`. Such messages are now
  written to the `messages` table with `msg_type = 4` together with their `src`,
  `dst`, `created_at` and `created_lt` header fields; previously they had no
  archive representation at all.

### Changed
- Bumped the workspace `tvm_*` (tvm-sdk) dependencies from `v3.0.3.an` to
  `v3.0.4.an`.

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
