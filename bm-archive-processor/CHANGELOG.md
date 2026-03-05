# bm-archive-processor Release Notes

All notable changes to `bm-archive-processor` are documented in this file.

## [0.2.0] - 2026-02-19

### Changed
- Replaced the `--compress` CLI flag with `--compression <none|gzip|xz>` (default: `gzip`).
- Replaced the `--require-all-servers` CLI flag with `--servers-match-mode <all|any>` (default: `any`).
- Updated processing, grouping, and SQLite integration paths to support branch migration work.
- Changed processing flow: source archive DBs are moved to `processed/` without compression, while the daily DB is moved separately with the configured compression mode and uploaded to S3 from its final processed path.
- Updated related tests, logging, and utility/config wiring.
- Updated daily DB assembly: all archive DBs in a group are now migrated to the latest schema before merge.
-  (optimization) The daily DB is initialized by copying one source DB instead of creating an empty DB from scratch.
- Fixed log formatting for redirected output: ANSI color codes are now enabled only when `stdout` is a TTY.
