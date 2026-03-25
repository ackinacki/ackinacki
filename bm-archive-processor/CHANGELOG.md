# bm-archive-processor Release Notes

All notable changes to `bm-archive-processor` are documented in this file.

## [0.4.0] - 2026-03-20

### Added
- Multithreaded XZ compression: uses all available CPU cores via liblzma `lzma_stream_encoder_mt`, significantly reducing compression time for large daily databases.
- Leftover recovery: on each run, compress any uncompressed `.db` files left in `daily/` from previous failed runs, then upload any compressed archives not yet sent to S3.
- Block gap report: after processing, query the full database for gaps in `blocks.seq_no` and log each gap as a warning with boundary timestamps and missing count.
- `--post-upload` flag (`keep` / `delete` / `move`) to control what happens to compressed daily DB files after successful S3 upload. Default: `move` (relocates to `uploaded/` directory).
- Single-instance guard via `flock`: prevents concurrent runs that could corrupt the full database. A second instance exits immediately with an error.

### Changed
- Pipeline no longer exits early when no new archive groups are found; leftover recovery and block gap reporting still run.

## [0.3.0] - 2026-02-24

### Added
- Added support for merging new BM archive tables: `bk_set_updates` and `attestations`.
- Added verification key detection for composite uniqueness (`block_id + target_type`) used by `attestations`.
- Added/updated tests for verification key detection for both `attestations` and `bk_set_updates`.

### Changed
- Updated merge verification logic to handle tables without `id` key and to validate by `block_id`/composite key when applicable.

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

## [0.1.0] - 2026-01-15

### Added
- Initial release of `bm-archive-processor`.
- Scan `incoming/<BM_ID>/` directories for SQLite archive databases.
- Group archive files by timestamp proximity (±1h window).
- Build daily merged database from grouped archives using `INSERT OR IGNORE`.
- Merge daily database into the full cumulative database.
- Gzip compression for processed daily databases.
- S3 multipart upload to Glacier storage class with configurable concurrency.
- Atomic file operations with `.part` temp files to prevent data corruption.
- Cross-device move fallback (copy + sync + delete) for different filesystems.
- OpenTelemetry metrics integration.
- `--dry-run` mode for safe testing.
- `--skip-upload` flag to disable S3 uploads.
