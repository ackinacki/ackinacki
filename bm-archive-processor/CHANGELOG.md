# bm-archive-processor Release Notes

All notable changes to `bm-archive-processor` are documented in this file.

## [0.5.0] - 2026-09-15

### Added
- `--daily-hook <CMD>`: command executed against the freshly built daily DB
  after assembly and before it is merged into the full DB — e.g.
  `cold-db-slim.py --run {} --daily` to trim it (NODE-3707). `{}` in the command
  is replaced with the daily path; without it the path is appended as the last
  argument. The first token is resolved through `PATH`; no shell is involved,
  so quoting is not interpreted. A non-zero exit aborts the group: nothing
  reaches the full DB, incoming files stay put, and the processor exits
  non-zero so the wrapper does not mark the day as processed.
- `--paranoid`: makes missing rows fatal instead of merely logged, and runs
  `PRAGMA quick_check` on the assembled daily (hours on large databases). Off by
  default — without it no integrity check runs at all, and missing rows only
  show up in the log line and the metrics scraped from it.
  The row check covers both merges and runs *after* the rows it checks are
  committed. A failure while assembling the daily keeps everything out of the
  full DB. A failure on the daily-to-full merge leaves that day **partially
  applied** to the full DB: nothing is corrupted, since the merge is
  `INSERT OR IGNORE` and a retry adds no duplicates, but the daily and the
  sources stay on disk and the group fails again on every run until the cause is
  fixed.
- `--upload-later`: process and compress daily DBs without S3 upload; writes a
  queue marker to `upload-queue/` for each compressed file, enabling a separate
  uploader process.
- `--upload-only <path>`: upload a single file to S3, apply `--post-upload`
  action, then exit. Designed to be called by an external uploader service
  reading from the upload queue.
- `upload-queue/` directory: file-based queue where `--upload-later` writes
  markers (filename = timestamp, content = path to the daily DB to upload).
- The startup configuration line now includes `paranoid`.

### Changed
- **Smart merge**: removed per-source-DB migration before merge. All incoming
  DBs must now be at the same schema version. This eliminates ~54 hours of
  redundant migration work (7 DBs x ~9h each).
- **Fast merge protocol**: when the full (target) DB is at v7 and source DBs are
  v3+, merge proceeds without any migration — `create_merge_query` already
  handles column differences by reading the target schema (extra columns like
  `boc` in older sources are silently ignored).
- **Version checks**: source DB versions must be equal to each other; if the
  target DB is missing the processor fails immediately (it must be pre-created
  via migration tool after rotation).
- **Fallback migration**: when the target DB is not v7, the daily DB is migrated
  once to the target version before merge (instead of migrating each source
  individually). The migrated daily is what gets uploaded afterwards.
- A source archive that has a non-empty `-wal`/`-wal2`/`-journal` sidecar next
  to it is refused: the daily starts as a byte copy of the first source, and a
  sidecar with content would mean un-checkpointed rows the copy silently
  drops. Block-manager check-points archives on rotation, so this is a guard,
  not an expected condition.
- Exit status now reflects processing outcomes: if any archive group fails, the
  processor exits non-zero. Wrappers that treat exit 0 as "day applied"
  (iterative-apply) now see the failure instead of silently losing the day. A
  group that keeps failing keeps every subsequent run non-zero on purpose,
  until an operator looks at it.
- The per-source verification line logs the exact `missing_keys=` count (the
  value the wrapper scrapes into metrics) but at most 20 example keys instead
  of the full list.
- Daily DBs are switched to `journal_mode=DELETE` during assembly, and the
  group fails if SQLite refuses the switch. They previously inherited WAL2 from
  the BM archive they were copied from, making them unreadable for stock
  SQLite; dailies uploaded to S3 before this change still require a
  WAL2-enabled build to open.
- With `--upload-later --post-upload delete`, processed source archives are
  deleted right after the daily is queued for upload (the upload itself happens
  later, in the external uploader).
- Leftover dailies found in `daily/` are queued for deferred upload when running
  with `--upload-later`; previously they were stranded (no S3 client in the
  process, no queue marker ever written). An existing queue marker is never
  overwritten, a daily that already has a marker is never compressed by the
  leftover scan (it belongs to the uploader), and dailies belonging to groups
  that failed in the same run are excluded from the scan.
- Upload-queue marker names now take everything before the first dot
  (`1760418000`, as documented in the README); previously a compressed daily
  produced `1760418000.db`, and `.db.xz`/`.db.gz` of the same day collided.
- Markers are written atomically (hidden temp file + rename), so a crash or a
  full disk cannot leave a truncated marker in the queue.
- `--dry-run` is honored by `--upload-only` (no upload, no post-upload action)
  and by the leftover scan, which no longer writes queue markers in a dry run.

### Fixed
- `--upload-only` exits non-zero when the post-upload action (delete/move)
  fails, so the deferred uploader keeps the queue marker instead of dropping it
  and re-uploading the same file to S3 Deep Archive on every cycle.
- A source or target DB whose schema version cannot be read (locked, truncated,
  not SQLite) now fails the run with an explicit error instead of being treated
  as version 0 and slipping past the version gates into the merge.

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
