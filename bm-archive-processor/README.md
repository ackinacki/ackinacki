# BM Archive Processor

Run as a script from crond and ....


The `bm-archive-processor` is built as a statically-linked binary using Rust and musl.

Example folder structure — default folder names:
```
<ROOT>
├── daily
│   └── 1760418000.db.xz
├── db
│   └── bm-archive.db
├── incoming
│   ├── 1
│   │   └── bm-archive-1760418001.db
│   ├── 2
│   │   └── bm-archive-1760418020.db
│   └── 3
│       └── bm-archive-1760418000.db
├── processed
│   ├── 1
│   │   └── bm-archive-1760418001.db
│   ├── 2
│   │   └── bm-archive-1760418020.db
│   └── 3
│       └── bm-archive-1760418000.db
├── uploaded          (with --post-upload move)
│   └── 1760418000.db.xz
└── upload-queue      (with --upload-later)
    └── 1760418000
```
* `db` - the current full database for the `gql-server`
* `incoming` - contains daily backups from BM (1, 2, …)
* `daily` - merged daily backups (compressed with `--compression`)
* `processed` - processed databases from `incoming` (uncompressed)
* `uploaded` - archives moved here after successful S3 upload (with `--post-upload move`)
* `upload-queue` - queue markers written by `--upload-later` (filename = timestamp, content = path to the daily DB to upload)

## Business Logic

On each run, `bm-archive-processor` executes the following pipeline:

1. Scan `incoming/<BM_ID>/` directories (numeric folder names only).
2. Read archive files and extract timestamps from filenames.
3. Group files by timestamp window (default `±1h`).
4. Filter groups using `--servers-match-mode`:
   - `any`: group can be processed even if some BM servers are missing.
   - `all`: group is processed only if all BM servers are present.

For each selected group:

1. Verify all source DBs are at the same schema version (fail if not).
2. Build daily DB:
   - refuse the group if any source has a non-empty `-wal`/`-wal2`/`-journal` sidecar next to it (block-manager check-points archives on rotation, so this never happens in a healthy setup);
   - copy the first DB as a base;
   - merge rows from other DBs with `INSERT OR IGNORE` for core tables, logging per source how many rows did not reach the daily (`missing_keys=`, scraped into metrics by the wrapper);
   - switch the daily to `journal_mode=DELETE` so stock SQLite can open it.
   - `--daily-hook <CMD>`: run a command against the finished daily (e.g. `cold-db-slim.py --run {} --daily`). No shell: the string is split on whitespace, `{}` is replaced with the daily path (or the path is appended). A non-zero exit fails the group before anything reaches the full DB.
   - `--paranoid`: fail the group if rows of any source did not reach the daily, and run `PRAGMA quick_check` on it (hours on large databases). Without it no integrity check runs and missing rows are only logged and counted.
3. Merge the produced daily DB into the full DB (`db/bm-archive.db` by default).
   - **Fast merge** (target v7, source v3+): merge directly without migration — the merge query reads columns from the target schema, so extra columns in older sources (e.g. `boc`) are silently ignored.
   - **Fallback**: if the target is not v7 and the daily version is lower, the daily DB is migrated once to the target version before merge.
   - `--paranoid`: the same row check applies to this merge, and it runs *after* the merge is committed. A daily whose rows did not all reach the full DB fails the group with the day already partially applied there — see "Design decisions".
4. Move original source files from `incoming/<BM_ID>/` to `processed/<BM_ID>/` without compression.
5. Move the produced daily DB with compression configured by `--compression` (`gzip` / `xz` / `none`).
   XZ compression is multithreaded (uses all available CPU cores).
6. Upload that processed daily DB to S3, or enqueue for later upload:
   - Default: upload immediately (unless disabled by `--skip-upload`).
   - `--upload-later`: skip upload, write a queue marker to `upload-queue/` instead.
7. Apply `--post-upload` action to the uploaded file (`keep` / `delete` / `move`).

After all groups are processed:

1. **Leftover recovery**: compress any uncompressed `.db` files in `daily/` left from previous failed runs, then upload any compressed archives in `daily/` not yet uploaded to S3.
2. **Block gap report**: query the full database for gaps in `blocks.seq_no` and log them as warnings.

Failure behavior:

- Processing is isolated per group: one failed group does not stop processing of other groups.
- The exit status is non-zero if any group failed. Wrappers use it to decide whether a day was applied, so a group that keeps failing keeps every run non-zero until an operator intervenes — by design, a day that cannot be applied is lost data.
- `--dry-run` skips group execution (no DB merge, no file move, no upload) and writes no queue markers.

Concurrency safety:

- A `flock`-based lock (`daily/.bm-archive-processor.lock`) prevents concurrent processing runs. If another instance is already running, the process exits immediately with an error. The lock is released automatically on exit (including crash or kill).
- `--upload-only` does not take the lock, so the deferred uploader can run while a processing run is in progress. With `--compression none` (the production setting) the two never touch the same file; with compression enabled, do not run the processor and an uploader concurrently on the same `daily/`.

## Design decisions

Behaviour that looks like a gap but is deliberate; see the comments at the code sites for the full reasoning.

- **Leftover handling is best-effort and never fails the run.** It only concerns dailies whose rows are already in the full DB. A failure there is retried by the next scan; surfacing it in the exit status would make the wrapper treat a successfully applied day as lost and pull, trim and merge it again.
- **`--paranoid` verification always runs after the rows it checks are committed** — both when a source is merged into the daily and when the daily is merged into the full DB. Rolling the transaction back would discard hours of work that a retry redoes and fails identically, since a row `INSERT OR IGNORE` drops is dropped deterministically. Know the consequence: a failure while assembling the daily keeps everything out of the full DB, but a failure on the daily-to-full merge means that day is **already partially applied** to the full DB. Nothing is corrupted — the merge is idempotent, so a retry adds no duplicates — but the daily and the sources stay on disk, the group fails on every subsequent run, `--post-upload delete` never cleans up, and the wrapper never marks the day as done until an operator fixes the cause.
- **`--upload-later --post-upload delete` removes sources once the daily is queued, not once it is in S3.** The rows are in the full DB and the BMs keep their archives for weeks; holding terabytes of sources until the deferred upload completes is what the mode exists to avoid.
- **A post-upload failure in the main (direct-upload) path is logged, not fatal.** Failing the group would make the wrapper re-pull the day. Known cost: with a compressed daily, the next leftover scan uploads the file again before retrying the cleanup. `--upload-only` — the production path — propagates the error instead.
- **No quarantine for a permanently failing group.** See "Failure behavior" above.
- **Fast merge is a strict `target == v7` check.** Past v7 an older daily is migrated up before the merge (the migrated file is what gets uploaded); in steady state all versions are equal and no migration runs.

## Building the Docker Image

### Build

Build the complete Docker image from the Acki Nacki project root:

```bash
docker build -f docker/bm-tools-musl.dockerfile --target artifact -t bm-tools-musl .
```

To force a complete rebuild:

```bash
docker build --no-cache -f docker/bm-tools-musl.dockerfile --target artifact -t bm-tools-musl .
```

## Extracting the Binary Artifact

Extract the compiled binary directly during build:

```bash
docker build -f docker/bm-tools-musl.dockerfile --target artifact --output type=local,dest=./output -t bm-tools-musl .
```

The binary will be available at `./output/bm-archive-processor`

## Using

```
AWS_ACCESS_KEY_ID=XXX \
AWS_SECRET_ACCESS_KEY=YYY \
AWS_REGION=eu-west-2 \
S3_BUCKET=ackinacki-bm-archive \
cargo run
```

## Grouping Mode

Archive grouping behavior is controlled by `--servers-match-mode`:

- `any` (default): process a timestamp group even if not all BM servers have a file.
- `all`: process a timestamp group only when all BM servers are present.

To keep the previous strict behavior, run with:

```bash
cargo run -- --servers-match-mode all
```

## Compression Mode

Compression for the produced daily DB is controlled by `--compression`:

- `gzip` (default): save processed daily DB as `.db.gz`.
- `xz`: save processed daily DB as `.db.xz` (multithreaded, high-load CPU, better compression ratio).
- `none`: move processed daily DB without compression (`.db`).

Example:

```bash
cargo run -- --compression xz
```

## Deferred Upload Mode

Use `--upload-later` to decouple processing from S3 uploads. The processor completes steps 1-5 (merge, compress) and writes a queue marker instead of uploading:

```bash
# Process incoming archives, skip upload, enqueue for later
cargo run -- --upload-later --compression xz
```

A separate process (cron job, systemd service, etc.) reads from `upload-queue/` and uploads each file:

```bash
# Upload a single queued file
cargo run -- --upload-only ./daily/1760418000.db.xz --post-upload delete
```

Example uploader loop (the production script lives in gosh-infrastructure, `scripts/bm-cold/deferred-uploader.sh`):

```bash
for marker in upload-queue/[0-9]*; do
  [ -f "$marker" ] || continue
  path=$(cat "$marker")
  if [ ! -f "$path" ]; then
    # Stale marker (file renamed or already gone): drop it. The processor's
    # next leftover scan re-queues the file under its current name.
    rm -f "$marker"
    continue
  fi
  bm-archive-processor --upload-only "$path" --post-upload delete && rm "$marker"
done
```

Marker rules: one marker per timestamp (everything before the first dot of the file name); the processor never overwrites an existing marker and never compresses a daily that has one — a queued file belongs to the uploader. `--upload-only` exits non-zero if the post-upload action fails after a successful upload, so the marker survives and the file is not uploaded again on the next cycle.

## Post-Upload Action

Behavior after successful S3 upload is controlled by `--post-upload`:

- `move` (default): move the uploaded file to `uploaded/` directory.
- `delete`: delete the uploaded file and, after a successful daily archive upload, delete the source files moved to `processed/`. With `--upload-later` the sources are deleted as soon as the daily is queued (see "Design decisions").
- `keep`: leave the file in place.

Example:

```bash
cargo run -- --post-upload delete
```
