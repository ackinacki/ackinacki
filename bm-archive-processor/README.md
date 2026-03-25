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
└── uploaded          (with --post-upload move)
    └── 1760418000.db.xz
```
* `db` - the current full database for the `gql-server`
* `incoming` - contains daily backups from BM (1, 2, …)
* `daily` - merged daily backups (compressed with `--compression`)
* `processed` - processed databases from `incoming` (uncompressed)
* `uploaded` - archives moved here after successful S3 upload (with `--post-upload move`)

## Business Logic

On each run, `bm-archive-processor` executes the following pipeline:

1. Scan `incoming/<BM_ID>/` directories (numeric folder names only).
2. Read archive files and extract timestamps from filenames.
3. Group files by timestamp window (default `±1h`).
4. Filter groups using `--servers-match-mode`:
   - `any`: group can be processed even if some BM servers are missing.
   - `all`: group is processed only if all BM servers are present.

For each selected group:

1. Migrate every source SQLite DB in the group to the latest schema.
2. Build daily DB:
   - copy the first DB as a base;
   - merge rows from other DBs with `INSERT OR IGNORE` for core tables.
3. Merge the produced daily DB into the full DB (`db/bm-archive.db` by default).
4. Move original source files from `incoming/<BM_ID>/` to `processed/<BM_ID>/` without compression.
5. Move the produced daily DB with compression configured by `--compression` (`gzip` / `xz` / `none`).
   XZ compression is multithreaded (uses all available CPU cores).
6. Upload that processed daily DB to S3 (unless upload is disabled by `--skip-upload`).
7. Apply `--post-upload` action to the uploaded file (`keep` / `delete` / `move`).

After all groups are processed:

1. **Leftover recovery**: compress any uncompressed `.db` files in `daily/` left from previous failed runs, then upload any compressed archives in `daily/` not yet uploaded to S3.
2. **Block gap report**: query the full database for gaps in `blocks.seq_no` and log them as warnings.

Failure behavior:

- Processing is isolated per group: one failed group does not stop processing of other groups.
- `--dry-run` skips group execution (no DB merge, no file move, no upload).

Concurrency safety:

- A `flock`-based lock (`daily/.bm-archive-processor.lock`) prevents concurrent runs. If another instance is already running, the process exits immediately with an error. The lock is released automatically on exit (including crash or kill).

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

## Post-Upload Action

Behavior after successful S3 upload is controlled by `--post-upload`:

- `move` (default): move the uploaded file to `uploaded/` directory.
- `delete`: delete the uploaded file.
- `keep`: leave the file in place.

Example:

```bash
cargo run -- --post-upload delete
```
