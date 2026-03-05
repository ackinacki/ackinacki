# BM Archive Processor

Run as a script from crond and ....


The `bm-archive-processor` is built as a statically-linked binary using Rust and musl.

Example folder structure — default folder names:
```
<ROOT>
├── daily
│   └── 1760418000.db
├── db
│   └── bm-archive.db
├── incoming
│   ├── 1
│   │   └── bm-archive-1760418001.db
│   ├── 2
│   │   └── bm-archive-1760418020.db
│   └── 3
│       └── bm-archive-1760418000.db
└── processed
    ├── 1
    │   └── bm-archive-1760418001.db.gz
    ├── 2
    │   └── bm-archive-1760418020.db.gz
    └── 3
        └── bm-archive-1760418000.db.gz
```
* `db` - the current full database for the `gql-server`
* `incoming` - contains daily backups from BM (1, 2, …)
* `daily` - merged daily backups
* `processed` - processed databases from `incoming`

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
6. Upload that processed daily DB to S3 (unless upload is disabled by `--skip-upload`).

Failure behavior:

- Processing is isolated per group: one failed group does not stop processing of other groups.
- `--dry-run` skips group execution (no DB merge, no file move, no upload).

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
- `xz`: save processed daily DB as `.db.xz`.
- `none`: move processed daily DB without compression (`.db`).

Example:

```bash
cargo run -- --compression xz
```
