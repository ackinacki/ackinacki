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

## Building the Docker Image

### Build

Build the complete Docker image from the Acki Nacki project root:

```bash
docker build -f docker/bm-archive-helper.dockerfile -t bm-archive-helper:latest .
```

To force a complete rebuild:

```bash
docker build --no-cache -f docker/bm-archive-helper.dockerfile -t bm-archive-helper:latest .
```

## Extracting the Binary Artifact

Extract the compiled binary directly during build:

```bash
docker build -f docker/bm-archive-helper.dockerfile --target artifact --output type=local,dest=./artifacts .
```

The binary will be available at `./artifacts/bm-archive-helper`

## Using

```
AWS_ACCESS_KEY_ID=XXX \
AWS_SECRET_ACCESS_KEY=YYY \
AWS_REGION=eu-west-2 \
S3_BUCKET=ackinacki-bm-archive \
cargo run
```
