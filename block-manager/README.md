# Block Manager

Block Manager (BM) subscribes to block streams from one or more BK nodes, applies blocks to a local SQLite database, and exposes a REST API for account queries and external messages.

## Usage

```
block-manager [OPTIONS]
```

Either `--config` or `--stream-src-url` must be provided.

### CLI arguments

| Argument | Env variable | Default | Description |
|---|---|---|---|
| `--config <PATH>` | `CONFIG_FILE` | — | Path to YAML config file (multi-node mode) |
| `--stream-src-url <URL>` | `STREAM_SRC_URL` | — | Single BK stream endpoint (legacy mode) |
| `--rest-api <ADDR>` | `REST_API` | `0.0.0.0:8001` | REST API listen address |
| `--sqlite-path <PATH>` | `SQLITE_PATH` | `./data` | SQLite database directory |
| `--clickhouse-url <URL>` | `CLICKHOUSE_URL` | — | ClickHouse URL for transaction activity export |
| `--clickhouse-user <USER>` | `CLICKHOUSE_USER` | — | ClickHouse user (required when clickhouse-url is set) |
| `--clickhouse-password <PASS>` | `CLICKHOUSE_PASSWORD` | — | ClickHouse password (required when clickhouse-url is set) |

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `DEFAULT_BP` | yes | Default BK node for API requests (`host:port`, default port 8500) |
| `BK_API_TOKEN` | yes | Bearer token for BK API authentication |
| `BM_OWNER_WALLET_PUBKEY` | no | Owner wallet public key |
| `BM_ISSUER_KEYS_FILE` | no | Path to signing keys file |

## Config file

When using `--config`, the YAML file supports multi-node block streaming and API failover.

```yaml
# BK nodes for block streaming (QUIC, port 12000)
# At least one endpoint is required.
# BM maintains up to 2 simultaneous connections from this list.
bk_stream_blocks_endpoints:
  - node0:12000
  - node1:12000
  - node2:12000

# BK nodes for account API queries (HTTP, port 8600)
# Optional. If omitted, DEFAULT_BP env variable is used.
# On request failure, BM tries the next endpoint and promotes
# the first successful one as the new default.
bk_api_endpoints:
  - node0:8600
  - node1:8600
  - node2:8600
```

Endpoint formats: `host:port`, `IP:port`, or `https://host:port`.

## Modes

### Single-node (legacy)

```
STREAM_SRC_URL=https://node0:12000 \
DEFAULT_BP=node0:8600 \
BK_API_TOKEN=secret \
block-manager
```

### Multi-node

```
DEFAULT_BP=node0:8600 \
BK_API_TOKEN=secret \
block-manager --config config.yaml
```

In multi-node mode:
- Up to 2 block-streaming connections are maintained simultaneously
- Failed connections enter a 10-second cooldown before reconnection
- Duplicate blocks from parallel connections are filtered automatically
- Account API requests fail over across `bk_api_endpoints`

## Signals

| Signal | Action |
|---|---|
| `SIGHUP` | Rotate SQLite DB file |
| `SIGUSR1` |  Reload config (multi-node mode) |
| `SIGTERM` | Graceful shutdown (flush DB, wait up to 5s) |
| `SIGINT` | Immediate exit |

### SIGHUP details

```
kill -HUP <pid>
```

On `SIGHUP` happen *SQLite DB rotation** — the current database file is closed and a new one is created. This allows external tools to archive or process the old file while BM continues writing to the new one.

### SIGUSR1 details

```
kill -USR1 <pid>
```

On `SIGUSR1` happen **Config reload** (multi-node mode only) — both `bk_stream_blocks_endpoints` and `bk_api_endpoints` are re-read from the config file. Connections to removed nodes are terminated; new nodes are picked up on the next health check cycle.

## ClickHouse export

When `--clickhouse-url` (or `CLICKHOUSE_URL`) is set, BM exports per-transaction activity summaries to the `main.tx_summary` ClickHouse table. This can be used to track active accounts and transaction statistics without querying the full SQLite archive.

Exported fields per transaction:

| Column | Type | Description |
|---|---|---|
| `tx_id` | String | Transaction hash |
| `block_time` | UInt32 | Block unix timestamp |
| `account_addr` | String | Account address |
| `tr_type` | UInt8 | Transaction type |
| `aborted` | Bool | Whether the transaction was aborted |
| `in_msg_type` | UInt8 | Inbound message type (255 = unknown) |
| `bounced` | Bool | Whether the inbound message was bounced |
| `total_fees` | String | The total amount of fees spent on processing the transaction |

Rows are batched (up to 10 000 rows or 5 s timeout) before flushing to ClickHouse. If the exporter fails, BM continues to operate normally — errors are logged but do not interrupt block processing.

If `CLICKHOUSE_URL` is not set, the exporter is disabled and no ClickHouse dependency is required.

## REST API

| Endpoint | Method | Description |
|---|---|---|
| `/v2/account?address=<addr>` | GET | Query account state from BK |
| `/v2/readiness` | GET | Health check |
| `/v2/messages` | POST | Submit external message |
