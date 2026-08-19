# gql-server

GraphQL API server for the Acki Nacki blockchain. Serves read-only queries against a SQLite archive database (`bm-archive.db`) and its rotated archive files.

## Usage

```bash
gql-server \
  --db data/bm-archive.db \
  --listen 127.0.0.1:3000 \
  --bm_api_socket http://127.0.0.1:5000 \
  --config config.yaml
```

| Flag | Env | Description |
|------|-----|-------------|
| `--db` | `DB` | Path to the main SQLite database file |
| `--listen` | `LISTEN` | Host and port to bind (default `127.0.0.1:3000`) |
| `--bm_api_socket` | `BM_API_SOCKET` | Block Manager API endpoint (required) |
| `--config` | `GQL_CONFIG_FILE` | Path to YAML config for runtime-tunable parameters |
| `--deprecated-api` | `GQL_DEPRECATED_API` | Enable deprecated API fields |
| `--cold-storage` | `GQL_COLD_STORAGE` | Run in cold-storage mode (hide fields whose data is not stored on cold-storage servers) |

## Configuration

Runtime parameters can be loaded from a YAML file (see `config-default.yaml` for annotated defaults).

### Hot-reload

Send `SIGUSR1` to reload the config without restarting:

```bash
kill -SIGUSR1 $(pidof gql-server)
```

| Parameter | Hot-reload | Description |
|-----------|-----------|-------------|
| `max_pool_connections` | yes (pool recreated) | Maximum SQLite connection pool size |
| `sqlite_query_timeout_secs` | yes | Query timeout before `SQLITE_INTERRUPT` |
| `max_attached_db` | yes (next archive resolve) | Max attached archive DBs (capped at 9) |
| `query_duration_boundaries` | startup only | Histogram buckets for GraphQL query duration |
| `sqlite_query_boundaries` | startup only | Histogram buckets for SQLite query duration |
| `deprecated_api` | yes | Enable/disable deprecated API fields |
| `cold_storage` | yes | Enable/disable cold-storage mode |

### Signals

| Signal | Action |
|--------|--------|
| `SIGHUP` | Re-scan and attach archive database files |
| `SIGUSR1` | Reload YAML config file |

## Metrics

Metrics are exported via OpenTelemetry (OTLP) when the `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable is set.

| Metric | Type | Description |
|--------|------|-------------|
| `gql_query_duration` | Histogram | GraphQL query execution time (ms) |
| `gql_sqlite_query_duration` | Histogram | SQLite query execution time (ms) |
| `gql_sqlite_pool_size` | Gauge | Total connections in the pool |
| `gql_sqlite_pool_idle` | Gauge | Idle connections in the pool |
| `gql_build_info` | Gauge | Build version and commit labels |

## Deprecated API

Deprecated root-level query resolvers (`account`, `accounts`, `blocks`, `messages`,
`transactions`) and `blockchain.accounts` are disabled by default. Enable them at
runtime with the `--deprecated-api` CLI flag, the `GQL_DEPRECATED_API=true`
environment variable, or the `deprecated_api: true` option in the YAML config file.

```bash
gql-server --deprecated-api
# or
GQL_DEPRECATED_API=true gql-server
```

## Cold storage

Cold-storage servers run against a database that no longer contains data pruned
to save space: the `transaction.boc` blob and all messages except external
outbound ones (external outbound = `ExtOut` and `ExtOutV2` message types).

Enable cold-storage mode with the `--cold-storage` CLI flag, the
`GQL_COLD_STORAGE=true` environment variable, or the `cold_storage: true` option
in the YAML config file. When enabled:

- `Transaction.boc`, `Transaction.in_message` and `Message.dst_transaction` are
  hidden from introspection and return an error if queried directly.
- `blockchain.account.messages` rejects `msg_type` filters other than external
  outbound (`ExtOut` / `ExtOutV2`), since inbound and internal messages are not
  stored.

The mode is disabled by default and supports hot-reload via `SIGUSR1`.

```bash
gql-server --cold-storage
# or
GQL_COLD_STORAGE=true gql-server
```

## GraphQL endpoints

| Path | Method | Description |
|------|--------|-------------|
| `/graphql` | `POST` | GraphQL query endpoint |
| `/graphql` | `GET` | GraphiQL interactive playground |
| `/graphql_old` | `GET` | Legacy GraphQL Playground |

## Query timeout

Queries exceeding `sqlite_query_timeout_secs` are interrupted via SQLite's progress handler. The client receives:

```json
{
  "errors": [{
    "message": "Request timeout",
    "path": ["blockchain", "account", "messages"],
    "extensions": { "code": "TIMEOUT" }
  }]
}
```

## SQL projections

GraphQL resolvers build explicit SQLite `SELECT` lists from the requested
GraphQL fields. Queries include the selected response fields plus technical
columns needed for cursors, ordering, deduplication, nested relation loading,
and type conversion. This avoids reading every column for large archive rows
when the client only requests a small subset of fields.
