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
