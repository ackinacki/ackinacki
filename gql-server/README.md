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
| `SIGHUP` | Re-scan and attach archive database files and the history proofs store |
| `SIGUSR1` | Reload YAML config file |

### History proofs store

When `bm-history-proofs.db` exists next to the main DB, gql-server attaches it read-only (the tenth SQLite attach slot; `max_attached_db` covers archives only). A file created after startup is picked up on the next `SIGHUP`. The volume must stay writable: readers of a WAL file write its `-shm`. The file is checked on one new connection when the pool is created (at startup and on `SIGHUP`). When that check fails (the ATTACH fails, or the file lacks one of its tables: `blocks`, `events`, `bk_set_updates`, `attestations`), gql-server logs an error naming the missing tables and answers as without the file until the next `SIGHUP`. Pooled connections live up to 30 minutes and keep the file they attached: after the file is replaced or deleted, send `SIGHUP` (or restart gql-server), or it keeps serving the old rows. A connection whose own ATTACH fails later still returns a database error for the file's queries.

block-manager writes the file and never rotates it. When a block, a bridge `WithdrawalInitiated` event or a RootPN `VoucherGenerated` event is not in the main DB or the archives, `blockchain.blockByHeight` (thread 0), `blockchain.block(hash)` and `blockchain.message(hash)` read it from the file. Such objects serve a closed list of fields:

- block: `id`, `hash`, `block_id`, `thread_id`, `height`, `seq_no` (equals `height`), `gen_utime`, `envelope_hash`, `tracked_ext_out_messages_root`, `tracked_ext_out_message_hashes`, `history_proofs`, `block_merkle_tree_leaves`, `key_block` (always `null`), `attestations` with all its fields (`block_id`, `parent_block_id`, `envelope_hash`, `target_type`, `aggregated_signature`, `signature_occurrences`);
- message: `id`, `boc`, `src`, `dst`, `created_at`, `src_dapp_id`, `block_id`, `msg_type`, `src_transaction`;
- `src_transaction` of such a message: `id`, `block_id`;
- `__typename` on all three.

Selecting any other field of such an object returns an error with `extensions.code` `HISTORY_FIELD_UNAVAILABLE`, naming the fields; a stored block row that fails the format checks returns `HISTORY_DATA_CORRUPT`. An event found in the main DB whose transaction has already left the archives gets `block_id` and `src_transaction { id block_id }` from the file, under the same list.

`blockchain.voucherEvents(sk_u_commit)` returns the RootPN vouchers whose `skUCommit` is `sk_u_commit`, by height (normally one), from the file only and under the message list above. The argument is 1 to 64 hex digits, with or without `0x`, any letter case; anything else returns `VOUCHER_EVENTS_BAD_ARGUMENT`. A server without the file returns `HISTORY_STORE_UNAVAILABLE`, never an empty list. An empty list means only that this server's file has no such voucher: its height may be in a gap of the block-manager whose file it reads (`history-proofs gaps`), only thread 0 is stored, and block-manager does not store a message to the voucher `dst` whose body is not `VoucherGenerated` (`bm_history_events_skipped{kind="voucher"}`). block-manager writes a voucher with its block, so it is found right away, also while the archives still hold it.

The file also keeps the thread-0 BK set updates and the attestations of key blocks (`height % 1024 == 0`) and of BK set rotation blocks. `blockchain.bkSetUpdates`, `Block.attestations` (also on blocks from the main DB) and `BkSetUpdate.attestations` read them from the file together with the main DB and the archives: a row in several of them is returned once (`bkSetUpdates` by `block_id`, attestations by `block_id` and `target_type`, the copy with the greatest `source_chain_order`), with the same ordering and cursors as before. Once the archives have rotated, `BkSetUpdate.attestations` returns only the stored attestations whose latest copy came in that rotation block, not every attestation it carried; the bridge reads attestations through `Block.attestations` only.

### `blockchain.layerTree`

`layerTree(thread_id: String!, height: Int!, layer: Int!)` returns `thread_id`, `height`, `layer`, `root_hash` (`history_proofs[layer]` of the block at `height`) and `leaves`: the 130 leaves of that history proof tree. Leaf 0 is the L(layer+1) root at the last L(layer+1) boundary below `height` (zero when there is none), leaf 1 is the L(layer) root at `height - 128^layer` (zero when `height = 128^layer`), then 128 data leaves: `Poseidon(block_id ‖ envelope_hash ‖ tracked_ext_out_messages_root)` of blocks `height-128 … height-1` for layer 1, the L(layer-1) roots at `height - 127·128^(layer-1) … height` above. Rows come from the main DB, the archives and the history proofs store; the tree is rebuilt and checked against `root_hash` before it is returned. Errors carry `extensions.code`:

| Code | Meaning |
|---|---|
| `LAYER_TREE_BAD_ARGUMENT` | not thread 0, `height` is not a positive multiple of `128^layer`, or `layer` is not 1–10 |
| `LAYER_TREE_MISSING_DATA` | a needed row or layer root is not on this server; the message lists the heights. Retry later or ask another server |
| `LAYER_TREE_ROOT_MISMATCH` | the rebuilt tree does not give the stored root |
| `HISTORY_DATA_CORRUPT` | a stored block row fails the format checks |

## Metrics

Metrics are exported via OpenTelemetry (OTLP) when the `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable is set.

| Metric | Type | Description |
|--------|------|-------------|
| `gql_query_duration` | Histogram | GraphQL query execution time (ms) |
| `gql_sqlite_query_duration` | Histogram | SQLite query execution time (ms) |
| `gql_sqlite_pool_size` | Gauge | Total connections in the pool |
| `gql_sqlite_pool_idle` | Gauge | Idle connections in the pool |
| `gql_build_info` | Gauge | Build version and commit labels |
| `gql_oldest_thread0_height` | Gauge | Lowest thread-0 height served (main DB, archives, history proofs store), refreshed every 60 s |
| `gql_oldest_thread0_age_seconds` | Gauge | Age of that block by its `gen_utime` |

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
