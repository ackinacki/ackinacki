# Block Manager

Block Manager (BM) subscribes to block streams from one or more BK nodes, applies blocks to a local SQLite database, and exposes a REST API for account queries and external messages.

## Usage

```
block-manager [OPTIONS] [COMMAND]
```

Either `--config` or `--stream-src-url` must be provided.

### CLI arguments

| Argument | Env variable | Default | Description |
|---|---|---|---|
| `--config <PATH>` | `CONFIG_FILE` | — | Path to YAML config file (multi-node mode) |
| `--stream-src-url <URL>` | `STREAM_SRC_URL` | — | Single BK stream endpoint (legacy mode) |
| `--rest-api <ADDR>` | `REST_API` | `0.0.0.0:8001` | REST API listen address |
| `--sqlite-path <PATH>` | `SQLITE_PATH` | `./data` | SQLite database directory |
| `--readiness-max-block-age-secs <SECONDS>` | `READINESS_MAX_BLOCK_AGE_SECS` | `30` | Maximum finalized block age before readiness returns 503 (allowed range: 1–60) |
| `--bk-set-poll-interval-secs <SECONDS>` | `BK_SET_POLL_INTERVAL_SECS` | `60` | How often to poll the BK `/v2/bk_set` endpoint for the `latest_bk_set` snapshot (allowed range: 1–3600) |
| `--clickhouse-url <URL>` | `CLICKHOUSE_URL` | — | ClickHouse URL for transaction activity export |
| `--clickhouse-user <USER>` | `CLICKHOUSE_USER` | — | ClickHouse user (required when clickhouse-url is set) |
| `--clickhouse-password <PASS>` | `CLICKHOUSE_PASSWORD` | — | ClickHouse password (required when clickhouse-url is set) |
| `--bridge-event-routing <DAPP::ACCOUNT>` | `BRIDGE_EVENT_ROUTING` | `0000…0000::1a1a…1a` (64 hex digits each) | Bridge account whose `WithdrawalInitiated` events are kept in the history proofs store |
| `--bridge-event-dst <ADDR>` | `BRIDGE_EVENT_DST` | `:000…026a` (64 hex digits) | `dst` of the bridge `WithdrawalInitiated` events: a 256-bit external address, `:` and 64 hex digits, any letter case; BM does not start with another form, such as a shorter address (`:26a`) or an internal `0:…` address |
| `--voucher-event-routing <DAPP::ACCOUNT>` | `VOUCHER_EVENT_ROUTING` | `0000…0004::1010…10` (64 hex digits each) | RootPN account whose `VoucherGenerated` events are kept in the history proofs store, each with its `skUCommit` |
| `--voucher-event-dst <ADDR>` | `VOUCHER_EVENT_DST` | `:000…0087` (64 hex digits) | `dst` of the RootPN `VoucherGenerated` events, in the form of `--bridge-event-dst`; the two pairs must differ |

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

## BK set snapshot (`latest_bk_set`)

BM keeps the exact BK set of thread 0 in the archive table `latest_bk_set` (created by migration `011-latest_bk_set`); gql-server serves it to the bridge prover.

Every `--bk-set-poll-interval-secs` BM calls `GET /v2/bk_set` on a BK API endpoint (same endpoint list and `BK_API_TOKEN` as for account queries; 10 s timeout per endpoint). The BK node must serve the new `/v2/bk_set` format (the exact signer set with `thread_id`); old-format responses and responses without `thread_id` are rejected. An old-format body with BK entries does not parse and counts as `bk_set_poll`; one that parses but fails validation counts `bk_set_invalid` and moves the cursor (`bk_set_stalled`). A response body above 4 MiB is rejected (by `Content-Length` and while reading).

A set of 300 or more signers is rejected (`bk_set_invalid`, and the cursor moves on: `bk_set_stalled`). This is by design: the BK set commitment stored in each block has 300 slots, the size the bridge circuit is built for, and ignores signers past them, so such a set cannot be shown to be complete. Once thread 0 has 300 BKs, every poll counts `bk_set_invalid` and `bk_set_stalled` and the snapshot is no longer updated, until the set drops below 300 or the commitment gets more slots (a node and bridge change). gql-server keeps serving the last snapshot while its anchor block is in the served history, then returns `null`. Watch the BK count against this limit.

The poller keeps its own cursor over `bk_api_endpoints` and does not change the default endpoint used for account queries. An HTTP or parse error is logged as a WARN `bk_set from <endpoint> failed, trying next: <error>` and the poll continues with the next endpoint; it is not `bk_set_stalled`, and the next poll starts from the endpoint that answered. The cursor itself moves to the next endpoint (counted as `bk_set_stalled`) when the BK answers with a `seq_no` that is not newer than the last written one (or than the newest queued candidate) 3 polls in a row, right after any other validation rejection, and right after a candidate supplied by the current endpoint fails the commitment or anchor check, expires without its anchor block, or is pushed out of the full queue. Only these moves are counted as `bk_set_stalled`; each is logged as a WARN with the `from` and `to` endpoints and a `reason`: not newer (`got`, `last`), invalid response, candidate rejected (`seq_no`, reason), expired (`seq_no`) or evicted (`seq_no`). A candidate supplied by an endpoint the poller has already left does not move the cursor again. During a chain halt every BK answers "not newer", so each BM counts `bk_set_stalled` every 3 polls and rotates through the endpoints; with a single endpoint the "advance" stays on the same endpoint.

A received set is queued until the block with the same `seq_no` is stored in the BM archive. The queue holds `max(10, ceil(600 s / poll interval))` candidates (600 at a 1 s interval), so a BM archive lagging up to 10 minutes behind the BK still gets the snapshot written. A candidate whose anchor block does not appear within 10 minutes is dropped (`bk_set_evicted`). Anchor lookups per resolve tick (every 5 s) grow with the queue capacity: up to 600 indexed lookups at a 1 s interval when anchors never appear. A failed anchor lookup (for example without an open archive DB connection mid-rotation) is an error counted as `bk_set_anchor`: the resolve tick leaves the queue as it is, nothing expires and the cursor does not move. Candidate age does not count during the outage: the age clock of every queued candidate restarts on each failed tick, so after recovery a candidate still gets the full 10 minutes. New responses still push the oldest candidates out of a full queue (`bk_set_evicted`) during the outage, without moving the cursor. The table is replaced only when that anchor block is present **and** the set matches the Poseidon commitment stored in the block; otherwise the previous snapshot stays. An ambiguous anchor (several `blocks` rows for the same `seq_no`, or a malformed row) rejects the candidate at once. After each archive DB rotation BM rewrites the last snapshot into the new DB within a few seconds (gql-server returns `null` until then). On start BM reads the whole snapshot stored in `latest_bk_set`: it ignores responses that are not newer, so a restart behind a lagging BK does not roll the snapshot back; it rewrites the snapshot after a rotation even if no new set arrives; and it reports `bm_latest_bk_set_seqno` right away. If the table is empty the poller starts without a snapshot; if it cannot be read, BM counts `bk_set_restore`, logs the error and starts without one. Without a restored snapshot there is no lower bound: until the first new write a lagging BK can replace the stored snapshot with an older (still verified) one, the gauge `bm_latest_bk_set_seqno` stays unset, and a rotation has nothing to rewrite until then. A failure of this service never stops block ingestion.

Metric:

| Metric | Labels | Description |
|---|---|---|
| `bm_latest_bk_set_seqno` | `thread` | `seq_no` of the last snapshot the archive writer stored in `latest_bk_set` (failed writes do not move it). `thread` has the same format as in `bm_last_finalized_seqno` (`<T:hex>`) |

Counter `bm_errors` kinds used by this service:

| `kind` | Meaning |
|---|---|
| `bk_set_poll` | Could not fetch a valid response from any BK API endpoint (HTTP error, timeout, body over 4 MiB, unparsable JSON) |
| `bk_set_invalid` | Response failed validation (no `thread_id`, empty set, 300 or more signers, bad signer index or public key, `seq_no` above `i64::MAX`, ...) |
| `bk_set_stalled` | The poller moved to the next BK API endpoint because of repeated not newer responses, a rejection, or a rejected, expired or evicted candidate supplied by the current endpoint. It also grows during a chain halt (every BK answers "not newer") |
| `bk_set_evicted` | A candidate was dropped before its anchor block appeared in the archive (queue overflow or 10-minute age limit) |
| `bk_set_anchor` | The anchor block lookup failed, or the anchor block is ambiguous or malformed |
| `bk_set_commitment` | The anchor block has no commitment, the set does not match it, or the commitment could not be computed |
| `bk_set_restore` | The snapshot stored in `latest_bk_set` could not be read at start; the poller starts without it |
| `bk_set_store` | Queueing the snapshot failed, or the archive writer thread failed to write it (for example, the DB has no `latest_bk_set` table after a rotation onto a `bm-schema.db` without migration `011-latest_bk_set`) |

A response not newer than the last written snapshot (for example after failover to a lagging BK) is ignored and only logged at DEBUG; it is not an error. Recommended alert: `bm_latest_bk_set_seqno` not increasing for 5 minutes or more.

## History proofs store (`bm-history-proofs.db`)

BM keeps `bm-history-proofs.db` next to `bm-archive.db` in `--sqlite-path`. It is never rotated and is not part of the daily archives. It holds one row per thread-0 block with the fields the bridge and DEX provers need (`block_id`, `envelope_hash`, tracked ext-out messages root and hashes, history proof layer roots, the block Merkle leaves of layer boundaries and of BK set update blocks, `gen_utime`) and every bridge `WithdrawalInitiated` and RootPN `VoucherGenerated` event (selected by `--bridge-event-routing` and `--bridge-event-dst`, and by `--voucher-event-routing` and `--voucher-event-dst`) with its block; a voucher also with its `skUCommit`, read from the message body. gql-server finds a voucher by it (`blockchain.voucherEvents`). It also keeps the BK set updates and attestations the bridge's bundle prover needs (see below). gql-server serves blocks, events, BK set updates and attestations from it after they have left the archives, and builds `blockchain.layerTree` from it. It grows by about 45 MB a day (about 16 GB a year).

The file starts empty and fills from live blocks only. Rows are write-once: a different block at an occupied height is a conflict, logged and counted, and the stored row is kept. A message comes in one block only (its hash covers the account's `created_lt`, which only grows): an event whose message the file holds at another height is not stored again, the block is written without it, and it is logged and counted in `bm_history_event_conflicts`. That happens on damaged or foreign data only, and another source does not cure it: restore the store. Only the log says which event it was: the counter starts from zero when BM restarts, and nothing in the file marks the row. A failed write to the file stops BM, the same as a failed block write. BM refuses to start when the file has a `user_version` other than 1 (2 and 3 are files of unreleased builds): move it away and start BM. A version-1 file of an early unreleased build (layer roots only) fails on its first write: move it away too.

### BK set updates and attestations

The bridge accepts a withdrawal proof only for a root kept by a bundle, and a bundle only when its key block is signed by the BK set the bridge knows. Its prover reads BK set rotations in order and the attestations of key blocks; a rotation or a key block attestation missing from GraphQL stops the bridge for good. The archives lose them with rotation, so BM also keeps, for thread 0:

- the `bk_set_updates` row of every block that changes the BK set (a rotation block), written in the same transaction as the block's row. A rotation block keeps its block Merkle leaves. A block whose BK set update does not belong to it (another block id, height or thread), or a row with leaves off a layer boundary without its BK set update, is not written at all (`bm_history_rejected`); another BK set update at the same height is a conflict (`bm_history_conflicts`);
- the attestations (PRIMARY and FALLBACK) of key blocks (`height % 1024 == 0`) and of rotation blocks: the ones the prover reads with `blockByHeight(h).attestations`. Other attestations, including the ones a rotation block carries for ordinary blocks, stay in the archives only.

An attestation is written only when it matches the stored row of the block it signs: the same `envelope_hash`, and a `parent_block_id` equal to the block id of the row below, when that row is stored. An attestation of a block that is not in the store (a gap) is not kept; `import` brings it with the block's row. When the row below a stored attestation arrives later and is not its parent, the attestation is deleted. The same attestation can come in several blocks with different signature sets: the copy from the latest carrier block (the greatest `source_chain_order`) is kept, whatever order the copies arrive in. Attestations are the only rows that are replaced or deleted.

A key or rotation block older than the last 256 stored heights without a PRIMARY attestation is a *missing attestation*: `gaps` lists it, `bm_history_missing_attestations` counts it, and the procedure below closes it like a gap. Missing attestations do not keep a height from being verified.

### Gaps

BM receives only the blocks its node sends after BM connects; it cannot ask for older ones. Every restart of BM or of its node, a network break, or a lagging connection (the node drops a subscriber that falls 10 blocks behind) leaves missing heights. A new BM starts with an empty file. Gaps are closed by copying height ranges from another BM that has them. Both keep running: the commands read and write in short transactions, next to the live writer.

| Command | Exit code 1 (incomplete) means | Exit code 3 (bad data) means |
|---|---|---|
| `block-manager history-proofs gaps [--json]` | — (prints `first_height`, `last_height`, the missing heights between them, `missing_attestations`, `failed_checks`: the heights whose rows failed a check other than `event_in_tree`, `unverified`: the stored heights not verified yet (rows waiting for their checks, or with a failed check other than `event_in_tree`), and `unprovable_events`: the heights with an event that cannot be proven, an alert only, as `heights` (their number), `ranges` (the newest 100 ranges) and `total_ranges` (the number of all ranges); a height with both is in both lists, and `failed_checks` decides) | — |
| `block-manager history-proofs export --from-height A --to-height B --out <file> [--json]` | the source misses some height of `A..=B` (printed as `source_gaps`) or some attestation of it (`source_missing_attestations`) | rows of the range that failed a check other than `event_in_tree` on the source (`failed_checks`), or that the file refused (`rejected`; the first 20 are printed to stderr): they are left out of the file, which is still written |
| `block-manager history-proofs import --from <file> [--all] [--json]` | heights of the file's range still missing (`remaining_gaps`) or without their attestation (`remaining_missing_attestations`) | rejected rows, conflicts, inserted rows that failed a check other than `event_in_tree` (`row_check_failures`), rows of the range that failed such a check earlier (`failed_checks`), rejected attestations (also one of another block than the target has at its height) and deleted ones; the first 20 of each kind are printed to stderr after their batch, a conflict with the fields that differ |

All three read `<--sqlite-path>/bm-history-proofs.db` and accept the global flags after the subcommand. Exit code 3 wins over 1: a source with bad data must not look like one that only lacks heights. Exit code 0 does not prove the rows are of this BM's chain: rows of another chain that agree with each other pass every check `import` runs, and only the verifier's layer checks at the gap edges catch them (see "Import only from BMs you run" below). Exit code 2 means any other error: wrong arguments, a missing file or an existing `--out` file, a foreign schema, an SQLite or IO error (such as a locked file); the `--help` of each subcommand lists the exit codes. `export` refuses an existing `--out` file. `import` takes the range from the export metadata and by default inserts only the heights the target is missing; `--all` compares every row of the range (conflicts are reported with their heights). A file without export metadata (a `VACUUM INTO` backup) needs `--from-height` and `--to-height`, and its event flags are not checked: import a backup only into a BM with the `--bridge-event-*` and `--voucher-event-*` flags the backup's BM had. `export` copies the BK set updates of the range, the attestations of its blocks and of the 256 blocks below it (they arrive in the blocks of the range, so a gap after a restart holds the attestation of a key block stored just before it); `--json` adds their counts (`bk_set_updates`, `attestations`) and the exported events by kind (`events_by_kind`: `withdrawal`, `voucher`); the export file records the four event flags. `import` writes BK set updates only with their rows, and attestations by the rules above at every height of the range, also outside the gaps: an import adds the missing attestations of rows the target already has and replaces a stored copy with a later one. Its `--json` adds `bk_set_updates` (`inserted`, `present`, `rejected`, `conflicts`) and `attestations` (`inserted`, `replaced`, `kept`, `not_kept`, `rejected`, `detached`); every rejected or deleted attestation is also printed as a problem. Import checks every row like a live write, and checks that each event of the file belongs to its row (the same block id and height), has the kind the flags give its `src` and `dst`, and is the message its BOC holds (message hash, `src`, `dst`, `created_at`, `src_dapp_id`, message type, and a voucher's `skUCommit`); a row with any other event is rejected whole and its height stays a gap. Each BM checks its own rows; no check results travel with an export, and `export` leaves out the rows its own checks failed (`failed_checks`, exit code 3). Only the ones that already failed: a row the source has not checked yet (a fresh row, or a window that waits for its neighbours) is exported, and the target checks it (`import` runs the row checks, its verifier the layer checks). A failed `event_in_tree` is an alert only (`bm_history_check_failures{check="event_in_tree"}` and its height in `unprovable_events` of `gaps` and `import`; a warning in the BM log for the rows BM wrote itself, a line `import` prints to stderr for imported rows, which the verifier does not check again): the event cannot be proven, but this also happens on correct data on every BM, for example after the bridge or RootPN routing left `tracked_ext_out_account_routings`. Each event keeps the routing it was stored with (the account of its `src` and the dApp of its kind's routing flag at that time) and is checked in that group, so a later change of the flag does not fail older rows. It does not make `import` or `export` exit 3, is not in `failed_checks`, and the row is still verified. Every other failed check (`events_root`, `merkle_leaves`, `event_boc`: a stored event that is not the message its BOC holds, `l1`, `layer_*`) is bad data. A forged event from a source also gives only the alert: import only from BMs you run. The events are kept as the source stored them: rows from before a change of an event flag keep their old events. An export file records the source's two flags, and `import` refuses (exit code 2) a file exported with other ones: such a source would bring other events, or none. `export` writes its metadata in one transaction at the end, and `import` refuses (exit code 2) a file whose metadata lacks a key, also with `--from-height` and `--to-height`: that export did not finish. A message comes in one block only: an event of the file that the target stores at another height makes its row a conflict. The background verifier checks the imported rows against their neighbours later. Importing the same file again is safe after exit code 0 or 1. After exit code 3, look at what it reports. Rejected rows and conflicts wrote nothing of those rows: their heights stay gaps, import them from another source (but not after an event conflict, see below). Rows that failed their checks (`row_check_failures`, `failed_checks`) are in the store: every later `import` of their range exits 3 and `gaps` lists them until you restore the store (below). A deleted attestation exits 3 once: the attestation is gone, so a second run exits 0 or 1, and the block shows in `missing_attestations` until an import brings an attestation that agrees with the row; the verifier checks the row itself.

In Docker: `docker compose exec -T block_manager block-manager history-proofs gaps --sqlite-path /workdir/data`; files under `/workdir/data` are in `{{ BM_DATA_DIR }}/workdir` on the host.

Procedure (on alerts, after a node release, for a new BM):

1. On the target: `gaps`, and note its first stored height (`bm_history_first_height`).
2. Pick a source: any BM, at any site, without gaps in those heights. On it, `export` a range covering the target's gaps; if the target starts later than the source, also export from the source's first height to the target's first height minus one (`gaps` does not show that part). For each height of the target's `missing_attestations`, export `--from-height h --to-height h` from a source that has it: `h` is not below the source's `first_height`, is older than its last 256 heights (`last_height - 256`), and its own `gaps` lists `h` neither in a gap nor in `missing_attestations`. If `export` prints `source_gaps` or `source_missing_attestations` in the heights you need, take another BM.
3. Copy the file to the target's data directory and `import` it. If `remaining_gaps` or `remaining_missing_attestations` is not empty, repeat 2–3 with another source; an import that closes a gap can show new missing attestations of the key blocks in it. Delete the export files.

If `export` exits 3, the source holds rows its own checks refuse (`failed_checks`) or the file refused (`rejected`); they are not in the file. Take another source, and restore the source's store. If `import` exits 3 with rejected rows or conflicts only, take another source for those heights. A conflict of an event (`event … is stored at height …`) is the exception: the same message at two heights means that the target or the source holds damaged or foreign data, so compare both heights with a third BM and restore the store that differs. If it reports `row_check_failures` or `failed_checks`, or `gaps` shows `failed_checks` on the target, restore the target's store as described below before you copy anything into it; then close its gaps from another source.

A missing attestation that no BM has (the same heights in `missing_attestations` on every BM) was lost network-wide, for example when all BMs restarted before its carrier blocks arrived. Escalate it: the cold archive may still have it.

A new BM: start it with an empty file, wait for its first stored height, then follow 1–3 (all its history is "below the target's start"). If the target restarts during the procedure, `gaps` shows the new gap; repeat.

Import only from BMs you run, and watch the checks after an import. Every block's layer roots commit to the whole history below it, so rows that do not match this BM's chain fail a check at the upper edge of the closed gap (`l1` or `layer_*`). An event that is not the message its BOC holds is rejected with its row, but an event that agrees with its BOC and is not in its routing's tracked group gives only the `event_in_tree` alert: `import` exits 0 and gql-server serves the event. Rows deeper in the gap can still be marked verified when they only agree with each other, so treat a failure at a gap edge as the whole import being bad. gql-server serves imported rows as soon as they are written, and keeps serving them after a failed check; `import` cannot replace them. Take the BM out of the public address, then restore the file from a backup taken before the import, or stop BM, delete the file with its `-wal` and `-shm`, start BM and close the gaps again from another source. Then send `SIGHUP` to gql-server (or restart it): its pooled connections keep the old file open for up to 30 minutes and serve its rows meanwhile.

The Ansible playbooks `upgrade-bm.yaml` and `block-manager-upgrading.yaml` run this after each BM start (`CLOSE_HISTORY_GAPS`), one BM at a time, with the other BMs of the inventory as sources (`bm_history_sources`), in chunks of `HISTORY_EXPORT_CHUNK` heights copied through the Ansible controller with `rsync`. Each source also copies the target's missing attestations it has, one height per export.

Sources that are unreachable over SSH, have no store, run a block-manager without `history-proofs`, or have rows that failed their checks (`failed_checks` in their `gaps`: restore that store) are not used. The play prints every source it did not use and why (unreachable over SSH, no store, no `history-proofs`, an empty store, rows that failed their checks, or `gaps` failed with its exit code and error). An unreachable source is not counted in the verdict, so check that list before you treat a height as lost. A source not used for its failed checks still counts in the verdict by its first height and the attestations it has: it stops the play only when this BM needs early history or a missing attestation that no usable source has but that source has. The playbooks run one BM at a time and stop at the first failure, so that also stops the rollout of the BMs after it; otherwise the rollout goes on without that source. A source whose `gaps` fails for another reason (a locked or unreadable file, a stopped container) may hold what the BM lacks: the play copies from the other sources and then stops, naming it and the list to pass instead, as in `--limit <target> -e '{"bm_history_sources": ["bm-2", "bm-3"]}'`. Pass the list as JSON (`-e bm_history_sources=bm-2,bm-3` gives a string, and the play fails on it), and with `--limit`: an `-e` value applies to every host of the play. A source left out with `-e` is not read at all, so it does not count in the verdict either: the play can succeed without history that only that source had. Check its `gaps` yourself.

An `export` or `import` that exits 3 (bad data) stops the play at once. After an import, the rows that failed their checks stay in this BM's store until it is restored, and every later import of their range exits 3 again. After an export, the source holds bad rows: when it failed a row during the copy (`failed_checks`), the next run does not use it; when the export file refused its rows (`rejected`), the next run uses it again, so leave it out with `-e` until its store is restored. The play also stops on an `export` or `import` that prints no JSON line (`docker compose exec` exits 1 when the container restarts or the daemon does not answer), and on a BM whose `gaps` shows `failed_checks`, before copying and after: a second run stops on the same rows until the store is restored (`-e CLOSE_HISTORY_GAPS=false` rolls out without this step). Change the event flags (`--bridge-event-*`, `--voucher-event-*`) on all BMs at once and close the gaps after: an `import` between BMs with different flags exits 2. A failed `export` or `import` task shows the command's stderr: the rows it refused, the conflicts, the checks that failed and the attestations it rejected or deleted.

The play succeeds when the BM has no gaps, no missing attestation that a reachable source with a store has (also one not used for its failed checks), and no such source starts earlier; otherwise it stops with what remains, and you follow the procedure by hand. Missing attestations that no source read has are printed in the report, not failed on; a source that was not read may still have one: an unreachable one or one whose `gaps` failed (both among the sources not used), or one left out with `-e bm_history_sources` (not listed). A source not used for its failed checks is read (see above). The play does not catch rows that fail their checks after the BM's own run: the verifier checks the layers at the edges of a closed gap seconds to minutes after the import, and the play reads the store right after it. Such a BM passes, and the later BMs of the rollout do not use it as a source; they stop only when they need what no usable source has (above). The signal is the alert `bm_history_check_failures{check!="event_in_tree"} > 0` (see "Checks and metrics"): it stays on until the store is restored, and the next run for that BM stops before copying.

Backup: `sqlite3 bm-history-proofs.db "VACUUM INTO '<backup>'"` or a full `export`. Do not copy the file as is: recent rows are in its `-wal`. Do not edit it with a long `INSERT ... SELECT`: a write transaction that holds the lock for more than 30 seconds stops BM.

### Deployment rules

- At every moment at least one site keeps serving blocks to its BMs: nodes and BMs of different sites are not restarted in the same run. Otherwise every BM gets the same gap and there is nowhere to copy it from.
- Seed the bridge only after the first L3 boundary (a thread-0 height divisible by 2,097,152, about every 8.2 days) that is at or above the first stored height of every BM behind the public address, and once `gaps` is empty on all of them. At every L2 boundary the bundle prover reads the row of the last L3 boundary, and at every L3 boundary the 128 L2 boundary rows before it. An L3 boundary below the start of the file is gone once the archives rotate, and the bridge stops at its first L2 boundary. On mainnet the L3 boundaries are 98,566,144, 100,663,296 (about 2026-11-09) and every 2,097,152 blocks after that.

### Checks and metrics

A background thread checks the rows within seconds of their arrival: the tracked ext-out messages root, the block Merkle leaves, that each event is in its routing's tracked group and is the message its BOC holds (for a voucher, with its `skUCommit`), the L1 tree of each 128-block window (at the next boundary) and each higher layer root, rebuilt from the stored rows with the node's code. A height counts as verified when all checks of its row ran and none failed other than `event_in_tree` (an alert only: the row is verified and the alert stays in `bm_history_check_failures`). The first windows of the file and the windows next to a gap wait for the rows they need. Attestations of live blocks are compared with the stored `envelope_hash` to name the block behind an L1 mismatch.

| Metric | Type | Description |
|---|---|---|
| `bm_history_first_height`, `bm_history_last_height` | gauge | First and last stored height |
| `bm_history_missing_blocks{scope="dex_window"}` | gauge | Missing heights in the last 2 113 536 blocks (the DEX window), not counting heights below the first stored one |
| `bm_history_missing_blocks{scope="total"}` | gauge | Missing heights between the first and the last stored height |
| `bm_history_check_failures{check}` | gauge | Rows with a failed check: `events_root`, `merkle_leaves`, `event_in_tree` (an alert only), `event_boc`, `l1`, `layer_2` … `layer_10`; and rows of the BK set tables that do not match their block row (only a manual edit makes them; checked every 60 verifier passes, about 5 minutes): `bk_set_update`, `attestation` |
| `bm_history_missing_attestations` | gauge | Key and rotation blocks older than the last 256 heights without a PRIMARY attestation |
| `bm_history_verifier_failures` | counter | Verifier passes that failed (the error is logged). The first and last height, missing blocks and missing attestations gauges are read on their own and still update unless that read fails too; the check gauges and `bm_history_unverified` keep their last values |
| `bm_history_unverified{state}` | gauge | Rows not verified yet: `checkable` (waiting for the verifier) and `not_checkable` (rows they need are missing) |
| `bm_history_rows_written`, `bm_history_events{kind}` | counter | Rows written; events written, `kind` `withdrawal` or `voucher` |
| `bm_history_conflicts` | counter | A different block at an occupied height or with a stored block id, or another BK set update at the block's height or block id; nothing of the block is written |
| `bm_history_event_conflicts{kind}` | counter | Events of a written block whose message the file holds at another height; the block is written without them (damaged or foreign data) |
| `bm_history_rejected` | counter | Blocks not written because their proof fields do not parse (for example a wrong layer set for the height), their BK set update does not belong to them, or they have block Merkle leaves off a layer boundary without a BK set update |
| `bm_history_seq_no_mismatch` | counter | Thread-0 blocks whose `seq_no` is not their height (they are written) |
| `bm_history_events_skipped{kind}` | counter | Events without a transaction id or BOC in a thread-0 block, and vouchers whose body is not `VoucherGenerated`, also in a block not written |
| `bm_history_attestation_mismatch`, `bm_history_attestation_missing` | counter | An attestation with another `envelope_hash`; no attestation within 256 blocks |
| `bm_history_bk_set_updates` | counter | BK set updates written |
| `bm_history_attestations{result}` | counter | Attestations carried in stored blocks: `written` (kept: of key and rotation blocks), `replaced` (a later copy of a kept one), `rejected` (any carried attestation with a wrong parent or block id, also of a plain block, or one deleted when the row below the block it signs arrived and is not its parent; an `envelope_hash` mismatch counts in `bm_history_attestation_mismatch` only) |

After a BM restart the node can send again blocks that BM received before the restart (while it runs, BM drops a copy of any of the last 10 000 blocks it received). Such a block counts again in `bm_history_conflicts`, `bm_history_event_conflicts`, `bm_history_rejected`, `bm_history_seq_no_mismatch`, `bm_history_events_skipped`, `bm_history_attestation_mismatch` and `bm_history_attestations{result="rejected"}`: growth right after a restart at heights the log named before it is the same problem, not a new one.

Suggested alerts: `bm_history_missing_blocks{scope="dex_window"} > 0` (run the procedure); `bm_history_missing_attestations > 0` (run the procedure; the same heights on every BM mean a network-wide loss: escalate); `bm_history_missing_blocks{scope="total"} > 0` (low priority); `bm_history_attestations{result="rejected"}` growing; `bm_history_verifier_failures` growing (the checks stopped, see the log); `bm_history_first_height` differs between the BMs behind one public address (a BM started later and did not take the start of the file); `bm_history_check_failures{check!="event_in_tree"} > 0` (rows that failed their checks: restore the store; it stays above zero until then, also on a BM the playbook passed); any of `bm_history_rejected`, `bm_history_seq_no_mismatch`, `bm_history_attestation_mismatch` growing; `bm_history_conflicts` growing (the node sent a block other than the stored one at its height: the file holds rows of another chain, usually from an import; compare the logged heights with another BM, `import --all` of that range names the fields that differ, and restore the store that differs); `bm_history_event_conflicts` growing (damaged or foreign data: restore the store); `bm_history_events_skipped{kind="voucher"}` growing, as a page (RootPN messages to the voucher `dst` whose body is not `VoucherGenerated` are not stored, so `voucherEvents` does not find them; the usual cause is a changed RootPN event, which needs a block-manager release with its new event id; their BOCs stay in the archives for about 29 hours, so act within that time); and, from gql-server, `gql_oldest_thread0_height` closer than 2 113 536 blocks to the head (expected during the first 8 days). Keep `bm_history_check_failures{check="event_in_tree"}` growing as a warning, not a page: it grows on every BM with correct data once the bridge or RootPN routing leaves `tracked_ext_out_account_routings`; on one BM only, after an import, it can be a forged event.

## REST API

| Endpoint | Method | Description |
|---|---|---|
| `/v2/account?address=<addr>` | GET | Query account state from BK |
| `/v2/readiness` | GET | Health check |
| `/v2/messages` | POST | Submit external message |
