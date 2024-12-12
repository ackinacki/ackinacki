## SQLite migration tool for Acki-Nacki Node DB

```sh
$ migration-tool --help
Acki-Nacki DB (sqlite) migration tool

Usage: migration-tool [OPTIONS] -p <DB_PATH>

Options:
  -p <DB_PATH>
          The path to the DB files (node.db and node-archive.db). Creates and inits to the latest version if they are missing
  -n, --node [<SCHEMA_VERSION>]
          [DEPRECATED] Migrate the node.db to the specified DB schema version (default: latest)
  -a, --archive [<ARC_SCHEMA_VERSION>]
          Migrate the node-archive.db to the specified DB schema version (default: latest)
      --block-manager [<SCHEMA_VERSION>]
          Migrate the bm-archive.db to the specified DB schema version (default: latest)
  -h, --help
          Print help
  -V, --version
          Print version
```
