## SQLite migration tool for Acki-Nacki Node DB

```sh
$ migration-tool --help
Acki-Nacki DB (sqlite) migration tool

Usage: migration-tool [OPTIONS] -p <DB_PATH>

Options:
  -p <DB_PATH>
          The path to the DB file (bm-archive.db). Creates and inits to the latest version if they are missing
      --block-manager [<SCHEMA_VERSION>]
          Migrate the bm-archive.db to the specified DB schema version (default: latest)
  -h, --help
          Print help
  -V, --version
          Print version
```
