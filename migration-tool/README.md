## SQLite migration tool for Acki-Nacki Node DB

```sh
$ migration-tool --help
Acki-Nacki DB (sqlite) migration tool

Usage: migration-tool [OPTIONS] -p <DB_PATH>

Options:
  -p <DB_PATH>
          The path to the DB file (bm-schema.db). Creates db file if it is missing. 
      --block-manager [<SCHEMA_VERSION>]
          Migrates the DB file to the specified DB schema version (default schema version: latest)
  -h, --help
          Print help
  -V, --version
          Print version
```
