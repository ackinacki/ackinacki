CREATE TABLE block_status (id INTEGER PRIMARY KEY, oid TEXT NOT NULL UNIQUE, data BLOB);
CREATE TABLE optimistic_state (id INTEGER PRIMARY KEY, oid TEXT NOT NULL UNIQUE, data BLOB);
CREATE TABLE candidate_blocks (id INTEGER PRIMARY KEY, oid TEXT NOT NULL UNIQUE, data BLOB);
CREATE TABLE metadata (id INTEGER PRIMARY KEY, oid TEXT NOT NULL UNIQUE, data BLOB);
CREATE TABLE ext_messages (id INTEGER PRIMARY KEY, oid TEXT NOT NULL UNIQUE, data BLOB);
CREATE TABLE accounts (id INTEGER PRIMARY KEY, block_id TEXT NOT NULL, address TEXT NOT NULL, data BLOB);
CREATE INDEX index_acc_block_id ON accounts (block_id);
CREATE INDEX index_acc_address ON accounts (address);
CREATE INDEX index_acc_block_addr ON accounts (block_id,address);
