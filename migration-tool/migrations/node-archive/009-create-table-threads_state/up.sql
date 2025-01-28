-- TODO remove after BM will be completed
CREATE TABLE threads (
    block_id TEXT NOT NULL UNIQUE,
    timestamp INTEGER NOT NULL,
    state BLOB NOT NULL
);
CREATE INDEX index_threads_timestamp ON threads (timestamp);

ALTER TABLE blocks ADD COLUMN thread_id TEXT;
ALTER TABLE blocks ADD COLUMN producer_id TEXT;
