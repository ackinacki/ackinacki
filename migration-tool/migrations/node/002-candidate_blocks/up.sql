CREATE TABLE block_markers (
    id INTEGER PRIMARY KEY,
    block_id TEXT NOT NULL UNIQUE,
    is_finalized INTEGER,
    is_main_candidate INTEGER,
    last_processed_ext_message INTEGER,
    is_verified INTEGER
);
CREATE INDEX index_block_markers_block_id ON block_markers (block_id);

DROP TABLE candidate_blocks;
CREATE TABLE candidate_blocks (
    id INTEGER PRIMARY KEY,
    block_id TEXT NOT NULL UNIQUE,
    seq_no INTEGER NOT NULL,
    parent TEXT NOT NULL,
    aggregated_signature BLOB,
    signature_occurrences BLOB,
    data BLOB
);
