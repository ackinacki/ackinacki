CREATE TABLE latest_bk_set (
    signer_index INTEGER PRIMARY KEY,
    pubkey       BLOB NOT NULL CHECK (length(pubkey) = 48),
    thread_id    TEXT NOT NULL,
    seq_no       INTEGER NOT NULL,
    block_id     TEXT NOT NULL,
    height       BLOB NOT NULL CHECK (length(height) = 8),
    chain_order  TEXT NOT NULL,
    updated_at   INTEGER NOT NULL  -- unix time of the write, seconds
);
