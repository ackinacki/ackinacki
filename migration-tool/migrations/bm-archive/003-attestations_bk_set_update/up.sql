CREATE INDEX IF NOT EXISTS index_transactions_chain_order ON transactions (chain_order);

CREATE TABLE bk_set_updates (
    rowid INTEGER PRIMARY KEY,
    block_id TEXT NOT NULL UNIQUE,
    thread_id TEXT NOT NULL,
    height BLOB CHECK (length(height) = 8) NOT NULL,
    chain_order TEXT NOT NULL,
    bk_set_update BLOB NOT NULL
);

CREATE INDEX idx_bk_set_updates_chain_order ON bk_set_updates (chain_order);
CREATE INDEX idx_bk_set_updates_thread_height ON bk_set_updates (thread_id, height);

CREATE TABLE attestations (
    rowid INTEGER PRIMARY KEY,
    block_id TEXT NOT NULL,
    parent_block_id TEXT NOT NULL,
    envelope_hash BLOB CHECK (length(envelope_hash) = 32) NOT NULL,
    target_type INTEGER NOT NULL CHECK (target_type IN (0, 1)),
    aggregated_signature BLOB NOT NULL,
    signature_occurrences BLOB NOT NULL,
    source_block_id TEXT NOT NULL,
    source_chain_order TEXT NOT NULL,
    UNIQUE(block_id, target_type)
);

CREATE INDEX idx_attestations_block_id ON attestations (block_id);
CREATE INDEX idx_attestations_source_chain_order ON attestations (source_chain_order);

CREATE INDEX IF NOT EXISTS index_messages_src_dst_chain_order ON messages(src, dst, msg_chain_order);
