CREATE INDEX IF NOT EXISTS index_attestations_source_block_id ON attestations (source_block_id);
CREATE INDEX IF NOT EXISTS index_blocks_thread_chain_order ON blocks(thread_id, chain_order);
