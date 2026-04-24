DROP INDEX IF EXISTS index_transactions_in_msg;
DROP INDEX IF EXISTS index_transactions_addr_order;
DROP INDEX IF EXISTS index_messages_src_msg_order;

DROP INDEX IF EXISTS index_blocks_thread_height;
CREATE INDEX IF NOT EXISTS idx_blocks_thread_height ON blocks(height, thread_id);

CREATE INDEX IF NOT EXISTS index_messages_msg_id ON messages (id);
CREATE INDEX IF NOT EXISTS index_messages_src ON messages(src);
CREATE INDEX IF NOT EXISTS index_transactions_transaction_id ON transactions (id);
CREATE INDEX IF NOT EXISTS index_blocks_block_id ON blocks (id);
CREATE INDEX IF NOT EXISTS idx_attestations_block_id ON attestations (block_id);
