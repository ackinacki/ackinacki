DROP INDEX IF EXISTS index_messages_msg_id;
DROP INDEX IF EXISTS index_messages_src;
DROP INDEX IF EXISTS index_transactions_transaction_id;
DROP INDEX IF EXISTS index_blocks_block_id;
DROP INDEX IF EXISTS idx_attestations_block_id;

CREATE INDEX IF NOT EXISTS index_transactions_in_msg ON transactions(in_msg);
CREATE INDEX IF NOT EXISTS index_transactions_addr_order ON transactions(account_addr, chain_order DESC);
CREATE INDEX IF NOT EXISTS index_messages_src_msg_order ON messages(src, msg_chain_order DESC);

DROP INDEX IF EXISTS idx_blocks_thread_height;
CREATE INDEX IF NOT EXISTS index_blocks_thread_height ON blocks(thread_id, height);
