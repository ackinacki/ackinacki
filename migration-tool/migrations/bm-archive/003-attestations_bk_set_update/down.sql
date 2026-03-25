DROP INDEX IF EXISTS index_messages_src_dst_chain_order;

DROP INDEX IF EXISTS index_transactions_chain_order;

DROP INDEX IF EXISTS idx_attestations_source_chain_order;
DROP INDEX IF EXISTS idx_attestations_block_id;
DROP TABLE IF EXISTS attestations;

DROP INDEX IF EXISTS idx_bk_set_updates_thread_height;
DROP INDEX IF EXISTS idx_bk_set_updates_chain_order;
DROP TABLE IF EXISTS bk_set_updates;
