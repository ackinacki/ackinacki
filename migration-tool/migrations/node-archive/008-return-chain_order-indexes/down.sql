DROP INDEX index_blocks_chain_order;
DROP INDEX index_transactions_chain_order;

ALTER TABLE transactions DROP COLUMN storage_status_change;
ALTER TABLE transactions DROP COLUMN storage_fees_collected;
ALTER TABLE transactions ADD COLUMN storage_status_change INTEGER NOT NULL;
ALTER TABLE transactions ADD COLUMN storage_fees_collected TEXT NOT NULL;
