CREATE INDEX index_blocks_chain_order ON blocks (chain_order);
CREATE INDEX index_transactions_chain_order ON transactions (chain_order);

ALTER TABLE transactions DROP COLUMN storage_status_change;
ALTER TABLE transactions DROP COLUMN storage_fees_collected;
ALTER TABLE transactions ADD COLUMN storage_status_change INTEGER;
ALTER TABLE transactions ADD COLUMN storage_fees_collected TEXT;