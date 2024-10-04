ALTER TABLE messages DROP COLUMN proof;
ALTER TABLE messages DROP COLUMN code;
ALTER TABLE messages DROP COLUMN code_hash;
ALTER TABLE messages DROP COLUMN data;
ALTER TABLE messages DROP COLUMN data_hash;
ALTER TABLE messages DROP COLUMN boc;
ALTER TABLE messages DROP COLUMN body;
ALTER TABLE messages DROP COLUMN value_other;
ALTER TABLE messages DROP COLUMN src_dapp_id;

ALTER TABLE messages ADD COLUMN boc TEXT;
ALTER TABLE messages ADD COLUMN body TEXT;
ALTER TABLE messages ADD COLUMN ihr_disabled INTEGER;
ALTER TABLE messages ADD COLUMN ihr_fee TEXT;
ALTER TABLE messages ADD COLUMN import_fee TEXT;

DROP INDEX index_accounts_dapp_id;
DROP INDEX index_blocks_chain_order;
DROP INDEX index_transactions_chain_order;
DROP INDEX index_transactions_complex_1;
DROP INDEX index_transactions_complex_2;
DROP INDEX index_transactions_in_msg;
