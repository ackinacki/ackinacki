ALTER TABLE messages DROP COLUMN boc;
ALTER TABLE messages DROP COLUMN body;
ALTER TABLE messages DROP COLUMN ihr_disabled;
ALTER TABLE messages DROP COLUMN ihr_fee;
ALTER TABLE messages DROP COLUMN import_fee;

ALTER TABLE messages ADD COLUMN proof BLOB;
ALTER TABLE messages ADD COLUMN code BLOB;
ALTER TABLE messages ADD COLUMN code_hash TEXT;
ALTER TABLE messages ADD COLUMN data BLOB;
ALTER TABLE messages ADD COLUMN data_hash TEXT;
ALTER TABLE messages ADD COLUMN boc BLOB;
ALTER TABLE messages ADD COLUMN body BLOB;
ALTER TABLE messages ADD COLUMN value_other BLOB;
ALTER TABLE messages ADD COLUMN src_dapp_id TEXT;
CREATE INDEX index_messages_src_dapp_id ON messages(src_dapp_id);

CREATE INDEX index_accounts_dapp_id ON accounts(dapp_id);
CREATE INDEX index_blocks_chain_order ON blocks (chain_order);
CREATE INDEX index_transactions_chain_order ON transactions (chain_order);
CREATE INDEX index_transactions_complex_1 ON transactions (account_addr,end_status,orig_status);
CREATE INDEX index_transactions_complex_2 ON transactions (block_id,now);
CREATE INDEX index_transactions_in_msg ON transactions (in_msg);
