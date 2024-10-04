CREATE INDEX index_messages_msg_id ON messages (id);
CREATE INDEX index_messages_status ON messages (status);
CREATE INDEX index_messages_msg_type ON messages (msg_type);
CREATE INDEX index_messages_created_at ON messages (created_at);
CREATE INDEX index_messages_src_dapp_id ON messages(src_dapp_id);
CREATE INDEX index_messages_transaction_id ON messages (transaction_id);
CREATE INDEX index_messages_dst ON messages(dst);
CREATE INDEX index_messages_src ON messages(src);
CREATE INDEX index_messages_complex_1 ON messages(dst,dst_chain_order);
CREATE INDEX index_messages_complex_2 ON messages(src,dst_chain_order);
CREATE INDEX index_messages_complex_3 ON messages(dst,msg_type,dst_chain_order);
CREATE INDEX index_messages_complex_4 ON messages(src,msg_type,dst_chain_order);
CREATE INDEX index_messages_complex_5 ON messages(dst,dst_chain_order,src);
CREATE INDEX index_messages_complex_6 ON messages(src,dst_chain_order,dst);

CREATE INDEX index_accounts_acc_type ON accounts (acc_type);
CREATE INDEX index_accounts_account_id ON accounts (id);
CREATE INDEX index_accounts_code_hash ON accounts (code_hash);
CREATE INDEX index_accounts_dapp_id ON accounts(dapp_id);
CREATE INDEX index_accounts_init_code_hash ON accounts (init_code_hash);

CREATE INDEX index_transactions_transaction_id ON transactions (id);
CREATE INDEX index_transactions_block_id ON transactions (block_id);
CREATE INDEX index_transactions_status ON transactions (status);
CREATE INDEX index_transactions_aborted ON transactions (aborted);
CREATE INDEX index_transactions_tr_type ON transactions (tr_type);
CREATE INDEX index_transactions_chain_order ON transactions (chain_order);
CREATE INDEX index_transactions_complex_1 ON transactions (account_addr,end_status,orig_status);
CREATE INDEX index_transactions_complex_2 ON transactions (block_id,now);
CREATE INDEX index_transactions_in_msg ON transactions (in_msg);

CREATE INDEX index_blocks_parent ON blocks (parent);
CREATE INDEX index_blocks_chain_order ON blocks (chain_order);
