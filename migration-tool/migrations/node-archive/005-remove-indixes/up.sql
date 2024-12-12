DROP INDEX index_messages_msg_id;
DROP INDEX index_messages_status;
DROP INDEX index_messages_msg_type;
DROP INDEX index_messages_created_at;
DROP INDEX index_messages_src_dapp_id;
DROP INDEX index_messages_transaction_id;
DROP INDEX index_messages_dst;
DROP INDEX index_messages_src;
DROP INDEX index_messages_complex_1;
DROP INDEX index_messages_complex_2;
DROP INDEX index_messages_complex_3;
DROP INDEX index_messages_complex_4;
DROP INDEX index_messages_complex_5;
DROP INDEX index_messages_complex_6;

DROP INDEX index_accounts_acc_type;
DROP INDEX index_accounts_account_id;
DROP INDEX index_accounts_code_hash;
DROP INDEX index_accounts_dapp_id;
DROP INDEX index_accounts_init_code_hash;

DROP INDEX index_transactions_transaction_id;
DROP INDEX index_transactions_block_id;
DROP INDEX index_transactions_status;
DROP INDEX index_transactions_aborted;
DROP INDEX index_transactions_tr_type;
DROP INDEX index_transactions_chain_order;
DROP INDEX index_transactions_complex_1;
DROP INDEX index_transactions_complex_2;
DROP INDEX index_transactions_in_msg;

DROP INDEX index_blocks_parent;
DROP INDEX index_blocks_chain_order;

CREATE INDEX index_blocks_prev ON blocks (prev_ref_root_hash);
CREATE INDEX index_blocks_prev_alt ON blocks (prev_alt_ref_root_hash);
