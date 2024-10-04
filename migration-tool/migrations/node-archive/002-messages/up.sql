CREATE TABLE messages (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    boc TEXT,
    body TEXT,
    body_hash TEXT,
    bounce INTEGER,
    bounced INTEGER,
    created_lt TEXT,
    created_at INTEGER,
    dst TEXT,
    dst_workchain_id INTEGER,
    fwd_fee TEXT,
    ihr_disabled INTEGER,
    ihr_fee TEXT,
    import_fee TEXT,
    msg_type INTEGER,
    src TEXT,
    src_workchain_id INTEGER,
    status INTEGER,
    transaction_id TEXT,
    value TEXT,
    dst_chain_order TEXT,
    src_chain_order TEXT
);

CREATE INDEX index_messages_msg_id ON messages (id);
CREATE INDEX index_messages_status ON messages (status);
CREATE INDEX index_messages_msg_type ON messages (msg_type);
CREATE INDEX index_messages_created_at ON messages (created_at);
CREATE INDEX index_messages_transaction_id ON messages (transaction_id);
CREATE INDEX index_messages_dst ON messages(dst);
CREATE INDEX index_messages_src ON messages(src);
CREATE INDEX index_messages_complex_1 ON messages(dst,dst_chain_order);
CREATE INDEX index_messages_complex_2 ON messages(src,dst_chain_order);
CREATE INDEX index_messages_complex_3 ON messages(dst,msg_type,dst_chain_order);
CREATE INDEX index_messages_complex_4 ON messages(src,msg_type,dst_chain_order);
CREATE INDEX index_messages_complex_5 ON messages(dst,dst_chain_order,src);
CREATE INDEX index_messages_complex_6 ON messages(src,dst_chain_order,dst);


CREATE TABLE accounts (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    workchain_id INTEGER NOT NULL,
    boc TEXT,
    init_code_hash TEXT,
    last_paid INTEGER,
    bits TEXT,
    cells TEXT,
    public_cells TEXT,
    last_trans_lt TEXT,
    last_trans_hash TEXT,
    balance TEXT,
    code TEXT,
    code_hash TEXT,
    data TEXT,
    data_hash TEXT,
    acc_type INTEGER NOT NULL,
    last_trans_chain_order TEXT
);
CREATE INDEX index_accounts_account_id ON accounts (id);
CREATE INDEX index_accounts_acc_type ON accounts (acc_type);
CREATE INDEX index_accounts_init_code_hash ON accounts (init_code_hash);
CREATE INDEX index_accounts_code_hash ON accounts (code_hash);

CREATE TABLE transactions (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    block_id TEXT NOT NULL,
    boc TEXT NOT NULL,
    status INTEGER NOT NULL,
    storage_fees_collected TEXT NOT NULL,
    storage_status_change INTEGER NOT NULL,
    credit TEXT,
    compute_success INTEGER,
    compute_msg_state_used INTEGER,
    compute_account_activated INTEGER,
    compute_gas_fees TEXT,
    compute_gas_used REAL,
    compute_gas_limit REAL,
    compute_mode INTEGER,
    compute_exit_code INTEGER,
    compute_skipped_reason INTEGER,
    compute_vm_steps INTEGER,
    compute_vm_init_state_hash TEXT,
    compute_vm_final_state_hash TEXT,
    compute_type INTEGER NOT NULL,
    action_success INTEGER,
    action_valid INTEGER,
    action_no_funds INTEGER,
    action_status_change INTEGER,
    action_result_code INTEGER,
    action_tot_actions INTEGER,
    action_spec_actions INTEGER,
    action_skipped_actions INTEGER,
    action_msgs_created INTEGER,
    action_list_hash TEXT,
    action_tot_msg_size_cells REAL,
    action_tot_msg_size_bits REAL,
    credit_first INTEGER NOT NULL,
    aborted INTEGER NOT NULL,
    destroyed INTEGER NOT NULL,
    tr_type INTEGER NOT NULL,
    lt TEXT NOT NULL,
    prev_trans_hash TEXT NOT NULL,
    prev_trans_lt TEXT NOT NULL,
    now INTEGER NOT NULL,
    outmsg_cnt INTEGER NOT NULL,
    orig_status INTEGER NOT NULL,
    end_status INTEGER NOT NULL,
    in_msg TEXT NOT NULL,
    out_msgs TEXT,
    account_addr TEXT NOT NULL,
    workchain_id INTEGER NOT NULL,
    total_fees TEXT,
    balance_delta TEXT NOT NULL,
    old_hash TEXT NOT NULL,
    new_hash TEXT NOT NULL,
    chain_order TEXT NOT NULL
);

CREATE INDEX index_transactions_transaction_id ON transactions (id);
CREATE INDEX index_transactions_block_id ON transactions (block_id);
CREATE INDEX index_transactions_status ON transactions (status);
CREATE INDEX index_transactions_aborted ON transactions (aborted);
CREATE INDEX index_transactions_tr_type ON transactions (tr_type);
ALTER TABLE blocks RENAME COLUMN id to rowid;
ALTER TABLE blocks RENAME COLUMN block_id to id;
ALTER TABLE blocks DROP COLUMN ext_messages;
ALTER TABLE blocks DROP COLUMN vert_seqno_incr;
ALTER TABLE blocks DROP COLUMN vert_seq_no;
ALTER TABLE blocks ADD COLUMN boc TEXT;
ALTER TABLE blocks ADD COLUMN file_hash TEXT;
ALTER TABLE blocks ADD COLUMN root_hash TEXT;
ALTER TABLE blocks ADD COLUMN prev_alt_ref_seq_no INTEGER;
ALTER TABLE blocks ADD COLUMN prev_alt_ref_end_lt TEXT;
ALTER TABLE blocks ADD COLUMN prev_alt_ref_file_hash TEXT;
ALTER TABLE blocks ADD COLUMN prev_alt_ref_root_hash TEXT;
ALTER TABLE blocks ADD COLUMN prev_ref_seq_no INTEGER;
ALTER TABLE blocks ADD COLUMN prev_ref_end_lt TEXT;
ALTER TABLE blocks ADD COLUMN prev_ref_file_hash TEXT;
ALTER TABLE blocks ADD COLUMN prev_ref_root_hash TEXT;
ALTER TABLE blocks ADD COLUMN in_msgs TEXT;
ALTER TABLE blocks ADD COLUMN out_msgs TEXT;
ALTER TABLE blocks DROP COLUMN shard;
ALTER TABLE blocks ADD COLUMN shard TEXT;
