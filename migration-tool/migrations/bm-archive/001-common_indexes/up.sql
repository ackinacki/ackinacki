CREATE TABLE messages (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    body_hash TEXT,
    bounce INTEGER,
    bounced INTEGER,
    created_lt TEXT,
    created_at INTEGER,
    dst TEXT,
    dst_workchain_id INTEGER,
    fwd_fee TEXT,
    msg_type INTEGER,
    src TEXT,
    src_workchain_id INTEGER,
    status INTEGER,
    transaction_id TEXT,
    value TEXT,
    dst_chain_order TEXT,
    src_chain_order TEXT,
    proof BLOB,
    code BLOB,
    code_hash TEXT,
    data BLOB,
    data_hash TEXT,
    boc BLOB,
    body BLOB,
    value_other BLOB,
    src_dapp_id TEXT
);

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

CREATE TABLE accounts (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    workchain_id INTEGER NOT NULL,
    init_code_hash TEXT,
    last_paid INTEGER,
    bits TEXT,
    cells TEXT,
    public_cells TEXT,
    last_trans_lt TEXT,
    last_trans_hash TEXT,
    balance TEXT,
    code_hash TEXT,
    data_hash TEXT,
    acc_type INTEGER NOT NULL,
    last_trans_chain_order TEXT,
    dapp_id TEXT,
    balance_other BLOB,
    boc BLOB,
    code BLOB,
    data BLOB,
    due_payment TEXT,
    proof BLOB,
    prev_code_hash TEXT,
    state_hash TEXT,
    split_depth INTEGER
);

CREATE INDEX index_accounts_acc_type ON accounts (acc_type);
CREATE INDEX index_accounts_account_id ON accounts (id);
CREATE INDEX index_accounts_code_hash ON accounts (code_hash);
CREATE INDEX index_accounts_dapp_id ON accounts(dapp_id);
CREATE INDEX index_accounts_init_code_hash ON accounts (init_code_hash);

CREATE TABLE transactions (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    block_id TEXT NOT NULL,
    status INTEGER NOT NULL,
    storage_fees_collected TEXT,
    storage_status_change INTEGER,
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
    chain_order TEXT NOT NULL,
    proof BLOB,
    boc BLOB
);

CREATE INDEX index_transactions_transaction_id ON transactions (id);
CREATE INDEX index_transactions_block_id ON transactions (block_id);
CREATE INDEX index_transactions_status ON transactions (status);
CREATE INDEX index_transactions_aborted ON transactions (aborted);
CREATE INDEX index_transactions_tr_type ON transactions (tr_type);
CREATE INDEX index_transactions_chain_order ON transactions (chain_order);
CREATE INDEX index_transactions_complex_1 ON transactions (account_addr,end_status,orig_status);
CREATE INDEX index_transactions_complex_2 ON transactions (block_id,now);
CREATE INDEX index_transactions_in_msg ON transactions (in_msg);

CREATE TABLE blocks (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    status INTEGER NOT NULL,
    seq_no INTEGER NOT NULL,
    parent TEXT NOT NULL,
    thread_id TEXT,
    producer_id TEXT,
    aggregated_signature BLOB,
    signature_occurrences BLOB,
    share_state_resource_address TEXT,
    global_id INTEGER,
    version INTEGER,
    after_merge INTEGER,
    before_split INTEGER,
    after_split INTEGER,
    want_split INTEGER,
    want_merge INTEGER,
    key_block INTEGER,
    flags INTEGER,
    workchain_id INTEGER,
    gen_utime INTEGER,
    gen_utime_ms_part INTEGER,
    start_lt TEXT,
    end_lt TEXT,
    gen_validator_list_hash_short INTEGER,
    gen_catchain_seqno INTEGER,
    min_ref_mc_seqno INTEGER,
    prev_key_block_seqno INTEGER,
    gen_software_version INTEGER,
    gen_software_capabilities TEXT,
    data BLOB,
    file_hash TEXT,
    root_hash TEXT,
    prev_alt_ref_seq_no INTEGER,
    prev_alt_ref_end_lt TEXT,
    prev_alt_ref_file_hash TEXT,
    prev_alt_ref_root_hash TEXT,
    prev_ref_seq_no INTEGER,
    prev_ref_end_lt TEXT,
    prev_ref_file_hash TEXT,
    prev_ref_root_hash TEXT,
    in_msgs TEXT,
    out_msgs TEXT,
    shard TEXT,
    chain_order TEXT,
    tr_count INTEGER,
    boc BLOB
);

CREATE INDEX index_blocks_block_id ON blocks (id);
CREATE INDEX index_blocks_seq_no ON blocks (seq_no);
CREATE INDEX index_blocks_parent ON blocks (parent);
CREATE INDEX index_blocks_chain_order ON blocks (chain_order);
CREATE INDEX index_blocks_thread_id ON blocks (thread_id);

CREATE TABLE threads (
    block_id TEXT NOT NULL UNIQUE,
    timestamp INTEGER NOT NULL,
    state BLOB NOT NULL
);
CREATE INDEX index_threads_timestamp ON threads (timestamp);
