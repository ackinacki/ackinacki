CREATE INDEX IF NOT EXISTS index_messages_ext_out_msg_chain_order ON messages(msg_chain_order) WHERE msg_type = 2;
