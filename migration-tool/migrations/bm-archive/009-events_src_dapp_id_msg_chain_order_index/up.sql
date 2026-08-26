DROP INDEX IF EXISTS index_messages_ext_out_msg_chain_order;
CREATE INDEX index_messages_ext_out_msg_chain_order
    ON messages(msg_chain_order)
    WHERE msg_type IN (2, 4);
CREATE INDEX index_messages_events_src_dapp_id_msg_chain_order
    ON messages(src_dapp_id, msg_chain_order)
    WHERE msg_type IN (2, 4);
