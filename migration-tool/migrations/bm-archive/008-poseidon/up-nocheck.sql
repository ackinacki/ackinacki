ALTER TABLE blocks ADD COLUMN block_merkle_leaves BLOB;
ALTER TABLE blocks ADD COLUMN history_proofs BLOB;
ALTER TABLE blocks ADD COLUMN tracked_ext_out_messages_root BLOB;
ALTER TABLE blocks ADD COLUMN tracked_ext_out_message_hashes BLOB;
ALTER TABLE blocks ADD COLUMN proof_block_refs BLOB;
