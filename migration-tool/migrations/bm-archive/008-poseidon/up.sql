ALTER TABLE blocks ADD COLUMN block_merkle_leaves BLOB
    CHECK (block_merkle_leaves IS NULL OR length(block_merkle_leaves) = 512);
-- Encoded CommonSection.history_proofs: `n: u8 || n * (layer: u8, hash: [u8; 32])`.
-- Length must be 1 + 33 * n where n in 0..=10, so total 1..=331.
ALTER TABLE blocks ADD COLUMN history_proofs BLOB
    CHECK (history_proofs IS NULL
           OR (length(history_proofs) >= 1
               AND length(history_proofs) <= 331
               AND ((length(history_proofs) - 1) % 33) = 0));
ALTER TABLE blocks ADD COLUMN tracked_ext_out_messages_root BLOB
    CHECK (tracked_ext_out_messages_root IS NULL OR length(tracked_ext_out_messages_root) = 32);
ALTER TABLE blocks ADD COLUMN tracked_ext_out_message_hashes BLOB;
ALTER TABLE blocks ADD COLUMN proof_block_refs BLOB;
