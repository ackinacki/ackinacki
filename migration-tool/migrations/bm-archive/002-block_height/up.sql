ALTER TABLE blocks ADD COLUMN height BLOB CHECK (length(height) = 8);
ALTER TABLE blocks ADD COLUMN envelope_hash BLOB CHECK (length(envelope_hash) = 32);

CREATE INDEX idx_blocks_thread_height ON blocks(height, thread_id);
