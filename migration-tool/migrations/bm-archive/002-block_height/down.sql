ALTER TABLE blocks DROP COLUMN height;
ALTER TABLE blocks DROP COLUMN envelope_hash;

DROP INDEX idx_blocks_thread_height;
