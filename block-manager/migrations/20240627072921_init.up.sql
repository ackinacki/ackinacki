-- Add up migration script here
CREATE TABLE IF NOT EXISTS "raw_blocks" (
        `id` text PRIMARY KEY,
        `data` blob
);
