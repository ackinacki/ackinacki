DROP TABLE threads;

ALTER TABLE blocks DROP COLUMN thread_id;
ALTER TABLE blocks DROP COLUMN producer_id;
