ALTER TABLE transactions DROP COLUMN storage_fees_collected;
ALTER TABLE transactions ADD COLUMN storage_fees_collected TEXT NOT NULL;
