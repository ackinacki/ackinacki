ALTER TABLE accounts ADD COLUMN balance_other BLOB;

ALTER TABLE accounts DROP COLUMN boc;
ALTER TABLE accounts ADD COLUMN boc BLOB;
ALTER TABLE accounts DROP COLUMN code;
ALTER TABLE accounts ADD COLUMN code BLOB;
ALTER TABLE accounts DROP COLUMN data;
ALTER TABLE accounts ADD COLUMN data BLOB;

ALTER TABLE accounts ADD COLUMN due_payment TEXT;
ALTER TABLE accounts ADD COLUMN proof BLOB;
ALTER TABLE accounts ADD COLUMN prev_code_hash TEXT;
ALTER TABLE accounts ADD COLUMN state_hash TEXT;
ALTER TABLE accounts ADD COLUMN split_depth INTEGER;

ALTER TABLE transactions ADD COLUMN proof BLOB;
ALTER TABLE transactions DROP COLUMN boc;
ALTER TABLE transactions ADD COLUMN boc BLOB;

ALTER TABLE blocks DROP COLUMN boc;
ALTER TABLE blocks ADD COLUMN boc BLOB;
