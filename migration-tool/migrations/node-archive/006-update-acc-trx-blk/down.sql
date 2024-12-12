ALTER TABLE accounts DROP COLUMN balance_other;

ALTER TABLE accounts DROP COLUMN boc;
ALTER TABLE accounts ADD COLUMN boc TEXT;
ALTER TABLE accounts DROP COLUMN code;
ALTER TABLE accounts ADD COLUMN code TEXT;
ALTER TABLE accounts DROP COLUMN data;
ALTER TABLE accounts ADD COLUMN data TEXT;

ALTER TABLE accounts DROP COLUMN due_payment;
ALTER TABLE accounts DROP COLUMN proof;
ALTER TABLE accounts DROP COLUMN prev_code_hash;
ALTER TABLE accounts DROP COLUMN state_hash;
ALTER TABLE accounts DROP COLUMN split_depth;

ALTER TABLE transactions DROP COLUMN proof;

ALTER TABLE transactions DROP COLUMN boc;
ALTER TABLE transactions ADD COLUMN boc TEXT;

ALTER TABLE blocks DROP COLUMN boc;
ALTER TABLE blocks ADD COLUMN boc TEXT;
