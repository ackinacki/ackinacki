-- Extra currencies moved by a transaction. Stored as a JSON array ordered by
-- currency id: `[{"currency":<u32>,"value":"<signed decimal>"}]`.
-- `messages.value_other` cannot be reused here: it is an `ExtraCurrencyCollection`
-- BOC and its `VarUInteger32` amounts have no sign, while a balance delta may be
-- negative. Amounts that net out to zero are not stored at all (NULL).
ALTER TABLE transactions ADD COLUMN balance_delta_other TEXT;
ALTER TABLE transactions ADD COLUMN total_fees_other TEXT;
