use std::collections::HashSet;

use node_types::AccountRouting;
use node_types::ThreadIdentifier;

use crate::bitmask::mask::Bitmask;

pub type ThreadsTable = crate::bitmask::table::BitmasksTable<AccountRouting, ThreadIdentifier>;

impl ThreadsTable {
    pub fn merge(&mut self, another_table: &ThreadsTable) -> anyhow::Result<()> {
        let mut merged_table = ThreadsTable::default();
        let mut inserted = HashSet::new();
        inserted.insert(ThreadIdentifier::default());
        let mut self_rows = self.rows().cloned().collect::<Vec<_>>();
        let mut another_table_rows = another_table.rows().cloned().collect::<Vec<_>>();
        anyhow::ensure!(
            self_rows.pop()
                == Some((Bitmask::<AccountRouting>::default(), ThreadIdentifier::default()))
        );
        anyhow::ensure!(
            another_table_rows.pop()
                == Some((Bitmask::<AccountRouting>::default(), ThreadIdentifier::default()))
        );
        while !self_rows.is_empty() || !another_table_rows.is_empty() {
            if !self_rows.is_empty() {
                let (mask, thread_id) = self_rows.last().cloned().unwrap();
                if another_table_rows.is_empty() {
                    self_rows.pop();
                    if inserted.insert(thread_id) {
                        merged_table.insert_above(0, mask, thread_id)?;
                    }
                } else {
                    let (mask2, thread_id2) = another_table_rows.last().cloned().unwrap();
                    if thread_id == thread_id2 {
                        anyhow::ensure!(mask == mask2);
                        another_table_rows.pop();
                        self_rows.pop();
                        if inserted.insert(thread_id) {
                            merged_table.insert_above(0, mask, thread_id)?;
                        }
                    } else if self_rows.iter().any(|(_m, t)| *t == thread_id2) {
                        self_rows.pop();
                        if inserted.insert(thread_id) {
                            merged_table.insert_above(0, mask, thread_id)?;
                        }
                    } else if another_table_rows.iter().any(|(_m, t)| *t == thread_id) {
                        another_table_rows.pop();
                        if inserted.insert(thread_id2) {
                            merged_table.insert_above(0, mask2, thread_id2)?;
                        }
                    } else {
                        self_rows.pop();
                        if inserted.insert(thread_id) {
                            merged_table.insert_above(0, mask, thread_id)?;
                        }
                    }
                }
            } else {
                let (mask, thread_id) = another_table_rows.pop().unwrap();
                if inserted.insert(thread_id) {
                    merged_table.insert_above(0, mask, thread_id)?;
                }
            }
        }
        if self != &merged_table {
            tracing::trace!("Threads table was updated after merge: {merged_table:?}");
        }
        *self = merged_table;
        Ok(())
    }

    pub fn list_threads(&self) -> impl Iterator<Item = &'_ ThreadIdentifier> {
        self.rows().map(|(_, thread)| thread).collect::<HashSet<_>>().into_iter()
    }

    pub fn length(&self) -> usize {
        self.rows().count()
    }
}

#[cfg(test)]
mod tests {
    use node_types::BlockIdentifier;

    use super::*;
    use crate::bitmask::mask::Bitmask;

    #[test]
    fn threads_table_is_serializable() {
        let mut threads_table = ThreadsTable::new();
        let any_bitmask = Bitmask::<AccountRouting>::builder()
            .meaningful_mask_bits(AccountRouting::default())
            .mask_bits(AccountRouting::default())
            .build();
        threads_table
            .insert_above(0, any_bitmask, ThreadIdentifier::new(&BlockIdentifier::default(), 1))
            .unwrap();
        let serialized_threads_table = serde_json::to_string(&threads_table).unwrap();

        let restored_table: ThreadsTable = serde_json::from_str(&serialized_threads_table).unwrap();

        assert_eq!(threads_table, restored_table);
    }

    fn add_mask(threads_table: &mut ThreadsTable, bits: &[usize], row: usize, thread_prefix: u16) {
        let mut routing = AccountRouting::default();
        for bit in bits {
            routing.set_bit(*bit);
        }
        let any_bitmask = Bitmask::<AccountRouting>::builder()
            .meaningful_mask_bits(routing)
            .mask_bits(routing)
            .build();
        threads_table
            .insert_above(
                row,
                any_bitmask,
                ThreadIdentifier::new(&BlockIdentifier::default(), thread_prefix),
            )
            .expect("insert error");
    }

    #[test]
    fn test_merge_tables() -> anyhow::Result<()> {
        let mut threads_table = ThreadsTable::new();
        add_mask(&mut threads_table, &[255], 0, 1);
        let mut threads_table2 = threads_table.clone();
        add_mask(&mut threads_table, &[254], 1, 2);
        add_mask(&mut threads_table2, &[254, 255], 0, 3);
        println!("{threads_table:?}");
        println!("{threads_table2:?}");
        threads_table.merge(&threads_table2)?;
        println!("{threads_table:?}");
        let mut etalon = threads_table2.clone();
        add_mask(&mut etalon, &[254], 2, 2);
        println!("{etalon:?}");
        assert_eq!(threads_table, etalon);
        Ok(())
    }

    /// Regression test: when two tables have the same threads in different
    /// relative orders, the old merge algorithm would insert a thread twice.
    /// E.g. table1=[T3, T2, T1, D, default] and table2=[T2, T3, T1, D, default]
    /// should merge without duplicates.
    #[test]
    fn test_merge_different_relative_order_no_duplicates() -> anyhow::Result<()> {
        // Build base: [T1, D, default]
        let mut base = ThreadsTable::new();
        add_mask(&mut base, &[255], 0, 1); // T1 at row 0
        add_mask(&mut base, &[254], 1, 10); // D at row 1

        // table1: T2 then T3 above T1 → [T3, T2, T1, D, default]
        let mut table1 = base.clone();
        add_mask(&mut table1, &[253], 0, 2); // T2 at row 0
        add_mask(&mut table1, &[252], 0, 3); // T3 at row 0 (pushes T2 down)

        // table2: T3 then T2 above T1 → [T2, T3, T1, D, default]
        let mut table2 = base.clone();
        add_mask(&mut table2, &[252], 0, 3); // T3 at row 0
        add_mask(&mut table2, &[253], 0, 2); // T2 at row 0 (pushes T3 down)

        // Verify setup: both tables have the same 4 threads
        assert_eq!(table1.len(), 5); // 4 threads + default
        assert_eq!(table2.len(), 5);

        // Merge
        table1.merge(&table2)?;

        // The merged table must have exactly 4 threads + default = 5 rows
        assert_eq!(table1.len(), 5, "Merge produced duplicates: {:?}", table1);

        // All 4 threads must be present
        let threads: HashSet<_> = table1.list_threads().collect();
        let block_id = BlockIdentifier::default();
        assert!(threads.contains(&ThreadIdentifier::new(&block_id, 1)));
        assert!(threads.contains(&ThreadIdentifier::new(&block_id, 2)));
        assert!(threads.contains(&ThreadIdentifier::new(&block_id, 3)));
        assert!(threads.contains(&ThreadIdentifier::new(&block_id, 10)));
        Ok(())
    }

    #[test]
    fn test_duplicates() -> anyhow::Result<()> {
        let mut threads_table = ThreadsTable::new();
        add_mask(&mut threads_table, &[254], 0, 1);
        add_mask(&mut threads_table, &[254], 0, 2);
        add_mask(&mut threads_table, &[255], 0, 3);
        add_mask(&mut threads_table, &[253, 255], 0, 4);
        add_mask(&mut threads_table, &[254, 255], 0, 5);
        add_mask(&mut threads_table, &[253, 254, 255], 0, 6);

        let rows = threads_table.rows().map(|(a, _b)| a.clone()).collect::<Vec<_>>();
        let mut rows_dedup = rows.clone();
        rows_dedup.dedup();
        assert_ne!(rows_dedup.len(), rows.len());
        Ok(())
    }
}
