use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use serde::Deserialize;
use serde::Serialize;

use crate::bitmask::mask::Bitmask;
use crate::types::ThreadsTable;

/// A single deferred row insertion. The ThreadIdentifier is constructed
/// at resolve-time from the owning block's ID and the instruction's
/// index in the list (used as the u16 prefix).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InsertInstruction {
    /// Row index in the base table to insert above (same semantics as
    /// `BitmasksTable::insert_above`).
    pub row_index: usize,
    /// The bitmask for the new row.
    pub mask: Bitmask<AccountRouting>,
}

/// A deferred ThreadsTable that can be resolved once the block ID is known.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadsTablePrefab {
    /// The base table (may already have removals applied for collapse).
    base_table: ThreadsTable,
    /// Deferred insertions that require the block ID to construct their
    /// ThreadIdentifier. Empty for collapse operations.
    insert_instructions: Vec<InsertInstruction>,
}

impl ThreadsTablePrefab {
    /// Create a prefab for a split: base table is the current table,
    /// and there is one insert instruction for the new thread.
    pub fn with_split(
        base_table: ThreadsTable,
        row_index: usize,
        mask: Bitmask<AccountRouting>,
    ) -> Self {
        Self { base_table, insert_instructions: vec![InsertInstruction { row_index, mask }] }
    }

    /// Create a prefab for a collapse or any change that doesn't need
    /// the block ID (the base_table is already fully resolved).
    pub fn resolved(table: ThreadsTable) -> Self {
        Self { base_table: table, insert_instructions: vec![] }
    }

    /// Returns true if there are deferred insert instructions
    /// (i.e., this is a split operation).
    pub fn has_insert_instructions(&self) -> bool {
        !self.insert_instructions.is_empty()
    }

    /// Materialize the final ThreadsTable by applying all insert
    /// instructions with the given block_id. The u16 prefix for each
    /// ThreadIdentifier is derived from the instruction's position
    /// in the list (0, 1, 2, ...).
    pub fn resolve(&self, block_id: &BlockIdentifier) -> anyhow::Result<ThreadsTable> {
        let mut table = self.base_table.clone();
        for (i, instr) in self.insert_instructions.iter().enumerate() {
            let thread_id = ThreadIdentifier::new(block_id, i as u16);
            table.insert_above(instr.row_index, instr.mask.clone(), thread_id)?;
        }
        Ok(table)
    }

    /// Access the base table (without insert instructions applied).
    pub fn base_table(&self) -> &ThreadsTable {
        &self.base_table
    }

    /// Access the insert instructions.
    pub fn insert_instructions(&self) -> &[InsertInstruction] {
        &self.insert_instructions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_prefab_produces_same_table() {
        let table = ThreadsTable::new();
        let prefab = ThreadsTablePrefab::resolved(table.clone());
        assert!(!prefab.has_insert_instructions());
        let resolved = prefab.resolve(&BlockIdentifier::default()).unwrap();
        assert_eq!(resolved, table);
    }

    #[test]
    fn split_prefab_inserts_row_on_resolve() {
        let table = ThreadsTable::new();
        let mask = Bitmask::<AccountRouting>::builder()
            .meaningful_mask_bits(AccountRouting::default())
            .mask_bits(AccountRouting::default())
            .build();
        let prefab = ThreadsTablePrefab::with_split(table.clone(), 0, mask.clone());
        assert!(prefab.has_insert_instructions());

        let block_id = BlockIdentifier::default();
        let resolved = prefab.resolve(&block_id).unwrap();

        // The resolved table should have one more row than the base
        assert_eq!(resolved.len(), table.len() + 1);

        // The new thread identifier should match
        let expected_thread_id = ThreadIdentifier::new(&block_id, 0u16);
        assert!(resolved.rows().any(|(_, tid)| *tid == expected_thread_id));
    }

    #[test]
    fn prefab_serialization_roundtrip() {
        let table = ThreadsTable::new();
        let mask = Bitmask::<AccountRouting>::builder()
            .meaningful_mask_bits(AccountRouting::default())
            .mask_bits(AccountRouting::default())
            .build();
        let prefab = ThreadsTablePrefab::with_split(table, 0, mask);

        let serialized = serde_json::to_string(&prefab).unwrap();
        let deserialized: ThreadsTablePrefab = serde_json::from_str(&serialized).unwrap();
        assert_eq!(prefab, deserialized);
    }
}
