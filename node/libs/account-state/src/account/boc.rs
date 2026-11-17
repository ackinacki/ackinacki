//! Canonicality detection inside the existing BOC reader's storage callbacks.
//!
//! The SDK still parses descriptors, validates references, constructs DataCells
//! and computes hashes. Dense index storages avoid its index-keyed HashMaps.
//! We check size-affecting flags while reading raw cells, and uniqueness when a
//! computed cell hash first becomes available. No decoded-DAG traversal follows.
use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;

use hashbrown::HashSet;
use tvm_types::boc::BocReader;
use tvm_types::boc::BocReaderResult;
use tvm_types::boc::DoneCellsStorage;
use tvm_types::boc::IndexedCellsStorage;
use tvm_types::boc::RawCell;
use tvm_types::Cell;
use tvm_types::UInt256;

const GENERIC_V1: u32 = 0xb5ee9c72;

// Reuse scratch for ordinary accounts, but do not let a single large BOC set
// the cost of every subsequent HashSet::clear() on this worker. These bounds
// limit retained capacity only; larger accounts are still decoded normally.
const MAX_RETAINED_CELLS: usize = 4096;

fn clear_scratch_vec<T>(buffer: &mut Vec<T>, max_capacity: usize) {
    buffer.clear();
    if buffer.capacity() > max_capacity {
        drop(std::mem::take(buffer));
    }
}

fn width(value: usize) -> usize {
    ((usize::BITS - value.leading_zeros()) as usize).div_ceil(8).max(1)
}

#[derive(Default)]
struct State {
    raw: Vec<RawCell>,
    done: Vec<Option<Cell>>,
    seen: HashSet<UInt256>,
    referenced: Vec<u64>,
    cells: usize,
    data_bytes: usize,
    references: usize,
    highest_reference: usize,
    checked_reachability: bool,
    max_cells: usize,
    canonical: bool,
    order_stack: Vec<(u32, bool)>,
}

impl State {
    fn clear(&mut self) {
        // Drop in ascending physical order: children have higher indices and
        // stay owned until their own slot is cleared. Never retain Cells in TLS
        // after success or malformed input; only buffer capacities are reused.
        clear_scratch_vec(&mut self.raw, MAX_RETAINED_CELLS);
        clear_scratch_vec(&mut self.done, MAX_RETAINED_CELLS);
        // Discard oversized tables before clearing: hashbrown resets all
        // allocated control bytes when a nonempty table is cleared.
        if self.seen.capacity() > MAX_RETAINED_CELLS {
            self.seen = HashSet::new();
        } else {
            self.seen.clear();
        }
        clear_scratch_vec(&mut self.referenced, MAX_RETAINED_CELLS.div_ceil(64));
        self.cells = 0;
        self.data_bytes = 0;
        self.references = 0;
        self.highest_reference = 0;
        self.checked_reachability = false;
        self.canonical = true;
        // DFS can schedule both an entry and an exit for each cell.
        clear_scratch_vec(&mut self.order_stack, 2 * MAX_RETAINED_CELLS);
    }

    fn mark_reference(&mut self, index: usize) {
        let word = index / 64;
        if self.referenced.len() <= word {
            self.referenced.resize(word + 1, 0);
        }
        self.referenced[word] |= 1 << (index % 64);
    }

    fn check_reachability(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.highest_reference < self.cells, "BOC reference out of range");
        // The SDK requires every reference to point to a strictly higher index.
        // In this finite DAG, an unreachable component has a smallest-indexed
        // vertex with no incoming edge. Thus every non-root having an incoming
        // edge proves reachability from root zero. This is NOT merely a count
        // of references, which would fail for repeated references to one child.
        for first in (0..self.cells).step_by(64) {
            let count = (self.cells - first).min(64);
            let required = if count == 64 { u64::MAX } else { (1 << count) - 1 };
            anyhow::ensure!(
                self.referenced.get(first / 64).copied().unwrap_or(0) & required == required,
                "duplicate or unreachable BOC cells"
            );
        }
        Ok(())
    }

    fn check_byte_order(&mut self) -> anyhow::Result<()> {
        // BocWriter emits reverse DFS postorder, visiting references in their
        // original order and deduplicating already discovered cells. Hash
        // uniqueness is checked by DoneStorage, so physical indices can stand
        // in for hashes here. Reuse the reachability bitmap as DFS discovery bits.
        self.referenced.fill(0);
        self.order_stack.push((0, false));
        let mut expected = self.cells;
        while let Some((index, post)) = self.order_stack.pop() {
            let at = index as usize;
            if post {
                expected = expected
                    .checked_sub(1)
                    .ok_or_else(|| anyhow::anyhow!("invalid BOC postorder"))?;
                anyhow::ensure!(at == expected, "noncanonical BOC cell order");
                continue;
            }
            if self.referenced[at / 64] & (1 << (at % 64)) != 0 {
                continue;
            }
            self.mark_reference(at);
            self.order_stack.push((index, true));
            // Raw storage is still complete at this point. The SDK has already
            // checked forward references; reachability checked all bounds.
            let raw = &self.raw[at];
            let refs = (raw.data[0] & 7) as usize;
            for &child in raw.refs[..refs].iter().rev() {
                self.order_stack.push((child, false));
            }
        }
        anyhow::ensure!(expected == 0, "unreachable BOC cells in postorder");
        Ok(())
    }
}

thread_local! {
    static STATE: Rc<RefCell<State>> = Rc::new(RefCell::new(State::default()));
}

struct RawStorage(Rc<RefCell<State>>);

impl IndexedCellsStorage for RawStorage {
    fn insert(&mut self, index: u32, cell: RawCell) -> tvm_types::Result<()> {
        let mut state = self.0.borrow_mut();
        anyhow::ensure!(index as usize == state.cells, "nonsequential BOC cell index");
        let descriptor = *cell.data.first().ok_or_else(|| anyhow::anyhow!("empty BOC cell"))?;
        if descriptor & 0x10 != 0 {
            state.canonical = false;
        }
        // Generic V1/big-cell validity and refs_count <= 4 are checked by the
        // SDK before this callback. Only physical forward references reach us.
        let refs = (descriptor & 7) as usize;
        anyhow::ensure!(refs <= cell.refs.len(), "invalid BOC reference count");
        for &reference in &cell.refs[..refs] {
            let reference = reference as usize;
            // Bound scratch allocation by the actual input length, never by an
            // unchecked declared count or arbitrary u32 reference value.
            anyhow::ensure!(reference < state.max_cells, "BOC reference out of range");
            state.highest_reference = state.highest_reference.max(reference);
            state.mark_reference(reference);
        }
        state.references = state
            .references
            .checked_add(refs)
            .ok_or_else(|| anyhow::anyhow!("BOC reference count overflow"))?;
        state.data_bytes = state
            .data_bytes
            .checked_add(cell.data.len())
            .ok_or_else(|| anyhow::anyhow!("BOC data size overflow"))?;
        state.cells += 1;
        state.raw.push(cell);
        Ok(())
    }

    fn remove(&mut self, index: u32) -> tvm_types::Result<RawCell> {
        let mut state = self.0.borrow_mut();
        // All raw cells have now been read. Detect unreachable records and
        // noncanonical order before constructing trees or computing hashes;
        // still decode the complete valid graph when canonicality is false.
        if !state.checked_reachability {
            // Broken references are decoder errors. Unreachable records and
            // a different valid order only make the encoding noncanonical.
            anyhow::ensure!(state.highest_reference < state.cells, "BOC reference out of range");
            if state.check_reachability().is_err() {
                state.canonical = false;
            }
            if state.canonical && state.check_byte_order().is_err() {
                state.canonical = false;
            }
            state.checked_reachability = true;
        }
        anyhow::ensure!(index as usize + 1 == state.raw.len(), "nonreverse BOC cell removal");
        state.raw.pop().ok_or_else(|| anyhow::anyhow!("missing raw BOC cell"))
    }

    fn cleanup(&mut self) -> tvm_types::Result<()> {
        Ok(())
    }
}

struct DoneStorage(Rc<RefCell<State>>);

impl DoneCellsStorage for DoneStorage {
    fn insert(&mut self, index: u32, cell: Cell) -> tvm_types::Result<()> {
        let mut state = self.0.borrow_mut();
        // repr_hash is cached by DataCell construction: no second hashing pass.
        if !state.seen.insert(cell.repr_hash()) {
            state.canonical = false;
        }
        if state.done.is_empty() {
            let cells = state.cells;
            state.done.resize_with(cells, || None);
        }
        let slot = state
            .done
            .get_mut(index as usize)
            .ok_or_else(|| anyhow::anyhow!("BOC cell index out of range"))?;
        anyhow::ensure!(slot.is_none(), "duplicate BOC cell index");
        *slot = Some(cell);
        Ok(())
    }

    fn get(&self, index: u32) -> tvm_types::Result<Cell> {
        self.0
            .borrow()
            .done
            .get(index as usize)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("BOC reference out of range"))
    }

    fn cleanup(&mut self) -> tvm_types::Result<()> {
        Ok(())
    }
}

/// Parsing errors remain errors. Encoding choices which represent a valid DAG
/// (including duplicates, unreachable cells or a different order) return the
/// decoded root(s) and false, so callers can normalize without decoding twice.
pub(super) fn decode_with_canonicality(bytes: &[u8]) -> anyhow::Result<(BocReaderResult, bool)> {
    anyhow::ensure!(bytes.len() >= 6, "truncated BOC header");
    let magic = u32::from_be_bytes(bytes[..4].try_into()?);
    if magic != GENERIC_V1 {
        // The SDK's V2/V3/legacy formats need their original reader path. They
        // are never our plain V1 byte canon, but may still be valid BOCs.
        return tvm_types::boc::read_boc(bytes).map(|boc| (boc, false));
    }
    STATE.with(|shared| {
        {
            let mut state = shared.borrow_mut();
            state.clear();
            state.max_cells = bytes.len() / 2;
            state.canonical = bytes[4] & !7 == 0;
            // Root zero is exempt from the incoming-edge requirement.
            state.mark_reference(0);
        }
        let result = BocReader::new()
            .set_indexed_cells_storage(Box::new(RawStorage(shared.clone())))
            .set_done_cells_storage(Box::new(DoneStorage(shared.clone())))
            .read(&mut Cursor::new(bytes))
            .and_then(|boc| {
                let state = shared.borrow();
                let header = &boc.header;
                anyhow::ensure!(header.cells_count == state.cells, "BOC cell count mismatch");
                let ref_size = width(state.cells);
                let payload = state
                    .references
                    .checked_mul(ref_size)
                    .and_then(|size| size.checked_add(state.data_bytes))
                    .ok_or_else(|| anyhow::anyhow!("BOC payload size overflow"))?;
                let offset_size = width(payload);
                let expected = 6usize
                    .checked_add(4 * ref_size)
                    .and_then(|size| size.checked_add(offset_size))
                    .and_then(|size| size.checked_add(payload))
                    .ok_or_else(|| anyhow::anyhow!("BOC size overflow"))?;
                let canonical = state.canonical
                    && header.roots_count == 1
                    && header.roots_indexes.as_slice() == [0]
                    && header.ref_size == ref_size
                    && header.offset_size == offset_size
                    && header.tot_cells_size == payload
                    && bytes.len() == expected;
                Ok((boc, canonical))
            });
        // Trait cleanup hooks may not run on parse errors. In every case keep
        // only empty reusable capacities, not ownership of decoded Cells.
        shared.borrow_mut().clear();
        result
    })
}

#[cfg(test)]
#[path = "boc_tests.rs"]
mod tests;
