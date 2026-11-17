use tvm_types::boc::read_boc;
use tvm_types::boc::BocHeader;
use tvm_types::boc::BocWriter;
use tvm_types::write_boc;
use tvm_types::BuilderData;
use tvm_types::CellType;

use super::*;

#[derive(Default)]
struct Scratch {
    seen: HashSet<UInt256>,
    stack: Vec<Cell>,
}

thread_local! {
    // Reuse oracle allocations between fixtures. No Cell may remain in
    // scratch after a check, including an early failure on untrusted data.
    static SCRATCH: RefCell<Scratch> = RefCell::new(Scratch::default());
}

fn check(root: &Cell, header: &BocHeader, encoded_len: usize) -> anyhow::Result<()> {
    anyhow::ensure!(header.magic == GENERIC_V1, "size-canonical BOC requires generic V1");
    anyhow::ensure!(
        header.roots_count == 1 && header.roots_indexes.as_slice() == [0],
        "size-canonical BOC requires one root at index zero"
    );
    anyhow::ensure!(
        !header.index_included && !header.has_crc && !header.has_cache_bits,
        "size-canonical BOC must not contain an index, CRC or cache bits"
    );
    SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        scratch.seen.clear();
        scratch.stack.clear();
        let result = scratch.check_graph(root, header, encoded_len);
        scratch.stack.clear();
        scratch.seen.clear();
        result
    })
}

impl Scratch {
    fn check_graph(
        &mut self,
        root: &Cell,
        header: &BocHeader,
        encoded_len: usize,
    ) -> anyhow::Result<()> {
        let mut data_bytes = 0usize;
        let mut references = 0usize;
        self.stack.push(root.clone());
        while let Some(cell) = self.stack.pop() {
            // Test flags even for a repeated hash: an alternative physical
            // representation must not hide behind a previously visited cell.
            anyhow::ensure!(cell.virtualization() == 0, "virtual cell in size-canonical BOC");
            anyhow::ensure!(cell.cell_type() != CellType::Big, "big cell in generic V1 BOC");
            anyhow::ensure!(!cell.store_hashes(), "stored cell hashes in size-canonical BOC");
            if !self.seen.insert(cell.repr_hash()) {
                continue;
            }
            data_bytes = data_bytes
                .checked_add(cell.raw_data().map_err(|error| anyhow::anyhow!("{error}"))?.len())
                .ok_or_else(|| anyhow::anyhow!("BOC data size overflow"))?;
            references = references
                .checked_add(cell.references_count())
                .ok_or_else(|| anyhow::anyhow!("BOC reference count overflow"))?;
            for index in 0..cell.references_count() {
                self.stack.push(cell.reference(index).map_err(|error| anyhow::anyhow!("{error}"))?);
            }
        }
        let cells = self.seen.len();
        anyhow::ensure!(cells == header.cells_count, "duplicate or unreachable BOC cells");
        // Match BocWriter's width of the COUNT, including the 255/256 boundary,
        // rather than taking the width of the largest index (count - 1).
        let ref_size = width(cells);
        anyhow::ensure!(
            ref_size <= 4 && header.ref_size == ref_size,
            "nonminimal BOC reference width"
        );
        let payload = references
            .checked_mul(ref_size)
            .and_then(|size| size.checked_add(data_bytes))
            .ok_or_else(|| anyhow::anyhow!("BOC payload size overflow"))?;
        let offset_size = width(payload);
        anyhow::ensure!(header.offset_size == offset_size, "nonminimal BOC offset width");
        anyhow::ensure!(header.tot_cells_size == payload, "noncanonical BOC payload size");
        // Magic + flags + offset width; cell/root/absent counts and root index;
        // payload-size field. No index table, CRC or additional roots.
        let expected = 6usize
            .checked_add(4 * ref_size)
            .and_then(|size| size.checked_add(offset_size))
            .and_then(|size| size.checked_add(payload))
            .ok_or_else(|| anyhow::anyhow!("BOC size overflow"))?;
        anyhow::ensure!(encoded_len == expected, "noncanonical BOC size or trailing bytes");
        Ok(())
    }
}

// Assert the production flag against an independent decoder + DAG walk and
// the SDK writer. Return size-policy failures for the malformed/noncanonical
// fixtures below; alternate valid cell orders still verify a false byte flag.
fn validate(bytes: &[u8]) -> anyhow::Result<()> {
    let reporting = decode_with_canonicality(bytes);
    let boc = match read_boc(bytes) {
        Ok(boc) => boc,
        Err(error) => {
            assert!(reporting.is_err(), "decoder accepted malformed BOC");
            return Err(anyhow::anyhow!("{error}"));
        }
    };
    let reference = check(&boc.roots[0], &boc.header, bytes.len());
    let expected = reference.is_ok() && write_boc(&boc.roots[0]).unwrap() == bytes;
    let (reported, canonical) = reporting?;
    assert_eq!(canonical, expected, "reported canonicality/writer mismatch");
    assert_eq!(reported.roots.len(), boc.roots.len());
    for (actual, expected) in reported.roots.iter().zip(&boc.roots) {
        assert_eq!(actual.repr_hash(), expected.repr_hash());
    }
    reference
}

fn decode_bytes(bytes: &[u8]) -> anyhow::Result<tvm_types::boc::BocReaderResult> {
    let (boc, canonical) = decode_with_canonicality(bytes)?;
    anyhow::ensure!(canonical, "noncanonical fixture");
    Ok(boc)
}

fn byte_cell(value: u16) -> Cell {
    BuilderData::with_raw(value.to_be_bytes().to_vec(), 16).unwrap().into_cell().unwrap()
}

fn graph(leaves: usize) -> Cell {
    let mut level: Vec<_> = (0..leaves).map(|index| byte_cell(index as u16)).collect();
    while level.len() > 1 {
        level = level
            .chunks(4)
            .map(|children| {
                BuilderData::with_raw_and_refs(Vec::new(), 0, children.iter().cloned())
                    .unwrap()
                    .into_cell()
                    .unwrap()
            })
            .collect();
    }
    level.pop().unwrap()
}

fn raw_boc(cells: &[&[u8]]) -> Vec<u8> {
    let payload: usize = cells.iter().map(|cell| cell.len()).sum();
    assert!(cells.len() < 256 && payload < 256);
    let mut bytes = vec![0xb5, 0xee, 0x9c, 0x72, 1, 1, cells.len() as u8, 1, 0, payload as u8, 0];
    for cell in cells {
        bytes.extend_from_slice(cell);
    }
    bytes
}

#[test]
fn default_writer_sizes_pass_for_shared_graphs_and_width_boundaries() -> anyhow::Result<()> {
    for leaves in [1, 2, 3, 190, 191, 192, 254, 255, 256, 257, 1024] {
        let child = graph(leaves);
        let root = BuilderData::with_raw_and_refs(Vec::new(), 0, [child.clone(), child])
            .unwrap()
            .into_cell()
            .unwrap();
        let bytes = write_boc(&root).map_err(|error| anyhow::anyhow!("{error}"))?;
        validate(&bytes)?;
    }
    Ok(())
}

#[test]
fn large_boc_does_not_inflate_scratch_for_subsequent_small_accounts() -> anyhow::Result<()> {
    fn capacities() -> [usize; 5] {
        STATE.with(|shared| {
            let state = shared.borrow();
            assert!(state.raw.is_empty());
            assert!(state.done.is_empty());
            assert!(state.seen.is_empty());
            assert!(state.referenced.is_empty());
            assert!(state.order_stack.is_empty());
            [
                state.raw.capacity(),
                state.done.capacity(),
                state.seen.capacity(),
                state.referenced.capacity(),
                state.order_stack.capacity(),
            ]
        })
    }

    let large = graph(2 * MAX_RETAINED_CELLS);
    let bytes = write_boc(&large).unwrap();
    let small = byte_cell(42);
    let small_bytes = write_boc(&small).unwrap();
    for malformed in [false, true] {
        if malformed {
            assert!(decode_with_canonicality(&bytes[..bytes.len() - 1]).is_err());
        } else {
            let (decoded, canonical) = decode_with_canonicality(&bytes)?;
            assert!(canonical);
            assert_eq!(decoded.roots[0].repr_hash(), large.repr_hash());
        }
        let retained = capacities();
        // A complete decode exceeds all three per-cell buffers. A raw parse
        // failure may never populate done/seen, leaving their small caches.
        if !malformed {
            assert_eq!(&retained[..3], &[0, 0, 0]);
        }
        assert!(retained[..3].iter().all(|&capacity| capacity <= MAX_RETAINED_CELLS));
        assert!(retained[3] <= MAX_RETAINED_CELLS.div_ceil(64));
        assert!(retained[4] <= 2 * MAX_RETAINED_CELLS);
        decode_with_canonicality(&small_bytes)?;
        let small_capacities = capacities();
        assert!(small_capacities[2] > 0, "ordinary tables should still be reused");
        for _ in 0..1000 {
            let (decoded, canonical) = decode_with_canonicality(&small_bytes)?;
            assert!(canonical);
            assert_eq!(decoded.roots[0].repr_hash(), small.repr_hash());
        }
        assert_eq!(capacities(), small_capacities);
    }
    Ok(())
}

#[test]
fn canonical_size_accepts_different_valid_cell_orders() -> anyhow::Result<()> {
    let first = raw_boc(&[&[2, 0, 1, 2], &[0, 2, 0x11], &[0, 2, 0x22]]);
    let second = raw_boc(&[&[2, 0, 2, 1], &[0, 2, 0x22], &[0, 2, 0x11]]);
    assert_ne!(first, second);
    validate(&first)?;
    validate(&second)?;
    let root_a = read_boc(&first).unwrap().withdraw_single_root().unwrap();
    let root_b = read_boc(&second).unwrap().withdraw_single_root().unwrap();
    assert_eq!(root_a.repr_hash(), root_b.repr_hash());
    assert_eq!(write_boc(&root_a).unwrap().len(), first.len());
    assert_eq!(write_boc(&root_b).unwrap().len(), second.len());
    Ok(())
}

#[test]
fn valid_but_size_increasing_writer_options_are_rejected() -> anyhow::Result<()> {
    let root = graph(17);
    for (index, crc, refs, offsets) in [
        (true, false, None, None),
        (false, true, None, None),
        (false, false, Some(3), None),
        (false, false, None, Some(4)),
    ] {
        let mut bytes = Vec::new();
        BocWriter::with_root(&root)
            .unwrap()
            .write_ex(&mut bytes, index, crc, refs, offsets)
            .unwrap();
        assert!(read_boc(&bytes).is_ok(), "fixture must be a valid BOC");
        assert!(validate(&bytes).is_err());
    }
    validate(&write_boc(&root).unwrap())?;
    Ok(())
}

#[test]
fn duplicate_and_unreachable_physical_cells_are_rejected() {
    let duplicate = raw_boc(&[&[2, 0, 1, 2], &[0, 2, 0x55], &[0, 2, 0x55]]);
    let unreachable = raw_boc(&[&[0, 0], &[0, 2, 0x55]]);
    for bytes in [duplicate, unreachable] {
        assert!(read_boc(&bytes).is_ok(), "fixture must decode successfully");
        assert!(validate(&bytes).unwrap_err().to_string().contains("duplicate or unreachable"));
    }
}

#[test]
fn optional_embedded_hashes_are_rejected_even_when_correct() {
    let empty = BuilderData::default().into_cell().unwrap();
    let mut cell = vec![0x10, 0];
    cell.extend_from_slice(empty.repr_hash().as_slice());
    cell.extend_from_slice(&[0, 0]);
    let bytes = raw_boc(&[&cell]);
    assert!(read_boc(&bytes).is_ok());
    assert!(validate(&bytes).unwrap_err().to_string().contains("stored cell hashes"));
    validate(&write_boc(&empty).unwrap()).unwrap();
}

#[test]
fn trailing_bytes_and_bad_header_are_rejected() {
    let original = write_boc(&graph(4)).unwrap();
    let mut trailing = original.clone();
    trailing.push(0);
    assert!(validate(&trailing).is_err());
    let mut nonminimal = original;
    nonminimal[5] = 8;
    assert!(validate(&nonminimal).is_err());
}

#[test]
fn account_validation_keeps_original_allocation_and_decoded_view() -> anyhow::Result<()> {
    let source = crate::ThreadAccount::default();
    let bytes = source.canonical_bytes().to_vec().into_boxed_slice();
    let ptr = bytes.as_ptr();
    let checked = crate::ThreadAccount::recanonicalize_tvm_bytes(bytes)?;
    assert_eq!(checked.canonical_bytes().as_ptr(), ptr);
    assert_eq!(checked.account_hash(), source.account_hash());
    assert_eq!(checked.tvm_shard_account()?, source.tvm_shard_account()?);
    let invalid_account = write_boc(&byte_cell(42)).unwrap().into_boxed_slice();
    assert!(crate::ThreadAccount::recanonicalize_tvm_bytes(invalid_account).is_err());
    Ok(())
}

#[test]
fn repeated_incoming_edges_do_not_hide_an_unreachable_cell() {
    // There are enough edges in total, but both point to cell 2. Cell 1 is
    // unreachable. Counting incoming edges instead of marking indices is wrong.
    let bytes = raw_boc(&[&[2, 0, 2, 2], &[0, 2, 0x11], &[0, 2, 0x22]]);
    assert!(read_boc(&bytes).is_ok());
    assert!(validate(&bytes).is_err());
}

#[test]
fn integrated_decoder_matches_reference_on_mutations_and_truncations() -> anyhow::Result<()> {
    for leaves in [1, 3, 17] {
        let original = write_boc(&graph(leaves)).unwrap();
        for end in 0..original.len() {
            assert!(validate(&original[..end]).is_err());
            // Exercise reuse on the same thread after every possible failure.
            assert_eq!(
                decode_with_canonicality(&original)?.0.roots[0].repr_hash(),
                graph(leaves).repr_hash()
            );
        }
        for position in 0..original.len() {
            for bit in [1, 0x10, 0x80] {
                let mut changed = original.clone();
                changed[position] ^= bit;
                // validate checks decoder acceptance against the independent
                // native-decoder + DAG-walk reference, including valid changes.
                let _ = validate(&changed);
            }
        }
        validate(&original)?;
    }
    Ok(())
}

#[test]
fn dangling_reference_equal_to_cell_count_is_rejected() {
    // The SDK raw reader permits r == cells_count, then normally fails during
    // reference resolution. The dense callback must reject it without indexing
    // outside its storage, and must leave scratch usable for the next account.
    let dangling = raw_boc(&[&[1, 0, 1]]);
    assert!(decode_with_canonicality(&dangling).is_err());
    validate(&write_boc(&graph(2)).unwrap()).unwrap();
}

#[test]
fn byte_canonical_mode_rejects_size_canonical_permutations() -> anyhow::Result<()> {
    for variants in [
        [
            raw_boc(&[&[2, 0, 1, 2], &[0, 2, 0x11], &[0, 2, 0x22]]),
            raw_boc(&[&[2, 0, 2, 1], &[0, 2, 0x22], &[0, 2, 0x11]]),
        ],
        // Both branches share the last leaf. A simple index sort is not the
        // writer's DFS order: traversal has to remember discovered vertices.
        [
            raw_boc(&[&[2, 0, 1, 2], &[1, 2, 0x11, 3], &[1, 2, 0x22, 3], &[0, 2, 0x33]]),
            raw_boc(&[&[2, 0, 2, 1], &[1, 2, 0x22, 3], &[1, 2, 0x11, 3], &[0, 2, 0x33]]),
        ],
    ] {
        for bytes in &variants {
            validate(bytes)?;
        }
        assert_eq!(variants.iter().filter(|bytes| decode_bytes(bytes).is_ok()).count(), 1);
    }
    Ok(())
}

#[test]
fn byte_order_matches_writer_for_random_shared_dags() -> anyhow::Result<()> {
    use rand::Rng;
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    for seed in 0..100u64 {
        let mut nodes = Vec::<Cell>::new();
        for index in 0..80u64 {
            let refs = if nodes.is_empty() { 0 } else { rng.gen_range(0..=4) };
            let children: Vec<_> =
                (0..refs).map(|_| nodes[rng.gen_range(0..nodes.len())].clone()).collect();
            let mut data = seed.to_be_bytes().to_vec();
            data.extend_from_slice(&index.to_be_bytes());
            nodes.push(
                BuilderData::with_raw_and_refs(data, 128, children).unwrap().into_cell().unwrap(),
            );
        }
        let root = BuilderData::with_raw_and_refs(Vec::new(), 0, nodes[76..].iter().cloned())
            .unwrap()
            .into_cell()
            .unwrap();
        let bytes = write_boc(&root).unwrap();
        let checked = decode_bytes(&bytes)?;
        assert_eq!(checked.roots[0].repr_hash(), root.repr_hash());
        validate(&bytes)?;
    }
    Ok(())
}

#[test]
fn noncanonical_boc_returns_the_root_instead_of_an_encoding_error() -> anyhow::Result<()> {
    let root = graph(17);
    let plain = write_boc(&root).unwrap();
    let (decoded, canonical) = crate::decode_boc_with_canonicality(&plain)?;
    assert!(canonical);
    assert_eq!(decoded.repr_hash(), root.repr_hash());
    for (index, crc, refs, offsets) in [
        (true, false, None, None),
        (false, true, None, None),
        (false, false, Some(3), None),
        (false, false, None, Some(4)),
    ] {
        let mut bytes = Vec::new();
        BocWriter::with_root(&root)
            .unwrap()
            .write_ex(&mut bytes, index, crc, refs, offsets)
            .unwrap();
        let (decoded, canonical) = crate::decode_boc_with_canonicality(&bytes)?;
        assert!(!canonical);
        assert_eq!(decoded.repr_hash(), root.repr_hash());
    }
    for bytes in [
        raw_boc(&[&[2, 0, 1, 2], &[0, 2, 0x55], &[0, 2, 0x55]]),
        raw_boc(&[&[0, 0], &[0, 2, 0x55]]),
        // Valid legacy indexed BOC, outside our plain generic-V1 canon.
        vec![0x68, 0xff, 0x65, 0xf3, 1, 1, 1, 1, 0, 2, 2, 0, 0],
    ] {
        let expected = read_boc(&bytes).unwrap().withdraw_single_root().unwrap();
        let (decoded, canonical) = crate::decode_boc_with_canonicality(&bytes)?;
        assert!(!canonical);
        assert_eq!(decoded.repr_hash(), expected.repr_hash());
    }
    Ok(())
}

#[test]
fn broken_crc_and_references_still_return_errors() -> anyhow::Result<()> {
    let root = graph(3);
    let mut bytes = Vec::new();
    BocWriter::with_root(&root).unwrap().write_ex(&mut bytes, false, true, None, None).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    assert!(crate::decode_boc_with_canonicality(&bytes).is_err());
    assert!(crate::decode_boc_with_canonicality(&raw_boc(&[&[1, 0, 1]])).is_err());
    let mut multiple = Vec::new();
    BocWriter::with_roots([byte_cell(1), byte_cell(2)]).unwrap().write(&mut multiple).unwrap();
    assert!(crate::decode_boc_with_canonicality(&multiple).is_err());
    let valid = write_boc(&root).unwrap();
    let (decoded, canonical) = crate::decode_boc_with_canonicality(&valid)?;
    assert!(canonical);
    assert_eq!(decoded.repr_hash(), root.repr_hash());
    Ok(())
}
