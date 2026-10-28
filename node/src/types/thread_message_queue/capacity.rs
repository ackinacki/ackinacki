use std::io::Read;
use std::io::Write;

use multi_map::MultiMapAugmentation;

/// Derived canonical message-body capacity, excluded from Merkle hashes.
///
/// Capacity is persisted with the map but deliberately excluded from its Merkle
/// hash. It is fully derivable from leaf metadata and exists only to make
/// snapshot partition planning independent of the number of messages.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InboxCapacity {
    bytes: u64,
}

impl InboxCapacity {
    pub fn new(bytes: u64) -> Self {
        Self { bytes }
    }

    pub fn bytes(self) -> u64 {
        self.bytes
    }
}

impl MultiMapAugmentation for InboxCapacity {
    fn identity() -> Self {
        Self::default()
    }

    fn combine(left: &Self, right: &Self) -> Self {
        Self::new(left.bytes.checked_add(right.bytes).expect("inbox capacity overflow"))
    }

    fn replace_child(
        current: &Self,
        old_child: Option<&Self>,
        new_child: Option<&Self>,
    ) -> Option<Self> {
        let without_old = current
            .bytes
            .checked_sub(old_child.map_or(0, |child| child.bytes))
            .expect("inbox capacity underflow");
        Some(Self::new(
            without_old
                .checked_add(new_child.map_or(0, |child| child.bytes))
                .expect("inbox capacity overflow"),
        ))
    }

    fn write<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Ok(writer.write_all(&self.bytes.to_le_bytes())?)
    }

    fn read<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Self::new(u64::from_le_bytes(bytes)))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_uses_one_u64_in_memory_and_on_disk() {
        assert_eq!(std::mem::size_of::<InboxCapacity>(), 8);
        let capacity = InboxCapacity::new(u64::MAX);
        let mut bytes = Vec::new();
        capacity.write(&mut bytes).unwrap();
        assert_eq!(bytes, u64::MAX.to_le_bytes());
        assert_eq!(InboxCapacity::read(&mut bytes.as_slice()).unwrap(), capacity);
        assert!(InboxCapacity::read(&mut &bytes[..7]).is_err());
        assert_eq!(
            InboxCapacity::replace_child(
                &InboxCapacity::new(10),
                Some(&InboxCapacity::new(4)),
                Some(&InboxCapacity::new(7))
            )
            .unwrap()
            .bytes(),
            13
        );
    }

    #[test]
    #[should_panic(expected = "inbox capacity overflow")]
    fn overflow_is_an_invariant_violation() {
        InboxCapacity::combine(&InboxCapacity::new(u64::MAX), &InboxCapacity::new(1));
    }
}
