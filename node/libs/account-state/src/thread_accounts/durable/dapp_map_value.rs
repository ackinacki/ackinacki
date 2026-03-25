use std::io;

use multi_map::repo::MultiMapRepository;
use multi_map::MultiMapRef;
use multi_map::MultiMapValue;
use node_types::Blake3Hashable;

use crate::thread_accounts::durable::dapp_accounts::AccountInfo;

/// A level-1 trie value that embeds an entire account map (MultiMapRef<AccountInfo>).
/// Replaces the old DAppAccountMapHash indirection.
#[derive(Clone)]
pub struct DAppMapValue {
    pub map: MultiMapRef<AccountInfo>,
}

impl DAppMapValue {
    pub fn new(map: MultiMapRef<AccountInfo>) -> Self {
        Self { map }
    }

    pub fn empty() -> Self {
        Self { map: MultiMapRepository::<AccountInfo>::new_map() }
    }
}

impl Blake3Hashable for DAppMapValue {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        // The hash of this value IS the root hash of the embedded account map.
        // This matches DAppAccountMapHash's Blake3Hashable (32 bytes of root hash),
        // ensuring hash-compatible level-1 tries.
        hasher.update(&self.map.root.hash());
    }
}

impl MultiMapValue for DAppMapValue {
    fn write_value<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
        MultiMapRepository::<AccountInfo>::new().write_map(&self.map, w)
    }

    fn read_value<R: io::Read>(r: &mut R) -> io::Result<Self> {
        let map = MultiMapRepository::<AccountInfo>::new().read_map(r)?;
        Ok(DAppMapValue { map })
    }
}
