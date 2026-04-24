mod types;
pub mod u256;

pub use types::AccountCodeHash;
pub use types::AccountDataHash;
pub use types::AccountHash;
pub use types::AccountIdentifier;
pub use types::AccountRouting;
pub use types::BlockIdentifier;
pub use types::DAppIdentifier;
pub use types::DAppIdentifierPath;
pub use types::ParentRef;
pub use types::TemporaryBlockId;
pub use types::ThreadAccountsHash;
pub use types::ThreadIdentifier;
pub use types::TransactionHash;

pub trait Blake3Hashable {
    fn update_hasher(&self, hasher: &mut blake3::Hasher);
    fn hash(&self, prefix_tag: u8) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&[prefix_tag]);
        self.update_hasher(&mut hasher);
        *hasher.finalize().as_bytes()
    }
}

impl Blake3Hashable for [u8; 32] {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(self);
    }

    fn hash(&self, prefix_tag: u8) -> [u8; 32] {
        let mut buf = [0u8; 33];
        buf[0] = prefix_tag;
        buf[1..33].copy_from_slice(self);
        *blake3::hash(&buf).as_bytes()
    }
}
